/*
 * bench_main.c - mdb2bench: latency benchmark for the MemDB2 C_DLL.
 *
 * Phases
 *   1. Preload  N records (default 5 million, ~128 bytes) in batches.
 *   2. Micro    each of the seven operations alone on one thread, so that
 *               latency, CPU cycles and allocations can be attributed to
 *               a single kind of transaction.
 *   3. Mixed    R readers + W writers concurrently for a fixed duration,
 *               with expiry sweeps every 500 us, optionally rate-paced.
 *
 * Time to expiry is counted in inserts ("logical time"): a record inserted
 * as the k-th record expires when the (k + lifetime)-th record is inserted.
 * This keeps the live population at about N whatever throughput the engine
 * achieves. The default lifetime allows for the rows that are cancelled
 * (deleted by id) first; with 5M records and --p-delete-id 0.5 it is about
 * 6.9M inserts, i.e. 6.9 s of wall-clock life when paced at 1M inserts/s.
 *
 * See README.md for options and interpretation.
 */
#include "bench.h"
#include "memdb2.h"
#include <process.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

/* ---------------------------------------------------------------------------
 * Globals shared with worker threads
 * ------------------------------------------------------------------------- */
#define PHASE_WARMUP   0
#define PHASE_MEASURE  1
#define PHASE_STOP     2

static const engine_vt *g_vt;
static void *g_eng;
static bench_cfg g_cfg;

static volatile LONG   g_go;          /* start barrier                      */
static volatile LONG   g_phase;
static volatile LONG   g_failed;
static volatile LONG64 g_next_id;     /* next id to allocate = logical now  */
static volatile LONG64 g_sweep_tick;  /* last claimed sweep tick            */
static uint64_t g_mix_start_ticks;
static uint64_t g_sweep_ticks;        /* sweep interval in TSC ticks        */
static uint64_t g_lifetime;           /* inserts from insert to expiry      */
static int64_t  g_live;               /* live rows, updated between phases  */
static volatile LONG64 g_ckpt_gen;    /* +1 at checkpoint start and end:
                                         odd = a checkpoint is running      */

typedef struct op_args {
    uint64_t id;
    int64_t  now;
    rec      r;
} op_args;

typedef struct tctx {
    int index;
    int writer;
    HANDLE thread;
    void *ts;
    rng r;
    uint8_t payload[MAX_PAYLOAD];
    rec_out out;
    hist h[OP_COUNT];
    hist iter;                         /* paced: response time per iteration */
    uint64_t misses[OP_COUNT];
    uint64_t conflicts[OP_COUNT];      /* retries after a clash              */
    uint64_t gave_up[OP_COUNT];        /* exceeded --max-retries             */
    /* The same, for transactions that overlapped a checkpoint (--ckpt). */
    hist hc[OP_COUNT];
    uint64_t misses_c[OP_COUNT];
    uint64_t conflicts_c[OP_COUNT];
    uint64_t gave_up_c[OP_COUNT];
    uint64_t inserted, deleted;        /* rows, measured or not              */
    uint64_t cyc0, cyc1;               /* thread cycles at measure start/end */
    volatile uint64_t sink;
} tctx;

/* ---------------------------------------------------------------------------
 * Helpers
 * ------------------------------------------------------------------------- */
static void die(const char *msg)
{
    fprintf(stderr, "%s\n", msg);
    exit(1);
}

static uint32_t payload_len(rng *r)
{
    uint32_t avg = g_cfg.record_size > 20 ? g_cfg.record_size - 20 : 0;
    uint32_t lo = avg / 2, span = avg + 1;           /* [avg/2, 3avg/2] */
    uint32_t len = lo + (uint32_t)rng_below(r, span);
    return len > MAX_PAYLOAD ? MAX_PAYLOAD : len;
}

static void make_rec(tctx *c, uint64_t id, int64_t expiry, rec *out)
{
    out->id = id;
    out->expiry = expiry;
    out->state = ST_NEW;
    out->payload_len = payload_len(&c->r);
    /* vary the bytes a little so blobs are not identical */
    c->payload[0] = (uint8_t)id;
    c->payload[1] = (uint8_t)(id >> 8);
    out->payload = c->payload;
}

static __inline uint64_t new_id(void)
{
    return (uint64_t)InterlockedIncrement64(&g_next_id) - 1;
}

static __inline uint64_t logical_now(void)
{
    return (uint64_t)g_next_id;
}

/* A random id that was inserted within the last 'lifetime' inserts, so it
   may still be live (it may have been deleted by id: see misses). */
static __inline uint64_t random_live_id(tctx *c)
{
    uint64_t hi = logical_now();
    uint64_t span = g_lifetime;
    uint64_t lo = hi > span ? hi - span : 1;
    return lo + rng_below(&c->r, hi - lo);
}

static int dispatch(tctx *c, op_kind op, op_args *a)
{
    switch (op) {
    case OP_INSERT:        return g_vt->insert(c->ts, &a->r);
    case OP_FIND_ID:       return g_vt->find_id(c->ts, a->id, &c->out);
    case OP_FIND_EARLIEST: return g_vt->find_earliest(c->ts, &c->out);
    case OP_DELETE_ID:     return g_vt->delete_id(c->ts, a->id);
    case OP_DELETE_EXPIRY: return g_vt->delete_expired(c->ts, a->now);
    case OP_TRANSITION:    { int32_t st; return g_vt->transition(c->ts, a->id, &st); }
    case OP_RECYCLE:       return g_vt->recycle(c->ts, a->now, &a->r);
    default:               return R_FAIL;
    }
}

/* Run one operation as a transaction, retrying clashes. The measured
   latency covers every attempt. */
static int timed_op(tctx *c, op_kind op, op_args *a, int record)
{
    uint64_t t0, t1;
    uint32_t attempt, clashes = 0;
    int rc, gave_up = 0, overlap;
    LONG64 gen0, gen1;

    gen0 = g_ckpt_gen;
    t0 = ticks_now();
    for (attempt = 0;; attempt++) {
        rc = dispatch(c, op, a);
        if (rc != R_CONFLICT)
            break;
        clashes++;
        if (attempt >= g_cfg.max_retries) {
            gave_up = 1;
            break;
        }
        if (attempt > 2)
            SwitchToThread();
    }
    t1 = ticks_now();
    gen1 = g_ckpt_gen;
    /* Overlapped a checkpoint if one was running at the start, or one
       started or finished while the transaction ran. */
    overlap = (gen0 & 1) || gen1 != gen0;

    if (rc == R_FAIL) {
        InterlockedExchange(&g_failed, 1);
        InterlockedExchange(&g_phase, PHASE_STOP);
        return rc;
    }
    if (overlap) {
        c->conflicts_c[op] += clashes;
        c->gave_up_c[op] += gave_up;
    } else {
        c->conflicts[op] += clashes;
        c->gave_up[op] += gave_up;
    }
    if (record) {
        hist_add(overlap ? &c->hc[op] : &c->h[op], t1 - t0);
        if (rc == R_MISS) {
            if (overlap) c->misses_c[op]++;
            else         c->misses[op]++;
        }
    }
    if (rc == R_OK) {
        if (op == OP_INSERT) c->inserted++;
        else if (op == OP_DELETE_ID || op == OP_DELETE_EXPIRY) c->deleted++;
        else if (op == OP_RECYCLE) { c->inserted++; c->deleted++; }
    }
    c->sink += c->out.id;
    return rc;
}

static tctx *tctx_new(int index, int writer)
{
    tctx *c = (tctx *)calloc(1, sizeof *c);
    int i;
    if (!c)
        die("out of memory");
    c->index = index;
    c->writer = writer;
    rng_seed(&c->r, g_cfg.seed * 1000003ULL + (uint64_t)index * 7919ULL + 17);
    for (i = 0; i < MAX_PAYLOAD; i++)
        c->payload[i] = (uint8_t)rng_next(&c->r);
    for (i = 0; i < OP_COUNT; i++) {
        hist_reset(&c->h[i]);
        hist_reset(&c->hc[i]);
    }
    hist_reset(&c->iter);
    return c;
}

static void pin_thread(HANDLE h, int cpu)
{
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    if (si.dwNumberOfProcessors == 0)
        return;
    cpu %= (int)si.dwNumberOfProcessors;
    if (cpu < (int)(8 * sizeof(DWORD_PTR)))
        SetThreadAffinityMask(h, (DWORD_PTR)1 << cpu);
}

/* ---------------------------------------------------------------------------
 * Reporting
 * ------------------------------------------------------------------------- */
static FILE *g_csv;
#if defined(_WIN64)
static const char *g_arch = "x64";
#else
static const char *g_arch = "x86";
#endif

static void csv_open(void)
{
    long size = 0;
    if (!g_cfg.csv_path)
        return;
    if (fopen_s(&g_csv, g_cfg.csv_path, "ab") != 0 || !g_csv) {
        fprintf(stderr, "Cannot open %s\n", g_cfg.csv_path);
        g_csv = NULL;
        return;
    }
    fseek(g_csv, 0, SEEK_END);
    size = ftell(g_csv);
    if (size == 0)
        fprintf(g_csv, "label,engine,arch,sync,writer_mode,reader_mode,isolation,readers,writers,"
                       "records,phase,op,ops,ops_per_s,mean_ns,p50_ns,p99_ns,p999_ns,p9999_ns,max_ns,"
                       "thread_cycles_per_op,process_cycles_per_op,allocs_per_op,misses,conflicts,gave_up\n");
}

static const char *iso_name(int iso)
{
    switch (iso) {
    case MDB_ISO_READ_COMMITTED: return "rc";
    case MDB_ISO_REPEATABLE_READ: return "rr";
    case MDB_ISO_SNAPSHOT: return "snap";
    case MDB_ISO_SERIALISABLE: return "ser";
    default: return "?";
    }
}

static void csv_row(const char *phase, const char *op, const hist *h, double secs,
                    double tcyc, double pcyc, double allocs,
                    uint64_t misses, uint64_t conflicts, uint64_t gave_up)
{
    if (!g_csv)
        return;
    fprintf(g_csv, "%s,%s,%s,%s,%s,%s,%s,%d,%d,%llu,%s,%s,%llu,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f,"
                   "%.1f,%.1f,%.3f,%llu,%llu,%llu\n",
            g_cfg.label ? g_cfg.label : "", g_vt->name, g_arch,
            g_cfg.sync_flush ? "flush" : "lazy",
            g_cfg.writer_exclusive ? "exclusive" : "shared",
            g_cfg.reader_rw ? (g_cfg.reader_commit ? "rw-commit" : "rw")
                            : (g_cfg.reader_commit ? "read-commit" : "read"),
            iso_name(g_cfg.isolation), g_cfg.readers, g_cfg.writers,
            (unsigned long long)g_cfg.records, phase, op,
            (unsigned long long)h->count, secs > 0 ? (double)h->count / secs : 0.0,
            h->count ? ticks_to_ns(h->sum) / (double)h->count : 0.0,
            ticks_to_ns(hist_percentile(h, 0.50)),
            ticks_to_ns(hist_percentile(h, 0.99)),
            ticks_to_ns(hist_percentile(h, 0.999)),
            ticks_to_ns(hist_percentile(h, 0.9999)),
            ticks_to_ns(h->count ? h->max : 0),
            tcyc, pcyc, allocs,
            (unsigned long long)misses, (unsigned long long)conflicts,
            (unsigned long long)gave_up);
    fflush(g_csv);
}

static void print_lat_header(const char *extra)
{
    printf("  %-20s %10s %9s %8s %8s %8s %8s %8s %9s%s\n",
           "operation", "txns", "ktxn/s", "mean", "p50", "p99", "p99.9", "p99.99", "max",
           extra);
}

static void print_lat(const char *name, const hist *h, double secs)
{
    double us = g_ns_per_tick / 1000.0;
    if (!h->count) {
        printf("  %-20s %10s\n", name, "-");
        return;
    }
    printf("  %-20s %10llu %9.1f %8.2f %8.2f %8.2f %8.2f %8.2f %9.1f",
           name, (unsigned long long)h->count,
           secs > 0 ? (double)h->count / secs / 1000.0 : 0.0,
           (double)h->sum / (double)h->count * us,
           (double)hist_percentile(h, 0.50) * us,
           (double)hist_percentile(h, 0.99) * us,
           (double)hist_percentile(h, 0.999) * us,
           (double)hist_percentile(h, 0.9999) * us,
           (double)h->max * us);
}

/* ---------------------------------------------------------------------------
 * Phase 1: preload
 * ------------------------------------------------------------------------- */
static void preload(void)
{
    tctx *c = tctx_new(1000, 1);
    rec *batch;
    uint64_t done = 0, next_report, t0, t1;
    uint32_t n, i;
    uint64_t mem0, mem1;

    if (g_vt->thread_init(g_eng, &c->ts) != R_OK)
        die("thread_init failed");
    batch = (rec *)calloc(g_cfg.preload_batch, sizeof *batch);
    if (!batch)
        die("out of memory");

    printf("\nPhase 1: preload %llu records, %u per transaction\n",
           (unsigned long long)g_cfg.records, g_cfg.preload_batch);
    mem0 = process_private_bytes();
    next_report = g_cfg.records / 10;
    t0 = ticks_now();
    while (done < g_cfg.records) {
        n = g_cfg.preload_batch;
        if (g_cfg.records - done < n)
            n = (uint32_t)(g_cfg.records - done);
        for (i = 0; i < n; i++) {
            uint64_t id = done + i + 1;
            /* expire at logical time N + U[1, lifetime]: nothing expired
               at start, then roughly one per insert. */
            int64_t exp = (int64_t)(g_cfg.records + 1 + rng_below(&c->r, g_lifetime));
            make_rec(c, id, exp, &batch[i]);
        }
        if (g_vt->preload_batch(c->ts, batch, n) != R_OK)
            die("preload failed");
        done += n;
        if (done >= next_report && done < g_cfg.records) {
            printf("  %3.0f%%  %.1f s\n", 100.0 * (double)done / (double)g_cfg.records,
                   ticks_to_ns(ticks_now() - t0) / 1e9);
            next_report += g_cfg.records / 10;
        }
    }
    t1 = ticks_now();
    mem1 = process_private_bytes();
    printf("  loaded in %.2f s (%.0f rows/s)\n", ticks_to_ns(t1 - t0) / 1e9,
           (double)g_cfg.records / (ticks_to_ns(t1 - t0) / 1e9));
    printf("  private memory: +%.1f MB, %.0f bytes per record (logical record ~%u bytes)\n",
           (double)(mem1 - mem0) / 1048576.0,
           (double)(mem1 - mem0) / (double)g_cfg.records, g_cfg.record_size);

    t0 = ticks_now();
    if (g_vt->after_preload(g_eng) != R_OK)
        die("after_preload (checkpoint) failed");
    t1 = ticks_now();
    if (g_cfg.checkpoint && strcmp(g_vt->name, "memdb2") == 0)
        printf("  checkpoint: %.2f s\n", ticks_to_ns(t1 - t0) / 1e9);

    g_next_id = (LONG64)(g_cfg.records + 1);
    g_live = (int64_t)g_cfg.records;
    free(batch);
    g_vt->thread_done(c->ts);
    free(c);
}

/* ---------------------------------------------------------------------------
 * Phase 2: isolated operations on one thread
 * ------------------------------------------------------------------------- */
static void micro(void)
{
    static const op_kind order[OP_COUNT] = {
        OP_INSERT, OP_FIND_ID, OP_FIND_EARLIEST, OP_TRANSITION,
        OP_DELETE_ID, OP_DELETE_EXPIRY, OP_RECYCLE
    };
    tctx *c = tctx_new(2000, 1);
    uint64_t *ids, n = g_cfg.micro_ops, i, j, tmp;
    HANDLE self = GetCurrentThread();
    int k, have_alloc;
    op_args a;

    if (g_cfg.pin)
        pin_thread(self, 0);
    if (g_vt->thread_init(g_eng, &c->ts) != R_OK)
        die("thread_init failed");
    ids = (uint64_t *)malloc((size_t)n * sizeof *ids);
    if (!ids)
        die("out of memory");

    printf("\nPhase 2: isolated operations, %llu transactions each, one thread\n",
           (unsigned long long)n);
    print_lat_header("  thr-cyc  proc-cyc  allocs   miss");
    printf("  %-20s %10s %9s %8s %8s %8s %8s %8s %9s\n", "", "", "",
           "us", "us", "us", "us", "us", "us");

    for (k = 0; k < OP_COUNT && !g_failed; k++) {
        op_kind op = order[k];
        uint64_t tc0, tc1, pc0, pc1, a0 = 0, f0 = 0, a1 = 0, f1 = 0, w0, w1;
        double secs;

        memset(&a, 0, sizeof a);
        if (op == OP_DELETE_ID) {
            /* delete the rows the insert step added, in random order */
            for (i = n; i > 1; i--) {
                j = rng_below(&c->r, i);
                tmp = ids[i - 1]; ids[i - 1] = ids[j]; ids[j] = tmp;
            }
        }
        g_vt->alloc_enable(g_eng, 1);
        have_alloc = g_vt->alloc_stats(g_eng, &a0, &f0);
        tc0 = thread_cycles(self);
        pc0 = process_cycles();
        w0 = ticks_now();
        /* For expiry operations, look far enough ahead in logical time that
           there are at least n expired rows. */
        a.now = (int64_t)(logical_now() + 2 * n);
        for (i = 0; i < n && !g_failed; i++) {
            switch (op) {
            case OP_INSERT:
                ids[i] = new_id();
                make_rec(c, ids[i], (int64_t)(ids[i] + g_lifetime), &a.r);
                break;
            case OP_FIND_ID:
            case OP_TRANSITION:
                a.id = random_live_id(c);
                break;
            case OP_DELETE_ID:
                a.id = ids[i];
                break;
            case OP_RECYCLE: {
                uint64_t id = new_id();
                make_rec(c, id, (int64_t)(id + g_lifetime), &a.r);
                break;
            }
            default:
                break;
            }
            timed_op(c, op, &a, 1);
        }
        w1 = ticks_now();
        pc1 = process_cycles();
        tc1 = thread_cycles(self);
        if (have_alloc)
            g_vt->alloc_stats(g_eng, &a1, &f1);
        g_vt->alloc_enable(g_eng, 0);

        secs = ticks_to_ns(w1 - w0) / 1e9;
        {
            const hist *h = &c->h[op];
            double cnt = h->count ? (double)h->count : 1.0;
            double tcyc = (double)(tc1 - tc0) / cnt;
            double pcyc = (double)(pc1 - pc0) / cnt;
            double al = have_alloc ? (double)(a1 - a0) / cnt : -1.0;
            print_lat(g_op_names[op], h, secs);
            if (have_alloc)
                printf(" %8.0f %9.0f %7.2f %6llu\n", tcyc, pcyc, al,
                       (unsigned long long)c->misses[op]);
            else
                printf(" %8.0f %9.0f %7s %6llu\n", tcyc, pcyc, "n/a",
                       (unsigned long long)c->misses[op]);
            csv_row("micro", g_op_names[op], h, secs, tcyc, pcyc, al,
                    c->misses[op], c->conflicts[op], c->gave_up[op]);
        }
    }
    printf("  thr-cyc = this thread's CPU cycles per txn; proc-cyc = whole process\n"
           "  (includes the journal thread); allocs = memory manager allocations per txn.\n");

    g_live += (int64_t)c->inserted - (int64_t)c->deleted;
    free(ids);
    g_vt->thread_done(c->ts);
    free(c);
    if (g_cfg.pin)
        SetThreadAffinityMask(self, (DWORD_PTR)-1);
}

/* ---------------------------------------------------------------------------
 * Phase 3: mixed concurrent workload
 * ------------------------------------------------------------------------- */
static void writer_iteration(tctx *c)
{
    op_args a;
    int record = g_phase == PHASE_MEASURE;
    uint64_t tick, id;
    LONG64 last;

    /* Expiry sweep: every sweep interval, one writer claims the tick and
       deletes everything that has expired (up to --sweep-cap rows). */
    tick = (ticks_now() - g_mix_start_ticks) / g_sweep_ticks;
    last = g_sweep_tick;
    if ((LONG64)tick > last &&
        InterlockedCompareExchange64(&g_sweep_tick, (LONG64)tick, last) == last) {
        uint32_t k;
        a.now = (int64_t)logical_now();
        for (k = 0; k < g_cfg.sweep_cap && g_phase != PHASE_STOP; k++)
            if (timed_op(c, OP_DELETE_EXPIRY, &a, record) != R_OK)
                break;
    }

    id = new_id();
    make_rec(c, id, (int64_t)(id + g_lifetime), &a.r);
    timed_op(c, OP_INSERT, &a, record);

    if (g_cfg.p_delete_id > 0 && rng_unit(&c->r) < g_cfg.p_delete_id) {
        a.id = random_live_id(c);
        timed_op(c, OP_DELETE_ID, &a, record);
    }
    if (g_cfg.p_transition > 0 && rng_unit(&c->r) < g_cfg.p_transition) {
        a.id = random_live_id(c);
        timed_op(c, OP_TRANSITION, &a, record);
    }
    if (g_cfg.p_recycle > 0 && rng_unit(&c->r) < g_cfg.p_recycle) {
        id = new_id();
        make_rec(c, id, (int64_t)(id + g_lifetime), &a.r);
        a.now = (int64_t)logical_now();
        timed_op(c, OP_RECYCLE, &a, record);
    }
}

static void reader_iteration(tctx *c, uint64_t n)
{
    op_args a;
    int record = g_phase == PHASE_MEASURE;
    if (g_cfg.earliest_every && n % g_cfg.earliest_every == 0) {
        timed_op(c, OP_FIND_EARLIEST, &a, record);
    } else {
        a.id = random_live_id(c);
        timed_op(c, OP_FIND_ID, &a, record);
    }
}

static unsigned __stdcall worker_main(void *p)
{
    tctx *c = (tctx *)p;
    double rate = c->writer
        ? (g_cfg.insert_rate > 0 ? g_cfg.insert_rate / g_cfg.writers : 0)
        : (g_cfg.read_rate > 0 ? g_cfg.read_rate / g_cfg.readers : 0);
    uint64_t interval = rate > 0 ? (uint64_t)(g_tsc_hz / rate) : 0;
    uint64_t next = 0, n = 0;

    while (!g_go)
        YieldProcessor();
    next = ticks_now();
    while (g_phase != PHASE_STOP) {
        if (interval) {
            while (ticks_now() < next)
                YieldProcessor();
        }
        if (c->writer)
            writer_iteration(c);
        else
            reader_iteration(c, n);
        n++;
        if (interval) {
            /* Response time from the intended start, so that falling behind
               shows up (no coordinated omission). */
            uint64_t now = ticks_now();
            if (g_phase == PHASE_MEASURE)
                hist_add(&c->iter, now > next ? now - next : 0);
            next += interval;
        }
    }
    return 0;
}

/* Checkpointer: back-to-back checkpoints from a thread of its own until the
   run stops. A new one starts as soon as the previous one returns (after
   --ckpt-gap-ms, if set). */
typedef struct ckpt_stats {
    hist h;                  /* duration of checkpoints started while measuring */
    uint64_t started, completed;
    uint64_t busy_ticks;     /* measured time with a checkpoint running         */
} ckpt_stats;

static unsigned __stdcall ckpt_main(void *p)
{
    ckpt_stats *s = (ckpt_stats *)p;
    int async = g_cfg.ckpt_mode == 1;
    while (!g_go)
        YieldProcessor();
    while (g_phase != PHASE_STOP && !g_failed) {
        int rec = g_phase == PHASE_MEASURE;
        uint64_t t0, t1;
        int rc;
        InterlockedIncrement64(&g_ckpt_gen);           /* now odd: running */
        s->started++;
        t0 = ticks_now();
        rc = g_vt->checkpoint(g_eng, async);
        t1 = ticks_now();
        InterlockedIncrement64(&g_ckpt_gen);           /* even: idle */
        if (rc != R_OK) {
            InterlockedExchange(&g_failed, 1);
            InterlockedExchange(&g_phase, PHASE_STOP);
            break;
        }
        s->completed++;
        if (rec) {
            hist_add(&s->h, t1 - t0);
            s->busy_ticks += t1 - t0;
        }
        if (g_cfg.ckpt_gap_ms > 0) {
            double left = g_cfg.ckpt_gap_ms;
            while (left > 0 && g_phase != PHASE_STOP) {
                DWORD slice = left > 50 ? 50 : (DWORD)left;
                Sleep(slice ? slice : 1);
                left -= slice ? slice : 1;
            }
        }
    }
    return 0;
}

/* Print and log one latency table. */
static void report_ops(const char *title, const char *phase, const hist *agg,
                       const uint64_t *misses, const uint64_t *confl,
                       const uint64_t *gaveup, double secs)
{
    int k;
    uint64_t n = 0;
    printf("\n  %s\n", title);
    for (k = 0; k < OP_COUNT; k++)
        n += agg[k].count;
    if (!n) {
        printf("  (none%s)\n", g_cfg.ckpt_mode && g_cfg.ckpt_gap_ms <= 0
               ? ": checkpoints ran back to back, so every transaction overlapped one;\n"
                 "   compare with a run without --ckpt, or add --ckpt-gap-ms" : "");
        return;
    }
    print_lat_header("     miss   clashes  gave-up");
    for (k = 0; k < OP_COUNT; k++) {
        if (!agg[k].count)
            continue;
        print_lat(g_op_names[k], &agg[k], secs);
        printf(" %8llu %9llu %8llu\n", (unsigned long long)misses[k],
               (unsigned long long)confl[k], (unsigned long long)gaveup[k]);
        csv_row(phase, g_op_names[k], &agg[k], secs, -1, -1, -1,
                misses[k], confl[k], gaveup[k]);
    }
}

static void mixed(void)
{
    int nthreads = g_cfg.readers + g_cfg.writers, i, k;
    tctx **cs = (tctx **)calloc((size_t)nthreads, sizeof *cs);
    uint64_t pc0, pc1, w0, w1, a0 = 0, f0 = 0, a1 = 0, f1 = 0, start_rows;
    int have_alloc;
    double secs;
    /* agg[0..6] outside checkpoints, aggc[0..6] overlapping one,
       aggt[0..6] both, then 2 paced-iteration histograms. */
    hist *agg = (hist *)malloc(sizeof(hist) * (3 * OP_COUNT + 2));
    hist *aggc = agg + OP_COUNT, *aggt = agg + 2 * OP_COUNT, *iters = agg + 3 * OP_COUNT;
    uint64_t misses[OP_COUNT] = {0}, confl[OP_COUNT] = {0}, gaveup[OP_COUNT] = {0};
    uint64_t misses_c[OP_COUNT] = {0}, confl_c[OP_COUNT] = {0}, gaveup_c[OP_COUNT] = {0};
    uint64_t wcyc = 0, rcyc = 0, wtx = 0, rtx = 0, ins = 0, del = 0;
    ckpt_stats *ck = NULL;
    HANDLE ck_thread = NULL;
    uint64_t ck_cyc0 = 0, ck_cyc1 = 0;

    if (!cs || !agg)
        die("out of memory");

    printf("\nPhase 3: mixed workload, %d readers + %d writers, %.0f s (+%.0f s warm-up)\n",
           g_cfg.readers, g_cfg.writers, g_cfg.duration_s, g_cfg.warmup_s);
    printf("  writers: insert; delete-by-id p=%.2f; transition p=%.2f; recycle p=%.2f;\n"
           "           expiry sweep every %.0f us (max %u rows)\n",
           g_cfg.p_delete_id, g_cfg.p_transition, g_cfg.p_recycle,
           g_cfg.sweep_us, g_cfg.sweep_cap);
    printf("  readers: find-by-id, find-earliest every %u reads\n", g_cfg.earliest_every);
    if (g_cfg.insert_rate > 0)
        printf("  paced: %.0f inserts/s total", g_cfg.insert_rate);
    else
        printf("  writers unpaced (closed loop)");
    if (g_cfg.read_rate > 0)
        printf(", %.0f reads/s total\n", g_cfg.read_rate);
    else
        printf(", readers unpaced\n");
    if (g_cfg.ckpt_mode) {
        printf("  checkpoints: %s, back to back", g_cfg.ckpt_mode == 1 ? "asynchronous" : "synchronous");
        if (g_cfg.ckpt_gap_ms > 0)
            printf(" with %.0f ms between", g_cfg.ckpt_gap_ms);
        printf(", from a separate thread\n");
        if (g_cfg.ckpt_mode == 1 && (!g_cfg.reader_rw || g_cfg.writer_exclusive))
            printf("  note: only READ_WRITE_SHARED transactions run during an asynchronous\n"
                   "  checkpoint; %s wait for it (--reader-mode rw --writer-mode shared\n"
                   "  to avoid that).\n",
                   !g_cfg.reader_rw && g_cfg.writer_exclusive ? "readers and writers"
                   : !g_cfg.reader_rw ? "READ_SHARED readers" : "WRITE_EXCLUSIVE writers");
    }

    g_go = 0;
    g_phase = PHASE_WARMUP;
    g_sweep_tick = 0;
    g_ckpt_gen = 0;
    g_sweep_ticks = (uint64_t)(g_cfg.sweep_us * 1e-6 * g_tsc_hz);
    if (g_sweep_ticks == 0)
        g_sweep_ticks = 1;
    start_rows = (uint64_t)g_next_id;

    for (i = 0; i < nthreads; i++) {
        tctx *c = tctx_new(i, i < g_cfg.writers);
        if (g_vt->thread_init(g_eng, &c->ts) != R_OK)
            die("thread_init failed");
        c->thread = (HANDLE)_beginthreadex(NULL, 0, worker_main, c, 0, NULL);
        if (!c->thread)
            die("cannot start thread");
        if (g_cfg.pin)
            pin_thread(c->thread, i);
        cs[i] = c;
    }
    if (g_cfg.ckpt_mode) {
        ck = (ckpt_stats *)calloc(1, sizeof *ck);
        if (!ck)
            die("out of memory");
        hist_reset(&ck->h);
        ck_thread = (HANDLE)_beginthreadex(NULL, 0, ckpt_main, ck, 0, NULL);
        if (!ck_thread)
            die("cannot start checkpoint thread");
    }

    g_mix_start_ticks = ticks_now();
    InterlockedExchange(&g_go, 1);
    Sleep((DWORD)(g_cfg.warmup_s * 1000.0));

    have_alloc = 0;
    if (g_cfg.count_allocs) {
        g_vt->alloc_enable(g_eng, 1);
        have_alloc = g_vt->alloc_stats(g_eng, &a0, &f0);
    }
    for (i = 0; i < nthreads; i++)
        cs[i]->cyc0 = thread_cycles(cs[i]->thread);
    if (ck_thread)
        ck_cyc0 = thread_cycles(ck_thread);
    pc0 = process_cycles();
    w0 = ticks_now();
    InterlockedExchange(&g_phase, PHASE_MEASURE);

    {
        /* Sleep in slices so a failure stops the run early. */
        double left = g_cfg.duration_s * 1000.0;
        while (left > 0 && !g_failed) {
            DWORD slice = left > 250 ? 250 : (DWORD)left;
            Sleep(slice);
            left -= slice;
        }
    }

    w1 = ticks_now();
    pc1 = process_cycles();
    for (i = 0; i < nthreads; i++)
        cs[i]->cyc1 = thread_cycles(cs[i]->thread);
    if (ck_thread)
        ck_cyc1 = thread_cycles(ck_thread);
    if (have_alloc)
        g_vt->alloc_stats(g_eng, &a1, &f1);
    InterlockedExchange(&g_phase, PHASE_STOP);
    g_vt->alloc_enable(g_eng, 0);

    for (i = 0; i < nthreads; i++) {
        WaitForSingleObject(cs[i]->thread, INFINITE);
        CloseHandle(cs[i]->thread);
    }
    if (ck_thread) {
        if (g_ckpt_gen & 1)
            printf("\n  (waiting for the running checkpoint to finish)\n");
        WaitForSingleObject(ck_thread, INFINITE);
        CloseHandle(ck_thread);
    }

    secs = ticks_to_ns(w1 - w0) / 1e9;
    for (k = 0; k < 3 * OP_COUNT + 2; k++)
        hist_reset(&agg[k]);
    for (i = 0; i < nthreads; i++) {
        tctx *c = cs[i];
        uint64_t tx = 0;
        for (k = 0; k < OP_COUNT; k++) {
            hist_merge(&agg[k], &c->h[k]);
            hist_merge(&aggc[k], &c->hc[k]);
            misses[k] += c->misses[k];
            confl[k] += c->conflicts[k];
            gaveup[k] += c->gave_up[k];
            misses_c[k] += c->misses_c[k];
            confl_c[k] += c->conflicts_c[k];
            gaveup_c[k] += c->gave_up_c[k];
            tx += c->h[k].count + c->hc[k].count;
        }
        hist_merge(&iters[c->writer ? 0 : 1], &c->iter);
        ins += c->inserted;
        del += c->deleted;
        if (c->writer) { wcyc += c->cyc1 - c->cyc0; wtx += tx; }
        else           { rcyc += c->cyc1 - c->cyc0; rtx += tx; }
    }
    for (k = 0; k < OP_COUNT; k++) {
        hist_merge(&aggt[k], &agg[k]);
        hist_merge(&aggt[k], &aggc[k]);
    }

    if (!g_cfg.ckpt_mode) {
        char title[96];
        sprintf_s(title, sizeof title, "Latency (us), measured over %.1f s:", secs);
        report_ops(title, "mixed", agg, misses, confl, gaveup, secs);
    } else {
        char title[160];
        double busy = ck->busy_ticks ? ticks_to_ns(ck->busy_ticks) / 1e9 : 0.0;
        sprintf_s(title, sizeof title,
                  "Latency (us), transactions NOT overlapping a checkpoint "
                  "(rates over the whole %.1f s):", secs);
        report_ops(title, "mixed", agg, misses, confl, gaveup, secs);
        sprintf_s(title, sizeof title,
                  "Latency (us), transactions overlapping a checkpoint:");
        report_ops(title, g_cfg.ckpt_mode == 1 ? "mixed-ckpt-async" : "mixed-ckpt-sync",
                   aggc, misses_c, confl_c, gaveup_c, secs);

        printf("\n  Checkpoints (%s): %llu started, %llu completed during the run;"
               " %llu timed while measuring\n",
               g_cfg.ckpt_mode == 1 ? "async" : "sync",
               (unsigned long long)ck->started, (unsigned long long)ck->completed,
               (unsigned long long)ck->h.count);
        if (ck->h.count) {
            double ms = g_ns_per_tick / 1e6;
            printf("  duration: mean %.1f ms, p50 %.1f ms, max %.1f ms;"
                   " a checkpoint was running %.0f%% of the measured time\n",
                   (double)ck->h.sum / (double)ck->h.count * ms,
                   (double)hist_percentile(&ck->h, 0.5) * ms,
                   (double)ck->h.max * ms,
                   secs > 0 ? 100.0 * (busy < secs ? busy : secs) / secs : 0.0);
            printf("  checkpoint thread CPU: %.0f Mcycles while measuring\n",
                   (double)(ck_cyc1 - ck_cyc0) / 1e6);
            csv_row(g_cfg.ckpt_mode == 1 ? "mixed-ckpt-async" : "mixed-ckpt-sync",
                    "checkpoint", &ck->h, secs, (double)(ck_cyc1 - ck_cyc0) / (double)ck->h.count,
                    -1, -1, 0, 0, 0);
        } else {
            printf("  no checkpoint started and finished inside the measured window:"
                   " lengthen --duration\n");
        }
    }

    if (iters[0].count) {
        printf("\n  Paced response time from intended start (us):\n");
        print_lat_header("");
        print_lat("writer iteration", &iters[0], secs);
        printf("\n");
        csv_row("mixed", "writer iteration (paced)", &iters[0], secs, -1, -1, -1, 0, 0, 0);
    }
    if (iters[1].count) {
        if (!iters[0].count) {
            printf("\n  Paced response time from intended start (us):\n");
            print_lat_header("");
        }
        print_lat("reader request", &iters[1], secs);
        printf("\n");
        csv_row("mixed", "reader request (paced)", &iters[1], secs, -1, -1, -1, 0, 0, 0);
    }

    {
        double all = (double)(wtx + rtx);
        uint64_t dels = 0;
        hist total;
        hist_reset(&total);
        for (k = 0; k < OP_COUNT; k++)
            hist_merge(&total, &aggt[k]);
        dels = aggt[OP_DELETE_ID].count - misses[OP_DELETE_ID] - misses_c[OP_DELETE_ID]
             + aggt[OP_DELETE_EXPIRY].count - misses[OP_DELETE_EXPIRY] - misses_c[OP_DELETE_EXPIRY];
        printf("\n  Throughput: %.0f txn/s (writers %.0f, readers %.0f)\n",
               all / secs, (double)wtx / secs, (double)rtx / secs);
        g_live += (int64_t)ins - (int64_t)del;
        printf("  Rows: inserted %.0f/s, deleted %.0f/s; live rows at end %lld "
               "(%llu ids issued in phase)\n",
               (double)aggt[OP_INSERT].count / secs, (double)dels / secs,
               (long long)g_live,
               (unsigned long long)((uint64_t)g_next_id - start_rows));
        printf("  CPU cycles per txn: writer threads %.0f, reader threads %.0f, "
               "whole process %.0f%s\n",
               wtx ? (double)wcyc / (double)wtx : 0.0,
               rtx ? (double)rcyc / (double)rtx : 0.0,
               all ? (double)(pc1 - pc0) / all : 0.0,
               g_cfg.ckpt_mode ? " (process includes checkpoints)" : "");
        if (have_alloc)
            printf("  Allocations per txn: %.2f (frees %.2f)\n",
                   all ? (double)(a1 - a0) / all : 0.0,
                   all ? (double)(f1 - f0) / all : 0.0);
        else if (!g_cfg.count_allocs)
            printf("  Allocations per txn: not counted here (add --count-allocs; the shared\n"
                   "  counter adds contention, so latencies from that run are not comparable)\n");
        else
            printf("  Allocations per txn: n/a (DLL built without MemDB2BenchHooks)\n");
        csv_row("mixed", "all", &total, secs,
                all ? (double)(wcyc + rcyc) / all : 0.0,
                all ? (double)(pc1 - pc0) / all : 0.0,
                have_alloc && all ? (double)(a1 - a0) / all : -1.0, 0, 0, 0);
    }

    for (i = 0; i < nthreads; i++) {
        g_vt->thread_done(cs[i]->ts);
        free(cs[i]);
    }
    free(cs);
    free(agg);
    free(ck);
}

/* ---------------------------------------------------------------------------
 * Command line
 * ------------------------------------------------------------------------- */
static double parse_num(const char *s)
{
    char *end;
    double v = strtod(s, &end);
    switch (*end) {
    case 'k': case 'K': v *= 1e3; break;
    case 'm': case 'M': v *= 1e6; break;
    case 'g': case 'G': v *= 1e9; break;
    default: break;
    }
    return v;
}

static void usage(void)
{
    printf(
"mdb2bench - MemDB2 C_DLL latency benchmark\n"
"\n"
"  --engine memdb2|baseline  engine to measure (default memdb2)\n"
"  --db <folder>             MemDB2 database folder (default .\\mdb2bench_db)\n"
"                            Its .dbincr/.dbinit files are deleted at start if\n"
"                            the folder name contains \"mdb2bench\"\n"
"  --fresh                   delete them whatever the folder is called\n"
"  --keep-db                 never delete them (the old journal is replayed)\n"
"  --records N               initial records (default 5M; k/M suffixes ok)\n"
"  --record-size B           average record size in bytes (default 128)\n"
"  --readers N --writers N   threads in the mixed phase (default 4 + 4)\n"
"  --duration S --warmup S   mixed phase length (default 30 s + 5 s)\n"
"  --micro-ops N             transactions per op in the isolated phase (200k)\n"
"  --no-micro / --no-mixed   skip a phase\n"
"  --rate R                  pace writers at R inserts/s total (0 = unpaced)\n"
"  --read-rate R             pace readers at R reads/s total (0 = unpaced)\n"
"  --sweep-us U              expiry sweep interval (default 500)\n"
"  --sweep-cap N             max rows deleted per sweep (default 10000)\n"
"  --p-delete-id P           per insert, chance of a delete by id (0.5)\n"
"  --p-transition P          per insert, chance of a state transition (0.25)\n"
"  --p-recycle P             per insert, chance of a recycle (0.1)\n"
"  --earliest-every N        reader: 1 in N reads is find-earliest (10)\n"
"  --sync lazy|flush         MemDB2 commit sync mode (default lazy)\n"
"  --writer-mode shared|exclusive   READ_WRITE_SHARED or WRITE_EXCLUSIVE\n"
"  --reader-mode read|rw     READ_SHARED (default) or READ_WRITE_SHARED\n"
"  --reader-end rollback|commit   how readers end a transaction (rollback)\n"
"  --iso rc|rr|snap|ser      isolation level (default rr)\n"
"  --batch N                 preload rows per transaction (default 10000)\n"
"  --no-checkpoint           skip the checkpoint after preload\n"
"  --max-retries N           retries on a concurrency clash (default 1000)\n"
"  --pin                     pin threads to CPUs 0..n-1\n"
"  --count-allocs            also count allocations in the mixed phase\n"
"  --ckpt async|sync|off     mixed phase: checkpoint back to back from another\n"
"                            thread, and report transactions that overlapped a\n"
"                            checkpoint separately (default off)\n"
"  --ckpt-gap-ms N           pause between those checkpoints (default 0)\n"
"  --csv <file>              append results to a CSV file\n"
"  --label <text>            label for the CSV rows\n"
"  --seed N                  random seed (default 1)\n"
"  --lifetime N              inserts from insert to expiry (default: chosen\n"
"                            so that about --records rows stay live)\n");
}

static void parse_args(int argc, char **argv)
{
    int i;
    bench_cfg *c = &g_cfg;
    memset(c, 0, sizeof *c);
    c->engine = "memdb2";
    c->db_path = "mdb2bench_db";
    c->records = 5000000;
    c->record_size = 128;
    c->readers = 4;
    c->writers = 4;
    c->duration_s = 30;
    c->warmup_s = 5;
    c->micro_ops = 200000;
    c->run_micro = 1;
    c->run_mixed = 1;
    c->sweep_us = 500;
    c->sweep_cap = 10000;
    c->p_delete_id = 0.5;
    c->p_transition = 0.25;
    c->p_recycle = 0.1;
    c->earliest_every = 10;
    c->isolation = MDB_ISO_REPEATABLE_READ;
    c->preload_batch = 10000;
    c->checkpoint = 1;
    c->max_retries = 1000;
    c->seed = 1;
    c->lifetime = 0;

#define NEXT() (i + 1 < argc ? argv[++i] : (usage(), exit(2), (char *)0))
    for (i = 1; i < argc; i++) {
        const char *a = argv[i];
        if (!strcmp(a, "--engine")) c->engine = NEXT();
        else if (!strcmp(a, "--db")) c->db_path = NEXT();
        else if (!strcmp(a, "--records")) c->records = (uint64_t)parse_num(NEXT());
        else if (!strcmp(a, "--record-size")) c->record_size = (uint32_t)parse_num(NEXT());
        else if (!strcmp(a, "--readers")) c->readers = (int)parse_num(NEXT());
        else if (!strcmp(a, "--writers")) c->writers = (int)parse_num(NEXT());
        else if (!strcmp(a, "--duration")) c->duration_s = parse_num(NEXT());
        else if (!strcmp(a, "--warmup")) c->warmup_s = parse_num(NEXT());
        else if (!strcmp(a, "--micro-ops")) c->micro_ops = (uint64_t)parse_num(NEXT());
        else if (!strcmp(a, "--no-micro")) c->run_micro = 0;
        else if (!strcmp(a, "--no-mixed")) c->run_mixed = 0;
        else if (!strcmp(a, "--rate")) c->insert_rate = parse_num(NEXT());
        else if (!strcmp(a, "--read-rate")) c->read_rate = parse_num(NEXT());
        else if (!strcmp(a, "--sweep-us")) c->sweep_us = parse_num(NEXT());
        else if (!strcmp(a, "--sweep-cap")) c->sweep_cap = (uint32_t)parse_num(NEXT());
        else if (!strcmp(a, "--p-delete-id")) c->p_delete_id = parse_num(NEXT());
        else if (!strcmp(a, "--p-transition")) c->p_transition = parse_num(NEXT());
        else if (!strcmp(a, "--p-recycle")) c->p_recycle = parse_num(NEXT());
        else if (!strcmp(a, "--earliest-every")) c->earliest_every = (uint32_t)parse_num(NEXT());
        else if (!strcmp(a, "--sync")) {
            const char *v = NEXT();
            if (!strcmp(v, "lazy")) c->sync_flush = 0;
            else if (!strcmp(v, "flush")) c->sync_flush = 1;
            else { usage(); exit(2); }
        }
        else if (!strcmp(a, "--writer-mode")) {
            const char *v = NEXT();
            if (!strcmp(v, "shared")) c->writer_exclusive = 0;
            else if (!strcmp(v, "exclusive")) c->writer_exclusive = 1;
            else { usage(); exit(2); }
        }
        else if (!strcmp(a, "--reader-mode")) {
            const char *v = NEXT();
            if (!strcmp(v, "read")) c->reader_rw = 0;
            else if (!strcmp(v, "rw")) c->reader_rw = 1;
            else { usage(); exit(2); }
        }
        else if (!strcmp(a, "--reader-end")) {
            const char *v = NEXT();
            if (!strcmp(v, "rollback")) c->reader_commit = 0;
            else if (!strcmp(v, "commit")) c->reader_commit = 1;
            else { usage(); exit(2); }
        }
        else if (!strcmp(a, "--iso")) {
            const char *v = NEXT();
            if (!strcmp(v, "rc")) c->isolation = MDB_ISO_READ_COMMITTED;
            else if (!strcmp(v, "rr")) c->isolation = MDB_ISO_REPEATABLE_READ;
            else if (!strcmp(v, "snap")) c->isolation = MDB_ISO_SNAPSHOT;
            else if (!strcmp(v, "ser")) c->isolation = MDB_ISO_SERIALISABLE;
            else { usage(); exit(2); }
        }
        else if (!strcmp(a, "--batch")) c->preload_batch = (uint32_t)parse_num(NEXT());
        else if (!strcmp(a, "--no-checkpoint")) c->checkpoint = 0;
        else if (!strcmp(a, "--max-retries")) c->max_retries = (uint32_t)parse_num(NEXT());
        else if (!strcmp(a, "--pin")) c->pin = 1;
        else if (!strcmp(a, "--count-allocs")) c->count_allocs = 1;
        else if (!strcmp(a, "--ckpt")) {
            const char *v = NEXT();
            if (!strcmp(v, "off")) c->ckpt_mode = 0;
            else if (!strcmp(v, "async")) c->ckpt_mode = 1;
            else if (!strcmp(v, "sync")) c->ckpt_mode = 2;
            else { usage(); exit(2); }
        }
        else if (!strcmp(a, "--ckpt-gap-ms")) c->ckpt_gap_ms = parse_num(NEXT());
        else if (!strcmp(a, "--fresh")) c->db_clean = 1;
        else if (!strcmp(a, "--keep-db")) c->db_clean = -1;
        else if (!strcmp(a, "--csv")) c->csv_path = NEXT();
        else if (!strcmp(a, "--label")) c->label = NEXT();
        else if (!strcmp(a, "--seed")) c->seed = (uint64_t)parse_num(NEXT());
        else if (!strcmp(a, "--lifetime")) c->lifetime = (uint64_t)parse_num(NEXT());
        else if (!strcmp(a, "-h") || !strcmp(a, "--help") || !strcmp(a, "/?")) { usage(); exit(0); }
        else { fprintf(stderr, "Unknown option %s\n\n", a); usage(); exit(2); }
    }
#undef NEXT
    if (c->records < 1000) die("--records must be at least 1000");
    if (c->preload_batch < 1) c->preload_batch = 1;
    if (c->readers < 0 || c->writers < 1) die("need at least one writer");
    if (c->micro_ops < 1) c->micro_ops = 1;
    if (c->ckpt_mode && strcmp(c->engine, "memdb2") != 0)
        die("--ckpt needs --engine memdb2");
    /* Lifetime that keeps ~records rows live when a fraction d of inserts is
       later deleted by id at random: steady state P = x d / -ln(1 - d). */
    if (c->lifetime == 0) {
        double d = c->p_delete_id;
        if (d <= 0.0)
            g_lifetime = c->records;
        else if (d < 0.95)
            g_lifetime = (uint64_t)((double)c->records * -log(1.0 - d) / d);
        else
            g_lifetime = c->records * 3;
    } else {
        g_lifetime = c->lifetime;
    }
    if (c->micro_ops > c->records / 4) {
        c->micro_ops = c->records / 4;
        fprintf(stderr, "--micro-ops reduced to %llu (records / 4)\n",
                (unsigned long long)c->micro_ops);
    }
}

int main(int argc, char **argv)
{
    char brand[64];
    SYSTEM_INFO si;
    uint64_t mem_start;

    parse_args(argc, argv);
    if (!strcmp(g_cfg.engine, "memdb2")) g_vt = &g_engine_memdb2;
    else if (!strcmp(g_cfg.engine, "baseline")) g_vt = &g_engine_baseline;
    else die("--engine must be memdb2 or baseline");

    timing_init();
    cpu_brand(brand, sizeof brand);
    GetSystemInfo(&si);

    printf("mdb2bench  engine=%s  arch=%s  %s, %u logical CPUs\n",
           g_vt->name, g_arch, brand, (unsigned)si.dwNumberOfProcessors);
    printf("TSC %.3f GHz%s\n", g_tsc_hz / 1e9,
           g_tsc_invariant ? " (invariant)" : "  WARNING: TSC not reported invariant; latencies may be unreliable");
    if (g_vt == &g_engine_memdb2)
        printf("MemDB2 C API version %d, db folder %s, sync %s, writers %s, readers %s (end with %s), isolation %s\n",
               (int)mdb_api_version(), g_cfg.db_path,
               g_cfg.sync_flush ? "flush" : "lazy",
               g_cfg.writer_exclusive ? "WRITE_EXCLUSIVE" : "READ_WRITE_SHARED",
               g_cfg.reader_rw ? "READ_WRITE_SHARED" : "READ_SHARED",
               g_cfg.reader_commit ? "commit" : "rollback",
               iso_name(g_cfg.isolation));
    printf("records %llu x ~%u bytes, %d readers, %d writers, lifetime %llu inserts\n",
           (unsigned long long)g_cfg.records, g_cfg.record_size,
           g_cfg.readers, g_cfg.writers, (unsigned long long)g_lifetime);

    csv_open();
    mem_start = process_private_bytes();
    (void)mem_start;

    if (g_vt->open(&g_cfg, &g_eng) != R_OK) {
        if (g_eng)
            g_vt->close(g_eng);
        die("could not open engine");
    }

    preload();
    if (g_cfg.run_micro && !g_failed)
        micro();
    if (g_cfg.run_mixed && !g_failed)
        mixed();

    g_vt->close(g_eng);
    if (g_csv)
        fclose(g_csv);
    if (g_failed) {
        fprintf(stderr, "\nBenchmark stopped on an error (see above).\n");
        return 1;
    }
    printf("\nDone.\n");
    return 0;
}
