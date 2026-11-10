/*
 * bench.h - shared declarations for mdb2bench, the MemDB2 C_DLL latency
 * benchmark. See README.md.
 */
#ifndef BENCH_H
#define BENCH_H

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>

/* ---------------------------------------------------------------------------
 * Operations measured (numbering follows the benchmark specification).
 * ------------------------------------------------------------------------- */
typedef enum op_kind {
    OP_INSERT = 0,        /* 1. insert a record, update both indexes        */
    OP_FIND_ID,           /* 2. find by ID                                   */
    OP_FIND_EARLIEST,     /* 3. find earliest expiry                         */
    OP_DELETE_ID,         /* 4. delete by ID                                 */
    OP_DELETE_EXPIRY,     /* 5. delete by expiry (earliest, if expired)      */
    OP_TRANSITION,        /* 6. atomic state transition (read-modify-write)  */
    OP_RECYCLE,           /* 7. reuse an expired record's storage in place   */
    OP_COUNT
} op_kind;

extern const char *const g_op_names[OP_COUNT];

/* Engine result codes. */
#define R_OK        0
#define R_MISS      1   /* no such row / nothing expired: still a valid txn */
#define R_CONFLICT  2   /* concurrency clash: harness rolls back and retries */
#define R_FAIL     -1   /* anything else: benchmark stops                   */

/* Record states for the state machine used by OP_TRANSITION. */
#define ST_NEW       0
#define ST_RESERVED  1
#define ST_FILLED    2
#define ST_STATES    3

#define MAX_PAYLOAD  4096

/* One logical record. Payload points at caller memory. */
typedef struct rec {
    uint64_t id;
    int64_t  expiry;      /* ns on the benchmark clock */
    int32_t  state;
    uint32_t payload_len;
    const uint8_t *payload;
} rec;

/* What a read returns (so the read is not optimised away). */
typedef struct rec_out {
    uint64_t id;
    int64_t  expiry;
    int32_t  state;
    uint32_t payload_len;
    uint8_t  payload[MAX_PAYLOAD];
} rec_out;

/* ---------------------------------------------------------------------------
 * Configuration.
 * ------------------------------------------------------------------------- */
typedef struct bench_cfg {
    const char *engine;        /* "memdb2" or "baseline"                     */
    const char *db_path;
    uint64_t records;          /* initial records                            */
    uint32_t record_size;      /* average logical record size, bytes         */
    int readers, writers;
    double duration_s, warmup_s;
    uint64_t micro_ops;        /* ops per operation in the isolated phase    */
    int run_micro, run_mixed;
    double insert_rate;        /* total inserts/s target, 0 = unpaced        */
    double read_rate;          /* total reads/s target, 0 = unpaced          */
    double sweep_us;           /* expiry sweep interval                      */
    uint32_t sweep_cap;        /* max deletions per sweep                    */
    double p_delete_id, p_transition, p_recycle;
    uint32_t earliest_every;   /* reader: 1 in N reads is find-earliest      */
    int sync_flush;            /* 0 lazy write, 1 flush buffers per commit   */
    int writer_exclusive;      /* writers use WRITE_EXCLUSIVE                */
    int reader_rw;             /* readers use READ_WRITE_SHARED              */
    int isolation;             /* MDB_ISO_*                                  */
    uint32_t preload_batch;
    int checkpoint;
    uint32_t max_retries;
    int pin;
    int count_allocs;          /* count allocations in the mixed phase too   */
    int ckpt_mode;             /* mixed phase: 0 none, 1 async, 2 sync loop  */
    double ckpt_gap_ms;        /* pause between checkpoints                  */
    const char *csv_path;
    const char *label;
    uint64_t seed;
    uint64_t lifetime;         /* 0 = derive from records and p_delete_id    */
} bench_cfg;

/* ---------------------------------------------------------------------------
 * Engine interface. One engine object, one thread-state object per thread.
 * A thread state is used by one thread only.
 * ------------------------------------------------------------------------- */
typedef struct engine_vt {
    const char *name;
    int  (*open)(const bench_cfg *cfg, void **out_eng);
    void (*close)(void *eng);
    int  (*thread_init)(void *eng, void **out_ts);
    void (*thread_done)(void *ts);
    int  (*preload_batch)(void *ts, const rec *recs, uint32_t n);
    int  (*after_preload)(void *eng);          /* e.g. checkpoint */
    int  (*insert)(void *ts, const rec *r);
    int  (*find_id)(void *ts, uint64_t id, rec_out *out);
    int  (*find_earliest)(void *ts, rec_out *out);
    int  (*delete_id)(void *ts, uint64_t id);
    int  (*delete_expired)(void *ts, int64_t now);
    int  (*transition)(void *ts, uint64_t id, int32_t *out_new_state);
    int  (*recycle)(void *ts, int64_t now, const rec *r);
    /* Allocation counters, if the engine can provide them. Returns 1 if
       available. */
    int  (*alloc_stats)(void *eng, uint64_t *allocs, uint64_t *frees);
    void (*alloc_enable)(void *eng, int on);
    /* Write a checkpoint (async != 0: MDB_CHECKPOINT_ASYNC). Called from a
       thread of its own while the workload runs. */
    int  (*checkpoint)(void *eng, int async);
} engine_vt;

extern const engine_vt g_engine_memdb2;
extern const engine_vt g_engine_baseline;

/* ---------------------------------------------------------------------------
 * Timing (bench_util.c). Time stamps are raw TSC ticks.
 * ------------------------------------------------------------------------- */
extern double g_tsc_hz;
extern double g_ns_per_tick;
extern int    g_tsc_invariant;

void     timing_init(void);
uint64_t ticks_now(void);
double   ticks_to_ns(uint64_t t);
void     cpu_brand(char *buf, size_t size);

/* Benchmark clock in ns, used for expiry values. */
void     clock_set_epoch(int64_t base_ns);
int64_t  clock_ns(void);

/* ---------------------------------------------------------------------------
 * Latency histogram: log-linear over TSC ticks, 64 sub-buckets per power of
 * two (< 1.6% relative error), exact min / max / sum.
 * ------------------------------------------------------------------------- */
#define HIST_SUB      64
#define HIST_BUCKETS  (64 * HIST_SUB)

typedef struct hist {
    uint64_t count;
    uint64_t sum;
    uint64_t min, max;
    uint64_t b[HIST_BUCKETS];
} hist;

void     hist_reset(hist *h);
void     hist_add(hist *h, uint64_t ticks);
void     hist_merge(hist *dst, const hist *src);
uint64_t hist_percentile(const hist *h, double p);   /* ticks */

/* ---------------------------------------------------------------------------
 * Misc.
 * ------------------------------------------------------------------------- */
typedef struct rng { uint64_t s; } rng;

static __inline uint64_t rng_next(rng *r)
{
    /* xorshift64* */
    uint64_t x = r->s;
    x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
    r->s = x;
    return x * 0x2545F4914F6CDD1DULL;
}
static __inline double rng_unit(rng *r)
{
    return (double)(rng_next(r) >> 11) * (1.0 / 9007199254740992.0);
}
static __inline uint64_t rng_below(rng *r, uint64_t n)
{
    return n ? rng_next(r) % n : 0;
}
void rng_seed(rng *r, uint64_t seed);

uint64_t process_private_bytes(void);
uint64_t process_cycles(void);
uint64_t thread_cycles(HANDLE h);

#endif /* BENCH_H */
