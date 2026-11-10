/*
 * bench_util.c - timing, histograms and process statistics for mdb2bench.
 */
#include "bench.h"
#include <intrin.h>
#include <string.h>
#include <math.h>
#define PSAPI_VERSION 2
#include <psapi.h>

const char *const g_op_names[OP_COUNT] = {
    "1 insert",
    "2 find by id",
    "3 find earliest",
    "4 delete by id",
    "5 delete by expiry",
    "6 state transition",
    "7 recycle storage",
};

/* ---------------------------------------------------------------------------
 * Timing
 * ------------------------------------------------------------------------- */
double g_tsc_hz = 0.0;
double g_ns_per_tick = 0.0;
int    g_tsc_invariant = 0;

static uint64_t g_epoch_ticks;
static int64_t  g_epoch_base_ns;

uint64_t ticks_now(void)
{
    return __rdtsc();
}

double ticks_to_ns(uint64_t t)
{
    return (double)t * g_ns_per_tick;
}

void timing_init(void)
{
    int regs[4];
    LARGE_INTEGER f, q0, q1;
    uint64_t t0, t1;
    double best = 0.0;
    int i;

    __cpuid(regs, 0x80000000);
    if ((unsigned)regs[0] >= 0x80000007u) {
        __cpuid(regs, 0x80000007);
        g_tsc_invariant = (regs[3] >> 8) & 1;
    }

    /* Calibrate TSC against QPC: three 100 ms samples, take the median-ish
       (the last one; the first warms things up). */
    QueryPerformanceFrequency(&f);
    for (i = 0; i < 3; i++) {
        QueryPerformanceCounter(&q0);
        t0 = __rdtsc();
        do {
            QueryPerformanceCounter(&q1);
        } while ((double)(q1.QuadPart - q0.QuadPart) < 0.1 * (double)f.QuadPart);
        t1 = __rdtsc();
        best = (double)(t1 - t0) * (double)f.QuadPart / (double)(q1.QuadPart - q0.QuadPart);
    }
    g_tsc_hz = best;
    g_ns_per_tick = 1e9 / g_tsc_hz;
    clock_set_epoch(0);
}

void clock_set_epoch(int64_t base_ns)
{
    g_epoch_base_ns = base_ns;
    g_epoch_ticks = __rdtsc();
}

int64_t clock_ns(void)
{
    return g_epoch_base_ns + (int64_t)((double)(__rdtsc() - g_epoch_ticks) * g_ns_per_tick);
}

void cpu_brand(char *buf, size_t size)
{
    int regs[12];
    char tmp[49];
    const char *p;
    __cpuid(regs, 0x80000000);
    if ((unsigned)regs[0] < 0x80000004u) {
        strncpy_s(buf, size, "unknown CPU", _TRUNCATE);
        return;
    }
    __cpuid(regs + 0, 0x80000002);
    __cpuid(regs + 4, 0x80000003);
    __cpuid(regs + 8, 0x80000004);
    memcpy(tmp, regs, 48);
    tmp[48] = 0;
    p = tmp;
    while (*p == ' ')
        p++;
    strncpy_s(buf, size, p, _TRUNCATE);
}

/* ---------------------------------------------------------------------------
 * Histogram
 * ------------------------------------------------------------------------- */
static __inline int msb64(uint64_t v)
{
    unsigned long i;
#if defined(_M_X64) || defined(_M_ARM64)
    _BitScanReverse64(&i, v);
    return (int)i;
#else
    if (v >> 32) {
        _BitScanReverse(&i, (unsigned long)(v >> 32));
        return (int)i + 32;
    }
    _BitScanReverse(&i, (unsigned long)v);
    return (int)i;
#endif
}

static __inline int hist_index(uint64_t v)
{
    int e;
    if (v < 2 * HIST_SUB)
        return (int)v;
    e = msb64(v) - 6;                 /* v >> e is in [64, 127] */
    return e * HIST_SUB + (int)(v >> e);
}

static uint64_t hist_upper(int idx)
{
    int e, m;
    if (idx < 2 * HIST_SUB)
        return (uint64_t)idx;
    e = idx / HIST_SUB - 1;
    m = idx % HIST_SUB + HIST_SUB;
    return (((uint64_t)m + 1) << e) - 1;
}

void hist_reset(hist *h)
{
    memset(h, 0, sizeof *h);
    h->min = UINT64_MAX;
}

void hist_add(hist *h, uint64_t v)
{
    h->count++;
    h->sum += v;
    if (v < h->min) h->min = v;
    if (v > h->max) h->max = v;
    h->b[hist_index(v)]++;
}

void hist_merge(hist *dst, const hist *src)
{
    int i;
    if (!src->count)
        return;
    dst->count += src->count;
    dst->sum += src->sum;
    if (src->min < dst->min) dst->min = src->min;
    if (src->max > dst->max) dst->max = src->max;
    for (i = 0; i < HIST_BUCKETS; i++)
        dst->b[i] += src->b[i];
}

uint64_t hist_percentile(const hist *h, double p)
{
    uint64_t target, cum = 0, v;
    int i;
    if (!h->count)
        return 0;
    if (p >= 1.0)
        return h->max;
    target = (uint64_t)ceil(p * (double)h->count);
    if (target < 1)
        target = 1;
    for (i = 0; i < HIST_BUCKETS; i++) {
        cum += h->b[i];
        if (cum >= target) {
            v = hist_upper(i);
            if (v > h->max) v = h->max;
            if (v < h->min) v = h->min;
            return v;
        }
    }
    return h->max;
}

/* ---------------------------------------------------------------------------
 * Misc
 * ------------------------------------------------------------------------- */
void rng_seed(rng *r, uint64_t seed)
{
    /* splitmix64 so that small seeds give good states */
    uint64_t z = seed + 0x9E3779B97F4A7C15ULL;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    r->s = z ? z : 1;
}

uint64_t process_private_bytes(void)
{
    PROCESS_MEMORY_COUNTERS_EX pmc;
    memset(&pmc, 0, sizeof pmc);
    pmc.cb = sizeof pmc;
    if (!GetProcessMemoryInfo(GetCurrentProcess(), (PROCESS_MEMORY_COUNTERS *)&pmc, sizeof pmc))
        return 0;
    return (uint64_t)pmc.PrivateUsage;
}

uint64_t process_cycles(void)
{
    ULONG64 c = 0;
    QueryProcessCycleTime(GetCurrentProcess(), &c);
    return (uint64_t)c;
}

uint64_t thread_cycles(HANDLE h)
{
    ULONG64 c = 0;
    QueryThreadCycleTime(h, &c);
    return (uint64_t)c;
}
