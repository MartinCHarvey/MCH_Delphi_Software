# mdb2bench: MemDB2 C_DLL latency benchmark

A C program, built with MSVC, that measures MemDB2 through its C API
(`..\C_API\memdb2.h`) against the Release `MemDB2Dll.dll`, x64 or x86. It also
includes a reference engine that runs the same workload on plain C data
structures, so you can measure MemDB2's overhead directly.

## Build

Include MemDB2BenchHooks into the DLL project file, and rebuild all vers,
then ...

```
..\C_DLL\make_importlib.bat     (once, after building the Release DLLs)
build.bat                       x64 and x86
build.bat x64                   just one
```

Output: `x64\mdb2bench.exe` and `x86\mdb2bench.exe`, each with its matching
`MemDB2Dll.dll` copied alongside. The DLL is delay-loaded, so
`--engine baseline` runs even without it.

## What it does

| Phase | What |
|---|---|
| 1. Preload | 5M records (~128 bytes each: `Id` uint64, `Expiry` int64, `State` int32, `Payload` blob of 54–162 bytes), 10,000 per transaction, then a checkpoint. Reports load rate and process memory per record. |
| 2. Micro | Each of the seven operations alone on one thread (200k transactions each), so latency, CPU cycles and allocations can be attributed to one kind of transaction. |
| 3. Mixed | 4 writers and 4 readers for 30 s after a 5 s warm-up, with an expiry sweep every 500 µs. Writers and readers can optionally be rate-paced. |

Indexes: `ById` (unique) and `ByExpiry` (non-unique).

### The seven operations

Each one is a complete transaction: begin, open table, work, post, commit.

| # | Operation | MemDB2 calls |
|---|---|---|
| 1 | Insert, both indexes updated | append, 4 writes, post |
| 2 | Find by ID | find on `ById`, read all 4 fields |
| 3 | Find earliest expiry | locate first on `ByExpiry`, read Id and Expiry |
| 4 | Delete by ID | find on `ById`, delete |
| 5 | Delete by expiry | locate first on `ByExpiry`; delete if expired, else roll back (a "miss") |
| 6 | Atomic state transition | find, read `State`, write `(State+1) mod 3`, post (NEW→RESERVED→FILLED→NEW). Clashes are retried. |
| 7 | Reuse deleted storage | locate the earliest expired row and overwrite it in place with a new record, instead of delete plus insert |

Compare #7 with #5 + #1 in the micro phase, in both latency and allocations,
to see what reusing a row saves.

### Metrics

For every operation:

- transaction count and rate
- mean, p50, p99, p99.9, p99.99 and max latency (µs)
- CPU cycles per transaction: the calling thread's cycles (`QueryThreadCycleTime`) and the whole process's cycles (`QueryProcessCycleTime`), which includes MemDB2's journal thread
- allocations per transaction (needs the optional hook below)
- misses, concurrency clashes and give-ups

Latencies are taken from the CPU's time-stamp counter, calibrated against
QPC at startup, so sub-microsecond operations resolve properly. The histogram
is log-linear (under 1.6% bucket error); the max is exact.

When paced (`--rate`, `--read-rate`), it also reports response time from each
iteration's intended start. This captures queueing when the engine falls
behind (no coordinated omission).

### Expiry is in "logical time"

A record's expiry is counted in inserts, not seconds: it expires a fixed
number of inserts after its own insert. That keeps the live population at
about `--records` whatever throughput the engine actually reaches. The
default lifetime allows for the share of inserts that are later cancelled
(deleted by ID): with 5M records and `--p-delete-id 0.5` it is about 6.9M
inserts. When paced at 1M inserts/s, that is 6.9 s of wall-clock life.

The 500 µs sweep interval is real wall-clock time. Each tick, one writer
claims the sweep and deletes everything that has expired, up to
`--sweep-cap` rows. Each deletion is its own measured transaction. The last
probe of a sweep finds nothing expired and counts as a miss.

## Suggested runs for the paper

```
rem journalling: lazy write vs flush-per-commit
mdb2bench --sync lazy  --csv results.csv --label lazy
mdb2bench --sync flush --csv results.csv --label flush

rem MemDB2 overhead vs the bare data structures
mdb2bench --engine baseline --csv results.csv --label baseline

rem concurrency mode: shared writers vs exclusive writers
mdb2bench --writer-mode exclusive --csv results.csv --label wexcl

rem the specified target rates (open loop)
mdb2bench --rate 1M --read-rate 1M --csv results.csv --label paced-1M

rem 32 vs 64 bit
x86\mdb2bench --records 2M --csv results.csv --label x86-2M
x64\mdb2bench --records 2M --csv results.csv --label x64-2M
```

Every run appends one CSV row per operation and phase, so `results.csv`
collects a comparison table across runs. For stable numbers: use the High
performance power plan, close other programs, and consider `--pin`.

## Checkpoints during the workload (`--ckpt`)

`--ckpt async` starts a thread in the mixed phase that calls
`mdb_db_checkpoint_ex(db, MDB_CHECKPOINT_ASYNC)` repeatedly. As soon as one
checkpoint returns, the next one starts, until the run stops. `--ckpt sync`
does the same with synchronous checkpoints, for comparison.

Each transaction is tagged by whether it overlapped a checkpoint: one was
running when it started, or one started or finished while it ran. The two
groups are reported in separate tables, and the CSV gets `mixed` and
`mixed-ckpt-async` (or `-sync`) rows. A `checkpoint` row records checkpoint
durations, and the console also shows how much of the measured time a
checkpoint was running.

With checkpoints back to back, nearly every transaction overlaps one, so the
clean comparison is two runs:

```
mdb2bench --reader-mode rw --no-micro --csv results.csv --label rws
mdb2bench --reader-mode rw --no-micro --ckpt async --csv results.csv --label rws-ckpt
```

To get both tables from a single run, add `--ckpt-gap-ms N` so the workload
has periods with no checkpoint running.

During an asynchronous checkpoint, only `READ_WRITE_SHARED` transactions
can run; `READ_SHARED` and `WRITE_EXCLUSIVE` ones wait. Use
`--reader-mode rw` (writers are shared by default) unless that waiting is
what you want to measure. If a checkpoint is still running when the run
ends, the benchmark waits for it to finish before closing the database.

## Allocations per transaction (optional DLL hook)

The DLL uses its own Delphi memory manager, so the C side can't see its
allocations. `MemDB2BenchHooks.pas` chains onto that memory manager and
counts calls. It exports `mdb_bench_alloc_enable` and
`mdb_bench_alloc_stats`, which mdb2bench finds with `GetProcAddress`. The
public header and `.def` file are untouched.

1. Add `MemDB2BenchHooks in '..\Bench\MemDB2BenchHooks.pas',` to the uses
   clause in `C_DLL\MemDB2Dll.dpr`.
2. Rebuild the DLL.

Allocations are then counted in the micro phase. In the mixed phase, add
`--count-allocs`: the shared counter adds contention, so take latency
figures from a run without it.

## Things to know when reading the results

- **Journalling can't be switched off through the C API.** `InitDB` is
  always called with `jtV2`. `--sync lazy` versus `--sync flush` is the
  nearest comparison. A true "journalling off" run needs a DLL change, such
  as a null journal type exposed as an `MDB_OPEN_*` flag. Putting the
  database folder on a RAM disk (`--db R:\bench`) is a cheaper
  approximation.
- **Readers and writers alternate in batches.** The RWW lock treats shared
  read and shared write as different lock states, so readers and writers
  don't run at the same time. Expect this to show up in the p99.9 and
  p99.99 tails of the mixed phase. `--reader-mode rw` puts readers in the
  writers' mode for comparison.
- **Every C API call takes one global lock.** The handle table in
  `MemDB2CApi.pas` is guarded by a single critical section, and a
  transaction here makes about 6 to 10 API calls. Under 8 threads this
  lock may be a measurable part of the overhead compared with using
  `TMemDB` directly from Delphi.
- **The baseline is a lower bound, not a competitor.** It is AVL trees, one
  malloc per record and one SRW lock: no isolation, no durability, no
  versioning. The gap between it and MemDB2 is the cost of being a
  database.
- **Misses are mixed into the latency figures.** Readers pick a random ID
  from the live window, and some of those rows have already been deleted.
  Hits and misses share one histogram; the miss count is shown alongside.
- **Mind memory on x86.** 5M records may not fit in a 32-bit process even
  with `/LARGEADDRESSAWARE` (which the build sets). Use `--records 2M` for
  32-bit versus 64-bit comparisons.

## Files

| File | Contents |
|---|---|
| `bench_main.c` | phases, workload, reporting, command line |
| `bench_util.c` | TSC timing, histograms, process statistics |
| `engine_memdb2.c` | the MemDB2 engine (C API) |
| `engine_baseline.c` | the reference engine (AVL + SRW lock) |
| `bench.h` | shared declarations |
| `MemDB2BenchHooks.pas` | optional allocation counter for the DLL |
| `build.bat` | MSVC build, x64 / x86 |

Run `mdb2bench --help` for every option.
