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
| 1. Preload | 5M records (~128 bytes each: `Id` uint64, `Expiry` int64, `State` int32, `Payload` blob of 54–162 bytes), 100 per transaction from 4 threads, then a checkpoint. Reports load rate and process memory per record. |
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

## The database folder

On startup the benchmark deletes the `*.dbincr` and `*.dbinit` files in the
`--db` folder, so MemDB2 doesn't spend time replaying the previous run's
journal. A long run can leave gigabytes of it. Nothing else in the folder
is touched.

As a safeguard, files are only deleted automatically when the folder's
name contains `mdb2bench` (the default is `mdb2bench_db`). A mistyped
`--db` pointing at a real database is left alone, with a message saying so.

- `--fresh` deletes them whatever the folder is called.
- `--keep-db` never deletes them; the old journal is replayed, then the
  `Bench` table is dropped and recreated as before.

## Read-only runs and reusing a loaded database (options)

- `--read-only` runs the mixed phase with readers only: `--writers 0`,
  8 readers unless `--readers` is given, and no isolated-operations phase.
  It measures the maximum read TPS. `--writers 0` or `--readers 0` can also
  be given on their own; at least one thread is needed.
- `--load-only` preloads (and checkpoints) the table, then exits. On
  success it writes `mdb2bench_loaded.txt` in the `--db` folder, which the
  batch files check for, since a pipe loses the exit code.
- `--load-threads N` preloads with N threads (default 4), and `--batch N`
  sets the rows per preload transaction (default 100). Each thread claims
  batches of ids and loads them `READ_WRITE_SHARED`, retrying a batch after
  a concurrency clash. With `--load-threads 1` each batch is a
  `WRITE_EXCLUSIVE` transaction. The defaults come from the batch-size
  runs (`Docs/MemDB2_Batch_Size_and_Threads_2026-11-16.pdf`): inserts are
  fastest with 3-4 threads and 20-100 rows per transaction, more threads
  only wait, and batches over a few hundred rows are slightly slower per
  row. Runs before 17 November 2026 loaded with `--load-threads 8 --batch
  10000`; give those options to reproduce them exactly.
- `--reuse-db` skips the preload and uses the `Bench` table left by an
  earlier run. The journal is never deleted in this mode (it implies
  `--keep-db`, and `--fresh` is refused). Startup prints the time taken to
  open the database, which includes replaying the journal, then counts the
  rows and finds the highest id. Give the same `--records` as the load:
  readers pick ids from a span of recent ids sized from `--records`, and the run
  warns if the row count is far from `--records`.

Runs with writers can reuse a table too, but they change it, so later runs
start from different data. Load again for a clean starting point.

## Test scripts

Two scripts, both on a 1M-row table, for comparing builds before and after a
change. Each takes a tag that starts every CSV label, so runs of different
builds share one CSV:

```
read_scaling.bat before            rem read-only, 1 2 4 6 8 readers
read_scaling.bat after "1 2 4 8"   rem pick the reader counts
mixed_tests.bat before             rem single ops, then 2+2 and 4+4 threads
```

| Script | Runs | Output |
|---|---|---|
| `read_scaling.bat` | `READ_SHARED` readers only, one run per reader count; `RWS=1` adds `READ_WRITE_SHARED` | `results_read.csv`, `results_read_execution.log` |
| `mixed_tests.bat` | the seven single operations (50,000 each; `NOMICRO=1` skips), then 2 readers + 2 writers and 4 + 4, each with `READ_SHARED` readers, `READ_WRITE_SHARED` readers, and `READ_WRITE_SHARED` with back-to-back async checkpoints | `results_mixed.csv`, `results_mixed_execution.log` |

Every run loads a fresh table (8 load threads, then a checkpoint, about a
minute) into the single folder `mdb2bench_db`. Writers grow and change the
table, so reloading means each run starts from identical data, and the
journal from the previous run is deleted at startup. The load isn't measured.
Timed phases are 30 s after a 5 s warm-up (`DURATION`, `WARMUP` in the
scripts).

For a build whose DLLs were made elsewhere (for example with a newer
Delphi), and a final comparison with every earlier run:

```
rebuild_bench.bat                  rem import libraries + bench only, DLLs untouched
final_run.bat final                rem both scripts above, then compare_runs.ps1
```

| Script | Does | Output |
|---|---|---|
| `rebuild_bench.bat` | `make_importlib.bat` for the DLLs already in `..\C_DLL\Win*\*`, then `build.bat`. map2pdb only with `PDB=1`, because it writes into the DLL | `rebuild_bench.log` |
| `final_run.bat` | `read_scaling.bat` with 1-8 readers and `mixed_tests.bat` under one tag (`RUNS=2` repeats them as `<tag>b`), then `compare_runs.ps1` | the CSVs and logs above, `final_compare.txt` |
| `compare_runs.ps1` | tables of throughput, CPU cycles per transaction and change against `before_vt` for every tag in the CSVs (`-Base`, `-Tags` to choose) | console |

Batch sizes: how rows per second depend on the number of rows per
transaction and the number of threads, for finds by id and for inserts:

```
batch_tests.bat after              rem finds, then inserts
batch_tests.bat after find         rem one of them
```

`batch_tests.bat` loads 1M rows once per operation and then runs every
combination of `THREADS` (default 1 2 3 4 5 6 8) and `SIZES` (default 0 1 2
5 10 20 50 100 200 500 1000 2000), 10 s each after a 2 s warm-up. Batch size
0 is an empty transaction (begin, open the table, rollback or commit): the
fixed cost every transaction pays. Insert runs delete the rows they added
after each combination and checkpoint, so each combination starts from the
same table, and stop early after `--batch-rows` (300k) measured rows.
Results go to `results_batch.csv`, which has its own columns (`threads`,
`batch`, `txn_per_s`, `rows_per_s`, latencies per transaction, CPU cycles
per transaction). The bench options are `--batch-op find|insert`,
`--batch-sizes`, `--batch-threads` and `--batch-rows`.

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
- **Readers end with a rollback.** They change nothing, so this skips the
  commit path, which in `READ_WRITE_SHARED` mode runs the full commit
  cycle and journals an empty changeset. `--reader-end commit` restores
  the commit, to measure that cost; the CSV `reader_mode` column then
  reads `rw-commit`. Results in CSVs from before this option was added
  with `reader_mode` `rw` were taken with commits. At `ser` isolation a
  rollback also skips the commit-time read validation.
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

## Profiling with VTune

1. Build the Win64 Release DLL with **Map file = Detailed** and **Debug
   information** on (line numbers come from the map).
2. Run `vtune_prep.bat`. It runs map2pdb (looked for under
   `C:\MCH Stuff\github\map2pdb`, or set `MAP2PDB`) to write and bind
   `MemDB2Dll.pdb`, then runs `build.bat x64`. Re-run it after every DLL build.
3. `build.bat` now always writes `mdb2bench.pdb`. If it finds the VTune SDK
   (`ITT_DIR`, `VTUNE_PROFILER_DIR\sdk`, or the default oneAPI folder) it
   says "with VTune markers" and builds with `USE_ITT`.
4. In VTune: application `Bench\x64\mdb2bench.exe`, working directory
   `Bench`, and the usual arguments plus `--profile`. With markers,
   collection is paused at startup, resumed once the database is loaded or
   reopened ("--profile: collection resumed"), and paused again before the
   close ("--profile: collection paused"). Pick the tests with the other
   options. Without markers `--profile` only prints a warning.

   Use a sampling analysis (Hotspots, Threading, Microarchitecture
   Exploration). Performance Snapshot in hardware mode only counts events
   over the whole run and ignores pause/resume.

Examples (reusing the database from `plain_tests.bat`):

    --db mdb2bench_plain_run --reuse-db --records 1000000 --read-only --readers 8 --profile
    --db mdb2bench_plain_run --reuse-db --records 1000000 --micro-ops 200000 --no-mixed --profile

Hotspots (user-mode sampling) and Threading need no driver and work with
Hyper-V / Memory Integrity on. Only hardware event-based sampling and the
microarchitecture analyses need VTune's sampling driver.
