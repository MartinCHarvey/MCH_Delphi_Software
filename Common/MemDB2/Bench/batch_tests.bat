@echo off
rem ==========================================================================
rem  batch_tests.bat - rows per second against batch size and thread count.
rem
rem    batch_tests.bat <tag> [find|insert|both]      default both
rem
rem  Each operation loads a fresh 1M-row table once, then runs every
rem  combination of thread count and batch size on it: T threads, each
rem  running transactions of N finds by id (READ_SHARED) or N inserts
rem  (READ_WRITE_SHARED), back to back. Batch size 0 is an empty
rem  transaction: the fixed cost per transaction.
rem  Insert runs delete the rows they added (and checkpoint) after each
rem  combination, so every combination starts from the same table.
rem
rem  Set THREADS, SIZES, DURATION or WARMUP first to change the defaults:
rem    THREADS  "1 2 3 4 5 6 8"
rem    SIZES    "0 1 2 5 10 20 50 100 200 500 1000 2000"
rem    DURATION 10 s and WARMUP 2 s per combination
rem  With the defaults, finds take about 20 minutes and inserts about 35.
rem
rem  Labels <tag>-batch-find and <tag>-batch-insert; results in
rem  results_batch.csv, console output also in results_batch_execution.log.
rem ==========================================================================
setlocal
cd /d "%~dp0"
if "%~1"=="" goto :usage
set "TAG=%~1"
set "WHICH=%~2"
if "%WHICH%"=="" set "WHICH=both"
if not defined THREADS set "THREADS=1 2 3 4 5 6 8"
if not defined SIZES set "SIZES=0 1 2 5 10 20 50 100 200 500 1000 2000"
if not defined DURATION set "DURATION=10"
if not defined WARMUP set "WARMUP=2"
set "CSV=results_batch.csv"
set "LOG=results_batch_execution.log"
set "LOAD=--db mdb2bench_db --fresh --records 1M --csv %CSV%"
set "B=--duration %DURATION% --warmup %WARMUP% --batch-threads "%THREADS%" --batch-sizes "%SIZES%""
if not exist x64\mdb2bench.exe (
    echo x64\mdb2bench.exe not found: run rebuild_bench.bat or rebuild_all.bat first.
    exit /b 1
)
if /i "%WHICH%"=="find"   goto :find
if /i "%WHICH%"=="insert" goto :insert
if /i not "%WHICH%"=="both" goto :usage
:find
call :run %LOAD% %B% --batch-op find --label %TAG%-batch-find
if /i "%WHICH%"=="find" exit /b 0
:insert
call :run %LOAD% %B% --batch-op insert --label %TAG%-batch-insert
exit /b 0

:usage
echo Usage: batch_tests.bat tag [find^|insert^|both]
exit /b 2

rem --------------------------------------------------------------------------
:run
>>"%LOG%" echo.
>>"%LOG%" echo ==== %DATE% %TIME%
>>"%LOG%" echo %CD%^>x64\mdb2bench %*
echo.
echo %CD%^>x64\mdb2bench %*
x64\mdb2bench %* 2>&1 | powershell -NoProfile -Command "$input | ForEach-Object { $_; Add-Content -LiteralPath $env:LOG -Value $_ }"
goto :eof
