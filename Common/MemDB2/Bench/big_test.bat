@echo off
rem ==========================================================================
rem  big_test.bat - mdb2bench with a database slightly bigger than RAM.
rem
rem  Loaded with 8 threads, a record takes about 1,906 bytes of private
rem  memory. On a 16 GB machine paging started at about 8.0M records
rem  (about 15.2 GB). 8.44M records (about 16.1 GB) is about 0.9 GB, or 6%,
rem  past that point, which is where a 21M load was stopped: from there on
rem  random inserts into the GUID-keyed index made it page heavily (about
rem  140 rows/s). The mixed phase is long so that at least one checkpoint can
rem  finish while paging.
rem
rem  The table is loaded once (--load-only) into %LOADDB%. Before each run
rem  that folder is copied to %RUNDB%, and the run uses the copy with
rem  --reuse-db. Every run therefore starts from the same checkpointed data,
rem  and runs with writers don't change the data the next run sees.
rem
rem  Three runs: READ_SHARED readers, READ_WRITE_SHARED readers, and
rem  READ_WRITE_SHARED readers with back-to-back async checkpoints in the
rem  mixed phase.
rem
rem  Set NOLOAD=1 first to skip the load and reuse the last one (same
rem  RECORDS).
rem
rem  Results are appended to results_big.csv. Console output is also appended
rem  to results_big_execution.log.
rem ==========================================================================
setlocal
cd /d "%~dp0"

set "RECORDS=8440000"
set "DURATION=1800"
set "WARMUP=60"
set "LOADDB=mdb2bench_big_load"
set "RUNDB=mdb2bench_big_run"
set "LOAD_THREADS=8"
set "CSV=results_big.csv"
set "LOG=results_big_execution.log"
set "COMMON=--records %RECORDS% --csv %CSV%"
set "REUSE=%COMMON% --db %RUNDB% --reuse-db --duration %DURATION% --warmup %WARMUP%"

if not "%NOLOAD%"=="1" (
    call :run %COMMON% --db %LOADDB% --load-only --load-threads %LOAD_THREADS% --label big-load
    rem The exit code is lost in the pipe: the marker file shows success.
    if not exist "%LOADDB%\mdb2bench_loaded.txt" goto :failed
)

call :copy && call :run %REUSE% --reader-mode read --label big-read
call :copy && call :run %REUSE% --reader-mode rw --label big-rws
call :copy && call :run %REUSE% --reader-mode rw --ckpt async --label big-rws-ckpt
exit /b 0

:failed
echo The load failed: see %LOG%. The test runs were not started.
exit /b 1

rem --------------------------------------------------------------------------
rem  Replace RUNDB with a fresh copy of the loaded database.
:copy
echo Copying %LOADDB% to %RUNDB% ...
>>"%LOG%" echo.
>>"%LOG%" echo ==== %DATE% %TIME% copying %LOADDB% to %RUNDB%
robocopy "%LOADDB%" "%RUNDB%" /MIR /NFL /NDL /NJH /NJS /NP >nul
if errorlevel 8 (
    echo Copy failed. Skipping this run.
    >>"%LOG%" echo Copy failed. Skipping this run.
    exit /b 1
)
exit /b 0

rem --------------------------------------------------------------------------
rem  Run one benchmark, showing output on the console and appending it to LOG.
:run
>>"%LOG%" echo.
>>"%LOG%" echo ==== %DATE% %TIME%
>>"%LOG%" echo %CD%^>x64\mdb2bench %*
echo %CD%^>x64\mdb2bench %*
x64\mdb2bench %* 2>&1 | powershell -NoProfile -Command "$input | ForEach-Object { $_; Add-Content -LiteralPath $env:LOG -Value $_ }"
goto :eof
