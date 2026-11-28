@echo off
rem ==========================================================================
rem  read_only_tests.bat - maximum read throughput, no writers.
rem
rem  Loads the table once (--load-only) into its own folder, then runs the
rem  mixed phase with readers only against that same data (--reuse-db), so
rem  each run starts from the identical, checkpointed database without
rem  repeating the preload:
rem
rem    1. 8 READ_SHARED readers           (the headline read-only TPS)
rem    2. 1, 2 and 4 READ_SHARED readers  (how reads scale with threads)
rem    3. 8 READ_WRITE_SHARED readers     (cost of the read/write txn type)
rem
rem  Usage:  read_only_tests.bat [records]      default 1M
rem          set NOLOAD=1 first to skip the load and reuse the last one
rem
rem  Results are appended to results_readonly.csv. Console output is also
rem  appended to results_readonly_execution.log.
rem ==========================================================================
setlocal
cd /d "%~dp0"

set "RECORDS=1M"
if not "%~1"=="" set "RECORDS=%~1"
set "DB=mdb2bench_ro_db"
set "DURATION=60"
set "WARMUP=10"
set "LOAD_THREADS=8"
set "CSV=results_readonly.csv"
set "LOG=results_readonly_execution.log"
set "COMMON=--db %DB% --records %RECORDS% --csv %CSV%"
set "RO=%COMMON% --reuse-db --read-only --duration %DURATION% --warmup %WARMUP%"

if not "%NOLOAD%"=="1" (
    call :run %COMMON% --load-only --load-threads %LOAD_THREADS% --label ro-load
    rem The exit code is lost in the pipe: the marker file shows success.
    if not exist "%DB%\mdb2bench_loaded.txt" goto :failed
)

call :run %RO% --reader-mode read --label ro-read-8
call :run %RO% --reader-mode read --readers 1 --label ro-read-1
call :run %RO% --reader-mode read --readers 2 --label ro-read-2
call :run %RO% --reader-mode read --readers 4 --label ro-read-4
call :run %RO% --reader-mode rw --label ro-rws-8
exit /b 0

:failed
echo The load failed: see %LOG%. The read-only runs were not started.
exit /b 1

rem --------------------------------------------------------------------------
rem  Run one benchmark, showing output on the console and appending it to LOG.
:run
>>"%LOG%" echo.
>>"%LOG%" echo ==== %DATE% %TIME%
>>"%LOG%" echo %CD%^>x64\mdb2bench %*
echo %CD%^>x64\mdb2bench %*
x64\mdb2bench %* 2>&1 | powershell -NoProfile -Command "$input | ForEach-Object { $_; Add-Content -LiteralPath $env:LOG -Value $_ }"
goto :eof
