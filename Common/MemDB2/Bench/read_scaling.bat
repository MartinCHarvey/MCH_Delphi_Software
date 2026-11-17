@echo off
rem ==========================================================================
rem  read_scaling.bat - read-only throughput against the number of readers.
rem
rem    read_scaling.bat <tag> ["reader counts"]
rem      e.g.  read_scaling.bat before
rem            read_scaling.bat after "1 2 3 4 5 6 7 8"
rem
rem  For each reader count (default 1 2 4 6 8), one run of READ_SHARED readers
rem  only, no writers: 30 s measured after a 5 s warm-up. Set RWS=1 first to
rem  repeat the counts with READ_WRITE_SHARED readers.
rem
rem  Every run loads a fresh 1M-row table (8 load threads, then a checkpoint)
rem  into the one folder mdb2bench_db, so each run starts from identical data
rem  and nothing carries over from the run before. The load isn't measured.
rem
rem  The tag starts every CSV label (<tag>-read-4, <tag>-rws-4), so before and
rem  after runs can share the CSV. Results: results_read.csv. Console output
rem  is also appended to results_read_execution.log.
rem
rem  With 8 logical CPUs, 8 readers share the machine with the journal thread
rem  and everything else running.
rem ==========================================================================
setlocal
cd /d "%~dp0"
if "%~1"=="" goto :usage
set "TAG=%~1"
set "COUNTS=1 2 4 6 8"
if not "%~2"=="" set "COUNTS=%~2"
set "DURATION=30"
set "WARMUP=5"
set "CSV=results_read.csv"
set "LOG=results_read_execution.log"
set "BASE=--db mdb2bench_db --fresh --records 1M --csv %CSV% --read-only --duration %DURATION% --warmup %WARMUP%"
if not exist x64\mdb2bench.exe (
    echo x64\mdb2bench.exe not found: run rebuild_all.bat first.
    exit /b 1
)

for %%N in (%COUNTS%) do call :run %BASE% --reader-mode read --readers %%N --label %TAG%-read-%%N
if "%RWS%"=="1" for %%N in (%COUNTS%) do call :run %BASE% --reader-mode rw --readers %%N --label %TAG%-rws-%%N
exit /b 0

:usage
echo Usage: read_scaling.bat tag ["reader counts"]
echo   e.g. read_scaling.bat before
echo        read_scaling.bat after "1 2 3 4 5 6 7 8"
exit /b 2

rem --------------------------------------------------------------------------
rem  Run one benchmark, showing output on the console and appending it to LOG.
:run
>>"%LOG%" echo.
>>"%LOG%" echo ==== %DATE% %TIME%
>>"%LOG%" echo %CD%^>x64\mdb2bench %*
echo.
echo %CD%^>x64\mdb2bench %*
x64\mdb2bench %* 2>&1 | powershell -NoProfile -Command "$input | ForEach-Object { $_; Add-Content -LiteralPath $env:LOG -Value $_ }"
goto :eof
