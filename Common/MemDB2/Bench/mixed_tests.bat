@echo off
rem ==========================================================================
rem  mixed_tests.bat - general performance: single operations, and readers
rem  and writers together on 4 and 8 cores.
rem
rem    mixed_tests.bat <tag>          e.g.  mixed_tests.bat before
rem
rem  Runs, each on a fresh 1M-row table:
rem    <tag>-micro           each of the seven operations alone on one thread,
rem                          50,000 transactions each (set NOMICRO=1 to skip)
rem    <tag>-c4-...          2 readers + 2 writers
rem    <tag>-c8-...          4 readers + 4 writers
rem  and for each of c4 and c8:
rem    read        READ_SHARED readers
rem    rws         READ_WRITE_SHARED readers
rem    rws-ckpt    READ_WRITE_SHARED readers, async checkpoints back to back
rem  Mixed runs are 30 s measured after a 5 s warm-up.
rem
rem  Every run loads a fresh 1M-row table (8 load threads, then a checkpoint)
rem  into the one folder mdb2bench_db. Writers grow and change the table, so
rem  reloading means each run starts from identical data. The load isn't
rem  measured.
rem
rem  The tag starts every CSV label, so before and after runs can share the
rem  CSV. Results: results_mixed.csv. Console output is also appended to
rem  results_mixed_execution.log.
rem ==========================================================================
setlocal
cd /d "%~dp0"
if "%~1"=="" goto :usage
set "TAG=%~1"
set "DURATION=30"
set "WARMUP=5"
set "CSV=results_mixed.csv"
set "LOG=results_mixed_execution.log"
set "LOAD=--db mdb2bench_db --fresh --records 1M --load-threads 8 --csv %CSV%"
set "MIX=%LOAD% --no-micro --duration %DURATION% --warmup %WARMUP%"
if not exist x64\mdb2bench.exe (
    echo x64\mdb2bench.exe not found: run rebuild_all.bat first.
    exit /b 1
)

if not "%NOMICRO%"=="1" call :run %LOAD% --no-mixed --micro-ops 50000 --label %TAG%-micro

for %%C in (4 8) do call :cores %%C
exit /b 0

:cores
set /a "HALF=%1 / 2"
set "T=--readers %HALF% --writers %HALF%"
call :run %MIX% %T% --reader-mode read --label %TAG%-c%1-read
call :run %MIX% %T% --reader-mode rw --label %TAG%-c%1-rws
call :run %MIX% %T% --reader-mode rw --ckpt async --label %TAG%-c%1-rws-ckpt
goto :eof

:usage
echo Usage: mixed_tests.bat tag
echo   e.g. mixed_tests.bat before
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
