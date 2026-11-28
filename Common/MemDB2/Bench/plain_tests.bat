@echo off
rem ==========================================================================
rem  plain_tests.bat - the standard 1M record runs of mdb2bench.
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
rem  Results are appended to results.csv. Console output is also appended
rem  to results_execution.log.
rem ==========================================================================
setlocal
cd /d "%~dp0"

set "RECORDS=1M"
set "LOADDB=mdb2bench_plain_load"
set "RUNDB=mdb2bench_plain_run"
set "LOAD_THREADS=8"
set "CSV=results.csv"
set "LOG=results_execution.log"
set "COMMON=--records %RECORDS% --csv %CSV%"
set "REUSE=%COMMON% --db %RUNDB% --reuse-db"

if not "%NOLOAD%"=="1" (
    call :run %COMMON% --db %LOADDB% --load-only --load-threads %LOAD_THREADS% --label load
    rem The exit code is lost in the pipe: the marker file shows success.
    if not exist "%LOADDB%\mdb2bench_loaded.txt" goto :failed
)

call :copy && call :run %REUSE% --reader-mode read --label read
call :copy && call :run %REUSE% --reader-mode rw --label rws
call :copy && call :run %REUSE% --reader-mode rw --ckpt async --label rws-ckpt
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
