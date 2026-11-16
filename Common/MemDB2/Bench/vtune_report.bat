@echo off
rem ==========================================================================
rem  vtune_report.bat - export a VTune result to CSV files that can be read
rem  without the VTune GUI.
rem
rem    vtune_report.bat <result folder> [name]
rem      e.g. vtune_report.bat "%USERPROFILE%\Documents\VTune\Projects\MemDB_PDB_micro_ops\r002hs" c4_rws
rem
rem  Writes to vtune_reports\<name>\ (name defaults to <project>_<result>):
rem    summary.txt    the summary page
rem    hotspots.csv   functions with self CPU time
rem    topdown.csv    the call tree with total and self CPU time
rem  Needs a result collected with call stacks for topdown.csv to be useful.
rem ==========================================================================
setlocal
if "%~1"=="" goto :usage
set "RES=%~f1"
if not exist "%RES%\*.vtune" (
    echo No VTune result in "%RES%".
    exit /b 1
)
set "VT=%ProgramFiles(x86)%\Intel\oneAPI\vtune\latest\bin64\vtune.exe"
if defined VTUNE_PROFILER_DIR set "VT=%VTUNE_PROFILER_DIR%\bin64\vtune.exe"
if not exist "%VT%" goto :novtune

set "NAME=%~2"
if defined NAME goto :have_name
for %%P in ("%RES%\..") do set "PROJ=%%~nxP"
for %%R in ("%RES%") do set "NAME=%PROJ%_%%~nxR"
:have_name
set "OUT=%~dp0vtune_reports\%NAME%"
if not exist "%OUT%" mkdir "%OUT%"

echo Writing %OUT% ...
"%VT%" -report summary -r "%RES%" -report-output "%OUT%\summary.txt"
"%VT%" -report hotspots -r "%RES%" -format csv -csv-delimiter comma -report-output "%OUT%\hotspots.csv"
"%VT%" -report top-down -r "%RES%" -format csv -csv-delimiter comma -report-output "%OUT%\topdown.csv"
echo Done.
exit /b 0

:novtune
echo vtune.exe not found. Set VTUNE_PROFILER_DIR to the VTune folder.
exit /b 1

:usage
echo Usage: vtune_report.bat ^<result folder^> [name]
exit /b 2
