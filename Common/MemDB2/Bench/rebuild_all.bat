@echo off
rem ==========================================================================
rem  rebuild_all.bat - rebuild the MemDB2 C DLL (Debug and Release, Win32 and
rem  Win64), make its MSVC import libraries and (with map2pdb) the Win64
rem  Release PDB for VTune, then build mdb2bench for x64 and x86. mdb2bench
rem  always builds against the Release DLLs.
rem  Output goes to the console and to rebuild_all.log.
rem ==========================================================================
setlocal
cd /d "%~dp0"
set "LOG=%~dp0rebuild_all.log"
if exist "%LOG%" del "%LOG%"
call :main > "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"
type "%LOG%"
if "%RC%"=="0" (echo ALL OK) else (echo FAILED, see %LOG%)
>>"%LOG%" echo EXIT %RC%
rem Set NOPAUSE=1 when calling from another script.
if not defined NOPAUSE pause
exit /b %RC%

:main
echo ==== %DATE% %TIME%
set "RSVARS="
for %%P in ("%ProgramFiles(x86)%" "%ProgramFiles%") do (
  for /d %%D in ("%%~P\Embarcadero\Studio\*" "%%~P\Embarcadero\RAD Studio\*") do (
    if exist "%%~D\bin\rsvars.bat" set "RSVARS=%%~D\bin\rsvars.bat"
  )
)
if not defined RSVARS (echo Cannot find rsvars.bat & exit /b 1)
echo Using %RSVARS%
call "%RSVARS%"

pushd "%~dp0..\C_DLL"
for %%C in (Debug Release) do (
  for %%A in (Win32 Win64) do (
    echo.
    echo ==== Rebuilding MemDB2Dll %%C %%A
    msbuild MemDB2Dll.dproj /nologo /v:minimal /t:Rebuild /p:Config=%%C /p:Platform=%%A
    if errorlevel 1 (popd & echo DLL BUILD FAILED: %%C %%A & exit /b 1)
  )
)
echo.
echo ==== make_importlib
call make_importlib.bat
if errorlevel 1 (popd & echo IMPORTLIB FAILED & exit /b 1)
popd

rem VTune symbols for the Win64 Release DLL. Needs map2pdb; skipped if it
rem is not found (set MAP2PDB or MAP2PDB_DIR as for vtune_prep.bat).
echo.
echo ==== map2pdb (Win64 Release)
if not defined MAP2PDB_DIR set "MAP2PDB_DIR=C:\MCH Stuff\github\map2pdb"
if defined MAP2PDB goto :have_map2pdb
for /r "%MAP2PDB_DIR%" %%f in (map2pdb.exe) do if exist "%%f" set "MAP2PDB=%%f"
:have_map2pdb
set "W64R=%~dp0..\C_DLL\Win64\Release"
if exist "%W64R%\MemDB2Dll.pdb" del "%W64R%\MemDB2Dll.pdb"
if not defined MAP2PDB goto :no_map2pdb
if not exist "%W64R%\MemDB2Dll.map" goto :no_map
"%MAP2PDB%" -bind:"%W64R%\MemDB2Dll.dll" "%W64R%\MemDB2Dll.map"
if errorlevel 1 (echo MAP2PDB FAILED & exit /b 1)
goto :map2pdb_done
:no_map2pdb
echo map2pdb.exe not found: skipped, no VTune symbols for the DLL.
goto :map2pdb_done
:no_map
echo No MemDB2Dll.map: skipped, no VTune symbols for the DLL.
:map2pdb_done

echo.
echo ==== Bench build (against the Release DLLs)
call "%~dp0build.bat"
if errorlevel 1 (echo BENCH BUILD FAILED & exit /b 1)
echo.
dir /tw "%~dp0x64\*.dll" "%~dp0x64\*.exe" "%~dp0x86\*.dll" "%~dp0x86\*.exe" | findstr /i "dll exe"
exit /b 0
