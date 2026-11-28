@echo off
rem ==========================================================================
rem  make_importlib.bat - build MSVC import libraries (MemDB2Dll.lib) for
rem  MemDB2Dll.dll from MemDB2Dll.def, for every built configuration:
rem    Win32\Debug, Win32\Release  (x86)
rem    Win64\Debug, Win64\Release  (x64)
rem  The .lib is written next to the matching MemDB2Dll.dll.
rem
rem  Finds Visual Studio 2017 or later with vswhere. If that fails, run this
rem  from a "Developer Command Prompt for VS" instead.
rem ==========================================================================
setlocal
set "HERE=%~dp0"
set "DEF=%HERE%MemDB2Dll.def"
if exist "%DEF%" goto :have_def
echo Cannot find %DEF%
exit /b 1
:have_def

rem Already in a Developer Command Prompt?
where lib.exe >nul 2>&1
if not errorlevel 1 goto :have_lib

set "VSWHERE=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe"
if exist "%VSWHERE%" goto :have_vswhere
echo Could not find vswhere.exe or lib.exe.
echo Run this from a "Developer Command Prompt for VS" instead.
exit /b 1
:have_vswhere
set "VSDIR="
for /f "usebackq delims=" %%i in (`"%VSWHERE%" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath`) do set "VSDIR=%%i"
if defined VSDIR goto :have_vsdir
echo Visual Studio with C++ tools not found.
echo Run this from a "Developer Command Prompt for VS" instead.
exit /b 1
:have_vsdir
call "%VSDIR%\VC\Auxiliary\Build\vcvarsall.bat" x86 >nul
where lib.exe >nul 2>&1
if not errorlevel 1 goto :have_lib
echo vcvarsall.bat did not put lib.exe on the PATH.
exit /b 1

:have_lib
set "MADE=0"
call :make Win32 Debug x86
call :make Win32 Release x86
call :make Win64 Debug x64
call :make Win64 Release x64
if "%MADE%"=="0" echo No built MemDB2Dll.dll found under Win32\ or Win64\. Build the DLL first.
endlocal
exit /b 0

rem --------------------------------------------------------------------------
:make
set "OUTDIR=%HERE%%1\%2"
if not exist "%OUTDIR%\MemDB2Dll.dll" goto :eof
lib.exe /nologo /def:"%DEF%" /machine:%3 /out:"%OUTDIR%\MemDB2Dll.lib"
if errorlevel 1 goto :make_failed
echo Made %OUTDIR%\MemDB2Dll.lib
set /a MADE+=1
goto :eof
:make_failed
echo FAILED: %OUTDIR%\MemDB2Dll.lib
goto :eof
