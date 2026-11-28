@echo off
rem ==========================================================================
rem  build.bat - build mdb2bench.exe with MSVC against the Release MemDB2Dll.
rem
rem    build.bat          x64 and x86
rem    build.bat x64      x64 only  -> x64\mdb2bench.exe (+ MemDB2Dll.dll)
rem    build.bat x86      x86 only  -> x86\mdb2bench.exe (+ MemDB2Dll.dll)
rem
rem  Needs the Release DLL and import library:
rem    ..\C_DLL\Win64\Release\MemDB2Dll.dll / .lib
rem    ..\C_DLL\Win32\Release\MemDB2Dll.dll / .lib
rem  (make the .lib files with ..\C_DLL\make_importlib.bat).
rem
rem  Finds Visual Studio 2017 or later with vswhere. Without vswhere, run it
rem  from a Developer Command Prompt; it then builds that prompt's target only.
rem
rem  Always writes mdb2bench.pdb (debug info, still fully optimised) and copies
rem  MemDB2Dll.pdb next to the DLL if one exists (see vtune_prep.bat).
rem  If the VTune SDK is found (set ITT_DIR to its "sdk" folder, or it is looked
rem  for under VTUNE_PROFILER_DIR and the default oneAPI install), the bench is
rem  built with USE_ITT so --profile can pause and resume collection.
rem ==========================================================================
setlocal
set "HERE=%~dp0"
set "WHICH=%~1"
if "%WHICH%"=="" set "WHICH=all"

set "VSDIR="
set "VSWHERE=%ProgramFiles(x86)%\Microsoft Visual Studio\Installer\vswhere.exe"
if not exist "%VSWHERE%" goto :no_vswhere
for /f "usebackq delims=" %%i in (`"%VSWHERE%" -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath`) do set "VSDIR=%%i"
:no_vswhere

if not defined ITT_DIR if defined VTUNE_PROFILER_DIR set "ITT_DIR=%VTUNE_PROFILER_DIR%\sdk"
if not defined ITT_DIR if exist "%ProgramFiles(x86)%\Intel\oneAPI\vtune\latest\sdk\include\ittnotify.h" set "ITT_DIR=%ProgramFiles(x86)%\Intel\oneAPI\vtune\latest\sdk"
if defined ITT_DIR if not exist "%ITT_DIR%\include\ittnotify.h" set "ITT_DIR="

set "FAILED=0"
if /i "%WHICH%"=="all" (
    call :build x64 Win64
    call :build x86 Win32
) else if /i "%WHICH%"=="x64" (
    call :build x64 Win64
) else if /i "%WHICH%"=="x86" (
    call :build x86 Win32
) else (
    echo Usage: build.bat [x64^|x86^|all]
    exit /b 2
)
if not "%FAILED%"=="0" exit /b 1
exit /b 0

rem --------------------------------------------------------------------------
:build
setlocal
set "ARCH=%1"
set "DLLDIR=%HERE%..\C_DLL\%2\Release"
set "OUT=%HERE%%ARCH%"

if defined VSDIR (
    call "%VSDIR%\VC\Auxiliary\Build\vcvarsall.bat" %ARCH% >nul
) else (
    where cl.exe >nul 2>&1
    if errorlevel 1 (
        echo Visual Studio with C++ tools not found. Run from a Developer Command Prompt.
        endlocal & set "FAILED=1"
        goto :eof
    )
    if /i not "%VSCMD_ARG_TGT_ARCH%"=="%ARCH%" (
        echo Skipping %ARCH%: this prompt targets %VSCMD_ARG_TGT_ARCH%.
        endlocal
        goto :eof
    )
)

if not exist "%DLLDIR%\MemDB2Dll.lib" (
    echo Missing %DLLDIR%\MemDB2Dll.lib - build the Release DLL and run make_importlib.bat.
    endlocal & set "FAILED=1"
    goto :eof
)
if not exist "%OUT%" mkdir "%OUT%"

set "LAA="
if /i "%ARCH%"=="x86" set "LAA=/LARGEADDRESSAWARE"

set "ITTC="
set "ITTL="
set "ITTLIB=lib64"
if /i "%ARCH%"=="x86" set "ITTLIB=lib32"
rem No ( ) block here: the SDK path usually contains "(x86)".
if not defined ITT_DIR goto :no_itt
if not exist "%ITT_DIR%\%ITTLIB%\libittnotify.lib" goto :no_itt
set ITTC=/DUSE_ITT /I"%ITT_DIR%\include"
set ITTL="%ITT_DIR%\%ITTLIB%\libittnotify.lib"
:no_itt

if defined ITTC (echo Building %ARCH% with VTune markers ...) else echo Building %ARCH% ...
cl /nologo /O2 /Oi /GS- /W3 /MT /Zi /DNDEBUG /D_CRT_SECURE_NO_WARNINGS %ITTC% ^
   /I"%HERE%..\C_API" /Fo"%OUT%\\" /Fd"%OUT%\vc_bench.pdb" /Fe"%OUT%\mdb2bench.exe" ^
   "%HERE%bench_main.c" "%HERE%bench_util.c" "%HERE%engine_memdb2.c" "%HERE%engine_baseline.c" ^
   /link /DEBUG /OPT:REF /OPT:ICF /PDB:"%OUT%\mdb2bench.pdb" /DELAYLOAD:MemDB2Dll.dll %LAA% ^
   "%DLLDIR%\MemDB2Dll.lib" delayimp.lib psapi.lib %ITTL%
if errorlevel 1 (
    echo FAILED: %ARCH%
    endlocal & set "FAILED=1"
    goto :eof
)
copy /y "%DLLDIR%\MemDB2Dll.dll" "%OUT%\" >nul
if exist "%DLLDIR%\MemDB2Dll.pdb" copy /y "%DLLDIR%\MemDB2Dll.pdb" "%OUT%\" >nul
echo Made %OUT%\mdb2bench.exe
endlocal
goto :eof
