@echo off
rem ==========================================================================
rem  vtune_prep.bat - make VTune-readable symbols for the Win64 Release DLL.
rem
rem    vtune_prep.bat
rem
rem  1. Runs map2pdb on ..\C_DLL\Win64\Release\MemDB2Dll.map, writing
rem     MemDB2Dll.pdb beside it and binding it to MemDB2Dll.dll (adds a debug
rem     directory entry pointing at the PDB, so VTune finds it by itself).
rem  2. Rebuilds the x64 bench (build.bat x64), which copies the DLL and both
rem     PDBs into x64\.
rem
rem  Build the DLL first with a detailed map file (Linking > Map file =
rem  Detailed) and Debug information on, for line numbers. Re-run this after
rem  every DLL rebuild: an old PDB will not match the new DLL.
rem
rem  map2pdb.exe is looked for under MAP2PDB_DIR (default
rem  C:\MCH Stuff\github\map2pdb); set MAP2PDB to its full path to override.
rem ==========================================================================
setlocal
set "HERE=%~dp0"
set "DLLDIR=%HERE%..\C_DLL\Win64\Release"
if not defined MAP2PDB_DIR set "MAP2PDB_DIR=C:\MCH Stuff\github\map2pdb"

if defined MAP2PDB goto :have_tool
for /r "%MAP2PDB_DIR%" %%f in (map2pdb.exe) do if exist "%%f" set "MAP2PDB=%%f"
:have_tool
if not defined MAP2PDB (
    echo map2pdb.exe not found under %MAP2PDB_DIR% - set MAP2PDB to its path.
    exit /b 1
)
if not exist "%DLLDIR%\MemDB2Dll.map" (
    echo Missing %DLLDIR%\MemDB2Dll.map - build the Release DLL with a detailed map file.
    exit /b 1
)
echo Using %MAP2PDB%

"%MAP2PDB%" -bind:"%DLLDIR%\MemDB2Dll.dll" "%DLLDIR%\MemDB2Dll.map"
if errorlevel 1 (
    echo map2pdb failed.
    exit /b 1
)
if not exist "%DLLDIR%\MemDB2Dll.pdb" (
    echo map2pdb did not write %DLLDIR%\MemDB2Dll.pdb
    exit /b 1
)

call "%HERE%build.bat" x64
if errorlevel 1 exit /b 1
echo.
echo Ready: x64\mdb2bench.exe, MemDB2Dll.dll and both PDBs.
exit /b 0
