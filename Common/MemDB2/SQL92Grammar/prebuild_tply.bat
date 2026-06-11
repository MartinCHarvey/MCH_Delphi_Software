setlocal

pushd "%~dp0"
set "OLDPATH=%PATH%"

for %%I in ("C:\MCH Stuff\github\tply41a\tply41a\Win32\Debug") do set "PATH=%PATH%;%%~fI"

del errors.txt
del SQL92Grammar.lst

plex SQL92Grammar.l SQL92Grammar_lexer
echo %ERRORLEVEL%

pyacc SQL92Grammar.y SQL92Grammar_parser

set "PATH=%OLDPATH%"
echo %ERRORLEVEL%

popd
echo %ERRORLEVEL%
endlocal
echo %ERRORLEVEL%

