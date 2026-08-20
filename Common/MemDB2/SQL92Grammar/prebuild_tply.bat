setlocal

pushd "%~dp0"
set "OLDPATH=%PATH%"

for %%I in ("C:\MCH Stuff\github\tply41a\tply41a\Win32\Debug") do set "PATH=%PATH%;%%~fI"
for %%I in ("C:\MCH Stuff\github\tply41a\tply41a\genstate_debug\Win32\Debug") do set "PATH=%PATH%;%%~fI"

del errors.txt
del *.lst

plex SQL92Grammar.l SQL92Grammar_lexer.pas SQL92Grammar_lexer.lst -v -oo
echo %ERRORLEVEL%

pyacc SQL92Grammar.y SQL92Grammar_parser.pas SQL92Grammar_parser.lst -v -oo

genstate_debug SQL92Grammar_parser.lst SQL92Grammar_parser_debug.pas
echo %ERRORLEVEL%

set "PATH=%OLDPATH%"
echo %ERRORLEVEL%

popd
echo %ERRORLEVEL%
endlocal
echo %ERRORLEVEL%

