unit AppVersionStr;
{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

{ Code to get version string from application }

interface

function GetAppVerStr: string;

implementation

uses Windows, Forms, SysUtils;

type
  TLangAndPage = packed record
    Lang: WORD;
    Page: WORD;
  end;
  PLangAndPage = ^TLangAndPage;


function GetAppVerStr: string;
var
  vfSize, ValSize, idx, tmp: cardinal;
  vInfo: pChar;
  Ptr: pointer;
  vVerStr: pChar;
  PLP: PLangAndPage;
  LocaleString: string;
  PathString: string;
  AppNameString: AnsiString;
begin
  result := '';
  AppNameString := AnsiString(Application.ExeName);
  vfSize := GetFileVersionInfoSizeA(PAnsiChar(AppNameString), tmp);
  GetMem(vInfo, vfSize);
  try
    if not GetFileVersionInfoA(PAnsiChar(AppNameString),
      tmp,
      vfSize,
      vInfo) then exit;
    if not VerQueryValue(vInfo,
      '\VarFileInfo\Translation',
      Ptr,
      ValSize) then exit;
    PLP := Ptr;
    for idx := 0 to Pred(ValSize div sizeof(TLangAndPage)) do
    begin
      FmtStr(LocaleString, '%.4x%.4x', [PLP.Lang, PLP.Page]);
      PathString := '\StringFileInfo\' + LocaleString + '\FileVersion';
      if VerQueryValue(vInfo,
        PChar(PathString),
        Ptr,
        tmp) then
      begin
        vVerStr := Ptr;
        result := StrPas(vVerStr);
        break;
      end;
      Inc(PLP);
    end;
  finally
    FreeMem(vInfo);
  end;
  if sizeof(pointer) = sizeof(integer) then
    result := result + ' (32 bit build).'
  else if sizeof(pointer) = sizeof(int64) then
    result := result + ' (64 bit build).'
end;

end.
