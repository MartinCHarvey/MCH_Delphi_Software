unit AppVersionStr;

{ Martin Harvey 23 Dec 2010

  Code to get version string from application }

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
