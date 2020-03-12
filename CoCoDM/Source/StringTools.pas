unit StringTools;

interface

uses
  Vcl.Graphics {for fonts};

const
  CRLF = #13#10; { MS-DOS/Windows }

function SaveStringToFile(const TheString : AnsiString; const FileName : AnsiString) : boolean;

function StrTok(
    var Text : AnsiString;
    const ch : AnsiChar) : AnsiString;

function FormatVersion(
    const Format : AnsiString;
    const MajorVersion : integer;
    const MinorVersion : integer;
    const Release : integer;
    const Build : integer) : AnsiString;

function StrIsNumeric(const Str : AnsiString; const AllowDecimal : boolean) : boolean;
function PadLeftCh(const Str : AnsiString; const ch : AnsiChar; const Len : integer) : AnsiString;
function PadRightCh(const Str : AnsiString; const ch : AnsiChar; const Len : integer) : AnsiString;
function PadLeft(const Str : AnsiString; const Len : integer) : AnsiString;
function PadRight(const Str : AnsiString; const Len : integer) : AnsiString;

function FontStyleSetToStr(const FontStyleSet : TFontStyles) : AnsiString;
function StrToFontStyleSet(FontStyleStr : AnsiString) : TFontStyles;

implementation

uses
  Classes, SysUtils, TypInfo;

function StrIsNumeric(const Str : AnsiString; const AllowDecimal : boolean) : boolean;
var
  ValidChars : set of AnsiChar;
  i : integer;
begin
  Result := TRUE;
  ValidChars := ['0'..'9'];
  if AllowDecimal then
    ValidChars := ValidChars + ['.'];
  for i := 1 to Length(Str) do
    if NOT (Str[i] IN ValidChars) then
    begin
      Result := FALSE;
      Break;
    end;
end; {StrIsNumeric}

function PadLeftCh(const Str : AnsiString; const ch : AnsiChar; const Len : integer) : AnsiString;
var
  i : integer;
begin
  Result := Str;
  for i := 1 to Len - (Length(Str)) do
    Result := ch + Result;
end; {PadLeftCh}

function PadRightCh(const Str : AnsiString; const ch : AnsiChar; const Len : integer) : AnsiString;
var
  i : integer;
begin
  Result := Str;
  for i := 1 to Len - (Length(Str)) do
    Result := Result + ch;
end; {PadRightCh}

function PadLeft(const Str : AnsiString; const Len : integer) : AnsiString;
begin
  Result := PadLeftCh(Str, ' ', Len);
end; {PadLeft}

function PadRight(const Str : AnsiString; const Len : integer) : AnsiString;
begin
  Result := PadRightCh(Str, ' ', Len);
end; {PadRight}

function StrTok(
    var Text : AnsiString;
    const ch : AnsiChar) : AnsiString;
var
  apos : integer;
begin
  apos := Pos(ch, Text);
  if (apos > 0) then
  begin
    Result := Copy(Text, 1, apos - 1);
    Delete(Text, 1, apos);
  end
  else
  begin
    Result := Text;
    Text := '';
  end;
end; {StrTok}

function FormatVersion(
    const Format : AnsiString;
    const MajorVersion : integer;
    const MinorVersion : integer;
    const Release : integer;
    const Build : integer) : AnsiString;
var
  InDblQuotes : boolean;
  InSingleQuotes : boolean;
  Specifier : AnsiString;
  i : integer;

  function GetVersionPartStr : AnsiString;
  var
    j : integer;
  begin
    Result := '';
    if Length(Specifier) > 0 then
    begin
      case Specifier[1] of
        'm' : Result := IntToStr(MajorVersion);
        'n' : Result := IntToStr(MinorVersion);
        'r' : Result := IntToStr(Release);
        'b' : Result := IntToStr(Build);
        else
          Result := '0';
      end; {case}
      for j := Length(Result) to Length(Specifier) - 1 do
        Result := '0' + Result;
    end; {Length > 0}
    Specifier := '';
  end; {GetVersionPartStr}

  function HandleSpecifier(const Ch : AnsiChar) : AnsiString;
  begin
    Result := '';
    if (Length(Specifier) = 0) then
      Specifier := Specifier + Ch
    else
    begin
      if copy(Specifier,1,1) = Ch then
        Specifier := Specifier + Ch
      else
      begin
        Result := Result + GetVersionPartStr;
        Specifier := Ch;
      end;
    end;
  end; {HandleSpecifier}

begin {FormatVersion}
  if Format = '' then
    Result := IntToStr(MajorVersion) + '.'
      + IntToStr(MinorVersion) + '.'
      + IntToStr(Release) + '.'
      + IntToStr(Build)
  else
  begin
    InDblQuotes := FALSE;
    InSingleQuotes := FALSE;
    Result := '';
    Specifier := '';
    for i := 1 to Length(Format) do
    begin
      if (InDblQuotes AND (Format[i] <> '"'))
          OR (InSingleQuotes AND (Format[i] <> #39)) then
        Result := Result + Format[i]
      else {NOT InQuotes}
      begin
        case Format[i] of
          '"' : InDblQuotes := NOT InDblQuotes;
          #39 : InSingleQuotes := NOT InSingleQuotes;
          'm',
          'n',
          'r',
          'b' : Result := Result + HandleSpecifier(Format[i]);
          else
            begin
              if Specifier <> '' then
                Result := Result + GetVersionPartStr;
              Result := Result + Format[i];
            end;
        end; {case}
      end; {NOT InQuotes}
    end;  {for i}
  if Specifier <> '' then
    Result := Result + GetVersionPartStr;
  end; {Format <> ''}
end; {FormatVersion}

function GetFontStyleStr(const fs : TFontStyle) : AnsiString;
begin
  Result := GetEnumName(TypeInfo(TFontStyle), ord(fs));
end; {GetFontStyleStr}

function AddFontStyleToSetStr(const fs : TFontStyle; const SetStr : AnsiString) : AnsiString;
begin
  if SetStr > '' then
    Result := Result + ',';
  Result := Result + GetFontStyleStr(fs);
end; {AddFontStyleToSetStr}

function FontStyleSetToStr(const FontStyleSet : TFontStyles) : AnsiString;
begin
  Result := '[';
  if fsBold IN FontStyleSet then
    Result := Result + AddFontStyleToSetStr(fsBold, Result);
  if fsItalic IN FontStyleSet then
    Result := Result + AddFontStyleToSetStr(fsItalic, Result);
  if fsUnderline IN FontStyleSet then
    Result := Result + AddFontStyleToSetStr(fsUnderline, Result);
  if fsStrikeOut IN FontStyleSet then
    Result := Result + AddFontStyleToSetStr(fsStrikeOut, Result);
  Result := Result  + ']';
end; {FontStyleSetToStr}

function StrToFontStyleSet(FontStyleStr : AnsiString) : TFontStyles;
var
  AFontStyle : AnsiString;
begin
  Result := [];
  FontStyleStr := copy(FontStyleStr, 2, length(FontStyleStr) - 2);
  while length(FontStyleStr) > 0 do
  begin
    AFontStyle := StrTok(FontStyleStr, ',');
    if AFontStyle = GetEnumName(TypeInfo(TFontStyle), ord(fsBold)) then
      Result := Result + [fsBold];
    if AFontStyle = GetEnumName(TypeInfo(TFontStyle), ord(fsItalic)) then
      Result := Result + [fsItalic];
    if AFontStyle = GetEnumName(TypeInfo(TFontStyle), ord(fsUnderline)) then
      Result := Result + [fsUnderline];
    if AFontStyle = GetEnumName(TypeInfo(TFontStyle), ord(fsStrikeout)) then
      Result := Result + [fsStrikeout];
  end;
end; {StrToFontStyleSet}

{ SaveStringToFile: This might work better with a TStringStream }
function SaveStringToFile(const TheString : AnsiString; const FileName : AnsiString) : boolean;
var
  SL : TStringList;
begin
  Result := TRUE;
  SL := TStringList.Create;
  try
    SL.Text := TheString;
    try
      SL.SaveToFile(FileName);
    except
      Result := FALSE;
    end;
  finally
    SL.Free;
  end;
end; {SaveStringToFile}

end.


