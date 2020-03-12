unit HTMLEscapeHelper;
{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

{ Actually more of a misc URL / string helper-parser unit }

interface

uses CoCoBase, Classes, IdGlobal, IdCoderMIME;

type
  THTMLDocType = (tdtUnknown, tdtHTML, tdtCSS, tdtJSON, tdtJScript);

//Functions to escape HTML string
function EscapeToHTML(const Str: string): string;
function NeedsEscape(WChar: WideChar; var Escape: string): boolean;

//Functions for HTML parser to unescape HTML string.
function LookupNumericEscape(Grammar: TCoCORGrammar; NumEsc: string): string;
function LookupIdEscape(Grammar: TCoCORGrammar; IdEsc:string): string;

//NB. Does not deal with query URL's (Get / Post with "?params=" etc)
//Use ParseURLEx for that.
procedure ParseURL(BaseUrl: string;
                  out ProtoStr: string;
                  out SiteStr: string;
                  out FullFileStr: string;
                  out NameStr: string;
                  out ExtStr: string);

procedure ParseURLEx(BaseUrl: string;
                  out ProtoStr: string;
                  out SiteStr: string;
                  out FullFileStr: string;
                  out NameStr: string;
                  out ExtStr: string;
                  out HasParams: boolean;
                  Names: TStrings;
                  Values: TStrings);

function BuildURL(ProtoStr: string;
                      SiteStr: string;
                      FullFileStr: string;
                      HasParams: boolean;
                      Names: TStrings;
                      Values: TStrings): string;

function MakeCanonicalURL(BaseURL: string; RelUrl: string):string;

function GetDocTypeFromContentHeader(Hdr: string): THTMLDocType;
function GetDocTypeFromURL(Url: string): THTMLDocType;
function MakeURLFromDocType(DocType: THTMLDocType):string;

const
  DocTypeStrs: array[THTMLDocType] of string =
    ('tdtUnknown', 'tdtHTML', 'tdtCSS', 'tdtJSON', 'tdtJScript');

procedure FixAnsifiedBackToUnicode(var Ansified: string);

function CheckedUTF8ToUnicode(UTF8: AnsiString): string;
function CheckedUnicodeToUTF8(Unicode: string): AnsiString;

procedure UnescapeJavascriptString(var JSString: string);
procedure UnescapeJSonString(var JSONString: string);

function OAuth1HMACTokenEncode(HTTPMethodString: string;
                                 OriginalURL: string;
                                 RequestParams: TStrings;
                                 RequestValues: TStrings): string;

function OAuth2HMACTokenEncode(Endpoint: string; const Params: TStrings; const Values:TStrings): string;
function URLPostDataEncode(const Params: TStrings; const Values:TStrings):string;
function OAuth1HeaderValEncode(const Params: TStrings; const Values:TStrings):string;
function PostDataCommonEncode(const Params: TStrings; const Values:TStrings; Separator: string; QuoteValues: boolean):string;

procedure URLPostDataDecode(const Src:String; Params:TStrings; Values:TStrings);

//Only use these functions if strings not in a parameter list of some sort.
//These use % encoding, and do not translate " " to "+"
function URLDecode(Input: string): string;
function URLEncode(Input:string):string;

//Translate Hex /UTF8 strings to byte arrays, for OAuth2
function IdBytesToHexString(const Bytes: TIDBytes): string;
function HexStringToIdBytes(Str: string; var Bytes: TIdBytes): boolean;
function UTF8StringToIdBytes(Str: string; var Bytes: TIdBytes): boolean;
function IdBytesToUTF8String(const Bytes: TIdBytes): string;

//Base64 encoding and decoding functions, for OAuth1.
function IdBytesToBase64String(const Bytes: TIDBytes): string;
function Base64StringToIdBytes(Str:string; var Bytes: TIdBytes): boolean;

implementation

uses
  SysUtils, HTMLGrammar, Data.Cloud.CloudAPI;

function BuildURL(ProtoStr: string;
                      SiteStr: string;
                      FullFileStr: string;
                      HasParams: boolean;
                      Names: TStrings;
                      Values: TStrings) : string;
begin
  Assert(Length(ProtoStr) > 0);
  result := ProtoStr;
  result := result + '://';
  Assert(Length(SiteStr) > 0);
  result := result + SiteStr;
  if (Length(FullFileStr) > 0) and (FullFileStr[1] <> '/') then
    result := result + '/';
  result := result + FullFileStr;
  if HasParams then
  begin
    result := result + '?';
    result := result + URLPostDataEncode(Names, Values);
  end;
end;


function URLEncode(Input:string):string;
var
  Ch: Char;
  XLate: boolean;
  i: integer;

begin
  Result := '';
  //Set up array of chars not to encode.
  for i := 1 to Length(Input) do
  begin
    XLate := true;
    Ch := Input[i];
    if (Ch >= 'a') and (Ch <= 'z') then XLate := false;
    if (Ch >= 'A') and (Ch <= 'Z') then XLate := false;
    if (Ch >= '0') and (Ch <= '9') then XLate := false;
    if (Ch = '-') or (Ch = '_') or
       (Ch = '.') or (Ch = '~') then
      XLate := false;

    if not XLate then
      result := result + Input[i]
    else
    begin
      if Ord(Ch) <= $FF then
        result := result + '%' + IntToHex(Ord(Input[i]), 2)
      else
        Assert(false); //result := result + '%' + IntToHex(Ord(Input[i]), 4)
    end;
  end;
end;

function URLDecode(Input: string): string;
var
  i, j: integer;
  HexStr: string;
  Encoded: Char;
begin
  result := '';
  i := 1;
  while i <= Length(Input) do
  begin
    if Input[i] <> '%' then
    begin
      result := result + Input[i];
      Inc(i);
    end
    else
    begin
      Inc(i);
      j := i;
      HexStr := '';
      while (i <= Length(input))
      and (((Input[i] >= 'A') and (Input[i] <= 'F'))
        or ((Input[i] >= 'a') and (Input[i] <= 'f'))
        or ((Input[i] >= '0') and (Input[i] <= '9')))
      and ((i -j) < 2) do
      begin
        HexStr := HexStr + Input[i];
        Inc(i);
      end;
      if Length(HexStr) > 0 then
      begin
        HexStr := '$' + HexStr.ToUpper;
        Encoded := Chr(StrToInt(HexStr));
        result := result + Encoded;
      end;
    end;
  end;
end;

function OAuth1HMACTokenEncode(HTTPMethodString: string;
                                 OriginalURL: string;
                                 RequestParams: TStrings;
                                 RequestValues: TStrings): string;
var
  URLParams, URLValues: TStringList;
  CombinedParams, CombinedValues: TStringList;
  SortedParams, SortedValues: TStringList;
  Proto, Site, FullFile, Name, Ext: string;
  HasParams: boolean;
  idx, Least: integer;
  ResultMethod, ResultBaseURI, ResultParams: string;
begin
  Assert(RequestParams.Count = RequestValues.Count);
  URLParams := TStringList.Create;
  URLValues := TStringList.Create;
  CombinedParams := TStringList.Create;
  CombinedValues := TStringList.Create;
  SortedParams := TStringList.Create;
  SortedValues := TStringList.Create;
  try
    ParseURLEx(OriginalURL, Proto, Site, FullFile, Name, Ext, HasParams,
               UrlParams, UrlValues);

    ResultMethod := UpperCase(HTTPMethodString);
    ResultBaseURI := LowerCase(Proto) + '://' + LowerCase(Site);
    if (Length(FullFile) > 0) and (FullFile[1] <> '/') then
      ResultBaseURI := ResultBaseURI + '/';
    ResultBaseURI := ResultBaseURI + FullFile;

    Assert(URLParams.Count = URLValues.Count);
    for idx := 0 to Pred(RequestParams.Count) do
    begin
      CombinedParams.Add(RequestParams[idx]);
      CombinedValues.Add(RequestValues[idx]);
    end;
    for idx := 0 to Pred(URLParams.Count) do
    begin
      CombinedParams.Add(URLParams[idx]);
      CombinedValues.Add(URLValues[idx]);
    end;
    //Expensive selection sort, but we expect number
    //of parameters to be relatively small.
    while CombinedParams.Count > 0 do
    begin
      Least := -1;
      for idx := 0 to Pred(CombinedParams.Count) do
      begin
        if (Least < 0)
          or (CompareText(URLEncode(CombinedParams[idx]), URLEncode(CombinedParams[Least])) < 0)
          or ((CompareText(URLEncode(CombinedParams[idx]), URLEncode(CombinedParams[Least])) = 0)
          and (CompareText(URLEncode(CombinedValues[idx]), URLEncode(CombinedValues[Least])) < 0)) then
            Least := idx;
      end;
      SortedParams.Add(CombinedParams[Least]);
      SortedValues.Add(CombinedValues[Least]);
      CombinedParams.Delete(Least);
      CombinedValues.Delete(Least);
    end;
    ResultParams := URLPostDataEncode(SortedParams, SortedValues);
    result := ResultMethod + '&' + URLEncode(ResultBaseURI) + '&' + URLEncode(ResultParams);
  finally
    URLParams.Free;
    URLValues.Free;
    CombinedParams.Free;
    CombinedValues.Free;
    SortedParams.Free;
    SortedValues.Free;
  end;
end;

function OAuth2HMACTokenEncode(Endpoint: string; const Params: TStrings; const Values:TStrings): string;
var
  i: integer;
  ParamsSorted, ValuesSorted: TStringList;
  ParamsCpy, ValuesCpy: TStringList;
  Least, idx: integer;
begin
  Assert(Assigned(Params));
  Assert(Assigned(Values));
  Assert(Params.Count = Values.Count);
  result := '';
  ParamsSorted := TStringList.Create;
  ValuesSorted := TStringList.Create;
  ParamsCpy := TStringList.Create;
  ValuesCpy := TStringList.Create;
  try
    //Nasty quick selection sort to get the keys in order.
    ParamsCpy.Assign(Params);
    ValuesCpy.Assign(Values);
    while ParamsCpy.Count > 0 do
    begin
      Least := -1;
      for idx := 0 to Pred(ParamsCpy.Count) do
      begin
        if (Least < 0)
          or (CompareText(ParamsCpy[idx], ParamsCpy[Least]) < 0)
          or ((CompareText(ParamsCpy[idx], ParamsCpy[Least]) = 0)
          and (CompareText(ValuesCpy[idx], ValuesCpy[Least]) < 0)) then
            Least := idx;
      end;
      ParamsSorted.Add(ParamsCpy[Least]);
      ValuesSorted.Add(ValuesCpy[Least]);
      ParamsCpy.Delete(Least);
      ValuesCpy.Delete(Least);
    end;
    result := PostDataCommonEncode(ParamsSorted, ValuesSorted, '|', false);
    result := Endpoint + '|' + result;
  finally
    ParamsSorted.Free;
    ValuesSorted.Free;
    ParamsCpy.Free;
    ValuesCpy.Free;
  end;
end;

function PostDataCommonEncode(const Params: TStrings; const Values:TStrings; Separator: string; QuoteValues: boolean):string;
var
  i: integer;
begin
  Assert(Assigned(Params));
  Assert(Assigned(Values));
  Assert(Params.Count = Values.Count);
  result := '';
  for i := 0 to Pred(Params.Count) do
  begin
    if not QuoteValues then
    begin
      result := result + UrlEncode(Params.Strings[i])
                  + '=' + UrlEncode(Values.Strings[i]);
    end
    else
    begin
      result := result + UrlEncode(Params.Strings[i])
                  + '="' + UrlEncode(Values.Strings[i]) + '"';
    end;
    if i < Pred(Params.Count) then
      result := result + Separator;
  end;
end;

function URLPostDataEncode(const Params: TStrings; const Values:TStrings):string;
begin
  result := PostDataCommonEncode(Params, Values, '&', false);
end;

function OAuth1HeaderValEncode(const Params: TStrings; const Values:TStrings):string;
begin
  result := PostDataCommonEncode(Params, Values, ', ', true);
end;

procedure URLPostDataDecode(const Src:String; Params:TStrings; Values:TStrings);
var
  i: integer;
  Param: string;
  Value: string;
begin
  Params.Clear;
  Values.Clear;
  i := 1;
  while i <= Length(Src) do
  begin
    if Src[i] <> '?' then
      Inc(i)
    else
      break;
  end;
  if (i > Length(Src)) or (Src[i] <> '?') then
    exit; //No params.
  Inc(i);
  while (i <= Length(Src)) do
  begin
    Param := '';
    Value := '';
    while (i <= Length(Src)) and (Src[i] <> '=') do
    begin
      Param := Param + Src[i];
      Inc(i);
    end;
    Inc(i);
    while (i <= Length(Src)) and (Src[i] <> '&') do
    begin
      Value := Value + Src[i];
      Inc(i);
    end;
    Inc(i);
    if Length(Param) > 0 then
    begin
      Params.Add(URLDecode(Param));
      Values.Add(URLDecode(Value));
    end;
  end;
end;

function HexChrToCard(HexChr: WideChar): cardinal;
begin
  if (HexChr >= '0') and (HexChr <= '9') then
    Result := Ord(HexChr) - Ord('0')
  else if (HexChr >= 'a') and (HexChr <= 'f') then
    Result := 10 + Ord(HexChr) - Ord('a')
  else if (HexChr >= 'A') and (HexChr <= 'F') then
    Result := 10 + Ord(HexChr) - Ord('A')
  else
  begin
    Assert(false);
    result := 0;
  end;
end;

procedure FixAnsifiedBackToUnicode(var Ansified: string);
var
  i: integer;
  Convert: boolean;
  AnsiS: AnsiString;
begin
  Convert := false;
  //Any conversion needed?
  for i := 1 to Length(Ansified) do
  begin
    if (Ord(Ansified[i]) >= $80) then
    begin
      Convert := true;
      break;
    end;
    Assert(Ord(Ansified[i]) <= $FF);
    //Otherwise string already has unicode chars
  end;
  if Convert then
  begin
    AnsiS := AnsiString(Ansified); //Hence preceeding assertion.
    Ansified := CheckedUTF8ToUnicode(AnsiS);
  end;
end;

function CheckedUTF8ToUnicode(UTF8: AnsiString): string;
begin
  result := UTF8ToUnicodeString(UTF8);
end;

function CheckedUnicodeToUTF8(Unicode: string): AnsiString;
{$IFOPT C+}
var
  TestStr: string;
{$ENDIF}
begin
  result := UTF8Encode(Unicode);
  //With any luck this will not do anything to simple ansi strings...
//And just check our conversion back is good.
{$IFOPT C+}
  TestStr := result;
  CheckedUTF8ToUnicode(TestStr);
  Assert(CompareStr(Unicode, TestStr) = 0);
{$ENDIF}
end;

procedure UnescapeJSCommon(var JSString: string);
var
  i,j: integer;
  Convert: boolean;
  HexEscape: string;
  Result: string;
  AnsiCh: AnsiChar;
  WideCh: WideChar;

begin
  Convert := false;
  for i:= 1 to Length(JSString) do
  begin
    if JSString[i] = '\' then
    begin
      Convert := true;
      break;
    end;
  end;
  if Convert then
  begin
    i := 1;
    Result := '';
    while (i <= Length(JSString)) do
    begin
      if JSString[i] <> '\' then
      begin
        Result := Result + JSString[i];
        Inc(i);
      end
      else
      begin
        Inc(i);
        HexEscape := '';
        if JSString[i] = 'b' then
        begin
          Result := Result + #8;
          Inc(i);
        end
        else if JSString[i] = 'f' then
        begin
          Result := Result + #12;
          Inc(i);
        end
        else if JSString[i] = 'n' then
        begin
          Result := Result + #10;
          Inc(i);
        end
        else if JSString[i] = 'r' then
        begin
          Result := Result + #13;
          Inc(i);
        end
        else if JSString[i] = 't' then
        begin
          Result := Result + #9;
          Inc(i);
        end
        else if JSString[i] = 'v' then
        begin
          Result := Result + #11;
          Inc(i);
        end
        else if JSString[i] = '0' then
        begin
          Result := Result + #0;
          Inc(i);
        end
        else if JSString[i] = 'u' then
        begin
          Inc(i);
          if (i <= Length(JSString)) and (JSString[i] = '{') then
            Inc(i);
          for j := 0 to 3 do
          begin
            HexEscape := HexEscape + JSString[i];
            Inc(i);
          end;
          if (i <= Length(JSString)) and (JSString[i] = '{') then
            Inc(i);
        end
        else if JSString[i] = 'x' then
        begin
          Inc(i);
          for j := 0 to 1 do
          begin
            HexEscape := HexEscape + JSString[i];
            Inc(i);
          end;
        end
        else if (JSString[i] = Chr(13)) or (JSString[i] = Chr(10)) then
        begin
          while (JSString[i] = Chr(13)) or (JSString[i] = Chr(10)) do
          begin
            Result := Result + JSString[i];
            Inc(i);
          end;
        end
        else
        begin
          Result := Result + JSString[i];
          Inc(i);
        end;
        if Length(HexEscape) > 0 then
        begin
          if Length(HexEscape) = 2 then
          begin
            AnsiCh := AnsiChar((HexChrToCard(HexEscape[1]) shl 4) or
                      (HexChrToCard(HexEscape[2])));
            Result := Result + AnsiCh;
          end
          else if Length(HexEscape) = 4 then
          begin
            WideCh := WideChar((HexChrToCard(HexEscape[1]) shl 12) or
                     (HexChrToCard(HexEscape[2]) shl 8) or
                     (HexChrToCard(HexEscape[3]) shl 4) or
                     (HexChrToCard(HexEscape[4])));
            Result := Result + WideCh;
          end
          else
            Assert(false);
        end;
      end;
    end;
    JSString := result;
  end;
end;

procedure UnescapeJavascriptString(var JSString: string);
begin
  UnescapeJSCommon(JSString);
end;

procedure UnescapeJSonString(var JSONString: string);
begin
  UnescapeJSCommon(JSONString);
end;

function GetDocTypeFromContentHeader(Hdr: string): THTMLDocType;
begin
  if Pos('javascript', Hdr) > 0 then
    result := tdtJScript
  else if Pos('json', Hdr) > 0 then
    result := tdtJSon
  else if Pos('css', Hdr) > 0 then
    result := tdtCSS
  else if (Pos('html', Hdr) > 0) or
          (Pos('plain', Hdr) > 0) then
    result := tdtHTML
  else
    result := tdtUnknown;
end;

function GetDocTypeFromURL(Url: string): THTMLDocType;
var
  Proto,
  Site,
  FullFIle,
  Name,
  Ext: string;
begin
  ParseURL(Url, Proto, Site, FullFile, Name, Ext);
  if  CompareText(Ext, 'js') = 0 then
    result := tdtJScript
  else if CompareText(Ext, 'json') = 0 then
    result := tdtJSon
  else if CompareText(Ext, 'css') = 0 then
    result := tdtCSS
  else if (CompareText(Ext, 'htm') = 0)
    or (CompareText(Ext, 'html') = 0)
    or (Length(Ext) = 0) then
    result := tdtHTML
  else
    result := tdtUnknown;
end;

function MakeURLFromDocType(DocType: THTMLDocType):string;
begin
  case (DocType) of
    tdtJScript: result := '.js';
    tdtJSon: result := '.json';
    tdtCSS: result := '.css';
    tdtHTML: result := '.html';
  else
    Assert(false);
  end;
end;

function EscapeToHTML(const Str: string): string;
var
  Escape: string;
  idx: integer;
begin
  //Slow, could do this in chunks of chars not needing escapes.
  for idx := 1 to Length(Str) do
  begin
    if NeedsEscape(Str[idx], Escape) then
      result := result + '&' + Escape + ';'
    else
      result := result + Str[idx];
  end;
end;

function NeedsEscape(WChar: WideChar; var Escape: string): boolean;
begin
  if Ord(WChar) >= 255 then
  begin
    result := true;
    Escape := IntToStr(Ord(WChar));
  end
  else
  begin
    case WChar of
     #8364: Escape := 'euro';
     #160: Escape := 'nbsp';
     '''': Escape := 'quot';
     '&': Escape := 'amp';
     '<': Escape := 'lt';
     '>': Escape := 'gt';
     #2013: Escape := 'ndash';
     #169: Escape := 'copy';
     #183: Escape := 'middot';
     #8207: Escape := 'rlm';
     #8657: Escape := 'uarr';
     #187: Escape := 'raquo';
     #171: Escape := 'laquo';
    else
      Escape := '';
    end;
    result := Length(Escape) > 0;
  end;
end;

function LookupNumericEscape(Grammar: TCoCORGrammar; NumEsc: string): string;
var
  CharNum: integer;
begin
  try
    CharNum := StrToInt(NumEsc);
    if (CharNum >= 0) and (CharNum <= 65535) then
    begin
      result := Chr(CharNum);
      exit;
    end;
  except
    on EConvertError do ; //And fallthrough to error case.
  end;
  result := '';
end;

function LookupIdEscape(Grammar: TCoCORGrammar; IdEsc:string): string;
var
  cand, replace: string;
  i:integer;
begin
  for i := 0 to 12 do
  begin
    case i of
      0: begin cand := 'euro'; replace := #8364 end;
      1: begin cand := 'nbsp'; replace := #160 end;
      2: begin cand := 'quot'; replace := '''' end;
      3: begin cand := 'amp'; replace := '&' end;
      4: begin cand := 'lt'; replace := '<' end;
      5: begin cand := 'gt'; replace := '>' end;
      6: begin cand := 'ndash'; replace := #2013 end;
      7: begin cand := 'copy'; replace := #169 end;
      8: begin cand := 'middot'; replace := #183 end;
      9: begin cand := 'rlm'; replace := #8207 end;
      10: begin cand := 'uarr'; replace := #8657 end;
      11: begin cand := 'raquo'; replace := #187 end;
      12: begin cand := 'laquo'; replace := #171 end;
    end;
    if AnsiCompareText(IdEsc, cand) = 0 then
    begin
      result := replace;
      exit;
    end;
  end;
  result := '';
end;

procedure ParseURLEx(BaseUrl: string;
                  out ProtoStr: string;
                  out SiteStr: string;
                  out FullFileStr: string;
                  out NameStr: string;
                  out ExtStr: string;
                  out HasParams: boolean;
                  Names: TStrings;
                  Values: TStrings);
var
  PreQueryUrl: string;
  PostQueryUrl: string;
  QueryPos: integer;
begin
  //Cut URL into two pieces: first part is proto, site, location information.
  //Second part is query params and values.
  QueryPos := BaseUrl.IndexOf('?');
  HasParams := QueryPos >= 0;
  if HasParams then
  begin
    PreQueryUrl := BaseURL.Substring(0, QueryPos);
    PostQueryUrl := BaseUrl.Substring(QueryPos + 1);
  end
  else
  begin
    PreQueryUrl := BaseUrl;
    PostQueryUrl := '';
  end;

  ParseURL(PreQueryUrl, ProtoStr, SiteStr, FullFileStr, NameStr, ExtStr);

  if HasParams then
    URLPostDataDecode('?' + PostQueryURL, Names, Values);
end;

//NB. Does not deal with query URL's (Get / Post with "?params=" etc)
//Use ParseURLEx for that.
procedure ParseURL(BaseUrl: string;
                  out ProtoStr: string;
                  out SiteStr: string;
                  out FullFileStr: string;
                  out NameStr: string;
                  out ExtStr: string);
var
  ColonPos: integer;
  RootPos: integer;
  HasProto: boolean;
  QueryPos: integer;

begin
  //First things first - remove the query component if it exists.
  QueryPos := BaseURL.IndexOf('?');
  if QueryPos >= 0 then
    BaseUrl := BaseUrl.Substring(0, QueryPos);

  //Parse out protocol.
  ColonPos := BaseURL.IndexOf(':');
  RootPos := BaseURL.IndexOf('/');
  if (ColonPos < 0) or (RootPos < 0) then
    HasProto := false
  else
    HasProto := ColonPos < RootPos;

  if HasProto then
  begin
    //Canonical URL
    ProtoStr := BaseURL.Substring(0, ColonPos);

    Assert(BaseURL[ColonPos+1] = ':');
    Assert(BaseURL[ColonPos+2] = '/');
    Assert(BaseURL[ColonPos+3] = '/');
    SiteStr := BaseURL.Substring(ColonPos+3);
    FullFileStr := SiteStr;

    //Parse out site name, and fully qualified file path.
    RootPos := SiteStr.IndexOf('/');
    if RootPos >= 0 then
    begin
      SiteStr := SiteStr.Substring(0, RootPos);
      FullFileStr := FullFileStr.Substring(RootPos);
    end
    else
    begin
      //SiteStr has the site name.
      FullFileStr := ''; //No filename specified.
    end;
  end
  else
  begin
    //Relative URL.
    ProtoStr := '';
    SiteStr := '';
    FullFileStr := BaseURL;
  end;

  //Parse out filename string.
  NameStr := FullFileStr;
  RootPos := NameStr.IndexOf('/');
  while RootPos >= 0 do
  begin
    NameStr := NameStr.Substring(RootPos + 1);
    RootPos := NameStr.IndexOf('/');
  end;

  //Parse out filename extension
  ExtStr := NameStr;
  RootPos := ExtStr.IndexOf('.');
  if RootPos >=0 then
  begin
    while RootPos >= 0 do
    begin
      ExtStr := ExtStr.Substring(RootPos + 1);
      RootPos := ExtStr.IndexOf('.');
    end;
  end
  else
    ExtStr := '';
end;


function MakeCanonicalURL(BaseURL: string; RelURL: string):string;
var
  BaseProto, BaseSite, BaseFullFile, BaseName, BaseExt: string;
  RelProto, RelSite, RelFullFile, RelName, RelExt: string;
begin
  ParseURL(BaseURL, BaseProto, BaseSite, BaseFullFile, BaseName, BaseExt);
  ParseURL(RelURL, RelProto, RelSite, RelFullFile, RelName, RelExt);

  if Length(RelProto) > 0 then
  begin
    //Relative URL already canonical.
    result := RelUrl;
  end
  else
  begin
    if RelUrl[1]<> '/' then
      RelUrl := '/' + RelUrl;

    if RelUrl[2]<> '/' then //Site local file, starting in /
      result := BaseProto + '://' + BaseSite + RelUrl
    else //Site specified in RelUrl starts with //, just append protocol.
      result := BaseProto + ':' + RelUrl;
  end;
end;

function IdBytesToHexString(const Bytes: TIdBytes): string;
var
  idx: integer;
begin
  result := '';
  for idx := 0 to Pred(Length(Bytes)) do
  begin
    result := result + IntToHex(Bytes[idx], 2);
  end;
  result := LowerCase(result);
end;

function UTF8StringToIdBytes(Str: string; var Bytes: TIdBytes): boolean;
var
  Idx: integer;
  WVal: integer;
begin
  SetLength(Bytes, Length(Str));
  for idx := 1 to Length(Str) do
  begin
    WVal := Ord(Str[idx]);
    if (WVal < 0) or (WVal > 255) then
    begin
      result := false;
      exit;
    end;
    Bytes[Pred(idx)] := Byte(WVal);
  end;
  result := true;
end;

function IdBytesToUTF8String(const Bytes: TIdBytes): string;
var
  AnsiS: AnsiString;
begin
  SetLength(AnsiS, Length(Bytes));
  Move(Bytes[0], AnsiS[1], Length(Bytes));
  result := AnsiS;
end;

function HexStringToIdBytes(Str: string; var Bytes: TIdBytes): boolean;
var
  StrByte: string;
  Chr: Byte;
  Idx: integer;
begin
  if (Length(Str) mod 2) <> 0 then
  begin
    result := false;
    exit;
  end;
  SetLength(Bytes, Length(str) div 2);
  try
    Idx := 0;
    while Length(Str) <> 0 do
    begin
      StrByte := Str.Substring(0, 2);
      Str := Str.Substring(2);
      StrByte := '$' + StrByte;
      Chr := Byte(StrToInt(StrByte));
      Bytes[idx] := Chr;
      Inc(Idx);
    end;
    result := true;
  except
    on EConvertError do
    begin
      result := false;
      exit;
    end;
  end;
end;

//Base64 encoding and decoding functions, for OAuth1.
function IdBytesToBase64String(const Bytes: TIDBytes): string;
var
  Enc: TIdEncoderMime;
begin
  Enc := TIdEncoderMIME.Create(nil);
  try
    result := Enc.EncodeBytes(Bytes);
  finally
    Enc.Free;
  end;
end;

function Base64StringToIdBytes(Str:string; var Bytes: TIdBytes): boolean;
var
  Dec: TIdDecoderMime;
begin
  Dec := TIdDecoderMime.Create;
  try
    Bytes := Dec.DecodeBytes(Str);
  finally
    Dec.Free;
  end;
  result := true;
end;

end.
