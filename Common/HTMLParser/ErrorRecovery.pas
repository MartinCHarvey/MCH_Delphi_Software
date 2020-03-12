unit ErrorRecovery;
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
interface

uses
  Classes, HTMLEscapeHelper
{$IFDEF USE_TRACKABLES}
    , Trackables
{$ENDIF}
    ;

type
  TLexingMode = (lmScript, lmCData, lmInSingleString, lmInDoubleString);

{$IFDEF USE_TRACKABLES}

  THTMLErrorRecovery = class(TTrackable)
{$ELSE}
  THTMLErrorRecovery = class
{$ENDIF}
  private
  protected
    function FindScriptEndInternal(Stream: TStream; ParsedOK: int64; DocType: THTMLDocType;
      var LexingMode: TLexingMode; var ActualScriptEnd: int64): boolean;
  public
    function FindScriptEnd(Stream: TStream; ParsedOK: int64; DocType: THTMLDocType;
      var ActualScriptEnd: int64): boolean;
  end;

implementation

uses SoftLexer, SysUtils;

type
  TSoftTokens = (stInvalid = TOK_INVALID, stHTMLEndScript, stCDataStart, stCDataEnd, stSingleQuote,
    stDoubleQuote, stSingleQuoteEscape, stDoubleQuoteEscape);

function THTMLErrorRecovery.FindScriptEnd(Stream: TStream; ParsedOK: int64; DocType: THTMLDocType;
  var ActualScriptEnd: int64): boolean;
var
  LexingMode: TLexingMode;
  Decrement: int64;
begin
  {
    Little bit of a hack here: some syntax errors only occur after
    the parse tree has unwound, which means that we should really go back a little
    way in the input before trying to resync, otherwise we can miss some HTML
    that we should take into account
  }
  Decrement := ParsedOK div 2;
  if Decrement > 256 then
    Decrement := 256;
  Dec(ParsedOK, Decrement);

  LexingMode := lmScript;
  result := FindScriptEndInternal(Stream, ParsedOK, DocType, LexingMode, ActualScriptEnd);
  if (not result) and ((LexingMode = lmInSingleString) or (LexingMode = lmInDoubleString)) then
  begin
    // Malformed string in script, so lexer will probably start in a string.
    result := FindScriptEndInternal(Stream, ParsedOK, DocType, LexingMode, ActualScriptEnd);
  end;
end;

// TODO - Enhance lexers to allow whitespace in tags
// in error recovery case.
function THTMLErrorRecovery.FindScriptEndInternal(Stream: TStream; ParsedOK: int64;
  DocType: THTMLDocType; var LexingMode: TLexingMode; var ActualScriptEnd: int64): boolean;

  function LclUpperCase(Str: AnsiString): AnsiString;
  begin
    result := AnsiString(UpperCase(String(Str)));
  end;

var
  Lexer: TAnsiSoftLexer;
  ErrMsg: string;
  AChar: AnsiChar;
  TokenResults: TList;
  AnsiTokenResult: TAnsiTokenResult;
  idx: integer;
  SoftToken: TSoftTokens;
  EndScriptString: AnsiString;
begin
  result := false;
  Lexer := TAnsiSoftLexer.Create;
  try
    case DocType of
      tdtCSS:
        begin
          EndScriptString := '</style>';
          Lexer.AddATokenString(EndScriptString, Ord(stHTMLEndScript));
          Lexer.AddATokenString(LclUpperCase(EndScriptString), Ord(stHTMLEndScript));
          Lexer.AddATokenString('<!--', Ord(stCDataStart));
          Lexer.AddATokenString('-->', Ord(stCDataEnd));
          Lexer.AddATokenString('''', Ord(stSingleQuote));
          Lexer.AddATokenString('"', Ord(stDoubleQuote));
          Lexer.AddATokenString('\"', Ord(stDoubleQuoteEscape));
          Lexer.AddATokenString('\''', Ord(stSingleQuoteEscape));
          // CSS identifiers etc cannot be made to to look like end script tags,
          // so OK, but allow HTML like comments, which we treat as CDATA.
        end;
      // TODO - Handle style tag and different quoting rules.
      // TODO - State machine below basically only handles javascript.
      tdtJSON:
        begin
          EndScriptString := '</script>';
          Lexer.AddATokenString(EndScriptString, Ord(stHTMLEndScript));
          Lexer.AddATokenString(LclUpperCase(EndScriptString), Ord(stHTMLEndScript));
          // JSON does not allow inline HTML comments or CDATA,
          // Arguable whether we should include it for error recovery purposes.
          Lexer.AddATokenString('"', Ord(stDoubleQuote));
          Lexer.AddATokenString('\"', Ord(stDoubleQuoteEscape));
          // JSON only allows double quoted strings, not single ones.
          // Arguable whether we should include it for error reocvery purposes.
        end;
      tdtJScript:
        begin
          EndScriptString := '</script>';
          Lexer.AddATokenString(EndScriptString, Ord(stHTMLEndScript));
          Lexer.AddATokenString(LclUpperCase(EndScriptString), Ord(stHTMLEndScript));
          Lexer.AddATokenString('<![CDATA[[', Ord(stCDataStart));
          Lexer.AddATokenString('<!]]>>', Ord(stCDataEnd));
          Lexer.AddATokenString('''', Ord(stSingleQuote));
          Lexer.AddATokenString('"', Ord(stDoubleQuote));
          Lexer.AddATokenString('\"', Ord(stDoubleQuoteEscape));
          Lexer.AddATokenString('\''', Ord(stSingleQuoteEscape));
          // JS cannot be confused with end script tags, but
          // embedded CDATA and strings need to be streated differently.
          // Have not considered regexps in error recovery case.
        end
    else
      result := false;
    end;
    if not Lexer.SetupLexer(ErrMsg) then
      exit;
    Assert(ParsedOK < Stream.Size);
    Stream.Seek(ParsedOK, soFromBeginning);
    ActualScriptEnd := Stream.Size;
    while (ParsedOK < Stream.Size) and not result do
    begin
      Stream.Read(AChar, sizeof(AChar));
      Inc(ParsedOK);
      TokenResults := nil;
      if not Lexer.InputAChar(AChar, TokenResults) then
        exit;
      if Assigned(TokenResults) then
      begin
        for idx := 0 to Pred(TokenResults.Count) do
        begin
          AnsiTokenResult := TObject(TokenResults[idx]) as TAnsiTokenResult;
          SoftToken := TSoftTokens(AnsiTokenResult.TokenTag);
          case LexingMode of
            lmScript:
              case SoftToken of
                stInvalid:
                  ; // Do nothing
                stHTMLEndScript:
                  begin
                    ActualScriptEnd := ParsedOK - Length(EndScriptString);
                    result := true;
                    break;
                  end;
                stCDataStart:
                  LexingMode := lmCData;
                stCDataEnd:
                  ; // Do nothing.
                stSingleQuote, stSingleQuoteEscape:
                  LexingMode := lmInSingleString;
                stDoubleQuote, stDoubleQuoteEscape:
                  LexingMode := lmInDoubleString;
              else
                Assert(false);
                result := false;
                exit;
              end;
            lmCData:
              case SoftToken of
                stInvalid, stHTMLEndScript, stCDataStart, // Don't think we can nest.
                stSingleQuote, stSingleQuoteEscape, stDoubleQuote, stDoubleQuoteEscape:
                  ; // Do nothing
                stCDataEnd:
                  LexingMode := lmScript;
              else
                Assert(false);
                result := false;
                exit;
              end;
            lmInSingleString:
              case SoftToken of
                stInvalid, stHTMLEndScript, stCDataStart, stCDataEnd:
                  ; // Do nothing
                stSingleQuote:
                  LexingMode := lmScript;
                stSingleQuoteEscape, stDoubleQuote, stDoubleQuoteEscape:
                  ; // Do nothing
              else
                Assert(false);
                result := false;
                exit;
              end;
            lmInDoubleString:
              case SoftToken of
                stInvalid, stHTMLEndScript, stCDataStart, stCDataEnd:
                  ; // Do nothing
                stDoubleQuoteEscape, stSingleQuote, stSingleQuoteEscape:
                  ; // Do nothing
                stDoubleQuote:
                  LexingMode := lmScript;
              else
                Assert(false);
                result := false;
                exit;
              end;
          else
            Assert(false);
            result := false;
            exit;
          end;
        end;
        for idx := 0 to Pred(TokenResults.Count) do
          TObject(TokenResults[idx]).Free;
        TokenResults.Free;
      end;
    end;
  finally
    Lexer.Free;
  end;
end;

end.
