unit HTMLParseEvents;
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
  Trackables, HTMLGrammar, JSONGrammar, JScriptGrammar, CoCoBase, SysUtils,
  CommonNodes;

type
  //Error types first, then warnings?
  TParseEventType = ( //Errors below here.

                      pitInfo, //Not used, info types below here.
                      pitTagMatchInfo,

                      pwtWarning, //Not used, warning below here.
                      pwtParseWarning,
                      pwtRefWarning,

                      petError, //Not used, error types below here.
                      petException,
                      petHTMLParseError,
                      petHTMLBlockError,
                      petScriptParseError
                      );


  TParseEvent = class(TTrackable)
  private
    FCol, FLine: integer;
    FCode: integer;
    FLanguage: string;
    FEvType: TParseEventType;
    FStrData: string;
  public
    //Generalised event from various stages of parsing and document processing,
    //including (possibly) scripts.
    class function CreateFromHtmlParseError(Err: TCoCoError; Grammar: THTMLGrammar): TParseEvent;
    class function CreateFromHtmlBlockError(Col, Line: integer; ErrStr: string): TParseEvent;
    class function CreateFromSecondaryParseError(Err: TCoCoError; Grammar: TCoCORGrammar): TParseEvent;
    class function CreateFromParseWarning(Col, Line, Code: Integer; StrData: string; Grammar: TCoCORGrammar): TParseEvent;
    class function CreateFromTagMatchString(StrData: string): TParseEvent;
    class function CreateFromScriptReferencingErr(Col, Line: integer; StrData: string): TParseEvent;
    class function CreateFromException(E:Exception; Grammar: TCocoRGrammar): TParseEvent;
    property Col: integer read FCol write FCol;
    property Line: integer read FLine write FLine;
    property Code: integer read FCode write FCode;
    property EvType: TParseEventType read FEvType write FEvType;
    property StrData:string read FStrData write FStrData;
    property Language:string read FLanguage write FLanguage;
  end;

implementation

class function TParseEvent.CreateFromHtmlParseError(Err: TCocoError;  Grammar: THTMLGrammar): TParseEvent;
begin
  result := TParseEvent.Create;
  result.FCol := Err.Col;
  result.FLine := Err.Line;
  result.FCode := Err.ErrorCode;
  result.FEvType := petHTMLParseError;
  result.FStrData :=  Grammar.ErrorStr(Err.ErrorCode, '');
  result.FLanguage := Grammar.LangStr;
end;

class function TParseEvent.CreateFromHtmlBlockError(Col, Line: integer; ErrStr: string): TParseEvent;
begin
  result := TParseEvent.Create;
  result.FCol := Col;
  result.FLine := Line;
  result.FCode := 8888;;
  result.FEvType := petHTMLBlockError;
  result.FStrData :=  'Internal error: ' + ErrStr;
  result.FLanguage := 'HTML';
end;

class function TParseEvent.CreateFromSecondaryParseError(Err: TCoCoError; Grammar: TCoCORGrammar): TParseEvent;
begin
  result := TParseEvent.Create;
  result.FCol := Err.Col;
  result.FLine := Err.Line;
  result.FCode := Err.ErrorCode;
  result.FEvType := petScriptParseError;
  result.FStrData :=  Grammar.ErrorStr(Err.ErrorCode, '');
  result.FLanguage := Grammar.LangStr;
end;

class function TParseEvent.CreateFromParseWarning(Col, Line, Code: Integer; StrData: string; Grammar: TCoCORGrammar): TParseEvent;
begin
  result := TParseEvent.Create;
  result.FCol := Col;
  result.FLine := Line;
  result.FCode := Code;
  result.FEvType := pwtParseWarning;
  result.FStrData :=  StrData;
  result.FLanguage := Grammar.LangStr;
end;

class function TParseEvent.CreateFromTagMatchString(StrData: string): TParseEvent;
begin
  result := TParseEvent.Create;
  result.FEvType := pitTagMatchInfo;
  result.FStrData :=  StrData;
  result.FLanguage := 'HTML';
end;

class function TParseEvent.CreateFromScriptReferencingErr(Col, Line: integer; StrData: string): TParseEvent;
begin
  result := TParseEvent.Create;
  result.FEvType := pwtRefWarning;
  result.FCol := Col;
  result.FLine := Line;
  result.FStrData := 'HTML ' + StrData;
end;

class function TParseEvent.CreateFromException(E:Exception; Grammar: TCOCORGrammar): TParseEvent;
begin
  result := TParseEvent.Create;
  result.FEvType := petException;
  if E is EParseAbort then
  begin
    result.FLine := (E as EParseAbort).Line;
    result.FCol := (E as EParseAbort).Column;
    result.FStrData := Grammar.LangStr + ' ' + E.Message;
  end
  else
  begin
    result.FCol := 0;
    result.FLine := 0;
    result.FStrData := Grammar.LangStr + ' parser raised exception ' +
      E.ClassName + ':' + E.Message;
  end;
end;


end.
