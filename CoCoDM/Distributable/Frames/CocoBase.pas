unit CocoBase;
{

Copyright � 2020 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the �Software�), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}
{Base components for Coco/R for Delphi grammars for use with version 1.3}

{$DEFINE REMOVE_DEPRECATED}

interface

uses
  Classes, SysUtils;

const
  setsize = 16; { sets are stored in 16 bits }

  { Standard Error Types }
  etSyntax = 0;
  etSymantic = 1;

  chCR = #13;
  chLF = #10;
  chNull = #0;
  chSpace = #32;
  chTab = #9;
  chEOL = chCR + chLF;  { End of line characters for Microsoft Windows }
  chLineSeparator = chCR;

type
  ECocoBookmark = class(Exception);
  TCocoStatusType = (cstInvalid, cstBeginParse, cstEndParse, cstLineNum, cstString);
  TCocoError = class(TObject)
  private
    FErrorCode : integer;
    FCol : integer;
    FLine : integer;
    FPos: Int64;
    FData : AnsiString;
    FErrorType : integer;
    function FixXmlStr(const Str : AnsiString) : AnsiString;
  public
    function ExportToXMLFragment(
        const ErrorTypeDesc : AnsiString;
        const ErrorText : AnsiString;
        const ErrorSeverity : AnsiString) : AnsiString;

    property ErrorType : integer read FErrorType write FErrorType;
    property ErrorCode : integer read FErrorCode write FErrorCode;
    property Line : integer read FLine write FLine;
    property Col : integer read FCol write FCol;
    property Pos : Int64 read FPos write FPos;
    property Data : AnsiString read FData write FData;
  end; {TCocoError}

  TCommentItem = class(TObject)
  private
    fComment: AnsiString;
    fLine: integer;
    fColumn: integer;
  public
    property Comment : AnsiString read fComment write fComment;
    property Line : integer read fLine write fLine;
    property Column : integer read fColumn write fColumn;
  end; {TCommentItem}

  TCommentList = class(TObject)
  private
    fList : TList;

    function FixComment(const S : AnsiString) : AnsiString;
    function GetComments(Idx: integer): AnsiString;
    procedure SetComments(Idx: integer; const Value: AnsiString);
    function GetCount: integer;
    function GetText: AnsiString;
    function GetColumn(Idx: integer): integer;
    function GetLine(Idx: integer): integer;
    procedure SetColumn(Idx: integer; const Value: integer);
    procedure SetLine(Idx: integer; const Value: integer);
  public
    constructor Create;
    destructor Destroy; override;

    procedure Clear;
    procedure Add(const S : AnsiString; const aLine : integer; const aColumn : integer);
    property Comments[Idx : integer] : AnsiString read GetComments write SetComments; default;
    property Line[Idx : integer] : integer read GetLine write SetLine;
    property Column[Idx : integer] : integer read GetColumn write SetColumn;
    property Count : integer read GetCount;
    property Text : AnsiString read GetText;
  end; {TCommentList}

  TSymbolPosition = class(TObject)
  private
    fLine : int64;
    fCol : int64;
    fLen : int64;
    fPos : Int64;
  public
    procedure Clear;
    procedure Assign(Source : TSymbolPosition);

    property Line : int64 read fLine write fLine; {line of symbol}
    property Col : int64 read fCol write fCol; {column of symbol}
    property Len : int64 read fLen write fLen; {length of symbol}
    property Pos : int64 read fPos write fPos; {file position of symbol}
  end; {TSymbolPosition}

  TGenListType = (glNever, glAlways, glOnError);

  TBitSet = set of 0..15;
  PStartTable = ^TStartTable;
  TStartTable = array[0..255] of integer;
  TCharSet = set of AnsiChar;

  TAfterGenListEvent = procedure(Sender : TObject;
    var PrintErrorCount : boolean) of object;
  TAfterGrammarGetEvent = procedure(Sender : TObject;
    var CurrentInputSymbol : integer) of object;
  TCommentEvent = procedure(Sender : TObject; CommentList : TCommentList) of object;
  TCustomErrorEvent = function(Sender : TObject; const ErrorCode : longint;
    const Data : AnsiString) : AnsiString of object;
  TErrorEvent = procedure(Sender : TObject; Error : TCocoError) of object;
  TErrorProc = procedure(const ErrorCode : integer; const Symbol : TSymbolPosition;
    const Data : AnsiString; const ErrorType : integer) of object;
  TFailureEvent = procedure(Sender : TObject; NumErrors : integer) of object;
  TGetCH = function(pos : int64) : AnsiChar of object;
  TStatusUpdateProc = procedure(Sender : TObject;
      const StatusType : TCocoStatusType;
      const Status : AnsiString;
      const LineNum : integer) of object;
  TFunctionSymbolPosition = function : TSymbolPosition of object;

  TCocoRScanner = class(TObject)
  private
    FbpCurrToken : Int64; {position of current token)}
    FBufferPosition : Int64; {current position in buf }
    FContextLen : integer; {length of appendix (CONTEXT phrase)}
    FCurrentCh : TGetCH; {procedural variable to get current input character}
    FCurrentSymbol : TSymbolPosition; {position of the current symbol in the source stream}
    FCurrInputCh : AnsiChar; {current input character}
    FCurrLine : int64; {current input line (may be higher than line)}
    FLastInputCh : AnsiChar; {the last input character that was read}
    FNextSymbol : TSymbolPosition; {position of the next symbol in the source stream}
    FNumEOLInComment : integer; {number of _EOLs in a comment}
    FOnStatusUpdate : TStatusUpdateProc;
    FScannerError : TErrorProc;
    FSourceLen : Int64; {source file size}
    FSrcStream : TMemoryStream; {source memory stream}
    FStartOfLine : int64;
    fLastSymbol: TSymbolPosition;

    function GetNStr(Symbol : TSymbolPosition; ChProc : TGetCh) : AnsiString;
    function ExtractBookmarkChar(var aBookmark: AnsiString): AnsiChar;
  protected
    FStartState : TStartTable; {start state for every character}

    function Bookmark : AnsiString; virtual;
    procedure GotoBookmark(aBookmark : AnsiString); virtual;

    function CapChAt(pos : int64) : AnsiChar;
    procedure Get(var sym : integer); virtual; abstract;
    procedure NextCh; virtual; abstract;

    function GetStartState : PStartTable;
    procedure SetStartState(aStartTable : PStartTable);

    property bpCurrToken : int64 read fbpCurrToken write fbpCurrToken;
    property BufferPosition : int64 read fBufferPosition write fBufferPosition;
    property LastSymbol : TSymbolPosition read fLastSymbol write fLastSymbol;
    property CurrentSymbol : TSymbolPosition read fCurrentSymbol write fCurrentSymbol;
    property NextSymbol : TSymbolPosition read fNextSymbol write fNextSymbol;
    property ContextLen : integer read fContextLen write fContextLen;
    property CurrentCh : TGetCh read fCurrentCh write fCurrentCh;
    property CurrInputCh : AnsiChar read fCurrInputCh write fCurrInputCh;
    property CurrLine : int64 read fCurrLine write fCurrLine;
    property LastInputCh : AnsiChar read fLastInputCh write fLastInputCh;
    property NumEOLInComment : integer read fNumEOLInComment write fNumEOLInComment;
    property OnStatusUpdate : TStatusUpdateProc read FOnStatusUpdate write FOnStatusUpdate;
    property ScannerError : TErrorProc read FScannerError write FScannerError;
    property SourceLen : int64 read fSourceLen write fSourceLen;
    property SrcStream : TMemoryStream read fSrcStream write fSrcStream;
    property StartOfLine : int64 read fStartOfLine write fStartOfLine;
    property StartState : PStartTable read GetStartState write SetStartState;
  public
    constructor Create;
    destructor Destroy; override;

    function CharAt(pos : Int64) : AnsiChar;
    function GetName(Symbol : TSymbolPosition) : AnsiString; // Retrieves name of symbol of length len at position pos in source file
    function GetString(Symbol : TSymbolPosition) : AnsiString; // Retrieves exact AnsiString of max length len from position pos in source file
    procedure _Reset;
  end; {TCocoRScanner}

  TCocoRGrammar = class(TComponent)
  private
    fAfterGet: TAfterGrammarGetEvent;
    FAfterGenList : TAfterGenListEvent;
    FAfterParse : TNotifyEvent;
    FBeforeGenList : TNotifyEvent;
    FBeforeParse : TNotifyEvent;
    fClearSourceStream : boolean;
    FErrDist : integer; // number of symbols recognized since last error
    FErrorList : TList;
    fGenListWhen : TGenListType;
    FListStream : TMemoryStream;
    FOnCustomError : TCustomErrorEvent;
    FOnError : TErrorEvent;
    FOnFailure : TFailureEvent;
    FOnStatusUpdate : TStatusUpdateProc;
    FOnSuccess : TNotifyEvent;
    FScanner : TCocoRScanner;
    FSourceFileName : AnsiString;
    fExtra : integer;
    FStreamPartRead: Int64;

    function GetSourceStream : TMemoryStream;
    procedure SetOnStatusUpdate(const Value : TStatusUpdateProc);
    procedure SetSourceStream(const Value : TMemoryStream);
    function GetLineCount: int64;
    function GetCharacterCount: int64;
  protected
    fCurrentInputSymbol : integer; // current input symbol
    FParseResult: TObject;
    FParseTracker: TObject;
    FLangStr: AnsiString;

    function Bookmark : AnsiString; virtual;
    procedure GotoBookmark(aBookmark : AnsiString); virtual;
    function GetSuccessful : boolean; virtual;

    procedure ClearErrors;
    procedure Expect(n : integer);
    procedure GenerateListing;
    procedure Get; virtual; abstract;
    procedure PrintErr(line : AnsiString; ErrorCode, col : integer;
      Data : AnsiString);
    procedure StoreError(const nr : integer; const Symbol : TSymbolPosition;
      const Data : AnsiString; const ErrorType : integer);
    function CurrentBufferPosition : int64;

    procedure DoAfterParse; virtual;
    procedure DoBeforeParse; virtual;

    property ClearSourceStream : boolean read fClearSourceStream write fClearSourceStream default true;
    property CurrentInputSymbol : integer read fCurrentInputSymbol write fCurrentInputSymbol;
    property ErrDist : integer read fErrDist write fErrDist; // number of symbols recognized since last error
    property Extra : integer read fExtra write fExtra;
    property GenListWhen : TGenListType read fGenListWhen write fGenListWhen default glOnError;
    property ListStream : TMemoryStream read FListStream write FListStream;
    property SourceFileName : AnsiString read FSourceFileName write FSourceFileName;

    {Events}
    property AfterParse : TNotifyEvent read fAfterParse write fAfterParse;
    property AfterGenList : TAfterGenListEvent read fAfterGenList write fAfterGenList;
    property AfterGet : TAfterGrammarGetEvent read fAfterGet write fAfterGet;
    property BeforeGenList : TNotifyEvent read fBeforeGenList write fBeforeGenList;
    property BeforeParse : TNotifyEvent read fBeforeParse write fBeforeParse;
    property OnCustomError : TCustomErrorEvent read FOnCustomError write FOnCustomError;
    property OnError : TErrorEvent read fOnError write fOnError;
    property OnFailure : TFailureEvent read FOnFailure write FOnFailure;
    property OnStatusUpdate : TStatusUpdateProc read FOnStatusUpdate write SetOnStatusUpdate;
    property OnSuccess : TNotifyEvent read FOnSuccess write FOnSuccess;
  public
    constructor Create(AOwner : TComponent); override;
    destructor Destroy; override;

    {$IFNDEF REMOVE_DEPRECATED}
    procedure _StreamLine(const s : AnsiString); deprecated;
    procedure _StreamLn(const s : AnsiString); deprecated;
    {$ENDIF REMOVE_DEPRECATED}

    procedure Execute; virtual; abstract;

    function ErrorStr(const ErrorCode : integer; const Data : AnsiString) : AnsiString; virtual; abstract;
    procedure GetLine(var pos : Integer; var line : AnsiString;
      var eof : boolean);
    function LastName: AnsiString;
    function LastString: AnsiString;
    function LexName : AnsiString;
    function LexString : AnsiString;
    function LookAheadName : AnsiString;
    function LookAheadString : AnsiString;
    procedure StreamToListFile(s : AnsiString; const AddEndOfLine : boolean);
    procedure SemError(const errNo : integer; const Data : AnsiString);
    procedure SynError(const errNo : integer);
    procedure FatalSynError(const errNo: integer);

    property SourceStream : TMemoryStream read GetSourceStream write SetSourceStream;
    property Successful : boolean read GetSuccessful;
    property ParseResult: TObject read FParseResult write FParseResult;
    property ParseTracker: TObject read FParseTracker write FParseTracker;
    property StreamPartRead: Int64 read FStreamPartRead write FStreamPartRead;

    property Scanner : TCocoRScanner read fScanner write fScanner;
    property LineCount : int64 read GetLineCount;
    property CharacterCount : int64 read GetCharacterCount;
    property ErrorList : TList read FErrorList write FErrorList;
    property LangStr: AnsiString read FLangStr;
  end; {TCocoRGrammar}

const
  _EF = chNull;
  _TAB = #09;
  _CR = chCR;
  _LF = chLF;
  _EL = _CR;
  _EOF = #26; {MS-DOS eof}
  LineEnds : TCharSet = [_CR, _LF, _EF];
  { not only for errors but also for not finished states of scanner analysis }
  minErrDist = 2; { minimal distance (good tokens) between two errors }

function PadL(S : AnsiString; ch : AnsiChar; L : integer) : AnsiString;
function StrTok(
    var Text : AnsiString;
    const ch : AnsiChar) : AnsiString;

implementation

uses
  TypInfo;

const
  INVALID_CHAR = 'Invalid Coco/R for Delphi bookmark character';
  INVALID_INTEGER = 'Invalid Coco/R for Delphi bookmark integer';
  BOOKMARK_STR_SEPARATOR = ' ';

function PadL(S : AnsiString; ch : AnsiChar; L : integer) : AnsiString;
var
  i : integer;
begin
  for i := 1 to L - (Length(s)) do
    s := ch + s;
  Result := s;
end; {PadL}

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

{ TSymbolPosition }

procedure TSymbolPosition.Assign(Source: TSymbolPosition);
begin
  fLine := Source.fLine;
  fCol := Source.fCol;
  fLen := Source.fLen;
  fPos := Source.fPos;
end; {Assign}

procedure TSymbolPosition.Clear;
begin
  fLen := 0;
  fPos := 0;
  fLine := 0;
  fCol := 0;
end; { Clear }

{ TCocoRScanner }

function TCocoRScanner.Bookmark: AnsiString;
begin
  Result := AnsiString(IntToStr(bpCurrToken) + BOOKMARK_STR_SEPARATOR
      + IntToStr(BufferPosition) + BOOKMARK_STR_SEPARATOR
      + IntToStr(ContextLen) + BOOKMARK_STR_SEPARATOR
      + IntToStr(CurrLine) + BOOKMARK_STR_SEPARATOR
      + IntToStr(NumEOLInComment) + BOOKMARK_STR_SEPARATOR
      + IntToStr(StartOfLine) + BOOKMARK_STR_SEPARATOR
      + IntToStr(LastSymbol.Line) + BOOKMARK_STR_SEPARATOR
      + IntToStr(LastSymbol.Col) + BOOKMARK_STR_SEPARATOR
      + IntToStr(LastSymbol.Len) + BOOKMARK_STR_SEPARATOR
      + IntToStr(LastSymbol.Pos) + BOOKMARK_STR_SEPARATOR
      + IntToStr(CurrentSymbol.Line) + BOOKMARK_STR_SEPARATOR
      + IntToStr(CurrentSymbol.Col) + BOOKMARK_STR_SEPARATOR
      + IntToStr(CurrentSymbol.Len) + BOOKMARK_STR_SEPARATOR
      + IntToStr(CurrentSymbol.Pos) + BOOKMARK_STR_SEPARATOR
      + IntToStr(NextSymbol.Line) + BOOKMARK_STR_SEPARATOR
      + IntToStr(NextSymbol.Col) + BOOKMARK_STR_SEPARATOR
      + IntToStr(NextSymbol.Len) + BOOKMARK_STR_SEPARATOR
      + IntToStr(NextSymbol.Pos) + BOOKMARK_STR_SEPARATOR
      + CurrInputCh
      + LastInputCh);
end; {Bookmark}

function TCocoRScanner.ExtractBookmarkChar(var aBookmark : AnsiString) : AnsiChar;
begin
  if length(aBookmark) > 0 then
    Result := aBookmark[1]
  else
    Raise ECocoBookmark.Create(INVALID_CHAR);
end; {ExtractBookmarkChar}

procedure TCocoRScanner.GotoBookmark(aBookmark: AnsiString);
var
  BookmarkToken : AnsiString;
begin
  try
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    bpCurrToken := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    BufferPosition := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    ContextLen := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    CurrLine := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    NumEOLInComment := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    StartOfLine := StrToInt(BookmarkToken);

    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    LastSymbol.Line := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    LastSymbol.Col := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    LastSymbol.Len := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    LastSymbol.Pos := StrToInt(BookmarkToken);

    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    CurrentSymbol.Line := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    CurrentSymbol.Col := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    CurrentSymbol.Len := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    CurrentSymbol.Pos := StrToInt(BookmarkToken);

    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    NextSymbol.Line := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    NextSymbol.Col := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    NextSymbol.Len := StrToInt(BookmarkToken);
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    NextSymbol.Pos := StrToInt(BookmarkToken);

    CurrInputCh := ExtractBookmarkChar(aBookmark);
    LastInputCh := ExtractBookmarkChar(aBookmark);
  except
    on EConvertError do
      Raise ECocoBookmark.Create(INVALID_INTEGER);
    else
      Raise;
  end;
end; {GotoBookmark}

constructor TCocoRScanner.Create;
begin
  inherited;
  fSrcStream := TMemoryStream.Create;
  LastSymbol := TSymbolPosition.Create;
  CurrentSymbol := TSymbolPosition.Create;
  NextSymbol := TSymbolPosition.Create;
end; {Create}

destructor TCocoRScanner.Destroy;
begin
  fSrcStream.Free;
  fSrcStream := NIL;
  LastSymbol.Free;
  LastSymbol := NIL;
  CurrentSymbol.Free;
  CurrentSymbol := NIL;
  NextSymbol.Free;
  NextSymbol := NIL;
  inherited;
end; {Destroy}

function TCocoRScanner.CapChAt(pos : int64) : AnsiChar;
begin
  Result := UpCase(CharAt(pos));
end; {CapCharAt}

function TCocoRScanner.CharAt(pos : int64) : AnsiChar;
var
  ch : AnsiChar;
begin
  if pos >= SourceLen then
  begin
    Result := _EF;
    exit;
  end;
  SrcStream.Seek(pos, soFromBeginning);
  SrcStream.Read(Ch, 1);
  if ch <> _EOF then
    Result := ch
  else
    Result := _EF
end; {CharAt}

function TCocoRScanner.GetNStr(Symbol : TSymbolPosition; ChProc : TGetCh) : AnsiString;
var
  i : integer;
  p : longint;
begin
  SetLength(Result, Symbol.Len);
  p := Symbol.Pos;
  i := 1;
  while i <= Symbol.Len do
  begin
    Result[i] := ChProc(p);
    inc(i);
    inc(p)
  end;
end; {GetNStr}

function TCocoRScanner.GetName(Symbol : TSymbolPosition) : AnsiString;
begin
  Result := GetNStr(Symbol, CurrentCh);
end; {GetName}

function TCocoRScanner.GetStartState : PStartTable;
begin
  Result := @fStartState;
end; {GetStartState}

procedure TCocoRScanner.SetStartState(aStartTable : PStartTable);
begin
  fStartState := aStartTable^;
end; {SetStartState}

function TCocoRScanner.GetString(Symbol : TSymbolPosition) : AnsiString;
begin
  Result := GetNStr(Symbol, CharAt);
end; {GetString}

procedure TCocoRScanner._Reset;
var
  len : longint;
begin
  CurrInputCh := _EF;
{
  Modified MCH.
  No longer insert _EF at end of stream; TCocoRScanner.CharAt checks lengths.
}
  SrcStream.Seek(0, soFromBeginning);

  LastInputCh := _EF;
  len := SrcStream.Size;
  SourceLen := len;
  CurrLine := 1;
  StartOfLine := -2;
  BufferPosition := -1;
  LastSymbol.Clear;
  CurrentSymbol.Clear;
  NextSymbol.Clear;
  NumEOLInComment := 0;
  ContextLen := 0;
  NextCh;
end; {_Reset}

{ TCocoRGrammar }

procedure TCocoRGrammar.ClearErrors;
var
  i : integer;
begin
  for i := 0 to fErrorList.Count - 1 do
    TCocoError(fErrorList[i]).Free;
  fErrorList.Clear;
end; {ClearErrors}

constructor TCocoRGrammar.Create(AOwner : TComponent);
begin
  inherited;
  FGenListWhen := glOnError;
  fClearSourceStream := true;
  fListStream := TMemoryStream.Create;
  fErrorList := TList.Create;
end; {Create}

destructor TCocoRGrammar.Destroy;
begin
  fListStream.Clear;
  fListStream.Free;
  ClearErrors;
  fErrorList.Free;
  inherited;
end; {Destroy}

procedure TCocoRGrammar.Expect(n : integer);
begin
  if CurrentInputSymbol = n then
    Get
  else
    SynError(n);
end; {Expect}

procedure TCocoRGrammar.GenerateListing;
  { Generate a source listing with error messages }
var
  i : integer;
  eof : boolean;
  lnr, errC : integer;
  srcPos : longint;
  line : AnsiString;
  PrintErrorCount : boolean;
begin
  if Assigned(BeforeGenList) then
    BeforeGenList(Self);
  srcPos := 0;
  GetLine(srcPos, line, eof);
  lnr := 1;
  errC := 0;
  while not eof do
  begin
    StreamToListFile(PadL(AnsiString(IntToStr(lnr)), ' ', 5) + '  ' + line, TRUE);
    for i := 0 to ErrorList.Count - 1 do
    begin
      if TCocoError(ErrorList[i]).Line = lnr then
      begin
        PrintErr(line, TCocoError(ErrorList[i]).ErrorCode,
          TCocoError(ErrorList[i]).Col,
          TCocoError(ErrorList[i]).Data);
        inc(errC);
      end;
    end;
    GetLine(srcPos, line, eof);
    inc(lnr);
  end;
  // Now take care of the last line.
  for i := 0 to ErrorList.Count - 1 do
  begin
    if TCocoError(ErrorList[i]).Line = lnr then
    begin
      PrintErr(line, TCocoError(ErrorList[i]).ErrorCode,
        TCocoError(ErrorList[i]).Col,
        TCocoError(ErrorList[i]).Data);
      inc(errC);
    end;
  end;
  PrintErrorCount := true;
  if Assigned(AfterGenList) then
    AfterGenList(Self, PrintErrorCount);
  if PrintErrorCount then
  begin
    StreamToListFile('', TRUE);
    StreamToListFile(PadL(AnsiString(IntToStr(errC)), ' ', 5) + ' error', FALSE);
    if errC <> 1 then
      StreamToListFile('s', TRUE);
  end;
end; {GenerateListing}

procedure TCocoRGrammar.GetLine(var pos : longint;
  var line : AnsiString;
  var eof : boolean);
  { Read a source line. Return empty line if eof }
var
  ch : AnsiChar;
  i : integer;
begin
  i := 1;
  eof := false;
  ch := Scanner.CharAt(pos);
  inc(pos);
  while not (ch in LineEnds) do
  begin
    SetLength(line, length(Line) + 1);
    line[i] := ch;
    inc(i);
    ch := Scanner.CharAt(pos);
    inc(pos);
  end;
  SetLength(line, i - 1);
  eof := (i = 1) and (ch = _EF);
  if ch = _CR then
  begin { check for MsDos end of lines }
    ch := Scanner.CharAt(pos);
    if ch = _LF then
    begin
      inc(pos);
      Extra := 0;
    end;
  end;
end; {GetLine}

function TCocoRGrammar.GetSourceStream : TMemoryStream;
begin
  Result := Scanner.SrcStream;
end; {GetSourceStream}

function TCocoRGrammar.GetSuccessful : boolean;
begin
  Result := ErrorList.Count = 0;
end; {GetSuccessful}

function TCocoRGrammar.LastName : AnsiString;
begin
  Result := Scanner.GetName(Scanner.LastSymbol)
end; {LastName}

function TCocoRGrammar.LastString : AnsiString;
begin
  Result := Scanner.GetString(Scanner.LastSymbol)
end; {LastString}

function TCocoRGrammar.LexName : AnsiString;
begin
  Result := Scanner.GetName(Scanner.CurrentSymbol)
end; {LexName}

function TCocoRGrammar.LexString : AnsiString;
begin
  Result := Scanner.GetString(Scanner.CurrentSymbol)
end; {LexString}

function TCocoRGrammar.LookAheadName : AnsiString;
begin
  Result := Scanner.GetName(Scanner.NextSymbol)
end; {LookAheadName}

function TCocoRGrammar.LookAheadString : AnsiString;
begin
  Result := Scanner.GetString(Scanner.NextSymbol)
end; {LookAheadString}

procedure TCocoRGrammar.PrintErr(line : AnsiString; ErrorCode : integer; col : integer; Data : AnsiString);
  { Print an error message }

  procedure DrawErrorPointer;
  var
    i : integer;
  begin
    StreamToListFile('*****  ', FALSE);
    i := 0;
    while i < col + Extra - 2 do
    begin
      if ((length(Line) > 0) and (length(Line) < i)) and (line[i] = _TAB) then
        StreamToListFile(_TAB, FALSE)
      else
        StreamToListFile(' ', FALSE);
      inc(i)
    end;
    StreamToListFile('^ ', FALSE)
  end; {DrawErrorPointer}

begin {PrintErr}
  DrawErrorPointer;
  StreamToListFile(ErrorStr(ErrorCode, Data), FALSE);
  StreamToListFile('', TRUE)
end; {PrintErr}

procedure TCocoRGrammar.SemError(const errNo : integer; const Data : AnsiString);
begin
  if errDist >= minErrDist then
    Scanner.ScannerError(errNo, Scanner.CurrentSymbol, Data, etSymantic);
  errDist := 0;
end; {SemError}

{$IFNDEF REMOVE_DEPRECATED}
procedure TCocoRGrammar._StreamLn(const s : AnsiString);
begin
  StreamToListFile(s, FALSE);
end; {_StreamLn}

procedure TCocoRGrammar._StreamLine(const s : AnsiString);
begin
  StreamToListFile(s, TRUE);
end; {_StreamLine}
{$ENDIF REMOVE_DEPRECATED}

procedure TCocoRGrammar.StreamToListFile(s: AnsiString; const AddEndOfLine : boolean);
begin
  if AddEndOfLine then
    s := s + chEOL;
  if length(s) > 0 then
    ListStream.WriteBuffer(s[1], length(s));
end; {StreamToListFile}

procedure TCocoRGrammar.SynError(const errNo : integer);
begin
  if errDist >= minErrDist then
    Scanner.ScannerError(errNo, Scanner.NextSymbol, '', etSyntax);
  errDist := 0;
end; {SynError}

procedure TCocoRGrammar.FatalSynError(const errNo: integer);
begin
  Scanner.ScannerError(errNo, Scanner.NextSymbol, '', etSyntax);
end;

procedure TCocoRGrammar.SetOnStatusUpdate(const Value : TStatusUpdateProc);
begin
  FOnStatusUpdate := Value;
  Scanner.OnStatusUpdate := Value;
end; {SetOnStatusUpdate}

procedure TCocoRGrammar.SetSourceStream(const Value : TMemoryStream);
begin
  Scanner.SrcStream := Value;
end; {SetSourceStream}

procedure TCocoRGrammar.StoreError(const nr : integer; const Symbol : TSymbolPosition;
  const Data : AnsiString; const ErrorType : integer);
  { Store an error message for later printing }
var
  Error : TCocoError;
begin
  Error := TCocoError.Create;
  Error.ErrorCode := nr;
  if Assigned(Symbol) then
  begin
    Error.Line := Symbol.Line;
    Error.Col := Symbol.Col;
    Error.Pos := Symbol.Pos;
  end
  else
  begin
    Error.Line := 0;
    Error.Col := 0;
  end;
  Error.Data := Data;
  Error.ErrorType := ErrorType;
  ErrorList.Add(Error);
  if Assigned(OnError) then
    OnError(self, Error);
end; {StoreError}

function TCocoRGrammar.GetLineCount: int64;
begin
  Result := Scanner.CurrLine;
end; {GetLineCount}

function TCocoRGrammar.GetCharacterCount: int64;
begin
  Result := Scanner.BufferPosition;
end; {GetCharacterCount}

procedure TCocoRGrammar.DoBeforeParse;
begin
  if Assigned(fBeforeParse) then
    fBeforeParse(Self);
  if Assigned(fOnStatusUpdate) then
    fOnStatusUpdate(Self, cstBeginParse, '', -1);
end; {DoBeforeParse}

procedure TCocoRGrammar.DoAfterParse;
begin
  if Assigned(fOnStatusUpdate) then
    fOnStatusUpdate(Self, cstEndParse, '', -1);
  if Assigned(fAfterParse) then
    fAfterParse(Self);
end; {DoAfterParse}

function TCocoRGrammar.Bookmark: AnsiString;
begin
  Result :=
        AnsiString(IntToStr(fCurrentInputSymbol) + BOOKMARK_STR_SEPARATOR
      + Scanner.Bookmark);
end; {Bookmark}

procedure TCocoRGrammar.GotoBookmark(aBookmark: AnsiString);
var
  BookmarkToken : AnsiString;
begin
  try
    BookmarkToken := StrTok(aBookmark, BOOKMARK_STR_SEPARATOR);
    fCurrentInputSymbol := StrToInt(BookmarkToken);
    Scanner.GotoBookmark(aBookmark);
  except
    on EConvertError do
      Raise ECocoBookmark.Create(INVALID_INTEGER);
    else
      Raise;
  end;
end; {GotoBookmark}

function TCocoRGrammar.CurrentBufferPosition: int64;
begin
  Result := Scanner.BufferPosition - Scanner.NextSymbol.Len;
end; {CurrentBufferPosition}

{ TCommentList }

procedure TCommentList.Add(const S : AnsiString; const aLine : integer;
    const aColumn : integer);
var
  CommentItem : TCommentItem;
begin
  CommentItem := TCommentItem.Create;
  try
    CommentItem.Comment := FixComment(S);
    CommentItem.Line := aLine;
    CommentItem.Column := aColumn;
    fList.Add(CommentItem);
  except
    CommentItem.Free;
  end;
end; {Add}

procedure TCommentList.Clear;
var
  i : integer;
begin
  for i := 0 to fList.Count - 1 do
    TCommentItem(fList[i]).Free;
  fList.Clear;
end; {Clear}

constructor TCommentList.Create;
begin
  fList := TList.Create;
end; {Create}

destructor TCommentList.Destroy;
begin
  Clear;
  if Assigned(fList) then
  begin
    fList.Free;
    fList := NIL;
  end;
  inherited;
end; {Destroy}

function TCommentList.FixComment(const S: AnsiString): AnsiString;
begin
  Result := S;
  while (length(Result) > 0) AND (Result[length(Result)] < #32) do
    Delete(Result,Length(Result),1);
end; {FixComment}

function TCommentList.GetColumn(Idx: integer): integer;
begin
  Result := TCommentItem(fList[Idx]).Column;
end; {GetColumn}

function TCommentList.GetComments(Idx: integer): AnsiString;
begin
  Result := TCommentItem(fList[Idx]).Comment;
end; {GetComments}

function TCommentList.GetCount: integer;
begin
  Result := fList.Count;
end; {GetCount}

function TCommentList.GetLine(Idx: integer): integer;
begin
  Result := TCommentItem(fList[Idx]).Line;
end; {GetLine}

function TCommentList.GetText: AnsiString;
var
  i : integer;
begin
  Result := '';
  for i := 0 to Count - 1 do
  begin
    Result := Result + Comments[i];
    if i < Count - 1 then
      Result := Result + chEOL;
  end;
end; {GetText}

procedure TCommentList.SetColumn(Idx: integer; const Value: integer);
begin
  TCommentItem(fList[Idx]).Column := Value;
end; {SetColumn}

procedure TCommentList.SetComments(Idx: integer; const Value: AnsiString);
begin
  TCommentItem(fList[Idx]).Comment := Value;
end; {SetComments}

procedure TCommentList.SetLine(Idx: integer; const Value: integer);
begin
  TCommentItem(fList[Idx]).Line := Value;
end; {SetLine}

{ TCocoError }

function TCocoError.ExportToXMLFragment(
    const ErrorTypeDesc : AnsiString;
    const ErrorText : AnsiString;
    const ErrorSeverity : AnsiString): AnsiString;
begin
  Result := AnsiString('<Error'
      + ' Severity="' + ErrorSeverity + '"'
      + ' Type="' + IntToStr(ErrorType) + '"'
      + ' Description="' + FixXmlStr(ErrorTypeDesc) + '"'
      + ' Code="' + IntToStr(ErrorCode) + '"'
      + ' Text="' + FixXmlStr(ErrorText) + '"'
      + ' Line="' + IntToStr(Line) + '"'
      + ' Col="' + IntToStr(Col) + '"'
      + ' Data="' + FixXmlStr(Data) + '"'
      + ' />');
end; {ExportToXMLFragment}

function TCocoError.FixXmlStr(const Str: AnsiString): AnsiString;
begin
  Result := AnsiString(StringReplace(Str, '&', '&amp;', [rfReplaceAll, rfIgnoreCase])); // must be first
  Result := AnsiString(StringReplace(Result, '"', '&quot;', [rfReplaceAll, rfIgnoreCase]));
  Result := AnsiString(StringReplace(Result, '<', '&lt;', [rfReplaceAll, rfIgnoreCase]));
  Result := AnsiString(StringReplace(Result, '>', '&gt;', [rfReplaceAll, rfIgnoreCase]));
end; {FixXmlStr}

end.

