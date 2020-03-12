unit CRS;
{$INCLUDE CocoCD.inc}

{ CRS     Scanner Generation
  ===     ================================
  (1) WriteScanner generates the scanner source file
---------------------------------------------------------------- }

interface

uses
  CRA, CRT, Classes, CocoComment, CRTypes, CocoOptions, CocoDat, StreamTools;

const
  MaxSourceLineLength = 78;

type
  TPutSProc = procedure(S : AnsiString) of object;
  TLiteralNameArray = array [0..maxLiterals] of CRTName;
  TLiteralSymArray = array [0..maxLiterals] of integer;

  TStartTab = array [0..255] of integer;

  TScannerGen = class(TObject)
  private
    fOptions: TCocoOptions;
    fTableHandler: TTableHandler;
    fAutomaton: TAutomaton;
    fStartTab : TStartTab;
    fCocoData: TCocoData;
    fStreamTools: TStreamTools;

    procedure StreamState(Stream : TStream; s : integer; var FirstState : boolean;
        DoItAll : boolean);
    procedure GenHashAddStrings(Stream: TStream);
    procedure SortLiteralList(var LiteralNames: TLiteralNameArray;
      var LiteralSymbols: TLiteralSymArray; var NumLiterals : integer);
    procedure GenLiteralSupport(Stream: TStream);
    function SymbolConstant(SymNum: integer): AnsiString;
    function StripQuotes(const Name: AnsiString): AnsiString;
    function HashCompareFunction: AnsiString;
    function PrimaryHashFunction: AnsiString;
    function SecondaryHashFunction: AnsiString;
    function SymbolHomographType(SymNum: integer): THomographType;
    procedure GenEquals(Stream: TStream);
    procedure GenGetNextSymbolString(Stream: TStream);
    procedure GenGetNextToken(Stream: TStream);
    procedure GenLiteralStub(Stream: TStream);
    procedure GenCharInIgnoreSet(const Stream : TStream);
  public
    { CopyFramePart: "stopStr" must not contain "FileIO.EOL".
       "leftMarg" is in/out-parameter  --  it has to be set once by the
       calling program.    }
    procedure CopyFramePart(stopStr : AnsiString; var leftMarg : integer;
      var framIn, framOut : TEXT);
    procedure GenComment(Stream : TStream; CommentItem: CocoComment.TCommentItem);
    procedure GenLiteralSupportDecl(Stream: TStream);
    procedure GenLiterals(Stream : TStream);
    procedure GenCommentImplementation(Stream: TStream);
    procedure GetGetSyB(Stream : TStream; DoItAll : boolean);
    function InitScanner : boolean; { Emits the source code of the generated scanner using the frame file scanner.frm }
    procedure ScannerInit(Stream: TStream);

    property Options : TCocoOptions read fOptions write fOptions;
    property TableHandler : TTableHandler read fTableHandler write fTableHandler;
    property Automaton : TAutomaton read fAutomaton write fAutomaton;
    property CocoData : TCocoData read fCocoData write fCocoData;
    property StreamTools: TStreamTools read fStreamTools write fStreamTools;
  end; {TScannerGen}

implementation

uses
  CocoSwitch, Sets, SysUtils, CocoBase;

{ GenComment            Generate a procedure to scan comments
-------------------------------------------------------------------------}

procedure TScannerGen.GenComment(Stream : TStream; CommentItem : CocoComment.TCommentItem);

  procedure GenBody;
  var
    idx2: integer;
  begin
    fStreamTools.StreamLine('{GenBody}');
    fStreamTools.StreamLine('while true do');
    fStreamTools.StreamLine('begin');
    for idx2 := 1 to Length(CommentItem.Stop) do
    begin
      fStreamTools.StreamLn('if ');
      fStreamTools.StreamChCond(CommentItem.Stop[idx2]);
      fStreamTools.StreamLine(' then');
      fStreamTools.StreamLine('begin');
      if idx2 = Length(CommentItem.Stop) then
      begin
        fStreamTools.StreamLine('level := level -  1;');
        if fCocoData.CommentList.HasCommentWithOneEndChar then
          fStreamTools.StreamLine('NumEOLInComment := CurrLine - startLine;');
        fStreamTools.StreamLine('NextCh;');
        fStreamTools.StreamLine('CommentStr := CommentStr + CharAt(BufferPosition);');

        fStreamTools.StreamLine('if level = 0 then');
        fStreamTools.StreamLine('begin');
        fStreamTools.StreamLine('  Result := true;');
        if (crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0) then
          fStreamTools.StreamLine('fLastCommentList.Add(CommentStr, CommentLine, CommentColumn);');
        fStreamTools.StreamLine('  Exit;');
        fStreamTools.StreamLine('end;');
      end
      else
      begin
        fStreamTools.StreamLine('NextCh;');
        fStreamTools.StreamLine('CommentStr := CommentStr + CharAt(BufferPosition);');
      end;
    end;
    for idx2 := Length(CommentItem.Stop) downto 1 do
        fStreamTools.StreamLine('end');
    if CommentItem.Nested then
    begin
      fStreamTools.StreamLn('else');
      for idx2 := 1 to Length(CommentItem.Start) do
      begin
        fStreamTools.StreamLn(' if ');
        fStreamTools.StreamChCond(CommentItem.Start[idx2]);
        fStreamTools.StreamLine(' then');
        fStreamTools.StreamLine('begin');
        fStreamTools.StreamLine('NextCh;');
        fStreamTools.StreamLine('CommentStr := CommentStr + CharAt(BufferPosition);');
        if idx2 = Length(CommentItem.Start) then
          fStreamTools.StreamLine('level := level + 1;');
      end;
      for idx2 := Length(CommentItem.Start) downto 1 do
      begin
        fStreamTools.StreamLine('end');
      end;
    end;
    fStreamTools.StreamLine('else if CurrInputCh = _EF then');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLine('  Result := false;');
    if (crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0) then
      fStreamTools.StreamLine('fLastCommentList.Add(CommentStr, CommentLine, CommentColumn);');
    fStreamTools.StreamLine('  Exit;');
    fStreamTools.StreamLine('end');
    fStreamTools.StreamLine('else');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLine('  NextCh;');
    fStreamTools.StreamLine('  CommentStr := CommentStr + CharAt(BufferPosition);');
    fStreamTools.StreamLine('end;');
    fStreamTools.StreamLine('end; { WHILE TRUE }');
    fStreamTools.StreamLine('{/GenBody}');
  end;


var
  idx: integer;
begin
  fStreamTools.Stream := Stream;
  fStreamTools.StreamLine('Result := false;');
  for idx := 1 to Length(CommentItem.Start) do
  begin
    fStreamTools.StreamLn('if ');
    fStreamTools.StreamChCond(CommentItem.Start[idx]);
    fStreamTools.StreamLine(' then');
    fStreamTools.StreamLine('  begin');
    fStreamTools.StreamLine('NextCh;');
    fStreamTools.StreamLine('  CommentStr := CommentStr + CharAt(BufferPosition);');
  end;
  GenBody;
  for idx := Length(CommentItem.Start) downto 1 do
  begin
    if idx > 1 then
    begin
      fStreamTools.StreamLine('end');
      fStreamTools.StreamLine('else');
      fStreamTools.StreamLine('begin');
      fStreamTools.StreamLine('if (CurrInputCh = _CR) OR (CurrInputCh = _LF) then');
      fStreamTools.StreamLine('begin');
      fStreamTools.StreamLine('CurrLine := CurrLine - 1;');
      fStreamTools.StreamLine('StartOfLine := oldLineStart');
      fStreamTools.StreamLine('end;');
      fStreamTools.StreamLine('BufferPosition := BufferPosition - ' + IntToStr(Pred(idx)) + ';');
      fStreamTools.StreamLine('CurrInputCh := StartCommentCh;');
      fStreamTools.StreamLine('Result := false;');
      fStreamTools.StreamLine('end;');
    end
    else
    begin
      fStreamTools.StreamLine('end;');
    end;
  end;
end;

procedure TScannerGen.CopyFramePart(stopStr : AnsiString;
  var leftMarg : integer;
  var framIn, framOut : TEXT);
var
  ch, startCh : AnsiChar;
  slen, i, j : integer;
  temp : array[1..255] of AnsiChar;
begin
  if length(stopStr) > 0 then
    startCh := stopStr[1]
  else
    StartCh := chNull;
  Read(framIn, ch);
  slen := Length(stopStr);
  while not EOF(framin) do
  begin
    if (ch = chCR) OR (ch = chLF) then
      leftMarg := 0
    else
      inc(leftMarg);
    if ch = startCh then // check if stopString occurs
    begin
      i := 1;
      while (i < slen) and (ch = stopStr[i]) and not EOF(framin) do
      begin
        temp[i] := ch;
        inc(i); Read(framIn, ch)
      end;
      if (slen > 0) AND (ch = stopStr[i]) then
      begin
        dec(leftMarg);
        EXIT
      end;
      // found ==> exit , else continue
      for j := 1 to i - 1 do
        Write(framOut, temp[j]);
      Write(framOut, ch);
      inc(leftMarg, i);
    end
    else
      Write(framOut, ch);
    Read(framIn, ch)
  end;
end;

(* ImportSymConsts was in the original TurboPascal version but never used.
   It is kept here for posterity.
{ ImportSymConsts      Generates the import of the named symbol constants
-------------------------------------------------------------------------}
procedure TScannerGen.ImportSymConsts(leader : AnsiString; putS : TPutSProc);
var
  oldLen, pos : integer;
  cname : CRTName;
  gn : CRTGraphNode;
  sn : CRTSymbolNode;
  gramName : AnsiString;

  procedure PutImportSym;
  begin
    if pos + oldLen > MaxSourceLineLength then
    begin
      putS('$  ');
      pos := 2
    end;
    putS(cname);
    inc(pos, oldLen + 1);
      { This is not strictly correct, as the increase of 2 should be
         lower. I omitted it, because to separate it would be too
         complicated, and no unexpected side effects are likely, since it
         is only called again outside the loop - after which "pos" is not
         used again
      }
  end;

begin
  { ----- Import list of the generated Symbol Constants Module ----- }
  fTableHandler.GetNode(fTableHandler.Root, gn);
  fTableHandler.GetSym(gn.p1, sn);
  putS(leader);
  gramName := Copy(sn.name, 1, 7);
  putS(gramName);
  putS('G { Symbol Constants };$');
end;
*)

function TScannerGen.SymbolConstant(SymNum : integer) : AnsiString;
var
  sn : CRTSymbolNode;
begin
  fTableHandler.GetSym(SymNum, sn);
  if Length(sn.constant) > 0 then
    Result := sn.constant
  else
    Result := IntToStr(SymNum);
end; {SymbolConstant}

function TScannerGen.SymbolHomographType(SymNum : integer) : THomographType;
var
  sn : CRTSymbolNode;
begin
  fTableHandler.GetSym(SymNum, sn);
    Result := sn.HomographType;
end; {SymbolHomographType}

function TScannerGen.StripQuotes(const Name : AnsiString) : AnsiString;
begin
  Result := copy(Name,2,length(Name) - 2);
end; {StripQuotes}

{ GenHashAddStrings }
procedure TScannerGen.GenHashAddStrings(Stream : TStream);
var
  i : integer;
  DefaultSym : CRTSymbolNode;
  SymConst : AnsiString;
  NumLiterals : integer;
  LiteralNames : TLiteralNameArray;
  LiteralSymbols : TLiteralSymArray;
begin
  fStreamTools.Stream := Stream;
  DefaultSym := fTableHandler.GetDefaultSymbol;
  SortLiteralList(LiteralNames, LiteralSymbols, NumLiterals);
  for i := 0 to NumLiterals - 1 do
  begin
    SymConst := SymbolConstant(LiteralSymbols[i]);
    if SymbolHomographType(LiteralSymbols[i]) = htHomograph then
      fStreamTools.StreamS('fHashList.AddString('
          + AnsiQuotedStr(StripQuotes(LiteralNames[i]),#39)
          + ', ' + SymConst + ', ' + DefaultSym.Constant + ');$')
    else
      fStreamTools.StreamS('fHashList.AddString('
          + AnsiQuotedStr(StripQuotes(LiteralNames[i]),#39)
          + ', ' + SymConst + ', ' + SymConst + ');$')
  end;
end; {GenHashAddStrings}

{ GenLiterals           Generate CASE for the recognition of literals
-------------------------------------------------------------------------}

procedure TScannerGen.SortLiteralList(var LiteralNames : TLiteralNameArray;
  var LiteralSymbols : TLiteralSymArray; var NumLiterals : integer);
var
  i : integer;
  j : integer;
  sn : CRTSymbolNode;
begin
  {-- sort literal list}
  i := 0;
  NumLiterals := 0;
  while i <= fTableHandler.MaxT do
  begin
    fTableHandler.GetSym(i, sn);
    if sn.struct = CRTlitToken then
    begin
      j := NumLiterals - 1;
      while (j >= 0) and (sn.name < LiteralNames[j]) do
      begin
        LiteralNames[j + 1] := LiteralNames[j];
        LiteralSymbols[j + 1] := LiteralSymbols[j];
        dec(j)
      end;
      LiteralNames[j + 1] := sn.name;
      LiteralSymbols[j + 1] := i;
      inc(NumLiterals);
      if NumLiterals > maxLiterals then
        fTableHandler.Restriction(10, maxLiterals);
    end;
    inc(i)
  end;
end;

procedure TScannerGen.GenLiteralSupportDecl(Stream : TStream);
begin
  fStreamTools.Stream := Stream;
  fStreamTools.StreamS('function CharInIgnoreSet(const Ch : AnsiChar) : boolean;$');
  if fTableHandler.HasLiterals OR fTableHandler.HasLiteralTokens then
  begin
    fStreamTools.StreamS('procedure CheckLiteral(var Sym : integer);$');
    if crsUseHashFunctions IN fOptions.Switches.SwitchSet then
    begin
      fStreamTools.StreamS('function GetNextSymbolString: AnsiString;$');
      if fTableHandler.HomographCount > 0 then
        fStreamTools.StreamS('function GetNextToken(var Sym : integer): AnsiString;$');
    end
    else if fTableHandler.HasLiterals then
      fStreamTools.StreamS('function Equal(s : AnsiString) : boolean;$');
  end;
end;

procedure TScannerGen.GenGetNextSymbolString(Stream : TStream);
begin
  fStreamTools.Stream := Stream;
  fStreamTools.StreamS('function T-->Grammar<--Scanner.GetNextSymbolString: AnsiString;$');
  fStreamTools.StreamS('var$');
  fStreamTools.StreamS('  i: integer;$');
  fStreamTools.StreamS('  q: int64;$');
  fStreamTools.StreamS('begin$');
  fStreamTools.StreamS('  Result := ' + AnsiQuotedStr('',#39) + ';$');
  fStreamTools.StreamS('  i := 1;$');
  fStreamTools.StreamS('  q := bpCurrToken;$');
  fStreamTools.StreamS('  while i <= NextSymbol.Len do$');
  fStreamTools.StreamS('  begin$');
  fStreamTools.StreamS('    Result := Result + CurrentCh(q);$');
  fStreamTools.StreamS('    inc(q);$');
  fStreamTools.StreamS('    inc(i);$');
  fStreamTools.StreamS('  end;$');
  fStreamTools.StreamS('end; {GetNextSymbolString}$$');
end;  

procedure TScannerGen.GenEquals(Stream : TStream);
begin
  fStreamTools.Stream := Stream;
  fStreamTools.StreamS('function T-->Grammar<--Scanner.Equal(s : AnsiString) : boolean;$');
  fStreamTools.StreamS('var$');
  fStreamTools.StreamS('  i : integer;$');
  fStreamTools.StreamS('  q : int64;$');
  fStreamTools.StreamS('begin$');
  fStreamTools.StreamS('  if NextSymbol.Len <> Length(s) then$');
  fStreamTools.StreamS('  begin$');
  fStreamTools.StreamS('    Result := false;$');
  fStreamTools.StreamS('    EXIT$');
  fStreamTools.StreamS('  end;$');
  fStreamTools.StreamS('  i := 1;$');
  fStreamTools.StreamS('  q := bpCurrToken;$');
  fStreamTools.StreamS('  while i <= NextSymbol.Len do$');
  fStreamTools.StreamS('  begin$');
  fStreamTools.StreamS('    if CurrentCh(q) <> s[i] then$');
  fStreamTools.StreamS('    begin$');
  fStreamTools.StreamS('      Result := false;$');
  fStreamTools.StreamS('      EXIT;$');
  fStreamTools.StreamS('    end;$');
  fStreamTools.StreamS('    inc(i);$');
  fStreamTools.StreamS('    inc(q);$');
  fStreamTools.StreamS('  end;$');
  fStreamTools.StreamS('  Result := true$');
  fStreamTools.StreamS('end;  {Equal}$$');
end;

procedure TScannerGen.GenGetNextToken(Stream : TStream);
begin
  fStreamTools.Stream := Stream;
  fStreamTools.StreamS('function T-->Grammar<--Scanner.GetNextToken(var Sym : integer) : AnsiString;$');
  fStreamTools.StreamS('var$');
  fStreamTools.StreamS('  i: integer;$');
  fStreamTools.StreamS('  q: integer;$');
  fStreamTools.StreamS('  bpToken : integer;$');
  fStreamTools.StreamS('  BufferPos : integer;$');
  fStreamTools.StreamS('  LastChar,$');
  fStreamTools.StreamS('  CurrentChar : AnsiChar;$');
  fStreamTools.StreamS('  ContextLen : integer;$');
  fStreamTools.StreamS('  state : integer;$');
  fStreamTools.StreamS('  Len : integer;$');
  fStreamTools.StreamS('begin$');
  fStreamTools.StreamS('  Result := ' + AnsiQuotedStr('',#39) + ';$');
  fStreamTools.StreamS('  Len := 0;$');
  fStreamTools.StreamS('  bpToken := BufferPosition;$');
  fStreamTools.StreamS('  CurrentChar := CurrInputCh;$');
  fStreamTools.StreamS('  LastChar := LastInputCh;$');
  fStreamTools.StreamS('  BufferPos := BufferPosition;$');
  fStreamTools.StreamS('  try$');
  fStreamTools.StreamS('  while CharInIgnoreSet(CurrInputCh) do$');
  fStreamTools.StreamS('    NextCh;$');
  fStreamTools.StreamS('  bpToken := BufferPosition;$');
  fStreamTools.StreamS('  ContextLen := 0;$');
  fStreamTools.StreamS('  state := StartState[ORD(CurrInputCh)];$');
  fStreamTools.StreamS('  while true do$');
  fStreamTools.StreamS('  begin$');
  fStreamTools.StreamS('    NextCh;$');
  fStreamTools.StreamS('    Len := Len + 1;$');
  fStreamTools.StreamS('    if BufferPosition > SrcStream.Size then$');
  fStreamTools.StreamS('    begin$');
  fStreamTools.StreamS('      sym := EOFSYMB;$');
  fStreamTools.StreamS('      CurrInputCh := _EF;$');
  fStreamTools.StreamS('      BufferPosition := BufferPosition - 1;$');
  fStreamTools.StreamS('      exit$');
  fStreamTools.StreamS('    end;$');
  fStreamTools.StreamS('    case state of$');

  GetGetSyB(Stream, FALSE);

  fStreamTools.StreamS('    else$');
  fStreamTools.StreamS('      begin$');
  fStreamTools.StreamS('        sym := _noSym;$');
  fStreamTools.StreamS('        EXIT;$');
  fStreamTools.StreamS('      end; {else}$');
  fStreamTools.StreamS('      end; {case}$');
  fStreamTools.StreamS('    end; {while true}$');
  fStreamTools.StreamS('  finally$');
  fStreamTools.StreamS('    i := 1;$');
  fStreamTools.StreamS('    q := bpToken;$');
  fStreamTools.StreamS('    while i <= Len do$');
  fStreamTools.StreamS('    begin$');
  fStreamTools.StreamS('      Result := Result + CurrentCh(q);$');
  fStreamTools.StreamS('      inc(q);$');
  fStreamTools.StreamS('      inc(i);$');
  fStreamTools.StreamS('    end;$');
  fStreamTools.StreamS('    CurrInputCh := CurrentChar;$');
  fStreamTools.StreamS('    LastInputCh := LastChar;$');
  fStreamTools.StreamS('    BufferPosition := BufferPos;$');
  fStreamTools.StreamS('  end; {try..finally}$');
  fStreamTools.StreamS('end;$$');
end;

procedure TScannerGen.GenLiteralSupport(Stream : TStream);
begin
  if fTableHandler.HasLiterals OR fTableHandler.HasLiteralTokens then
  begin
    if crsUseHashFunctions IN fOptions.Switches.SwitchSet then
    begin
      GenGetNextSymbolString(Stream);
      if fTableHandler.HomographCount > 0 then
        GenGetNextToken(Stream);
    end
    else if fTableHandler.HasLiterals then
      GenEquals(Stream);
  end;
end; {GenLiteralSupport}

procedure TScannerGen.GenLiteralStub(Stream : TStream);
begin
  fStreamTools.Stream := Stream;
  GenLiteralSupport(Stream);
  fStreamTools.StreamS('procedure T-->Grammar<--Scanner.CheckLiteral(var Sym : integer);$');
  fStreamTools.StreamS('begin$');
  fStreamTools.StreamS('  {This space intentionally left blank}$');
  fStreamTools.StreamS('end; {CheckLiteral}$$');
end; {GenLiteralStub}

procedure TScannerGen.GenLiterals(Stream : TStream);
var
  i : integer;
  j : integer;
  NumLiterals : integer;
  LiteralNames : TLiteralNameArray;
  LiteralSymbols : TLiteralSymArray;
  ch : AnsiChar;
begin
  fStreamTools.Stream := Stream;
  GenCharInIgnoreSet(Stream);
  if (NOT fTableHandler.HasLiterals) AND (fTableHandler.HasLiteralTokens) then
    GenLiteralStub(Stream)
  else if fTableHandler.HasLiterals then
  begin
    GenLiteralSupport(Stream);
    fStreamTools.StreamS('procedure T-->Grammar<--Scanner.CheckLiteral(var Sym : integer);$');
    if crsUseHashFunctions IN fOptions.Switches.SwitchSet then
    begin
      fStreamTools.StreamS('var$');
      fStreamTools.StreamS('  SymId : integer;$');
      fStreamTools.StreamS('  DefaultSymId : integer;$');
      fStreamTools.StreamS('  aToken : AnsiString;$');
      if fTableHandler.HomographCount > 0 then
      begin
        fStreamTools.StreamS('  BookmarkStr : AnsiString;$');
        fStreamTools.StreamS('  aNextToken : AnsiString;$');
        fStreamTools.StreamS('  aNextSymbol : integer;$');
      end;
      fStreamTools.StreamS('begin$');
      (*********** NOT GetName for the first token *****************)
      fStreamTools.StreamS('  aToken := GetNextSymbolString;$');
      fStreamTools.StreamS('  if fHashList.Hash(aToken, SymId, DefaultSymId) then$');
      fStreamTools.StreamS('  begin$');
      if fTableHandler.HomographCount > 0 then
      begin
        fStreamTools.StreamS('    if (SymId <> DefaultSymId) AND Assigned(fOnHomograph) then$');
        fStreamTools.StreamS('    begin$');
        fStreamTools.StreamS('      BookmarkStr := Bookmark;$');
        fStreamTools.StreamS('      try$');
        fStreamTools.StreamS('        Get(aNextSymbol);$');
        fStreamTools.StreamS('        aNextToken := GetName(CurrentSymbol);$');
        fStreamTools.StreamS('      finally$');
        fStreamTools.StreamS('        GotoBookmark(BookmarkStr);$');
        fStreamTools.StreamS('      end;$');
        fStreamTools.StreamS('      sym := fOnHomograph(aToken, aNextToken, aNextSymbol, SymId, DefaultSymId)$');
        fStreamTools.StreamS('    end$');
        fStreamTools.StreamS('    else$');
      end;
      fStreamTools.StreamS('      sym := SymId;$');
      fStreamTools.StreamS('  end;$');
    end
    else
    begin
      SortLiteralList(LiteralNames, LiteralSymbols, NumLiterals);
      fStreamTools.StreamS('begin$');
      {-- print CASE statement}
      if NumLiterals <> 0 then
      begin
        fStreamTools.StreamS('case CurrentCh(bpCurrToken) of$');
        i := 0;
        while i < NumLiterals do
        begin
          ch := LiteralNames[i, 2]; {LiteralNames[i, 0] = quote}
          if i <> 0 then
            fStreamTools.StreamLine('');
          fStreamTools.StreamS('  ');
          fStreamTools.StreamC(ch);
          j := i;
          repeat
            if i = j then
              fStreamTools.StreamS(': if')
            else
            begin
              fStreamTools.StreamS('end$');
              fStreamTools.StreamS('else if')
            end;
            fStreamTools.StreamS(' Equal(');
            fStreamTools.StreamS1(LiteralNames[i]);
            fStreamTools.StreamS(') then$');
            fStreamTools.StreamSE(LiteralSymbols[i]);
            inc(i);
          until (i = NumLiterals) or (LiteralNames[i, 2] <> ch);
          fStreamTools.StreamS('end;$');
        end;
        fStreamTools.StreamLine('');
        fStreamTools.StreamS('else$');
        fStreamTools.StreamS('begin$');
        fStreamTools.StreamS('end$');
        fStreamTools.StreamS('end$')
      end;
    end;
    fStreamTools.StreamS('end; {CheckLiteral}$$');
  end;
end;

{ WriteState           Write the source text of a scanner state
-------------------------------------------------------------------------}

procedure TScannerGen.StreamState(
    Stream : TStream;
    s: integer;
    var FirstState: boolean;
    DoItAll : boolean);
var
  anAction : Action;
  first, ctxEnd : boolean;
  sn : CRTSymbolNode;
  endOf : integer;
  sset : CRTSet;
begin
  fStreamTools.Stream := Stream;
  endOf := fAutomaton.StateList[s].endOf;
  if (endOf > fTableHandler.MaxT) and (endOf <> CRTnoSym) then {pragmas have been moved}
    endOf := fTableHandler.MaxT + maxSymbols - endOf;
  if FirstState then
    FirstState := false;
  fStreamTools.StreamS('  ');
  fStreamTools.StreamI2(s, 2);
  fStreamTools.StreamS(': ');
  first := true;
  ctxEnd := fAutomaton.StateList[s].ctx;
  anAction := fAutomaton.StateList[s].firstAction;
  while anAction <> nil do
  begin
    if first then
    begin
      fStreamTools.StreamS('if ');
      first := false;
    end
    else
    begin
      fStreamTools.StreamS('end$');
      fStreamTools.StreamS('else if ');
    end;
    if anAction^.typ = CRTchart then
    begin
      fStreamTools.StreamChCond(AnsiChar(anAction^.sym))
    end
    else
    begin
      fTableHandler.GetClass(anAction^.sym, sset);
      fStreamTools.StreamRange(sset, 'CurrInputCh');
    end;
    fStreamTools.StreamS(' then$');
    fStreamTools.StreamS('begin$');
    if anAction^.target^.theState <> s then
    begin
      fStreamTools.StreamS('state := ');
      fStreamTools.StreamI(anAction^.target^.theState);
      fStreamTools.StreamS(';')
    end;
    if anAction^.tc = contextTrans then
    begin
      fStreamTools.StreamS('ContextLen := ContextLen + 1');
      ctxEnd := false
    end
    else if fAutomaton.StateList[s].ctx then
      fStreamTools.StreamS('ContextLen := 0');
    fStreamTools.StreamS(' $');
    anAction := anAction^.next
  end;
  if fAutomaton.StateList[s].firstAction <> nil then
  begin
    fStreamTools.StreamS('end$');
    fStreamTools.StreamS('else$');
  end;
  if endOf = CRTnoSym then
  begin
    fStreamTools.StreamS('begin$');
    fStreamTools.StreamS('  sym := _noSym;$');
  end
  else {final theState}
  begin
    fTableHandler.GetSym(endOf, sn);
    if ctxEnd then {cut appendix}
    begin
      fStreamTools.StreamS('begin$');
      fStreamTools.StreamS('  BufferPosition := BufferPosition - ContextLen - 1;$');
      fStreamTools.StreamS('  NextSymbol.Len := NextSymbol.Len - ContextLen;$');
      fStreamTools.StreamS('  NextCh;$')
    end;
    fStreamTools.StreamSE(endOf);
    if (sn.struct = CRTclassLitToken) AND DoItAll then
    begin
      fStreamTools.StreamS('CheckLiteral(sym);$')
    end;
  end;
  if ctxEnd then
  begin
    fStreamTools.StreamS('exit;$');
    fStreamTools.StreamS('end;$');
    fStreamTools.StreamS('end;$')
  end
  else
  begin
    fStreamTools.StreamS('exit;$');
    fStreamTools.StreamS('end;$');
  end;
end;

procedure TScannerGen.GenCommentImplementation(Stream : TStream);
var
  i : integer;
begin
  fStreamTools.Stream := Stream;
  if not Sets.IsIn(fTableHandler.IgnoredCharSet, ORD(chCR)) then
  begin
    fStreamTools.StreamS('if NumEOLInComment > 0 then$');
    fStreamTools.StreamS('begin$');
    fStreamTools.StreamS('  BufferPosition := BufferPosition - 1;$');
    fStreamTools.StreamS('  NumEOLInComment := NumEOLInComment - 1;$');
    fStreamTools.StreamS('  CurrInputCh := _CR;$');
    fStreamTools.StreamS('end;$')
  end;
  fStreamTools.StreamS('while CharInIgnoreSet(CurrInputCh) do$');
  fStreamTools.StreamS('  NextCh;$');
  if fCocoData.CommentList.Count > 0 then
  begin
    fStreamTools.StreamS('if (');
    for i := 0 to fCocoData.CommentList.Count - 1 do
    begin
      fStreamTools.StreamChCond(fCocoData.CommentList.Items[i].start[1]);
      if i < (fCocoData.CommentList.Count - 1) then
        fStreamTools.StreamS(' OR ');
    end;
    fStreamTools.StreamS(') AND Comment then goto __start_get;$');
  end;
end;

function TScannerGen.PrimaryHashFunction : AnsiString;
begin
  Result := fOptions.AGI.HashPrimary;
  if fTableHandler.IgnoreCase then
    Result := 'I' + Result;
end;

function TScannerGen.SecondaryHashFunction : AnsiString;
begin
  Result := fOptions.AGI.HashSecondary;
end;

function TScannerGen.HashCompareFunction : AnsiString;
begin
  if (crsUseSameTextCompare IN fOptions.Switches.SwitchSet)
      AND fTableHandler.IgnoreCase then
    Result := 'SameText'
  else
  begin
    Result := 'HashCompare';
    if fTableHandler.IgnoreCase then
      Result := 'I' + Result;
  end;
end;

procedure TScannerGen.ScannerInit(Stream : TStream);
var
  i, j : integer;
begin
  fStreamTools.Stream := Stream;
  if crsUseHashFunctions IN fOptions.Switches.SwitchSet then
  begin
    fStreamTools.StreamS('fHashList := TmwStringHashList.Create('
        + PrimaryHashFunction + ', ' + SecondaryHashFunction + ', '
        + HashCompareFunction + ');$');
    GenHashAddStrings(Stream);
  end;

  if ((crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0)) then
    fStreamTools.StreamS('fLastCommentList := TCommentList.Create;');

  if fTableHandler.IgnoreCase then
    fStreamTools.StreamS('CurrentCh := CapChAt;$')
  else
    fStreamTools.StreamS('CurrentCh := CharAt;$');
  i := 0;
  while i < 64 {PDT} do
  begin
    if (i <> 0) then
      fStreamTools.StreamS('$');
    j := 0;
    while j < 4 do
    begin
      fStreamTools.StreamS('fStartState[');
      fStreamTools.StreamI2(4 * i + j, 3);
      fStreamTools.StreamS('] := ');
      fStreamTools.StreamI2(fStartTab[4 * i + j], 2);
      fStreamTools.StreamS('; ');
      inc(j);
    end;
    inc(i);
  end;
  fStreamTools.StreamS('$');
end;

{ WriteScanner         Write the scanner source file
-------------------------------------------------------------------------}

function TScannerGen.InitScanner : boolean;

  procedure FillStartTab;
  var
    anAction : Action;
    i, targetState, undefState : integer;
    chrclass : CRTSet;
  begin
    undefState := fAutomaton.lastState + 2;
    fStartTab[0] := fAutomaton.lastState + 1; {eof}
    i := 1;
    while i < 256 {PDT} do
    begin
      fstartTab[i] := undefState;
      inc(i)
    end;
    anAction := fAutomaton.StateList[fAutomaton.rootState].firstAction;
    while anAction <> nil do
    begin
      targetState := anAction^.target^.theState;
      if anAction^.typ = CRTchart then
        fstartTab[anAction^.sym] := targetState
      else
      begin
        fTableHandler.GetClass(anAction^.sym, chrclass);
        i := 0;
        while i < 256 {PDT} do
        begin
          if Sets.IsIn(chrclass, i) then
            fstartTab[i] := targetState;
          inc(i)
        end
      end;
      anAction := anAction^.next
    end
  end;

begin
  if fTableHandler.CocoAborted then
  begin
    Result := FALSE;
    Exit;
  end
  else
    Result := TRUE;
  if fAutomaton.dirtyDFA then
    fAutomaton.MakeDeterministic(Result);
  FillStartTab;
end; {InitScanner}

procedure TScannerGen.GetGetSyB(Stream: TStream; DoItAll : boolean);
var
  s : integer;
  FirstState : boolean;
begin
  fStreamTools.Stream := Stream;
  s := fAutomaton.rootState + 1;
  FirstState := true;
  while s <= fAutomaton.lastState do
  begin
    StreamState(Stream, s, FirstState, DoItAll);
    inc(s)
  end;
  fStreamTools.StreamS('  ');
  fStreamTools.StreamI2(fAutomaton.lastState + 1, 2);
  fStreamTools.StreamS(': ');
  fStreamTools.StreamSE(0);
  fStreamTools.StreamS('CurrInputCh := chNull;$');
  fStreamTools.StreamS('BufferPosition := BufferPosition - 1;$');
  fStreamTools.StreamS('exit$');
  fStreamTools.StreamS('end;$');
end;

procedure TScannerGen.GenCharInIgnoreSet(const Stream : TStream);
begin
  fStreamTools.Stream := Stream;
  fStreamTools.StreamS('function T-->Grammar<--Scanner.CharInIgnoreSet(const Ch : AnsiChar) : boolean;$');
  fStreamTools.StreamS('begin$');

  fStreamTools.StreamS('Result := ');
  if NOT (crsSpaceAsWhitespace IN fOptions.Switches.SwitchSet)
      AND (Sets.Empty(fTableHandler.IgnoredCharSet)) then
    fStreamTools.StreamS('FALSE')
  else
  begin
    if crsSpaceAsWhitespace IN fOptions.Switches.SwitchSet then
      fStreamTools.StreamS('(Ch = ' + AnsiQuotedStr(' ',#39) + ')');
    if not Sets.Empty(fTableHandler.IgnoredCharSet) then
    begin
      if crsSpaceAsWhitespace IN fOptions.Switches.SwitchSet then
        fStreamTools.StreamS('    OR$');
      fStreamTools.StreamRange(fTableHandler.IgnoredCharSet, 'Ch');
    end;
  end;
  fStreamTools.StreamS(';$');
  fStreamTools.StreamS('end; {CharInIgnoreSet}$$');
end; {GenCharInIgnoreSet}

end.

