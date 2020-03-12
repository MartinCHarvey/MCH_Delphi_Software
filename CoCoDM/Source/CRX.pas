unit CRX;
{$INCLUDE CocoCD.inc}

{ CRX   Parser Generation
   ===   =================

   Uses the top-down graph and the computed sets of terminal start symbols
   from CRTable to generate recursive descent parsing procedures.

   Errors are reported by error numbers. The corresponding error messages
   are written to <grammar name>.err.

   ---------------------------------------------------------------------}

interface

uses
  CRTypes, CRT, CocoBase, Classes, StreamTools, CocoOptions;

const
  tErr = 0; { unmatched terminal symbol }
  altErr = 1; { unmatched alternatives }
  syncErr = 2; { error reported at synchronization point }

type
  TIndentProcType = procedure(i : integer) of object;

  TParserGen = class(TObject)
  private
    fLastErrorNum: integer;
    fmaxSS : integer;
    fOnSetGrammarName: TSetGrammarNameEvent;
    fOnGetGrammarName: TGetGrammarNameEvent;
    fTableHandler: TTableHandler;
    fOnAbortErrorMessage: TAbortErrorMessageEvent;
    fOnScannerError: TErrorProc;
    fOnStreamLine: TProcedureStreamLine;
    fOnGetCharAt: TGetCH;
    fCommentsExist: boolean;
    fAppendSemiColon: boolean;
    fOnExecuteFrameParser: TExecuteFrameParser;
    fStreamTools: TStreamTools;
    fOptions: TCocoOptions;

    procedure GenErrorMsg(errTyp, errSym: integer; var errNr: integer);
    procedure GenProcedureHeading(Stream : TStream; sn: CRTSymbolNode;
        ProcImp: boolean);
    function NewCondSet(newSet: CRTSet): integer;

    procedure GenCodeStream(Stream: TStream; gp: integer; checked: CRTSet);
    procedure GenCondStream(Stream: TStream; newSet: CRTSet);
    procedure CopySourcePartStream(Stream: TStream; pos: CRTPosition);
    procedure StreamInitSets(Stream: TStream);
    function ExpandErrorStr(const ErrorMsg, Desc: AnsiString): AnsiString;
  public
    procedure GenProductions(Stream : TStream);
    procedure GenParseRoot(Stream : TStream);
    procedure GenCompiler(ParserName : AnsiString; OutputDir : AnsiString); { Generates the target compiler (parser). }
    procedure GenParserInit(Stream: TStream);

    procedure Initialize;

    property LastErrorNum : integer read fLastErrorNum write fLastErrorNum; { number of last generated error message}
    property MaxSS : integer read fMaxSS write fMaxSS;
    function GenErrorList: AnsiString;

    property TableHandler : TTableHandler read fTableHandler write fTableHandler;
    property CommentsExist : boolean read fCommentsExist write fCommentsExist;
    property AppendSemiColon : boolean read fAppendSemiColon write fAppendSemicolon;
    property StreamTools: TStreamTools read fStreamTools write fStreamTools;
    property Options : TCocoOptions read fOptions write fOptions;

    property OnGetGrammarName : TGetGrammarNameEvent read fOnGetGrammarName
        write fOnGetGrammarName;
    property OnSetGrammarName : TSetGrammarNameEvent read fOnSetGrammarName
        write fOnSetGrammarName;
    property OnScannerError : TErrorProc read fOnScannerError write fOnScannerError;
    property OnAbortErrorMessage : TAbortErrorMessageEvent read fOnAbortErrorMessage write fOnAbortErrorMessage;
    property OnStreamLine : TProcedureStreamLine read fOnStreamLine write fOnStreamLine;
    property OnGetCharAt : TGetCH read fOnGetCharAt write fOnGetCharAt;
    property OnExecuteFrameParser : TExecuteFrameParser
        read fOnExecuteFrameParser write fOnExecuteFrameParser;
  end; {TParserGen}

implementation

uses
  Sets, SysUtils, CocoDefs, CocoSwitch;

const
  maxTerm = 5; { sets of size < maxTerm are enumerated }

var
  symSet : array[0..symSetSize] of CRTSet; { symbol sets in the generated parser }
  curSy : integer; { symbol whose production is currently generated }

{ CopySourcePartStream       Copy sequence <pos> from input file to Stream
----------------------------------------------------------------------}

procedure TParserGen.CopySourcePartStream(Stream : TStream; pos : CRTPosition);
var
  lastCh, ch : AnsiChar;
  extra, col, i : integer;
  bp : longint;
  nChars : longint;
begin
  if (NOT (crsDoNotGenSymanticActions IN fOptions.Switches.SwitchSet)) AND (pos.beg >= 0) then
  begin
    fStreamTools.Stream := Stream;
    bp := pos.beg;
    nChars := pos.len;
    col := pos.col - 1;
    ch := chSpace;
    extra := 0;
    while (nChars > 0) and ((ch = chSpace) or (ch = chTab)) do
    begin
      { skip leading white space }
      { skip leading blanks }
      ch := fOnGetCharAt(bp);
      inc(bp);
      dec(nChars);
      inc(col);
    end;
    while true do
    begin
      while (ch = chCR) or (ch = chLF) do
      begin
          { Write blank lines with the correct number of leading blanks }
        fStreamTools.StreamS('$'); //WriteLn(syn);
        lastCh := ch;
        if nChars > 0 then
        begin
          ch := fOnGetCharAt(bp);
          inc(bp);
          dec(nChars);
        end
        else
          Exit;
        if (ch = chLF) and (lastCh = chCR) then
        begin
          extra := 1
          { must be MS-DOS format };
          if nChars > 0 then
          begin
            ch := fOnGetCharAt(bp);
            inc(bp);
            dec(nChars);
          end
          else
            Exit;
        end;
        if (ch <> chCR) and (ch <> chLF) then
        { we have something on this line }
        begin
          i := col - 1 - extra;
          while ((ch = chSpace) or (ch = chTab)) and (i > 0) do
          begin
            { skip at most "col-1" white space chars at start of line }
            if nChars > 0 then
            begin
              ch := fOnGetCharAt(bp);
              inc(bp);
              dec(nChars);
            end
            else
              Exit;
            dec(i);
          end;
        end;
      end;
      { Handle extra blanks }
      i := 0;
      while ch = chSpace do
      begin
        if nChars > 0 then
        begin
          ch := fOnGetCharAt(bp);
          inc(bp);
          dec(nChars)
        end
        else
          Exit;
        inc(i);
      end;
      if (ch <> chCR) and (ch <> chLF) and (ch <> chNull) then
      begin
        if i > 0 then
          fStreamTools.StreamB(i);
        fStreamTools.StreamS(ch);
        if nChars > 0 then
        begin
          ch := fOnGetCharAt(bp);
          inc(bp);
          dec(nChars)
        end
        else
          Exit;
      end;
    end;
  end;
end;

function TParserGen.ExpandErrorStr(const ErrorMsg : AnsiString; const Desc : AnsiString) : AnsiString;
begin
  if Pos(#39,ErrorMsg) > 0 then
    Result := SysUtils.StringReplace(ErrorMsg, '''', '''''', [rfReplaceAll])
  else
    Result := ErrorMsg;
  Result := SysUtils.StringReplace(Result, '%D%', Desc, [rfReplaceAll]);
end; {ExpandErrorStr}

{ GenErrorMsg          Generate an error message and return its number
----------------------------------------------------------------------}

procedure TParserGen.GenErrorMsg(errTyp, errSym : integer; var errNr : integer);
var
  description : CRTName;
  sn : CRTSymbolNode;
  s : AnsiString;
begin
  inc(fLastErrorNum);
  errNr := fLastErrorNum;
  fTableHandler.GetSym(errSym, sn);
  if sn.ErrorDesc <> '' then
    description := copy(sn.ErrorDesc,2,length(sn.ErrorDesc) - 2)
  else
    description := sn.name;
  if Pos(#39,description) > 0 then
    description := SysUtils.StringReplace(description, '''', '''''', [rfReplaceAll]);
  s := PadL(IntToStr(errNr), ' ', 4) + ' : Result := '#39;
  case errTyp of
    tErr : s := s + ExpandErrorStr(fOptions.AGI.StringExpected, description);
    altErr : s := s + ExpandErrorStr(fOptions.AGI.StringInvalid, description);
    syncErr : s := s + ExpandErrorStr(fOptions.AGI.StringNotExpected, description);
  end;
  s := s + #39';';
  fTableHandler.ErrorList.Add(s);
end;

{ NewCondSet    Generate a new condition set, if set not yet exists
----------------------------------------------------------------------}

function TParserGen.NewCondSet(newSet : CRTSet) : integer;
var
  i : integer;
begin
  i := 1; {skip symSet[0]}
  while i <= fmaxSS do
  begin
    if Sets.Equal(newSet, symSet[i]) then
    begin
      Result := i;
      EXIT
    end;
    inc(i)
  end;
  inc(fmaxSS);
  if fmaxSS > symSetSize then
    fTableHandler.Restriction(5, symSetSize);
  symSet[fmaxSS] := newSet;
  Result := fmaxSS
end;

{ GenCond              Generate code to check if sym is in set
----------------------------------------------------------------------}

procedure TParserGen.GenCondStream(Stream : TStream; newSet : CRTSet);
var
  i, n : integer;

  function Small(s : CRTSet) : boolean;
  begin
    i := Sets.size;
    while i <= fTableHandler.MaxT do
    begin
      if Sets.IsIn(s, i) then
      begin
        Result := false;
        Exit;
      end;
      inc(i)
    end;
    Result := true
  end;

begin
  fStreamTools.Stream := Stream;
  n := Sets.Elements(newSet, i);
  if n = 0 then
    {TODO: Write a warning when we get here}
    fStreamTools.StreamS(' FALSE') {this branch should never be taken}
  else if n <= maxTerm then
  begin
    i := 0;
    while i <= fTableHandler.MaxT do
    begin
      if Sets.IsIn(newSet, i) then
      begin
        fStreamTools.StreamS(' (fCurrentInputSymbol = ');
        fStreamTools.StreamSN(i);
        fStreamTools.StreamS(')');
        dec(n);
        if n > 0 then
        begin
          fStreamTools.StreamS(' OR');
          fStreamTools.StreamS('$');
        end;
      end;
      inc(i)
    end
  end
  else if Small(newSet) then
  begin
    fStreamTools.StreamS(' (fCurrentInputSymbol < ');
    fStreamTools.StreamI2(Sets.size, 2);
    fStreamTools.StreamS(') { prevent range error } AND$');
    fStreamTools.StreamS(' (fCurrentInputSymbol IN [');
    fStreamTools.StreamBitSet(newSet[0], 0);
    fStreamTools.StreamS(']) ')
  end
  else
  begin
    fStreamTools.StreamS(' _In(symSet[');
    fStreamTools.StreamI(NewCondSet(newSet));
    fStreamTools.StreamS('], fCurrentInputSymbol)')
  end;
end;

{ GenCode              Generate code for graph gp in production curSy
----------------------------------------------------------------------}

procedure TParserGen.GenCodeStream(Stream : TStream; gp : integer; checked : CRTSet);
var
  gn, gn2 : CRTGraphNode;
  sn : CRTSymbolNode;
  gp2 : integer;
  s1, s2 : CRTSet;
  errNr, alts : integer; // indent1, addInd : integer;
  equal : boolean;
  NextIsIfStatement : boolean;
  HasSemantic : boolean;
begin
  fStreamTools.Stream := Stream;
  while gp > 0 do
  begin
    fTableHandler.GetNode(gp, gn);
    case gn.typ of
      CRTnt :
        begin
          fTableHandler.GetSym(gn.p1, sn);
          fStreamTools.StreamS('_');
          fStreamTools.StreamS(sn.name);
          if (NOT (crsDoNotGenSymanticActions IN fOptions.Switches.SwitchSet)) AND (gn.pos.beg >= 0) then
          begin
            fStreamTools.StreamS('(');
            CopySourcePartStream(Stream, gn.pos);
            { was      CopySourcePart(gn.pos, 0, IndentProc); ++++ }
            fStreamTools.StreamS(')')
          end;
          fStreamTools.StreamS(';$')
        end;
      CRTt :
        begin
          fTableHandler.GetSym(gn.p1, sn);
          if Sets.IsIn(checked, gn.p1) then
            fStreamTools.StreamS('Get;$')
          else
          begin
            fStreamTools.StreamS('Expect(');
            fStreamTools.StreamSN(gn.p1);
            fStreamTools.StreamS(');$')
          end
        end;
      CRTwt :
        begin
          fTableHandler.CompExpected(ABS(gn.next), curSy, s1);
          fTableHandler.GetSet(0, s2);
          Sets.Unite(s1, s2);
          fTableHandler.GetSym(gn.p1, sn);
          fStreamTools.StreamS('ExpectWeak(');
          fStreamTools.StreamSN(gn.p1);
          fStreamTools.StreamS(', ');
          fStreamTools.StreamI(NewCondSet(s1));
          fStreamTools.StreamS(');$')
        end;
      CRTany :
        begin
          fStreamTools.StreamS('Get;$')
        end;
      CRTeps :
        { nothing }
        begin
        end;
      CRTsem :
        begin
          CopySourcePartStream(Stream, gn.pos);
          if fAppendSemicolon then
            fStreamTools.StreamS(';$')
          else
            fStreamTools.StreamS('$')
        end;
      CRTsync :
        begin
          fTableHandler.GetSet(gn.p1, s1);
          GenErrorMsg(syncErr, curSy, errNr);
          fStreamTools.StreamS('while not (');
          GenCondStream(Stream, s1);
          fStreamTools.StreamS(') do begin SynError(');
          fStreamTools.StreamI(errNr);
          fStreamTools.StreamS('); Get; end;$')
        end;
      CRTalt :
        begin
          fTableHandler.CompFirstSet(gp, s1, false);
          equal := Sets.Equal(s1, checked);
          alts := fTableHandler.Alternatives(gp);
          if alts > fTableHandler.MaxAlternates then
          begin
            fStreamTools.StreamS('case fCurrentInputSymbol of$');
          end;
          gp2 := gp;
          while gp2 <> 0 do
          begin
            fTableHandler.GetNode(gp2, gn2);
            fTableHandler.CompExpected(gn2.p1, curSy, s1);
            NextIsIfStatement := fTableHandler.NextPointerLeadsToIfStatement(
                gn2.p1, HasSemantic);
            if alts > fTableHandler.MaxAlternates then
            begin
              fStreamTools.StreamS('  ');
              fStreamTools.StreamSet(s1);
              fStreamTools.StreamS(' : begin$');
            end
            else if gp2 = gp then
            begin
              NextIsIfStatement := fTableHandler.NextPointerLeadsToIfStatement(
                  gn2.p1, HasSemantic);
              if NOT NextIsIfStatement then
              begin
                fStreamTools.StreamS('if');
                GenCondStream(Stream, s1);
                fStreamTools.StreamS(' then begin$')
              end;
            end
            else if (gn2.p2 = 0) and equal then
            begin
              fStreamTools.StreamS('end else begin$');
            end
            else
            begin
              fStreamTools.StreamS('end else ');
              if NOT NextIsIfStatement then
              begin
                fStreamTools.StreamS('if');
                GenCondStream(Stream, s1);
                fStreamTools.StreamS(' then begin$')
              end;
            end;
            Sets.Unite(s1, checked);
            GenCodeStream(Stream, gn2.p1, s1);
            if (NOT NextIsIfStatement) AND (alts > fTableHandler.MaxAlternates) then
            begin
              fStreamTools.StreamS('    end;$');
            end;
            gp2 := gn2.p2;
          end;
          if not equal then
          begin
            GenErrorMsg(altErr, curSy, errNr);
            if not (alts > fTableHandler.MaxAlternates) then
            begin
              fStreamTools.StreamS('end ');
            end;
            fStreamTools.StreamS('else begin SynError(');
            fStreamTools.StreamI(errNr);
            fStreamTools.StreamS(');$');
            if alts > fTableHandler.MaxAlternates then
            begin
              fStreamTools.StreamS('    end;$');
            end;
          end;
          fStreamTools.StreamS('end;$');
        end;
      CRTiter :
        begin
          fTableHandler.GetNode(gn.p1, gn2);
          fStreamTools.StreamS('while');
          if gn2.typ = CRTwt then
          begin
            fTableHandler.CompExpected(ABS(gn2.next), curSy, s1);
            fTableHandler.CompExpected(ABS(gn.next), curSy, s2);
            fTableHandler.GetSym(gn2.p1, sn);
            fStreamTools.StreamS(' WeakSeparator(');
            fStreamTools.StreamSN(gn2.p1);
            fStreamTools.StreamS(', ');
            fStreamTools.StreamI(NewCondSet(s1));
            fStreamTools.StreamS(', ');
            fStreamTools.StreamI(NewCondSet(s2));
            fStreamTools.StreamS(')');
            Sets.Clear(s1);
            {for inner structure}
            if gn2.next > 0 then
              gp2 := gn2.next
            else
              gp2 := 0;
          end
          else
          begin
            gp2 := gn.p1;
            fTableHandler.CompFirstSet(gp2, s1, false);
            GenCondStream(Stream, s1)
          end;
          fStreamTools.StreamS(' do begin$');
          GenCodeStream(Stream, gp2, s1);
          fStreamTools.StreamS('end;$');
        end;
      CRTopt :
        begin
          fTableHandler.CompFirstSet(gn.p1, s1, false);
          fTableHandler.GetNode(gn.p1, gn2);
          if Sets.Equal(checked, s1) then
            GenCodeStream(Stream,gn.p1, checked)
          else
          begin
////            NextIsIfStatement := fTableHandler.NextPointerLeadsToIfStatement(
////                gn2.p1, HasSemantic);
////            if NOT NextIsIfStatement then
////            begin
              fStreamTools.StreamS('if');
              GenCondStream(Stream, s1);
              fStreamTools.StreamS(' then begin$');
////            end;
            GenCodeStream(Stream, gn.p1, s1);
////            if NOT NextIsIfStatement then
              fStreamTools.StreamS('end;$');
          end
        end;
      CRTif :
        begin
          fStreamTools.StreamS('if ');
          fStreamTools.StreamS(gn.BooleanFunction);
          fStreamTools.StreamS(' then begin$');
        end;
      CRTendsc :
        begin
          fStreamTools.StreamS('end;$');
        end;
      CRTend :
        begin
          fStreamTools.StreamS('end$');
        end;
      CRTelse :
        begin
          fStreamTools.StreamS('else begin$');
        end;
    end;
    if (gn.typ <> CRTeps) and (gn.typ <> CRTsem) and (gn.typ <> CRTsync) then
      Sets.Clear(checked);
    gp := gn.next;
  end; { WHILE gp > 0 }
end;

{ GenProcedureHeading  Generate procedure heading
----------------------------------------------------------------------}

procedure TParserGen.GenProcedureHeading(Stream : TStream; sn : CRTSymbolNode;
    ProcImp : boolean);
begin
  fStreamTools.Stream := Stream;
  if not ProcImp then
    fStreamTools.StreamS('    ');
  fStreamTools.StreamS('procedure ');
  if ProcImp AND Assigned(fOnGetGrammarName) then
    fStreamTools.StreamS('T' + fOnGetGrammarName + '.');
  fStreamTools.StreamS('_');
  fStreamTools.StreamS(sn.name);
  if (NOT (crsDoNotGenSymanticActions IN fOptions.Switches.SwitchSet)) AND (sn.attrPos.beg >= 0) then
  begin
    fStreamTools.StreamS(' (');
    CopySourcePartStream(Stream, sn.attrPos);
    fStreamTools.StreamS(')')
  end;
  fStreamTools.StreamS(';')
end;

{ GenProductions       Generate code for all productions
----------------------------------------------------------------------}

procedure TParserGen.GenProductions(Stream : TStream);
var
  sn : CRTSymbolNode;
  checked : CRTSet;
begin
  fStreamTools.Stream := Stream;
  curSy := fTableHandler.FirstNt;
  while curSy <= fTableHandler.LastNt do
  begin { for all nonterminals }
    fTableHandler.GetSym(curSy, sn);
    GenProcedureHeading(Stream, sn, true);
    fStreamTools.StreamS('');
    if sn.semPos.beg >= 0 then
    begin
      CopySourcePartStream(Stream, sn.semPos);
      fStreamTools.StreamS('$');
    end;
    fStreamTools.StreamS('begin$');
    Sets.Clear(checked);
    GenCodeStream(Stream, sn.struct, checked);

    if (curSy = fTableHandler.LastNt) AND fCommentsExist then
    begin
      fStreamTools.StreamS('if GetScanner.fLastCommentList.Count > 0 then$');
      fStreamTools.StreamS('begin$');
      fStreamTools.StreamS('  if Assigned(fInternalGrammarComment) then$');
      fStreamTools.StreamS('    fInternalGrammarComment(Self,GetScanner.fLastCommentList);$');
      fStreamTools.StreamS('  GetScanner.fLastCommentList.Clear;$');
      fStreamTools.StreamS('end;$');
    end;

    fStreamTools.StreamS('end;$$');
    inc(curSy);
  end;
end;

{ GenSetInits          Initialise all sets
----------------------------------------------------------------------}

procedure TParserGen.StreamInitSets(Stream : TStream);
var
  i, j : integer;
begin
  fStreamTools.Stream := Stream;
  fTableHandler.GetSet(0, symSet[0]);
  i := 0;
  while i <= fmaxSS do
  begin
    if i <> 0 then
      fStreamTools.StreamS('$');
    j := 0;
    while j <= fTableHandler.MaxT div Sets.size do
    begin
      if j <> 0 then
        fStreamTools.StreamS('$');
      fStreamTools.StreamS('symSet[');
      fStreamTools.StreamI2(i,2);
      fStreamTools.StreamS(', ');
      fStreamTools.StreamI(j);
      fStreamTools.StreamS('] := [');
      fStreamTools.StreamBitSet(symSet[i, j], j * Sets.size);
      fStreamTools.StreamS('];');
      inc(j);
    end;
    inc(i)
  end;
  fStreamTools.StreamS('$');
end;

procedure TParserGen.GenParseRoot(Stream: TStream);
var
  checked : CRTSet;
begin
  fStreamTools.Stream := Stream;
  fStreamTools.StreamS('GetScanner._Reset;$');
  fStreamTools.StreamS('Get;$');

  Sets.Clear(checked);
  GenCodeStream(Stream, fTableHandler.Root, checked);
end;

procedure TParserGen.GenParserInit(Stream : TStream);
begin
  StreamInitSets(Stream);
end;

function TParserGen.GenErrorList : AnsiString;
begin
  Result := fTableHandler.ErrorList.Text;
end;

{ GenCompiler          Generate the target compiler
----------------------------------------------------------------------}

procedure TParserGen.GenCompiler(ParserName : AnsiString; OutputDir : AnsiString);
var
  errNr : integer;
  i : integer;
  gn : CRTGraphNode;
  sn : CRTSymbolNode;
  gramName : AnsiString;
  fGramName : AnsiString;
  ParserFrame : AnsiString;
  Symbol : TSymbolPosition;
begin
  fTableHandler.ErrorList.Clear;
  for i := 0 to fTableHandler.MaxT do
    GenErrorMsg(tErr, i, errNr);

  if fTableHandler.CocoAborted then
    Exit;
  if ParserName <> '' then
    ParserFrame := ParserName
  else
    ParserFrame := Concat(fTableHandler.SrcDirectory, 'component.frm');

  fTableHandler.GetNode(fTableHandler.Root, gn);
  fTableHandler.GetSym(gn.p1, sn);
  gramName := sn.name;
  if OutputDir = '' then
    fGramName := Concat(fTableHandler.SrcDirectory, gramName)
  else
    fGramName := Concat(OutputDir, gramName);

  fGramName := ChangeFileExt(fGramName, '.PAS');

  if Assigned(fOnExecuteFrameParser) then
  begin
    if NOT fOnExecuteFrameParser(fGramName, gramName, ParserFrame) then
    begin
      fTableHandler.CocoAborted := true;
      fOnAbortErrorMessage('component not generated');
      Symbol := TSymbolPosition.Create;
        try
          fOnScannerError(152, Symbol, ' component (parser error - check '
              + lowercase(LST_FILE_EXT) + ' file)', etGeneration);
        finally
          Symbol.Free;
        end;
    end;
  end;
end; {GenCompiler}

procedure TParserGen.Initialize;
begin
  fLastErrorNum := -1;
  fmaxSS := 0; {symSet[0] reserved for allSyncSyms}
end;

end {CRX}.

