unit FrameTools;
{$INCLUDE CocoCD.inc}

interface

uses
  CocoAGI, Classes, CRTypes, CRT, CocoOptions, CocoBase, CRS, CRX, CocoDat,
  StreamTools;

type
  TFrameTools = class(TObject)
  private
    fOptions: TCocoOptions;
    fOnGetCharAt: TGetCH;
    fGrammarName: AnsiString;
    fTableHandler: TTableHandler;
    fScannerGenerator: TScannerGen;
    fParserGen: TParserGen;
    fCocoData: TCocoData;
    fStreamTools: TStreamTools;
    procedure CopySourcePart(Stream: TStream; SrcPos: TCRTPosition;
      Indent: integer);
    function CopySourceStr(SrcPos: CRTPosition): AnsiString;
    function GenProcedureHeading(sn: CRTSymbolNode;
      ProcImp: boolean): AnsiString;
  public
    procedure ConsoleVersion(Stream : TStream);
    procedure VersionBtnDec(Stream : TStream);
    procedure VersionBtn(Stream : TStream);
    procedure VersionBtnDfm(Stream : TStream);
    procedure WriteConstants(Stream : TStream);
    procedure WriteSourcePart(Stream : TStream; SrcPos : TCRTPosition; Indent : integer);
    procedure WriteProductionsDec(Stream : TStream);
    procedure WriteConst(Stream : TStream);
    procedure GenPragmaCode(Stream : TStream; leftMarg : integer);
    procedure VersionMethods(Stream : TStream);
    procedure VersionProperties(Stream : TStream);
    procedure VersionString(Stream : TStream);
    procedure VersionImpl(Stream : TStream);
    procedure WriteWeakMethods(Stream : TStream);
    procedure WriteWeakImpl(Stream : TStream);
    procedure WriteAncestor(Stream : TStream);
    procedure WriteComponentComment(Stream : TStream);
    procedure WriteLiterals(Stream : TStream);
    procedure WriteLiteralSupportDecl(Stream : TStream);
    procedure WriteCommentImplementation(Stream : TStream);
    procedure WriteGetSyB(Stream : TStream);
    procedure WriteScannerInit(Stream : TStream);
    procedure WriteProductions(Stream : TStream);
    procedure WriteParseRoot(Stream : TStream);
    procedure WriteParserInit(Stream : TStream);
    procedure WriteCleanup(Stream : TStream; GrammarName : AnsiString);
    procedure WriteScannerDestroyDecl(Stream : TStream);
    procedure WriteScannerDestroyImpl(Stream : TStream);
    procedure WriteScannerHashField(Stream : TStream);
    procedure WriteOnHomographEvent(Stream : TStream);
    procedure WriteOnHomographProperty(Stream : TStream);
    procedure WriteOnHomographField(Stream : TStream);
    procedure WriteGrammarGetComment(Stream : TStream);
    procedure WriteGrammarCommentField(Stream : TStream);
    procedure WriteGrammarCommentProperty(Stream : TStream);
    procedure WriteScannerCommentField(Stream : TStream);
    procedure WriteScannerComment(Stream : TStream);

    function GetMemoType : AnsiString;
    function GetMemoProperties : AnsiString;

    function GetGrammarVersion : AnsiString;
    function GetGrammarVersionLastBuild : AnsiString;
    function GetGrammarVersionInfo : AnsiString;

    property Options : TCocoOptions read fOptions write fOptions;
    property GrammarName : AnsiString read fGrammarName write fGrammarName;
    property TableHandler : TTableHandler read fTableHandler write fTableHandler;
    property ParserGen : TParserGen read fParserGen write fParserGen;
    property ScannerGenerator : TScannerGen read fScannerGenerator write fScannerGenerator;
    property CocoData : TCocoData read fCocoData write fCocoData;
    property StreamTools: TStreamTools read fStreamTools write fStreamTools;

    property OnGetCharAt : TGetCH read fOnGetCharAt write fOnGetCharAt;
end; {TFrameTools}

implementation

uses
  StringTools, SysUtils, CocoSwitch;

{ TFrameTools }

function TFrameTools.CopySourceStr(SrcPos : CRTPosition) : AnsiString;
var
  i : integer;
  ch : AnsiChar;
begin
  Result := '';
  for i := SrcPos.beg to SrcPos.beg + SrcPos.Len - 1 do
  begin
    ch := fOnGetCharAt(i);
    Result := Result + ch;
  end;
end; {CopySourceStr}

procedure TFrameTools.CopySourcePart(Stream : TStream; SrcPos : TCRTPosition; Indent : integer);
var
  i : integer;
  ch : AnsiChar;
begin
  for i := SrcPos.StartText to SrcPos.StartText + SrcPos.TextLength - 1 do
  begin
    ch := fOnGetCharAt(i);
    Stream.WriteBuffer(ch, 1);
  end;
end;

procedure TFrameTools.ConsoleVersion(Stream : TStream);
begin
  fStreamTools.Stream := Stream;
  if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
  begin
    fStreamTools.StreamLine('  Writeln('#39'  - '#39' + ' + fGrammarName + '1.Version');
    fStreamTools.StreamLine('    + FormatDateTime('#39' (ddddd t)'#39','
      + fGrammarName + '1.BuildDate));');
   if fOptions.AGI.VersionInfo.Count > 0 then
      fStreamTools.StreamLine('  Writeln('#39'    '#39' + '
        + fGrammarName + '1.VersionInfo);');
  end
  else
    fStreamTools.StreamLine('  Writeln;');
end;

procedure TFrameTools.VersionBtnDec(Stream : TStream);
begin
  fStreamTools.Stream := Stream;
  if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
  begin
    fStreamTools.StreamLine('    btnVersion: TButton;');
    fStreamTools.StreamLine('    procedure btnVersionClick(Sender: TObject);');
  end;
end;

procedure TFrameTools.VersionBtn(Stream : TStream);
begin
  if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('procedure TfmTest' + fGrammarName + '.btnVersionClick(Sender: TObject);');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLn('  MessageDlg('#39);
    fStreamTools.StreamLine(fGrammarName + ' - '#39' + '
        + fGrammarName + '.VersionStr + #13#10');
    if fOptions.AGI.StringLastBuildFormat <> '' then
      fStreamTools.StreamLn('      + FormatDateTime('
          + AnsiQuotedStr(' (' + fOptions.AGI.StringLastBuildFormat + ')',#39) + ','
          + fGrammarName + '.BuildDate)')
    else
      fStreamTools.StreamLn(' + ' + AnsiQuotedStr('(',#39) + '+ DateTimeToStr('
          + fGrammarName + '.BuildDate) + ' + AnsiQuotedStr(')',#39));

    if fOptions.AGI.VersionInfo.Count > 0 then
    begin
      fStreamTools.StreamLine('');
      fStreamTools.StreamLn('      + #13#10#13#10 + '
          + fGrammarName + '.VersionInfo');
    end;

    fStreamTools.StreamLine(',mtInformation,[mbOk],0);');
    fStreamTools.StreamLine('end;');
  end;
end;

procedure TFrameTools.VersionBtnDfm(Stream : TStream);
begin
  if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('    object btnVersion: TButton');
    fStreamTools.StreamLine('      Left = 275');
    fStreamTools.StreamLine('      Hint = '#39 + fGrammarName + ' Version'#39);
    fStreamTools.StreamLine('      Top = 2');
    fStreamTools.StreamLine('      Width = 75');
    fStreamTools.StreamLine('      Height = 25');
    fStreamTools.StreamLine('      Caption = '#39'&Version'#39);
    fStreamTools.StreamLine('      TabOrder = 3');
    fStreamTools.StreamLine('      ParentShowHint = False');
    fStreamTools.StreamLine('      ShowHint = True');
    fStreamTools.StreamLine('      OnClick = btnVersionClick');
    fStreamTools.StreamLine('    end');
  end;
end;

procedure TFrameTools.WriteConstants(Stream : TStream);
begin
  fStreamTools.Stream := Stream;
  fStreamTools.StreamLine('maxT = ' + IntToStr(fTableHandler.MaxT) + ';');
  if fTableHandler.MaxP > fTableHandler.MaxT then
    fStreamTools.StreamLine('maxP = ' + IntToStr(fTableHandler.MaxP) + ';');
end;

procedure TFrameTools.WriteSourcePart(Stream : TStream; SrcPos : TCRTPosition; Indent : integer);
begin
  if SrcPos.TextPresent then
  begin
    fStreamTools.Stream := Stream;
    CopySourcePart(Stream, SrcPos, Indent);
    fStreamTools.StreamLine('');
  end;
end;

function TFrameTools.GenProcedureHeading(sn : CRTSymbolNode; ProcImp : boolean) : AnsiString;
begin
  if not ProcImp then
    Result := '    ';
  Result := Result + 'procedure ';
  if ProcImp then
    Result := Result + 'T' + fGrammarName + '.';
  Result := Result + '_';
  Result := Result + sn.name;
  if sn.attrPos.beg >= 0 then
  begin
    Result := Result + ' (';
    Result := Result + CopySourceStr(sn.attrPos);
    Result := Result + ')';
  end;
  Result := Result + ';';
end;

procedure TFrameTools.WriteProductionsDec(Stream : TStream);
var
  sp : integer;
  sn : CRTSymbolNode;
begin
  fStreamTools.Stream := Stream;
  sp := fTableHandler.FirstNt;
  while sp <= fTableHandler.LastNt do
  begin (* for all nonterminals *)
    fTableHandler.GetSym(sp, sn);
    fStreamTools.StreamLine(GenProcedureHeading(sn, false));
    inc(sp)
  end;
  fStreamTools.StreamLine('');
end;

procedure TFrameTools.WriteConst(Stream : TStream);
var
  i : integer;
  ErrNr : integer;
  Digits : integer;
  sn : CRTSymbolNode;
  len : integer;
  pos : integer;
begin
  fStreamTools.Stream := Stream;
  fStreamTools.StreamLine('const');
  i := 0;
  pos := CRS.MaxSourceLineLength + 1;
  repeat
    fTableHandler.GetSym(i, sn);
    len := Length(sn.constant);
    if len > 0 then
    begin
      errNr := i;
      Digits := 1;
      while errNr >= 10 do
      begin
        inc(Digits);
        errNr := errNr div 10;
      end;
      inc(len, 3 + Digits + 1);
      if pos + len > CRS.MaxSourceLineLength then
      begin
        fStreamTools.StreamLine('');
        pos := 0
      end;
      fStreamTools.StreamLn('  ' + sn.constant + ' = ' + IntToStr(i) + ';');
      inc(pos, len + 2);
    end;
    inc(i);
  until i > fTableHandler.MaxP;
end;

procedure TFrameTools.GenPragmaCode(Stream : TStream; leftMarg : integer);
var
  i : integer;
  sn : CRTSymbolNode;
  FirstCase : boolean;
  SrcPos : TCRTPosition;
begin
  fStreamTools.Stream := Stream;
  i := fTableHandler.MaxT + 1;
  if i > fTableHandler.MaxP then
    EXIT;
  FirstCase := true;
  fStreamTools.StreamLine('case fCurrentInputSymbol of');
  SrcPos := TCRTPosition.Create;
  try
    while true do
    begin
      fTableHandler.GetSym(i, sn);
      if FirstCase then
        FirstCase := false;
      fStreamTools.StreamLn('  ');
      fStreamTools.StreamSN(i);
      fStreamTools.StreamLn(': begin ');
      SrcPos.TextPresent := true;
      SrcPos.StartText := sn.semPos.beg;
      SrcPos.TextLength := sn.semPos.len;
      SrcPos.Column := sn.semPos.col;
      WriteSourcePart(Stream, SrcPos, LeftMarg + 6);
      fStreamTools.StreamLn(' end;');
      if i = fTableHandler.MaxP then
        Break;
      inc(i);
      fStreamTools.StreamLine('');
    end;
  finally
    SrcPos.Free;
  end;
  fStreamTools.StreamLine('');
  fStreamTools.StreamLine('end;');
  fStreamTools.StreamLine('GetScanner.NextSymbol.Pos := GetScanner.CurrentSymbol.Pos;');
  fStreamTools.StreamLine('GetScanner.NextSymbol.Col := GetScanner.CurrentSymbol.Col;');
  fStreamTools.StreamLine('GetScanner.NextSymbol.Line := GetScanner.CurrentSymbol.Line;');
  fStreamTools.StreamLine('GetScanner.NextSymbol.Len := GetScanner.CurrentSymbol.Len;');
end;

procedure TFrameTools.VersionMethods(Stream : TStream);
begin
  if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('    function GetBuildDate : TDateTime;');
    fStreamTools.StreamLine('    function GetVersion : AnsiString;');
    fStreamTools.StreamLine('    function GetVersionStr : AnsiString;');
    fStreamTools.StreamLine('    procedure SetVersion(const Value : AnsiString);');
    if fOptions.AGI.VersionInfo.Count > 0 then
      fStreamTools.StreamLine('    function GetVersionInfo : AnsiString;');
  end;
end;

procedure TFrameTools.VersionProperties(Stream : TStream);
begin
  if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('    property BuildDate : TDateTime read GetBuildDate;');
    fStreamTools.StreamLine('    property VersionStr : AnsiString read GetVersionStr;');
    if fOptions.AGI.VersionInfo.Count > 0 then
      fStreamTools.StreamLine('    property VersionInfo : AnsiString read GetVersionInfo;');
  end;
end;

procedure TFrameTools.VersionString(Stream : TStream);
begin
  if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('property Version : AnsiString read GetVersion write SetVersion;');
  end;
end;

procedure TFrameTools.VersionImpl(Stream : TStream);
var
  Ver : AnsiString;
begin
  if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('function T' + fGrammarName
        + '.GetBuildDate : TDateTime;');
    fStreamTools.StreamLine('const');
    fStreamTools.StreamLine('  BDate = ' + IntToStr(Trunc(fOptions.AGI.LastBuild)) + ';');
    fStreamTools.StreamLine('  Hour = ' + FormatDateTime('HH', fOptions.AGI.LastBuild) + ';');
    fStreamTools.StreamLine('  Min = ' + FormatDateTime('NN', fOptions.AGI.LastBuild) + ';');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLine('  Result := BDate + EncodeTime(Hour, Min, 0 ,0);');
    fStreamTools.StreamLine('end;');
    fStreamTools.StreamLine('');

    Ver := IntToStr(fOptions.AGI.MajorVersion) + '.'
        + IntToStr(fOptions.AGI.MinorVersion) + '.'
        + IntToStr(fOptions.AGI.Release) + '.'
        + IntToStr(fOptions.AGI.Build);
    fStreamTools.StreamLine('function T' + fGrammarName + '.GetVersion : AnsiString;');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLine('  Result := ' + AnsiQuotedStr(Ver,#39) + ';');
    fStreamTools.StreamLine('end;');
    fStreamTools.StreamLine('');

    fStreamTools.StreamLine('function T' + fGrammarName + '.GetVersionStr : AnsiString;');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLine('  Result := ' + AnsiQuotedStr(
          FormatVersion(fOptions.AGI.StringVersionFormat,
          fOptions.AGI.MajorVersion, fOptions.AGI.MinorVersion,
          fOptions.AGI.Release, fOptions.AGI.Build),#39)
        + ';');
    fStreamTools.StreamLine('end;');
    fStreamTools.StreamLine('');

    if fOptions.AGI.VersionInfo.Count > 0 then
    begin
      fStreamTools.StreamLine('function T' + fGrammarName + '.GetVersionInfo : AnsiString;');
      fStreamTools.StreamLine('begin');
      fStreamTools.StreamLine('  Result := ' + fOptions.AGI.VersionInfoCode + ';');
      fStreamTools.StreamLine('end;');
      fStreamTools.StreamLine('');
    end;

    fStreamTools.StreamLine('procedure T' + fGrammarName + '.SetVersion(const Value : AnsiString);');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLine('  // This is a read only property. However, we want the value');
    fStreamTools.StreamLine('  // to appear in the Object Inspector during design time.');
    fStreamTools.StreamLine('end;');
  end;
end;

procedure TFrameTools.WriteWeakMethods(Stream : TStream);
begin
  if fTableHandler.HasWeak then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('function WeakSeparator(n, syFol, repFol : integer) : boolean;');
    fStreamTools.StreamLine('procedure ExpectWeak(n, follow : integer);');
  end;
end;

procedure TFrameTools.WriteWeakImpl(Stream : TStream);
begin
  if fTableHandler.HasWeak then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('procedure T' + fGrammarName + '.ExpectWeak(n, follow : integer);');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLine('  if fCurrentInputSymbol = n then');
    fStreamTools.StreamLine('    Get');
    fStreamTools.StreamLine('  else');
    fStreamTools.StreamLine('  begin');
    fStreamTools.StreamLine('    SynError(n);');
    fStreamTools.StreamLine('    while (fCurrentInputSymbol > EOFSYMB) AND (not _In(symSet[follow], fCurrentInputSymbol)) do');
    fStreamTools.StreamLine('      Get;');
    fStreamTools.StreamLine('  end');
    fStreamTools.StreamLine('end;  {ExpectWeak}');
    fStreamTools.StreamLine('');

    fStreamTools.StreamLine('function T' + fGrammarName + '.WeakSeparator(n, syFol, repFol : integer) : boolean;');
    fStreamTools.StreamLine('var');
    fStreamTools.StreamLine('  s : SymbolSet;');
    fStreamTools.StreamLine('  i : integer;');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLine('  if fCurrentInputSymbol = n then');
    fStreamTools.StreamLine('  begin');
    fStreamTools.StreamLine('    Get;');
    fStreamTools.StreamLine('    Result := true;');
    fStreamTools.StreamLine('    exit;');
    fStreamTools.StreamLine('  end');
    fStreamTools.StreamLine('  else if _In(symSet[repFol], fCurrentInputSymbol) then');
    fStreamTools.StreamLine('  begin');
    fStreamTools.StreamLine('    Result := false;');
    fStreamTools.StreamLine('    exit;');
    fStreamTools.StreamLine('  end');
    fStreamTools.StreamLine('  else');
    fStreamTools.StreamLine('  begin');
    fStreamTools.StreamLine('    i := 0;');
    fStreamTools.StreamLine('    while i <= maxT div setsize do');
    fStreamTools.StreamLine('    begin');
    fStreamTools.StreamLine('      s[i] := symSet[0, i] + symSet[syFol, i] + symSet[repFol, i];');
    fStreamTools.StreamLine('      inc(i)');
    fStreamTools.StreamLine('    end;');
    fStreamTools.StreamLine('    SynError(n);');
    fStreamTools.StreamLine('    while not _In(s, fCurrentInputSymbol) do');
    fStreamTools.StreamLine('      Get;');
    fStreamTools.StreamLine('    Result := _In(symSet[syFol], fCurrentInputSymbol)');
    fStreamTools.StreamLine('  end');
    fStreamTools.StreamLine('end;  {WeakSeparator}');
    fStreamTools.StreamLine('');
  end;
end;

procedure TFrameTools.WriteAncestor(Stream : TStream);
begin
  if Trim(fOptions.AGI.FrameAncestor) <> '' then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLn('  , '
      + ExtractFileName(ChangeFileExt(fOptions.AGI.FrameAncestor, ''))
      + ' in ' + AnsiQuotedStr(fOptions.AGI.FrameAncestor,#39));
  end;
end;

function TFrameTools.GetMemoType : AnsiString;
begin
  if (crsUseRichEdit IN fOptions.Switches.SwitchSet) then
    Result := 'TRichEdit'
  else
    Result := 'TMemo';
end;

function TFrameTools.GetMemoProperties : AnsiString;
begin
  if (crsUseRichEdit IN fOptions.Switches.SwitchSet) then
    Result := 'PlainText = True'
  else
    Result := '';
end;

procedure TFrameTools.WriteScannerCommentField(Stream : TStream);
begin
  if (crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0) then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('    fLastCommentList : TCommentList;');
  end;
end;

procedure TFrameTools.WriteScannerComment(Stream : TStream);
begin
  if (fCocoData.CommentList.Count > 0) then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('    function Comment : boolean;');
  end;
end;

procedure TFrameTools.WriteComponentComment(Stream : TStream);
var
  i : integer;
begin
  if fCocoData.CommentList.Count > 0 then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('function T-->Scanner<--.Comment : boolean;');
    fStreamTools.StreamLine('var');
    fStreamTools.StreamLine('  level : integer;');
    fStreamTools.StreamLine('  StartCommentCh: AnsiChar;');
    if (crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0) then
    begin
      fStreamTools.StreamLine('  CommentColumn : integer;');
      fStreamTools.StreamLine('  CommentLine : integer;');
    end;
    if fCocoData.CommentList.HasCommentWithOneEndChar then
      fStreamTools.StreamLine('  startLine : integer;');
    fStreamTools.StreamLine('  oldLineStart : longint;');
    fStreamTools.StreamLine('  CommentStr : AnsiString;');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLine('StartCommentCh := CurrInputCh;');
    fStreamTools.StreamLine('  level := 1;');
    if fCocoData.CommentList.HasCommentWithOneEndChar then
      fStreamTools.StreamLine('  startLine := CurrLine;');
    fStreamTools.StreamLine('  oldLineStart := StartOfLine;');
    fStreamTools.StreamLine('  CommentStr := CharAt(BufferPosition);');
    if (crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0) then
    begin
      fStreamTools.StreamLine('  CommentColumn := BufferPosition - StartOfLine - 1;');
      fStreamTools.StreamLine('  CommentLine := CurrLine;');
    end;
    for i := 0 to fCocoData.CommentList.Count - 1 do
      fScannerGenerator.GenComment(Stream, fCocoData.CommentList.Items[i]);

    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('end;  { Comment }');
  end;
end;

procedure TFrameTools.WriteGrammarGetComment(Stream : TStream);
begin
  if (crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0) then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('if GetScanner.fLastCommentList.Count > 0 then');
    fStreamTools.StreamLine('begin');
    fStreamTools.StreamLine('  if Assigned(fInternalGrammarComment) then');
    fStreamTools.StreamLine('    fInternalGrammarComment(Self,GetScanner.fLastCommentList);');
    fStreamTools.StreamLine('  GetScanner.fLastCommentList.Clear;');
    fStreamTools.StreamLine('end;');
  end;
end;

procedure TFrameTools.WriteGrammarCommentField(Stream : TStream);
begin
  if (crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0) then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('    fInternalGrammarComment : TCommentEvent;');
  end;
end;

procedure TFrameTools.WriteGrammarCommentProperty(Stream : TStream);
begin
  if (crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0) then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('    property InternalGrammarComment : TCommentEvent '
        + 'read fInternalGrammarComment write fInternalGrammarComment;');
  end;
end;

procedure TFrameTools.WriteLiterals(Stream : TStream);
begin
  fScannerGenerator.GenLiterals(Stream);
end;

procedure TFrameTools.WriteLiteralSupportDecl(Stream : TStream);
begin
  fScannerGenerator.GenLiteralSupportDecl(Stream);
end;

procedure TFrameTools.WriteCommentImplementation(Stream : TStream);
begin
  fScannerGenerator.GenCommentImplementation(Stream);
end;

procedure TFrameTools.WriteGetSyB(Stream : TStream);
begin
  fScannerGenerator.GetGetSyB(Stream, TRUE);
end;

procedure TFrameTools.WriteScannerInit(Stream : TStream);
begin
  fScannerGenerator.ScannerInit(Stream);
end;

procedure TFrameTools.WriteProductions(Stream : TStream);
begin
  fParserGen.GenProductions(Stream);
end;

procedure TFrameTools.WriteParseRoot(Stream : TStream);
begin
  fParserGen.GenParseRoot(Stream);
end;

procedure TFrameTools.WriteParserInit(Stream : TStream);
begin
  fParserGen.GenParserInit(Stream);
end;

procedure TFrameTools.WriteCleanup(Stream : TStream; GrammarName : AnsiString);
var
  ErrorStr : AnsiString;
begin
  if fParserGen.MaxSS < 0 then
    fParserGen.MaxSS := 0;

  ErrorStr := fParserGen.GenErrorList;

  fStreamTools.Stream := Stream;
  fStreamTools.ReplaceStrInStream(
      ['-->GRAMMAR<--', '-->SCANNER<--', '-->ERRORS<--', '-->MAX<--'],
      [GrammarName, GrammarName + 'Scanner', ErrorStr, IntToStr(fParserGen.MaxSS)]);
end; {WriteCleanup}

function TFrameTools.GetGrammarVersion : AnsiString;
begin
  if crsGenVersionInfo IN fOptions.Switches.SwitchSet then
    Result := FormatVersion(fOptions.AGI.StringVersionFormat,
        fOptions.AGI.MajorVersion,
        fOptions.AGI.MinorVersion,
        fOptions.AGI.Release,
        fOptions.AGI.Build)
  else
    Result := '';
end; {GetGrammarVersion}

function TFrameTools.GetGrammarVersionLastBuild : AnsiString;
begin
  Result := FormatDateTime('ddddd t', Now);
end;

function TFrameTools.GetGrammarVersionInfo : AnsiString;
begin
  Result := fOptions.AGI.VersionInfoText;
end; {GetGrammarVersionInfo}

procedure TFrameTools.WriteScannerDestroyDecl(Stream : TStream);
begin
  if (crsUseHashFunctions IN fOptions.Switches.SwitchSet)
      OR ((crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0)) then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('destructor Destroy; override;');
  end;
end;

procedure TFrameTools.WriteScannerDestroyImpl(Stream : TStream);
begin
  if (crsUseHashFunctions IN fOptions.Switches.SwitchSet)
      OR ((crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0)) then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('destructor T-->Scanner<--.Destroy;');
    fStreamTools.StreamLine('begin');
    if (crsUseHashFunctions IN fOptions.Switches.SwitchSet) then
    begin
      fStreamTools.StreamLine('  fHashList.Free;');
      fStreamTools.StreamLine('  fHashList := NIL;');
    end;
    if ((crsGenCommentEvents IN fOptions.Switches.SwitchSet) AND (fCocoData.CommentList.Count > 0)) then
    begin
      fStreamTools.StreamLine('  if Assigned(fLastCommentList) then');
      fStreamTools.StreamLine('  begin');
      fStreamTools.StreamLine('    fLastCommentList.Free;');
      fStreamTools.StreamLine('    fLastCommentList := NIL;');
      fStreamTools.StreamLine('  end;');
    end;
    fStreamTools.StreamLine('  inherited;');
    fStreamTools.StreamLine('end;');
  end;
end;

procedure TFrameTools.WriteOnHomographEvent(Stream : TStream);
begin
  if (crsUseHashFunctions IN fOptions.Switches.SwitchSet) AND (fTableHandler.HomographCount > 0) then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine(
        'TOnHomographEvent = function(const aToken: AnsiString; const aNextToken : AnsiString;');
    fStreamTools.StreamLine(
        '    aNextSymbol : integer; SymId: integer; DefaultIdentId: integer): integer of object;');
  end;
end;

procedure TFrameTools.WriteOnHomographProperty(Stream : TStream);
begin
  if (crsUseHashFunctions IN fOptions.Switches.SwitchSet) AND (fTableHandler.HomographCount > 0) then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('property OnHomograph : TOnHomographEvent read fOnHomograph write fOnHomograph;');
  end;
end;

procedure TFrameTools.WriteOnHomographField(Stream : TStream);
begin
  if (crsUseHashFunctions IN fOptions.Switches.SwitchSet) AND (fTableHandler.HomographCount > 0) then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('fOnHomograph : TOnHomographEvent;');
  end;
end;

procedure TFrameTools.WriteScannerHashField(Stream : TStream);
begin
  if crsUseHashFunctions IN fOptions.Switches.SwitchSet then
  begin
    fStreamTools.Stream := Stream;
    fStreamTools.StreamLine('fHashList: TmwStringHashList;');
  end;
end;

end.

