unit CocoCompiler;
{$INCLUDE CocoCD.inc}

interface

uses
  CRX,
  CRC,
  CRT,
  CRS,
  FrameTools,
  CocoDefs,
  Classes,
  CocoOptions,
  CocoBase,
  Coco,
  CocoDat,
  StreamTools;

type
  TCocoCompiler = class(TObject)
  private
    fCocoR: TCoco;
    fParserGen : TParserGen;
    fScannerGenerator : TScannerGen;
    fComponentGen : TComponentGen;
    fCocoData : TCocoData;

    fOnShowMessage: TCocoShowMessage;
    fOptions: TCocoOptions;
    fFrameTools: TFrameTools;
    fStreamTools: TStreamTools;

    function GetGrammarName: AnsiString;
    procedure SetGrammarName(const Value: AnsiString);

    procedure AfterParse(Sender : TObject);
    procedure BeforeParse(Sender : TObject);
    procedure CocoRAfterGenList(Sender : TObject; var PrintErrorCount : boolean);
    procedure CocoRBeforeGenList(Sender : TObject);
    procedure CocoROnError(Sender : TObject; Error : TCocoError);
    function GetOnStatusUpdate: TStatusUpdateProc;
    procedure SetOnStatusUpdate(const Value: TStatusUpdateProc);
    procedure ShowMsg(const Msg : AnsiString);
    procedure SetOnShowMessage(const Value: TCocoShowMessage);
    function GetErrorCount: integer;
    function GetLL1TestConducted: boolean;
    function GetLL1WarningCount: integer;
    function GetSourceStream: TStream;
    function GetLL1Acceptable: boolean;
    function GetGrammarErrorCount: integer;
    function GetShowWarnings: boolean;
    function GetWarningCount: integer;
    procedure PerformGrammarTests;
    procedure GenerateFileHeader;
    procedure GenerateDebuggingText;
    function ParserFrameFileExists: boolean;
    procedure SetupCocoR;
    function OutputDirectoryOk: boolean;
    procedure SetOptions(const Value: TCocoOptions);
    procedure SetAbortErrorMessage(const ErrMsg : AnsiString);
    function TableHandler : TTableHandler;
    function ExecuteFrameParser(const aFileName, aGrammarName,
      aFrameFileName: AnsiString): boolean;
    function GetGrammarSourceFileName: AnsiString;
    procedure SetGrammarSourceFileName(const Value: AnsiString);
    function ErrorSeverity(const ErrorType: integer): AnsiString;
  public
    constructor Create;
    destructor Destroy; override;

    procedure LoadFromIniFile(const IniFileName : AnsiString);
    function Execute : boolean;
    procedure AddError(
        const ErrorType : integer;
        const ErrorCode : integer;
        const Line : integer = 0;
        const Col : integer = 0;
        const Data : AnsiString = '');
    procedure ExportToXML;

    property FrameTools : TFrameTools read fFrameTools write fFrameTools;
    property Options : TCocoOptions read fOptions write SetOptions; // not created - passed in
    property ErrorCount : integer read GetErrorCount;
    property LL1TestConducted : boolean read GetLL1TestConducted;
    property LL1WarningCount : integer read GetLL1WarningCount;
    property LL1Acceptable : boolean read GetLL1Acceptable;
    property GrammarErrorCount : integer read GetGrammarErrorCount;
    property ShowWarnings : boolean read GetShowWarnings;
    property WarningCount : integer read GetWarningCount;

    property GrammarSourceFileName : AnsiString read GetGrammarSourceFileName write SetGrammarSourceFileName;
    property GrammarName : AnsiString read GetGrammarName write SetGrammarName;
    property SourceStream : TStream read GetSourceStream;

    property OnShowMessage : TCocoShowMessage read fOnShowMessage write SetOnShowMessage;
    property OnStatusUpdate : TStatusUpdateProc read GetOnStatusUpdate write SetOnStatusUpdate;
  end; {TCocoCompiler}

implementation

uses
  CocoSwitch, SysUtils, CRTypes, Frame;

{ TCocoCompiler }

procedure TCocoCompiler.ShowMsg(const Msg: AnsiString);
begin
  fCocoR.Msg(Msg, fCocoR.Output);
  if Assigned(fCocoR.OnStatusUpdate) then
    fCocoR.OnStatusUpdate(fCocoR, cstString, Msg, -1);
end; {ShowMsg}

function TCocoCompiler.ExecuteFrameParser(
    const aFileName : AnsiString;
    const aGrammarName : AnsiString;
    const aFrameFileName : AnsiString) : boolean;
var
  ccFrame : TFrame;
begin
  ccFrame := TFrame.Create(nil);
  try
    ccFrame.FileName := aFileName;
    ccFrame.GrammarName := aGrammarName;
    ccFrame.FrameTools := fFrameTools;
    GrammarName := aGrammarName;
    ccFrame.Init;
    ccFrame.SourceFileName := aFrameFileName;
    ccFrame.TableHandler := TableHandler;
    ccFrame.Execute;
    Result := ccFrame.Successful;
    if NOT Result then
      begin
        ListFileName := ChangeFileExt(aGrammarName, LST_FILE_EXT);
        ccFrame.ListStream.SaveToFile(ListFileName);
      end;
  finally
    ccFrame.Free;
  end;
end; {ExecuteFrameParser}

procedure TCocoCompiler.PerformGrammarTests;
begin
  if Assigned(fCocoR.OnStatusUpdate) then
    fCocoR.OnStatusUpdate(fCocoR, cstString, 'testing grammar', -1);
  fCocoR.PrintDivider;
  fCocoR.StreamToListFile('Grammar Tests:', TRUE);
  TableHandler.CompSymbolSets(false);
  fCocoR.GrammarCount := fCocoR.GrammarCount + TableHandler.TestCompleteness;
  if fCocoR.GrammarCount = 0 then
    fCocoR.GrammarCount := fCocoR.GrammarCount + TableHandler.TestIfAllNtReached;
  if fCocoR.GrammarCount = 0 then
    fCocoR.GrammarCount := fCocoR.GrammarCount + TableHandler.FindCircularProductions;
  if fCocoR.GrammarCount = 0 then
    fCocoR.GrammarCount := fCocoR.GrammarCount + TableHandler.TestIfNtToTerm;
  if fCocoR.GrammarCount = 0 then
    fCocoR.GrammarCount := fCocoR.GrammarCount + TableHandler.TestHomographs;
  if fCocoR.GrammarCount = 0 then
    begin
      fCocoR.LL1Count := TableHandler.LL1Test;
      fCocoR.LL1TestConducted := TRUE;
    end;
  fCocoR.WarningCount := TableHandler.NumWarnings;
end; {PerformGrammarTests}

procedure TCocoCompiler.GenerateFileHeader;
begin
  fCocoR.StreamToListFile(GrammarName, TRUE);
  fCocoR.StreamToListFile('', TRUE);
  fCocoR.StreamToListFile('Compiled: ' + FormatDateTime('dddddd - tttttt', Now), TRUE);
  fCocoR.StreamToListFile('Output sent to: ' + fOptions.AGI.OutputDirectory, TRUE);
  fCocoR.PrintDivider;
  fCocoR.StreamToListFile(fOptions.AGI.Switches.Text, TRUE);
end; {GenerateFileHeader}

procedure TCocoCompiler.GenerateDebuggingText;
begin
  fCocoR.PrintDivider;
  TableHandler.WriteStatistics(fParserGen.MaxSS);
  TableHandler.PrintStartFollowerSets;
  TableHandler.CompSymbolSets(true);
end; {GenerateDebuggingText}

procedure TCocoCompiler.AfterParse(Sender: TObject);
begin
  fParserGen.CommentsExist := (crsGenCommentEvents IN fOptions.Switches.SwitchSet)
      AND (fCocoData.CommentList.Count > 0);
  fParserGen.AppendSemiColon := crsAppendSemiColon IN fOptions.Switches.SwitchSet;
  fCocoR.GrammarCount := 0;
  if fCocoR.ErrorList.Count = 0 then
  begin
    GenerateFileHeader;
    PerformGrammarTests;
    GenerateDebuggingText;
    if Assigned(fCocoR.OnStatusUpdate) then
      fCocoR.OnStatusUpdate(fCocoR, cstString, 'generating symbol names', -1);
    TableHandler.AssignSymNames;
    if (fCocoR.GrammarCount = 0) and not (crsGrammarTestsOnly  IN fOptions.Switches.SwitchSet) then
    begin
      if NOT fScannerGenerator.InitScanner then
        fCocoR.OnStatusUpdate(fCocoR, cstString, 'could not initialize scanner', -1)
      else
      begin
        if Assigned(fCocoR.OnStatusUpdate) then
          fCocoR.OnStatusUpdate(fCocoR, cstString, 'generating parser', -1);
        fParserGen.GenCompiler(fOptions.AGI.FrameParser, fOptions.AGI.OutputDirectory);
        if Assigned(fCocoR.OnStatusUpdate) then
          fCocoR.OnStatusUpdate(fCocoR, cstString, 'generating scanner', -1);
        if TableHandler.GenScanner and (crsTraceAutomaton IN fOptions.Switches.SwitchSet) then
          fCocoR.Automaton.PrintStates;
        if Assigned(fCocoR.OnStatusUpdate) then
          fCocoR.OnStatusUpdate(fCocoR, cstString, 'generating component', -1);
        if (crsGenRegistration IN fOptions.Switches.SwitchSet) then
        begin
          if Assigned(fCocoR.OnStatusUpdate) then
            fCocoR.OnStatusUpdate(fCocoR, cstString, 'generating registration unit', -1);
          fComponentGen.WriteCocoRegistration(fOptions.AGI.OutputDirectory);
        end;
        if crsGenTestProgram IN fOptions.Switches.SwitchSet then
        begin
          if Assigned(fCocoR.OnStatusUpdate) then
            fCocoR.OnStatusUpdate(fCocoR, cstString, 'generating test program', -1);
          fComponentGen.WriteCocoTestProgram(fOptions.AGI.FrameTestProject,
              fOptions.AGI.FrameTestForm, fOptions.AGI.FrameTestDfm,
              fOptions.AGI.OutputDirectory);
        end;
        if crsGenConsoleApp IN fOptions.Switches.SwitchSet then
        begin
          if Assigned(fCocoR.OnStatusUpdate) then
            fCocoR.OnStatusUpdate(fCocoR, cstString, 'generating console app', -1);
          fComponentGen.WriteConsoleApp(fOptions.AGI.FrameConsole,
              fOptions.AGI.OutputDirectory);
        end;
        if crsPrintXRef IN fOptions.Switches.SwitchSet then
          TableHandler.XRef;
        if crsPrintGraph IN fOptions.Switches.SwitchSet then
        begin
          fCocoR.PrintDivider;
          TableHandler.PrintGraph;
        end;
        if crsPrintSymbolTable IN fOptions.Switches.SwitchSet then
        begin
          fCocoR.PrintDivider;
          TableHandler.PrintSymbolTable;
        end;
      end; // if InitScanner
    end;
    if fCocoR.GrammarCount > 0 then
      ShowMsg('Compilation ended with errors in grammar tests.')
    else if (fCocoR.LL1Count > 0) then
      ShowMsg('Compilation ended with LL(1) errors.')
    else
      ShowMsg('Compilation completed. No errors detected.');
  end
  else
  begin
    ShowMsg('*** errors detected ***');
    fCocoR.PrintDivider;
    fCocoR.StreamToListFile(fOptions.AGI.Switches.Text, TRUE);
  end;
  if Assigned(fCocoR.OnStatusUpdate) then
    fCocoR.OnStatusUpdate(fCocoR, cstString, 'done', -1);
  fCocoR.ErrorCount := fCocoR.ErrorCnt;
  fCocoR.Automaton.Finalize;

  if (fCocoR.GrammarCount = 0) AND (NOT (crsGrammarTestsOnly IN fOptions.Switches.SwitchSet)) then
  begin
    if crsAutoIncBuild IN fOptions.Switches.SwitchSet then
      fOptions.AGI.Build := fOptions.AGI.Build + 1;
    fOptions.AGI.LastBuild := Now;
  end;
end; {AfterParse}

procedure TCocoCompiler.BeforeParse(Sender: TObject);
begin
end; {BeforeParse}

procedure TCocoCompiler.CocoRAfterGenList(Sender: TObject;
  var PrintErrorCount: boolean);
var
  i : integer;
  ErrorArray : array[0..5] of integer;
begin
  PrintErrorCount := false;
  FillChar(ErrorArray, sizeof(ErrorArray), 0);
  if fCocoR.ErrorList.Count = 0 then
    fCocoR.StreamToListFile('No errors.', FALSE)
  else
  begin
    fCocoR.StreamToListFile('', TRUE);
    for i := 0 to fCocoR.ErrorList.Count - 1 do
      ErrorArray[TCocoError(fCocoR.ErrorList[i]).ErrorType] :=
        ErrorArray[TCocoError(fCocoR.ErrorList[i]).ErrorType] + 1;
    for i := 0 to 5 do
    begin
      if ErrorArray[i] > 0 then
      begin
        fCocoR.StreamToListFile(PadL(IntToStr(ErrorArray[i]), ' ', 5) + ' ', FALSE);
        fCocoR.StreamToListFile(CRTYPES.ErrorDesc(i), FALSE);
        if ErrorArray[i] <> 1 then
          fCocoR.StreamToListFile('s', TRUE)
        else
          fCocoR.StreamToListFile('', TRUE);
      end;
    end;
  end;
  fCocoR.StreamToListFile('', TRUE);
end; {CocoRAfterGenList}

procedure TCocoCompiler.CocoRBeforeGenList(Sender: TObject);
begin
  (Sender as TCoco).PrintDivider;
  (Sender as TCoco).StreamToListFile('Listing', TRUE);
  (Sender as TCoco).StreamToListFile('', TRUE);
end; {CocoRBeforeGenList}

procedure TCocoCompiler.CocoROnError(Sender: TObject; Error: TCocoError);
begin
  if (Error.ErrorType = etSymantic)
    or (Error.ErrorType = etSyntax)
    or (Error.ErrorType = etGeneration) then
    (Sender as TCoco).ErrorCnt := (Sender as TCoco).ErrorCnt + 1;
end; {CocoROnError}

constructor TCocoCompiler.Create;
begin
  inherited;
  fCocoData := TCocoData.Create;

  fCocoR := TCoco.Create(nil);
  fCocoR.OnError := CocoROnError;
  fCocoR.AfterGenList := CocoRAfterGenList;
  fCocoR.BeforeGenList := CocoRBeforeGenList;
  fCocoR.AfterParse := AfterParse;
  fCocoR.BeforeParse := BeforeParse;
  fCocoR.OnInsertComment := fCocoData.CommentList.InsertAtTop;

  fStreamTools := TStreamTools.Create;
  fStreamTools.TableHandler := fCocoR.TableHandler;

  fCocoData.OnScannerError := fCocoR.GetScannerError;
  fCocoData.TableHandler := TableHandler;
  fCocoData.OnGetCurrentSymbol := fCocoR.GetCurrentSymbol;

  fComponentGen := TComponentGen.Create;
  fComponentGen.TableHandler := TableHandler;
  fComponentGen.OnSetGrammarName := SetGrammarName;
  fComponentGen.OnScannerError := fCocoR.GetScannerError;
  fComponentGen.OnAbortErrorMessage := SetAbortErrorMessage;
  fComponentGen.OnExecuteFrameParser := ExecuteFrameParser;
  fComponentGen.StreamTools := fStreamTools;

  fParserGen := TParserGen.Create;
  fParserGen.TableHandler := TableHandler;
  fParserGen.OnGetGrammarName := GetGrammarName;
  fParserGen.OnExecuteFrameParser := ExecuteFrameParser;
  fParserGen.OnScannerError := fCocoR.GetScannerError;
  fParserGen.OnAbortErrorMessage := SetAbortErrorMessage;
  fParserGen.OnStreamLine := fCocoR.StreamToListFile;
  fParserGen.OnGetCharAt := fCocoR.GetCharAt;
  fParserGen.StreamTools := fStreamTools;

  fScannerGenerator := TScannerGen.Create;
  fScannerGenerator.TableHandler := TableHandler;
  fScannerGenerator.Automaton := fCocoR.Automaton;
  fScannerGenerator.CocoData := fCocoData;
  fScannerGenerator.StreamTools := fStreamTools;

  fFrameTools := TFrameTools.Create;
  fFrameTools.TableHandler := TableHandler;
  fFrameTools.OnGetCharAt := fCocoR.GetCharAt;
  fFrameTools.ParserGen := fParserGen;
  fFrameTools.ScannerGenerator := fScannerGenerator;
  fFrameTools.CocoData := fCocoData;
  fFrameTools.StreamTools := fStreamTools;
end; {Create}

destructor TCocoCompiler.Destroy;
begin
  FreeAndNIL(fCocoData);
  FreeAndNIL(fCocoR);
  FreeAndNIL(fComponentGen);
  FreeAndNIL(fParserGen);
  FreeAndNIL(fFrameTools);
  FreeAndNIL(fStreamTools);
  FreeAndNIL(fScannerGenerator);
  inherited;
end; {Destroy}

function TCocoCompiler.Execute: boolean;
begin
  if ParserFrameFileExists AND OutputDirectoryOk then
  begin
    SetupCocoR;
    fCocoR.Execute;

    if fCocoR.SourceFileName <> '' then
    begin
      ListFileName := ChangeFileExt(fCocoR.SourceFileName, LST_FILE_EXT);
      fCocoR.ListStream.SaveToFile(ListFileName);
    end;
    fCocoR.ListStream.Position := 0; // return to beginning of the list stream
    Result := fCocoR.Successful;
  end
  else
    Result := FALSE;
end; {Execute}

function TCocoCompiler.GetGrammarName: AnsiString;
begin
  Result := fOptions.GrammarName;
end; {GetGrammarName}

procedure TCocoCompiler.SetGrammarName(const Value: AnsiString);
begin
  fOptions.GrammarName := Value;
  fFrameTools.GrammarName := Value;
end; {SetGrammarName}

function TCocoCompiler.ParserFrameFileExists : boolean;
var
  MsgString : AnsiString;
begin
  Result := TRUE;
  if not FileExists(fOptions.AGI.FrameParser) then
  begin
    if Assigned(fOnShowMessage) then
    begin
      if fOptions.AGI.FrameParser = '' then
        MsgString := 'No frame file defined'
      else
        MsgString := 'Frame file (' + fOptions.AGI.FrameParser + ') not found';
      fOnShowMessage(MsgString, FALSE);
    end;
    Result := FALSE;
  end;
end; {ParserFrameFileExists}

function TCocoCompiler.OutputDirectoryOk : boolean;
var
  s : AnsiString;
begin
  Result := TRUE;
  fOptions.AGI.OutputDirectory := Trim(fOptions.AGI.OutputDirectory);
  if (length(fOptions.AGI.OutputDirectory) > 0) then
  begin
    fOptions.AGI.OutputDirectory := IncludeTrailingPathDelimiter(fOptions.AGI.OutputDirectory);
    if not DirectoryExists(fOptions.AGI.OutputDirectory) then
    begin
      s := 'Output directory (' + fOptions.AGI.OutputDirectory + ') not found. Create it?';
      if Assigned(fOnShowMessage) AND fOnShowMessage(s, TRUE) then
        ForceDirectories(fOptions.AGI.OutputDirectory)
      else
        Result := FALSE;
    end;
  end;
end; {OutputDirectoryOk}

procedure TCocoCompiler.SetupCocoR;
begin
  fCocoR.ResetCocoR;
  fParserGen.Initialize;
  TableHandler.Initialize;
  if crsForceListing in fOptions.Switches.SwitchSet then
    fCocoR.GenListWhen := glAlways
  else
    fCocoR.GenListWhen := glOnError;
end; {SetupCocoR}

function TCocoCompiler.GetOnStatusUpdate: TStatusUpdateProc;
begin
  Result := fCocoR.OnStatusUpdate;
end; {GetOnStatusUpdate}

procedure TCocoCompiler.SetOnStatusUpdate(const Value: TStatusUpdateProc);
begin
  fCocoR.OnStatusUpdate := Value;
end; {SetOnStatusUpdate}

procedure TCocoCompiler.SetOnShowMessage(const Value: TCocoShowMessage);
begin
  fOnShowMessage := Value;
  fOptions.OnShowMessage := Value;
end; {SetOnShowMessage}

function TCocoCompiler.GetErrorCount: integer;
begin
  Result := fCocoR.ErrorCount;
end; {GetErrorCount}

function TCocoCompiler.GetLL1TestConducted: boolean;
begin
  Result := fCocoR.LL1TestConducted;
end; {GetLL1TestConducted}

function TCocoCompiler.GetLL1WarningCount: integer;
begin
  Result := fCocoR.LL1Count;
end; {GetLL1WarningCount}

function TCocoCompiler.GetSourceStream: TStream;
begin
  Result := fCocoR.SourceStream;
end; {GetSourceStream}

function TCocoCompiler.GetLL1Acceptable: boolean;
begin
  Result := TableHandler.LL1Acceptable;
end; {GetLL1Acceptable}

function TCocoCompiler.GetGrammarErrorCount: integer;
begin
  Result := fCocoR.GrammarCount;
end; {GetGrammarErrorCount}

function TCocoCompiler.GetShowWarnings: boolean;
begin
  Result := TableHandler.ShowWarnings;
end; {GetShowWarnings}

function TCocoCompiler.GetWarningCount: integer;
begin
  Result := fCocoR.WarningCount;
end; {GetWarningCount}

procedure TCocoCompiler.SetOptions(const Value: TCocoOptions);
begin
  fOptions := Value;
  fCocoR.Options := fOptions;
  fComponentGen.Options := fOptions;
  fScannerGenerator.Options := fOptions;
  fParserGen.Options := fOptions;
  fFrameTools.Options := fOptions;
end; {SetOptions}

procedure TCocoCompiler.SetAbortErrorMessage(const ErrMsg: AnsiString);
begin
  fCocoR.CRAbortErr := ErrMsg;
end; {SetAbortErrorMessage}

function TCocoCompiler.TableHandler: TTableHandler;
begin
  Result := fCocoR.TableHandler;
end; {TableHandler}

procedure TCocoCompiler.LoadFromIniFile(const IniFileName: AnsiString);
begin
  fOptions.AGI.LoadFromFile(IniFileName);
end; {LoadFromIniFile}

function TCocoCompiler.GetGrammarSourceFileName: AnsiString;
begin
  Result := fCocoR.SourceFileName;
end; {GetGrammarSourceFileName}

procedure TCocoCompiler.SetGrammarSourceFileName(const Value: AnsiString);
begin
  fCocoR.SourceFileName := Value;
end; {SetGrammarSourceFileName}

function TCocoCompiler.ErrorSeverity(const ErrorType : integer) : AnsiString;
begin
  case ErrorType of
    etSyntax      {0}: Result := 'Error';
    etSymantic    {1}: Result := 'Error';
    etLL1         {2}: Result := 'Warning';
    etGrammar     {3}: Result := 'Error';
    etWarning     {4}: Result := 'Warning';
    etGeneration  {5}: Result := 'Warning';
    etMetaError   {6}: Result := 'Error';
    etMetaWarning {7}: Result := 'Warning';
  else
    Result := 'Error';
  end; {case}
end;

procedure TCocoCompiler.ExportToXML;
var
  i : integer;
  XML : TStringList;
begin
  XML := TStringList.Create;
  try
    XML.Add('<?xml version="1.0" ?>');
    XML.Add('<CocoRDelphi>');
    XML.Add('  <Files>');
    XML.Add('    <File Type="Source" Display="Source File" FileName="' + GrammarSourceFileName + '" />');
    if ListFileName > '' then
      XML.Add('    <File Type="Info" Display="Trace Listing" FileName="' + ListFileName + '" />');
    if ComponentFileName > '' then
      XML.Add('    <File Type="Info" Display="Generated Component File" FileName="' + ComponentFileName + '" />');
    XML.Add('  </Files>');
    XML.Add('  <Errors>');
    for i := 0 to fCocoR.ErrorList.Count - 1 do
      XML.Add('    ' + TCocoError(fCocoR.ErrorList[i]).ExportToXMLFragment(
          CRTYPES.ErrorDesc(TCocoError(fCocoR.ErrorList[i]).ErrorType),
          fCocoR.ErrorStr(TCocoError(fCocoR.ErrorList[i]).ErrorCode,
          TCocoError(fCocoR.ErrorList[i]).Data),
          ErrorSeverity(TCocoError(fCocoR.ErrorList[i]).ErrorType)));
    XML.Add('  </Errors>');
    XML.Add('</CocoRDelphi>');
    if Trim(GrammarSourceFileName) > '' then
      XML.SaveToFile(ChangeFileExt(GrammarSourceFileName, '.xml'));
  finally
    FreeAndNIL(XML);
  end;
end; {ExportToXML}

procedure TCocoCompiler.AddError(const ErrorType, ErrorCode, Line,
  Col: integer; const Data: AnsiString);
var
  Error : TCocoError;
begin
  Error := TCocoError.Create;
  Error.ErrorType := ErrorType;
  Error.ErrorCode := ErrorCode;
  Error.Line := Line;
  Error.Col := Col;
  Error.Data := Data; 
  fCocoR.ErrorList.Add(Error);
end;

end.

