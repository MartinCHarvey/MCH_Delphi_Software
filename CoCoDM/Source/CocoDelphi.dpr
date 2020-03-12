program CocoDelphi;

{$INCLUDE CocoCD.inc}
{$APPTYPE CONSOLE}

uses
  SysUtils,
  IniFiles,
  CocoDefs,
  Classes,
  mwStringHashList in '..\Distributable\Frames\mwStringHashList.pas',
  CocoBase in '..\Distributable\Frames\CocoBase.pas',
  Coco in 'Coco.pas',
  CocoAGI in 'CocoAGI.pas',
  CocoComment in 'CocoComment.pas',
  CocoCompiler in 'CocoCompiler.pas',
  CocoDat in 'CocoDat.pas',
  CocoOptions in 'CocoOptions.pas',
  CocoRegistry in 'CocoRegistry.pas',
  CocoSwitch in 'CocoSwitch.pas',
  CocoTools in 'CocoTools.PAS',
  CRA in 'CRA.pas',
  CRC in 'CRC.pas',
  Crg in 'Crg.pas',
  CRS in 'CRS.pas',
  CRT in 'CRT.pas',
  CRTypes in 'CRTypes.pas',
  CRX in 'CRX.pas',
  Frame in 'Frame.pas',
  FrameTools in 'FrameTools.pas',
  Sets in 'Sets.pas',
  StreamTools in 'StreamTools.pas',
  StringTools in 'StringTools.pas',
  TZRegistry in 'TZRegistry.pas';

const
  Version = 'Version 1.3.3';
  Indent = '   ';
  NoQuestionsModeStr = 'N - No questions mode (answer yes to all questions)';
  QuietModeStr = 'Q - Quiet mode';
  PauseStr = 'K - Pause';

type
  TStubObj = class(TObject)
  public
    function ShowMessage(const Msg: AnsiString; const YesNo : boolean = FALSE) : boolean;
    procedure UpdateStatus(Sender : TObject;
        const StatusType : TCocoStatusType;
        const Status : AnsiString;
        const LineNum : integer);
  end; {TStubObj}

var
  fPause : boolean;
  fQuietMode : boolean;
  fNoQuestionsMode : boolean;
  fStubObj : TStubObj;
  fCocoCo : TCocoCompiler;
  fOptions : TCocoOptions;
  fVerboseMessages : TStringList;

{ TStubObj }

function TStubObj.ShowMessage(const Msg: AnsiString; const YesNo : boolean) : boolean;
var
  s : string;
begin
  if YesNo OR (Not fQuietMode) then
  begin
    s := Msg;
    if YesNo then
      s := s + ' (Y/N) ';
    Write(s);
    fVerboseMessages.Add(s);
    if YesNo then
    begin
      if fNoQuestionsMode then
      begin
        s := 'Y';
        Write(s);
      end
      else
        Read(s);
      Result := Uppercase(s) = 'Y';
    end
    else
      Result := TRUE;
    Writeln;
  end
  else
    Result := TRUE;
end; {ShowMessage}

procedure TStubObj.UpdateStatus(Sender: TObject;
    const StatusType : TCocoStatusType;
    const Status: AnsiString;
    const LineNum: integer);
begin
  if (LineNum < 0) then
    fStubObj.ShowMessage(Status);
end; {UpdateStatus}

{ cCocoR }

procedure Create;
begin
  fQuietMode := FALSE;
  fPause := FALSE;
  fNoQuestionsMode := FALSE;

  fStubObj := TStubObj.Create;

  fOptions := TCocoOptions.Create;
  fCocoCo := TCocoCompiler.Create;
  fCocoCo.Options := fOptions;
  fCocoCo.OnShowMessage := fStubObj.ShowMessage;
  fCocoCo.OnStatusUpdate := fStubObj.UpdateStatus;
  fVerboseMessages := TStringList.Create;
  fVerboseMessages.Add('== Verbose Messages ============================================================');
  fVerboseMessages.Add('');
end; {Create}

procedure Destroy;
begin
  fCocoCo.ExportToXML;
  fStubObj.Free;
  fStubObj := NIL;
  fOptions.Free;
  fOptions := NIL;
  fCocoCo.Free;
  fCocoCo := NIL;
  fVerboseMessages.Free;
  fVerboseMessages := NIL;
end; {Destroy}

procedure ShowTitleAndVersion;
begin
  fStubObj.ShowMessage('Coco/R for Delphi, Command Line Compiler - ' + Version);
  fStubObj.ShowMessage('');
end; {ShowTitleAndVersion}

procedure ShowHelp;
var
  i : TCocoSwitch;
begin
  fStubObj.ShowMessage('Usage: cCocoR [Switches] [Grammar[' + ATG_FILE_EXT + ']] [Switches]');
  fStubObj.ShowMessage('Example: cCocoR +cs-r Test');
  fStubObj.ShowMessage('');
  fStubObj.ShowMessage('Switches are:');
  for i := Low(TCocoSwitch) to High(TCocoSwitch) do
    fStubObj.ShowMessage(Indent + fOptions.Switches.SwitchChar(i) + ' - '
        + fOptions.Switches.SwitchString(i));
  fStubObj.ShowMessage(Indent + NoQuestionsModeStr);
  fStubObj.ShowMessage(Indent + QuietModeStr);
  fStubObj.ShowMessage(Indent + PauseStr);
end; {ShowHelp}

procedure ModifySwitchs(SwitchChar : char; SwitchOn : boolean);
var
  Switch : TCocoSwitch;
begin
  Switch := fOptions.Switches.SwitchFromChar(SwitchChar);
  if SwitchOn then
    fOptions.Switches.SwitchSet := fOptions.Switches.SwitchSet + [Switch]
  else
    fOptions.Switches.SwitchSet := fOptions.Switches.SwitchSet - [Switch];
end; {ModifySwitchs}

procedure GetCommandLineParamSwitchs;
var
  i,j : integer;
  SwitchOn : boolean;
  Param : string;
begin
  SwitchOn := True;
  for i := 1 to ParamCount do
  begin
    Param := ParamStr(i);
    if Param[1] IN ['+','-'] then
      for j := 1 to length(Param) do
      begin
        if Param[j] = '+' then
          SwitchOn := True
        else if Param[j] = '-' then
          SwitchOn := False
        else if UpCase(Param[j]) = 'Q' then
          fQuietMode := SwitchOn
        else if UpCase(Param[j]) = 'K' then
          fPause := SwitchOn
        else if UpCase(Param[j]) = 'N' then
          fNoQuestionsMode := SwitchOn
        else
          ModifySwitchs(UpCase(Param[j]),SwitchOn);
      end;
  end;
end; {GetCommandLineParamSwitchs}

procedure DisplayParams;
var
  i : integer;
begin
  fStubObj.ShowMessage('');
  fStubObj.ShowMessage('Parameters:');
  for i := 0 to ParamCount do
    fStubObj.ShowMessage(Indent + '[' + IntToStr(i) + '] - ' + ParamStr(i));
end; {DisplayParams}

procedure DisplaySwitches;
var
  i : TCocoSwitch;
begin
  fStubObj.ShowMessage('');
  fStubObj.ShowMessage('Final switches:');
  for i := low(TCocoSwitch) to High(TCocoSwitch) do
    if i IN fOptions.Switches.SwitchSet then
      fStubObj.ShowMessage(Indent + fOptions.Switches.SwitchChar(i) + ' - '
          + fOptions.Switches.SwitchString(i));

  if fNoQuestionsMode then
    fStubObj.ShowMessage(Indent + NoQuestionsModeStr);
  if fQuietMode then
    fStubObj.ShowMessage(Indent + NoQuestionsModeStr);
  if fPause then
    fStubObj.ShowMessage(Indent + PauseStr);
end; {DisplaySwitches}

procedure DisplayOtherOptions;
begin
  fStubObj.ShowMessage('');
  fStubObj.ShowMessage('Other Options');
  fStubObj.ShowMessage(Indent + 'Grammar Name: ' + fOptions.GrammarName);
  fStubObj.ShowMessage(Indent + 'File Name: ' + fOptions.AGI.FileName);
  fStubObj.ShowMessage(Indent + 'Frame Ancestor: ' + fOptions.AGI.FrameAncestor);
  fStubObj.ShowMessage(Indent + 'Frame Console: ' + fOptions.AGI.FrameConsole);
  fStubObj.ShowMessage(Indent + 'Frame Parser: ' + fOptions.AGI.FrameParser);
  fStubObj.ShowMessage(Indent + 'Frame Test DFM: ' + fOptions.AGI.FrameTestDFM);
  fStubObj.ShowMessage(Indent + 'Frame Test Form: ' + fOptions.AGI.FrameTestForm);
  fStubObj.ShowMessage(Indent + 'Frame Test Project: ' + fOptions.AGI.FrameTestProject);
  fStubObj.ShowMessage(Indent + 'Hash Primary: ' + fOptions.AGI.HashPrimary);
  fStubObj.ShowMessage(Indent + 'Hash Secondary: ' + fOptions.AGI.HashSecondary);
  fStubObj.ShowMessage(Indent + 'Registration Palette: ' + fOptions.AGI.RegistrationPalette);
  fStubObj.ShowMessage(Indent + 'Expected Error: ' + fOptions.AGI.StringExpected);
  fStubObj.ShowMessage(Indent + 'Invalid Error: ' + fOptions.AGI.StringInvalid);
  fStubObj.ShowMessage(Indent + 'Not Expected Error: ' + fOptions.AGI.StringNotExpected);
  fStubObj.ShowMessage(Indent + 'Last Build Format: ' + fOptions.AGI.StringLastBuildFormat);
  fStubObj.ShowMessage(Indent + 'Version Format: ' + fOptions.AGI.StringVersionFormat);
  fStubObj.ShowMessage(Indent + 'Version Number: ' + fOptions.AGI.VersionNumber);
  fStubObj.ShowMessage(Indent + 'Version Info Code: ' + fOptions.AGI.VersionInfoCode);
  fStubObj.ShowMessage(Indent + 'Version Info Text: ' + fOptions.AGI.VersionInfoText);
end; {DisplayOtherOptions}

procedure DisplayInfo;
begin
  DisplayParams;
  DisplaySwitches;
  DisplayOtherOptions;
end; {DisplaySwitches}

function GetATGFileName : string;
var
  i : integer;
begin
  for i := 1 to ParamCount do
    if NOT (ParamStr(i)[1] IN ['+','-']) then
    begin
      Result := ParamStr(i);
      Break;
    end;
  if ExtractFileExt(Result) = '' then
    Result := Result + ATG_FILE_EXT;
end; {GetATGFileName}

procedure LoadCommandLineDefaultsFromIniFile;
var
  IniFileName : string;
  IniFile : TIniFile;
  i : TCocoSwitch;
  SwitchChar : string;
begin
  IniFileName := ChangeFileExt(ParamStr(0),AGI_FILE_EXT);
  if FileExists(IniFileName) then
  begin
    IniFile := TIniFile.Create(IniFileName);
    try
      fOptions.AGI.FrameAncestor := fOptions.AGI.UnAliasDirectory(IniFile.ReadString('Frames', 'Ancestor', ''));
      fOptions.AGI.FrameParser := fOptions.AGI.UnAliasDirectory(IniFile.ReadString('Frames', 'Parser', ''));
      fOptions.AGI.FrameTestProject := fOptions.AGI.UnAliasDirectory(IniFile.ReadString('Frames', 'Test Project', ''));
      fOptions.AGI.FrameTestForm := fOptions.AGI.UnAliasDirectory(IniFile.ReadString('Frames', 'Test Form', ''));
      fOptions.AGI.FrameTestDFM := fOptions.AGI.UnAliasDirectory(IniFile.ReadString('Frames', 'Test DFM', ''));
      fOptions.AGI.FrameConsole := fOptions.AGI.UnAliasDirectory(IniFile.ReadString('Frames', 'Console', ''));
      fOptions.AGI.OutputDirectory := fOptions.AGI.UnAliasDirectory(IniFile.ReadString('Output', 'Directory',
          ExtractFilePath(ParamStr(0))));
      fOptions.Switches.SwitchSet := [];
      for i := LowSwitch to HighSwitch do
      begin
        SwitchChar := fOptions.Switches.SwitchChar(i);
        if IniFile.ReadBool('Options', SwitchChar, false) then
          fOptions.Switches.SwitchSet := fOptions.Switches.SwitchSet + [i];
      end;
      fOptions.AGI.RegistrationPalette  := IniFile.ReadString('Registration', 'Palette', 'Coco/R');
    finally
      IniFile.Free;
    end;
  end;
end; {LoadCommandLineDefaultsFromIniFile}

function ListFileName : string;
begin
  Result := ChangeFileExt(fCocoCo.GrammarSourceFileName, LST_FILE_EXT);
end; {ListFileName}

function XmlFileName : string;
begin
  Result := ChangeFileExt(fCocoCo.GrammarSourceFileName, '.XML');
end; {XmlFileName}

procedure PrependVerboseMessagesToListFile;
var
  FileName : string;
  CurrentFileContents : TStringList;
begin
  fVerboseMessages.Add('================================================================================');
  fVerboseMessages.Add('');
  FileName := ListFileName;
  if FileExists(FileName) then
    begin
      CurrentFileContents := TStringList.Create;
      try
        CurrentFileContents.LoadFromFile(FileName);
        fVerboseMessages.AddStrings(CurrentFileContents);
        fVerboseMessages.SaveToFile(FileName);
      finally
        FreeAndNIL(CurrentFileContents);
      end;
    end
  else
    fVerboseMessages.SaveToFile(FileName);
end; {PrependVerboseMessagesToListFile}

var
  IniSwitches : TCocoSwitchSet;
  AGIFileName : string;
  Success : boolean;
begin
  Success := TRUE;
  Create;
  try
    ShowTitleAndVersion;
    if (ParamCount = 0) OR (ParamStr(1) = '?') OR (ParamStr(1) = '/?') then
      ShowHelp
    else
    begin
      fCocoCo.GrammarSourceFileName := GetATGFileName;
      AGIFileName := ChangeFileExt(fCocoCo.GrammarSourceFileName, AGI_FILE_EXT);

      if FileExists(ListFileName) then
        DeleteFile(ListFileName);
      if FileExists(XmlFileName) then
        DeleteFile(XmlFileName);
      if NOT FileExists(fCocoCo.GrammarSourceFileName) then
      begin
        DisplayParams;
        fCocoCo.AddError(etMetaError, META_ERROR_SOURCE_NOT_FOUND);
        fStubObj.ShowMessage(ATG_EXT + ' file "' + fCocoCo.GrammarSourceFileName + '" not found.');
        Success := false;
      end
      else
      begin
        LoadCommandLineDefaultsFromIniFile;
        fCocoCo.LoadFromIniFile(AGIFileName);
        IniSwitches := fOptions.Switches.SwitchSet;
        GetCommandLineParamSwitchs;
        fStubObj.ShowMessage('Compiling: ' + fCocoCo.GrammarSourceFileName);
        fCocoCo.Options := fOptions;
        if (crsVerboseMessages IN fCocoCo.Options.Switches.SwitchSet) then
          DisplayInfo;
        Success := fCocoCo.Execute;
        if Success then
          fOptions.Switches.SwitchSet := IniSwitches;
      end; {ParamCount > 0}
    end;
    if (NOT Success) OR (crsVerboseMessages IN fCocoCo.Options.Switches.SwitchSet) then
      PrependVerboseMessagesToListFile;
  finally
    Destroy;
    if fPause AND (NOT fNoQuestionsMode) then
    begin
      Writeln;
      Write('Press return to continue...');
      Readln;
    end;
    if NOT Success then
      Halt(1);
  end;
end {cCocoR} .

