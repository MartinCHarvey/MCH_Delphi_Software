unit CocoAGI;
{$INCLUDE CocoCD.inc}

interface

uses
  CocoDefs, StringTools, CocoSwitch, IniFiles, Classes;

const
  VERSION_NUMBER_STR = 'Version Number';
  LAST_BUILD_STR = 'Last Build';

type
  TCocoAGI = class(TObject)
  private
    fFileName : AnsiString;

    fMajorVersion: integer;
    fBuild: integer;
    fMinorVersion: integer;
    fRelease: integer;
    fRegistrationPalette: AnsiString;
    fLastBuild: TDateTime;
    fVersionInfo: TStringList;
    fOutputDirectory: AnsiString;
    fHashSecondary: AnsiString;
    fFrameParser: AnsiString;
    fStringInvalid: AnsiString;
    fFrameTestDFM: AnsiString;
    fFrameConsole: AnsiString;
    fMessageShowLL1StartSucc: boolean;
    fFrameAncestor: AnsiString;
    fFrameTestProject: AnsiString;
    fStringExpected: AnsiString;
    fStringNotExpected: AnsiString;
    fHashPrimary: AnsiString;
    fFrameTestForm: AnsiString;
    fMessageShowWarnings: boolean;
    fSwitches: TCocoSwitches;
    fOnShowMessage: TCocoShowMessage;
    fStringVersionFormat: AnsiString;
    fStringLastBuildFormat: AnsiString;
    function GetVersionNumber: AnsiString;
    procedure ReadFrameInformation(const IniFile: TIniFile);
    procedure ReadSwitchInformation(const IniFile: TIniFile);
    procedure ReadVersionInformation(const IniFile: TIniFile);
    procedure ReadMiscInformation(const IniFile: TIniFile);
    procedure ReadStringInformation(const IniFile: TIniFile);

    procedure SetHashPrimary(const Value: AnsiString);
    procedure SetHashSecondary(const Value: AnsiString);
    procedure SetRegistrationPalette(const Value: AnsiString);
    function GetVersionInfoText: AnsiString;
    function GetVersionInfoCode: AnsiString;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Clear;
    procedure LoadFromFile; overload;
    procedure LoadFromFile(const aFileName : AnsiString); overload;
    function UnAliasDirectory(const Dir: AnsiString): AnsiString;

    property FileName : AnsiString read fFileName write fFileName;
    property Switches : TCocoSwitches read fSwitches write fSwitches;

    { MISCELLANEOUS Information }
    property RegistrationPalette : AnsiString
        read fRegistrationPalette write SetRegistrationPalette;
    property OutputDirectory : AnsiString read fOutputDirectory write fOutputDirectory;

    { VERSION Information }
    property VersionNumber : AnsiString read GetVersionNumber;
    property LastBuild : TDateTime read fLastBuild write fLastBuild;
    property VersionInfo : TStringList read fVersionInfo write fVersionInfo;
    property VersionInfoText : AnsiString read GetVersionInfoText;
    property VersionInfoCode : AnsiString read GetVersionInfoCode;

    property MajorVersion : integer read fMajorVersion write fMajorVersion;
    property MinorVersion : integer read fMinorVersion write fMinorVersion;
    property Release : integer read fRelease write fRelease;
    property Build : integer read fBuild write fBuild;

    { FRAME Information }
    property FrameAncestor : AnsiString read fFrameAncestor write fFrameAncestor;
    property FrameParser : AnsiString read fFrameParser write fFrameParser;
    property FrameTestProject : AnsiString read fFrameTestProject write fFrameTestProject;
    property FrameTestForm : AnsiString read fFrameTestForm write fFrameTestForm;
    property FrameTestDFM : AnsiString read fFrameTestDFM write fFrameTestDFM;
    property FrameConsole : AnsiString read fFrameConsole write fFrameConsole;

    { MESSAGE Information }
    property MessageShowWarnings : boolean read fMessageShowWarnings write fMessageShowWarnings;
    property MessageShowLL1StartSucc : boolean read fMessageShowLL1StartSucc write fMessageShowLL1StartSucc;

    { HASH Information }
    property HashPrimary : AnsiString read fHashPrimary write SetHashPrimary;
    property HashSecondary : AnsiString read fHashSecondary write SetHashSecondary;

    { AnsiString Information }
    property StringExpected : AnsiString read fStringExpected write fStringExpected;
    property StringInvalid : AnsiString read fStringInvalid write fStringInvalid;
    property StringNotExpected : AnsiString read fStringNotExpected write fStringNotExpected;
    property StringLastBuildFormat : AnsiString read fStringLastBuildFormat write fStringLastBuildFormat;
    property StringVersionFormat : AnsiString read fStringVersionFormat write fStringVersionFormat;

    { Events }
    property OnShowMessage : TCocoShowMessage read fOnShowMessage write fOnShowMessage;
  end; {TCocoAGI}

implementation

uses
   SysUtils, Vcl.Forms;

const
  HomeDirAlias = '$(CocoR)';
  Expected_Msg = '%D% expected';
  Invalid_Msg = 'invalid %D%';
  Not_Expected_Msg = 'this symbol not expected in %D%';
   
{ TCocoAGI }

procedure TCocoAGI.Clear;
begin
  fFileName := '';

  fVersionInfo.Clear;
  fMajorVersion := 0;
  fBuild := 0;
  fMinorVersion := 0;
  fRelease := 0;
  fLastBuild := 0;

  RegistrationPalette := '';
  fOutputDirectory := '';

  HashSecondary := '';
  HashPrimary := '';

  fMessageShowLL1StartSucc := TRUE;
  fMessageShowWarnings := TRUE;
  fStringLastBuildFormat := '';
  fStringVersionFormat := '';
end; {Clear}

constructor TCocoAGI.Create;
begin
  fVersionInfo := TStringList.Create;
  Clear;
end; {Create}

destructor TCocoAGI.Destroy;
begin
  if Assigned(fVersionInfo) then
    FreeAndNil(fVersionInfo);
  inherited;
end; {Destroy}

function TCocoAGI.GetVersionNumber: AnsiString;
begin
  Result := '';
end; {GetVersionNumber}

function TCocoAGI.UnAliasDirectory(const Dir: AnsiString): AnsiString;
var
  PathFollowingAlias : AnsiString;
begin
  if Uppercase(copy(Dir,1,length(HomeDirAlias))) = UpperCase(HomeDirAlias) then
    begin
      PathFollowingAlias := copy(Dir,length(HomeDirAlias) + 1,MaxInt);
      while (Length(PathFollowingAlias) > 0) AND (PathFollowingAlias[1] = SysUtils.PathDelim) do
        Delete(PathFollowingAlias, 1, 1);
      Result := IncludeTrailingPathDelimiter(ExtractFilePath(Application.ExeName))
          + PathFollowingAlias;
    end
  else
    Result := Dir;
end; {UnAliasDirectory}

procedure TCocoAGI.ReadFrameInformation(const IniFile : TIniFile);
begin
  fFrameAncestor := UnAliasDirectory(IniFile.ReadString('Frames', 'Ancestor', ''));
  fFrameParser := UnAliasDirectory(IniFile.ReadString('Frames', 'Parser', ''));
  fFrameTestProject := UnAliasDirectory(IniFile.ReadString('Frames', 'Test Project', ''));
  fFrameTestForm := UnAliasDirectory(IniFile.ReadString('Frames', 'Test Form', ''));
  fFrameTestDFM := UnAliasDirectory(IniFile.ReadString('Frames', 'Test DFM', ''));
  fFrameConsole := UnAliasDirectory(IniFile.ReadString('Frames', 'Console', ''));
end; {ReadFrameInformation}

procedure TCocoAGI.ReadStringInformation(const IniFile : TIniFile);
begin
  fStringExpected := IniFile.ReadString('Error Msg', 'Expected', Expected_Msg);
  fStringInvalid := IniFile.ReadString('Error Msg', 'Invalid', Invalid_Msg);
  fStringNotExpected := IniFile.ReadString('Error Msg', 'Not Expected', Not_Expected_Msg);
  fStringLastBuildFormat := IniFile.ReadString('Strings', 'Last Build Format', '');
  fStringVersionFormat := IniFile.ReadString('Strings', 'Version Format', '');
end; {ReadStringInformation}

procedure TCocoAGI.ReadMiscInformation(const IniFile: TIniFile);
begin
  fOutputDirectory := UnAliasDirectory(IniFile.ReadString('Output', 'Directory', ''));
  RegistrationPalette := IniFile.ReadString('Registration', 'Palette', 'Coco/R');

  fMessageShowWarnings := IniFile.ReadBool('Messages','Show Warnings',TRUE);
  fMessageShowLL1StartSucc := IniFile.ReadBool('Messages','Show LL(1) Start Succ',TRUE);
  HashPrimary := IniFile.ReadString('Hash','Primary','');
  HashSecondary := IniFile.ReadString('Hash','Secondary','');
end; {ReadMiscInformation}

procedure TCocoAGI.ReadSwitchInformation(const IniFile: TIniFile);
var
  Switch : TCocoSwitch;
  SwitchChar : AnsiString;
begin
  fSwitches.Clear;
  for Switch := low(TCocoSwitch) to High(TCocoSwitch) do
  begin
    SwitchChar := fSwitches.SwitchChar(Switch);
    if IniFile.ReadBool('Options', SwitchChar, Switch IN [crsSpaceAsWhitespace]) then
      fSwitches.SwitchSet := fSwitches.SwitchSet + [Switch];
  end;
end; {ReadSwitchInformation}

procedure TCocoAGI.ReadVersionInformation(const IniFile: TIniFile);
var
  TempStr : AnsiString;
  VersionSection : TStringList;
  i : integer;
  LastBuildStr : AnsiString;
begin
  TempStr := IniFile.ReadString('Version Info', 'Version Number', '');
  if TempStr = '' then
  begin
    fMajorVersion := IniFile.ReadInteger('Version Info', 'Major Version', 0);
    IniFile.DeleteKey('Version Info','Major Version');
    fMinorVersion := IniFile.ReadInteger('Version Info', 'Minor Version', 0);
    IniFile.DeleteKey('Version Info','Minor Version');
    fRelease := IniFile.ReadInteger('Version Info', 'Release', 0);
    IniFile.DeleteKey('Version Info','Release');
    fBuild := IniFile.ReadInteger('Version Info', 'Next Build', 0);
    IniFile.DeleteKey('Version Info','Next Build');
  end
  else
  begin
    fMajorVersion := StrToIntDef(StrTok(TempStr,'.'),0);
    fMinorVersion := StrToIntDef(StrTok(TempStr,'.'),0);
    fRelease := StrToIntDef(StrTok(TempStr,'.'),0);
    fBuild := StrToIntDef(TempStr,0);
  end;
  LastBuildStr := IniFile.ReadString('Version Info', 'Last Build', '');
  fLastBuild := StrToDateTimeDef(LastBuildStr, 0);
  fVersionInfo.Clear;
  VersionSection := TStringList.Create;
  try
    IniFile.ReadSection('Version Info', VersionSection);
    for i := 0 to VersionSection.Count - 1 do
      if (UpperCase(VersionSection[i]) <> 'LAST BUILD')
          AND (UpperCase(VersionSection[i]) <> 'VERSION NUMBER') then
        fVersionInfo.Add(VersionSection[i] + '='
            + IniFile.ReadString('Version Info', VersionSection[i], ''));
  finally
    FreeAndNIL(VersionSection);
  end; {try..finally}
end; {ReadVersionInformation}

procedure TCocoAGI.LoadFromFile;
var
  IniFile : TIniFile;
begin
  if FileExists(fFileName) then
  begin
    IniFile := TIniFile.Create(fFileName);
    try
      ReadFrameInformation(IniFile);
      ReadSwitchInformation(IniFile);
      ReadVersionInformation(IniFile);
      ReadMiscInformation(IniFile);
      ReadStringInformation(IniFile);
    finally
      IniFile.Free;
    end;
  end;
end; {LoadFromFile}

procedure TCocoAGI.LoadFromFile(const aFileName: AnsiString);
begin
  fFileName := aFileName;
  LoadFromFile;
end; {LoadFromFile}

procedure TCocoAGI.SetHashPrimary(const Value: AnsiString);
begin
  fHashPrimary := Value;
  if Assigned(fSwitches) then
    fSwitches.PrimaryHash := fHashPrimary;
end; {SetHashPrimary}

procedure TCocoAGI.SetHashSecondary(const Value: AnsiString);
begin
  fHashSecondary := Value;
  if Assigned(fSwitches) then
    fSwitches.SecondaryHash := fHashSecondary;
end; {SetHashSecondary}

procedure TCocoAGI.SetRegistrationPalette(const Value: AnsiString);
begin
  fRegistrationPalette := Value;
  if Assigned(fSwitches) then
    fSwitches.RegistrationPalette := fRegistrationPalette;
end; {SetRegistrationPalette}

function TCocoAGI.GetVersionInfoCode: AnsiString;
var
  i : integer;
begin
  Result := '';
  for i := 0 to VersionInfo.Count - 1 do
  begin
    Result := Result + AnsiQuotedStr(StringReplace(VersionInfo[i],'=',': ',[rfReplaceAll]),#39);
    if i < VersionInfo.Count - 1 then
      Result := Result + ' + #13#10 +' + CRLF;
  end;
end; {GetVersionInfoText}

function TCocoAGI.GetVersionInfoText: AnsiString;
var
  i : integer;
begin
  Result := '';
  for i := 0 to VersionInfo.Count - 1 do
  begin
    Result := Result + StringReplace(VersionInfo[i],'=',': ',[rfReplaceAll]);
    if i < VersionInfo.Count - 1 then
      Result := Result + CRLF;
  end;
end; {GetVersionInfoText}

end.

