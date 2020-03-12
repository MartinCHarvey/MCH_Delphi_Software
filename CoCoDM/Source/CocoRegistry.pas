unit CocoRegistry;
{$INCLUDE CocoCD.inc}

interface

uses
  Classes,
  Graphics;

type
  TCocoRegistry = class(TObject)
  private
    fDefaultFrameDir: AnsiString;
    fHomeDir : AnsiString;
    fDefaultErrorMsgExpected : AnsiString;
    fDefaultErrorMsgInvalid : AnsiString;
    fDefaultErrorMsgNotExpected : AnsiString;
    fDefaultVersionFormat: AnsiString;
    fDefaultLastBuildFormat: AnsiString;

    function GetHomeDir: AnsiString;
  public
    constructor Create;
    procedure LoadDefaults;
    procedure SaveDefaults;
    procedure SetHomeDir(const Dir: AnsiString);
    function AliasDirectory(const Dir : AnsiString) : AnsiString;
    function UnAliasDirectory(const Dir : AnsiString) : AnsiString;

    property DefaultErrorMsgExpected : AnsiString read fDefaultErrorMsgExpected write fDefaultErrorMsgExpected;
    property DefaultErrorMsgInvalid : AnsiString read fDefaultErrorMsgInvalid write fDefaultErrorMsgInvalid;
    property DefaultErrorMsgNotExpected : AnsiString read fDefaultErrorMsgNotExpected write fDefaultErrorMsgNotExpected;
    property DefaultLastBuildFormat : AnsiString read fDefaultLastBuildFormat write fDefaultLastBuildFormat;
    property DefaultVersionFormat : AnsiString read fDefaultVersionFormat write fDefaultVersionFormat;
    property DefaultFrameDir : AnsiString read fDefaultFrameDir write fDefaultFrameDir;
    property HomeDir : AnsiString read GetHomeDir;
  end; {TCocoRegistry}

implementation

uses
  StringTools, SysUtils, TZRegistry;

const
  HKEY_LOCAL_MACHINE = $80000002;
  HomeDirAlias = '$(CocoR)';
  BaseRegistry = 'Software\CocoR for Delphi';

  Expected_Msg = '%D% expected';
  Invalid_Msg = 'invalid %D%';
  Not_Expected_Msg = 'this symbol not expected in %D%';

procedure TCocoRegistry.LoadDefaults;
var
  Registry : TTZRegistry;
begin
  Registry := TTZRegistry.Create;
  try
    Registry.OpenKey(BaseRegistry + '\Defaults', TRUE);
    fDefaultFrameDir := UnAliasDirectory(Registry.ReadStringDef('Frame Dir', HomeDirAlias));

    fDefaultErrorMsgExpected := Registry.ReadStringDef(
        'Default Error Msg Expected', Expected_Msg);
    fDefaultErrorMsgInvalid := Registry.ReadStringDef(
        'Default Error Msg Invalid', Invalid_Msg);
    fDefaultErrorMsgNotExpected := Registry.ReadStringDef(
        'Default Error Msg Not Expected', Not_Expected_Msg);
    fDefaultVersionFormat := Registry.ReadStringDef('Default Version Format', '');
    fDefaultLastBuildFormat := Registry.ReadStringDef('Default Last Build Format', '');
  finally
    Registry.Free;
  end;
end;

procedure TCocoRegistry.SaveDefaults;
var
  Registry : TTZRegistry;
begin
  Registry := TTZRegistry.Create;
  try
    Registry.OpenKey(BaseRegistry + '\Defaults', TRUE);
    Registry.WriteString('Frame Dir',AliasDirectory(fDefaultFrameDir));
    Registry.WriteString('Default Error Msg Expected', fDefaultErrorMsgExpected);
    Registry.WriteString('Default Error Msg Invalid', fDefaultErrorMsgInvalid);
    Registry.WriteString('Default Error Msg Not Expected', fDefaultErrorMsgNotExpected);
    Registry.WriteString('Default Version Format', fDefaultVersionFormat);
    Registry.WriteString('Default Last Build Format', fDefaultLastBuildFormat);
  finally
    Registry.Free;
  end;
end;

function TCocoRegistry.GetHomeDir: AnsiString;
var
  Reg : TTZRegistry;
begin
  if fHomeDir = '' then
  begin
    Reg := TTZRegistry.Create;
    try
      Reg.RootKey := HKEY_LOCAL_MACHINE;
      Reg.OpenKey(BaseRegistry,TRUE);
      fHomeDir := Reg.ReadString('Path');
    finally
      Reg.Free;
    end;
  end;
  Result := fHomeDir;
end;

procedure TCocoRegistry.SetHomeDir(const Dir : AnsiString);
var
  Reg : TTZRegistry;
begin
  Reg := TTZRegistry.Create;
  try
    Reg.RootKey := HKEY_LOCAL_MACHINE;
    Reg.OpenKey(BaseRegistry, TRUE);
    Reg.WriteString('Path', Dir);
  finally
    Reg.Free;
  end;
end;

function TCocoRegistry.AliasDirectory(const Dir: AnsiString): AnsiString;
begin
  if Uppercase(copy(Dir,1,length(HomeDir))) = UpperCase(HomeDir) then
    Result := HomeDirAlias + copy(Dir,length(HomeDir) + 1,MaxInt)
  else
    Result := Dir;
end; {AliasDirectory}

function TCocoRegistry.UnAliasDirectory(const Dir: AnsiString): AnsiString;
begin
  if Uppercase(copy(Dir,1,length(HomeDirAlias))) = UpperCase(HomeDirAlias) then
    Result := HomeDir + copy(Dir,length(HomeDirAlias) + 1,MaxInt)
  else
    Result := Dir;
end; {UnAliasDirectory}

constructor TCocoRegistry.Create;
begin
  fHomeDir := '';
end; {Create}

end.

