unit CocoOptions;
{$INCLUDE CocoCD.inc}

interface

uses
  StringTools, CocoSwitch, CocoAGI, CocoDefs;

type
  TCocoOptions = class(TObject)
  private
    fCocoAGI: TCocoAGI;
    fSwitches: TCocoSwitches;
    fGrammarName: AnsiString;
    fAGIFileName: AnsiString;
    fOnShowMessage: TCocoShowMessage;
    procedure SetGrammarName(const Value: AnsiString);
    procedure SetOnShowMessage(const Value: TCocoShowMessage);
  public
    constructor Create;
    destructor Destroy; override;

    procedure ClearAGI;

    property Switches : TCocoSwitches read fSwitches write fSwitches;
    property AGI : TCocoAGI read fCocoAGI write fCocoAGI;
    property GrammarName : AnsiString read fGrammarName write SetGrammarName;
    property AGIFileName : AnsiString read fAGIFileName;

    property OnShowMessage : TCocoShowMessage read fOnShowMessage write SetOnShowMessage;
  end; {TCocoOptions}

implementation

uses
  SysUtils;

{ TCocoOptions }

procedure TCocoOptions.ClearAGI;
begin
  fGrammarName := '';
  fSwitches.Clear;
  fCocoAGI.Clear;
end; {ClearAGI}

constructor TCocoOptions.Create;
begin
  fSwitches := TCocoSwitches.Create;
  fCocoAGI := TCocoAGI.Create;
  fCocoAGI.Switches := fSwitches;
end; {Create}

destructor TCocoOptions.Destroy;
begin
  if Assigned(fCocoAGI) then
    FreeAndNil(fCocoAGI);
  if Assigned(fSwitches) then
    FreeAndNIL(fSwitches);
  inherited;
end; {Destroy}

procedure TCocoOptions.SetGrammarName(const Value: AnsiString);
begin
  fGrammarName := Value;
  fAGIFileName := ChangeFileExt(fGrammarName,AGI_FILE_EXT);
end; {SetGrammarName}

procedure TCocoOptions.SetOnShowMessage(const Value: TCocoShowMessage);
begin
  fOnShowMessage := Value;
  fCocoAGI.OnShowMessage := Value;
end; {SetOnShowMessage}

end.

