unit FractLegacy;

interface

uses
  FractSettings;

type
  { Legacy class. For backwards compat only }
  TFractSettings = class(TFractStreamable)
  private
    FFormula: TFractFormula;
    FScalePPU: double;
    FInitialPPU: double;
    FMultPPU: double;
    FBailoutOrder: cardinal;
    FPortionCount: cardinal;
    FPalette: TFractPalette;
    FRotRadians: double;
    FCenterX: double;
    FCenterY: double;
    FJuliaConstX: double;
    FJuliaConstY: double;
    FSavedOtherPPU:double;
  public
    constructor Create; override;
    destructor Destroy; override;
    function ConvertLegacySettings(var Err:string): TFractV2SettingsBundle;
  published
    property Formula: TFractFormula read FFormula write FFormula;
    property ScalePPU: double read FScalePPU write FScalePPU;
    property InitialPPU: double read FInitialPPU write FInitialPPU;
    property MultPPU: double read FMultPPU write FMultPPU;
    property BailoutOrder: cardinal read FBailoutOrder write FBailoutOrder;
    property PortionCount: cardinal read FPortionCount write FPortionCount;
    property Palette: TFractPalette read FPalette write FPalette;
    property RotRadians: double read FRotRadians write FRotRadians;
    property STREAM_CenterX: double read FCenterX write FCenterX;
    property STREAM_CenterY: double read FCenterY write FCenterY;
    property STREAM_JuliaConstX: double read FJuliaConstX write FJuliaConstX;
    property STREAM_JuliaConstY: double read FJuliaConstY write FJuliaConstY;
    property STREAM_SavedOtherPPU:double read FSavedOtherPPU write FSavedOtherPPU;
  end;

implementation

uses
  FractMisc;

constructor TFractSettings.Create;
begin
  inherited;
  if not CreatingInStreamer then
    FPalette := TFractPalette.Create;
  FCenterX := InitialMandelX;
  FCenterY := InitialMandelY;
  FScalePPU := InitialZoom;
  FSavedOtherPPU := InitialZoom;
  FInitialPPU := InitialZoom;
  FMultPPU := InitialMult;
  FBailoutOrder := InitialBailoutOrder;
  FPortionCount := InitialPortions;
  FRotRadians := 0;
end;

destructor TFractSettings.Destroy;
begin
  FPalette.Free;
  inherited;
end;

function TFractSettings.ConvertLegacySettings(var Err: string): TFractV2SettingsBundle;
var
  TP: TRealPoint;
begin
  result := TFractV2SettingsBundle.Create;
  result.Palette.Assign(FPalette);
  TP.X := FJuliaConstX;
  TP.Y := FJuliaConstY;
  with result.Formula do
  begin
    Formula := self.Formula;
    JuliaConst := TP;
    MultPPU := self.MultPPU;
    InitialPPU := self.InitialPPU;
    PortionCount := self.PortionCount;
    SavedOtherPPU := self.FSavedOtherPPU;
  end;
  TP.X := self.FCenterX;
  TP.Y := self.FCenterY;
  with result.Location do
  begin
    Center := TP;
    ScalePPU := self.ScalePPU;
    BailoutOrder := self.BailoutOrder;
    RotRadians := self.RotRadians;
  end;
  if not result.CheckSane(Err) then
  begin
    result.Free;
    result := nil;
  end;
end;

end.
