unit FractSettingsDialog;

interface

uses Windows, SysUtils, Classes, Graphics, Forms, Controls, StdCtrls,
  Buttons, ExtCtrls, ComCtrls, FractSettings;

type
  TFractSettingsDlg = class(TForm)
    OKBtn: TButton;
    CancelBtn: TButton;
    Bevel1: TBevel;
    Label1: TLabel;
    EditX: TEdit;
    Label2: TLabel;
    EditY: TEdit;
    EditZoom: TEdit;
    Label3: TLabel;
    Label4: TLabel;
    Label5: TLabel;
    Label6: TLabel;
    Label7: TLabel;
    ZoomInitTrack: TTrackBar;
    ZoomByTrack: TTrackBar;
    DivTrack: TTrackBar;
    DetTrack: TTrackBar;
    ZoomInitLbl: TLabel;
    ZoomByLbl: TLabel;
    DivLbl: TLabel;
    DetLbl: TLabel;
    JuliaX: TEdit;
    JuliaY: TEdit;
    Label8: TLabel;
    Label9: TLabel;
    Label10: TLabel;
    TypeLbl: TLabel;
    FlipBtn: TButton;
    EditRotate: TEdit;
    Label11: TLabel;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure TracksChanged(Sender: TObject);
    procedure OKBtnClick(Sender: TObject);
    procedure FlipBtnClick(Sender: TObject);
  private
    { Private declarations }
    FLFSettings: TFractV2SettingsBundle;
    Loading: boolean;
    procedure RefreshEdits;
    procedure SaveEdits;
    procedure LoadTracks;
    procedure RefreshLbls;
    procedure SetTrackSat(Track: TTrackBar; Val: integer);
  public
    procedure LoadFromClasses(Settings: TFractV2SettingsBundle);
    property DlgLocalSettings: TFractV2SettingsBundle read FLFSettings;
    { Public declarations }
  end;

var
  FractSettingsDlg: TFractSettingsDlg;

implementation

uses
  FractMisc;

const
  ZoomInitMult = 20;
  ZoomByMult = 0.5;

{$R *.DFM}

procedure TFractSettingsDlg.FormCreate(Sender: TObject);
begin
  FLFSettings := TFractV2SettingsBundle.Create;
end;

procedure TFractSettingsDlg.FormDestroy(Sender: TObject);
begin
  FLFSettings.Free;
end;

procedure TFractSettingsDlg.RefreshEdits;
begin
  EditX.Text := FloatToStr(FLFSettings.Location.Center.X);
  EditY.Text := FloatToStr(FLFSettings.Location.Center.Y);
  EditZoom.Text := FloatToStr(FLFSettings.Location.ScalePPU);
  EditRotate.Text := FloatToStr(FLFSettings.Location.RotRadians);
  JuliaX.Text := FloatToStr(FLFSettings.Formula.JuliaConst.X);
  JuliaY.Text := FloatToStr(FLFSettings.Formula.JuliaConst.Y);
  JuliaX.Enabled := FLFSettings.Formula.Formula = ffJulia;
  JuliaY.Enabled := FLFSettings.Formula.Formula = ffJulia;
  case FLFSettings.Formula.Formula of
    ffMandel: TypeLbl.Caption := 'Mandelbrot';
    ffJulia: TypeLbl.Caption := 'Julia';
  else
    Assert(false);
  end;
end;

procedure TFractSettingsDlg.SaveEdits;
var
  TRP: TRealPoint;
  OldX, OldY, OldScale, OldRot: string;
begin
  OldX := FloatToStr(FLFSettings.Location.Center.X);
  OldY := FloatToStr(FLFSettings.Location.Center.Y);
  OldScale := FloatToStr(FLFSettings.Location.ScalePPU);
  OldRot := FloatToStr(FLFSettings.Location.RotRadians);
  if (CompareStr(OldX, EditX.Text) <> 0) or
    (CompareStr(OldY, EditY.Text) <> 0) then
  begin
    TRP.X := StrToFloat(EditX.Text);
    TRP.Y := StrToFloat(EditY.Text);
    FLFSettings.Location.Center := TRP;
  end;
  if (CompareStr(OldScale, EditZoom.Text) <> 0) then
    FLFSettings.Location.ScalePPU := StrToFloat(EditZoom.Text);
  if (CompareStr(OldRot, EditRotate.Text) <> 0) then
    FLFSettings.Location.RotRadians := StrToFloat(EditRotate.Text);
  OldX := FloatToStr(FLFSettings.Formula.JuliaConst.X);
  OldY := FloatToStr(FLFSettings.Formula.JuliaConst.Y);
  if (CompareStr(OldX, JuliaX.Text) <> 0) or
    (CompareStr(OldY, JuliaY.Text) <> 0) then
  begin
    TRP.X := StrToFloat(JuliaX.Text);
    TRP.Y := StrToFloat(JuliaY.Text);
    FLFSettings.Formula.JuliaConst := TRP;
  end;
end;

procedure TFractSettingsdlg.RefreshLbls;
var
  bail: cardinal;
begin
  bail := Pow2Val(FLFSettings.Location.BailoutOrder);
  ZoomInitLbl.Caption := FloatToStr(FLFSettings.Formula.InitialPPU);
  ZoomByLbl.Caption := FLoatToStr(FLFSettings.Formula.MultPPU);
  DivLbl.Caption := IntToStr(FLFSettings.Formula.PortionCount);
  DetLbl.Caption := IntToStr(bail);
end;

procedure TFractSettingsdlg.SetTrackSat(Track: TTrackBar; Val: integer);
begin
  if (Val >= Track.Min) and (Val <= Track.Max) then
    Track.Position := Val
  else if (Val < Track.Min) then
    Track.Position := Track.Min
  else
    Track.Position := Track.Max;
end;

procedure TFractSettingsDlg.TracksChanged(Sender: TObject);
begin
  if not Loading then
  begin
    FLFSettings.Formula.InitialPPU := ZoomInitTrack.Position * ZoomInitMult;
    FLFSettings.Formula.MultPPU := ZoomByTrack.Position * ZoomByMult;
    FLFSettings.Formula.PortionCount := DivTrack.Position;
    FLFSettings.Location.BailoutOrder := DetTrack.Position;
    RefreshLbls;
  end;
end;

procedure TFractSettingsDlg.LoadTracks;
begin
  RefreshLbls;
  DetTrack.Min := MinBailoutOrder;
  DetTrack.Max := MaxBailoutOrder;
  SetTrackSat(ZoomInitTrack, Trunc(FLFSettings.Formula.InitialPPU / ZoomInitMult));
  SetTrackSat(ZoomByTrack, Trunc(FLFSettings.Formula.MultPPU / ZoomByMult));
  SetTrackSat(DivTrack, FLFSettings.Formula.PortionCount);
  SetTrackSat(DetTrack, FLFSettings.Location.BailoutOrder);
end;

procedure TFractSettingsDlg.LoadFromClasses(Settings: TFractV2SettingsBundle);
begin
  Loading := true;
  try
    FLFSettings.Assign(Settings);
    { Set up edits }
    RefreshEdits;
    LoadTracks;
  finally
    Loading := false;
  end;
end;


procedure TFractSettingsDlg.OKBtnClick(Sender: TObject);
begin
  SaveEdits;
end;

procedure TFractSettingsDlg.FlipBtnClick(Sender: TObject);
begin
  Loading := true;
  try
    SaveEdits;
    FLFSettings.FlipJulia;
  finally
    RefreshEdits;
    LoadTracks;
  end;
end;

end.
