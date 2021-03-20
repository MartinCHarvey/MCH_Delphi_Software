unit PaletteSettingsDialog;

interface

uses Windows, SysUtils, Classes, Graphics, Forms, Controls, StdCtrls,
  Buttons, ExtCtrls, Dialogs, Grids, ComCtrls, FractSettings;

type
  TPaletteSettingsDlg = class(TForm)
    OKBtn: TButton;
    CancelBtn: TButton;
    Bevel1: TBevel;
    ColorDialog1: TColorDialog;
    NewBtn: TButton;
    DelBtn: TButton;
    ResetBtn: TButton;
    Panel1: TPanel;
    DrawGrid1: TDrawGrid;
    PaintBox1: TPaintBox;
    Image1: TImage;
    Label1: TLabel;
    PalLenLbl: TLabel;
    TrackBar1: TTrackBar;
    LogCheck: TCheckBox;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure DrawGrid1DrawCell(Sender: TObject; ACol, ARow: Integer;
      Rect: TRect; State: TGridDrawState);
    procedure PaintBox1Paint(Sender: TObject);
    procedure DrawGrid1SelectCell(Sender: TObject; ACol, ARow: Integer;
      var CanSelect: Boolean);
    procedure DrawGrid1DblClick(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure DelBtnClick(Sender: TObject);
    procedure NewBtnClick(Sender: TObject);
    procedure ResetBtnClick(Sender: TObject);
    procedure PaintBox1MouseDown(Sender: TObject; Button: TMouseButton;
      Shift: TShiftState; X, Y: Integer);
    procedure PaintBox1DragOver(Sender, Source: TObject; X, Y: Integer;
      State: TDragState; var Accept: Boolean);
    procedure PaintBox1DragDrop(Sender, Source: TObject; X, Y: Integer);
    procedure TrackBar1Change(Sender: TObject);
    procedure LogCheckClick(Sender: TObject);
  private
    { Private declarations }
    FLFPalette: TFractPalette;
    FGridSizeSet: boolean;
    FTrackLoading: boolean;
    FLogLoading: boolean;
    procedure RefreshGrid;
    procedure RefreshSliders;
    procedure RefreshImage;
    procedure RefreshTrack;
    procedure RefreshLogChk;
    procedure RefreshAll;
  public
    { Public declarations }
    procedure LoadFromClasses(Palette: TFractPalette);
    property DlgLocalPalette: TFractPalette read FLFPalette;
  end;

var
  PaletteSettingsDlg: TPaletteSettingsDlg;

implementation

uses FractMisc;

{$R *.DFM}

procedure TPaletteSettingsDlg.RefreshGrid;
begin
  if not FGridSizeSet then
  begin
    if DrawGrid1.DefaultRowHeight <> DrawGrid1.ClientHeight then
      DrawGrid1.DefaultRowHeight := DrawGrid1.ClientHeight;
    if DrawGrid1.DefaultColWidth <> DrawGrid1.DefaultRowHeight then
      DrawGrid1.DefaultColWidth := DrawGrid1.DefaultRowHeight;
    if DrawGrid1.RowCount <> 1 then
      DrawGrid1.RowCount := 1;
    FGridSizeSet := true;
  end;
  if DrawGrid1.ColCount <> Integer(FLFPalette.NumMarks) then
    DrawGrid1.ColCount := FLFPalette.NumMarks;
  DrawGrid1.Invalidate;
end;

procedure TPaletteSettingsDlg.RefreshSliders;
begin
  PaintBox1.Invalidate;
end;

procedure TPaletteSettingsDlg.RefreshTrack;
begin
  FTrackLoading := true;
  try
    TrackBar1.Position := FLFPalette.LengthOrder;
    PalLenLbl.Caption := IntToStr(Pow2Val(FLFPalette.LengthOrder));
  finally
    FTrackLoading := false;
  end;
end;

procedure TPaletteSettingsDlg.RefreshLogChk;
begin
  FLogLoading := true;
  try
    LogCheck.Checked := FLFPalette.Logarithmic;
  finally
    FLogLoading := false;
  end;
end;

procedure TPaletteSettingsDlg.TrackBar1Change(Sender: TObject);
begin
  if not FTrackLoading then
  begin
    FLFPalette.LengthOrder := TrackBar1.Position;
    RefreshTrack;
  end;
end;

procedure TPaletteSettingsDlg.LogCheckClick(Sender: TObject);
begin
  if not FLogLoading then
  begin
    FLFPalette.Logarithmic := LogCheck.Checked;
    RefreshLogChk;
  end;
end;


procedure TPaletteSettingsDlg.LoadFromClasses(Palette: TFractPalette);
begin
  FLFPalette.Assign(Palette);
  FTrackLoading := true;
  try
    TrackBar1.Min := MinPaletteOrder;
    TrackBar1.Max := MaxPaletteOrder;
  finally
    FTrackLoading := false;
  end;
end;

procedure TPaletteSettingsDlg.RefreshAll;
begin
  RefreshGrid;
  RefreshSliders;
  RefreshImage;
  RefreshTrack;
  RefreshLogChk;
end;

procedure TPaletteSettingsDlg.FormShow(Sender: TObject);
begin
  RefreshAll;
end;

procedure TPaletteSettingsDlg.FormCreate(Sender: TObject);
begin
  FLFPalette := TFractPalette.Create;
end;

procedure TPaletteSettingsDlg.FormDestroy(Sender: TObject);
begin
  FLFPalette.Free;
end;

procedure TPaletteSettingsDlg.DrawGrid1DrawCell(Sender: TObject; ACol,
  ARow: Integer; Rect: TRect; State: TGridDrawState);
var
  DGCanvas: TCanvas;
  CMData: TColorMarkData;
  Color, InvColor: TColor;
  CircRect: TRect;
  CircRad: integer;
  CenterPoint: TPoint;
begin
  Assert(ARow = 0);
  DGCanvas := DrawGrid1.Canvas;
  CMData := FLFPalette.ColorMarks[ACol];
  Color := PackedColorToColor(CMData.Color);
  DGCanvas.Brush.Color := Color;
  DGCanvas.Pen.Color := Color;
  DGCanvas.FillRect(Rect);
  if ACol = DrawGrid1.Col then
  begin
    InvColor := Color xor $00FFFFFF;
    DGCanvas.Brush.Color := InvColor;
    DGCanvas.Pen.Color := InvColor;
    //If selected, draw a blob of opposite color.
    CenterPoint.X := (Rect.Left + Rect.Right) div 2;
    CenterPoint.Y := (Rect.Top + Rect.Bottom) div 2;
    CircRad := (Rect.Right - Rect.Left) div 8;
    CircRect.Left := CenterPoint.x - CircRad;
    CircRect.Right := CenterPoint.x + CircRad;
    CircRect.Top := CenterPoint.y - CircRad;
    CircRect.Bottom := CenterPoint.y + CircRad;
    DGCanvas.Ellipse(CircRect);
  end;
end;

procedure TPaletteSettingsDlg.DrawGrid1SelectCell(Sender: TObject; ACol,
  ARow: Integer; var CanSelect: Boolean);
begin
  RefreshSliders;
end;

procedure TPaletteSettingsDlg.DrawGrid1DblClick(Sender: TObject);
var
  CMData: TColorMarkData;
begin
  CMData := FLFPalette.ColorMarks[DrawGrid1.Col];
  ColorDialog1.Color := PackedColorToColor(CMData.Color);
  if ColorDialog1.Execute then
  begin
    CMData.Color := ColorToPackedColor(ColorDialog1.Color);
    FLFPalette.SetColorMark(CMData);
    RefreshGrid;
    RefreshImage;
  end;
end;

procedure TPaletteSettingsDlg.PaintBox1MouseDown(Sender: TObject;
  Button: TMouseButton; Shift: TShiftState; X, Y: Integer);
var
  Loc: double;
  MarkX: integer;
  MarkY: integer;
  MarkWidth: integer;
begin
  if Button = mbLeft then
  begin
    Loc := FLFPalette.ColorMarks[DrawGrid1.Col].Loc;
    MarkX := Trunc(Loc * PaintBox1.Width);
    MarkY := PaintBox1.Height div 2;
    MarkWidth := PaintBox1.Height div 4;
    if (X > MarkX - MarkWidth) and (X < MarkX + MarkWidth)
      and (Y > MarkY - MarkWidth) and (Y < MarkY + MarkWidth) then
      PaintBox1.BeginDrag(false);
  end;
end;

procedure TPaletteSettingsDlg.PaintBox1Paint(Sender: TObject);
var
  MarkerColor: TColor;
  idx: cardinal;
  CMData: TColorMarkData;
  CircRect: TRect;
  CircCX: integer;
  CircRad: integer;
begin
  with PaintBox1.Canvas do
  begin
    Brush.Color := clBtnFace;
    FillRect(PaintBox1.ClientRect);
    //Now draw markers.
    for idx := 0 to Pred(FLFPalette.NumMarks) do
    begin
      CMData := FLFPalette.ColorMarks[idx];
      if Integer(idx) = DrawGrid1.Col then
        MarkerColor := clActiveCaption
      else
        MarkerColor := clInactiveCaption;
      Brush.Color := MarkerColor;
      Pen.Color := MarkerColor;
      CircCX := Trunc(CMData.Loc * PaintBox1.Width);
      CircRad := PaintBox1.Height div 4;
      CircRect.Left := CircCX - CircRad;
      CircRect.Right := CircCX + CircRad;
      CircRect.Top := (PaintBox1.Height div 2) - CircRad;
      CircRect.Bottom := (PaintBox1.Height div 2) + CircRad;
      Ellipse(CircRect);
    end;
  end;
end;

procedure TPaletteSettingsDlg.RefreshImage;
var
  PPix: PPackedColor;
  idx, datlen: cardinal;

begin
  with Image1.Picture.Bitmap do
  begin
    if PixelFormat <> pf32Bit then
      PixelFormat := pf32Bit;
    if Width <> Image1.Width then
      Width := Image1.Width;
    if Height <> Image1.Height then
      Height := Image1.Height;
    Assert(Width > 0);
    Assert(Height > 1);
    //Calc the first line.
    PPix := ScanLine[0];
    for idx := 0 to Pred(Width) do
    begin
      PPix^ := FLFPalette.ShadeArbitraryLen(idx, Width);
      Inc(PPix);
    end;
    datlen := sizeof(PPix^) * Width;
    for idx := 1 to Pred(Height) do
      CopyMemory(ScanLine[idx], Scanline[0], datlen);
  end;
  Image1.Invalidate;
end;


procedure TPaletteSettingsDlg.DelBtnClick(Sender: TObject);
begin
  FLFPalette.DeleteMark(DrawGrid1.Col);
  RefreshAll;
end;

procedure TPaletteSettingsDlg.NewBtnClick(Sender: TObject);
var
  PrevMarkLoc, NextMarkLoc: double;
  BestDistance, BestLoc: double;
  NewMark: TColorMarkData;
  idx: cardinal;
begin
  BestDistance := 0;
  BestLoc := 0;
  for idx := 0 to Pred(FLFPalette.NumMarks) do
  begin
    PrevMarkLoc := FLFPalette.ColorMarks[idx].Loc;
    if idx = Pred(FLFPalette.NumMarks) then
      NextMarkLoc := FLFPalette.ColorMarks[0].Loc + 1
    else
      NextMarkLoc := FLFPalette.ColorMarks[Succ(idx)].Loc;
    if NextMarkLoc - PrevMarkLoc > BestDistance then
    begin
      BestDistance := NextMarkLoc - PrevMarkLoc;
      BestLoc := (NextMarkLoc + PrevMarkLoc) / 2;
      if not (BestLoc < 1) then
        BestLoc := BestLoc - 1;
    end;
  end;
  Assert(BestLoc >= 0);
  Assert(BestLoc < 1);
  NewMark.Loc := BestLoc;
  NewMark.Color := FLFPalette.ShadeRealLoc(BestLoc);
  FLFPalette.SetColorMark(NewMark);
  RefreshAll;
end;

procedure TPaletteSettingsDlg.ResetBtnClick(Sender: TObject);
begin
  FLFPalette.ResetAllMarks;
  RefreshAll;
end;

procedure TPaletteSettingsDlg.PaintBox1DragOver(Sender, Source: TObject; X,
  Y: Integer; State: TDragState; var Accept: Boolean);
begin
  if Sender = Source then
    Accept := true;
end;

procedure TPaletteSettingsDlg.PaintBox1DragDrop(Sender, Source: TObject; X,
  Y: Integer);
var
  SrcIdx: integer;
  SrcDat: TColorMarkData;
  NewLoc: double;
begin
  //Now, remember, can't delete the last mark, so need to
  //add the "new" mark before deleting the "old" one.
  SrcIdx := DrawGrid1.Col;
  SrcDat := FLFPalette.ColorMarks[SrcIdx];
  NewLoc := X / PaintBox1.Width;
  if NewLoc = SrcDat.Loc then
    exit;
  if NewLoc < SrcDat.Loc then
    Inc(SrcIdx);
  SrcDat.Loc := NewLoc;
  FLFPalette.SetColorMark(SrcDat);
  FLFPalette.DeleteMark(SrcIdx);
  RefreshSliders;
  RefreshImage;
end;

end.
