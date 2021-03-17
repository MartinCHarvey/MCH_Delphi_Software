unit DragHelperFrm;

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, Forms, Dialogs,
  ExtCtrls;

type
  TFractDragType = (fdtMove, fdtZoom, fdtRotate);

  TGetBackgroundBitmapEvent = procedure(Sender: TObject;
                                        ScreenRect: TRect;
                                        var Bmp: TBitmap) of object;

  TDragHelperForm = class(TForm)
    PB: TPaintBox;
    procedure FormShow(Sender: TObject);
    procedure PBPaint(Sender: TObject);
  private
    { Private declarations }
    FDragType: TFractDragType;
    FDragMoveOffset: TPoint; //Copy of input param.
    FDragMagnitude: double;  //Copy of input param.
    FDragZoomConstant: double;
    FDragRotConstant: double;
    FDragAngle: double;
    UnitaryDistanceRot: Cardinal;
    UnitaryDistanceZoom: Cardinal;
    FBackgroundBmp: TBitmap;
    FOnGetBackgroundBmp: TGetBackgroundBitmapEvent;
  protected
    procedure PaintRotHelper(Sender: TObject);
    procedure PaintMoveHelper(Sender: TObject);
    procedure PaintZoomHelper(Sender: TObject);
  public
    { Public declarations }
    procedure UpdateForm(DragOffset: TPoint;
                         DragMagnitude: Cardinal;
                         DragAngle: double;
                         Button: TMouseButton);

    property DragType: TFractDragType read FDragType;
    property DragMoveOffset: TPoint read FDragMoveOffset; //Pixel offset for move by pels.
    property DragZoomConstant: double read FDragZoomConstant; //Radian rotation rot by rads.
    property DragRotateConstant: double read FDragRotConstant; //Zoom constant zoom by units.
    property OnGetBackgroundBmp: TGetBackgroundBitmapEvent
      read FOnGetBackgroundBmp write FOnGetBackgroundBmp;
  end;

var
  DragHelperForm: TDragHelperForm;

implementation

{$R *.DFM}

const
  OneEighth = Pi / 4;
  GlyphRadius = 3/8;

procedure TDragHelperForm.UpdateForm(DragOffset: TPoint;
                                 DragMagnitude: Cardinal;
                                 DragAngle: double;
                                 Button: TMouseButton);
begin
  if DragAngle < -Pi then
    DragAngle := DragAngle + 2 * Pi
  else if DragAngle > Pi then
    DragAngle := DragAngle - 2 * Pi;

  Assert(DragAngle >= -Pi);
  Assert(DragAngle <= Pi);
  FDragAngle := DragAngle;

  if Button = mbLeft then
  begin
    if ((DragAngle > -OneEighth) and (DragAngle < OneEighth))
      or ((DragAngle > OneEighth * 3) or (DragAngle < - OneEighth * 3)) then
      FDragType := fdtRotate
    else
      FDragType := fdtZoom;
  end
  else if Button = mbRight then
    FDragType := fdtMove;

  case FDragType of
    fdtRotate:
    begin
      ZeroMemory(@FDragMoveOffset, sizeof(FDragMoveOffset));
      FDragZoomConstant := 0;
      if DragMagnitude < UnitaryDistanceRot then
        FDragRotConstant := (DragMagnitude / UnitaryDistanceRot) * Pi
      else
        FDragRotConstant := Pi;
      if not ((DragAngle > -OneEighth) and (DragAngle < OneEighth)) then
        FDragRotConstant := FDragRotConstant * -1;
    end;
    fdtZoom:
    begin
      ZeroMemory(@FDragMoveOffset, sizeof(FDragMoveOffset));
      FDragRotConstant := 0;
      if DragAngle < 0 then //Up so zoom in,
        FDragZoomConstant := DragMagnitude / UnitaryDistanceZoom
      else //Down, so zoom out
        FDragZoomConstant := -(DragMagnitude / UnitaryDistanceZoom);
    end;
    fdtMove:
    begin
     FDragMoveOffset := DragOffset;
     FDragMagnitude := DragMagnitude;
     FDragZoomConstant := 0;
     FDragRotConstant := 0;
    end;
  end;

  PB.Invalidate;
end;

procedure TDragHelperForm.FormShow(Sender: TObject);
var
  ScreenRect: TRect;
begin
  Height := Height + (ClientWidth - ClientHeight);
  if Screen.Height > Screen.Width then
    UnitaryDistanceZoom := Screen.Width div 8
  else
    UnitaryDistanceZoom := Screen.Height div 8;
  UnitaryDistanceRot := UnitaryDistanceZoom * 2;
  ScreenRect.TopLeft := PB.ClientToScreen(PB.ClientRect.TopLeft);
  ScreenRect.BottomRight := PB.ClientToScreen(PB.ClientRect.BottomRight);
  if Assigned(FBackgroundBmp) then
    FreeAndNil(FBackgroundBmp);
  if Assigned(FOnGetBackgroundBmp) then
    FOnGetBackgroundBmp(Self, ScreenRect, FBackgroundBmp);
end;

procedure TDragHelperForm.PaintRotHelper(Sender: TObject);

const
  ArrowLen = 1/10;
  ArrowHeadLen = 1/20;

var
  C: TCanvas;
  P: TPen;
  B: TBrush;
  SegStart, SegEnd, Tmp: TPoint;
  GlyphCentre: TPoint;
  ArrowStart, ArrowEnd, AHead1, AHead2: TPoint;

begin
  C := (Sender as TPaintBox).Canvas;
  P := C.Pen;
  B := C.Brush;

  GlyphCentre.x := ClientWidth div 2;
  GlyphCentre.y := ClientHeight div 2;

  //Draw a little pie segment with the amount of rotation
  P.Color := clWindowText;
  B.Color := clActiveCaption;

  P.Width := Trunc(Abs(FDragRotConstant) + 1);
  
  Tmp.x := Trunc(GlyphCentre.x + Cos(FDragRotConstant) * ClientWidth * GlyphRadius);
  Tmp.y := Trunc(GlyphCentre.y + Sin(FDragRotConstant) * ClientHeight * GlyphRadius);
  if FDragRotConstant < 0 then
  begin
    SegStart.x := Trunc(GlyphCentre.x + Width * GlyphRadius);
    SegStart.y := GlyphCentre.y;
    SegEnd := Tmp;
  end
  else
  begin
    SegStart := tmp;
    SegEnd.x := Trunc(GlyphCentre.x + Width * GlyphRadius);
    SegEnd.y := GlyphCentre.y;
  end;
  C.Pie(GlyphCentre.x - Trunc(ClientWidth * GlyphRadius),
        GlyphCentre.y - Trunc(ClientHeight * GlyphRadius),
        GlyphCentre.x + Trunc(ClientWidth * GlyphRadius),
        GlyphCentre.y + Trunc(ClientHeight * GlyphRadius),
        SegStart.x, SegStart.y,
        SegEnd.x, SegEnd.y);

  //And now, just for a bit more fun, we'll draw a little arrow.
  if FDragRotConstant < 0 then
  begin
    ArrowStart := SegEnd;
    ArrowEnd.x := ArrowStart.x  + Trunc(Sin(FDragRotConstant) * ArrowLen * ClientWidth);
    ArrowEnd.y := ArrowStart.y  - Trunc(Cos(FDragRotConstant) * ArrowLen * ClientWidth);

    AHead1.x := ArrowEnd.x + Trunc(Sin(FDragRotConstant + OneEighth * 3) * ArrowHeadLen * ClientWidth);
    AHead1.y := ArrowEnd.y - Trunc(Cos(FDragRotConstant + OneEighth * 3) * ArrowHeadLen * ClientWidth);
    AHead2.x := ArrowEnd.x + Trunc(Sin(FDragRotConstant - OneEighth * 3) * ArrowHeadLen * ClientWidth);
    AHead2.y := ArrowEnd.y - Trunc(Cos(FDragRotConstant - OneEighth * 3) * ArrowHeadLen * ClientWidth);
  end
  else
  begin
    ArrowStart := SegStart;
    ArrowEnd.x := ArrowStart.x  - Trunc(Sin(FDragRotConstant) * ArrowLen * ClientWidth);
    ArrowEnd.y := ArrowStart.y  + Trunc(Cos(FDragRotConstant) * ArrowLen * ClientWidth);

    AHead1.x := ArrowEnd.x - Trunc(Sin(FDragRotConstant  + OneEighth * 3) * ArrowHeadLen * ClientWidth);
    AHead1.y := ArrowEnd.y + Trunc(Cos(FDragRotConstant  + OneEighth * 3) * ArrowHeadLen * ClientWidth);
    AHead2.x := ArrowEnd.x - Trunc(Sin(FDragRotConstant  - OneEighth * 3) * ArrowHeadLen * ClientWidth);
    AHead2.y := ArrowEnd.y + Trunc(Cos(FDragRotConstant  - OneEighth * 3) * ArrowHeadLen * ClientWidth);
  end;

  C.MoveTo(ArrowStart.x, ArrowStart.y);
  C.LineTo(ArrowEnd.x, ArrowEnd.y);
  C.LineTo(AHead1.x, AHead1.y);
  C.MoveTo(ArrowEnd.x, ArrowEnd.y);
  C.LineTo(AHead2.x, AHead2.y);
end;

procedure TDragHelperForm.PaintMoveHelper(Sender: TObject);
const
  ArrowHeadLen = GlyphRadius /2;
var
  C:TCanvas;
  P:TPen;
  B:TBrush;
  GlyphCentre: TPoint;
  ArrowEnd, AHead1, AHead2: TPoint;
  DMX, DMY, OfsLen: double;

begin
  C := (Sender as TPaintBox).Canvas;
  P := C.Pen;
  B := C.Brush;

  P.Color := clWindowText;
  B.Color := clActiveCaption;
  GlyphCentre.x := ClientWidth div 2;
  GlyphCentre.y := ClientHeight div 2;
  DMX := DragMoveOffset.x;
  DMY := DragMoveOffset.y;
  OfsLen := Sqrt(DMX * DMX + DMY * DMY);
  if OfsLen > 0 then
  begin
    DMX := DMX / OfsLen;
    DMY := DMY / OfsLen;

    ArrowEnd.x := Trunc(GlyphCentre.x + DMX * GlyphRadius * ClientWidth);
    ArrowEnd.y := Trunc(GlyphCentre.y + DMY * GlyphRadius * ClientHeight);

    AHead1.x := Trunc(ArrowEnd.x + Cos(FDragAngle + OneEighth * 3) * ArrowHeadLen * ClientWidth);
    AHead1.y := Trunc(ArrowEnd.y + Sin(FDragAngle + OneEighth * 3) * ArrowHeadLen * ClientHeight);
    AHead2.x := Trunc(ArrowEnd.x + Cos(FDragAngle - OneEighth * 3) * ArrowHeadLen * ClientWidth);
    AHead2.y := Trunc(ArrowEnd.y + Sin(FDragAngle - OneEighth * 3) * ArrowHeadLen * ClientHeight);

    P.Width := Trunc((FDragMagnitude / UnitaryDistanceZoom) + 2);
    C.MoveTo(GlyphCentre.x, GlyphCentre.y);
    C.LineTo(ArrowEnd.x, ArrowEnd.y);
    C.LineTo(AHead1.x, AHead1.y);
    C.MoveTo(ArrowEnd.x, ArrowEnd.y);
    C.LineTo(AHead2.x, AHead2.y);
  end;
end;

procedure TDragHelperForm.PaintZoomHelper(Sender: TObject);
const
  ArrowTB = 4/10;
  ArrowNeckY = 3/10;
  ArrowLRNeck = 2/10;
  ArrowLRTip = 1/2;
var
  C: TCanvas;
  P:TPen;
  B:TBrush;
  ABase, ATip: TPoint;
  ANeck1, ANeck2: TPoint;
  AEdge1, AEdge2: TPoint;
  GlyphCentre: TPoint;
  NeckWidth: double;
  Polygon: array of TPoint;

begin
  C := (Sender as TPaintBox).Canvas;
  P := C.Pen;
  B := C.Brush;
  GlyphCentre.x := ClientWidth div 2;
  GlyphCentre.y := ClientHeight div 2;

  P.Color := clWindowText;
  B.Color := clActiveCaption;
  P.Width := Trunc(Abs(FDragZoomConstant * 2) + 1);

  //Y co-ordnates first.
  if FDragZoomConstant < 0 then
  begin
    ABase.y := Trunc(GlyphCentre.y - ArrowTB * ClientHeight);
    ATip.y  := Trunc(GlyphCentre.y + ArrowTB * ClientHeight);
    ANeck1.y := Trunc(GlyphCentre.y + ArrowNeckY * ClientHeight);
    ANeck2.y := ANeck1.y;
    AEdge1.y := ANeck1.y;
    AEdge2.y := ANeck1.y;
  end
  else
  begin
    ABase.y := Trunc(GlyphCentre.y + ArrowTB * ClientHeight);
    ATip.y  := Trunc(GlyphCentre.y - ArrowTB * ClientHeight);
    ANeck1.y := Trunc(GlyphCentre.y - ArrowNeckY * ClientHeight);
    ANeck2.y := ANeck1.y;
    AEdge1.y := ANeck1.y;
    AEdge2.y := ANeck1.y;
  end;

  //X Coordinates.
  NeckWidth := 1 - ( 1 / (1 + Abs(FDragZoomConstant)));
  ABase.x := GlyphCentre.x;
  ATip.x := GlyphCentre.x;
  AEdge1.x := Trunc(GlyphCentre.x + ArrowLRTip * ClientWidth * NeckWidth);
  AEdge2.x := Trunc(GlyphCentre.x - ArrowLRTip * ClientWidth * NeckWidth);
  ANeck1.x := Trunc(GlyphCentre.x + ArrowLRNeck * ClientWidth * NeckWidth);
  ANeck2.x := Trunc(GlyphCentre.x - ArrowLRNeck * ClientWidth * NeckWidth);

  SetLength(Polygon, 6);
  Polygon[0] := ABase;
  Polygon[1] := ANeck1;
  Polygon[2] := AEdge1;
  Polygon[3] := ATip;
  Polygon[4] := AEdge2;
  Polygon[5] := ANeck2;
  C.Polygon(Polygon);
end;

procedure TDragHelperForm.PBPaint(Sender: TObject);
var
  C: TCanvas;
  B: TBrush;
  SR, DR: TRect;
begin
  C := (Sender as TPaintBox).Canvas;
  B := C.Brush;
  B.Color := clBtnFace;

  Assert(ClientWidth = ClientHeight);
  if Assigned(FBackgroundBmp) then
  begin
    SR.Top := 0;
    SR.Left := 0;
    SR.Bottom := FBackgroundBmp.Height;
    SR.Right := FBackgroundBmp.Width;
    DR := (Sender as TPaintBox).ClientRect;
    C.CopyRect(DR, FBackgroundBmp.Canvas, SR);
  end
  else
    C.FillRect((Sender as TPaintBox).ClientRect);
    
  case FDragType of
    fdtRotate: PaintRotHelper(Sender);
    fdtZoom: PaintZoomHelper(Sender);
    fdtMove: PaintMoveHelper(Sender);
  else
    Assert(false);
  end;
end;

end.
