unit SizeableImage;
{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, Forms, Dialogs,
  ExtCtrls, Math;

type
  TMouseButtonSet = set of TMouseButton;

  TSizeableImage = class(TImage)
  private
    { Private declarations }
    FDragButtonSet: TMouseButtonSet;
    FDraggingButton: TMouseButton;
    FStartDragLoc: TPoint;
    FDragOffset: TPoint;
    FDragMagnitude: cardinal; //Pels from start
    FDragAngle: double; //radians
  protected
    { Protected declarations }
    procedure MouseDown(Button: TMouseButton; Shift: TShiftState;
      X, Y: Integer); override;
    procedure MouseUp(Button: TMouseButton; Shift: TShiftState;
      X, Y: Integer); override;
    procedure DoStartDrag(var DragObject: TDragObject);override;
    procedure DragOver(Source: TObject; X, Y: Integer; State: TDragState; var Accept: Boolean); override;
    procedure CalcOffsets(ClientPos: TPoint);
  public
    { Public declarations }
    procedure DragDrop(Source: TObject; X,Y: Integer); override;
    property StartDragLoc: TPoint read FStartDragLoc;
    property DragOffset: TPoint read FDragOffset;
    property DraggingButton: TMouseButton read FDraggingButton;
    property DragMagnitude: cardinal read FDragMagnitude;
    property DragAngle: double read FDragAngle;
    property OnResize;
  published
    { Published declarations }
    property DragButtonSet: TMouseButtonSet read FDragButtonSet write FDragButtonSet;
  end;

procedure Register;

implementation

procedure TSizeableImage.CalcOffsets(ClientPos: TPoint);
var
  tmp1, tmp2: double;
begin
  FDragOffset.X := ClientPos.X - FStartDragLoc.X;
  FDragOffset.Y := ClientPos.Y - FStartDragLoc.Y;
  tmp1 := FDragOffset.X * FDragOffset.X;
  tmp2 := FDragOffset.Y * FDragOffset.Y;
  FDragMagnitude := Trunc(Sqrt(tmp1 + tmp2));

  FDragAngle := ArcTan2(FDragOffset.Y, FDragOffset.X);
end;

procedure TSizeableImage.DoStartDrag(var DragObject: TDragObject);
begin
  FStartDragLoc := ScreenToClient(Mouse.CursorPos);
  inherited;
end;

procedure TSizeableImage.DragOver(Source: TObject; X, Y: Integer; State: TDragState; var Accept: Boolean);
var
  Pt: TPoint;
begin
  Pt.x := x;
  Pt.y := y;
  CalcOffsets(Pt);
  if Source = Self then
    Accept := true;
  inherited;
end;

procedure TSizeableImage.DragDrop(Source: TObject; X,Y: Integer);
var
  Pt: TPoint;
begin
  Pt.x := x;
  Pt.y := y;
  CalcOffsets(Pt);
  inherited;
end;

procedure TSizeableImage.MouseDown(Button: TMouseButton; Shift: TShiftState; X, Y: Integer);
begin
  inherited;
  if Button in FDragButtonSet then
  begin
    FDraggingButton := Button;
    BeginDrag(true);
  end;
end;

procedure TSizeableImage.MouseUp(Button: TMouseButton; Shift: TShiftState; X, Y: Integer);
begin
  inherited;
  if Dragging then
  begin
    EndDrag(true);
  end;
end;


procedure Register;
begin
  RegisterComponents('MCH', [TSizeableImage]);
end;

end.
