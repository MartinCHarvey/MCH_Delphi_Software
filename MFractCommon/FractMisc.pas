unit FractMisc;

{ Martin Harvey 16 May 2013 }

interface

uses
  SysUtils,
  System.Types,
  System.UITypes;

type
  TRealPoint = record
    X: double;
    Y: double;
  end;

  TRealRect = record
    case Integer of
      0: (Left, Top, Right, Bottom: double);
      1: (TopLeft, BottomRight: TRealPoint);
  end;

  TPackedColor = packed record
    Blue, Green, Red, Spare: byte;
  end;
  PPackedColor = ^TPackedColor;

  TRealColor = record
    Blue, Green, Red: double;
  end;

function IntPointToRealPoint(IntPt: TPoint; ScalePPU: double): TRealPoint;
function AddRealPoints(a, b: TRealPoint): TRealPoint;
function SubRealPoints(a, b: TRealPoint): TRealPoint;
function ScaleRealPt(a: TRealPoint; sc: double): TRealPoint;
function RotateRealPt(a: TRealPoint; rot: double): TRealPoint;
function RealPointsSame(a, b: TRealPoint): boolean;

function ScreenPointToRealPoint(ScreenPoint: TPoint;
  ScreenWidth: integer;
  ScreenHeight: integer;
  ScreenCenter: TRealPoint;
  ScalePPU: double;
  RotRads: double): TRealPoint;

function Pow2Val(Pow: cardinal): cardinal;
function Val2Pow(Val: cardinal): cardinal;

function ColorToPackedColor(C: TColor): TPackedColor;
function PackedColorToColor(P: TPackedColor): TColor;
{$IFDEF IS_FIREMONKEY}
function PackedColorToAlphaColor(PC: TPackedColor): TAlphaColor;
function AlphaColorToPackedColor(AC: TAlphaColor): TPackedColor;
{$ENDIF}

function IntColorToRealColor(Int: TPackedColor): TRealColor;
function RealColorToIntColor(Real: TRealColor): TPackedColor;
function RealColorAdd(a, b: TRealColor): TRealColor;
function RealColorMult(c: TRealColor; x: double): TRealColor;

function SecsToStr(Secs: integer): string;

implementation

{ Misc functions }

function IntPointToRealPoint(IntPt: TPoint; ScalePPU: double): TRealPoint;
begin
  result.X := IntPt.x / ScalePPU;
  result.Y := -(IntPt.y / ScalePPU); //Screen bitmaps go down with +y
end;

function AddRealPoints(a, b: TRealPoint): TRealPoint;
begin
  result.X := a.X + b.X;
  result.Y := a.Y + b.Y;
end;

function SubRealPoints(a, b: TRealPoint): TRealPoint;
begin
  result.X := a.X - b.X;
  result.Y := a.Y - b.Y;
end;

function ScaleRealPt(a: TRealPoint; sc: double): TRealPoint;
begin
  result.X := a.X * sc;
  result.Y := a.Y * sc;
end;

function RotateRealPt(a: TRealPoint; rot: double): TRealPoint;
var
  CosR, SinR: double;
begin
  CosR := Cos(Rot);
  SinR := Sin(Rot);
  result.X := a.X * CosR - a.Y * SinR;
  result.Y := a.X * SinR + a.Y * CosR;
end;

function RealPointsSame(a, b: TRealPoint): boolean;
begin
  result := (a.X = b.X) and (a.Y = b.Y);
end;

function ScreenPointToRealPoint(ScreenPoint: TPoint;
  ScreenWidth: integer;
  ScreenHeight: integer;
  ScreenCenter: TRealPoint;
  ScalePPU: double;
  RotRads: double): TRealPoint;
var
  WinCorner: TPoint;
  CenterPelOffset, LocPelOffset: TRealPoint;
begin
  WinCorner.x := ScreenWidth;
  WinCorner.y := ScreenHeight;
  CenterPelOffset := IntPointToRealPoint(WinCorner, 2 * ScalePPU);
  LocPelOffset := IntPointToRealPoint(ScreenPoint, ScalePPU);
  LocPelOffset := SubRealPoints(LocPelOffset, CenterPelOffset);
  LocPelOffset := RotateRealPt(LocPelOffset, RotRads);
  Result := AddRealPoints(ScreenCenter, LocPelOffset);
end;

function Pow2Val(Pow: cardinal): cardinal;
begin
  if (Pow < 32) then
    result := (1 shl Pow) - 1
  else if (Pow = 32) then
    result := High(cardinal)
  else begin
    Assert(false);
    result := 0;
  end;
end;

function Val2Pow(Val: cardinal): cardinal;
var
  idx: cardinal;
begin
  result := 0;
  for idx := 0 to 32 do
  begin
    if Pow2Val(idx) >= Val then
    begin
      result := idx;
      exit;
    end;
  end;
end;

function IntColorToRealColor(Int: TPackedColor): TRealColor;
begin
  result.Red := Int.Red / 256;
  result.Green := Int.Green / 256;
  result.Blue := Int.Blue / 256;
end;

function RealColorToIntColor(Real: TRealColor): TPackedColor;
begin
  result.Red := Trunc(Real.Red * 256);
  result.Green := Trunc(Real.Green * 256);
  result.Blue := Trunc(Real.Blue * 256);
end;

function RealColorAdd(a, b: TRealColor): TRealColor;
begin
  result.Red := a.Red + b.Red;
  result.Green := a.Green + b.Green;
  result.Blue := a.blue + b.Blue;
end;

function ColorToPackedColor(C: TColor): TPackedColor;
begin
  result.Red := C and $FF;
  result.Green := (C and $FF00) shr 8;
  result.Blue := (C and $FF0000) shr 16;
end;

function PackedColorToColor(P: TPackedColor): TColor;
begin
  result := P.Red
            or (P.Green shl 8)
            or (P.Blue shl 16);
end;

{$IFDEF IS_FIREMONKEY}
function PackedColorToAlphaColor(PC: TPackedColor): TAlphaColor;
begin
result := $FF000000; //Alpha channel.
result := Result or (Cardinal(PC.Red) shl 16)
                 or (Cardinal(PC.Green) shl 8)
                 or (Cardinal(PC.Blue));
end;

function AlphaColorToPackedColor(AC: TAlphaColor): TPackedColor;
begin
  result.Red := (AC and $FF0000) shr 16;
  result.Green := (AC and $FF00)shr 8;
  result.Blue := AC and $FF;
end;
{$ENDIF}


function RealColorMult(c: TRealColor; x: double): TRealColor;
begin
  result.Red := c.Red * x;
  result.Green := c.Green * x;
  result.Blue := c.Blue * x;
end;

function SecsToStr(Secs: integer): string;
begin
  if Secs < 60 then
  begin
    if Secs = 1 then
      result := IntToStr(secs) + ' second to go '
    else
      result := IntToStr(secs) + ' seconds to go ';
  end
  else
  begin
    Secs := Secs div 60;
    if Secs < 60 then
    begin
      if Secs = 1 then
        result := IntToStr(secs) + ' minute to go '
      else
        result := IntToStr(secs) + ' minutes to go ';
    end
    else
    begin
      Secs := Secs div 60;
      if Secs = 1 then
        result := IntToStr(secs) + ' hour to go '
      else
        result := IntToStr(secs) + ' hours to go ';
    end;
  end;
end;

end.
