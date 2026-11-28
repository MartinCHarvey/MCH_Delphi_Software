unit CellCountShared;
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
  SharedSymmetries;

{$IFDEF ORDER2}
{$DEFINE OPTIMISE_SMALL_SETS}
{$ENDIF}
{$IFDEF ORDER3}
{$DEFINE OPTIMISE_SMALL_SETS}
{$ENDIF}

type
  TRowColCellCount = 0.. (ORDER * ORDER);
  TStackBandCellCount = 0 .. (ORDER * ORDER * ORDER);

{$IFDEF OPTIMISE_SMALL_SETS}
  TSmallSet = type UInt32;

  TStackBandCellCountSet = TSmallSet;
  TRowColCellCountSet = TSmallSet;
{$ELSE}
  TStackBandCellCountSet = set of TStackBandCellCount;
  TRowColCellCountSet = set of TRowColCellCount;
{$ENDIF}

  //Rows or cols in a stack or band.
  TRowColCountInfo = record
    CellCounts: array[TOrderIdx] of TRowColCellCount;
    CellCountSet: TRowColCellCountSet;
  end;
  PRowColCountInfo = ^TRowColCountInfo;

  TStackBandCountInfo = record
    CellCounts: array[TOrderIdx] of TStackBandCellCount;
    CellCountSet: TStackBandCellCountSet;
    //Unpermuted line info (CellCounts) is primary information,
    //All other counts calculated from there.
    UnpermutedLineInfo: array[TOrderIdx] of TRowColCountInfo;
  end;
  PStackBandCountInfo = ^TStackBandCountInfo;

  TCountInfo = array [boolean] of TStackBandCountInfo;
  PCountInfo = ^TCountInfo;


  TRowColCountInfoMT = array[TOrderIdx] of TRowColCellCount;
  PRowColCountInfoMT = ^TRowColCountInfoMT;

  TStackBandCountInfoMT = record
    CellCounts: array[TOrderIdx] of TStackBandCellCount;
    //Unpermuted line info (CellCounts) is primary information,
    //All other counts calculated from there.
    UnpermutedLineInfo: array[TOrderIdx] of TRowColCountInfoMT;
  end;
  PStackBandCountInfoMT = ^TStackBandCountInfoMT;

  TCountInfoMT = array [boolean] of TStackBandCountInfoMT;
  PCountInfoMT = ^TCountInfoMT;

  TRowColSelectedPerms = array[TOrderIdx] of TPermIdx;

  TStackBandSelectedPerms = record
    StackBandPerm: TPermIdx;
    RowColPerms: TRowColSelectedPerms;
  end;

  TSelectedPerms = record
    StackBandPerms: array [boolean] of TStackBandSelectedPerms;
    Reflect: boolean;
  end;

  TPermCount = 0 .. FACT_ORDER;

  TPermMask = array[TOrderIdx] of integer;

  TPermMaskList = array[TPermIdx] of TPermMask;

  TGenAllowedPerms = record
    PermIdxs: TPermIdxList;
    PermMasks: TPermMaskList;
    Count: TPermCount;
  end;

  TRowColAllowedPerms = array[TOrderIdx] of TGenAllowedPerms;

  TStackBandAllowedPerms = record
    RowColAllowed: TRowColAllowedPerms;
    StackBandAllowed: TGenAllowedPerms;
  end;

  TAllowedPerms = array[boolean] of TStackBandAllowedPerms;

{$IFDEF OPTIMISE_SMALL_SETS}
function SmallSetUnion(A, B: TSmallSet): TSmallSet; inline;
function SmallSetIntersection(A, B: TSmallSet): TSmallSet; inline;
function SmallSetUnitary(B: integer): TSmallSet; inline;
function SmallSetEmpty: TSmallSet; inline;
function SmallSetIn(A: integer; B: TSmallSet): boolean; inline;
{$ENDIF}

implementation

{ Misc functions }

{$IFDEF OPTIMISE_SMALL_SETS}
function SmallSetUnion(A, B: TSmallSet): TSmallSet; inline;
begin
  result := A or B;
end;

function SmallSetIntersection(A, B: TSmallSet): TSmallSet; inline;
begin
  result := A and B;
end;

function SmallSetUnitary(B: integer): TSmallSet; inline;
begin
  Assert(B < (sizeof(TSmallSet) * 8));
  result := 1 shl B;
end;

function SmallSetEmpty: TSmallSet; inline;
begin
  result := 0;
end;

function SmallSetIn(A: integer; B: TSmallSet): boolean; inline;
begin
  result := SmallSetIntersection(SmallSetUnitary(A), B)
    <> SmallSetEmpty;
end;

{$ENDIF}

end.
