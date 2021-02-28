unit NaiveSymmetries;

{

Copyright © 2021 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
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

type
  TStackBandPermutations = array [boolean] of TPermIdx;
  TRowColPermutations = array [TOrderIdx] of TPermIdx;
  TAllRowColPermutations = array [boolean] of TRowColPermutations;

  TNaiveSymBoard = class (TSymBoard)
  protected
    FReflect: boolean;
    FStackBandPermutation: TStackBandPermutations;
    FRowColPermutations: TAllRowColPermutations;

    function GetStackBandPerms(Rows: boolean): TPermIdx;
    procedure SetStackBandPerms(Rows: boolean; Val: TPermIdx);
    function GetAllRowColPerms(Rows: boolean; StackBandIdx: TOrderIdx): TPermIdx;
    procedure SetAllRowColPerms(Rows: boolean; StackBandIdx: TOrderIdx; Val: TPermIdx);

    procedure Map(Row, Col: TRowColIdx; var MapRow: TRowColIdx; Var MapCol: TRowColIdx);

    function GetMappedEntry(Row, Col: TRowColIdx): integer;
    procedure SetMappedEntry(Row, Col: TRowColIdx; Val: integer);
    function MappedSame(Other: TNaiveSymBoard): boolean;
public
    //TODO - individual isomorphic comparisons might or might not be the best
    //way to search our way thru the space.
    function AmIsomorphicTo(Other: TSymBoard): boolean; override;
    procedure Assign(Src: TSymBoard); override;

    property Reflect: boolean read FReflect write FReflect;
    property StackBandPerms[Rows: boolean]: TPermIdx read GetStackBandPerms write SetStackBandPerms;
    property AllRowColPerms[Rows: boolean; StackBandIdx: TOrderIdx]: TPermIdx read GetAllRowColPerms write SetAllRowColPerms;

    property MappedEntries[Row, Col: TRowColIdx]: integer read GetMappedEntry write SetMappedEntry;
  end;


implementation

{ TNaiveSymBoard }

procedure TNaiveSymBoard.Assign(Src: TSymBoard);
var
  S: TNaiveSymBoard;
begin
  if Assigned(Src) and (Src is TNaiveSymBoard) then
  begin
    S := Src as TNaiveSymBoard;
    FReflect := S.FReflect;
    FStackBandPermutation := S.FStackBandPermutation;
    FRowColPermutations := S.FRowColPermutations;
  end;
  inherited;
end;

function TNaiveSymBoard.GetStackBandPerms(Rows: boolean): TPermIdx;
begin
  result := FStackBandPermutation[Rows];
end;

procedure TNaiveSymBoard.SetStackBandPerms(Rows: boolean; Val: TPermIdx);
begin
  Assert((Val >= 0) and (Val < Length(PermList)));
  FStackBandPermutation[Rows] := Val;
end;

function TNaiveSymBoard.GetAllRowColPerms(Rows: boolean; StackBandIdx: TOrderIdx): TPermIdx;
begin
  result := FRowColPermutations[Rows, StackBandIdx];
end;

procedure TNaiveSymBoard.SetAllRowColPerms(Rows: boolean; StackBandIdx: TOrderIdx; Val: TPermIdx);
begin
  Assert((Val >= 0) and (Val < Length(PermList)));
  FRowColPermutations[Rows, StackBandIdx] := Val;
end;


procedure TNaiveSymBoard.Map(Row, Col: TRowColIdx; var MapRow: TRowColIdx; var MapCol: TRowColIdx);
var
  Band, Stack: TOrderIdx;
  RowInBand, ColInStack: TOrderIdx;
  Tmp: TRowColIdx;
begin
  AbsToRelIndexed(Row, Band, RowInBand);
  AbsToRelIndexed(Col, Stack, ColInStack);

  Assert((AllRowColPerms[true, Band] >= 0) and (AllRowColPerms[true, Band] < Length(PermList)));
  Assert((AllRowColPerms[false, Stack] >= 0) and (AllRowColPerms[false, Stack] < Length(PermList)));
  RowInBand := PermList[AllRowColPerms[true, Band]][RowInBand];
  ColInStack := PermList[AllRowColPerms[false, Stack]][ColInStack];

  Assert((StackBandPerms[true] >= 0) and (StackBandPerms[true] < Length(PermList)));
  Assert((StackBandPerms[false] >= 0) and (StackBandPerms[false] < Length(PermList)));
  Band := PermList[StackBandPerms[true]][Band];
  Stack := PermList[StackBandPerms[false]][Stack];

  RelToAbsIndexed(Band, RowInBand, MapRow);
  RelToAbsIndexed(Stack, ColInStack, MapCol);
  if Reflect then
  begin
    Tmp := MapRow;
    MapRow := MapCol;
    MapCol := Tmp;
  end;
end;

procedure InitRowColPerms(var RC: TRowColPermutations);
var
  Idx: TOrderIdx;
begin
  for Idx := Low(Idx) to High(Idx) do
    RC[Idx] := 0;
end;

procedure IncRowColPerms(var RC: TRowColPermutations);
var
  Col: TOrderIdx;
  Carry: boolean;
begin
  Carry := true;
  Col := 0;
  while Carry do
  begin
    Carry := not (RC[Col] < Pred(Length(PermList)));
    if not Carry then
      Inc(RC[Col])
    else
    begin
      RC[Col] := 0;
      if Col < High(TOrderIdx) then
        Inc(Col)
      else
        Carry := false; //we're done.
    end;
  end;
end;

function DoneRowColPerms(const RC: TRowColPermutations): boolean;
var
  Idx: TOrderIdx;
begin
  result := true;
  for Idx := Low(Idx) to High(Idx) do
    result := result and (RC[Idx] = 0);
end;

function TNaiveSymBoard.MappedSame(Other: TNaiveSymBoard): boolean;
var
  Row, Col: TRowColIdx;
begin
  result := true;
  for Row := Low(Row) to High(Row) do
    for Col := Low(Col) to High(Col) do
      if MappedEntries[Row, Col] <> Other.Entries[Row, Col] then
      begin
        result := false;
        exit;
      end;
end;


function TNaiveSymBoard.AmIsomorphicTo(Other: TSymBoard): boolean;
var
  OldIStack, OldIBand: TPermIdx;
  SReflect: boolean;
  SStackBandPermutation: TStackBandPermutations;
  SRowColPermutations: TAllRowColPermutations;

  IReflect: boolean;
  IStack, IBand: TPermIdx;

label
  done;

begin
  //Save.
  SReflect := FReflect;
  SStackBandPermutation := FStackBandPermutation;
  SRowColPermutations:= FRowColPermutations;
  result := false;
  for IReflect := Low(IReflect) to High(IReflect) do
  begin
    Reflect := IReflect;
    for IStack := 0 to Pred(Length(PermList)) do
      for IBand := 0 to Pred(Length(PermList)) do
      begin
        StackBandPerms[true] := IBand;
        StackBandPerms[false] := IStack;
        OldIStack := IStack;
        OldIBand := IBand;

        InitRowColPerms(FRowColPermutations[false]);
        Assert(OldIStack = IStack);
        Assert(OldIBand = IBand);
        repeat
          InitRowColPerms(FRowColPermutations[true]);
          Assert(OldIStack = IStack);
          Assert(OldIBand = IBand);
          repeat
            if MappedSame(Other as TNaiveSymBoard) then
            begin
              result := true;
              goto done;
            end;
            IncRowColPerms(FRowColPermutations[true]);
            Assert(OldIStack = IStack);
            Assert(OldIBand = IBand);
          until DoneRowColPerms(FRowColPermutations[true]);
          Assert(OldIStack = IStack);
          Assert(OldIBand = IBand);
          IncRowColPerms(FRowColPermutations[false]);
          Assert(OldIStack = IStack);
          Assert(OldIBand = IBand);
        until DoneRowColPerms(FRowColPermutations[false]);
        Assert(OldIStack = IStack);
        Assert(OldIBand = IBand);
      end;
  end;

done:
  //Restore.
  FReflect := SReflect;
  FStackBandPermutation := SStackBandPermutation;
  FRowColPermutations:= SRowColPermutations;
end;

function TNaiveSymBoard.GetMappedEntry(Row, Col: TRowColIdx): integer;
begin
  Map(Row, Col, Row, Col);
  result := Entries[Row, Col];
end;

procedure TNaiveSymBoard.SetMappedEntry(Row, Col: TRowColIdx; Val: integer);
begin
  Map(Row, Col, Row, Col);
  Entries[Row, Col] := Val;
end;

end.
