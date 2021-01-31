unit SudokuBoard;

{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

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

{$DEFINE OPTIMISE_COUNT_BITS}
{$DEFINE OPTIMISE_SET_HANDLING}

{$IFOPT R-}
{$DEFINE UNROLL_INNER_LOOP}
{$ENDIF}

{$DEFINE EXPAND_GETALLOWEDSET}

interface

uses
  Classes, SSStreamables, StreamingSystem, SudokuAbstracts;

type
  TSBoardState = class(TSAbstractBoardState)
  private
    FBoard: TSBoard;
    FRowExcludes: TSConstraint;
    FColExcludes: TSConstraint;
    FBlockExcludes: TSConstraint;
  protected
    function GetAllowedSet(Row, Col: TSNumber): TSNumberSet; inline;

    function GetOptEntry(Row, Col: TSNumber): TSOptNumber; override;

    function SetEntry(Row, Col, Entry: TSNumber): boolean; override;
    procedure ClearEntry(Row, Col: TSNumber); override;

    function GetComplete: boolean; override;
    function GetProceedable: boolean; override;
  public
    //Find the minimally constrained position for a next move,
    //or return empty set if there is none.
    function FindNextMove(var Row, Col: TSNumber): TSNumberSet; override;

    function EvolveIfSingular(Row, Col: TSNumber; NSet: TSNumberSet): boolean; override;
    procedure Clear; override;
  end;

  //Simple recursive (and hence hopefully quick), single thread solver.
  TSSimpleSolver = class(TSAbstractSolver)
  protected
    function SolveSingleThreadedInt: boolean;
    function CreateBoardState: TSAbstractBoardState; override;
  public
    //Returns whether any solutions found.
    function Solve: boolean; override;
  end;


{$IFDEF OPTIMISE_BLOCK_SET_MAP}
var
  PrecalcMap: array[TSNumber, TSNumber] of TSNumber;
{$ENDIF}

implementation

uses
  SysUtils, SSAbstracts;

{ TSSimpleSolver }

function TSSimpleSolver.Solve: boolean;
begin
  FStats.StartTime := Now;
  result := SolveSingleThreadedInt;
  FStats.ElapsedTime := Now - FStats.StartTime;
end;

function TSSimpleSolver.SolveSingleThreadedInt: boolean;
var
  NextSet: TSNumberSet;
  AppliedNumber: TSNumber;
  Row, Col: TSNumber;
  SetOK: boolean;
begin
  NextSet := FBoardState.FindNextMove(Row, Col);
  if NextSet = EmptySet then
  begin
    result := FBoardState.Complete;
    if result then
    begin
      Inc(FStats.CorrectSolutions);
      case FStats.CorrectSolutions of
        1:
        begin
          if not Assigned(FUniqueSolution) then
            FUniqueSolution := CreateBoardState;
          FUniqueSolution.Assign(FBoardState);
        end;
        2:
        begin
          FUniqueSolution.Free;
          FUniqueSolution := nil;
        end;
      end;
    end
    else
      Inc(FStats.DeadEnds);
  end
  else
  begin
    if FBoardState.EvolveIfSingular(Row, Col, NextSet) then
    begin
      result := SolveSingleThreadedInt;
      Assert(FBoardState is TSBoardState);
      TSBoardState(FBoardState).ClearEntry(Row, Col);
    end
    else
    begin
      Inc(FStats.DecisionPoints);
      result := false;
      //TODO - Magical way of pulling out the bottom bit index?
      for AppliedNumber := Low(TSNumber) to High(TSNumber) do
      begin
        if TSNumberIn(NextSet, AppliedNumber) then
        begin
          Assert(FBoardState is TSBoardState);
          SetOK := TSBoardState(FBoardState).SetEntry(Row, Col, AppliedNumber);
          Assert(SetOK);
          case FSolveMode of
            ssmFindOne:
            begin
              result := SolveSingleThreadedInt;
              if result then
                break; //Out of loop, not case.
            end;
            ssmFindAll: result := SolveSingleThreadedInt or result;
          else
            Assert(false);
          end;
          Assert(FBoardState is TSBoardState);
          TSBoardState(FBoardState).ClearEntry(Row, Col);
        end;
      end;
      if result then
        Inc(FStats.DecisionPointsWithSolution)
      else
        Inc(FStats.DecisionPointsAllDeadEnd);
    end;
  end;
end;

function TSSimpleSolver.CreateBoardState: TSAbstractBoardState;
begin
  result := TSBoardState.Create;
end;


{ TSBoardState }


function TSBoardState.GetComplete: boolean;
var
  N: TSNumber;
begin
  //Faster to check constraints.
  for N := Low(TSNumber) to High(TSNumber) do
  begin
    result := (FRowExcludes[N] = AllAllowed)
      and (FColExcludes[N] = AllAllowed)
      and (FBlockExcludes[N] = AllAllowed);
    if not result then
      exit;
  end;
end;

function TSBoardState.GetProceedable: boolean;
var
  Row, Col: TSNumber;
begin
  result := FindNextMove(Row, Col) <> EmptySet;
end;

function TSBoardState.EvolveIfSingular(Row, Col: TSNumber; NSet: TSNumberSet): boolean;
var
  SetOK: boolean;
  Singular: TSNumber;
begin
  result := CountBits(NSet) = 1;
  if result then
  begin
    Singular := FindSetBit(NSet);
    SetOK := SetEntry(Row, Col, Singular);
    Assert(SetOK);
  end;
end;

function TSBoardState.FindNextMove(var Row, Col: TSNumber): TSNumberSet;
var
  LRow, LCol: TSNumber;
  Branches, MinTreeBranches: integer;
  MinTreeRow, MinTreeCol: TSNumber;
  TreeSet, MinTreeSet: TSNumberSet;
begin
  MinTreeBranches := High(integer);
  MinTreeRow := 1;
  MinTreeCol := 1;
  MinTreeSet := EmptySet;
  for LRow := Low(LRow) to High(LRow) do
  begin
{$IFDEF UNROLL_INNER_LOOP}
    LCol := Low(LCol);
    while LCol <= High(LCol) do
    begin
{$IFDEF EXPAND_GETALLOWEDSET}
      if FBoard[LRow][LCol] <> 0 then
        TreeSet := EmptySet
      else
      begin
        TreeSet := TSSetSub(AllAllowed,
          TSSetAdd(TSSetAdd(FRowExcludes[LRow], FColExcludes[LCol]),
            FBlockExcludes[MapToBlockSetIdx(LRow, LCol)]));
      end;
{$ELSE}
      TreeSet := GetAllowedSet(LRow, LCol);
{$ENDIF}
      Branches := CountBits(TreeSet);
      if (Branches > 0) and (Branches < MinTreeBranches) then
      begin
        MinTreeBranches := Branches;
        MinTreeRow := LRow;
        MinTreeCol := LCol;
        MinTreeSet := TreeSet;
      end;

{$IFDEF EXPAND_GETALLOWEDSET}
      if FBoard[LRow][LCol + 1] <> 0 then
        TreeSet := EmptySet
      else
      begin
        TreeSet := TSSetSub(AllAllowed,
          TSSetAdd(TSSetAdd(FRowExcludes[LRow], FColExcludes[LCol + 1]),
            FBlockExcludes[MapToBlockSetIdx(LRow, LCol + 1)]));
      end;
{$ELSE}
      TreeSet := GetAllowedSet(LRow, LCol + 1);
{$ENDIF}
      Branches := CountBits(TreeSet);
      if (Branches > 0) and (Branches < MinTreeBranches) then
      begin
        MinTreeBranches := Branches;
        MinTreeRow := LRow;
        MinTreeCol := LCol + 1;
        MinTreeSet := TreeSet;
      end;

{$IFDEF EXPAND_GETALLOWEDSET}
      if FBoard[LRow][LCol + 2] <> 0 then
        TreeSet := EmptySet
      else
      begin
        TreeSet := TSSetSub(AllAllowed,
          TSSetAdd(TSSetAdd(FRowExcludes[LRow], FColExcludes[LCol + 2]),
            FBlockExcludes[MapToBlockSetIdx(LRow, LCol + 2)]));
      end;
{$ELSE}
      TreeSet := GetAllowedSet(LRow, LCol + 2);
{$ENDIF}
      Branches := CountBits(TreeSet);
      if (Branches > 0) and (Branches < MinTreeBranches) then
      begin
        MinTreeBranches := Branches;
        MinTreeRow := LRow;
        MinTreeCol := LCol + 2;
        MinTreeSet := TreeSet;
      end;

      Inc(LCol, 3);
    end;
{$ELSE}
    for LCol := Low(LCol) to High(LCol) do
    begin
      TreeSet := GetAllowedSet(LRow, LCol);
      Branches := CountBits(TreeSet);
      if (Branches > 0) and (Branches < MinTreeBranches) then
      begin
        MinTreeBranches := Branches;
        MinTreeRow := LRow;
        MinTreeCol := LCol;
        MinTreeSet := TreeSet;
      end;
    end;
{$ENDIF}
  end;
  if MinTreeBranches <> High(Integer) then
  begin
   Row := MinTreeRow;
   Col := MinTreeCol;
   result := MinTreeSet;
  end
  else
    result := EmptySet;
end;

function TSBoardState.GetAllowedSet(Row: TSNumber; Col: TSNumber): TSNumberSet;
begin
  if FBoard[Row][Col] <> 0 then
    result := EmptySet
  else
  begin
    result := TSSetSub(AllAllowed,
      TSSetAdd(TSSetAdd(FRowExcludes[Row], FColExcludes[Col]),
        FBlockExcludes[MapToBlockSetIdx(Row, Col)]));
  end;
end;

function TSBoardState.GetOptEntry(Row, Col: TSNumber): TSOptNumber;
begin
  result := FBoard[Row][Col];
end;

function TSBoardState.SetEntry(Row, Col, Entry: TSNumber): boolean;
var
  OldEntry: TSOptNumber;
  Map: TSNumber;
  OldEntryValid, Reset: boolean;
begin
  OldEntry := FBoard[Row][Col];
  OldEntryValid :=  OldEntry <> 0;
  if OldEntryValid then
    ClearEntry(Row, Col);

  if not TSNumberIn(GetAllowedSet(Row, Col), Entry) then
    result := false
  else
  begin
    Map := MapToBlockSetIdx(Row, Col);
    Assert(not TSNumberIn(FRowExcludes[Row], Entry));
    Assert(not TSNumberIn(FColExcludes[Col], Entry));
    Assert(not TSNumberIn(FBlockExcludes[Map], Entry));
    FRowExcludes[Row] := TSSetAdd(FRowExcludes[Row], TSSetIndividual(Entry));
    FColExcludes[Col] := TSSetAdd(FColExcludes[Col], TSSetIndividual(Entry));
    FBlockExcludes[Map] := TSSetAdd(FBlockExcludes[Map], TSSetIndividual(Entry));
    FBoard[Row][Col] := Entry;
    result := true;
  end;
  if OldEntryValid and not result then
  begin
    Reset := SetEntry(Row, Col, OldEntry);
    Assert(Reset);
  end;
end;

procedure TSBoardState.ClearEntry(Row: TSNumber; Col: TSNumber);
var
  Entry: TSOptNumber;
  Map: TSNumber;
begin
  Entry := FBoard[Row][Col];
  if Entry <> 0 then
  begin
    Map := MapToBlockSetIdx(Row, Col);
    Assert(TSNumberIn(FRowExcludes[Row], Entry));
    Assert(TSNumberIn(FColExcludes[Col], Entry));
    Assert(TSNumberIn(FBlockExcludes[Map], Entry));
    FRowExcludes[Row] := TSSetSub(FRowExcludes[Row], TSSetIndividual(Entry));
    FColExcludes[Col] := TSSetSub(FColExcludes[Col], TSSetIndividual(Entry));
    FBlockExcludes[Map] := TSSetSub(FBlockExcludes[Map], TSSetIndividual(Entry));
    FBoard[Row][Col] := 0;
  end;
end;

procedure TSBoardState.Clear;
var
  i,j: TSNumber;
begin
  for i := Low(TSNumber) to High(TSNumber) do
  begin
    FRowExcludes[i] := EmptySet;
    FColExcludes[i] := EmptySet;
    FBlockExcludes[i] := EmptySet;
    for j := Low(TSNumber) to High(TSNumber) do
      FBoard[i,j] := 0;
  end;
end;

end.
