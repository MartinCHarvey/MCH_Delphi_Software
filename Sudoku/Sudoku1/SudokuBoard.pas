unit SudokuBoard;

{$DEFINE OPTIMISE_COUNT_BITS}
{$DEFINE OPTIMISE_SET_HANDLING}
//{$DEFINE OPTIMISE_BLOCK_SET_MAP} Nope, makes it slower!
//Testament to good branch prediction on modern CPU's.

{$IFOPT R-}
{$DEFINE UNROLL_INNER_LOOP}
{$ENDIF}

{$DEFINE EXPAND_GETALLOWEDSET}
//{$DEFINE FIND_JUST_ONE_SOLN}

interface

uses
  Classes, SSStreamables, StreamingSystem, SudokuAbstracts;

//TODO - We can carefully unpick this, and make some of it abstract and
//put both sorts of solver into the one app.

type
  TSBoardState = class(TSAbstractBoardState)
  private
    FBoard: TSBoard;
    FRowExcludes: TSConstraint;
    FColExcludes: TSConstraint;
    FBlockExcludes: TSConstraint;
  protected
    function MapToBlockSetIdx(Row, Col: TSNumber): TSNumber; inline;
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


const
{$IFDEF OPTIMISE_SET_HANDLING}
  AllAllowed: TSNumberSet = $3FE;
  EmptySet: TSNumberSet = 0;
{$ELSE}
  AllAllowed: TSNumberSet = [1,2,3,4,5,6,7,8,9];
  EmptySet: TSNumberSet = [];
{$ENDIF}

{$IFDEF OPTIMISE_BLOCK_SET_MAP}
var
  PrecalcMap: array[TSNumber, TSNumber] of TSNumber;
{$ENDIF}

implementation

uses
  SysUtils, SSAbstracts;

{ Misc util functions }

function TSNumberIn(NSet: TSNumberSet; Number: integer): boolean; inline;
begin
{$IFNDEF OPTIMISE_SET_HANDLING}
  result := Number in NSet;
{$ELSE}
  result := (Word(1 shl Number) and NSet) <> 0;
{$ENDIF}
end;

function TSSetSub(X, Y: TSNumberSet): TSNumberSet; inline;
begin
{$IFNDEF OPTIMISE_SET_HANDLING}
  result := X - Y;
{$ELSE}
  result := X and not Y;
{$ENDIF}
end;

function TSSetAdd(X, Y: TSNumberSet): TSNumberSet; inline;
begin
{$IFNDEF OPTIMISE_SET_HANDLING}
  result := X + Y;
{$ELSE}
  result := X or Y;
{$ENDIF}
end;

function TSSetIndividual(X: integer): TSNumberSet; inline;
begin
{$IFNDEF OPTIMISE_SET_HANDLING}
  result := [X];
{$ELSE}
  result := 1 shl X;
{$ENDIF}
end;

function CountBits(NSet: TSNumberSet): integer; inline;
var
{$IFDEF OPTIMISE_COUNT_BITS}
  C: WORD;
{$ELSE}
  i: TSNumber;
{$ENDIF}
begin
  result := 0;
{$IFDEF OPTIMISE_COUNT_BITS}
  Assert(Sizeof(NSet) = sizeof(Word));
{$IFDEF OPTIMISE_SET_HANDLING}
  C := NSet;
{$ELSE}
  C := PWord(@NSet)^;
{$ENDIF}
  while C <> 0 do
  begin
    C := C and (C-1);
    Inc(Result);
  end;
{$ELSE}
  if NSet <> EmptySet then
  begin
    for i := Low(i) to High(i) do
    begin
      if TSNumberIn(NSet,i) then
        Inc(result);
    end;
  end;
{$ENDIF}
end;

function FindSetBit(NSet: TSNumberSet): TSNumber;
var
  N: TSNumber;
begin
  result := Low(N);
  Assert(CountBits(NSet) = 1);
  for N := Low(TSNumber) to High(TSNumber) do
    if TSNumberIn(NSet, N) then
    begin
      result := N;
      exit;
    end;
end;

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
{$IFDEF FIND_JUST_ONE_SOLN}
          result := SolveSingleThreadedInt;
          if result then
            break;
{$ELSE}
          result := SolveSingleThreadedInt or result;
{$ENDIF}
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



{$IFDEF OPTIMISE_BLOCK_SET_MAP}
function TSBoardState.MapToBlockSetIdx(Row, Col: TSNumber): TSNumber;
begin
  result := PrecalcMap[Row][Col];
end;

function MapToBlockSetIdxSlow(Row, Col: TSNumber): TSNumber;
{$ELSE}
function TSBoardState.MapToBlockSetIdx(Row, Col: TSNumber): TSNumber;
{$ENDIF}
label error;
begin
  case Row of
    1,2,3:
      case Col of
        1,2,3: result := 1;
        4,5,6: result := 2;
        7,8,9: result := 3;
      else
        goto error;
      end;
    4,5,6:
      case Col of
        1,2,3: result := 4;
        4,5,6: result := 5;
        7,8,9: result := 6;
      else
        goto error;
      end;
    7,8,9:
      case Col of
        1,2,3: result := 7;
        4,5,6: result := 8;
        7,8,9: result := 9;
      else
        goto error;
      end;
    else
      goto error;
  end;
  exit;
error:
  Assert(false);
  result := 1;
end;

{$IFDEF OPTIMISE_BLOCK_SET_MAP}
procedure Precalc;
var
  Row, Col: TSNumber;
begin
  for Row := Low(Row) to High(Row) do
    for Col := Low(Col) to High(Col) do
      PrecalcMap[Row][Col] := MapToBlockSetIdxSlow(Row, Col);
end;
{$ENDIF}

initialization
{$IFDEF OPTIMISE_BLOCK_SET_MAP}
  Precalc;
{$ENDIF}
end.
