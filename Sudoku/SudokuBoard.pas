unit SudokuBoard;

interface

uses
  Classes;

type
  TSNumber = 1..9;
  TSNumberSet = set of TSNumber;
  TSConstraint = array[TSNumber] of TSNumberSet;

  TSOptNumber = 0..9;
  TSRow = array[TSNumber] of TSOptNumber;
  TSBoard = array[TSNumber] of TSRow;

{$IFDEF USE_TRACKABLES}
  TSBoardState = class(TTrackable)
{$ELSE}
  TSBoardState = class
{$ENDIF}
  private
    FBoard: TSBoard;
    FRowConstraints: TSConstraint;
    FColConstraints: TSConstraint;
    FBlockConstraints: TSConstraint;
  protected
    function MapToBlockSetIdx(Row, Col: TSNumber): TSNumber;
    function GetAllowedSet(Row, Col: TSNumber): TSNumberSet;

    function GetOptEntry(Row, Col: TSNumber): TSOptNumber;
    procedure SetOptEntry(Row, Col: TSNumber; Entry: TSOptNumber);

    function SetEntry(Row, Col, Entry: TSNumber): boolean;
    procedure ClearEntry(Row, Col: TSNumber);

    function GetComplete: boolean;
    function GetProceedable: boolean;
  public
    procedure Assign(Src: TSBoardState);
    //Find the minimally constrained position for a next move,
    //or return empty set if there is none.
    function FindNextMove(var Row, Col: TSNumber): TSNumberSet;
    class function CountBits(NSet: TSNumberSet): integer;
    class function FindSetBit(NSet: TSNumberSet): TSNumber;

    function EvolveIfSingular(Row, Col: TSNumber; NSet: TSNumberSet): boolean;
    function GenChildTree(Row, Col: TSNumber; NSet: TSNumberSet): TList;
    //TODO - GenChildTree to add to existing lists? Depends what prob you want to solve.
    //TODO - List merge?
    //TODO - Generation counting?

    property Entries[Row, Col: TSNumber]: TSOptNumber read GetOptEntry write SetOptEntry; default;
    property Complete: boolean read GetComplete;
    property Proceedable: boolean read GetProceedable;
  end;

const
  AllAllowed: TSNumberSet = [1,2,3,4,5,6,7,8,9];

implementation

function TSBoardState.GetComplete: boolean;
var
  Row, Col: TSNumber;
begin
  result := true;
  for Row := Low(TSNumber) to High(TSNumber) do
    for Col := Low(TSNumber) to High(TSNumber) do
      if FBoard[Row,Col] = 0 then
      begin
        result := false;
        exit;
      end;
end;

function TSBoardState.GetProceedable: boolean;
var
  Row, Col: TSNumber;
begin
  result := CountBits(FindNextMove(Row, Col)) > 0;
end;

function TSBoardState.EvolveIfSingular(Row, Col: TSNumber; NSet: TSNumberSet): boolean;
var
  NextEntry: TSNumber;
  SetOK: boolean;
begin
  result := CountBits(NSet) = 1;
  if result then
  begin
    NextEntry := FindSetBit(NSet);
    SetOK := SetEntry(Row, Col, NextEntry);
    Assert(SetOK);
  end;
end;

function TSBoardState.GenChildTree(Row, Col: TSNumber; NSet: TSNumberSet): TList;
var
  N: TSNumber;
  NewBoard: TSBoardState;
begin
  result := nil;
  for N := Low(TSNumber) to High(TSNumber) do
  begin
    if N in NSet then
    begin
      if not Assigned(result) then
        result := TList.Create;
      NewBoard := TSBoardState.Create;
      NewBoard.Assign(self);
      NewBoard.SetEntry(Row, Col, N);
      result.Add(NewBoard);
    end;
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
  for LRow := Low(LRow) to High(LRow) do
  begin
    //TODO - candidate for inner loop unroll - recheck inlines.
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
  end;
  if MinTreeBranches <> High(Integer) then
  begin
   Row := MinTreeRow;
   Col := MinTreeCol;
   result := MinTreeSet;
  end
  else
    result := [];
end;


class function TSBoardState.FindSetBit(NSet: TSNumberSet): TSNumber;
var
  N: TSNumber;
begin
  result := Low(N);
  Assert(CountBits(NSet) = 1);
  for N := Low(TSNumber) to High(TSNumber) do
    if N in NSet then
    begin
      result := N;
      exit;
    end;
end;

class function TSBoardState.CountBits(NSet: TSNumberSet): integer;
var
  i: TSNumber;
begin
  result := 0;
  if NSet <> [] then
  begin
    for i := Low(i) to High(i) do
    begin
      if i in NSet then
        Inc(result);
    end;
  end;
end;

function TSBoardState.GetOptEntry(Row, Col: TSNumber): TSOptNumber;
begin
  result := FBoard[Row][Col];
end;

procedure TSBoardState.SetOptEntry(Row, Col: TSNumber; Entry: TSOptNumber);
begin
  if Entry = 0 then
    ClearEntry(Row, Col)
  else
    SetEntry(Row, Col, Entry);
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

  if not (Entry in GetAllowedSet(Row, Col)) then
    result := false
  else
  begin
    Map := MapToBlockSetIdx(Row, Col);
    Assert(not (Entry in FRowConstraints[Row]));
    Assert(not (Entry in FColConstraints[Col]));
    Assert(not (Entry in FBlockConstraints[Map]));
    FRowConstraints[Row] := FRowConstraints[Row] + [Entry];
    FColConstraints[Col] := FColConstraints[Col] + [Entry];
    FBlockConstraints[Map] := FBlockConstraints[Map] + [Entry];
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
    Assert(Entry in FRowConstraints[Row]);
    Assert(Entry in FColConstraints[Col]);
    Assert(Entry in FBlockConstraints[Map]);
    FRowConstraints[Row] := FRowConstraints[Row] - [Entry];
    FColConstraints[Col] := FColConstraints[Col] - [Entry];
    FBlockConstraints[Map] := FBlockConstraints[Map] - [Entry];
    FBoard[Row][Col] := 0;
  end;
end;

function TSBoardState.GetAllowedSet(Row: TSNumber; Col: TSNumber): TSNumberSet;
begin
  if FBoard[Row][Col] <> 0 then
    result := []
  else
  begin
    result := AllAllowed -
      (FRowConstraints[Row] + FColConstraints[Col]
        + FBlockConstraints[MapToBlockSetIdx(Row, Col)]);
  end;
end;

function TSBoardState.MapToBlockSetIdx(Row, Col: TSNumber): TSNumber;
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

procedure TSBoardState.Assign(Src: TSBoardState);
begin
  FBoard := Src.FBoard;
  FRowConstraints := Src.FRowConstraints;
  FColConstraints := Src.FColConstraints;
  FBlockConstraints := Src.FBlockConstraints;
end;

end.
