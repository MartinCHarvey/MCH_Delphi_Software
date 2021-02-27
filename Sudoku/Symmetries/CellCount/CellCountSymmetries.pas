unit CellCountSymmetries;

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SharedSymmetries;

type
  TCellCountBoard = class(TSymBoard)
  protected
  public
    function CountCellsLine(Row: boolean; Idx: TRowColIdx): integer; inline;
    function AmIsomorphicTo(Other: TSymBoard): boolean; override;
  end;

  //////////////// Counts.

  TRowColCellCount = 0.. (ORDER * ORDER);
  TRowColCellCountSet = set of TRowColCellCount;

  TStackBandCellCount = 0 .. (ORDER * ORDER * ORDER);
  TStackBandCellCountSet = set of TStackBandCellCount;

  //Rows or cols in a stack or band.
  TRowColCountInfo = record
    CellCounts: array[TOrderIdx] of TRowColCellCount;
    CellCountSet: TRowColCellCountSet;
  end;
  PRowColCountInfo = ^TRowColCountInfo;

  TStackBandCountInfo = record
    CellCounts: array[TOrderIdx] of TStackBandCellCount;
    CellCountSet: TStackBandCellCountSet;
    UnpermutedLineInfo: array[TOrderIdx] of TRowColCountInfo;
  end;
  PStackBandCountInfo = ^TStackBandCountInfo;

  TCountInfo = array [boolean] of TStackBandCountInfo;
  PCountInfo = ^TCountInfo;

  //TODO - Should I make this a set type. Faster?
  TAllowedPerms = record
    PermIdxs: TPermIdxList;
    Count: TPermIdx;
  end;

  TRowColAllowedPerms = array[TOrderIdx] of TAllowedPerms;
  TRowColSelectedPerms = array[TOrderIdx] of TPermIdx;

  /////////////// Permutations.

{$IFDEF USE_TRACKABLES}
  TCellCountIsomorphismChecker = class(TTrackable)
{$ELSE}
  TCellCountIsomorphismChecker = class
{$ENDIF}
  private
    //Counts all unpermuted.
    FCountsA, FCountsB: TCountInfo;
  protected
    function  MatchCell(A,B: TCellCountBoard;
                        ABand, ARowInBand, AStack, AColInStack,
                        BBand, BRowInBand, BStack, BColInStack: TOrderIdx;
                        InitialReflect: boolean): boolean;

    procedure SetupPermutations(InitialBandPerm, InitialStackPerm: TOrderIdx;
                                InitialReflect: boolean;
                                var RowPerms:TRowColAllowedPerms;
                                var ColPerms:TRowColAllowedPerms);

    procedure SetupCounts(A,B: TCellCountBoard);
    procedure StackBandPermSet(PA, PB: PStackBandCountInfo; var PL: TAllowedPerms);
    procedure RowColPermSet(PA, PB: PRowColCountInfo; var PL: TAllowedPerms);
    function IsomorphicRowCol(A,B: TCellCountBoard;
                              InitialReflect: boolean;
                              InitialBandPerm, InitialStackPerm: TPermIdx): boolean; //Band = Row
    function IsomorphicStackBand(A,B: TCellCountBoard):boolean;
  public
    function AreIsomorphic(A,B: TCellCountBoard):boolean;
  end;

implementation

{ TCellCountBoard }

function TCellCountBoard.CountCellsLine(Row: boolean; Idx: TRowColIdx): integer;
var
  VarIdx: TRowColIdx;
  Contents: integer;
begin
  result := 0;
  for VarIdx := Low(VarIdx) to High(VarIdx) do
  begin
    if Row then
      Contents := Entries[Idx, VarIdx]
    else
      Contents := Entries[VarIdx, Idx];
    if Contents <> 0 then
      Inc(Result);
  end;
end;

function TCellCountBoard.AmIsomorphicTo(Other: TSymBoard): boolean;
var
  C: TCellCountIsomorphismChecker;
begin
  C := TCellCountIsomorphismChecker.Create;
  try
    result := C.AreIsomorphic(self, Other as TCellCountBoard);
  finally
    C.Free;
  end;
end;

{ TCellCountIsomorphismChecker }

procedure TCellCountIsomorphismChecker.SetupCounts(A,B: TCellCountBoard);
var
  Row, Board: boolean;
  StackBandIdx, LineIdx: TOrderIdx;
  AbsLineIdx: TRowColIdx;
  PCounts: PCountInfo;
  BoardSrc: TCellCountBoard;
begin
  for Board := Low(Board) to High(Board) do
  begin
    if Board = Low(Board) then
    begin
      PCounts := @FCountsA;
      BoardSrc := A;
    end
    else
    begin
      PCounts := @FCountsB;
      BoardSrc := B;
    end;
    for Row := Low(Row) to High(Row) do
    begin
      for StackBandIdx := Low(StackBandIdx) to High(StackBandIdx) do
      begin
        with PCounts[Row] do
        begin
          for LineIdx := Low(LineIdx) to High(LineIdx) do
          begin
            RelToAbsIndexed(StackBandIdx, LineIdx, AbsLineIdx);
            with UnpermutedLineInfo[StackBandIdx] do
            begin
              CellCounts[LineIdx] := BoardSrc.CountCellsLine(Row, AbsLineIdx);
              CellCountSet := CellCountSet + [CellCounts[LineIdx]];
            end;
            CellCounts[StackBandIdx] := CellCounts[StackBandIdx] +
              UnpermutedLineInfo[StackBandIdx].CellCounts[LineIdx];
          end;
          CellCountSet := CellCountSet+ [CellCounts[StackBandIdx]];
        end;
      end;
    end;
  end;
end;

//TODO - Yuck yuck - so similar to stack band perm set, but not quite same types.
procedure TCellCountIsomorphismChecker.RowColPermSet(PA, PB: PRowColCountInfo; var PL: TAllowedPerms);
var
  TestPermIdx: TPermIdx;
  OIdx: TOrderIdx;
  PermutedBCounts: array[TOrderIdx] of TRowColCellCount;
  PGood: boolean;
  SelectedPerm: ^TPermutation;
begin
  FillChar(PL, sizeof(PL), 0);
  if PA.CellCountSet <> PB.CellCountSet then
     exit; //No perm of the two is going to match.

  if (PA.CellCountSet = [0]) or
     (PA.CellCountSet =  [ORDER * ORDER]) then //No cells or all cells.
  begin
    //All perms should work, but we'll try just one.
    PL.PermIdxs[PL.Count] := 0;
    Inc(PL.Count);
    exit;
  end;
  //Okay, check which perms make counts match
  for TestPermIdx := Low(TPermIdx) to High(TPermIdx) do
  begin
    PGood := true;
    SelectedPerm := @Permlist[TestPermIdx];
    for OIdx := Low(TOrderIdx) to High(TOrderIdx) do
      PermutedBCounts[OIdx] := PB.CellCounts[SelectedPerm[OIdx]];
    for OIdx := Low(TOrderIdx) to High(TOrderIdx) do
      if PermutedBCounts[OIdx] <> PA.CellCounts[OIdx] then
      begin
        PGood := false;
        break;
      end;
    if PGood then
    begin
      PL.PermIdxs[PL.Count] := TestPermIdx;
      Inc(PL.Count);
    end;
  end;
end;

function ResetPermIdxs(const RAllowed, CAllowed: TRowColAllowedPerms;
                       var RSelected, CSelected: TRowColSelectedPerms): boolean;
var
  i: TOrderIdx;
begin
  for i := Low(TOrderIdx) to High(TOrderIdx) do
  begin
    RSelected[i] := 0;
    CSelected[i] := 0;
    //Some of the rows or cols cannot be permuted to produce the
    //required result in a stack or band.
    if (RAllowed[i].Count = 0) or (CAllowed[i].Count = 0) then
    begin
      result := false;
      exit;
    end;
  end;
  result := true;
end;

procedure IncPermIdxC(var Carry: boolean; const Perms: TAllowedPerms; var PermIdx: TPermIdx);
begin
  Carry := PermIdx >= Pred(Perms.Count);
  if not Carry then
    Inc(PermIdx)
  else
    PermIdx := 0;
end;

function IncPermIdxs(const RowPerms, ColPerms: TRowColAllowedPerms;
                     var RowPermIdx, ColPermIdx: TRowColSelectedPerms): boolean;

var
  Carry, IncrementingCols: boolean;
  IncrementOrder: TOrderIdx;
begin
  Carry := false;
  IncrementingCols := false;
  IncrementOrder := Low(IncrementOrder);
  repeat
    if IncrementingCols then
      IncPermIdxC(Carry, ColPerms[IncrementOrder], ColPermIdx[IncrementOrder])
    else
      IncPermIdxC(Carry, RowPerms[IncrementOrder], RowPermIdx[IncrementOrder]);
    if Carry then
    begin
      if IncrementOrder < High(IncrementOrder) then
        Inc(IncrementOrder)
      else
      begin
        IncrementOrder := Low(IncrementOrder);
        if IncrementingCols < High(IncrementingCols) then
          Inc(IncrementingCols)
        else
        begin
          result := true;
          exit;
        end;
      end;
    end;
  until not Carry;
  result := false;
end;

function  TCellCountIsomorphismChecker.MatchCell(A,B: TCellCountBoard;
                    ABand, ARowInBand, AStack, AColInStack,
                    BBand, BRowInBand, BStack, BColInStack: TOrderIdx;
                    InitialReflect: boolean): boolean;
var
  AAbsRow, AAbsCol, BAbsRow, BAbsCol: TRowColIdx;
  CellA, CellB: integer;
begin
  RelToAbsIndexed(ABand, ARowInBand, AAbsRow);
  RelToAbsIndexed(AStack, AColInStack, AAbsCol);
  RelToAbsIndexed(BBand, BRowInBand, BAbsRow);
  RelToAbsIndexed(BStack, BColInStack, BAbsCol);
  CellA := A.Entries[AAbsRow, AAbsCol];
  if not InitialReflect then
    CellB := B.Entries[BAbsRow, BAbsCol]
  else
    CellB := B.Entries[BAbsCol, BAbsRow];
  result := CellA = CellB;
end;

procedure TCellCountIsomorphismChecker.SetupPermutations(InitialBandPerm, InitialStackPerm: TOrderIdx;
                                                         InitialReflect: boolean;
                                                         var RowPerms:TRowColAllowedPerms;
                                                         var ColPerms:TRowColAllowedPerms);
var
  PACounts, PBCounts: PRowColCountInfo;
  ABand, BBand: TOrderIdx;
  Rows: boolean;
begin
  for Rows := Low(Rows) to High(Rows) do
  begin
    //Initial band, stack and reflect operations.
    for ABand := Low(ABand) to High(ABand) do
    begin
      if Rows then
        BBand := Permlist[InitialBandPerm][ABand]
      else
        BBand := PermList[InitialStackPerm][ABand];
      PACounts := @FCountsA[Rows].UnpermutedLineInfo[ABand];
      if not InitialReflect then
        PBCounts := @FCountsB[Rows].UnpermutedLineInfo[BBand]
      else
        PBCounts := @FCountsB[not Rows].UnpermutedLineInfo[BBand];
      if Rows then
        RowColPermSet(PACounts, PBCounts, RowPerms[ABand])
      else
        RowColPermSet(PACounts, PBCounts, ColPerms[ABand]);
    end;
  end;
end;

function TCellCountIsomorphismChecker.IsomorphicRowCol(A,B: TCellCountBoard;
                          InitialReflect: boolean;
                          InitialBandPerm, InitialStackPerm: TPermIdx): boolean; //Band = Row

const
  RefineColSets: boolean = false;

var
  RowPerms, ColPerms: TRowColAllowedPerms;
  RowPermIdx, ColPermIdx: TRowColSelectedPerms;

var
  ABand, BBand, AStack, BStack: TOrderIdx;
  ARowInBand, BRowInBand, AColInStack, BColInStack: TOrderIdx;
  MatchThisPerm: boolean;
  SelectedRowPermIdx, SelectedColPermIdx: TPermIdx;

label
  NextPermNoRefine, NextPermWithRefine;

begin
  SetupPermutations(InitialBandPerm, InitialStackPerm, InitialReflect,
                    RowPerms, ColPerms);
  if not ResetPermIdxs(RowPerms, ColPerms, RowPermIdx, ColPermIdx) then
  begin
    result := false;
    exit;
  end;
  //TODO - RefineColSets = true... ?
  if not RefineColSets then //Not sure which will be quicker / better.
  begin
    //Go through all the perms checking for a match on the entire board.
    repeat
      MatchThisPerm := true;
      for ABand := Low(ABand) to High(ABand) do
      begin
        BBand := Permlist[InitialBandPerm][ABand];
        SelectedRowPermIdx := RowPerms[ABand].PermIdxs[RowPermIdx[ABand]];
        for AStack := Low(AStack) to High(AStack) do
        begin
          BStack := PermList[InitialStackPerm][AStack];
          SelectedColPermIdx := ColPerms[AStack].PermIdxs[ColPermIdx[AStack]];
          for ARowInBand := Low(ARowInBand) to High(ARowInBand) do
          begin
            BRowInBand := PermList[SelectedRowPermIdx][ARowInBand];
            for AColInStack := Low(AColInStack) to High(AColInStack) do
            begin
              BColInStack := PermList[SelectedColPermIdx][AColInStack];
              if not MatchCell(A, B, ABand, ARowInBand,AStack, AColInStack,
                               BBand, BRowInBand, BStack, BColInStack, InitialReflect) then
              begin
                MatchThisPerm := false;
                goto NextPermNoRefine;
              end;
            end;
          end;
        end;
      end;
NextPermNoRefine:
      if MatchThisPerm then
      begin
        result := true;
        exit;
      end;
    until IncPermIdxs(RowPerms, ColPerms, RowPermIdx, ColPermIdx);
  end
  else
  begin
    Assert(false); // TODO - More fancy algorithm.

    //Now, can do the bands separately. For each band, find out what the set
    //of actual row/col permutations is that provides a match, and re-use
    //the col permutations for the next band.
  end;
  result := false;
end;

procedure TCellCountIsomorphismChecker.StackBandPermSet(PA, PB: PStackBandCountInfo; var PL: TAllowedPerms);
var
  TestPermIdx: TPermIdx;
  OIdx: TOrderIdx;
  PermutedBCounts: array[TOrderIdx] of TStackBandCellCount;
  PGood: boolean;
  SelectedPerm: ^TPermutation;
begin
  FillChar(PL, sizeof(PL), 0);
  if PA.CellCountSet <> PB.CellCountSet then
     exit; //No perm of the two is going to match.

  if (PA.CellCountSet = [0]) or
     (PA.CellCountSet =  [ORDER * ORDER * ORDER]) then //No cells or all cells.
  begin
    //All perms should work, but we'll try just one.
    PL.PermIdxs[PL.Count] := 0;
    Inc(PL.Count);
    exit;
  end;
  //Okay, check which perms make counts match
  for TestPermIdx := Low(TPermIdx) to High(TPermIdx) do
  begin
    PGood := true;
    SelectedPerm := @Permlist[TestPermIdx];
    for OIdx := Low(TOrderIdx) to High(TOrderIdx) do
      PermutedBCounts[OIdx] := PB.CellCounts[SelectedPerm[OIdx]];
    for OIdx := Low(TOrderIdx) to High(TOrderIdx) do
      if PermutedBCounts[OIdx] <> PA.CellCounts[OIdx] then
      begin
        PGood := false;
        break;
      end;
    if PGood then
    begin
      PL.PermIdxs[PL.Count] := TestPermIdx;
      Inc(PL.Count);
    end;
  end;
end;

function TCellCountIsomorphismChecker.IsomorphicStackBand(A,B: TCellCountBoard):boolean;
var
  PARowCounts, PBRowCounts: PStackBandCountInfo;
  PAColCounts, PBColCounts: PStackBandCountInfo;
  RowPerms, ColPerms: TAllowedPerms;
  RowPermIdx, ColPermIdx: TPermIdx;
  Reflect: boolean;
begin
  result := false;
  for Reflect := Low(Reflect) to High(Reflect) do
  begin
    PARowCounts := @FCountsA[true];
    PAColCounts := @FCountsA[false];
    if not Reflect then
    begin
      PBRowCounts := @FCountsB[true];
      PBColCounts := @FCountsB[false];
    end
    else
    begin
      PBRowCounts := @FCountsB[false];
      PBColCounts := @FCountsB[true];
    end;
    StackBandPermSet(PARowCounts, PBRowCounts, RowPerms);
    StackBandPermSet(PAColCounts, PBColCounts, ColPerms);
    for RowPermIdx := 0 to Pred(RowPerms.Count) do
    begin
      for ColPermIdx := 0 to Pred(ColPerms.Count) do
      begin
        if IsomorphicRowCol(A, B, Reflect,
                         RowPerms.PermIdxs[RowPermIdx],
                         ColPerms.PermIdxs[ColPermIdx]) then
        begin
          result := true;
          exit;
        end;
      end;
    end;
  end;
end;

function TCellCountIsomorphismChecker.AreIsomorphic(A,B: TCellCountBoard):boolean;
begin
  SetupCounts(A, B);
  result := IsomorphicStackBand(A,B);
end;

end.
