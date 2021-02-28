unit CellCountSymmetries;

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SharedSymmetries;

{$IFDEF ORDER2}
{$DEFINE OPTIMISE_SMALL_SETS}
{$ENDIF}
{$IFDEF ORDER3}
{$DEFINE OPTIMISE_SMALL_SETS}
{$DEFINE UNROLL_INNER_LOOP}
{$ENDIF}

type
  TCellCountBoard = class(TSymBoard)
  protected
    FRowCounts, FColCounts: TSymRow;

    function FastGetBoardEntry(Row, Col: TRowColIdx): integer; inline;
    procedure SetBoardEntry(Row, Col: TRowColIdx; Entry: integer); override;
  public
    //No override assign needed, CellCount setter does the work
    function AmIsomorphicTo(Other: TSymBoard): boolean; override;
    property FastEntries[Row, Col: TRowColIdx]: integer read FastGetBoardEntry;
  end;

  //////////////// Counts.

  TRowColCellCount = 0.. (ORDER * ORDER);
  TStackBandCellCount = 0 .. (ORDER * ORDER * ORDER);

{$IFDEF OPTIMISE_SMALL_SETS}
  TSmallSet = type UInt32;

  TStackBandCellCountSet = TSmallSet;
  TRowColCellCountSet = TSMallSet;
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

    function SetupPermutations(InitialBandPerm, InitialStackPerm: TOrderIdx;
                                InitialReflect: boolean;
                                var RowPerms:TRowColAllowedPerms;
                                var ColPerms:TRowColAllowedPerms): boolean;

    procedure SetupCounts(A,B: TCellCountBoard);
    function StackBandPermSet(PA, PB: PStackBandCountInfo; var PL: TAllowedPerms):boolean; //Returns any valid permutations.
    function RowColPermSet(PA, PB: PRowColCountInfo; var PL: TAllowedPerms): boolean; //Returns any valid permutations.
    function IsomorphicRowCol(A,B: TCellCountBoard;
                              InitialReflect: boolean;
                              InitialBandPerm, InitialStackPerm: TPermIdx): boolean; //Band = Row
    function IsomorphicStackBand(A,B: TCellCountBoard):boolean;
  public
    function AreIsomorphic(A,B: TCellCountBoard):boolean;
  end;

implementation

{ Misc functions }

{$IFDEF OPTIMISE_SMALL_SETS}
function SmallSetUnion(A, B: TSmallSet): TSmallSet; inline;
begin
  result := A or B;
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
{$ENDIF}

{ TCellCountBoard }

procedure TCellCountBoard.SetBoardEntry(Row, Col: TRowColIdx; Entry: integer);
begin
  if FastEntries[Row, Col] <> 0 then
  begin
    if Entry = 0 then
    begin
      Dec(Self.FRowCounts[Row]);
      Dec(Self.FColCounts[Col]);
    end;
  end
  else
  begin
    if Entry <> 0 then
    begin
      Inc(Self.FRowCounts[Row]);
      Inc(Self.FColCounts[Col]);
    end;
  end;
  inherited;
end;

function TCellCountBoard.FastGetBoardEntry(Row, Col: TRowColIdx): integer;
begin
  result := FSymBoardState[Row, Col];
end;


function TCellCountBoard.AmIsomorphicTo(Other: TSymBoard): boolean;
var
  C: TCellCountIsomorphismChecker;
begin
  C := TCellCountIsomorphismChecker.Create;
  try
    Assert(Other is TCellCountBoard);
    result := C.AreIsomorphic(self, TCellCountBoard(Other));
  finally
    C.Free;
  end;
end;

{ TCellCountIsomorphismChecker }


procedure TCellCountIsomorphismChecker.SetupCounts(A,B: TCellCountBoard);
var
  Row:boolean;
  Board: boolean;
  StackBandIdx:TOrderIdx;
{$IFNDEF UNROLL_INNER_LOOP}
  LineIdx: TOrderIdx;
{$ENDIF}
  AbsLineIdx: TRowColIdx;
  PCounts: PCountInfo;
  BoardSrc: TCellCountBoard;
  Tmp: TRowColCellCount;
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
{$IFDEF UNROLL_INNER_LOOP}
///////////// 0
          RelToAbsIndexed(StackBandIdx, 0, AbsLineIdx);
          with UnpermutedLineInfo[StackBandIdx] do
          begin
            if Row then
              Tmp := BoardSrc.FRowCounts[AbsLineIdx]
            else
              Tmp := BoardSrc.FColCounts[AbsLineIdx];
            CellCounts[0] := Tmp;
{$IFDEF OPTIMISE_SMALL_SETS}
            CellCountSet := SmallSetUnion(CellCountSet, SmallSetUnitary(Tmp));
{$ELSE}
            CellCountSet := CellCountSet + [Tmp];
{$ENDIF}
          end;
          Inc(CellCounts[StackBandIdx], Tmp);
///////////// 1
          RelToAbsIndexed(StackBandIdx, 1, AbsLineIdx);
          with UnpermutedLineInfo[StackBandIdx] do
          begin
            if Row then
              Tmp := BoardSrc.FRowCounts[AbsLineIdx]
            else
              Tmp := BoardSrc.FColCounts[AbsLineIdx];
            CellCounts[1] := Tmp;
{$IFDEF OPTIMISE_SMALL_SETS}
            CellCountSet := SmallSetUnion(CellCountSet, SmallSetUnitary(Tmp));
{$ELSE}
            CellCountSet := CellCountSet + [Tmp];
{$ENDIF}
          end;
          Inc(CellCounts[StackBandIdx], Tmp);
///////////// 2
          RelToAbsIndexed(StackBandIdx, 2, AbsLineIdx);
          with UnpermutedLineInfo[StackBandIdx] do
          begin
            if Row then
              Tmp := BoardSrc.FRowCounts[AbsLineIdx]
            else
              Tmp := BoardSrc.FColCounts[AbsLineIdx];
            CellCounts[2] := Tmp;
{$IFDEF OPTIMISE_SMALL_SETS}
            CellCountSet := SmallSetUnion(CellCountSet, SmallSetUnitary(Tmp));
{$ELSE}
            CellCountSet := CellCountSet + [Tmp];
{$ENDIF}
          end;
          Inc(CellCounts[StackBandIdx], Tmp);
{$ELSE} //UNROLL_INNER_LOOP
////////////////////////////////////////////
          for LineIdx := Low(LineIdx) to High(LineIdx) do
          begin
            RelToAbsIndexed(StackBandIdx, LineIdx, AbsLineIdx);
            with UnpermutedLineInfo[StackBandIdx] do
            begin
              if Row then
                Tmp := BoardSrc.FRowCounts[AbsLineIdx]
              else
                Tmp := BoardSrc.FColCounts[AbsLineIdx];
              CellCounts[LineIdx] := Tmp;
{$IFDEF OPTIMISE_SMALL_SETS}
              CellCountSet := SmallSetUnion(CellCountSet, SmallSetUnitary(Tmp));
{$ELSE}
              CellCountSet := CellCountSet + [Tmp];
{$ENDIF}
            end;
            Inc(CellCounts[StackBandIdx], Tmp);
          end;
{$ENDIF}
{$IFDEF OPTIMISE_SMALL_SETS}
          CellCountSet := SmallSetUnion(CellCountSet, SmallSetUnitary(CellCounts[StackBandIdx]));
{$ELSE}
          CellCountSet := CellCountSet+ [CellCounts[StackBandIdx]];
{$ENDIF}
        end;
      end;
    end;
  end;
end;

//TODO - Yuck yuck - so similar to stack band perm set, but not quite same types.
function TCellCountIsomorphismChecker.RowColPermSet(PA, PB: PRowColCountInfo; var PL: TAllowedPerms): boolean;
var
  TestPermIdx: TPermIdx;
  OIdx: TOrderIdx;
  PGood: boolean;
  SelectedPerm: ^TPermutation;
begin
  FillChar(PL, sizeof(PL), 0);
  if PA.CellCountSet <> PB.CellCountSet then
  begin
     result := false;
     exit; //No perm of the two is going to match.
  end;
  result := true;

{$IFDEF OPTIMISE_SMALL_SETS}
  if (PA.CellCountSet = SmallSetUnitary(0)) or
     (PA.CellCountSet =  SmallSetUnitary(ORDER * ORDER)) then //No cells or all cells.
{$ELSE}
  if (PA.CellCountSet = [0]) or
     (PA.CellCountSet =  [ORDER * ORDER]) then //No cells or all cells.
{$ENDIF}
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
      if PB.CellCounts[SelectedPerm[OIdx]] <> PA.CellCounts[OIdx] then
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

procedure ResetPermIdxs(const RAllowed, CAllowed: TRowColAllowedPerms;
                       var RSelected, CSelected: TRowColSelectedPerms);
var
  i: TOrderIdx;
begin
  for i := Low(TOrderIdx) to High(TOrderIdx) do
  begin
    RSelected[i] := 0;
    CSelected[i] := 0;
    //Some of the rows or cols cannot be permuted to produce the
    //required result in a stack or band.
    Assert((RAllowed[i].Count <> 0) and (CAllowed[i].Count <> 0));
  end;
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
  CellA := A.FastEntries[AAbsRow, AAbsCol];
  if not InitialReflect then
    CellB := B.FastEntries[BAbsRow, BAbsCol]
  else
    CellB := B.FastEntries[BAbsCol, BAbsRow];
  result := CellA = CellB;
end;

function TCellCountIsomorphismChecker.SetupPermutations(InitialBandPerm, InitialStackPerm: TOrderIdx;
                                                         InitialReflect: boolean;
                                                         var RowPerms:TRowColAllowedPerms;
                                                         var ColPerms:TRowColAllowedPerms): boolean;
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
        result := RowColPermSet(PACounts, PBCounts, RowPerms[ABand])
      else
        result := RowColPermSet(PACounts, PBCounts, ColPerms[ABand]);
      if not result then
        exit;
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

  ABand, BBand, AStack, BStack: TOrderIdx;
  ARowInBand, BRowInBand, AColInStack, BColInStack: TOrderIdx;
  MatchThisPerm: boolean;
  SelectedRowPermIdx, SelectedColPermIdx: TPermIdx;

label
  NextPermNoRefine, NextPermWithRefine;

begin
  if not SetupPermutations(InitialBandPerm, InitialStackPerm, InitialReflect,
                    RowPerms, ColPerms) then
  begin
    result := false;
    exit;
  end;
  ResetPermIdxs(RowPerms, ColPerms, RowPermIdx, ColPermIdx);
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

function TCellCountIsomorphismChecker.StackBandPermSet(PA, PB: PStackBandCountInfo; var PL: TAllowedPerms): boolean;
var
  TestPermIdx: TPermIdx;
  OIdx: TOrderIdx;
  PGood: boolean;
  SelectedPerm: ^TPermutation;
begin
  FillChar(PL, sizeof(PL), 0);
  if PA.CellCountSet <> PB.CellCountSet then
  begin
     result := false;
     exit; //No perm of the two is going to match.
  end;
  result := true;

{$IFDEF OPTIMISE_SMALL_SETS}
  if (PA.CellCountSet = SmallSetUnitary(0)) or
     (PA.CellCountSet =  SmallSetUnitary(ORDER * ORDER * ORDER)) then //No cells or all cells.
{$ELSE}
  if (PA.CellCountSet = [0]) or
     (PA.CellCountSet =  [ORDER * ORDER * ORDER]) then //No cells or all cells.
{$ENDIF}
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
      if PB.CellCounts[SelectedPerm[OIdx]] <> PA.CellCounts[OIdx] then
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
    if not (StackBandPermSet(PARowCounts, PBRowCounts, RowPerms)
            and StackBandPermSet(PAColCounts, PBColCounts, ColPerms)) then
      continue;
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
