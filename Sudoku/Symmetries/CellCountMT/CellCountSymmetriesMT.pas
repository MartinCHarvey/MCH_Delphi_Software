unit CellCountSymmetriesMT;

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SharedSymmetries, IndexedStore, SysUtils, SyncObjs, CellCountShared;

//{$DEFINE DEBUG_CELLCOUNTS}

//TODO - Go thru and work out which types are redundnt.

{$IFDEF ORDER2}
{$DEFINE OPTIMISE_SMALL_SETS}
{$ENDIF}
{$IFDEF ORDER3}
{$DEFINE OPTIMISE_SMALL_SETS}
{$ENDIF}

{
  A note on canonical form used here:

  This canonical form is not absolute minlex, it's just a canonical form
  easily done with what I have so far.
  Importantly, if grids are isomorphic, they will reduce to the same canonical form,
  even though it's not minlex. The row col and block orderings will be sorted
  the same way, and the (relatively) best found for each.

  Given the way we are iterating through solutions and finding isomorphisms,
  if each solution is given a number, and the number of its parent, then
  they can be easily sorted into minlex order - we can give each one
  a numbering of where the selected cells are:

  eg 00-04-17-19-46-80.

  For the future:

  A full minlex solution can be obtained by:
  1. Take each block (2x2), (3x3)
  2. Set all possible permutations of row/col stack/block as possible (set).
  3. Find perm combinations which give minlex for each block (rows / cols).
     (rows sorted by cellcount, col perms set to shift bits left in bottom row,
      then next row, etc).
  4. Put that block highest, and constrain permutations appropriately.
  4a) If ambigous which block, need to consider all possible blocks you could put
      highest, so you might need to recurse and backtrack...
  5. Repeat procedure for remaining blocks, subject to sets of row/col perms still
     allowed .... And subject to block orderings still allowed.
}


const
  CountsSigValidFlag = (1 shl 0);
  CanonicalValidFlag = (1 shl 1);

type
  TCellCountMTBoard = class(TSymBoardPacked)
  protected
    FCanonicalRep: TSymBoardPackedState;
    FCountInfo: TCountInfoMT;
    FFlags: Byte;
    //Unpermuted line info (CellCounts) is primary information,
    //All other counts calculated from there.

    procedure SetBoardEntry(Row, Col: TRowColIdx; Entry: integer); override;
    procedure SetupCounts;
{$IFDEF DEBUG_CELLCOUNTS}
    procedure DumpCounts;
    procedure DumpAllowedPerms(const CanonicalAllowedPerms: TAllowedPerms);
    procedure DumpSelectedPerms(const CanonicalAllowedPerms: TAllowedPerms;
                                const CurrentSelectedPerms: TSelectedPerms);
{$ENDIF}
    procedure FindCanonicalPerms(var CanonicalAllowedPerms: TAllowedPerms);
    procedure MakeCanonicalString(const CanonicalAllowedPerms: TAllowedPerms);
    procedure Invalidate;
    function GetCanonicalRep: TSymBoardPackedState; inline;
    function GetMinLexRep: TSymBoardPackedState; inline;
    procedure OptCalcSig;
    procedure OptCalcCanonical;
    function GetBoardEntryFast(Row, Col: TRowColIdx): boolean; inline;
    property FastEntries[Row, Col: TRowColIdx]: boolean read GetBoardEntryFast;
  public
    constructor Create; override;
    destructor Destroy; override;

    //No override assign needed, CellCount setter does the work
    function AmIsomorphicTo(Other: TSymBoardAbstract): boolean; override;
    property CanonicalRep: TSymBoardPackedState read GetCanonicalRep;
    property MinlexRep: TSymBoardPackedState read GetMinLexRep;
  end;

  TCellCountBoardClass = class of TCellCountMTBoard;

  //TODO - Work on these states a bit.
  TTraverseState = (ttsAdding, ttsTraversing, ttsTraversed);
  TSSAddBoardRet = (abrNotMinimal, abrCanAdd, abrAdded);
  TLexStrIndexNodeType = (lsitCanonical, lsitMinlex);

  TPackedRepSearchVal = class;

{$IFDEF USE_TRACKABLES}
  TMTIsoList = class(TTrackable)
{$ELSE}
  TMTIsoList = class
{$ENDIF}
  public
    FStore: TIndexedStore;
  private
    FLock: TMultiReadExclusiveWriteSynchronizer;
    FTraverseState: TTraverseState;
    FTravIrec: TItemRec;
    function GetCount: integer;
    function AddBoardInternal(Board: TCellCountMTBoard; Readonly: boolean; SV: TPackedRepSearchVal): TSSAddBoardRet;
    procedure ListStateCanonicalMode;
    procedure ListStateMinlexMode;
  public
    constructor Create;
    destructor Destroy; override;
    //Pass in a LexStrSearchVal that is thread local.
    //Saves mem alloc costs.
    function AddBoard(Board: TCellCountMTBoard; SV: TPackedRepSearchVal): boolean;
    function GetInMinlexOrder: TCellCountMTBoard;
    procedure Clear;
    procedure SetTraversed;
    procedure ResetTraverse;
    property Count: integer read GetCount;
  end;


  TPackedRepIndexNode = class(TIndexNode)
  public
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;override;
  end;

  TPackedRepSearchVal = class(TPackedRepIndexNode)
  public
    SearchVal: TSymBoardPackedState;
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;override;
  end;

implementation

uses
  StrUtils;

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

{ TCellCountMTBoard }

constructor TCellCountMTBoard.Create;
begin
  inherited;
end;

destructor TCellCountMTBoard.Destroy;
begin
  inherited;
end;

procedure TCellCountMTBoard.SetBoardEntry(Row, Col: TRowColIdx; Entry: integer);
var
  Band, RelRow, Stack, RelCol: TOrderIdx;
begin
  AbsToRelIndexed(Row, Band, RelRow);
  AbsToRelIndexed(Col, Stack, RelCol);
  if Entries[Row, Col] <> 0 then
  begin
    if Entry = 0 then
    begin
      Invalidate;
      with FCountInfo[true] do
      begin
        Assert(UnpermutedLineInfo[Band][RelRow] > 0);
        Dec(UnpermutedLineInfo[Band][RelRow]);
      end;
      with FCountInfo[false] do
      begin
        Assert(UnpermutedLineInfo[Stack][RelCol] > 0);
        Dec(UnpermutedLineInfo[Stack][RelCol]);
      end;
    end;
  end
  else
  begin
    if Entry <> 0 then
    begin
      Invalidate;
      Inc(FCountInfo[true].UnpermutedLineInfo[Band][RelRow]);
      Inc(FCountInfo[false].UnpermutedLineInfo[Stack][RelCol]);
    end;
  end;
  inherited;
end;

function SBSetToStr(S: TStackBandCellCountSet): string;
var
  C: TStackBandCellCount;
begin
  SetLength(result, 0);
  for C := Low(C) to High(C) do
  begin
{$IFDEF OPTIMISE_SMALL_SETS}
    if SmallSetIn(C,S) then
{$ELSE}
    if C in S then
{$ENDIF}
      result := result + IntToStr(C)
    else
      result := result + '.';
  end;
end;

function RCSetToStr(R: TRowColCellCountSet): string;
var
  C: TRowColCellCount;
begin
  SetLength(result, 0);
  for C := Low(C) to High(C) do
  begin
{$IFDEF OPTIMISE_SMALL_SETS}
    if SmallSetIn(C,R) then
{$ELSE}
    if C in R then
{$ENDIF}
      result := result + IntToStr(C)
    else
      result := result + '.';
  end;
end;

{$IFDEF DEBUG_CELLCOUNTS}
procedure TCellCountMTBoard.DumpCounts;
var
  Row: boolean;
  OIdx: TOrderIdx;
  LIdx: TOrderIdx;
begin
  for Row := Low(Row) to High(Row) do
  begin
    WriteLn;
    if Row then
      WriteLn('Row counts:')
    else
      WriteLn('Col Counts:');
    for OIdx := Low(OIdx) to High(OIdx) do
      WriteLn('SB CellCount [' , OIdx, ']: ' , FCountInfo[Row].CellCounts[OIdx]);

    for OIdx := Low(OIdx) to High(OIdx) do
    begin
      for LIdx := Low(LIdx) to High(LIdx) do
        WriteLn('RC CellCount [' , LIdx , ']: ', FCountInfo[Row].UnpermutedLineInfo[OIdx][LIdx]);
    end;
  end;
end;
{$ENDIF}

procedure TCellCountMTBoard.SetupCounts;
var
  Row: boolean;
  StackBand: TOrderIdx;
  RowCol: TOrderIdx;
  Tmp: TRowColCellCount;
  Tmp2: TStackBandCellCount;
begin
  for Row := Low(Row) to High(Row) do
  begin
    with FCountInfo[Row] do
    begin
      for StackBand := Low(StackBand) to High(StackBand) do
      begin
        Tmp2 := 0;
        for RowCol := Low(RowCol) to High(RowCol) do
        begin
          Tmp := UnpermutedLineInfo[StackBand][RowCol];
          Inc(Tmp2, Tmp);
        end;
        CellCounts[StackBand] := Tmp2;
      end;
    end;
  end;
{$IFDEF DEBUG_CELLCOUNTS}
  DumpCounts;
{$ENDIF}
end;

{$IFDEF DEBUG_CELLCOUNTS}
procedure TCellCountMTBoard.DumpAllowedPerms(const CanonicalAllowedPerms: TAllowedPerms);

  function DumpGenAllowed(const P: TGenAllowedPerms): string;
  var
    i: Integer;
  begin
    result := '{';
    for i := 0 to Pred(P.Count) do
    begin
      result := Result + IntToStr(P.PermIdxs[i]);
      if i < Pred(P.Count) then
        result := result + ', ';
    end;
    result := result + '}';
  end;

var
  Row: Boolean;
  OIdx: TOrderIdx;
begin
  for Row := Low(Row) to High(Row) do
  begin
    WriteLn;
    if Row then
      WriteLn('Row allowed perms:')
    else
      WriteLn('Col allowed perms:');
    WriteLn('StackBand: ' + DumpGenAllowed(CanonicalAllowedPerms[Row].StackBandAllowed));
    for OIdx := Low(OIdx) to High(OIdx) do
      WriteLn('RowCol [' + IntToStr(OIdx) + ']:'
        + DumpGenAllowed(CanonicalAllowedPerms[Row].RowColAllowed[OIdx]));
  end;
end;
{$ENDIF}


function MaskUnique(var AllPerm: TGenAllowedPerms; const PermMask: TPermMask):boolean;
var
  Idx: integer;
  OIdx: TOrderIdx;
  Same: boolean;
begin
  for Idx := 0 to Pred(AllPerm.Count) do
  begin
    Same := true;
    for OIdx := Low(OIdx) to High(OIdx) do
    begin
      if AllPerm.PermMasks[Idx][Oidx] <> PermMask[OIdx] then
      begin
        Same := false;
        break;
      end;
    end;
    if Same then
    begin
      result := false;
      exit;
    end;
  end;
  result := true;
end;

procedure GenAddPerm(var AllPerm: TGenAllowedPerms; Perm: TPermIdx; const PermMask: TPermMask);
begin
  AllPerm.PermIdxs[AllPerm.Count] := Perm;
  AllPerm.PermMasks[AllPerm.Count] := PermMask;
  Inc(AllPerm.Count);
end;

procedure TCellCountMTBoard.FindCanonicalPerms(var CanonicalAllowedPerms: TAllowedPerms);

const
  MaskNoCells = -1;
  MaskAllCells = -2;

var
  Row: boolean;
  StackBand, Idx, PermedIdx: TOrderIdx;
  Perm: TPermIdx;
  FoundPerm: boolean;
  PermMask: TPermMask;
  LastCount, Count: integer;
begin
  FillChar(CanonicalAllowedPerms, sizeof(CanonicalAllowedPerms), 0);
  for Row := Low(Row) to High(Row) do
  begin
    //Stack/band permutations.
    for Perm := Low(TPermIdx) to High(TPermIdx) do
    begin
      LastCount := High(LastCount);
      FoundPerm := true;
      for Idx := Low(Idx) to High(Idx) do
      begin
        PermedIdx := PermList[Perm][Idx];
        Count := FCountInfo[Row].CellCounts[PermedIdx];
        if Count > LastCount then
        begin
          FoundPerm := false;
          break;
        end;
        LastCount := Count;
      end;
      if FoundPerm then
      begin
        //Generate a mask for this perm, and check not the same as
        //any previous.
        for Idx := Low(Idx) to High(Idx) do
        begin
          PermedIdx := PermList[Perm][Idx];
          Count := FCountInfo[Row].CellCounts[PermedIdx];
          if (Count = 0) then
            PermMask[Idx] := MaskNoCells
          else if (Count = ORDER * ORDER * ORDER) then
            PermMask[Idx] := MaskAllCells
          else
            PermMask[Idx] := PermedIdx;
        end;
        FoundPerm := MaskUnique(CanonicalAllowedPerms[Row].StackBandAllowed, PermMask);
      end;
      if FoundPerm then
        GenAddPerm(CanonicalAllowedPerms[Row].StackBandAllowed, Perm, PermMask);
    end;
    Assert(CanonicalAllowedPerms[Row].StackBandAllowed.Count > 0);
    //Now row/col permutations for each stack/band.
    for StackBand := Low(StackBand) to High(StackBand) do
    begin
      for Perm := Low(TPermIdx) to High(TPermIdx) do
      begin
        LastCount := High(LastCount);
        FoundPerm := true;
        for Idx := Low(Idx) to High(Idx) do
        begin
          PermedIdx := PermList[Perm][Idx];
          Count := FCountInfo[Row].UnpermutedLineInfo[StackBand][PermedIdx];
          if Count > LastCount then
          begin
            FoundPerm := false;
            break;
          end;
          LastCount := Count;
        end;
        if FoundPerm then
        begin
          //Generate a mask for this perm, and check not the same as
          //any previous.
          for Idx := Low(Idx) to High(Idx) do
          begin
            PermedIdx := PermList[Perm][Idx];
            Count := FCountInfo[Row].UnpermutedLineInfo[StackBand][PermedIdx];
            if (Count = 0) then
              PermMask[Idx] := MaskNoCells
            else if (Count = ORDER * ORDER) then
              PermMask[Idx] := MaskAllCells
            else
              PermMask[Idx] := PermedIdx;
          end;
          FoundPerm := MaskUnique(CanonicalAllowedPerms[Row].RowColAllowed[StackBand], PermMask);
        end;
        if FoundPerm then
          GenAddPerm(CanonicalAllowedPerms[Row].RowColAllowed[StackBand], Perm, PermMask);
      end;
      Assert(CanonicalAllowedPerms[Row].RowColAllowed[StackBand].Count > 0);
    end;
  end;
{$IFDEF DEBUG_CELLCOUNTS}
  DumpAllowedPerms(CanonicalAllowedPerms);
{$ENDIF}
end;

procedure IncPermIdxC(var Carry: boolean; const Perms: TGenAllowedPerms; var PermIdx: TPermIdx);
begin
  Assert(Perms.Count > 0);
  Carry := PermIdx >= Pred(Perms.Count);
  if not Carry then
    Inc(PermIdx)
  else
    PermIdx := 0;
end;

procedure InitPermIdxs(const AllowedPerms: TAllowedPerms;
                     var SelectedPerms: TSelectedPerms);
var
  Rows: boolean;
  Idx: TOrderIdx;
begin
  for Rows := Low(Rows) to High(Rows) do
  begin
    SelectedPerms.StackBandPerms[Rows].StackBandPerm := 0;
    Assert(AllowedPerms[Rows].StackBandAllowed.Count > 0);
    for Idx := Low(Idx) to High(Idx) do
    begin
      SelectedPerms.StackBandPerms[Rows].RowColPerms[Idx] := 0;
      Assert(AllowedPerms[Rows].RowColAllowed[Idx].Count > 0);
    end;
  end;
  SelectedPerms.Reflect := false;
end;

function IncPermIdxs(const AllowedPerms: TAllowedPerms;
                     var SelectedPerms: TSelectedPerms): boolean;

const
  OrderLowRow = Low(TOrderIdx);
  OrderHighRow = High(TOrderIdx);
  OrderBand = Succ(OrderHighRow);
  OrderLowCol = Succ(OrderBand);
  OrderHighCol = Succ(OrderBand) + (High(TOrderIdx) - Low(TOrderIdx));
  OrderStack = Succ(OrderHighCol);
  OrderReflect = Succ(OrderStack);

type
  TOrder = OrderLowRow .. OrderReflect;

var
  //Where we are incrementing.
  Carry: boolean;
  Order: TOrder;
begin
  Carry := false;
  Order := Low(Order);
  repeat
    case Order of
     OrderLowRow .. OrderHighRow:
     begin
       IncPermIdxC(Carry, AllowedPerms[true].RowColAllowed[Order],
                   SelectedPerms.StackBandPerms[true].RowColPerms[Order]);
     end;
     OrderBand:
     begin
       IncPermIdxC(Carry, AllowedPerms[true].StackBandAllowed,
                    SelectedPerms.StackBandPerms[true].StackBandPerm);
     end;
     OrderLowCol .. OrderHighCol:
     begin
       IncPermIdxC(Carry, AllowedPerms[false].RowColAllowed[Order - OrderLowCol],
                   SelectedPerms.StackBandPerms[false].RowColPerms[Order - OrderLowCol]);
     end;
     OrderStack:
     begin
       IncPermIdxC(Carry, AllowedPerms[false].StackBandAllowed,
                    SelectedPerms.StackBandPerms[false].StackBandPerm);
     end;
     OrderReflect:
     begin
       Carry := SelectedPerms.Reflect;
       SelectedPerms.Reflect := not SelectedPerms.Reflect;
     end
    else
      Assert(false);
    end;
    //Done the increment.
    if Carry then
    begin
      if Order = High(Order) then
      begin
        result := true;
        exit;
      end
      else
        Inc(Order);
    end;
  until not Carry;
  result := false;
end;

function IsLowerOrder(const Cand:TSymBoardPackedState; const Existing: TSymBoardPackedState): boolean;
var
  i: TSymBoardPackedIdx;
begin
  result := false;
  for i := High(i) downto Low(i) do
  begin
    if Cand[i] <> Existing[i] then
    begin
      result := Cand[i] < Existing[i];
      exit;
    end;
  end;
end;

function IsLowerOrderReplaceZero(const Cand:TSymBoardPackedState; const Existing: TSymBoardPackedState): boolean;
begin
  result := IsZeroRep(Existing);
  if not result then
    result := IsLowerOrder(Cand, Existing);
end;

function IsLowerPacked(const Cand:TSymBoardPackedState; const Existing: TSymBoardPackedState): boolean;
var
  i: TSymBoardPackedIdx;
  b: integer;
  x: Uint64;
begin
  result := false;
  for i := Low(i) to High(i) do
  begin
    for b := 0 to Pred(BitsPerU64) do
    begin
      x := (Cand[i] and (Uint64(1) shl b));
      if x <> (Existing[i] and (Uint64(1) shl b)) then
      begin
        result := x <> 0;
        exit;
      end;
    end;
  end;
end;

{$IFDEF DEBUG_CELLCOUNTS}
procedure TCellCountMTBoard.DumpSelectedPerms(const CanonicalAllowedPerms: TAllowedPerms;
                            const CurrentSelectedPerms: TSelectedPerms);

  procedure DumpGenSelected(SelIdx: integer; const AllowedPerms: TGenAllowedPerms);
  begin
    Write(' P', IntToStr(SelIdx), '=', AllowedPerms.PermIdxs[SelIdx]);
  end;

var
  Row: boolean;
  RowCol: TOrderIdx;
begin
  WriteLn;
  WriteLn('--- Selected perms ---');
  for Row := Low(Row) to High(Row) do
  begin
    if Row then
      Write('Row perms: ')
    else
      Write('Col perms: ');
    DumpGenSelected(CurrentSelectedPerms.StackBandPerms[Row].StackBandPerm,
                    CanonicalAllowedPerms[Row].StackBandAllowed);
    WriteLn;
    for RowCol := Low(RowCol) to High(RowCol) do
    begin
      DumpGenSelected(CurrentSelectedPerms.StackBandPerms[Row].RowColPerms[RowCol],
        CanonicalAllowedPerms[Row].RowColAllowed[RowCol]);
      Write(' ');
    end;
    WriteLn;
  end;
  if CurrentSelectedPerms.Reflect then
    WriteLn('Reflect.')
  else
    WriteLn('No reflect.');
end;
{$ENDIF}

const
  Sqs = ORDER * ORDER * ORDER * ORDER;

procedure PermuteRowData(const CurrentSelectedPerms: TSelectedPerms; const CanonicalAllowedPerms: TAllowedPerms; const AbsRow: TRowColIdx; var AbsPRow: TRowColIdx);
var
  SelectedPermIdx: TPermIdx;
  Band, Row: TOrderIdx;
  PBand, PRow: TOrderIdx;
begin
  AbsToRelIndexed(AbsRow, Band, Row);

  SelectedPermIdx := CurrentSelectedPerms.StackBandPerms[true].StackBandPerm;
  Assert(SelectedPermIdx < CanonicalAllowedPerms[true].StackBandAllowed.Count);
  PBand := PermList[CanonicalAllowedPerms[true].StackBandAllowed.PermIdxs[SelectedPermIdx]][Band];

  //Note, feed permuted band in here.
  SelectedPermIdx := CurrentSelectedPerms.StackBandPerms[true].RowColPerms[PBand];
  Assert(SelectedPermIdx < CanonicalAllowedPerms[true].RowColAllowed[PBand].Count);
  PRow := PermList[CanonicalAllowedPerms[true].RowColAllowed[PBand].PermIdxs[SelectedPermIdx]][Row];

  RelToAbsIndexed(PBand, PRow, AbsPRow);
end;

procedure PermuteColData(const CurrentSelectedPerms: TSelectedPerms; const CanonicalAllowedPerms: TAllowedPerms; const AbsCol:TRowColIdx; var AbsPCol: TRowColIdx);
var
  Stack, Col: TOrderIdx;
  SelectedPermIdx: TPermIdx;
  PStack, PCol: TOrderIdx;
begin
  AbsToRelIndexed(AbsCol, Stack, Col);

  SelectedPermIdx := CurrentSelectedPerms.StackBandPerms[false].StackBandPerm;
  Assert(SelectedPermIdx < CanonicalAllowedPerms[false].StackBandAllowed.Count);
  PStack := PermList[CanonicalAllowedPerms[false].StackBandAllowed.PermIdxs[SelectedPermIdx]][Stack];

  //Note, feed permuted stack in here.
  SelectedPermIdx := CurrentSelectedPerms.StackBandPerms[false].RowColPerms[PStack];
  Assert(SelectedPermIdx < CanonicalAllowedPerms[false].RowColAllowed[PStack].Count);
  PCol := PermList[CanonicalAllowedPerms[false].RowColAllowed[PStack].PermIdxs[SelectedPermIdx]][Col];

  RelToAbsIndexed(PStack, PCol, AbsPCol);
end;

function TCellCountMTBoard.GetBoardEntryFast(Row, Col: TRowColIdx): boolean;
begin
  result := GetPackedEntry(FSymBoardPackedState, Row, Col);
end;

procedure TCellCountMTBoard.MakeCanonicalString(const CanonicalAllowedPerms: TAllowedPerms);

var
  CurrentSelectedPerms: TSelectedPerms;

var
  AbsRow, AbsCol: TRowColIdx;
  AbsPRow, AbsPCol: TRowColIdx;
  B: boolean;
  TmpPackedRep: TSymBoardPackedState;
  ColPermute: array[TRowColIdx] of TRowColIdx;
{$IFDEF DEBUG_CELLCOUNTS}
  DbgState: TSymBoardState;

  procedure DumpPermutedBoard;
  var
    i, j: TRowColIdx;
  begin
    WriteLn('-Permuted');
    for i := Low(i) to High(i) do
    begin
      for j := Low(j) to High(j) do
      begin
        if DbgState[i,j] <> 0 then
          Write('X')
        else
          Write('0');
      end;
      WriteLn;
    end;
  end;

{$ENDIF}
begin
  FillChar(TmpPackedRep, sizeof(TmpPackedRep), 0);
  FillChar(FCanonicalRep, sizeof(FCanonicalRep), 0);
  //There is always at least one permutation, even if all counts are the same.
  InitPermIdxs(CanonicalAllowedPerms, CurrentSelectedPerms);
  repeat
{$IFDEF DEBUG_CELLCOUNTS}
      DumpSelectedPerms(CanonicalAllowedPerms, CurrentSelectedPerms);
      FillChar(DbgState, sizeof(DbgState), 0);
{$ENDIF}
    for AbsCol := Low(AbsCol) to High(AbsCol) do
    begin
        PermuteColData(CurrentSelectedPerms, CanonicalAllowedPerms, AbsCol, AbsPCol);
        ColPermute[AbsCol] := AbsPCol;
    end;

    for AbsRow := Low(AbsRow) to High(AbsRow) do
    begin
      PermuteRowData(CurrentSelectedPerms, CanonicalAllowedPerms, AbsRow, AbsPRow);
      for AbsCol := Low(AbsCol) to High(AbsCol) do
      begin
        AbsPCol := ColPermute[AbsCol];
{$IFDEF DEBUG_CELLCOUNTS}
        if CurrentSelectedPerms.Reflect then
          DbgState[AbsCol, AbsRow] := Entries[AbsPRow, AbsPCol]
        else
          DbgState[AbsRow, AbsCol] := Entries[AbsPRow, AbsPCol];
{$ENDIF}
        B := FastEntries[AbsPRow, AbsPCol];
        if CurrentSelectedPerms.Reflect then
          SetPackedEntry(TmpPackedRep, AbsCol, AbsRow, B)
        else
          SetPackedEntry(TmpPackedRep, AbsRow, AbsCol, B);
      end;
    end;
{$IFDEF DEBUG_CELLCOUNTS}
    DumpPermutedBoard;
{$ENDIF}
    if IsLowerOrderReplaceZero(TmpPackedRep, FCanonicalRep) then
      FCanonicalRep := TmpPackedRep;
  until IncPermIdxs(CanonicalAllowedPerms, CurrentSelectedPerms);
end;


procedure TCellCountMTBoard.OptCalcSig;
begin
  if (FFlags and CountsSigValidFlag) = 0 then
  begin
    SetupCounts;
    FFlags := FFlags or CountsSigValidFlag;
  end;
end;

procedure TCellCountMTBoard.OptCalcCanonical;
var
  CanonicalAllowedPerms: TAllowedPerms;
begin
  if (FFlags and CanonicalValidFlag) = 0 then
  begin
    OptCalcSig;
    FindCanonicalPerms(CanonicalAllowedPerms);
    MakeCanonicalString(CanonicalAllowedPerms);
    FFlags := FFlags or CanonicalValidFlag;
  end;
end;

procedure TCellCountMTBoard.Invalidate;
begin
  FFlags := 0;
end;


function TCellCountMTBoard.GetCanonicalRep: TSymBoardPackedState;
begin
  OptCalcCanonical;
  result := FCanonicalRep;
end;

function TCellCountMTBoard.GetMinLexRep: TSymBoardPackedState;
begin
  result := self.FSymBoardPackedState;
end;

procedure SWr(S:string);
begin
  WriteLn(S);
end;

function TCellCountMTBoard.AmIsomorphicTo(Other: TSymBoardAbstract): boolean;
var
  O: TCellCountMTBoard;
{$IFDEF DEBUG_CELLCOUNTS}
  S1, S2: string;
{$ENDIF}
begin
  O := Other as TCellCountMTBoard;
{$IFDEF DEBUG_CELLCOUNTS}
  LogBoard(SWr);
  O.LogBoard(SWr);
  WriteLn('--------------');
  WriteLn('S1: -TODO - Packed rep to string.');
//  S1 := CanonicalString;
  WriteLn('--------------');
  WriteLn('S2: -TODO - Packed rep to string.');
//  S2 := O.CanonicalString;
  WriteLn('');
  WriteLn(S1);
  WriteLn(S2);
{$ENDIF}
  result := IsEqualRep(CanonicalRep, O.CanonicalRep);
{$IFDEF DEBUG_CELLCOUNTS}
  WriteLn(result);
  WriteLn('--------------');
{$ENDIF}
end;

{ TMTIsoList }

procedure TMTIsoList.ListStateCanonicalMode;
begin
  if FStore.HasIndex(Ord(lsitMinlex)) then
    FStore.DeleteIndex(Ord(lsitMinlex));
  if not FStore.HasIndex(Ord(lsitCanonical)) then
    FStore.AddIndex(TPackedRepIndexNode, Ord(lsitCanonical));
end;

procedure TMTIsoList.ListStateMinlexMode;
begin
  if FStore.HasIndex(Ord(lsitCanonical)) then
    FStore.DeleteIndex(Ord(lsitCanonical));
  if not FStore.HasIndex(Ord(lsitMinlex)) then
    FStore.AddIndex(TPackedRepIndexNode, Ord(lsitMinlex));
end;

constructor TMTIsoList.Create;
begin
  inherited;
  FStore := TIndexedStore.Create;
  ListStateCanonicalMode;
  FLock := TMultiReadExclusiveWriteSynchronizer.Create;
  FTraverseState := ttsAdding;
end;

destructor TMTIsoList.Destroy;
var
  Rec, Rec2: TItemRec;
  Obj: TObject;
begin
  if FStore.HasIndex(Ord(lsitCanonical)) then
    FStore.DeleteIndex(Ord(lsitCanonical));
  if FStore.HasIndex(Ord(lsitMinlex)) then
    FStore.DeleteIndex(Ord(lsitMinlex));
  Rec := FStore.GetAnItem;
  while Assigned(Rec) do
  begin
    Obj := Rec.Item;
    Rec2 := Rec;
    FStore.GetAnotherItem(Rec);
    FStore.RemoveItem(Rec2);
    Obj.Free;
  end;
  FStore.Free;
  FLock.Free;
  inherited;
end;

procedure TMTIsoList.Clear;
var
  Rec, Rec2: TItemRec;
  Obj: TObject;
begin
  if FStore.HasIndex(Ord(lsitCanonical)) then
    FStore.DeleteIndex(Ord(lsitCanonical));
  if FStore.HasIndex(Ord(lsitMinlex)) then
    FStore.DeleteIndex(Ord(lsitMinlex));

  Rec := FStore.GetAnItem;
  while Assigned(Rec) do
  begin
    Obj := Rec.Item;
    Rec2 := Rec;
    FStore.GetAnotherItem(Rec);
    FStore.RemoveItem(Rec2);
    Obj.Free;
  end;
  ListStateCanonicalMode;
  FTraverseState := ttsAdding;
  inherited;
end;

procedure TMTIsoList.SetTraversed;
begin
  Assert(FTraverseState = ttsAdding);
  ListStateMinlexMode;
  FTraverseState := ttsTraversed;
end;

procedure TMTIsoList.ResetTraverse;
begin
  Assert(FTraverseState = ttsTraversed);
  FTraverseState := ttsTraversing;
end;

function TMTIsoList.AddBoardInternal(Board: TCellCountMTBoard; Readonly: boolean; SV: TPackedRepSearchVal): TSSAddBoardRet;
var
  Rec: TItemRec;
  RV: TISRetVal;
  Existing: TCellCountMTBoard;
begin
  Existing := nil;
  SV.SearchVal := Board.CanonicalRep;
  RV := FStore.FindByIndex(Ord(lsitCanonical), SV, Rec);
  Assert(RV in [rvOK, rvNotFound]);
  if RV = rvOK then
  begin
    Assert(Rec.Item is TCellCountMTBoard);
    Existing := TCellCountMTBoard(Rec.Item);
    if IsLowerPacked(Board.MinlexRep, Existing.MinlexRep) then
    begin
      result := abrCanAdd;
      Assert(not IsEqualRep(Board.MinlexRep, Existing.MinlexRep));
    end
    else
      result := abrNotMinimal;
  end
  else
    result := abrCanAdd;
  if (result = abrCanAdd) and not ReadOnly then
  begin
    if Assigned(Existing) then
    begin
      //Rec is still valid, remove and delete existing item.
      RV := FStore.RemoveItem(Rec);
      Assert(RV = rvOK);
      Existing.Free;
    end;
    RV := FStore.AddItem(Board, Rec);
    Assert(RV = rvOK);
    result := abrAdded;
  end;
end;

function TMTIsoList.AddBoard(Board: TCellCountMTBoard; SV: TPackedRepSearchVal): boolean;
var
  AddRet: TSSAddBoardRet;
begin
  if FTraverseState <> ttsAdding then
  begin
    Assert(false);
    result := false;
    exit;
  end;
  //First R/O scan thru to check not already in store.

  //Precalc values for this board before trying to insert
  //so as not to waste time under lock.
  Board.GetCanonicalRep;
  FLock.BeginRead;
  try
    AddRet := AddBoardInternal(Board, true, SV);
    case AddRet of
      abrNotMinimal:
      begin
        result := false;
        exit;
      end;
      abrCanAdd: ; //Continue.
      abrAdded:
      begin
        //Should not have this ret yet.
        Assert(false);
        result := false;
        exit;
      end;
    else
      Assert(false);
      result := false;
      exit;
    end;
  finally
    FLock.EndRead;
  end;
  //Second time, do the insertion for real.
  FLock.BeginWrite;
  try
    AddRet := AddBoardInternal(Board, false, SV);
    case AddRet of
      abrNotMinimal: result := false; //Race cond?
      abrAdded: result := true;
      abrCanAdd:
      begin
        Assert(false);
        result := false;
      end;
    else
      Assert(false);
      result := false;
    end;
  finally
    FLock.EndWrite;
  end;
end;

function TMTIsoList.GetCount: integer;
begin
  FLock.BeginRead;
  try
    result := FStore.Count;
  finally
    FLock.EndRead;
  end;
end;

function TMTIsoList.GetInMinlexOrder: TCellCountMTBoard;

var
  SRV: TIsRetVal;

begin
  FLock.BeginWrite;
  try
    Assert(FStore.HasIndex(Ord(lsitMinlex))
      = ((FTraverseState = ttsTraversing) or (FTraverseState = ttsTraversed)));

    if not FStore.HasIndex(Ord(lsitMinlex)) then
    begin
      ListStateMinlexMode;
      FTraverseState := ttsTraversing;
    end;

    case FTraverseState of
      ttsAdding:
      begin
        Assert(false);
        result := nil;
        exit;
      end;
      ttsTraversing:
      begin
        if not Assigned(FTravIrec) then
          SRV := FStore.FirstByIndex(Ord(lsitMinlex), FTravIRec)
        else
          SRV := FStore.NextByIndex(Ord(lsitMinlex), FTravIRec);
        Assert((SRV = rvOK) or (SRV = rvNotFound));
        if SRV = rvOK then
        begin
          Assert(FTRavIRec.Item is TCellCountMTBoard);
          result := TCellCountMTBoard(FTRavIRec.Item);
        end
        else
        begin
          Assert(not Assigned(FTravIRec));
          result := nil;
          FTraverseState := ttsTraversed;
        end;
      end;
      ttsTraversed: result := nil;
    else
      Assert(false);
      result := nil;
    end;
  finally
    FLock.EndWrite;
  end;
end;

type
  PUint32 = ^Uint32;

function TPackedRepIndexNode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  OwnBoard, OtherBoard: TCellCountMTBoard;
begin
  Assert(OwnItem is TCellCountMTBoard);
  Assert(OtherItem is TCellCountMTBoard);
  OwnBoard := TCellCountMTBoard(OwnItem);
  OtherBoard := TCellCountMTBoard(OtherItem);
  case IndexTag of
    Ord(lsitCanonical) :
    begin
      if IsLowerOrder(OwnBoard.CanonicalRep, OtherBoard.CanonicalRep) then
        result := 1
      else if IsEqualRep(OwnBoard.CanonicalRep, OtherBoard.CanonicalRep) then
        result := 0
      else
        result := -1;
    end;
    Ord(lsitMinlex):
    begin
      if IsLowerPacked(OwnBoard.MinlexRep, OtherBoard.MinlexRep) then
        result := 1
      else if IsEqualRep(OwnBoard.MinlexRep, OtherBoard.MinlexRep) then
        result := 0
      else
        result := -1;
    end
  else
    Assert(false);
    result := 0;
  end;
end;

function TPackedRepSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  OtherBoard: TCellCountMTBoard;
begin
  Assert(OtherItem is TCellCountMTBoard);
  OtherBoard := TCellCountMTBoard(OtherItem);
  case IndexTag of
    Ord(lsitCanonical) :
    begin
      if IsLowerOrder(SearchVal, OtherBoard.CanonicalRep) then
        result := 1
      else if IsEqualRep(SearchVal, OtherBoard.CanonicalRep) then
        result := 0
      else
        result := -1;
    end;
    Ord(lsitMinlex):
    begin
      if IsLowerPacked(SearchVal, OtherBoard.MinlexRep) then
        result := 1
      else if IsEqualRep(SearchVal, OtherBoard.MinlexRep) then
        result := 0
      else
        result := -1;
    end
  else
    Assert(false);
    result := 0;
  end;
end;

end.
