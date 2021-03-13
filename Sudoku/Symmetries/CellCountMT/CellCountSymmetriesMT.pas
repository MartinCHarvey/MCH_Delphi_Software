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


type
  TCellCountMTBoard = class(TSymBoard)
  protected
    FBoardLock: TCriticalSection;
    FCountsSigValid: boolean;
    FCountInfo: TCountInfo;
    //Unpermuted line info (CellCounts) is primary information,
    //All other counts calculated from there.
    FSignature: TStackBandCellCountSet;

    FCanonicalValid: boolean;
    FCanonicalAllowedPerms: TAllowedPerms;
    FCanonicalSelectedPerms, FCurrentSelectedPerms: TSelectedPerms;
    FCanonicalString: string;
    FMinlexValid: boolean;
    FMinlex: string;

    function FastGetBoardEntry(Row, Col: TRowColIdx): integer; inline;
    procedure SetBoardEntry(Row, Col: TRowColIdx; Entry: integer); override;
    procedure SetupCounts;
{$IFDEF DEBUG_CELLCOUNTS}
    procedure DumpCounts;
    procedure DumpAllowedPerms;
    procedure DumpSelectedPerms;
{$ENDIF}
    procedure FindCanonicalPerms;
    procedure FindCanonicalPermsSimpleGuess;
    procedure MakeCanonicalString;
    procedure CalcMinLex;
    procedure Invalidate;
    function GetCanonicalString: string;
    function GetSignature: TStackBandCellCountSet;
    function GetMinLex: string;
    procedure OptCalcSig;
    procedure OptCalcCanonical;

    property FastEntries[Row, Col: TRowColIdx]: integer read FastGetBoardEntry;
  public
    constructor Create; override;
    destructor Destroy; override;

    //No override assign needed, CellCount setter does the work
    function AmIsomorphicTo(Other: TSymBoard): boolean; override;
    property CanonicalString: string read GetCanonicalString;
    property Signature: TStackBandCellCountSet read GetSignature;
    property Minlex: string read GetMinLex;
  end;

  TCellCountBoardClass = class of TCellCountMTBoard;

  //TODO - Work on these states a bit.
  TTraverseState = (ttsAdding, ttsTraversing, ttsTraversed);
  TSSAddBoardRet = (abrNotMinimal, abrCanAdd, abrAdded);
  TLexStrIndexNodeType = (lsitCanonical, lsitMinlex);

  TLexStrSearchVal = class;

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
    function AddBoardInternal(Board: TCellCountMTBoard; Readonly: boolean; SV: TLexStrSearchVal): TSSAddBoardRet;
  public
    constructor Create;
    destructor Destroy; override;
    //Pass in a LexStrSearchVal that is thread local.
    //Saves mem alloc costs.
    function AddBoard(Board: TCellCountMTBoard; SV: TLexStrSearchVal): boolean;
    function GetInMinlexOrder: TCellCountMTBoard;
    procedure Clear;
    procedure ResetTraverse;
    property Count: integer read GetCount;
  end;


  TLexStrIndexNode = class(TIndexNode)
  public
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;override;
  end;

  TLexStrSearchVal = class(TLexStrIndexNode)
  public
    SearchVal: string;
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;override;
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
  FBoardLock := TCriticalSection.Create;
end;

destructor TCellCountMTBoard.Destroy;
begin
  FBoardLock.Free;
  inherited;
end;

procedure TCellCountMTBoard.SetBoardEntry(Row, Col: TRowColIdx; Entry: integer);
var
  Band, RelRow, Stack, RelCol: TOrderIdx;
begin
  FBoardLock.Acquire;
  try
    AbsToRelIndexed(Row, Band, RelRow);
    AbsToRelIndexed(Col, Stack, RelCol);
    if FastEntries[Row, Col] <> 0 then
    begin
      if Entry = 0 then
      begin
        Invalidate;
        with FCountInfo[true].UnpermutedLineInfo[Band] do
        begin
          Assert(CellCounts[RelRow] > 0);
          Dec(CellCounts[RelRow]);
        end;
        with FCountInfo[false].UnpermutedLineInfo[Stack] do
        begin
          Assert(CellCounts[RelCol] > 0);
          Dec(CellCounts[RelCol]);
        end;
      end;
    end
    else
    begin
      if Entry <> 0 then
      begin
        Invalidate;
        with FCountInfo[true].UnpermutedLineInfo[Band] do
          Inc(CellCounts[RelRow]);
        with FCountInfo[false].UnpermutedLineInfo[Stack] do
          Inc(CellCounts[RelCol]);
      end;
    end;
    inherited;
  finally
    FBoardLock.Release;
  end;
end;

function TCellCountMTBoard.FastGetBoardEntry(Row, Col: TRowColIdx): integer;
begin
  result := FSymBoardState[Row, Col];
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
    WriteLn('StackBand set: ' + SBSetToStr(FCountInfo[Row].CellCountSet));
    for OIdx := Low(OIdx) to High(OIdx) do
      WriteLn('SB CellCount [' , OIdx, ']: ' , FCountInfo[Row].CellCounts[OIdx]);

    for OIdx := Low(OIdx) to High(OIdx) do
    begin
      WriteLn('RowCol set: ' + RCSetToStr(FCountInfo[Row].UnpermutedLineInfo[OIdx].CellCountSet));
      for LIdx := Low(LIdx) to High(LIdx) do
        WriteLn('RC CellCount [' , LIdx , ']: ', FCountInfo[Row].UnpermutedLineInfo[OIdx].CellCounts[LIdx]);
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
{$IFDEF OPTIMISE_SMALL_SETS}
      CellCountSet := SmallSetEmpty;
{$ELSE}
      CellCountSet := [];
{$ENDIF}
      for StackBand := Low(StackBand) to High(StackBand) do
      begin
        Tmp2 := 0;
        with UnpermutedLineInfo[StackBand] do
        begin
{$IFDEF OPTIMISE_SMALL_SETS}
          CellCountSet := SmallSetEmpty;
{$ELSE}
          CellCountSet := [];
{$ENDIF}
          for RowCol := Low(RowCol) to High(RowCol) do
          begin
            Tmp := CellCounts[RowCol];
{$IFDEF OPTIMISE_SMALL_SETS}
            CellCountSet := SmallSetUnion(CellCountSet, SmallSetUnitary(Tmp));
{$ELSE}
            CellCountSet := CellCountSet + [Tmp];
{$ENDIF}
            Inc(Tmp2, Tmp);
          end;
        end;
        CellCounts[StackBand] := Tmp2;
{$IFDEF OPTIMISE_SMALL_SETS}
        CellCountSet := SmallSetUnion(CellCountSet, SmallSetUnitary(Tmp2));
{$ELSE}
        CellCountSet := CellCountSet + [Tmp2];
{$ENDIF}
      end;
    end;
  end;
  FSignature := Self.FCountInfo[false].CellCountSet
    + Self.FCountInfo[true].CellCountSet;
{$IFDEF DEBUG_CELLCOUNTS}
  DumpCounts;
{$ENDIF}
end;

{$IFDEF DEBUG_CELLCOUNTS}
procedure TCellCountMTBoard.DumpAllowedPerms;

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
    WriteLn('StackBand: ' + DumpGenAllowed(FCanonicalAllowedPerms[Row].StackBandAllowed));
    for OIdx := Low(OIdx) to High(OIdx) do
      WriteLn('RowCol [' + IntToStr(OIdx) + ']:'
        + DumpGenAllowed(FCanonicalAllowedPerms[Row].RowColAllowed[OIdx]));
  end;
end;
{$ENDIF}


procedure TCellCountMTBoard.FindCanonicalPerms;
begin
  FindCanonicalPermsSimpleGuess;
end;

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

procedure TCellCountMTBoard.FindCanonicalPermsSimpleGuess;

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
  FillChar(FCanonicalAllowedPerms, sizeof(FCanonicalAllowedPerms), 0);
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
        FoundPerm := MaskUnique(FCanonicalAllowedPerms[Row].StackBandAllowed, PermMask);
      end;
      if FoundPerm then
        GenAddPerm(FCanonicalAllowedPerms[Row].StackBandAllowed, Perm, PermMask);
    end;
    Assert(FCanonicalAllowedPerms[Row].StackBandAllowed.Count > 0);
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
          Count := FCountInfo[Row].UnpermutedLineInfo[StackBand].CellCounts[PermedIdx];
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
            Count := FCountInfo[Row].UnpermutedLineInfo[StackBand].CellCounts[PermedIdx];
            if (Count = 0) then
              PermMask[Idx] := MaskNoCells
            else if (Count = ORDER * ORDER) then
              PermMask[Idx] := MaskAllCells
            else
              PermMask[Idx] := PermedIdx;
          end;
          FoundPerm := MaskUnique(FCanonicalAllowedPerms[Row].RowColAllowed[StackBand], PermMask);
        end;
        if FoundPerm then
          GenAddPerm(FCanonicalAllowedPerms[Row].RowColAllowed[StackBand], Perm, PermMask);
      end;
      Assert(FCanonicalAllowedPerms[Row].RowColAllowed[StackBand].Count > 0);
    end;
  end;
{$IFDEF DEBUG_CELLCOUNTS}
  DumpAllowedPerms;
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

function IsLowerOrder(Cand, Existing: string): boolean;
var
  i: integer;
begin
  result := Length(Existing) = 0;
  if not result then
  begin
    Assert(Length(Cand) = Length(Existing));
    for i := Length(Cand) downto 1 do
    begin
      //First one with an 'X' where the other does not,
      //is the one with a higher order.
      if Cand[i] <> Existing[i] then
      begin
        result := (Existing[i] = 'X');
        exit;
      end;
    end;
  end;
end;

function IsLowerPacked(Cand, Existing: string): boolean;
var
  i: integer;
begin
  Assert(Length(Cand) = Length(Existing));
  Assert(Length(Existing) > 0);
  result := false;
  for i := 1 to Length(Cand) do
  begin
    //First one with an 'X' where the other does not,
    //is the one with a higher order.
    if Cand[i] <> Existing[i] then
    begin
      result := (Cand[i] = 'X');
      exit;
    end;
  end;
end;

{$IFDEF DEBUG_CELLCOUNTS}
procedure TCellCountMTBoard.DumpSelectedPerms;

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
    DumpGenSelected(FCurrentSelectedPerms.StackBandPerms[Row].StackBandPerm,
                    FCanonicalAllowedPerms[Row].StackBandAllowed);
    WriteLn;
    for RowCol := Low(RowCol) to High(RowCol) do
    begin
      DumpGenSelected(FCurrentSelectedPerms.StackBandPerms[Row].RowColPerms[RowCol],
        FCanonicalAllowedPerms[Row].RowColAllowed[RowCol]);
      Write(' ');
    end;
    WriteLn;
  end;
  if FCurrentSelectedPerms.Reflect then
    WriteLn('Reflect.')
  else
    WriteLn('No reflect.');
end;
{$ENDIF}

const
  Sqs = ORDER * ORDER * ORDER * ORDER;

procedure TCellCountMTBoard.MakeCanonicalString;

var
  Stack, Band, Row, Col: TOrderIdx;
  PStack, PBand, PRow, PCol: TOrderIdx;
  AbsRow, AbsCol: TRowColIdx;
  AbsPRow, AbsPCol: TRowColIdx;
  SelectedPermIdx: TPermIdx;
  Ch: WideChar;
  B: boolean;
  S: string;
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
  SetLength(S, Sqs);
  SetLength(FCanonicalString, 0);
  //There is always at least one permutation, even if all counts are the same.
  InitPermIdxs(FCanonicalAllowedPerms, FCurrentSelectedPerms);
  repeat
{$IFDEF DEBUG_CELLCOUNTS}
      DumpSelectedPerms;
      FillChar(DbgState, sizeof(DbgState), 0);
{$ENDIF}
    for AbsRow := Low(AbsRow) to High(AbsRow) do
    begin
      AbsToRelIndexed(AbsRow, Band, Row);

      SelectedPermIdx := FCurrentSelectedPerms.StackBandPerms[true].StackBandPerm;
      Assert(SelectedPermIdx < FCanonicalAllowedPerms[true].StackBandAllowed.Count);
      PBand := PermList[FCanonicalAllowedPerms[true].StackBandAllowed.PermIdxs[SelectedPermIdx]][Band];

      //Note, feed permuted band in here.
      SelectedPermIdx := FCurrentSelectedPerms.StackBandPerms[true].RowColPerms[PBand];
      Assert(SelectedPermIdx < FCanonicalAllowedPerms[true].RowColAllowed[PBand].Count);
      PRow := PermList[FCanonicalAllowedPerms[true].RowColAllowed[PBand].PermIdxs[SelectedPermIdx]][Row];

      RelToAbsIndexed(PBand, PRow, AbsPRow);
      for AbsCol := Low(AbsCol) to High(AbsCol) do
      begin
        AbsToRelIndexed(AbsCol, Stack, Col);

        SelectedPermIdx := FCurrentSelectedPerms.StackBandPerms[false].StackBandPerm;
        Assert(SelectedPermIdx < FCanonicalAllowedPerms[false].StackBandAllowed.Count);
        PStack := PermList[FCanonicalAllowedPerms[false].StackBandAllowed.PermIdxs[SelectedPermIdx]][Stack];

        //Note, feed permuted stack in here.
        SelectedPermIdx := FCurrentSelectedPerms.StackBandPerms[false].RowColPerms[PStack];
        Assert(SelectedPermIdx < FCanonicalAllowedPerms[false].RowColAllowed[PStack].Count);
        PCol := PermList[FCanonicalAllowedPerms[false].RowColAllowed[PStack].PermIdxs[SelectedPermIdx]][Col];

        RelToAbsIndexed(PStack, PCol, AbsPCol);
{$IFDEF DEBUG_CELLCOUNTS}
        if FCurrentSelectedPerms.Reflect then
          DbgState[AbsCol, AbsRow] := FastEntries[AbsPRow, AbsPCol]
        else
          DbgState[AbsRow, AbsCol] := FastEntries[AbsPRow, AbsPCol];
{$ENDIF}
        B := FastEntries[AbsPRow, AbsPCol] <> 0;
        if B then
          Ch := 'X'
        else
          Ch := '.';
        if FCurrentSelectedPerms.Reflect then
          S[Succ(AbsCol + (AbsRow * ORDER * ORDER))] :=  Ch
        else
          S[Succ(AbsRow + (AbsCol * ORDER * ORDER))] :=  Ch
      end;
    end;
{$IFDEF DEBUG_CELLCOUNTS}
    DumpPermutedBoard;
{$ENDIF}
    if IsLowerOrder(S, FCanonicalString) then
    begin
      FCanonicalString := S;
      FCanonicalSelectedPerms := FCurrentSelectedPerms;
    end;
  until IncPermIdxs(FCanonicalAllowedPerms, FCurrentSelectedPerms);
end;

procedure TCellCountMTBoard.CalcMinLex;
var
  B: boolean;
  Row, Col: TRowColIdx;
  Ch: WideChar;
begin
  SetLength(FMinlex, Sqs);
  for Row := Low(Row) to High(Row) do
    for Col := Low(Col) to High(Col) do
    begin
      B := FastEntries[Row, Col] <> 0;
      if B then
        Ch := 'X'
      else
        Ch := '.';
      FMinlex[Succ(Col + (Row * ORDER * ORDER))] :=  Ch
    end;
end;

procedure TCellCountMTBoard.OptCalcSig;
begin
  if not FCountsSigValid then
  begin
    SetupCounts;
    FCountsSigValid := true;
  end;
end;

procedure TCellCountMTBoard.OptCalcCanonical;
begin
  if not FCanonicalValid then
  begin
    OptCalcSig;
    FindCanonicalPerms;
    MakeCanonicalString;
    FCanonicalValid := true;
  end;
end;

procedure TCellCountMTBoard.Invalidate;
begin
  FCountsSigValid := false;
  FCanonicalValid := false;
  FMinlexValid := false;
end;

function TCellCountMTBoard.GetCanonicalString: string;
begin
  FBoardLock.Acquire;
  try
    OptCalcCanonical;
    result := FCanonicalString;
  finally
    FBoardLock.Release;
  end;
end;

function TCellCountMTBoard.GetMinLex: string;
begin
  FBoardLock.Acquire;
  try
    if not FMinLexValid then
    begin
      CalcMinLex;
      FMinLexValid := true;
    end;
    result := FMinlex;
  finally
    FBoardLock.Release;
  end;
end;

function TCellCountMTBoard.GetSignature: TStackBandCellCountSet;
begin
  FBoardLock.Acquire;
  try
    OptCalcSig;
    result := FSignature;
  finally
    FBoardLock.Release;
  end;
end;

procedure SWr(S:string);
begin
  WriteLn(S);
end;

function TCellCountMTBoard.AmIsomorphicTo(Other: TSymBoard): boolean;
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
  WriteLn('S1: ');
  S1 := CanonicalString;
  WriteLn('--------------');
  WriteLn('S2: ');
  S2 := O.CanonicalString;
  WriteLn('');
  WriteLn(S1);
  WriteLn(S2);
{$ENDIF}
  result := (Signature = O.Signature) and
    (CanonicalString = O.CanonicalString);
{$IFDEF DEBUG_CELLCOUNTS}
  WriteLn(result);
  WriteLn('--------------');
{$ENDIF}
end;

{ TMTIsoList }

constructor TMTIsoList.Create;
begin
  inherited;
  FStore := TIndexedStore.Create;
  FStore.AddIndex(TLexStrIndexNode, Ord(lsitCanonical));
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
  if FStore.HasIndex(Ord(lsitMinlex)) then
    FStore.DeleteIndex(Ord(lsitMinlex));
  FTraverseState := ttsAdding;
  Rec := FStore.GetAnItem;
  while Assigned(Rec) do
  begin
    Obj := Rec.Item;
    Rec2 := Rec;
    FStore.GetAnotherItem(Rec);
    FStore.RemoveItem(Rec2);
    Obj.Free;
  end;
  inherited;
end;

procedure TMTIsoList.ResetTraverse;
begin
  Assert(FTraverseState = ttsTraversed);
  FTraverseState := ttsTraversing;
end;

function TMTIsoList.AddBoardInternal(Board: TCellCountMTBoard; Readonly: boolean; SV: TLexStrSearchVal): TSSAddBoardRet;
var
  Rec: TItemRec;
  RV: TISRetVal;
  Existing: TCellCountMTBoard;
begin
  Existing := nil;
  SV.SearchVal := Board.CanonicalString;
  RV := FStore.FindByIndex(Ord(lsitCanonical), SV, Rec);
  Assert(RV in [rvOK, rvNotFound]);
  if RV = rvOK then
  begin
    Assert(Rec.Item is TCellCountMTBoard);
    Existing := TCellCountMTBoard(Rec.Item);
    if IsLowerPacked(Board.Minlex, Existing.Minlex) then
    begin
      result := abrCanAdd;
      Assert(Board.Minlex <> Existing.Minlex);
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

function TMTIsoList.AddBoard(Board: TCellCountMTBoard; SV: TLexStrSearchVal): boolean;
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
  Board.GetCanonicalString;
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

  function SetupCompleteStore: boolean;
  var
    RV: TIsRetVal;
  begin
    if not FStore.HasIndex(Ord(lsitMinlex)) then
    begin
      RV := FStore.AddIndex(TLexStrIndexNode, Ord(lsitMinlex));
      result := RV = rvOK;
      Assert(result);
    end
    else
      result := true;
    FTraverseState := ttsTraversing;
  end;

var
  SRV: TIsRetVal;

begin
  FLock.BeginWrite;
  try
    Assert(FStore.HasIndex(Ord(lsitMinlex))
      = ((FTraverseState = ttsTraversing) or (FTraverseState = ttsTraversed)));

    if not FStore.HasIndex(Ord(lsitMinlex)) then
    begin
      if not SetupCompleteStore then
      begin
        result := nil;
        exit;
      end;
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

function TLexStrIndexNode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
var
  OwnBoard, OtherBoard: TCellCountMTBoard;
begin
  Assert(OwnItem is TCellCountMTBoard);
  Assert(OtherItem is TCellCountMTBoard);
  OwnBoard := TCellCountMTBoard(OwnItem);
  OtherBoard := TCellCountMTBoard(OtherItem);
  case IndexTag of
    Ord(lsitCanonical) : result := CompareStr(OwnBoard.CanonicalString, OtherBoard.CanonicalString);
    Ord(lsitMinlex):
    begin
      if IsLowerPacked(OwnBoard.Minlex, OtherBoard.Minlex) then
        result := 1
      else if (OwnBoard.Minlex = OtherBoard.Minlex) then
        result := 0
      else
        result := -1;
    end
  else
    Assert(false);
    result := 0;
  end;
end;

function TLexStrSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
var
  OtherBoard: TCellCountMTBoard;
begin
  Assert(OtherItem is TCellCountMTBoard);
  OtherBoard := TCellCountMTBoard(OtherItem);
  case IndexTag of
    Ord(lsitCanonical) : result := CompareStr(SearchVal, OtherBoard.CanonicalString);
    Ord(lsitMinlex):
    begin
      if IsLowerPacked(SearchVal, OtherBoard.Minlex) then
        result := 1
      else if (SearchVal = OtherBoard.Minlex) then
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
