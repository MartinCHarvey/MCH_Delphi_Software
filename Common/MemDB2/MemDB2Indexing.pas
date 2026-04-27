unit MemDB2Indexing;
{

Copyright � 2020 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the �Software�), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

{
  In memory database. Indexing and constraint code.
}

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  MemDB2Misc, MemDB2Streamable, CowTree;

type
  TMemDbIndexLeafGeneric = class;

  { Quick reminder. Indexes are *immutable* once created,
    so provided you add-ref the root, you're good }
  TMemDBIndexGeneric = class(TCowTree)
  private
    FParentIndex: TMemDBIndexGeneric;
    FRoot: TMemDBIndexLeafGeneric;
    FNext: TMemDBIndexLeafGeneric;
  protected
    function SelToRoot(Sel: TAbSelType): TMemDBIndexLeafGeneric;
  public
    constructor Create; virtual;
    destructor Destroy; override;
    function Clone: TMemDBIndexGeneric; virtual;

    //We don't inc/dec the ref for leaves with user ops here,
    //but other cursor handling code might.
    function Locate(Sel: TAbSelType; Pos: TMemAPIPosition; Cur: TMemDBIndexLeafGeneric): TMemDBIndexLeafGeneric;
    function CheckNonAliasedPresent(Sel: TAbSelType; Node: TMemDBIndexLeafGeneric): boolean;

    //Add / remove guaranteed to add or remove in the tree, but
    //you may have to do a bit of hunting to find the right node from the row.

    //Cloned indexes, add and remove from current (cloned) only.
    //Non-cloned indexes, add / remove from next only.
    function Add(Sel: TAbSelType; New: TMemDBIndexLeafGeneric): boolean;
    function Remove(Sel: TAbSelType; Old: TMemDBIndexLeafGeneric): boolean;

    procedure RootToNext;
    procedure CommitNextToRoot;
    procedure DiscardNext;

    property ParentIndex: TMemDbIndexGeneric read FParentIndex;
  end;

  TMemDBIndexGenericClass = class of TMemDBIndexGeneric;

  TMemDbIndexLeaf = class;
  TMemDBIndexSearchVal = class;

  TMemDBIndex = class(TMemDBIndexGeneric)
  private
    FFinalFieldOffsets: TFieldOffsets;
    FSparseFieldOffsets: TFieldOffsets;
  public
    function Clone: TMemDBIndexGeneric; override;
    function Find(Sel: TAbSelType; SV: TMemDBIndexSearchVal): TMemDBIndexLeaf;

    property FinalFieldOffsets: TFieldOffsets read FFinalFieldOffsets write FFinalFieldOffsets;
    property SparseFieldOffsets: TFieldOffsets read FSparseFieldOffsets write FSparseFieldOffsets;
  end;

  TMemDBIndexInternal = class(TMemDBIndexGeneric)
  end;

  TMemDBIndexLeafGeneric = class(TCowTreeItem)
  private
    FOriginalIndex: TMemDBIndexGeneric;
    FPinned: TMemDBStreamable;
    FRow: TObject;
{$IFOPT C+}
    FPinnedSet, FRowSet: boolean;
    procedure SetPinned(NewPinned: TMemDBStreamable);
    procedure SetRow(NewRow: TObject);
{$ENDIF}
  protected
    class function ComparePointers(Own, Other: Pointer): integer;
    function DupNoInit(SrcTree: TCowTree): TCowTreeItem; override;
  public
    destructor Destroy; override;

    procedure CopyFrom(Source: TCoWTreeItem); override;

    property OriginalIndex: TMemDbIndexGeneric read FOriginalIndex write FOriginalIndex;
{$IFOPT C+}
    property Pinned: TMemDBStreamable read FPinned write SetPinned;
    property Row: TObject read FRow write SetRow;
{$ELSE}
    property Pinned: TMemDBStreamable read FPinned write FPinned;
    property Row: TObject read FRow write FRow;
{$ENDIF}
  end;

  TMemDBIndexLeaf = class(TMemDBIndexLeafGeneric)
  public
    function Compare(Other: TCoWTreeItem;
                     AllowKeyDedupe: boolean): integer; override;
  end;

  TMemDBInternalIndexLeaf = class (TMemDBIndexLeaf)
  public
    function Compare(Other: TCoWTreeItem;
                     AllowKeyDedupe: boolean): integer; override;
  end;

  TMemDbIndexSearchVal = class(TMemDBIndexLeaf)
  public
    FFieldSearchVals: TMemDBFieldDataRecs;
    function Compare(Other: TCoWTreeItem;
                     AllowKeyDedupe: boolean): integer; override;
    procedure CopyFrom(Source: TCoWTreeItem); override;
  end;

{$IFDEF MEMDB2_TEMP_REMOVE}
type

  //FK change-list indexing.
  TMemDBRowLookasideIndexNode = class(TDuplicateValIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  end;

  //FK change-list indexing.
  TMemDBRowLookasideSearchVal = class(TMemDBRowLookasideIndexNode)
  private
    FFieldSearchVals: TMemDbFieldDataRecs;
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  public
    property FieldSearchVals: TMemDbFieldDataRecs read FFieldSearchVals write FFieldSearchVals;
  end;

{$ENDIF}

function CompareFields(const OwnRec: TMemDbFieldDataRec; const OtherRec: TMemDbFieldDataRec): integer;

const
  S_INDEXING_INTERNAL_FIELD_TYPE_1 = 'Internal indexing error: Different field types.';
  S_INDEXING_INTERNAL_FIELD_TYPE_2 = 'Internal indexing error: Unsupported field type.';
  S_CANNOT_CLONE_CLONES = 'Cannot clone cloned indexes at the moment.';
  S_IDX_BY_CUR_NEXT = 'Access by indexes: use cur or nxt, not latest.';
  S_BAD_POS_FOR_INDEX_OP = 'Bad API position argument for index operation';
  S_NEXT_PREVIOUS_REQUIRES_CURRENT = 'Next or previous by index requires current node.';
  S_INDEX_MOD_BAD_SELECTOR = 'Index selector does not agree with state of index.';
  S_ADD_REQUIRES_NODE = 'Index addition requires a node to add, and not in some other index.';
  S_FIND_REQUIRES_SEARCHVAL = 'Index find requires search value';

implementation

uses
{$IFOPT C+}
{$ELSE}
  Classes,
{$ENDIF}
  MemDB2Buffered, SysUtils, Reffed;

{ Misc functions }

//Unfortunately no BSF / BSR ASM in 64 bit code.
function TrailingZeros64(x: UInt64): Integer;
begin
  Result := 0;

  if (x and $FFFFFFFF) = 0 then
  begin
    Inc(Result, 32);
    x := x shr 32;
  end;

  if (x and $FFFF) = 0 then
  begin
    Inc(Result, 16);
    x := x shr 16;
  end;

  if (x and $FF) = 0 then
  begin
    Inc(Result, 8);
    x := x shr 8;
  end;

  if (x and $F) = 0 then
  begin
    Inc(Result, 4);
    x := x shr 4;
  end;

  if (x and $3) = 0 then
  begin
    Inc(Result, 2);
    x := x shr 2;
  end;

  if (x and $1) = 0 then
    Inc(Result);
end;

function CompareFields(const OwnRec: TMemDbFieldDataRec; const OtherRec: TMemDbFieldDataRec): integer;
var
  OwnP, OtherP: PByte;
  OwnB, OtherB: Byte;
  Own64, Other64: UInt64;
  Cnt: Uint64;
  diff: UInt64;
  idx: Integer;
  shift: Integer;
begin
  if OwnRec.FieldType <> OtherRec.FieldType then
    raise EMemDBInternalException.Create(S_INDEXING_INTERNAL_FIELD_TYPE_1);
  case OwnRec.FieldType of
    ftInteger:
      if OtherRec.i32Val > OwnRec.i32Val then
        result := 1
      else if OtherRec.i32Val < OwnRec.i32Val then
        result := -1
      else
        result := 0;
    ftCardinal:
      if OtherRec.u32Val > OwnRec.u32Val then
        result := 1
      else if OtherRec.u32Val < OwnRec.u32Val then
        result := -1
      else
        result := 0;
    ftInt64:
      if OtherRec.i64Val > OwnRec.i64Val then
        result := 1
      else if OtherRec.i64Val < OwnRec.i64Val then
        result := -1
      else
        result := 0;
    ftUint64:
      if OtherRec.u64Val > OwnRec.u64Val then
        result := 1
      else if OtherRec.u64Val < OwnRec.u64Val then
        result := -1
      else
        result := 0;
    ftUnicodeString: result := CompareStr(OtherRec.sVal, OwnRec.sVal);
    ftDouble:
      if OtherRec.dVal > OwnRec.dVal then
        result := 1
      else if OtherRec.dVal < OwnRec.dVal then
        result := -1
      else
        result := 0;
    ftGuid: result := CompareGuids(OtherRec.gVal, OwnRec.gVal);
    ftBlob:
    begin
      if OtherRec.size > OwnRec.size then
        result := 1
      else if OtherRec.size < OwnRec.Size then
        result := -1
      else
      begin
        Assert(Assigned(OwnRec.Data) = Assigned(OtherRec.Data));
        result := 0;
        if Assigned(OwnRec.Data) then
        begin
          OwnP := OwnRec.Data;
          OtherP := OtherRec.Data;
          Cnt := OwnRec.size;
          while (result = 0) and (Cnt > 0) do
          begin
            if Cnt >= sizeof(Uint64) then
            begin
              Own64 := PUint64(OwnP)^;
              Other64 := PUint64(OtherP)^;

              diff := Own64 xor Other64;

              if diff <> 0 then
              begin
                // Find index of first differing bit (LSB = byte 0)
                idx := TrailingZeros64(diff);  // Delphi intrinsic
                // Convert bit index → byte index (0..7)
                shift := idx and not 7;  // round down to multiple of 8
                OwnB := (Own64 shr shift) and $FF;
                OtherB := (Other64 shr shift) and $FF;
                if OtherB > OwnB then
                  result := 1
                else
                  result := -1;
              end;
              Inc(OwnP, sizeof(Uint64));
              Inc(OtherP, sizeof(Uint64));
              Dec(Cnt, sizeof(Uint64));
            end
            else
            begin
              OwnB := OwnP^;
              OtherB := OtherP^;
              if OtherB > OwnB then
                result := 1
              else if OtherB < OwnB then
                result := -1;
              Inc(OwnP);
              Inc(OtherP);
              Dec(Cnt);
            end
          end;
        end;
      end;
    end;
  else
    raise EMemDBInternalException.Create(S_INDEXING_INTERNAL_FIELD_TYPE_2);
    result := 0;
  end;
end;

{ TMemDBIndexGeneric }

constructor TMemDBIndexGeneric.Create;
begin
  inherited;
end;

destructor TMemDBIndexGeneric.Destroy;
begin
  FParentIndex.Release;
  FRoot.Release;
  FNext.Release;
  inherited;
end;

function TMemDbIndexGeneric.Clone;
begin
  if Assigned(FParentIndex) then
    raise EMemDBInternalException.Create(S_CANNOT_CLONE_CLONES);
  Assert(not Assigned(FParentIndex));
  //We could clone clones, but there are good reasons why we don't want to.
  result := TMemDBIndexGenericClass(self.ClassType).Create;
  with result do
  begin
    FParentIndex:= self.AddRef as TMemDBIndexGeneric;
    FRoot := self.FRoot.AddRef as TMemDbIndexLeaf;
    FNext := nil;
  end
end;

procedure TMemDbIndexGeneric.RootToNext;
begin
  FNext.Release;
  FNext := FRoot.AddRef as TMemDBIndexLeaf;
end;

procedure TMemDbIndexGeneric.CommitNextToRoot;
begin
  FRoot.Release;
  FRoot := FNext;
  FNext := nil;
end;

procedure TMemDbIndexGeneric.DiscardNext;
begin
  FNext.Release;
  FNext := nil;
end;

function TMemDbIndexGeneric.SelToRoot(Sel: TAbSelType): TMemDBIndexLeafGeneric;
begin
  case Sel of
    abCurrent: result := FRoot;
    abNext: result := FNext;
  else
    raise EMemDBInternalException.Create(S_IDX_BY_CUR_NEXT);
  end;
end;

function TMemDbIndexGeneric.Locate(Sel: TAbSelType; Pos: TMemAPIPosition; Cur: TMemDBIndexLeafGeneric): TMemDBIndexLeafGeneric;
var
  Root: TMemDBIndexLeafGeneric;
  FoundOrigin: TChkBool;
begin
{$IFOPT C+}
  FoundOrigin := TChkBool.Create;
  result := nil;
  try
{$ENDIF}
    Root := SelToRoot(Sel);
    case Pos of
      ptFirst: result := LeftMost(Root) as TMemDbIndexLeaf;
      ptLast: result := RightMost(Root) as TMemDbIndexLeaf;
      ptNext, ptPrevious: begin
        if not Assigned(Cur) then
          raise EMemDBException.Create(S_NEXT_PREVIOUS_REQUIRES_CURRENT);
        WrCk(FoundOrigin, false);
        result := self.FindNeighbour(Root, Cur, FoundOrigin, Pos = ptPrevious) as TMemDbIndexLeaf;
        Assert(RdCk(FoundOrigin)); //Normal run-off-the end should still find what we supplied.
      end;
    else
      raise EMemDbInternalException.Create(S_BAD_POS_FOR_INDEX_OP);
    end;
{$IFOPT C+}
  finally
    FoundOrigin.Free;
  end;
{$ENDIF}
end;


function TMemDbIndexGeneric.CheckNonAliasedPresent(Sel: TAbSelType; Node: TMemDBIndexLeafGeneric): boolean;
var
  Root, Leaf: TMemDbIndexLeafGeneric;
begin
  Root := SelToRoot(Sel);
  Assert(Assigned(Node));
  Leaf := SearchNode(Node, Root) as TMemDBIndexLeaf;
  //Unfortunately, it all turns into attack of the clones ...
  //The indexes de-dupe on key, and identity of key item (pinned data).
  //Local index clones can be easily separated from the originals, but for
  //a-b index modification, it's easy to find an alias in "the other" tree.
  //We can't de-dupe on index nodes themselves, as they change when bits
  //of the tree get rebalanced. Best I can find is to search for the node
  //key, and then check it is the same node object.
  if Assigned(Leaf) and (Leaf <> Node) then
    Leaf := nil;
  result := Assigned(Leaf);
end;

function TMemDbIndexGeneric.Add(Sel: TAbSelType; New: TMemDBIndexLeafGeneric): boolean;
var
  Root, NewRoot: TMemDBIndexLeafGeneric;
  SelGood: boolean;
  h, found: TChkBool;
begin
{$IFOPT C+}
  h := TChkBool.Create;
  found := TChkBool.Create;
  result := false;
  try
{$ENDIF}
    Root := SelToRoot(Sel);
    //Selectors: abCurrent for both cloned and original indexes.
    //abNext only for non-cloned indexes.
    case Sel of
      abCurrent: SelGood := true;
      abNext: SelGood := not Assigned(FParentIndex);
    else
      SelGood := false;
    end;
    if not SelGood then
      raise EMemDBInternalException.Create(S_INDEX_MOD_BAD_SELECTOR);
    if (not Assigned(New)) or Assigned(New.FOriginalIndex) then
      raise EMemDBException.Create(S_ADD_REQUIRES_NODE);

    New.FOriginalIndex := self;
    NewRoot := SearchAndInsert(New, Root, h, found) as TMemDBIndexLeaf;
    result := not RdCk(found);
    if Result then
    begin
      //Mods are serialised. No clever interlocked roots.
      case Sel of
        abCurrent: begin
          FRoot.Release; FRoot := NewRoot;
        end;
        abNext: begin
          FNext.Release; FNext := NewRoot;
        end;
      end;
    end
    else
      New.FOriginalIndex := nil;
      //Just about the only time when we can alter node fields.
{$IFOPT C+}
  finally
    h.Free;
    found.Free;
  end;
{$ENDIF}
end;

function TMemDbIndexGeneric.Remove(Sel: TAbSelType; Old: TMemDBIndexLeafGeneric): boolean;
var
  Root, NewRoot: TMemDBIndexLeafGeneric;
  SelGood: boolean;
  h, found: TChkBool;
begin
{$IFOPT C+}
  h := TChkBool.Create;
  found := TChkBool.Create;
  result := false;
  try
{$ENDIF}
    Root := SelToRoot(Sel);
    case Sel of
      abCurrent: SelGood := Assigned(FParentIndex);
      abNext: SelGood := not Assigned(FParentIndex);
    else
      SelGood := false;
    end;
    if not SelGood then
      raise EMemDBInternalException.Create(S_INDEX_MOD_BAD_SELECTOR);
    if not Assigned(Old) then
      raise EMemDBException.Create(S_ADD_REQUIRES_NODE);
    NewRoot := Delete(Old, root, h, found) as TMemDBIndexLeaf;
    result := RdCk(found);
    if Result then
    begin
      //Mods are serialised. No clever interlocked roots.
      case Sel of
        abCurrent: begin
          FRoot.Release; FRoot := NewRoot;
        end;
        abNext: begin
          FNext.Release; FNext := NewRoot;
        end;
      end;
    end;
{$IFOPT C+}
  finally
    h.Free;
    found.Free;
  end;
{$ENDIF}
end;

{ TMemDBIndex }

function TMemDBIndex.Clone: TMemDBIndexGeneric;
begin
  result := inherited;
  if Assigned(result) then
  begin
    Assert(result is TMemDBIndex);
    with result as TMemDBIndex do
    begin
      FFinalFieldOffsets := self.FFinalFieldOffsets;
      FSparseFieldOffsets := self.FSparseFieldOffsets;
    end;
  end;
end;

function TMemDbIndex.Find(Sel: TAbSelType; SV: TMemDBIndexSearchVal): TMemDBIndexLeaf;
var
  Root: TMemDBIndexLeaf;
begin
  Root := SelToRoot(Sel) as TMemDbIndexLeaf;
  if not Assigned(SV) then
    raise EMemDBException.Create(S_FIND_REQUIRES_SEARCHVAL);
  result := SearchItem(SV, Root) as TMemDbIndexLeaf;
end;

{ TMemDbIndexLeafGeneric }

destructor TMemDBIndexLeafGeneric.Destroy;
begin
  Assert(Assigned(self.FPinned) = Assigned(self.FRow));
  if Assigned(FPinned) then
    (FRow as TMemDBRow).UnpinFromIndex(self);
  FPinned.Release;
  FRow := nil;
  FPinned := nil; //Protect against too much freeing in exception handlers.
  inherited;
end;

function TMemDBIndexLeafGeneric.DupNoInit(SrcTree: TCowTree): TCowTreeItem;

begin
  result := inherited;
{$IFOPT C+}
  TMemDBIndexLeafGeneric(result).FOriginalIndex := SrcTree as TMemDbIndexGeneric;
{$ELSE}
  TMemDBIndexLeafGeneric(result).FOriginalIndex := TMemDBIndexGeneric(SrcTree);
{$ENDIF}
end;

{$IFOPT C+}
procedure TMemDBIndexLeafGeneric.SetPinned(NewPinned: TMemDBStreamable);
begin
  Assert(not FPinnedSet);
  FPinnedSet := true;
  FPinned := NewPinned;
end;

procedure TMemDBIndexLeafGeneric.SetRow(NewRow: TObject);
begin
  Assert(not FRowSet);
  FRowSet := true;
  FRow := NewRow;
end;
{$ENDIF}

procedure TMemDBIndexLeafGeneric.CopyFrom(Source: TCoWTreeItem);
var
  SL: TMemDbIndexLeafGeneric;
{$IFOPT C+}
  SameFamily: boolean;
  IndexCheck: TMemDBIndexGeneric;
{$ENDIF}
begin
  Assert(Assigned(Source) and (Source.ClassType = Self.ClassType));
  SL := TMemDBIndexLeafGeneric(Source);
  //Leave FOriginalIndex as set in my node: nodes immutable,
  //and the new copy we're creating is in the New (changed) index.

  //As we're a newer node, we check in the same family,
  //we might be in a clone, or might be in same index as source
  //but source will never be in a clone of our index.
{$IFOPT C+}
  SameFamily := false;
  IndexCheck := FOriginalIndex;
  Assert(Assigned(IndexCheck));
  while (not SameFamily) and Assigned(IndexCheck) do
  begin
    SameFamily := IndexCheck = SL.FOriginalIndex;
    IndexCheck := IndexCheck.FParentIndex;
  end;
  Assert(SameFamily);
{$ENDIF}
  (SL.Row as TMemDBRow).DupIndexPin(SL, self);
  //Now we need a key de-dupe which is invariant even when we copy nodes,
  //otherwise very bad things happen...
  Assert(self.FPinned = SL.FPinned);
end;


{$HINTS OFF}
class function TMemDbIndexLeafGeneric.ComparePointers(Own, Other: Pointer): integer;
var
  OwnInt, OtherInt: Cardinal;
  Own64, Other64: UInt64;
begin
  if sizeof(Pointer) = sizeof(Cardinal) then
  begin
    OwnInt := Cardinal(Own);
    OtherInt := Cardinal(Other);
    if OtherInt > OwnInt then
      result := 1
    else if OtherInt < OwnInt then
      result := -1
    else
      result := 0;
  end
  else if sizeof(Pointer) = sizeof(UInt64) then
  begin
    Own64 := UInt64(Own);
    Other64 := UInt64(Other);
    if Other64 > Own64 then
      result := 1
    else if Other64 < Own64 then
      result := -1
    else
      result := 0;
  end
  else
    Assert(false);
end;
{$HINTS ON}

{ TMemDbIndexLeaf }

function TMemDBIndexLeaf.Compare(Other: TCoWTreeItem;
                 AllowKeyDedupe: boolean): integer;
var
  OtherLeaf: TMemDBIndexLeaf;
  OwnOffsets, OtherOffsets: TFieldOffsets;
  OwnFields, OtherFields: TMemRowFields;
  ff: integer;
  OwnField, OtherField: TMemDBStreamable;
  OwnFieldData, OtherFieldData: TMemFieldData;
  OrigIndex, OtherOrigIndex: TMemDBIndex;
begin
{$IFOPT C+}
  OtherLeaf := Other as TMemDbIndexLeaf;;
{$ELSE}
  OtherLeaf := TMemDbIndexLeaf(Other);
{$ENDIF}
  Assert(Assigned(OtherLeaf));
  //Let's assume not putting deleted items in index.
  Assert(AssignedNotsentinel(FPinned));
  Assert(AssignedNotSentinel(OtherLeaf.FPinned));
{$IFOPT C+}
  OwnFields := self.FPinned as TMemRowFields;
  OtherFields := OtherLeaf.FPinned as TMemRowFields;
{$ELSE}
  OwnFields := TMemRowFields(self.FPinned);
  OtherFields := TMemRowFields(OtherLeaf.FPinned);
{$ENDIF}
{$IFOPT C+}
  OrigIndex := FOriginalIndex as TMemDbIndex;
  OtherOrigIndex := OtherLeaf.FOriginalIndex as TMemDbIndex;
{$ELSE}
  OrigIndex := TMemDbIndex(FOriginalIndex);
  OtherOrigIndex := TMemDbIndex(OtherLeaf.FOriginalIndex);
{$ENDIF}
  if OwnFields.Sparse then
    OwnOffsets := OrigIndex.FSparseFieldOffsets
  else
    OwnOffsets := OrigIndex.FFinalFieldOffsets;
  if OtherFields.Sparse then
    OtherOffsets := OtherOrigIndex.FSparseFieldOffsets
  else
    OtherOffsets := OtherOrigIndex.FFinalFieldOffsets;

  Assert(Length(OwnOffsets) = Length(OtherOffsets));
  ff := 0;
  result := 0;
  while (result = 0) and (ff < Length(OwnOffsets)) do
  begin
    OwnField := OwnFields[OwnOffsets[ff]];
    OtherField := OtherFields[OtherOffsets[ff]];
    Assert(AssignedNotSentinel(OwnField));
    Assert(AssignedNotSentinel(OtherField));
{$IFOPT C+}
    OwnFieldData := OwnField as TMemFieldData;
    OtherFieldData := OtherField as TMemFieldData;
{$ELSE}
    OwnFieldData := TMemFieldData(OwnField);
    OtherFieldData := TMemFieldData(OtherField);
{$ENDIF}
    result := CompareFields(OwnFieldData.FDataRec,
                            OtherFieldData.FDataRec);
    Inc(ff);
  end;
  if (result = 0) and AllowKeyDedupe then
    result := ComparePointers(self.FPinned, OtherLeaf.FPinned);
end;

{ TMemDbInternalIndexLeaf }

function TMemDBInternalIndexLeaf.Compare(Other: TCoWTreeItem;
                 AllowKeyDedupe: boolean): integer;
var
  OtherLeaf: TMemDBInternalIndexLeaf;
begin
{$IFOPT C+}
  OtherLeaf := Other as TMemDbInternalIndexLeaf;
{$ELSE}
  OtherLeaf := TMemDbInternalIndexLeaf(Other);
{$ENDIF}
  Assert(AllowKeyDedupe);
  result := ComparePointers(self.FPinned, OtherLeaf.FPinned);
end;

{ TMemDBIndexSearchVal }

function TMemDBIndexSearchVal.Compare(Other: TCoWTreeItem;
                 AllowKeyDedupe: boolean): integer;
var
  OtherLeaf: TMemDBIndexLeaf;
  OtherOffsets: TFieldOffsets;
  OtherFields: TMemRowFields;
  ff: integer;
  OtherField: TMemDBStreamable;
  OtherOrigIndex: TMemDBIndex;

begin
  OtherLeaf := TMemDbIndexLeaf(Other);
  Assert(Assigned(OtherLeaf));
  //Let's assume not putting deleted items in index.
  Assert(not Assigned(FPinned));
  Assert(not Assigned(FOriginalIndex));
  Assert(AssignedNotSentinel(OtherLeaf.FPinned));
{$IFOPT C+}
  OtherFields := OtherLeaf.FPinned as TMemRowFields;
{$ELSE}
  OtherFields := TMemRowFields(OtherLeaf.FPinned);
{$ENDIF}
{$IFOPT C+}
  OtherOrigIndex := OtherLeaf.FOriginalIndex as TMemDBIndex;
{$ELSE}
  OtherOrigIndex := TMemDBIndex(OtherLeaf.FOriginalIndex);
{$ENDIF}
  //Own offsets just the index ff.
  if OtherFields.Sparse then
    OtherOffsets := OtherOrigIndex.FSparseFieldOffsets
  else
    OtherOffsets := OtherOrigIndex.FFinalFieldOffsets;

  Assert(Length(FFieldSearchVals) = Length(OtherOffsets));
  ff := 0;
  result := 0;
  while (result = 0) and (ff < Length(FFieldSearchVals)) do
  begin
    OtherField := OtherFields[OtherOffsets[ff]];
    Assert(AssignedNotSentinel(OtherField));
{$IFOPT C+}
    result := CompareFields(FFieldSearchVals[ff],
                            (OtherField as TMemFieldData).FDataRec);
{$ELSE}
    result := CompareFields(FFieldSearchVals[ff],
                            TMemFieldData(OtherField).FDataRec);
{$ENDIF}
    Inc(ff);
  end;
  Assert(not AllowKeyDedupe);
end;

procedure TMemDbIndexSearchVal.CopyFrom(Source: TCoWTreeItem);
begin
  Assert(false);
end;


{ TMemDbIndexNode }

{ Two separate cases:
  1. Changing data:
    a) Current Copy should be either NIL or valid.
    b) Next copy is Valid or delete sentinel

  - CurrentIndex always deals with CurrentCopy,
    sufficient to put NIL's at one end (low / high) of the index.

  - NextData deals with most recent copy,
    - might have both NIL in current and delete sentinel if added and
      deleted new row.

  - Don't distinguish between NIL and delete sentinel once we've finished
    resolving the field list

    - For the moment, don't skip delete sentinels on fields,
      we'll assume the index tags are up to date and valid.

  2. Changing field lists.

   - Items gets removed and re-added.

     - We encode the current field, and the next field in the "extra field",
       current field is used on tree removal, and next field on re-insertion,
       in cases where metadata changed.

       Current and next are the same for existing indices when items removed
       from the tree for modification via metadata changes, so no index tag
       re-name required.

       However, upon re-insertion into the tree (with delete sentinels),
       the current and next are different, the next value being used
       just after commit time, when the delete sentinels have been removed.

}

//TODO - Consider some radical hack whereby we don't need to adjust the indexes
//to temporary values at all.

var
  GoodTagsRead: integer = 0;

{ TMemDBRowLookasideIndexNode}

{$IFDEF MEMDB2_TEMP_REMOVE}
function TMemDBRowLookasideIndexNode.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  Tag: PMemDBMetaTags;
  OwnFieldList, OtherFieldList: TMemStreamableList;
  OwnRow, OtherRow: TMemDbRow;
  OwnField, OtherField: TMemDBStreamable;
  ff: integer;
  PtrFieldOffset: PFieldOffset;

begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OwnItem is TMemDBRow);
  Assert(OtherItem is TMemDBRow);
  Tag := PMemDBMetaTags(IndexTag);

  //First off, deal with NULL abData cases.
{$IFOPT C+}
  OwnRow := OwnItem as TMemDBRow;
  OtherRow := OtherItem as TMemDbRow;
{$ELSE}
  OwnRow := TMemDBRow(OwnItem);
  OtherRow := TMemDBRow(OtherItem);
{$ENDIF}

  if AssignedNotSentinel(OwnRow.ABData[Tag.abBuf]) then
  begin
{$IFOPT C+}
    OwnFieldList := OwnRow.ABData[Tag.abBuf] as TMemStreamableList;
{$ELSE}
    OwnFieldList := TMemStreamableList(OwnRow.ABData[Tag.abBuf]);
{$ENDIF}
  end
  else
    OwnFieldList := nil;

  if AssignedNotSentinel(OtherRow.ABData[Tag.abBuf]) then
  begin
{$IFOPT C+}
    OtherFieldList := OtherRow.ABData[Tag.abBuf] as TMemStreamableList;
{$ELSE}
    OtherFieldList := TMemStreamableList(OtherRow.ABData[Tag.abBuf]);
{$ENDIF}
  end
  else
    OtherFieldList := nil;

  if not (Assigned(OwnFieldList) or Assigned(OtherFieldList)) then
    result := 0
  else if not (Assigned(OwnFieldList) and Assigned(OtherFieldList)) then
  begin
    if Assigned(OwnFieldList) then
      result := 1
    else
      result := -1;
  end
  else //Both assigned.
  begin
      Assert(Tag.FieldAbsIdxsFast.Count > 0);
      result := 0;
      ff := 0;
      PtrFieldOffset := Tag.FieldAbsIdxsFast.PtrFirst;
      while (result = 0) and (ff < Tag.FieldAbsIdxsFast.Count) do
      begin
        Assert(PtrFieldOffset^ < OwnFieldList.Count);
        Assert(PtrFieldOffset^ < OtherFieldList.Count);
        OwnField := OwnFieldList.Items[PtrFieldOffset^];
        OtherField := OtherFieldList.Items[PtrFieldOffset^];
        Assert(not (OwnField is TMemDeleteSentinel));
        Assert(not (OtherField is TMemDeleteSentinel));
{$IFOPT C+}
        result := CompareFields((OwnField as TMemFieldData).FDataRec,
                                (OtherField as TMemFieldData).FDataRec);
{$ELSE}
        result := CompareFields(TMemFieldData(OwnField).FDataRec,
                                TMemFieldData(OtherField).FDataRec);
{$ENDIF}
        Inc(ff);
        Inc(PtrFieldOffset);
      end;
  end;
end;
{$ENDIF}

{ TMemDBRowLookasideSearchVal }

{$IFDEF MEMDB2_TEMP_REMOVE}
function TMemDBRowLookasideSearchVal.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  Tag: PMemDBMetaTags;
  OtherFieldList: TMemStreamableList;
  OtherRow: TMemDbRow;
  OtherField: TMemDBStreamable;
  ff: integer;
  PtrFieldOffset: PFieldOffset;

begin
  Assert(not Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OtherItem is TMemDBRow);
  Tag := PMemDBMetaTags(IndexTag);

  //First off, deal with NULL abData cases.
{$IFOPT C+}
  OtherRow := OtherItem as TMemDbRow;
{$ELSE}
  OtherRow := TMemDbRow(OtherItem);
{$ENDIF}

  if AssignedNotSentinel(OtherRow.ABData[Tag.abBuf]) then
  begin
{$IFOPT C+}
    OtherFieldList := OtherRow.ABData[Tag.abBuf] as TMemStreamableList
{$ELSE}
    OtherFieldList := TMemStreamableList(OtherRow.ABData[Tag.abBuf]);
{$ENDIF}
  end
  else
    OtherFieldList := nil;

  if not Assigned(OtherFieldList) then
      result := 1
  else //Both assigned.
  begin
      Assert(Tag.FieldAbsIdxsFast.Count > 0);
      Assert(Tag.FieldAbsIdxsFast.Count = Length(FFieldSearchVals));
      result := 0;
      ff := 0;
      PtrFieldOffset := Tag.FieldAbsIdxsFast.PtrFirst;
      while (result = 0) and (ff < Tag.FieldAbsIdxsFast.Count) do
      begin
        Assert(PtrFieldOffset^ < OtherFieldList.Count);
        OtherField := OtherFieldList.Items[PtrFieldOffset^];
        Assert(not (OtherField is TMemDeleteSentinel));
{$IFOPT C+}
        result := CompareFields(FFieldSearchVals[ff],
                                (OtherField as TMemFieldData).FDataRec);
{$ELSE}
        result := CompareFields(FFieldSearchVals[ff],
                                TMemFieldData(OtherField).FDataRec);
{$ENDIF}
        Inc(ff);
        Inc(PtrFieldOffset);
      end;
  end;
end;
{$ENDIF}

end.
