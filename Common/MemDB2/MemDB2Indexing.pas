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
  MemDB2Misc, MemDB2Streamable, CowTree, IndexedStore, Reffed, Classes;

const
  NODE_CACHE_SIZE = 64; //Longest possible path + rebalances

type
  TMemDbIndexLeafGeneric = class;

  TMemDBNodeCache = class(TReffedCache)
  protected
    FPtrs: TList;
    FPinCache: TObject;
  public
    constructor Create;
    destructor Destroy; override;
    function GetFromCache: TReffed; override;
    function PutToCache(Reffed: TReffed): boolean; override;
    property PinCache: TObject read FPinCache;
  end;

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

    //Index addition can end up consuming new index leaf both in success, and
    //some exception cases (freeing partially built new tree).
    function Add(Sel: TAbSelType; var New: TMemDBIndexLeafGeneric): boolean;
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

    FFinalFastBase, FSparseFastBase: PFieldOffset;
    FFinalFastCount, FSparseFastCount: integer;
  protected
    procedure SetFinalFieldOffsets(FinalOffsets: TFieldOffsets);
    procedure SetSparseFieldOffsets(SparseOffsets: TFieldOffsets);

{$IFOPT C+}
    procedure CheckFastConsistent;

    function GetFinalFieldOffsets: TFieldOffsets;
    function GetSparseFieldOffsets: TFieldOffsets;
    function GetFinalFastBase: PFieldOffset;
    function GetSparseFastBase: PFieldOffset;
    function GetFinalFastCount: integer;
    function GetSparseFastCount: integer;
{$ENDIF}
  public
    destructor Destroy; override;
    function Clone: TMemDBIndexGeneric; override;
    function Find(Sel: TAbSelType; SV: TMemDBIndexSearchVal): TMemDBIndexLeaf;

{$IFOPT C+}
    property FinalFieldOffsets: TFieldOffsets read GetFinalFieldOffsets write SetFinalFieldOffsets;
    property SparseFieldOffsets: TFieldOffsets read GetSparseFieldOffsets write SetSparseFieldOffsets;
    property FinalFastBase: PFieldOffset read GetFinalFastBase;
    property SparseFastBase: PFieldOffset read GetSparseFastBase;
    property FinalFastCount: integer read GetFinalFastCount;
    property SparseFastCount: integer read GetSparseFastCount;
{$ELSE}
    property FinalFieldOffsets: TFieldOffsets read FFinalFieldOffsets write SetFinalFieldOffsets;
    property SparseFieldOffsets: TFieldOffsets read FSparseFieldOffsets write SetSparseFieldOffsets;
    property FinalFastBase: PFieldOffset read FFinalFastBase;
    property SparseFastBase: PFieldOffset read FSparseFastBase;
    property FinalFastCount: integer read FFinalFastCount;
    property SparseFastCount: integer read FSparseFastCount;
{$ENDIF}
  end;

  TMemDBIndexInternal = class(TMemDBIndexGeneric)
  end;

  TMemDBIndexLeafGeneric = class(TCowTreeItem)
  private
    FOriginalIndex: TMemDBIndexGeneric;
    FPin: Pointer; //PMemDBIndexPin;
    FRow: TObject;
{$IFOPT C+}
    FPinSet, FRowSet: boolean;
    procedure SetPin(NewPin: Pointer);
    procedure SetRow(NewRow: TObject);
{$ENDIF}
  protected
    class function ComparePointers(Own, Other: Pointer): integer;
    function DupNoInit(SrcTree: TCowTree): TCowTreeItem; override;
  public
    destructor Destroy; override;
    procedure Reset(Cache: TReffedCache); override;

    procedure CopyFrom(Source: TCoWTreeItem; SrcTree: TCowTree); override;

    property OriginalIndex: TMemDbIndexGeneric read FOriginalIndex write FOriginalIndex;
{$IFOPT C+}
    property Pin: Pointer read FPin write SetPin;
    property Row: TObject read FRow write SetRow;
{$ELSE}
    property Pin: Pointer read FPin write FPin;
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
    procedure CopyFrom(Source: TCoWTreeItem; SrcTree: TCowTree); override;
  end;

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
  MemDB2Buffered, SysUtils, MemDB2BufBase;

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

constructor TMemDBNodeCache.Create;
begin
  inherited;
  FPtrs := TList.Create;
  FPinCache :=  TMemDBPinCache.Create;
end;

destructor TMemDBNodeCache.Destroy;
var
  i: integer;
begin
  for i := 0 to Pred(FPtrs.Count) do
    TReffed(FPtrs[i]).Release; //Refcounts 1 for cached objects.
  FPtrs.Free;
  FPinCache.Free;
  inherited;
end;

function TMemDBNodeCache.GetFromCache: TReffed;
var
  Count: integer;
begin
  Count := FPtrs.Count;
  if Count > 0 then
  begin
    Dec(Count);
    result := TReffed(FPtrs[Count]);
    FPtrs.Delete(Count);
  end
  else
    result := nil;
end;

function TMemDBNodeCache.PutToCache(Reffed: TReffed): boolean;
var
  Count: integer;
begin
  if Assigned(Reffed) then
  begin
    Count := FPtrs.Count;
    if Count < NODE_CACHE_SIZE then
    begin
      FPtrs.Add(Reffed); //Add before reset so size calc is OK ...
      Reffed.Reset(self);
      result := true;
    end
    else
      result := false;
  end
  else
    result := true;
end;

{ TMemDBIndexGeneric }

constructor TMemDBIndexGeneric.Create;
begin
  FCache := TMemDBNodeCache.Create;
  inherited;
end;

destructor TMemDBIndexGeneric.Destroy;
begin
  FParentIndex.Release;
  FRoot.Release; //Not to cache...
  FNext.Release; //Not to cache...
  FCache.Free;
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

  //Don't allow aliases. Their data may be identical, but their lifetime may not be.
  //could allow specific (local / non-local) optimizations for certain cases,
  //but that's a whole class of hard-to-repro bugs which I'd rather not deal with.
  if Assigned(Leaf) and (Leaf <> Node) then
    Leaf := nil;
  result := Assigned(Leaf);
end;

function TMemDbIndexGeneric.Add(Sel: TAbSelType; var New: TMemDBIndexLeafGeneric): boolean;
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
    try
      NewRoot := SearchAndInsert(New, Root, h, found) as TMemDBIndexLeaf;
    except
      New := nil; //Unfortunately consumed as part of clean-up
      raise;
    end;
    result := not RdCk(found);
    if Result then
    begin
      New := nil; //Consumed with good addition.

      //Mods are serialised. No clever interlocked roots.
      case Sel of
        abCurrent: begin
          FRoot.ReleaseToCache(FCache); FRoot := NewRoot;
        end;
        abNext: begin
          FNext.ReleaseToCache(FCache); FNext := NewRoot;
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
          FRoot.ReleaseToCache(FCache); FRoot := NewRoot;
        end;
        abNext: begin
          FNext.ReleaseTOCache(FCache); FNext := NewRoot;
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

{$IFOPT C+}
procedure TMemDbIndex.CheckFastConsistent;
begin
  Assert(Length(FFinalFieldOffsets) = FFinalFastCount);
  Assert(Length(FSparseFieldOffsets) = FSparseFastCount);
  Assert(Assigned(FFinalFastBase) = (FFinalFastCount <> 0));
  Assert(Assigned(FSparseFastBase) = (FSparseFastCount <> 0));
end;

function TMemDbIndex.GetFinalFieldOffsets: TFieldOffsets;
begin
  CheckFastConsistent;
  result := FFinalFieldOffsets;
end;

function TMemDbIndex.GetSparseFieldOffsets: TFieldOffsets;
begin
  CheckFastConsistent;
  result := FSparseFieldOffsets;
end;

function TMemDbIndex.GetFinalFastBase: PFieldOffset;
begin
  CheckFastConsistent;
  result := FFinalFastBase;
end;

function TMemDbIndex.GetSparseFastBase: PFieldOffset;
begin
  CheckFastConsistent;
  result := FSparseFastBase;
end;

function TMemDbIndex.GetFinalFastCount: integer;
begin
  CheckFastConsistent;
  result := FFinalFastCount;
end;

function TMemDbIndex.GetSparseFastCount: integer;
begin
  CheckFastConsistent;
  result := FSparseFastCount;
end;

{$ENDIF}

procedure TMemDbIndex.SetFinalFieldOffsets(FinalOffsets: TFieldOffsets);
var
  NewLen, i: integer;
  WPtr: PFieldOffset;
begin
{$IFOPT C+}
  CheckFastConsistent;
{$ENDIF}
  FFinalFieldOffsets := FinalOffsets;
  NewLen := Length(FFinalFieldOffsets);
  if NewLen <> FFinalFastCount then
  begin
    if Assigned(FFinalFastBase) then
    begin
      FreeMem(FFinalFastBase);
      FFinalFastBase := nil;
    end;
    if NewLen > 0 then
    begin
      GetMem(FFinalFastBase, NewLen * sizeof(TFieldOffset));
    end;
    FFinalFastCount := NewLen;
  end;
  WPtr := FFinalFastBase;
  for i := 0 to Pred(NewLen) do
  begin
    WPtr^ := FFinalFieldOffsets[i];
    Inc(WPtr);
  end;
{$IFOPT C+}
  CheckFastConsistent;
{$ENDIF}
end;

procedure TMemDbIndex.SetSparseFieldOffsets(SparseOffsets: TFieldOffsets);
var
  NewLen, i: integer;
  WPtr: PFieldOffset;
begin
{$IFOPT C+}
  CheckFastConsistent;
{$ENDIF}
  FSparseFieldOffsets := SparseOffsets;
  NewLen := Length(FSparseFieldOffsets);
  if NewLen <> FSparseFastCount then
  begin
    if Assigned(FSparseFastBase) then
    begin
      FreeMem(FSparseFastBase);
      FSparseFastBase := nil;
    end;
    if NewLen > 0 then
    begin
      GetMem(FSparseFastBase, NewLen * sizeof(TFieldOffset));
    end;
    FSparseFastCount := NewLen;
  end;
  WPtr := FSparseFastBase;
  for i := 0 to Pred(NewLen) do
  begin
    WPtr^ := FSparseFieldOffsets[i];
    Inc(WPtr);
  end;
{$IFOPT C+}
  CheckFastConsistent;
{$ENDIF}
end;

destructor TMemDBIndex.Destroy;
begin
  if Assigned(FFinalFastBase) then
    FreeMem(FFinalFastBase);
  if Assigned(FSparseFastBase) then
    FreeMem(FSparseFastBase);
  inherited;
end;

function TMemDBIndex.Clone: TMemDBIndexGeneric;
begin
{$IFOPT C+}
  CheckFastConsistent;
{$ENDIF}
  result := inherited;
  if Assigned(result) then
  begin
    Assert(result is TMemDBIndex);
    with result as TMemDBIndex do
    begin
      FinalFieldOffsets := self.FinalFieldOffsets;
      SparseFieldOffsets := self.SparseFieldOffsets;
    end;
  end;
{$IFOPT C+}
  CheckFastConsistent;
  (result as TMemDBIndex).CheckFastConsistent;
{$ENDIF}
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
  Assert(Assigned(self.FPin) = Assigned(self.FRow));
  if Assigned(FPin) then
    (FRow as TMemDBRow).UnpinFromIndex(self, nil);
  inherited;
end;

procedure TMemDBIndexLeafGeneric.Reset(Cache: TReffedCache);
var
  NCache: TMemDBNodeCache;
  PCache: TMemDBPinCache;
begin
  Assert(Assigned(self.FPin) = Assigned(self.FRow));
  Assert(Assigned(Cache));
{$IFOPT C+}
  NCache := Cache as TMemDbNodeCache;
  PCache := NCache.PinCache as TMemDBPinCache;
{$ELSE}
  NCache := TMemDBNodeCache(Cache);
  PCache := TMemDbPinCache(NCache.PinCache);
{$ENDIF}
  if Assigned(FPin) then
    (FRow as TMemDBRow).UnpinFromIndex(self, PCache);
  FOriginalIndex := nil;
  FPin := nil;
  FRow := nil;
{$IFOPT C+}
  FPinSet := false;
  FRowSet := false;
{$ENDIF}
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
procedure TMemDBIndexLeafGeneric.SetPin(NewPin: pointer);
begin
  Assert(not FPinSet);
  FPinSet := true;
  FPin := NewPin;
end;

procedure TMemDBIndexLeafGeneric.SetRow(NewRow: TObject);
begin
  Assert(not FRowSet);
  FRowSet := true;
  FRow := NewRow;
end;
{$ENDIF}

procedure TMemDBIndexLeafGeneric.CopyFrom(Source: TCoWTreeItem; SrcTree: TCowTree);
var
  SL: TMemDbIndexLeafGeneric;
{$IFOPT C+}
  SameFamily: boolean;
{$ENDIF}
  IndexCheck: TMemDBIndexGeneric;
  NCache: TMemDBNodeCache;
  PCache: TMemDBPinCache;
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
{$IFOPT C+}
  IndexCheck := SrcTree as TMemDBIndexGeneric;
  NCache := IndexCheck.FCache as TMemDbNodeCache;
  if Assigned(NCache) then
    PCache := NCache.PinCache as TMemDBPinCache
  else
    PCache := nil;
{$ELSE}
  IndexCheck := TMemDbIndexGeneric(SrcTree);
  NCache := TMemDBNodeCache(IndexCheck.FCache);
  if Assigned(NCache) then
    PCache := TMemDBPinCache(NCache.PinCache)
  else
    PCache := nil;
{$ENDIF}
  (SL.Row as TMemDBRow).DupIndexPin(SL, self, PCache);
  //Now we need a key de-dupe which is invariant even when we copy nodes,
  //otherwise very bad things happen...
  Assert(PMemDBIndexPin(self.FPin).PinnedItem = PMemDBIndexPin(SL.FPin).PinnedItem);
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
  POwnOffset, POtherOffset: PFieldOffset;
  OwnOffsetCount, OtherOffsetCount: integer;
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
  Assert(AssignedNotsentinel(FPin));
  Assert(AssignedNotSentinel(OtherLeaf.FPin));
{$IFOPT C+}
  Assert(TObject(FPin) is PMemDBIndexPin);
  Assert(TObject(OtherLeaf.FPin) is PMemDBIndexPin);

  OwnFields := PMemDBIndexPin(self.Pin).PinnedItem as TMemRowFields;
  OtherFields := PMemDBIndexPin(OtherLeaf.Pin).PinnedItem as TMemRowFields;
{$ELSE}
  OwnFields := TMemRowFields(PMemDBIndexPin(self.Pin).PinnedItem);
  OtherFields := TMemRowFields(PMemDBIndexPin(OtherLeaf.Pin).PinnedItem);
{$ENDIF}
{$IFOPT C+}
  OrigIndex := FOriginalIndex as TMemDbIndex;
  OtherOrigIndex := OtherLeaf.FOriginalIndex as TMemDbIndex;
{$ELSE}
  OrigIndex := TMemDbIndex(FOriginalIndex);
  OtherOrigIndex := TMemDbIndex(OtherLeaf.FOriginalIndex);
{$ENDIF}
  if OwnFields.Sparse then
  begin
    POwnOffset := OrigIndex.SparseFastBase;
    OwnOffsetCount := OrigIndex.SparseFastCount;
  end
  else
  begin
    POwnOffset := OrigIndex.FinalFastBase;
    OwnOffsetCount := OrigIndex.FinalFastCount;
  end;
  if OtherFields.Sparse then
  begin
    POtherOffset := OtherOrigIndex.SparseFastBase;
    OtherOffsetCount := OtherOrigIndex.SparseFastCount;
  end
  else
  begin
    POtherOffset := OtherOrigIndex.FinalFastBase;
    OtherOffsetCount := OtherOrigIndex.FinalFastCount;
  end;

  Assert(OwnOffsetCount = OtherOffsetCount);
  ff := 0;
  result := 0;
  while (result = 0) and (ff < OwnOffsetCount) do
  begin
    OwnField := OwnFields[POwnOffset^];
    OtherField := OtherFields[POtherOffset^];
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
    Inc(POwnOffset);
    Inc(POtherOffset);
  end;
  if (result = 0) and AllowKeyDedupe then
  begin
    //Similar to internal index, use RowID to ensure a stable sort even when
    //pinned data items change.
{$IFOPT C+}
    result := CompareGuids((OtherLeaf.Row as TMemDBRow).RowId,
                           (self.Row as TMemDBRow).RowId);
{$ELSE}
  result := CompareGuids(TMemDBRow(OtherLeaf.Row).RowId,
                         TMemDBRow(self.Row).RowId);
{$ENDIF}
  end;
end;

{ TMemDbInternalIndexLeaf }

function TMemDBInternalIndexLeaf.Compare(Other: TCoWTreeItem;
                 AllowKeyDedupe: boolean): integer;
var
  OtherLeaf: TMemDBInternalIndexLeaf;
begin
  //OK, we now need to de-dupe the rows in a way which is stable,
  //such that if we mod / unpin / repin the comparison does not
  //change.
{$IFOPT C+}
  OtherLeaf := Other as TMemDbInternalIndexLeaf;
  Assert(AllowKeyDedupe);
  result := CompareGuids((OtherLeaf.Row as TMemDBRow).RowId,
                         (self.Row as TMemDBRow).RowId);
{$ELSE}
  OtherLeaf := TMemDbInternalIndexLeaf(Other);
  result := CompareGuids(TMemDBRow(OtherLeaf.Row).RowId,
                         TMemDBRow(self.Row).RowId);
{$ENDIF}
end;

{ TMemDBIndexSearchVal }

function TMemDBIndexSearchVal.Compare(Other: TCoWTreeItem;
                 AllowKeyDedupe: boolean): integer;
var
  OtherLeaf: TMemDBIndexLeaf;
  POtherOffset: PFieldOffset;
  OtherOffsetCount: integer;
  OtherFields: TMemRowFields;
  ff: integer;
  OtherField: TMemDBStreamable;
  OtherOrigIndex: TMemDBIndex;

begin
  OtherLeaf := TMemDbIndexLeaf(Other);
  Assert(Assigned(OtherLeaf));
  //Let's assume not putting deleted items in index.
  Assert(not Assigned(FPin));
  Assert(not Assigned(FOriginalIndex));
  Assert(AssignedNotSentinel(OtherLeaf.FPin));
{$IFOPT C+}
  Assert(TObject(OtherLeaf.FPin) is PMemDBIndexPin);

  OtherFields := PMemDBIndexPin(OtherLeaf.Pin).PinnedItem as TMemRowFields;
{$ELSE}
  OtherFields := TMemRowFields(PMemDBIndexPin(OtherLeaf.Pin).PinnedItem);
{$ENDIF}
{$IFOPT C+}
  OtherOrigIndex := OtherLeaf.FOriginalIndex as TMemDBIndex;
{$ELSE}
  OtherOrigIndex := TMemDBIndex(OtherLeaf.FOriginalIndex);
{$ENDIF}
  //Own offsets just the index ff.
  if OtherFields.Sparse then
  begin
    POtherOffset := OtherOrigIndex.SparseFastBase;
    OtherOffsetCount := OtherOrigIndex.SparseFastCount;
  end
  else
  begin
    POtherOffset := OtherOrigIndex.FinalFastBase;
    OtherOffsetCount := OtherOrigIndex.FinalFastCount;
  end;

  Assert(Length(FFieldSearchVals) = OtherOffsetCount);
  ff := 0;
  result := 0;
  while (result = 0) and (ff < Length(FFieldSearchVals)) do
  begin
    OtherField := OtherFields[POtherOffset^];
    Assert(AssignedNotSentinel(OtherField));
{$IFOPT C+}
    result := CompareFields(FFieldSearchVals[ff],
                            (OtherField as TMemFieldData).FDataRec);
{$ELSE}
    result := CompareFields(FFieldSearchVals[ff],
                            TMemFieldData(OtherField).FDataRec);
{$ENDIF}
    Inc(ff);
    Inc(POtherOffset);
  end;
  Assert(not AllowKeyDedupe);
end;

procedure TMemDbIndexSearchVal.CopyFrom(Source: TCoWTreeItem; SrcTree: TCowTree);
begin
  Assert(false);
end;

{ TMemDBRowLookasideIndexNode}

function TMemDBRowLookasideIndexNode.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  OwnRow, OtherRow: TMemDbRow;
  OwnS, OtherS: TMemDBStreamable;
  OwnFields, OtherFields: TMemRowFields;
  OwnField, OtherField: TMemDBStreamable;
  ff, FieldCount: integer;
  PMeta: PMemDBFKMeta;

begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OwnItem is TMemDBRow);
  Assert(OtherItem is TMemDBRow);

{$IFOPT C+}
  OwnRow := OwnItem as TMemDBRow;
  OtherRow := OtherItem as TMemDbRow;
{$ELSE}
  OwnRow := TMemDBRow(OwnItem);
  OtherRow := TMemDBRow(OtherItem);
{$ENDIF}

  PMeta := PMemDBFKMeta(IndexTag);
  OwnS := OwnRow.GetNext(PMeta.Tid);
  OtherS := OtherRow.GetNext(PMeta.Tid);

  Assert(AssignedNotSentinel(OwnS));
  Assert(AssignedNotSentinel(OtherS));

  OwnFields := OwnS as TMemRowFields;
  OtherFields := OtherS as TMemRowFields;

  //Field offsets.
  //We can get here in various change situations, but not when the fields are sparse.
  Assert(not OwnFields.Sparse);
  Assert(not OtherFields.Sparse);

  Assert(FieldOffsetsSame(PMeta.FieldDefsReferredAbsIdx,
    PMeta.IndexReferred.FinalFieldOffsets));

  FieldCount := Length(PMeta.IndexReferred.FinalFieldOffsets);
  Assert(FieldCount > 0);

  result := 0;
  ff := 0;
  while (result = 0) and (ff < FieldCount) do
  begin
    OwnField := OwnFields.Items[PMeta.IndexReferred.FinalFieldOffsets[ff]];
    OtherField := OtherFields.Items[PMeta.IndexReferred.FinalFieldOffsets[ff]];
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
  end;
end;

{ TMemDBRowLookasideSearchVal }

function TMemDBRowLookasideSearchVal.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  OtherS: TMemDBStreamable;
  OtherFieldList: TMemRowFields;
  OtherRow: TMemDbRow;
  OtherField: TMemDBStreamable;
  ff, FieldCount: integer;
  PMeta: PMemDBFKMeta;

begin
  Assert(not Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OtherItem is TMemDBRow);

{$IFOPT C+}
  OtherRow := OtherItem as TMemDbRow;
{$ELSE}
  OtherRow := TMemDBRow(OtherItem);
{$ENDIF}

  PMeta := PMemDBFKMeta(IndexTag);
  OtherS := OtherRow.GetNext(PMeta.Tid);

  Assert(AssignedNotSentinel(OtherS));

  OtherFieldList := OtherS as TMemRowFields;

  //Field offsets.
  //We can get here in various change situations, but not when the fields are sparse.
  Assert(not OtherFieldList.Sparse);

  Assert(FieldOffsetsSame(PMeta.FieldDefsReferredAbsIdx,
    PMeta.IndexReferred.FinalFieldOffsets));

  FieldCount := Length(PMeta.IndexReferred.FinalFieldOffsets);
  Assert(FieldCount > 0);
  Assert(FieldCount = Length(FFieldSearchVals));

  result := 0;
  ff := 0;
  while (result = 0) and (ff < FieldCount) do
  begin
    OtherField := OtherFieldList.Items[PMeta.IndexReferred.FinalFieldOffsets[ff]];
    Assert(not (OtherField is TMemDeleteSentinel));
{$IFOPT C+}
    result := CompareFields(FFieldSearchVals[ff],
                            (OtherField as TMemFieldData).FDataRec);
{$ELSE}
    result := CompareFields(FFieldSearchVals[ff],
                            TMemFieldData(OtherField).FDataRec);
{$ENDIF}
    Inc(ff);
  end;
end;


end.
