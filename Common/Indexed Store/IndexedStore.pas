unit IndexedStore;
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

{
  This unit defines some classes to allow one to maintain an "indexed store",
  which consists of a store of objects and multiple indices into those
  objects for fast removal / searching etc.

  Updated Martin Harvey 2016.
}
interface

{$IFDEF USE_TRACKABLES}
{$UNDEF USE_TRACKABLES}
//Not in this unit unless changes made, thanks. Slows stuff down.
{$ENDIF}

uses BinaryTree, DLList, Trackables, Parallelizer;

{
  N.B. *VERY* Limited support in this unit for concurrency:

  - You can add or remove (but not both) indexes in parallel.
  - Assumption is that items are not added/removed when you're changing indexes.
  - No other concurrency control - for that you'll need to wrap the datastructure
    in a lock, or ask me to implement this in a cleverer manner using interlocked
    queue/list functions.

    Please turn assertions on to check for misuse of aforementioned concurrency.
    Additionally, we handle out of memory exceptions OK, but do not expect to
    encounter other exceptions in this unit.
}

const
  SPIN_COUNT = 200;

type
  TISRetVal = (rvOK,
    rvDuplicateTag,
    rvInvalidClassType,
    rvTagNotFound,
    rvInvalidItem,
    rvInvalidSearchVal,
    rvNotFound,
    rvInternalError,
    rvDuplicateKey,
    rvNilEvent,
    DEPRECATED_rvInsufficientResources, //Protect with exceptions.
    rvBadIndex,
    rvInvalidTag,
    rvAsyncAlready,
    rvAsyncInProgress
    );

  TItemRec = class;
  TIndexedStoreG = class;
  TIndexNodeLink = class;
  TSIndex = class;

  //Was tempted to put templates in here, but it then breaks the metaclassing,
  //so to keep our sanity, will avoid template use.
{$IF DEFINED(CPUX86)}
  TTagType = type Int64;
{$ELSEIF DEFINED(CPUX64)}
  TTagType = type NativeInt;
{$ELSE}
{$ERROR Set the tag type up to be at least as big as a ptr}
{$ENDIF}

  //Node in a particular index.
  TIndexNode = class(TBinTreeItem)
  private
    FIndexLink: TIndexNodeLink;
  protected
    //In some cases, we may need to get hold of the index link for one
    //or both nodes.
    property IndexLink: TIndexNodeLink read FIndexLink;
    function Compare(Other: TBinTreeItem; AllowKeyDedupe: boolean): integer; override; // data
    procedure CopyFrom(Source: TBinTreeItem); override; // data

   // other < self :-1  other=self :0  other > self :+1
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; virtual;
      abstract;
  public
{$IFOPT C+}
    class function ComparePointers(Own, Other: Pointer): integer;
{$ELSE}
    class function ComparePointers(Own, Other: Pointer): integer; inline;
{$ENDIF}
    constructor Create; virtual;
  end;
  TIndexNodeClass = class of TIndexNode;

  TDuplicateValIndexNode = class(TIndexNode)
    function Compare(Other: TBinTreeItem; AllowKeyDedupe: boolean): integer; override; // data
  end;

{$IFDEF USE_TRACKABLES}
  TIndexNodeLink = class(TTrackable)
{$ELSE}
  TIndexNodeLink = class
{$ENDIF}
  private
    FSiblingListEntryIdx: TDLEntry;
    FIndexNode: TIndexNode;
    FItemRec: TItemRec;
    FTSIndex: TSIndex;
  protected
    procedure IndexCopiedFrom(Dest, Source: TIndexNode);
    property SiblingListEntryIdx: TDLEntry read FSiblingListEntryIdx write FSiblingListEntryIdx;
    property IndexNode: TIndexNode read FIndexNode write FIndexNode;
    property ItemRec: TItemRec read FItemRec write FItemRec;
  public
    constructor Create;
    property RootIndex: TSIndex read FTSIndex;
  end;

  //Cross link that lets us get from an item to the nodes in each index
  //that reference it.
{$IFDEF USE_TRACKABLES}
  TItemRec = class(TTrackable)
{$ELSE}
  TItemRec = class
{$ENDIF}
  private
    FIndexNodesLock: integer;
    FIndexNodes: TDLEntry;
    FItem: TObject;
    FSiblingListEntryRec: TDLEntry;
    procedure SetItem(NewItem: TObject);
    function GetIndexLinkByRoot(RootIdx: TSIndex; UseLocks: boolean): TIndexNodeLink;
  protected
    //I need a really lightweight lock here - one of these is created for
    //every single DB row. Additionally, we expect the chances of conflict to be
    //low, and gets lower as dataset gets larger.
    procedure LockIndexNodeList; inline; //like that makes any difference with lock prefix...
    procedure UnlockIndexNodeList; inline;
  public
    constructor Create;
    property Item: TObject read FItem write SetItem;
  end;

  TIndexAsyncState = (iasNone, iasAsyncAdding, iasAsyncDeleting);

  //Entry in the master list of indices which allows us to reference
  //whether an index exists, and a bit about it.
{$IFDEF USE_TRACKABLES}
  TSIndex = class(TTrackable)
{$ELSE}
  TSIndex = class
{$ENDIF}
  private
    FRoot: TBinTree;
    FNodeClassType: TIndexNodeClass;
    FTag: TTagType;
    FSiblingListEntry: TDLEntry;
    FAsyncState: TIndexAsyncState;
  public
    constructor Create;
    destructor Destroy; override;
    property Root: TBinTree read FRoot;
    property NodeClassType: TIndexNodeClass read FNodeClassType write FNodeClassType;
    property Tag: TTagType read FTag write FTag;
    property SiblingListEntry: TDLEntry read FSiblingListEntry write FSiblingListEntry;
    property AsyncState: TIndexAsyncState read FAsyncState write FAsyncState;
  end;

  TStoreTraversalEvent = procedure(Store: TIndexedStoreG;
    Item: TItemRec) of object;

//Generic indexed store.
{$IFDEF USE_TRACKABLES}
  TIndexedStoreG = class(TTrackable)
{$ELSE}
  TIndexedStoreG = class
{$ENDIF}
  private
    //List of AVL tree indices.
    FIndices: TDLEntry;
    //count of items in tree.
    FCount: integer;
    //Doubly linked list of item records.
    FItemRecList: TDLEntry;
    FTraversalEvent: TStoreTraversalEvent;
  protected
    procedure AsyncIndexDeleteWorker(Index: TSIndex);
    function AsyncIndexAddWorker(NewIndex: TSIndex): TIsRetVal;
    procedure DeleteIndexTail(Index: TSIndex);
    procedure AddIndexCleanupTail(NewIndex: TSIndex);
    procedure DeleteIndexInternal(Index: TSIndex);

    function ParallelAddHandler(Ref1, Ref2: pointer): pointer;
    function ParallelDeleteHandler(Ref1, Ref2: pointer): pointer;

    function CreateItemRec(Item: TObject): TItemRec; virtual;
    function GetIndexByTag(Tag: TTagType): TSIndex;
    function AddIndexNodeForItem(Index: TSIndex; Item: TItemRec; UseLocks: boolean): TISRetVal;
    function DeleteIndexNodeForItem(Index: TSIndex; Item: TItemRec; UseLocks: boolean): TISRetVal;
    procedure TraversalHandler(Tree: TBinTree; Item: TBinTreeItem; Level:
      integer);
    function EdgeByIndex(IndexTag: TTagType; var Res: TItemRec; First: boolean): TISRetVal;
    function NeighbourByIndex(IndexTag:TTagType; var Res: TItemRec; Lower: boolean): TISRetVal;

    function _IndexInfoByOrdinal(Idx: integer;
                                var Tag: TTagType;
                                var IndNodeClassType: TIndexNodeClass):TISRetVal;
    function _HasIndex(Tag: TTagType): boolean;
    function _AddIndex(IndNodeClassType: TIndexNodeClass; Tag: TTagType; Async: boolean):
      TISRetVal;
    function _DeleteIndex(Tag: TTagType; Async: boolean): TISRetVal;
    function _AdjustIndexTag(OldTag, NewTag: TTagType): TISRetVal;
    function _FindByIndex(IndexTag: TTagType; SearchVal: TIndexNode; var Res:
      TItemRec): TISRetVal;
    function _FindNearByIndex(IndexTag: TTagType; SearchVal: TIndexNode; var Res:
      TItemRec): TISRetVal;
    function _TraverseByIndex(IndexTag: TTagType; Event: TStoreTraversalEvent;
      Forwards: boolean): TISRetVal;
    function _FirstByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
    function _LastByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
    function _NextByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
    function _PreviousByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;

  public
    constructor Create;
    function IndexCount: integer;

    function PerformAsyncActions(ExcHandlers: PExceptionHandlerChain): TIsRetVal;
    procedure ForceClearAsyncFlags;
    //To be used only in cases where exceptions in async ops,
    //and you'd like to serialise everything to clean up the mess.
    //Alloc failures handled, so most likely case is exceptions in index node
    //comparison.

    function AddItem(NewItem: TObject; var Res: TItemRec): TISRetVal;
    function AddItemInPlace(NewItem: TObject; const LPos: PDLEntry; var Res: TItemRec): TISRetVal;
    function RemoveItem(ItemRec: TItemRec): TISRetVal;
    function RemoveItemInPlace(ItemRec: TItemRec; var LPos:PDLEntry): TIsRetVal;
    function GetAnItem: TItemRec;
    function GetAnotherItem(var AnItem: TItemRec): TISRetVal;
    function GetLastItem: TItemRec;
    function GetPreviousItem(var AnItem: TItemRec): TIsRetVal;
    function GetAnotherItemWraparound(var AnItem: TItemRec): TISRetVal;

    destructor Destroy; override;
    procedure DeleteChildren;
    property Count: integer read FCount;
  end;

  //Yes, we could have templated these if it weren't for backward compatiblity
  //and sanity. Trade a bit more verbosity for compatibility and simplicity.

  //Indexed store with int tags (at least 64 bits wide)

  TIndexedStore = class(TIndexedStoreG)
  public
    constructor Create; deprecated;
    function IndexInfoByOrdinal(Idx: integer;
                                var Tag: TTagType;
                                var IndNodeClassType: TIndexNodeClass):TISRetVal; inline;
    function HasIndex(Tag: TTagType): boolean; inline;
    function AddIndex(IndNodeClassType: TIndexNodeClass; Tag: TTagType):TISRetVal; inline;
    function DeleteIndex(Tag: TTagType): TISRetVal; inline;
    function AdjustIndexTag(OldTag, NewTag: TTagType): TISRetVal; inline;
    function FindByIndex(IndexTag: TTagType; SearchVal: TIndexNode; var Res:
      TItemRec): TISRetVal; inline;
    function FindNearByIndex(IndexTag: TTagType; SearchVal: TIndexNode; var Res:
      TItemRec): TISRetVal; inline;
    function TraverseByIndex(IndexTag: TTagType; Event: TStoreTraversalEvent;
      Forwards: boolean): TISRetVal; inline;
    function FirstByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal; inline;
    function LastByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal; inline;
    function NextByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal; inline;
    function PreviousByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal; inline;
  end;

  //Indexed store with ptr tags
  TIndexedStoreO = class(TIndexedStoreG)
  private
    FAllowNullTags: boolean;
  public
    function IndexInfoByOrdinal(Idx: integer;
                                var Tag: pointer;
                                var IndNodeClassType: TIndexNodeClass):TISRetVal;
    function HasIndex(Tag: pointer): boolean;
    function AddIndex(IndNodeClassType: TIndexNodeClass; Tag: pointer; Async: boolean):TISRetVal;
    function DeleteIndex(Tag: pointer; Async: boolean): TISRetVal;
    function AdjustIndexTag(OldTag, NewTag: pointer): TISRetVal;
    function FindByIndex(IndexTag: pointer; SearchVal: TIndexNode; var Res:
      TItemRec): TISRetVal;
    function FindNearByIndex(IndexTag: pointer; SearchVal: TIndexNode; var Res:
      TItemRec): TISRetVal;
    function TraverseByIndex(IndexTag: pointer; Event: TStoreTraversalEvent;
      Forwards: boolean): TISRetVal;
    function FirstByIndex(IndexTag: pointer; var Res: TItemRec): TISRetVal;
    function LastByIndex(IndexTag: pointer; var Res: TItemRec): TISRetVal;
    function NextByIndex(IndexTag: pointer; var Res: TItemRec): TISRetVal;
    function PreviousByIndex(IndexTag: pointer; var Res: TItemRec): TISRetVal;

    property AllowNullTags: boolean read FAllowNulltags;
  end;
{
  And now some example index node classes, just to make things simple.
  For the purposes of speed, we have two separate classes:

  One which lives in the actual tree indexes, and always expects the first
  argument of compare items to be assigned.

  Another which should be created and passed in as a "FindByIndex" argument
  which knows that the first argument is likely to be NULL, and stores the
  search index locally, using the inherited comparison function.

  This then means we don't need to store space for an extra key in every item,
  especially if the key is big.

  However, if you want, you can wrap the two up into one class, simply by
  making the CompareItems function change behaviour depending on whether
  the first argument is assigned nor not.
}

  TPointerINode = class(TIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  end;

  TSearchPointerINode = class(TPointerINode)
  private
    FSearchVal: TObject;
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  public
    property SearchVal: TObject read FSearchVal write FSearchVal;
  end;

implementation

uses SysUtils, Windows;
//Use Windows CMPXCHG not TInterlocked, which is broken on XE4.

const
  S_CORRUPTED_INDEX =
    'Index node comparison bad: one or other is nil (or wrong type).';
  S_CORRUPTED_INDEX_REC =
    'Index node comparison bad: item rec is nil.';
  S_CORRUPTED_INDEX_LINK =
    'Index node comparison bad: item link is nil.';
  S_ITEMREC_INDEX_LINK_BROKEN =
    'Link INode copy notification has nil source or destination.';
  S_ITEMREC_INDEX_LINK_MISMATCH =
    'Link INode copy notification source does not match original INode.';
  S_ITEMREC_NO_ITEM =
    'ItemRec created without an underlying item.';
  S_ADD_INDEXNODE_PARAMS_NIL =
    'Adding index node for item - some required parameters are NIL';
  S_DEL_INDEXNODE_PARAMS_NIL =
    'Deleting index node for item - some required parameters are NIL';
  S_DEL_INDEXNODE_NO_LINK =
    'Deleting index node for item - internal link structure is missing.';
  S_CHANGE_INDEXNODE_NO_LINK =
    'Changing index node by tag - no link structure with correct tag.';
  S_CHANGE_INDEXNODE_DUP_LINK =
    'Changing index node by tag - already have link structure with this tag.';
  S_ITEM_ALREADY_INDEXED =
    'Adding index node for item - item already has an index node!';
  S_DEL_INDEXNODE_FAILED =
    'Deleting index node for item - couldn''t find index node to delete!';
  S_ADD_ITEM_NIL =
    'Adding itemrec - given nil rec.';
  S_DEL_ITEM_NIL =
    'Deleting itemrec - given nil rec.';
  S_DEL_INDEX_INTERNAL_UNINDEXED_ITEM =
    'Deleting an index, and found an item not attached to that index!';
  S_ITEM_HAS_LINK_TAG_WITH_NO_INDEX =
    'Deleting an item, and found and index link with no corresponding index!';
  S_ROLLBACK_ADD_INDEX_FAILED =
    'Trying to rollback failed add index, but couldn''t remove index items.';
  S_ROLLBACK_ADJUST_INDEX_FAILED =
    'Trying to rollback index tag change, but couldn''t undo previous change.';
  S_ROLLBACK_ADD_ITEM_FAILED =
    'Trying to rollback failed add item, but couldn''t remove index items.';
  S_ROLLBACK_ADD_ITEM_FAILED2 =
    'Trying to rollback failed add item, but couldn''t remove item records.';
  S_COUNT_CORRUPTED_AFTER_ADD =
    'Indexed store count corrupted after an addition operation.';
  S_COUNT_CORRUPTED_AFTER_DEL =
    'Indexed store count corrupted after a deletion operation.';
  S_ITEM_REC_FOUND_NO_ITEM =
    'A search operation found the link node, but it was not attached to an item.';
  S_INDEX_LINK_MISSING_ITEM_REC =
    'A search operation found the index node, but it was not attached to a link node.';
  S_TRAVERSAL_NO_FINAL_HANDLER =
    'Intermediate tree traversal handler was not given a final handler function.';
  S_TRAVERSAL_ITEM_BAD_TYPE =
    'Node encountered during tree traversal is not index node.';
  S_POINTERS_ODD_SIZE =
    'Can''t convert pointers to ordinal. What platform is this?';
  S_INODE_COMPARE_NIL_PTRS =
    'Pointer index node trying to compare nil pointers.';
  S_SEARCH_INODE_COMPARE_ASSG_PTRS =
    'Search pointer index node pointer wrongly assigned.';
  S_SEARCH_INODE_COMPARE_NIL_PTRS =
    'Search pointer index node trying to compare nil pointers.';
  S_ITEMREC_ITEM_CHANGED_WHILST_INDEXED =
    'Item property of Itemrec cannot be changed when item is indexed. Remove indexes, or remove and reinsert item.';
  S_NO_INDEX_LINK =
    'Store has an index, but the link node for a specific item could not be found.';
  S_NO_INODE_WITH_LINK =
    'Store found an index link node, but it was not attached to an index node.';
  S_NO_MEM_FORWARDED =
    'Out of memory exception forwarded from client thread';

(************************************
 * TItemRec                         *
 ************************************)

constructor TItemRec.Create;
begin
  inherited Create;
  DLItemInitObj(self, @FSiblingListEntryRec);
  DLItemInitList(@FIndexNodes);
end;

procedure TItemRec.LockIndexNodeList;
var
  Tries: integer;
begin
  while True do
  begin
    Tries := SPIN_COUNT;
    while Tries > 0 do
    begin
      if Windows.InterlockedCompareExchange(FIndexNodesLock, 1, 0) = 0 then
        exit;
      Dec(Tries);
    end;
    Sleep(0); //Yield.
  end;
end;

procedure TItemRec.UnlockIndexNodeList;
var
  PrevLocked: integer;
begin
  PrevLocked := Windows.InterlockedExchange(FIndexNodesLock, 0);
  Assert(PrevLocked = 1);
end;

//Lock in caller. Thank heavens this function does not worry
//about the order of the nodes in the list.
function TItemRec.GetIndexLinkByRoot(RootIdx: TSIndex; UseLocks: boolean): TIndexNodeLink;
begin
  Assert(UseLocks = (FIndexNodesLock = 1));
{$IFOPT C+}
  result := FIndexNodes.FLink.Owner as TIndexNodeLink;
{$ELSE}
  result := TIndexNodeLink(FIndexNodes.FLink.Owner);
{$ENDIF}
  while Assigned(result) do
  begin
    if result.RootIndex = RootIdx then
      exit;
{$IFOPT C+}
    result := result.FSiblingListEntryIdx.FLink.Owner as TIndexNodeLink;
{$ELSE}
    result := TIndexNodeLink(result.FSiblingListEntryIdx.FLink.Owner);
{$ENDIF}
  end;
end;

procedure TItemRec.SetItem(NewItem: TObject);
begin
{$IFOPT C+}
  LockIndexNodeList;
  Assert(DLList.DlItemIsEmpty(@FIndexNodes), S_ITEMREC_ITEM_CHANGED_WHILST_INDEXED);
  UnlockIndexNodeList;
{$ENDIF}
  FItem := NewItem;
end;

(************************************
 * TIndexNodeLink                   *
 ************************************)

constructor TIndexNodeLink.Create;
begin
  inherited;
  DLItemInitObj(self, @FSiblingListEntryIdx);
end;

procedure TIndexNodeLink.IndexCopiedFrom(Dest, Source: TIndexNode);
begin
  Assert(Assigned(Source), S_ITEMREC_INDEX_LINK_BROKEN);
  Assert(Assigned(Dest), S_ITEMREC_INDEX_LINK_BROKEN);
  Assert((IndexNode = Source), S_ITEMREC_INDEX_LINK_MISMATCH);
  IndexNode := Dest;
end;

(************************************
 * TSIndex                           *
 ************************************)

constructor TSIndex.Create;
begin
  inherited;
  FRoot := TBinTree.Create;
  DLItemInitObj(self, @FSiblingListEntry);
end;

destructor TSIndex.Destroy;
begin
  FRoot.Free;
  inherited;
end;

(************************************
 * TIndexedStoreG                   *
 ************************************)

constructor TIndexedStoreG.Create;
begin
  inherited;
  DLItemInitList(@FItemRecList);
  DLItemInitList(@FIndices);
end;

function TIndexedStoreG.CreateItemRec(Item: TObject): TItemRec;
begin
  Assert(Assigned(Item), S_ITEMREC_NO_ITEM);
  result := TItemRec.Create;
  if Assigned(result) then
    result.Item := Item;
end;

function TIndexedStoreG.GetIndexByTag(Tag: TTagType): TSIndex;
begin
{$IFOPT C+}
  result := self.FIndices.FLink.Owner as TSIndex;
{$ELSE}
  result := TSIndex(self.FIndices.FLink.Owner);
{$ENDIF}
  while Assigned(result) do
  begin
    if tag = result.Tag then
      exit;
{$IFOPT C+}
    result := result.FSiblingListEntry.FLink.Owner as TSIndex;
{$ELSE}
    result := TSIndex(result.FSiblingListEntry.FLink.Owner);
{$ENDIF}
  end;
  result := nil;
end;

function TIndexedStoreG.IndexCount: integer;
var
  Index: TSIndex;
begin
  result := 0;
{$IFOPT C+}
  Index := FIndices.FLink.Owner as TSIndex;
{$ELSE}
  Index := TSIndex(FIndices.FLink.Owner);
{$ENDIF}
  while Assigned(Index) do
  begin
    Inc(result);
{$IFOPT C+}
    Index := Index.FSiblingListEntry.FLink.Owner as TSIndex;
{$ELSE}
    Index := TSIndex(Index.FSiblingListEntry.FLink.Owner);
{$ENDIF}
  end;
end;

function TIndexedStoreG._IndexInfoByOrdinal(Idx: integer;
                            var Tag: TTagType;
                            var IndNodeClassType: TIndexNodeClass):TISRetVal;
var
  i: integer;
  Index: TSIndex;
begin
  if Idx < 0 then
  begin
    result := rvBadIndex;
    exit;
  end;
{$IFOPT C+}
  Index := FIndices.FLink.Owner as TSIndex;
{$ELSE}
  Index := TSIndex(FIndices.FLink.Owner);
{$ENDIF}
  i := 0;
  while (i < Idx) and Assigned(Index) do
  begin
{$IFOPT C+}
    Index := Index.FSiblingListEntry.FLink.Owner as TSIndex;
{$ELSE}
    Index := TSIndex(Index.FSiblingListEntry.FLink.Owner);
{$ENDIF}
    Inc(i);
  end;
  if not Assigned(Index) then
  begin
    result := rvBadIndex;
    exit;
  end;
  Tag := Index.Tag;
  IndNodeClassType := Index.NodeClassType;
  result := rvOK;
end;


function TIndexedStoreG._HasIndex(Tag: TTagType): boolean;
begin
  result := Assigned(GetIndexByTag(Tag));
end;

function TIndexedStoreG.AddIndexNodeForItem(Index: TSIndex; Item: TItemRec; UseLocks: boolean):TISRetVal;
var
  IndexNode: TIndexNode;
  IndexLink: TIndexNodeLink;
  InClassType: TIndexNodeClass;
begin
  if (not Assigned(Index)) or
    (not Assigned(Item)) then
  begin
    Assert(false, S_ADD_INDEXNODE_PARAMS_NIL);
    result := rvInternalError;
    exit;
  end;
  IndexLink := TIndexNodeLink.Create;
  //First exception, no special handling.

  InClassType := Index.NodeClassType;
  try
    IndexNode := InClassType.Create;
  except
    on E: EOutOfMemory do
    begin
      IndexLink.Free;
      raise;
    end;
  end;

  //No mem allocation for remainder of function,
  //expect no exceptions in INode comparison funcs.
  IndexNode.FIndexLink := IndexLink;
  IndexLink.IndexNode := IndexNode;
  IndexLink.ItemRec := Item;
  IndexLink.FTSIndex := Index;
  if Index.Root.Add(IndexNode) then
  begin
    result := rvOK;
    if UseLocks then
      Item.LockIndexNodeList;
    Assert(not Assigned(Item.GetIndexLinkByRoot(Index, UseLocks)), S_ITEM_ALREADY_INDEXED);
    DLListInsertTail(@Item.FIndexNodes, @IndexLink.SiblingListEntryIdx);
    if UseLocks then
      Item.UnlockIndexNodeList;
  end
  else
  begin
    result := rvDuplicateKey;
    IndexNode.Free;
    IndexLink.Free;
  end;
end;

function TIndexedStoreG.DeleteIndexNodeForItem(Index: TSIndex; Item: TItemRec; UseLocks: boolean):
  TISRetVal;
var
  Link: TIndexNodeLink;
  INode: TIndexNode;
begin
  if (not Assigned(Item)) or (not Assigned(Index)) then
  begin
    Assert(false, S_DEL_INDEXNODE_PARAMS_NIL);
    result := rvInternalError;
    exit;
  end;
  if UseLocks then
    Item.LockIndexNodeList;
  Link := Item.GetIndexLinkByRoot(Index, UseLocks);
  if not Assigned(Link) then
  begin
    if UseLocks then
      Item.UnlockIndexNodeList;

    Assert(false, S_DEL_INDEXNODE_NO_LINK);
    result := rvInternalError;
    exit;
  end;
  DLListRemoveObj(@Link.SiblingListEntryIdx);
  if UseLocks then
    Item.UnlockIndexNodeList;

  INode := Link.IndexNode;
  if Index.Root.Remove(INode) then
    result := rvOK
  else
  begin
    //N.B. Assertions here tend to indicate you've modified an
    //indexed field whilst the item in still in the list.
    Assert(false, S_DEL_INDEXNODE_FAILED);
    result := rvInternalError;
  end;
  Link.Free;
end;

procedure TIndexedStoreG.AsyncIndexDeleteWorker(Index: TSIndex);
var
  CurItem: TItemRec;
  Link: TIndexNodeLink;
  UseLocks: boolean;
begin
  UseLocks := Index.AsyncState <> iasNone;
  // Somewhat optimised way of deleting an index. O(n)
{$IFOPT C+}
  CurItem := Self.FItemRecList.FLink.Owner as TItemRec;
{$ELSE}
  CurItem := TItemRec(Self.FItemRecList.FLink.Owner);
{$ENDIF}
  while Assigned(CurItem) do
  begin
    if UseLocks then
      CurItem.LockIndexNodeList;
    Link := CurItem.GetIndexLinkByRoot(Index, UseLocks);
    Assert(Assigned(Link), S_DEL_INDEX_INTERNAL_UNINDEXED_ITEM);
    DLListRemoveObj(@Link.SiblingListEntryIdx);
    if UseLocks then
      CurItem.UnlockIndexNodeList;

    Link.Free;
{$IFOPT C+}
    CurItem := CurItem.FSiblingListEntryRec.FLink.Owner as TItemRec;
{$ELSE}
    CurItem := TItemRec(CurItem.FSiblingListEntryRec.FLink.Owner);
{$ENDIF}
  end;
end;

procedure TIndexedStoreG.DeleteIndexTail(Index: TSIndex);
begin
  DLListRemoveObj(@Index.SiblingListEntry);
  Index.Free;
end;

procedure TIndexedStoreG.AddIndexCleanupTail(NewIndex: TSIndex);
begin
  DLListRemoveObj(@NewIndex.SiblingListEntry);
  NewIndex.Free;
end;

procedure TIndexedStoreG.DeleteIndexInternal(Index: TSIndex);
begin
  Assert(Index.AsyncState = iasNone);
  //Executed synchronously.
  AsyncIndexDeleteWorker(Index);
  DeleteIndexTail(Index);
end;

function TIndexedStoreG.AsyncIndexAddWorker(NewIndex: TSIndex): TIsRetVal;

var
  CurItem: TItemRec;
  UseLocks: boolean;

  procedure Cleanup; //One index for all the items
  var
    res2: TIsRetVal;
  begin
    Assert(Assigned(CurItem));
    Assert(Assigned(NewIndex));
{$IFOPT C+}
    CurItem := CurItem.FSiblingListEntryRec.BLink.Owner as TItemRec;
{$ELSE}
    CurItem := TItemRec(CurItem.FSiblingListEntryRec.BLink.Owner);
{$ENDIF}
    while Assigned(CurItem) do
    begin
{$IFOPT C+}
      res2 := DeleteIndexNodeForItem(NewIndex, CurItem, UseLocks);
      Assert(res2 = rvOK, S_ROLLBACK_ADD_INDEX_FAILED);
{$ELSE}
      DeleteIndexNodeForItem(NewIndex, CurItem, UseLocks);
{$ENDIF}
{$IFOPT C+}
      CurItem := CurItem.FSiblingListEntryRec.BLink.Owner as TItemRec;
{$ELSE}
      CurItem := TItemRec(CurItem.FSiblingListEntryRec.BLink.Owner);
{$ENDIF}
    end;
  end;

begin
  //Now need to go through all items that exist, adding a new index item.
  //In case of failure, need to roll back all changes.
  result := rvOK;
  UseLocks := NewIndex.AsyncState <> iasNone;

{$IFOPT C+}
  CurItem := FItemRecList.FLink.Owner as TItemRec;
{$ELSE}
  CurItem := TItemRec(FItemRecList.FLink.Owner);
{$ENDIF}
  try
    while Assigned(CurItem) do
    begin
      result := AddIndexNodeForItem(NewIndex, CurItem, UseLocks);
      if result <> rvOK then break;
{$IFOPT C+}
      CurItem := CurItem.FSiblingListEntryRec.FLink.Owner as TItemRec;
{$ELSE}
      CurItem := TItemRec(CurItem.FSiblingListEntryRec.FLink.Owner);
{$ENDIF}
    end;
  except
    on E: EOutOfMemory do
    begin
      Cleanup;
      raise;
    end;
  end;

  //OK, all done!
  if result <> rvOK then
    Cleanup;
end;

function TIndexedStoreG._AddIndex(IndNodeClassType: TIndexNodeClass; Tag:TTagType; Async: boolean): TISRetVal;
var
  NewIndex: TSIndex;

begin
  NewIndex := TSIndex.Create;
  //First exception, no special handling.

  //Check tag not already represented.
  if Assigned(GetIndexByTag(Tag)) then
  begin
    result := rvDuplicateTag;
    NewIndex.Free;
    exit;
  end;
  //Check index node class is sensible.
  if not IndNodeClassType.InheritsFrom(TIndexNode) then
  begin
    result := rvInvalidClassType;
    NewIndex.Free;
    exit;
  end;
  NewIndex.NodeClassType := IndNodeClassType;
  NewIndex.Tag := Tag;
  //New index has created root object.
  DLListInsertTail(@FIndices, @NewIndex.SiblingListEntry);
  if Async then
  begin
    NewIndex.AsyncState :=iasAsyncAdding;
    result := rvOK;
  end
  else
  begin
    try
      result := AsyncIndexAddWorker(NewIndex); //Executed synchronously.
      if result <> rvOK then
        AddIndexCleanupTail(NewIndex);
    except
      on E:EOutOfMemory do
      begin
        AddIndexCleanupTail(NewIndex);
        raise;
      end;
    end;
  end;
end;

function TIndexedStoreG._DeleteIndex(Tag: TTagType; Async: boolean): TISRetVal;
var
  Index: TSIndex;
begin
  Index := GetIndexByTag(Tag);
  if not Assigned(Index) then
  begin
    result := rvTagNotFound;
    exit;
  end;
  case Index.AsyncState of
    iasNone: result := rvOK; //OK.
    iasAsyncAdding: result := rvAsyncInProgress;
    iasAsyncDeleting: begin
      if Async then
        result := rvAsyncAlready
      else
        result := rvAsyncInProgress;
    end;
  else
    Assert(false);
    result :=rvInternalError
  end;
  if result = rvOK then
  begin
    if Async then
      Index.AsyncState := iasAsyncDeleting
    else
      DeleteIndexInternal(Index);
  end;
end;

function TIndexedStoreG.ParallelAddHandler(Ref1, Ref2: pointer): pointer;
var
  Ret: TIsRetVal;
begin
  Ret := AsyncIndexAddWorker(TSIndex(Ref1));
  result := Pointer(Ret);
end;

function TIndexedStoreG.ParallelDeleteHandler(Ref1, Ref2: pointer): pointer;
begin
  AsyncIndexDeleteWorker(TSIndex(Ref1));
  result := Pointer(rvOK);
end;

function IndexedStoreSwallowENoMemExceptions(EClass: SysUtils.ExceptClass;
                                             EMsg: string): boolean;
begin
  result := (EClass = EOutOfMemory);
end;

function TIndexedStoreG.PerformAsyncActions(ExcHandlers: PExceptionHandlerChain): TIsRetVal;
var
  AsyncCount, AsyncIdx: integer;
  Index: TSIndex;
  Refs1,Refs2,Rets: TPHRefs;
  Handlers: TParallelHandlers;
  Excepts: TPHExcepts;
  LocalExcHander: TExceptionHandlerChain;

begin
  result := rvOK;
  AsyncCount := 0;
{$IFOPT C+}
  Index := FIndices.FLink.Owner as TSIndex;
{$ELSE}
  Index := TSIndex(FIndices.FLink.Owner);
{$ENDIF}
  while Assigned(Index) do
  begin
    if (Index.AsyncState <> iasNone) then
      Inc(AsyncCount);
{$IFOPT C+}
    Index := Index.FSiblingListEntry.FLink.Owner as TSIndex;
{$ELSE}
    Index := TSIndex(Index.FSiblingListEntry.FLink.Owner);
{$ENDIF}
  end;
  if AsyncCount = 0 then
    exit;
  LocalExcHander.Func := IndexedStoreSwallowENoMemExceptions;
  LocalExcHander.Next := ExcHandlers;
  SetLength(Refs1, AsyncCount);
  //SetLength(Refs2, AsyncCount);
  SetLength(Rets, AsyncCount);
  SetLength(Handlers, AsyncCount);
  SetLength(Excepts, AsyncCount);

  AsyncIdx := 0;
{$IFOPT C+}
  Index := FIndices.FLink.Owner as TSIndex;
{$ELSE}
  Index := TSIndex(FIndices.FLink.Owner);
{$ENDIF}
  while Assigned(Index) do
  begin
    if (Index.AsyncState <> iasNone) then
    begin
      Refs1[AsyncIdx] := Index;
      case Index.AsyncState of
        iasAsyncAdding: Handlers[AsyncIdx] := ParallelAddHandler;
        iasAsyncDeleting: Handlers[AsyncIdx] := ParallelDeleteHandler;
      else
        Assert(false);
        result := rvInternalError;
        exit;
      end;
      Inc(AsyncIdx);
    end;
{$IFOPT C+}
    Index := Index.FSiblingListEntry.FLink.Owner as TSIndex;
{$ELSE}
    Index := TSIndex(Index.FSiblingListEntry.FLink.Owner);
{$ENDIF}
  end;
  Assert(AsyncIdx = AsyncCount);
  Parallelizer.ExecParallel(Handlers, Refs1, Refs2, Excepts, Rets, @LocalExcHander);
  //Now run the tiny little tail func for successful deletions,
  //and failed additions, and once that's happened re-raise if necessary.
  //Only exceptions we expect to handle explicitly here are ENoMem, everything
  //else gets forwarded thru.
  for AsyncIdx := 0 to Pred(AsyncCount) do
  begin
    Index := Refs1[AsyncIdx];
    case Index.AsyncState of
      iasNone: Assert(false);
      iasAsyncAdding: begin
        //Check for failed ret, or out of memory.
        if (TIsRetVal(Rets[AsyncIdx]) <> rvOK) or (Excepts[AsyncIdx] = EOutOfMemory) then
          self.AddIndexCleanupTail(Index);
      end;
      iasAsyncDeleting: begin
        Assert(TIsRetVal(Rets[AsyncIdx]) = rvOK);
        self.DeleteIndexTail(Index);
      end;
    else
      Assert(false);
    end;
    Index.AsyncState := iasNone;
  end;
  for AsyncIdx := 0 to Pred(AsyncCount) do
  begin
    if Excepts[AsyncIdx] = EOutOfMemory then
      raise EOutOfMemory.Create(S_NO_MEM_FORWARDED);
  end;
  //Exceptions get forwarded as expected. Now collate return codes.
  for AsyncIdx := 0 to Pred(AsyncCount) do
  begin
    if TIsRetVal(Rets[AsyncIdx]) <> rvOK then
    begin
      result := TIsRetVal(Rets[AsyncIdx]);
      exit;
    end;
  end;
end;

procedure TIndexedStoreG.ForceClearAsyncFlags;
var
  Index: TSIndex;
begin
  //No checking here.
  Index := FIndices.FLink.Owner as TSIndex;
  while Assigned(Index) do
  begin
    Index.AsyncState := iasNone;
    Index := Index.FSiblingListEntry.FLink.Owner as TSIndex;
  end;
end;

function TIndexedStoreG._AdjustIndexTag(OldTag, NewTag: TTagType): TISRetVal;
var
  Index: TSIndex;
begin
  Index := GetIndexByTag(OldTag);
  if not Assigned(Index) then
  begin
    result := rvTagNotFound;
    exit;
  end;
  if Assigned(GetIndexByTag(NewTag)) then
  begin
    result := rvDuplicateTag;
    exit;
  end;
  if Index.AsyncState <> iasNone then
  begin
    result := rvAsyncInProgress;
    exit;
  end;
  Index.Tag := NewTag;
  result := rvOK;
  //OK, all done!
end;

function TIndexedStoreG.AddItem(NewItem: TObject; var Res: TItemRec): TISRetVal;
begin
  result := AddItemInPlace(NewItem, nil, Res);
end;

function TIndexedStoreG.AddItemInPlace(NewItem: TObject; const LPos: PDLEntry; var Res: TItemRec): TISRetVal;
var
  Index: TSIndex;
{$IFOPT C+}
  FailRet: TISRetVal;
{$ENDIF}

  procedure Cleanup; //All Indexes for the one item.
  begin
    Assert(Assigned(Index));
    Assert(Assigned(Res));
{$IFOPT C+}
    Index := Index.SiblingListEntry.BLink.Owner as TSIndex;
{$ELSE}
    Index := TSIndex(Index.SiblingListEntry.BLink.Owner);
{$ENDIF}
    while Assigned(Index) do
    begin
{$IFOPT C+}
      Failret := DeleteIndexNodeForItem(Index, Res, false);
      Assert(Failret = rvOK, S_ROLLBACK_ADD_ITEM_FAILED);
{$ELSE}
      DeleteIndexNodeForItem(Index, Res, false);
{$ENDIF}
{$IFOPT C+}
      Index := Index.SiblingListEntry.BLink.Owner as TSIndex;
{$ELSE}
      Index := TSIndex(Index.SiblingListEntry.BLink.Owner);
{$ENDIF}
    end;
    DLListRemoveObj(@Res.FSiblingListEntryRec);
    Res.Free;
    Res := nil;
  end;

begin
  if not Assigned(NewItem) then
  begin
    result := rvInvalidItem;
    exit;
  end;
  //Create the item.
  Res := CreateItemRec(NewItem);
  //No exception handler here, first allocation, can fall straight out.

  //Try to add the item to the list.
  if Assigned(LPos) then
    DlItemInsertAfter(LPos, @Res.FSiblingListEntryRec)
  else
    DLListInsertTail(@FItemRecList, @Res.FSiblingListEntryRec);

  //For all the indices that exist try and add, protect with exception handler
  //for out of memory cases.
  result := rvOK;
{$IFOPT C+}
  Index := FIndices.FLink.Owner as TSIndex;
{$ELSE}
  Index := TSIndex(FIndices.FLink.Owner);
{$ENDIF}
  try
    while Assigned(Index) do
    begin
      if Index.AsyncState <> iasNone then
      begin
        result := rvAsyncInProgress;
        break;
      end;
      result := AddIndexNodeForItem(Index, Res, false);
      if result <> rvOK then break;
{$IFOPT C+}
      Index := Index.SiblingListEntry.FLink.Owner as TSIndex;
{$ELSE}
      Index := TSIndex(Index.SiblingListEntry.FLink.Owner);
{$ENDIF}
    end;
  except
    on E: EOutOfMemory do
    begin
      Cleanup; //Prev indices which were not async.
      raise;
    end;
  end;

  //If fail, then unindex and remove.
  if result <> rvOK then
    Cleanup
  else
    Inc(FCount);

  Assert(DlItemIsEmpty(@FItemRecList) = (FCount = 0), S_COUNT_CORRUPTED_AFTER_ADD);
end;

function TIndexedStoreG.RemoveItem(ItemRec: TItemRec): TISRetVal;
var
  LPos: PDLEntry;
begin
  result := RemoveItemInPlace(ItemRec, LPos);
end;

function TIndexedStoreG.RemoveItemInPlace(ItemRec: TItemRec; var LPos:PDLEntry): TIsRetVal;
var
  Index: TSIndex;
  CallRes: TISRetVal;
begin
  //Rollbacks not easily possible here, so just keep going as best we can
  //but accumulate error flag.
  if not Assigned(ItemRec) then
  begin
    result := rvInvalidItem;
    exit;
  end;
  result := rvOK;

  //Refactor to go via main index list first, like all the other functions.
  //Check none of them async before deleting any (sorry, this slows things down).

  //Hence quicker (if we want to clear everything) to delete indices before deleting items.
{$IFOPT C+}
  Index := FIndices.FLink.Owner as TSIndex;
{$ELSE}
  Index := TSIndex(FIndices.FLink.Owner);
{$ENDIF}
  while Assigned(Index) do
  begin
    if Index.AsyncState <> iasNone then
    begin
      result := rvAsyncInProgress;
      exit;
    end;
{$IFOPT C+}
    Index := Index.SiblingListEntry.FLink.Owner as TSIndex;
{$ELSE}
    Index := TSIndex(Index.SiblingListEntry.FLink.Owner);
{$ENDIF}
  end;

{$IFOPT C+}
  Index := FIndices.FLink.Owner as TSIndex;
{$ELSE}
  Index := TSIndex(FIndices.FLink.Owner);
{$ENDIF}
  while Assigned(Index) do
  begin
    CallRes := DeleteIndexNodeForItem(Index, ItemRec, false);
    if (result = rvOK) and (CallRes <> rvOK) then
      result := CallRes;
{$IFOPT C+}
    Index := Index.SiblingListEntry.FLink.Owner as TSIndex;
{$ELSE}
    Index := TSIndex(Index.SiblingListEntry.FLink.Owner);
{$ENDIF}
  end;
  //No need to lock/unlock here.
{$IFOPT C+}
  Assert(DlItemIsEmpty(@ItemRec.FIndexNodes));
{$ENDIF}
  LPos := ItemRec.FSiblingListEntryRec.BLink;
  DLListRemoveObj(@ItemRec.FSiblingListEntryRec);
  ItemRec.Free;
  Dec(FCount);
  Assert(DlItemIsEmpty(@FItemRecList) = (FCount = 0), S_COUNT_CORRUPTED_AFTER_DEL);
end;

function TIndexedStoreG._FindByIndex(IndexTag: TTagType;
  SearchVal: TIndexNode; var Res: TItemRec): TISRetVal;
var
  Index: TSIndex;
  ResNode: TIndexNode;
  Link: TIndexNodeLink;
begin
  Res := nil;
  Index := GetIndexByTag(IndexTag);
  if not Assigned(Index) then
  begin
    result := rvTagNotFound;
    exit;
  end;
  if Index.AsyncState <> iasNone then
  begin
    result := rvAsyncInProgress;
    exit;
  end;
  if not (Assigned(SearchVal)
    and (SearchVal is Index.NodeClassType)) then
  begin
    result := rvInvalidSearchVal;
    exit;
  end;
  if Assigned(SearchVal.FIndexLink) then
  begin
    result := rvInvalidSearchVal;
    exit;
  end;
  ResNode := TIndexNode(Index.Root.Search(SearchVal));
  if Assigned(ResNode) then
  begin
    Link := ResNode.FIndexLink;
    if not Assigned(Link) then
    begin
      Assert(false, S_INDEX_LINK_MISSING_ITEM_REC);
      result := rvInternalError;
      exit;
    end;
    Res := Link.ItemRec;
    if not Assigned(Res) then
    begin
      Assert(false, S_ITEM_REC_FOUND_NO_ITEM);
      result := rvInternalError;
      exit;
    end;
    result := rvOK;
  end
  else
  begin
    Res := nil;
    result := rvNotFound;
  end;
end;

//A bit of code duplication, but ... whatever (don't touch the working code!).
function TIndexedStoreG._FindNearByIndex(IndexTag: TTagType; SearchVal: TIndexNode; var Res:
  TItemRec): TISRetVal;
var
  Index: TSIndex;
  ResNode: TIndexNode;
  Link: TIndexNodeLink;
begin
  Res := nil;
  Index := GetIndexByTag(IndexTag);
  if not Assigned(Index) then
  begin
    result := rvTagNotFound;
    exit;
  end;
  if Index.AsyncState <> iasNone then
  begin
    result := rvAsyncInProgress;
    exit;
  end;
  if not (Assigned(SearchVal)
    and (SearchVal is Index.NodeClassType)) then
  begin
    result := rvInvalidSearchVal;
    exit;
  end;
  if Assigned(SearchVal.FIndexLink) then
  begin
    result := rvInvalidSearchVal;
    exit;
  end;
  ResNode := TIndexNode(Index.Root.SearchNear(SearchVal));
  if Assigned(ResNode) then
  begin
    Link := ResNode.FIndexLink;
    if not Assigned(Link) then
    begin
      Assert(false, S_INDEX_LINK_MISSING_ITEM_REC);
      result := rvInternalError;
      exit;
    end;
    Res := Link.ItemRec;
    if not Assigned(Res) then
    begin
      Assert(false, S_ITEM_REC_FOUND_NO_ITEM);
      result := rvInternalError;
      exit;
    end;
    result := rvOK;
  end
  else
  begin
    Res := nil;
    result := rvNotFound;
  end;
end;

function TIndexedStoreG._TraverseByIndex(IndexTag: TTagType; Event:
  TStoreTraversalEvent; Forwards: boolean): TISRetVal;
var
  Order: TBinTraversalOrder;
  Index: TSIndex;
begin
  Index := GetIndexByTag(IndexTag);
  if not Assigned(Index) then
  begin
    result := rvTagNotFound;
    exit;
  end;
  if Index.AsyncState <> iasNone then
  begin
    result := rvAsyncInProgress;
    exit;
  end;
  if not Assigned(Event) then
  begin
    result := rvNilEvent;
    exit;
  end;
  FTraversalEvent := Event;
  if Forwards then
    Order := btoInOrderLoHi
  else
    Order := btoInOrderHiLo;
  Index.Root.Traverse(TraversalHandler, Order);
  result := rvOK;
end;

function TIndexedStoreG.EdgeByIndex(IndexTag: TTagType; var Res: TItemRec; First: boolean): TISRetVal;
var
  Index: TSIndex;
  Link: TIndexNodeLink;
  ResNode: TIndexNode;
begin
  Res := nil;
  Index := GetIndexByTag(IndexTag);
  if not Assigned(Index) then
  begin
    result := rvTagNotFound;
    exit;
  end;
  if Index.AsyncState <> iasNone then
  begin
    result := rvAsyncInProgress;
    exit;
  end;
  if First then
  begin
{$IFOPT C+}
    ResNode := Index.Root.First as TIndexNode;
{$ELSE}
    ResNode := TIndexNode(Index.Root.First);
{$ENDIF}
  end
  else
  begin
{$IFOPT C+}
    ResNode := Index.Root.Last as TIndexNode;
{$ELSE}
    ResNode := TIndexNode(Index.Root.Last);
{$ENDIF}
  end;
  if Assigned(ResNode) then
  begin
    Link := ResNode.FIndexLink;
    if not Assigned(Link) then
    begin
      Assert(false, S_INDEX_LINK_MISSING_ITEM_REC);
      result := rvInternalError;
      exit;
    end;
    Res := Link.ItemRec;
    if not Assigned(Res) then
    begin
      Assert(false, S_ITEM_REC_FOUND_NO_ITEM);
      result := rvInternalError;
      exit;
    end;
    result := rvOK;
  end
  else
  begin
    Res := nil;
    result := rvNotFound;
  end;
end;

function TIndexedStoreG._FirstByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
begin
  result := EdgeByIndex(IndexTag, Res, true);
end;

function TIndexedStoreG._LastByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
begin
  result := EdgeByIndex(IndexTag, Res, false);
end;

function TIndexedStoreG.NeighbourByIndex(IndexTag: TTagType; var Res: TItemRec; Lower: boolean): TISRetVal;
var
  Index: TSIndex;
  Link: TIndexNodeLink;
  ResNode: TIndexNode;

begin
  if not Assigned(Res) then
  begin
    result := rvInvalidItem;
    exit;
  end;
  Index := GetIndexByTag(IndexTag);
  if not Assigned(Index) then
  begin
    result := rvTagNotFound;
    exit;
  end;
  if Index.AsyncState <> iasNone then
  begin
    result := rvAsyncInProgress;
    exit;
  end;
  Link := Res.GetIndexLinkByRoot(Index, false);
  if not Assigned(Link) then
  begin
    Assert(false, S_NO_INDEX_LINK);
    result := rvInternalError;
    exit;
  end;
  if not Assigned(Link.IndexNode) then
  begin
    Assert(false, S_NO_INODE_WITH_LINK);
    result := rvInternalError;
    exit;
  end;
{$IFOPT C+}
  ResNode := Index.Root.NeighbourNode(Link.IndexNode, Lower) as TIndexNode;
{$ELSE}
  ResNode := TIndexNode(Index.Root.NeighbourNode(Link.IndexNode, Lower));
{$ENDIF}
  if not Assigned(ResNode) then
  begin
    Res := nil;
    result := rvNotFound;
    exit;
  end;
  //And now go back thru the link etc to the new ItemRec.
  Link := ResNode.FIndexLink;
  if not Assigned(Link) then
  begin
    Assert(false, S_INDEX_LINK_MISSING_ITEM_REC);
    result := rvInternalError;
    exit;
  end;
  Res := Link.ItemRec;
  if not Assigned(Res) then
  begin
    Assert(false, S_ITEM_REC_FOUND_NO_ITEM);
    result := rvInternalError;
    exit;
  end;
  result := rvOK;
end;

function TIndexedStoreG._NextByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
begin
  result := NeighbourByIndex(IndexTag, Res, false);
end;

function TIndexedStoreG._PreviousByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
begin
  result := NeighbourByIndex(IndexTag, res, true);
end;

procedure TIndexedStoreG.TraversalHandler(Tree: TBinTree; Item: TBinTreeItem;
  Level: integer);
var
  Link: TIndexNodeLink;
  Res: TItemRec;
begin
  Assert(Assigned(FTraversalEvent), S_TRAVERSAL_NO_FINAL_HANDLER);
  Assert(Item is TIndexNode, S_TRAVERSAL_ITEM_BAD_TYPE);
{$IFOPT C+}
  Link := (Item as TIndexNode).IndexLink;
{$ELSE}
  Link := TIndexNode(Item).IndexLink;
{$ENDIF}
  Assert(Assigned(Link), S_INDEX_LINK_MISSING_ITEM_REC);
  Res := Link.ItemRec;
  Assert(Assigned(Res), S_ITEM_REC_FOUND_NO_ITEM);
  FTraversalEvent(self, Res);
end;

function TIndexedStoreG.GetAnItem: TItemRec;
begin
{$IFOPT C+}
  result := self.FItemRecList.FLink.Owner as TItemRec;
{$ELSE}
  result := TItemRec(self.FItemRecList.FLink.Owner);
{$ENDIF}
end;

function TIndexedStoreG.GetAnotherItem(var AnItem: TItemRec): TISRetVal;
begin
  if not Assigned(AnItem) then
  begin
    result := rvInvalidItem;
    exit;
  end;
{$IFOPT C+}
  AnItem := AnItem.FSiblingListEntryRec.FLink.Owner as TItemRec;
{$ELSE}
  AnItem := TItemRec(AnItem.FSiblingListEntryRec.FLink.Owner);
{$ENDIF}
  if Assigned(AnItem) then
    result := rvOK
  else
    result := rvNotFound;
end;

function TIndexedStoreG.GetLastItem: TItemRec;
begin
{$IFOPT C+}
  result := self.FItemRecList.BLink.Owner as TItemRec;
{$ELSE}
  result := TItemRec(self.FItemRecList.BLink.Owner);
{$ENDIF}
end;

function TIndexedStoreG.GetPreviousItem(var AnItem: TItemRec): TIsRetVal;
begin
  if not Assigned(AnItem) then
  begin
    result := rvInvalidItem;
    exit;
  end;
{$IFOPT C+}
  AnItem := AnItem.FSiblingListEntryRec.BLink.Owner as TItemRec;
{$ELSE}
  AnItem := TItemRec(AnItem.FSiblingListEntryRec.BLink.Owner);
{$ENDIF}
  if Assigned(AnItem) then
    result := rvOK
  else
    result := rvNotFound;
end;

function TIndexedStoreG.GetAnotherItemWraparound(var AnItem: TItemRec): TISRetVal;
var
  NEnt: PDLEntry;
begin
  if not Assigned(AnItem) then
  begin
    result := rvInvalidItem;
    exit;
  end;
  result := rvOK;
  NEnt := AnItem.FSiblingListEntryRec.FLink;
{$IFOPT C+}
  AnItem := NEnt.Owner as TItemRec;
{$ELSE}
  AnItem := TItemRec(NEnt.Owner);
{$ENDIF}
  if not Assigned(AnItem) then
  begin
    NEnt := NEnt.FLink;
{$IFOPT C+}
    AnItem := NEnt.Owner as TItemRec;
{$ELSE}
    AnItem := TItemRec(NEnt.Owner);
{$ENDIF}
  end;
end;

procedure TIndexedStoreG.DeleteChildren;
var
  Rec: TItemRec;
  Obj: TObject;
begin
  Rec := GetAnItem;
  while Assigned(Rec) do
  begin
    Obj := Rec.FItem;
    RemoveItem(Rec);
    Obj.Free;
    Rec := GetAnItem;
  end;
end;

destructor TIndexedStoreG.Destroy;
var
  Index: TSIndex;
  Rec: TItemRec;
begin
{$IFOPT C+}
  Index := FIndices.FLink.Owner as TSIndex;
{$ELSE}
  Index := TSIndex(FIndices.FLink.Owner);
{$ENDIF}
  while Assigned(Index) do
  begin
    DeleteIndexInternal(Index);
{$IFOPT C+}
    Index := FIndices.FLink.Owner as TSIndex;
{$ELSE}
    Index := TSIndex(FIndices.FLink.Owner);
{$ENDIF}
  end;
  while not DlItemIsEmpty(@FItemRecList) do
  begin
{$IFOPT C+}
    Rec := FItemRecList.FLink.Owner as TItemRec;
{$ELSE}
    Rec := TItemRec(FItemRecList.FLink.Owner);
{$ENDIF}
    DLListRemoveObj(@Rec.FSiblingListEntryRec);
    Rec.Free;
  end;
  inherited;
end;

(************************************
 * TIndexNode                       *
 ************************************)

constructor TIndexNode.Create;
begin
  inherited;
  //Need this constructor, because virtual,
  //because variable class type creation used here.
end;

class function TIndexNode.ComparePointers(Own, Other: Pointer): integer;
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
    Assert(false, S_POINTERS_ODD_SIZE);
end;

function TIndexNode.Compare(Other: TBinTreeItem; AllowKeyDedupe: boolean): integer;
var
  OtherNode: TIndexNode;
  OtherLink: TIndexNodeLink;
  OwnRec, OtherRec: TItemRec;
  IndexTag: TTagType;
begin
  Assert(Assigned(Other) and (Other is TIndexNode), S_CORRUPTED_INDEX);
{$IFOPT C+}
  OtherNode := Other as TIndexNode;
{$ELSE}
  OtherNode := TIndexNode(Other);
{$ENDIF}
  OtherLink := OtherNode.IndexLink;
  Assert(Assigned(OtherLink), S_CORRUPTED_INDEX_LINK);
  IndexTag := OtherLink.RootIndex.Tag;
  OtherRec := OtherLink.ItemRec;
  Assert(Assigned(OtherRec), S_CORRUPTED_INDEX_REC);
  if Assigned(IndexLink) then
  begin
    Assert(IndexLink.RootIndex = OtherLink.RootIndex);
    OwnRec := IndexLink.ItemRec;
    Assert(Assigned(OwnRec), S_CORRUPTED_INDEX_REC);
  end
  else
    OwnRec := nil;
  if Assigned(OwnRec) then
    result := CompareItems(OwnRec.Item, OtherRec.Item, IndexTag, OtherNode)
  else
    result := CompareItems(nil, OtherRec.Item, IndexTag, OtherNode);
end;

procedure TIndexNode.CopyFrom(Source: TBinTreeItem);
var
  SourceIndexNode: TIndexNode;
begin
  Assert(Assigned(Source) and (Source is TIndexNode), S_CORRUPTED_INDEX);
  SourceIndexNode := TIndexNode(Source);
  Assert(Assigned(IndexLink), S_CORRUPTED_INDEX_LINK);
  Assert(Assigned(SourceIndexNode.IndexLink), S_CORRUPTED_INDEX_LINK);
  FIndexLink := SourceIndexNode.IndexLink;
  IndexLink.IndexCopiedFrom(self, SourceIndexNode);
end;

(************************************
 * TDuplicateValIndexNode           *
 ************************************)

function TDuplicateValIndexNode.Compare(Other: TBinTreeItem; AllowKeyDedupe: boolean): integer;
var
  OtherLink: TIndexNodeLink;
  MyRec, OtherRec: TItemRec;
begin
  Assert(Assigned(Other) and (Other is TDuplicateValIndexNode), S_CORRUPTED_INDEX);
  result := inherited Compare(Other, AllowKeyDedupe);
  if (result = 0) and AllowKeyDedupe then
  begin
    if Assigned(IndexLink) then
    begin
      MyRec := IndexLink.ItemRec;
      Assert(Assigned(MyRec), S_CORRUPTED_INDEX_REC);
{$IFOPT C+}
      OtherLink := (Other as TDuplicateValIndexNode).IndexLink;
{$ELSE}
      OtherLink := TDuplicateValIndexNode(Other).IndexLink;
{$ENDIF}
      Assert(Assigned(OtherLink), S_CORRUPTED_INDEX_LINK);
      OtherRec := OtherLink.ItemRec;
      Assert(Assigned(OtherRec), S_CORRUPTED_INDEX_REC);
      Assert(Assigned(MyRec.Item));
      Assert(Assigned(OtherRec.Item));
      result := ComparePointers(MyRec.Item, OtherRec.Item);
    end;
  end;
  //Else if not assigned FItemRec, then is a created search val, and we
  //just need it to match something in the tree.
end;

(************************************
 * TPointerINode                    *
 ************************************)

function TPointerINode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
begin
  Assert(Assigned(OwnItem), S_INODE_COMPARE_NIL_PTRS);
  Assert(Assigned(OtherItem), S_INODE_COMPARE_NIL_PTRS);
  result := ComparePointers(OwnItem, OtherItem);
end;

(************************************
 * TSearchPointerINode              *
 ************************************)

function TSearchPointerINode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
begin
  Assert(not Assigned(OwnItem), S_SEARCH_INODE_COMPARE_ASSG_PTRS);
  Assert(Assigned(OtherItem), S_SEARCH_INODE_COMPARE_NIL_PTRS);
  result := inherited CompareItems(FSearchVal, OtherItem, IndexTag, OtherNode);
end;


(************************************
 * TIndexedStore                    *
 ************************************)

constructor TIndexedStore.Create;
begin
  inherited;
end;

function TIndexedStore.IndexInfoByOrdinal(Idx: integer;
                            var Tag: TTagType;
                            var IndNodeClassType: TIndexNodeClass):TISRetVal;
begin
  result := _IndexInfoByOrdinal(Idx, Tag, IndNodeClassType);
end;

function TIndexedStore.HasIndex(Tag: TTagType): boolean;
begin
  result := _HasIndex(Tag);
end;

function TIndexedStore.AddIndex(IndNodeClassType: TIndexNodeClass; Tag: TTagType):
  TISRetVal;
begin
  result := _AddIndex(IndNodeClassType, Tag, false);
end;

function TIndexedStore.DeleteIndex(Tag: TTagType): TISRetVal;
begin
  result := _DeleteIndex(Tag, false);
end;

function TIndexedStore.AdjustIndexTag(OldTag, NewTag: TTagType): TISRetVal;
begin
  result := _AdjustIndexTag(OldTag, NewTag);
end;

function TIndexedStore.FindByIndex(IndexTag: TTagType; SearchVal: TIndexNode; var Res:
  TItemRec): TISRetVal;
begin
  result := _FindByIndex(IndexTag, SearchVal, Res);
end;

function TIndexedStore.FindNearByIndex(IndexTag: TTagType; SearchVal: TIndexNode; var Res:
  TItemRec): TISRetVal;
begin
  result := _FindNearByIndex(IndexTag, SearchVal, Res);
end;

function TIndexedStore.TraverseByIndex(IndexTag: TTagType; Event: TStoreTraversalEvent;
  Forwards: boolean): TISRetVal;
begin
  result := _TraverseByIndex(IndexTag, Event, Forwards);
end;

function TIndexedStore.FirstByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
begin
  result := _FirstByIndex(IndexTag, Res);
end;

function TIndexedStore.LastByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
begin
  result := _LastByIndex(IndexTag, Res);
end;

function TIndexedStore.NextByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
begin
  result := _NextByIndex(IndexTag, Res);
end;

function TIndexedStore.PreviousByIndex(IndexTag: TTagType; var Res: TItemRec): TISRetVal;
begin
  result := _PreviousByIndex(IndexTag, Res);
end;

(************************************
 * TIndexedStore                    *
 ************************************)

function TIndexedStoreO.IndexInfoByOrdinal(Idx: integer;
                            var Tag: pointer;
                            var IndNodeClassType: TIndexNodeClass):TISRetVal;
var
  OrdTag, BitsLost: TTagType;

begin
  if not (FAllowNullTags or Assigned(Tag)) then
    result := rvInvalidTag
  else
  begin
    result := _IndexInfoByOrdinal(Idx, OrdTag, IndNodeClassType);
    Tag := pointer(OrdTag);
    BitsLost := OrdTag and (not TTagType(Tag));
    Assert(BitsLost = 0);
  end;
end;

function TIndexedStoreO.HasIndex(Tag: pointer): boolean;
begin
  if not (FAllowNullTags or Assigned(Tag)) then
  begin
    Assert(false);
    result := false;
  end
  else
  begin
    Assert(sizeof(Tag) <= sizeof(TTagType));
    result := _HasIndex(TTagType(Tag));
  end;
end;

function TIndexedStoreO.AddIndex(IndNodeClassType: TIndexNodeClass; Tag: pointer; Async: boolean):TISRetVal;
begin
  if not (FAllowNullTags or Assigned(Tag)) then
    result := rvInvalidTag
  else
  begin
    Assert(sizeof(Tag) <= sizeof(TTagType));
    result := _AddIndex(IndNodeClassType, TTagType(Tag), Async);
  end;
end;

function TIndexedStoreO.DeleteIndex(Tag: pointer; Async: boolean): TISRetVal;
begin
  if not (FAllowNullTags or Assigned(Tag)) then
    result := rvInvalidTag
  else
  begin
    Assert(sizeof(Tag) <= sizeof(TTagType));
    result := _DeleteIndex(TTagType(Tag), Async);
  end;
end;

function TIndexedStoreO.AdjustIndexTag(OldTag, NewTag: pointer): TISRetVal;
begin
  if not (FAllowNullTags or (Assigned(OldTag) and Assigned(NewTag))) then
    result := rvInvalidTag
  else
  begin
    Assert(sizeof(OldTag) <= sizeof(TTagType));
    Assert(sizeof(NewTag) <= sizeof(TTagType));
    result := _AdjustIndexTag(TTagType(OldTag), TTagType(NewTag));
  end;
end;

function TIndexedStoreO.FindByIndex(IndexTag: pointer; SearchVal: TIndexNode; var Res:
  TItemRec): TISRetVal;
begin
  if not (FAllowNullTags or Assigned(IndexTag)) then
    result := rvInvalidTag
  else
  begin
    Assert(sizeof(IndexTag) <= sizeof(TTagType));
    result := _FindByIndex(TTagType(IndexTag), SearchVal, Res);
  end;
end;

function TIndexedStoreO.FindNearByIndex(IndexTag: pointer; SearchVal: TIndexNode; var Res:
  TItemRec): TISRetVal;
begin
  if not (FAllowNullTags or Assigned(IndexTag)) then
    result := rvInvalidTag
  else
  begin
    Assert(sizeof(IndexTag) <= sizeof(TTagType));
    result := _FindNearByIndex(TTagType(IndexTag), SearchVal, Res);
  end;
end;

function TIndexedStoreO.TraverseByIndex(IndexTag: pointer; Event: TStoreTraversalEvent;
  Forwards: boolean): TISRetVal;
begin
  if not (FAllowNullTags or Assigned(IndexTag)) then
    result := rvInvalidTag
  else
  begin
    Assert(sizeof(IndexTag) <= sizeof(TTagType));
    result := _TraverseByIndex(TTagType(IndexTag), Event, Forwards);
  end;
end;

function TIndexedStoreO.FirstByIndex(IndexTag: pointer; var Res: TItemRec): TISRetVal;
begin
  if not (FAllowNullTags or Assigned(IndexTag)) then
    result := rvInvalidTag
  else
  begin
    Assert(sizeof(IndexTag) <= sizeof(TTagType));
    result := _FirstByIndex(TTagType(IndexTag), Res);
  end;
end;

function TIndexedStoreO.LastByIndex(IndexTag: pointer; var Res: TItemRec): TISRetVal;
begin
  if not (FAllowNullTags or Assigned(IndexTag)) then
    result := rvInvalidTag
  else
  begin
    Assert(sizeof(IndexTag) <= sizeof(TTagType));
    result := _LastByIndex(TTagType(IndexTag), Res);
  end;
end;

function TIndexedStoreO.NextByIndex(IndexTag: pointer; var Res: TItemRec): TISRetVal;
begin
  if not (FAllowNullTags or Assigned(IndexTag)) then
    result := rvInvalidTag
  else
  begin
    Assert(sizeof(IndexTag) <= sizeof(TTagType));
    result := _NextByIndex(TTagType(IndexTag), Res);
  end;
end;

function TIndexedStoreO.PreviousByIndex(IndexTag: pointer; var Res: TItemRec): TISRetVal;
begin
  if not (FAllowNullTags or Assigned(IndexTag)) then
    result := rvInvalidTag
  else
  begin
    Assert(sizeof(IndexTag) <= sizeof(TTagType));
    result := _PreviousByIndex(TTagType(IndexTag), Res);
  end;
end;

end.
