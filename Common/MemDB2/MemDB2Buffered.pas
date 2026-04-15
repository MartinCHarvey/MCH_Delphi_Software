unit MemDB2Buffered;
{

Copyright � 2026 Martin Harvey <martin_c_harvey@hotmail.com>

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
  In memory database.

  Also main logic for table and DB handling, ACID evolution.
}

//TODO - Check use of TMemDbRowFields in all cases where applicable.

// TODO - When all is up and working, look at possibly
// out of memory exception paths, and how to make them better.
// Just go thru all the cases by class type and alloc position.

interface

{$IFDEF DEBUG_DATABASE_DELETE}
{$DEFINE DEBUG_DATABASE_NAVIGATE}
{$ENDIF}

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  IndexedStore, MemDB2Streamable, Classes, MemDB2Misc,
  DLList, LockAbstractions, TinyLock, MemDB2BufBase, MemDB2Indexing, Reffed;

type
  TMemDBAPIInterfacedObject = class;

{$IFDEF USE_TRACKABLES}
  TMemDBAPI = class(TTrackable)
{$ELSE}
  TMemDBAPI = class
{$ENDIF}
  protected
    FAssociatedTransaction: TObject;
    FInterfacedObject: TMemDBAPIInterfacedObject;
    //Yes, you can attempt all write operations as write-shared...
    procedure CheckWriteTransaction;
    //These do not handle persistence of interfaced obj during the acquire.
    //Suggest you get an entity list, and hold on to it, until you have
    //the requested interface.
    function GetApiObject(ID: TMemDBAPIId; RaiseIfNoAPI: boolean = true): TMemDBAPI;
  public
    destructor Destroy; override;
  end;

  TAPIObjectRequest = function(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI of object;

{$IFDEF USE_TRACKABLES}
  TMemDBAPIInterfacedObject = class(TTrackable)
{$ELSE}
  TMemDBAPIInterfacedObject = class
{$ENDIF}
  protected
    FParent: TObject;
    FAPIObjects: TList;
    FAPIListLock: TCriticalSection;
    FAPIObjectRequest: TAPIObjectRequest;
    //Locking might be needed if multiple reader threads
    //use state shared in the same API objects.

    FOnAddRefForApi: TNotifyEvent;
    FOnReleaseForApi: TNotifyEvent;
    procedure DoAddRefForApi;
    procedure DoReleaseForApi;
    function DoAPIObjectRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;

    procedure PutAPIObjectLocal(API:TMemDBAPI);
  public
    function GetAPIObject(Transaction: TObject; ID: TMemDBAPIId; RaiseIfNoAPI: boolean = true): TMemDBAPI;
    constructor Create;
    destructor Destroy; override;
    property Parent: TObject read FParent;
  end;


//TODO - Go through and sort all MEMDB2_TMP_REMOVE declarations

  TMemDBRow = class;
{$IFDEF MEMDB2_TEMP_REMOVE}

  //TODO - Smarter removal / re-insertion for items when changing stuff,
  //i.e. don't completely remove and insert, instead, on commit / rollback
  //remove re-insert only from indices that you have to.

  //Indexed list is for large data lists (table and other datasets),
  //Not for metadata.

  //IndexedList handles per-row tracking of empty / changed lists
  //on a per-tid basis. Mainly concerned with internal structure.
  TMemDBIndexedList = class(TMemDBJournalCreator)
  protected
    //TODO - Index locking and list locking. In table class?

    //TODO - Add remove from indexes and list separately here?
    //TODO - Lock ordering; Function to lock multiple lists?

    //TODO - Quick review of where when things get added/removed and how indexed
    //(influences locking).
    FStore: TIndexedStoreO;
    FRefChangedRows: TDLEntry;
    FRefEmptyRows: TDLEntry;

    //IRec is assigned if row already in list.
    function LookaheadHelper(Stream: TStream; var IRec: TItemRec): TMemDBRow;

    function WhichList(Row:TMemDbRow): PDLEntry;
  public
    constructor Create;
    destructor Destroy; override;

    procedure RmRowQuickLists(Row: TMemDBRow; var LPos:PDLEntry; var LHead: PDLEntry);
    procedure AddRowQuicklists(Row: TMemDbRow; LPos: PDLEntry; LHead: PDLEntry);

    procedure ToScratch(Stream: TStream);override;
    procedure FromJournal(const PseudoTid: TTransactionId; Stream: TStream);override;
    procedure FromScratch(const PesudoTid: TTransactionId; Stream: TStream);override;

    property Store: TIndexedStoreO read FStore write FStore;
  end;

  TSelectiveRowHandler = procedure(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList) of object;

{$IFDEF USE_TRACKABLES}
  TEditRec = class(TTrackable)
{$ELSE}
  TEditRec = class
{$ENDIF}
    IRec, NextIRec: TItemRec;
    LPos: PDLEntry;
    QLPos, QLHead: PDLEntry;
    RemovedItem: TMemDBRow;
  end;

  //Main table datastore.

  //Additional to Indexed list handling above, provides metadata and data functions
  //for changing rows, and handling concurrency between them & meta/client edits.
  TMemDBTableList = class(TMemDBIndexedList)
  protected
    //Selective row handlers.
    procedure WriteRowToJournal(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);
    procedure CommitRollbackChangedRow(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);
    procedure RemoveEmptyRow(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);

    //Selective row iterators
    procedure ForEachRow(Handler: TSelectiveRowHandler;
                         Ref1, Ref2: TObject);
    procedure ForEachChangedRow(Handler: TSelectiveRowHandler;
                                Ref1, Ref2: TObject);
    procedure ForEachEmptyRow(Handler: TSelectiveRowHandler;
                                Ref1, Ref2: TObject);
  public
    constructor Create;
    destructor Destroy; override;
    //Functions to modify data when metadata indicates change
    //in table structure necessary.

    //Meta edits.
    function MetaEditFirst: TEditRec;
    function MetaEditNext(EditRec: TEditRec):TEditRec;
    function MetaRemoveRowForEdit(EditRec: TEditRec): TMemDbRow;
    procedure MetaInsertRowAfterEdit(EditRec: TEditRec; Row: TMemDbRow);

    procedure ToJournal(const Tid: TTransactionId; Stream: TStream);override;

    function AnyChangesAtAll: boolean; override;

    //TODO - Table data pre-commit checks transaction parenting of changed rows.
    procedure PreCommit(const Tid: TTransactionId; Reason: TMemDbTransReason); override;
    procedure Commit(const Tid: TTransactionId; Reason: TMemDbTransReason); override;
    procedure Rollback(const Tid: TTransactionId; Reason: TMemDbTransReason); override;

    //Navigation and change functions for user API.
    //TODO - These need rethinking.
    function MoveToRowByIndexTag(Iso: TMDBIsolationLevel;
                                 TagStruct: PITagStruct;
                                 Cursor: TItemRec;
                                 Pos: TMemAPIPosition): TItemRec;
    function FindRowByIndexTag(Iso: TMDBIsolationLevel;
                               IndexDef: TMemIndexDef;
                               FieldDefs: TMemFieldDefs;
                               ITagStruct: PITagStruct;
                               const DataRecs: TMemDbFieldDataRecs): TItemRec;

    //Client edits?
    //TODO - And so do these.
    procedure ReadRowData(Row: TMemDBRow; Iso: TMDBIsolationLevel;
                          Fields: TMemStreamableList);
    //If row not assigned, then append.
    procedure WriteRowData(var Cursor: TItemRec; Iso: TMDBIsolationLevel;
                          Fields: TMemStreamableList);
    //Client delete?
    procedure DeleteRow(var Cursor: TItemRec; Iso: TMDBIsolationLevel);

    //TODO - Client pin operation?

    //TODO - Where commit edits & handling?
    //all in selective handlers, or elsewhere?
  end;
{$ENDIF}

  TMemDBTablePersistent = class;
  TTidLocal = class;

  TMemDbRowProxy = class(TReffedProxy)
  end;

  TMemDBRow = class(TMemDBMultiBufferedTiny)
  private
    FRowID: TGUID;
    FTable: TMemDBTablePersistent;
    //TODO - Not even convinced we need Row proxy, remove it?
    //I think DCP handlers are enough (no external references
    //outside transaction, all handled by pins).
    //Depends what query engine stuff might do - probably refs
    //data items itself.
    FProxy: TMemDBRowProxy;
    FMasterRec: TItemRec;
  protected
    procedure DCPHandle(const Update: TReferenceUpdate); override;
    procedure CPTidHandle(const Update: TTidUpdate); override;
    function MakeCandidateIPins(Index: TMemDbIndex): TList;
  public
    constructor Create;
    procedure Init(Table: TMemDBTablePersistent; const Guid:TGuid);

    //This function checks formats enough so algorithms don't die, but
    //is not the final concurrency check (which is done via Tid comparison).

    //TODO - One day, a list of format changes so we can fix-up on the fly?
    function CheckFormatAgainstMeta(const Tid: TTransactionId; AB: TAbSelType; MD: TMemTableMetadataItem; PinReason: TPinReason): boolean;
    class function StaticCheckFormatAgainstMeta(Data: TMemDBStreamable; MD: TMemTableMetadataItem): boolean;
    function CheckFormatAgainstMetaDefs(const Tid: TTransactionId; AB: TAbSelType; MetaFieldDefs: TMemStreamableList; PinReason: TPinReason): boolean;
    class function StaticCheckFormatAgainstMetaDefs(Data: TMemDBStreamable; MetaFieldDefs: TMemStreamableList): boolean;

    procedure ToJournal(const Tid: TTransactionId; Stream: TStream);override;
    procedure ToScratch(const PseudoTid:TTransactionId; Stream: TStream);override;
    procedure FromJournal(const PseudoTid: TTransactionId; Stream: TStream);override;
    procedure FromScratch(const PseudoTid: TTransactionId; Stream: TStream);override;

    procedure RemoveFromLocalIndices(TidLocal: TTidLocal);
    procedure AddToLocalIndices(TidLocal: TTidLocal);

    property RowId:TGUID read FRowId write FRowId;
  end;

  //TODO Lack of atomicity in metadata names etc currently protected
  //by commit lock, multi-write only on table data.
  //This may change in future.


  TMemDBEntity = class;

  TMemDBEntityMetadata = class(TMemDBMultiBufferedCrit)
  private
    FEntity: TMemDBEntity;
  protected
  public
    constructor Create;
    procedure Init(const Tid: TTransactionId; Parent: TObject; Name:string; DSName: boolean); virtual;
    procedure DCPHandle(const Update: TReferenceUpdate); override;
  end;

  //Internal maintenance operations at this level.
  TMemDBTableMetadata = class(TMemDBEntityMetadata)
  private
    //A/B buffer holds TMemTableMetadataItem
    procedure CheckABListChanges(const Tid: TTransactionId);
  protected
    //TODO - Recheck callers of these have TidLocal or use META_ functions.
    function FieldsByNames(const AB: TBufSelector; const Names: TMDBFieldNames; var AbsIdxs: TFieldOffsets; Reason: TPinReason): TMemFieldDefs;
    function FieldByName(const AB: TBufSelector; const Name: string; var AbsIndex: integer; Reason: TPinReason): TMemFieldDef;
    function IndexByName(const AB: TBufSelector; const Name: string; var AbsIndex: integer; Reason: TPinReason): TMemIndexDef;
  public
    procedure PreCommit(const Tid: TTransactionId; Reason: TMemDbTransReason); override;
    procedure Init(const Tid: TTransactionId; Parent: TObject; Name:string; DSName: boolean); override;
  end;

  TMemDBFKRefType = (fkTable, fkIndex);

  TMemDBForeignKeyMetadata = class(TMemDBEntityMetadata)
  protected
{$IFDEF MEMDB2_TEMP_REMOVE}
    //A/B buffer holds TMemForeignKeyMetadataItem
    function GetReferer(RT: TMemDBFKRefType; const AB: TBufSelector):string;
    function GetReferred(RT: TMemDBFKRefType; const AB: TBufSelector):string;
    procedure ConsistencyCheck;
  public

    property Referer[RT: TMemDBFKRefType; const AB: TBufSelector]: string read GetReferer;
    property Referred[RT: TMemDBFKRefType; const AB: TBufSelector]: string read GetReferred;

    procedure SetReferer(RT: TMemDBFKRefType; Referer: string);
    procedure SetReferred(RT: TMemDBFKRefType; Referred: string);
{$ELSE}
  public
{$ENDIF}
    procedure Init(const Tid: TTransactionId; Parent: TObject; Name:string; DSName: boolean); override;
  end;

  //TMemDBDatabaseMetadata - not yet.

  TMemDBDatabasePersistent = class;
  TMemDBEntityProxy = class;

  TMemDBEntity = class(TMemDBJournalCreator)
  private
    FParentDB: TMemDBDatabasePersistent;
    FProxy: TMemDBEntityProxy;
    FInterfaced: TMemDBAPIInterfacedObject;
  protected
    FMetadata: TMemDBEntityMetadata;

    //TODO - Atomic pinning and flags here.
    function HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;virtual;
    procedure HandleAddRefForAPI(Sender: TObject);
    procedure HandleReleaseForAPI(Sender: TObject);
    procedure MetadataDCPHandle(Sender: TObject; const Update:TReferenceUpdate);

    property Metadata: TMemDBEntityMetadata read FMetadata;    
  public
    constructor Create;
    destructor Destroy; override;

{$IFDEF MEMDB2_TEMP_REMOVE}
    //function AnyChangesAtAll: boolean; override;
{$ENDIF} // MEMDB2_TEMP_REMOVE

    function META_PinCurrent(const Tid: TTransactionId; Reason: TPinReason): TMemDBStreamable; virtual;
    function META_GetNext(const Tid: TTransactionId): TMemDBStreamable; virtual;
    function META_GetPinLatest(const Tid: TTransactionId;
                            var BufSelected: TAbSelType; Reason: TPinReason): TMemDBStreamable; virtual;
    procedure META_RequestChange(const Tid: TTransactionId); virtual;
    procedure META_Delete(const Tid: TTransactionId); virtual;

    procedure PreCommit(const TId: TTransactionId; Reason: TMemDBTransReason); override;
    procedure Commit(const TId: TTransactionId; Reason: TMemDbTransReason); override;
    procedure Rollback(const TId: TTransactionId; Reason: TMemDbTransReason); override;
    procedure Init(const Tid: TTransactionId; Parent: TObject; Name:string; DSName: boolean);

    property ParentDB: TMemDBDatabasePersistent read FParentDB;
    property Proxy: TMemDBEntityProxy read FProxy;
    property Interfaced: TMemDBAPIInterfacedObject read FInterfaced;
  end;

  //TODO - Re-check this, I think it's OK,
  //however, calc via index tags ....
  TMemDBFKMetaTags = record
    abBuf: TBufSelector;
    FieldAbsIdxs: TFieldOffsets;
    FieldAbsIdxsFast: TFieldOffsetsFast;
  end;
  PMemDBMetaTags = ^TMemDbFKMetaTags;

  TMemDBFKMetaLists = record
    FReferringAdded,
    FReferredDeleted,
    FReferredAdded: TIndexedStoreO;

    //Only need one extra index out of a possible six!
    TagReferredAddedNext: TMemDbFkMetaTags;
  end;

  //A temp struct we pass up the stack, to let functions
  //know what's what, and where.
  TMemDBFKMeta = record
    TableReferring, TableReferred: TMemDbTablePersistent;
    IndexDefReferring, IndexDefReferred: TMemIndexDef;
    FieldDefsReferring, FieldDefsReferred: TMemFieldDefs;
    TableReferringIdx, TableReferredIdx,
    //IndexIdx and FieldIdx absolute in rearrangement cases.
    IndexDefReferringAbsIdx, IndexDefReferredAbsIdx: integer;
    FieldDefsReferringAbsIdx, FieldDefsReferredAbsIdx: TFieldOffsets;

    Lists: TMemDBFKMetaLists;
  end;
  PMemDBFKMeta = ^TMemDBFKMeta;

  TMemDBForeignKeyPersistent = class(TMemDBEntity)
{$IFDEF MEMDB2_TEMP_REMOVE}
  private
  protected
    procedure SetupIndexes(var Meta: TMemDBFKMeta);
    procedure ClearIndexes(var Meta: TMemDbFkMeta);

    //TODO - Check TIndexSelector, TSubIndexSelType
    function FindIndexTag(Table: TMemDbTablePersistent;
                          IndexDef: TMemIndexDef;
                          var OutChangeset: TIndexChangeset;
                          SubIndexClass: TIndexSelector): PITagStruct;

    procedure ProcessRow(Row: TMemDBRow;
                         Ref1, Ref2: TObject; TblList: TMemDbIndexedList);

    procedure CreateCheckForeignKeyRowSets(var Meta: TMemDBFKMeta; Reason: TMemDBTransReason);
    procedure CreateReferringAddedList(const Meta: TMemDBFKMeta; Reason: TMemDBTransReason);
    procedure TrimReferringAddedList(const Meta: TMemDBFKMeta; Reason: TMemDBTransReason);

    procedure CreateReferredDeletedList(const Meta: TMemDBFKMeta; Reason: TMemDBTransReason);
    procedure TrimReferredDeletedList(const Meta: TMemDBFKMeta; Reason: TMemDBTransReason);
    procedure CheckOutstandingCrossRefs(const Meta: TMemDBFKMeta; Reason: TMemDBTransReason);

  public
    constructor Create;
    destructor Destroy; override;
    procedure PreCommit(const TId: TTransactionId; Reason: TMemDBTransReason); override;

    procedure ToJournal(const Tid: TTransactionId; Stream: TStream); override;
    procedure ToScratch(const PseudoTid: TTransactionId; Stream: TStream); override;
    procedure FromJournal(const PseudoTid: TTransactionId; Stream: TStream); override;
    procedure FromScratch(const PseudoTid: TTransactionId; Stream: TStream); override;
{$ENDIF} // MEMDB2_TEMP_REMOVE

    procedure CheckAPITableDelete(const Sel: TBufSelector; TableName: string);
    procedure CheckAPIIndexDelete(const Sel: TBufSelector; IsoDeterminedTableName, IndexName: string);
    function HandleAPITableRename(const Sel: TBufSelector; OldName, NewName: string): boolean;
    function HandleAPIIndexRename(const Sel: TBufSelector; IsoDeterminedTableName, OldName, NewName: string): boolean;
  end;

  TFieldChangeType = (fctAdded,
                      fctDeleted,
                      fctChangedFieldNumber);

  TFieldChangeSet = set of TFieldChangeType;
  TFieldChangeArray = array of TFieldChangeSet;

  TIndexChangeType = (ictAdded,
                      ictDeleted,
                      ictChangedFieldNumber);

  TIndexChangeSet = set of TIndexChangeType;
  TIndexChangeArray = array of TIndexChangeSet;

  TMemDblBufListHelper = class;

  TCommitChangeMade = ( ccmAddedNewTagStructs,
                        ccmDeleteUnusedIndices,
                        ccmIndexesToTemporary);
  TCommitChangesMade = set of TCommitChangeMade;

{$IFDEF USE_TRACKABLES}
  TMemDBCursor = class(TTrackable)
{$ELSE}
  TMemDBCursor = class
{$ENDIF}
  private
    FRow: TMemDBRow;
    FPinTid: TTransactionId;
    //Some fields so we can do something sensible on deletion.
    FIterIndex: TMemDBIndex;
    FIterInode: TMemDBIndexLeaf;
    FIterInc: TMemAPIPosition;
    FTidLocal: TTidLocal;
  protected
  public
    procedure SetRowPin(const Tid: TTransactionId; NewRow: TMemDBRow);
    destructor Destroy; override;
    property Row: TMemDBRow read FRow;
    property IterIndex: TMemDBIndex read FIterIndex;
    property IterInc: TMemAPIPosition read FIterInc;
    property TidLocal: TTidLocal read FTidLocal;
  end;


{$IFDEF USE_TRACKABLES}
  TTidLocal = class(TTrackable)
{$ELSE}
  TTidLocal = class
{$ENDIF}
  private
    //TODO - Do I need a lock here? I don't think so.
    //TODO - Race conditions will involve metadata/data access outside
    //commit lock - need to check row formats etc etc.

    //TODO - Check handles multiple format changes back and forth ...
    //(row addition still handled by addition lock).

    FTidLocalLinks: TDLEntry;
    FParentTable: TMemDBTablePersistent;
    FTid: TTransactionId;

    FDataChanged: boolean;
    FLayoutChangeRequired: boolean;
    FIndexingChangeRequired: boolean;

    FIndexHelper, FFieldHelper: TMemDblBufListHelper;
    FEmptyList: TMemStreamableList;
    FIndexChangesets: TIndexChangeArray;
    FFieldChangesets: TFieldChangeArray;
    FIndexInit: boolean;

    FLocalIndexCopies: TReffedList;
    FNewBuildIndices: TReffedList;

    FCPRows: TIndexedStoreO; //Rows changed or pinned by cur Txion.

    function LookaheadHelper(Stream: TStream; Scratch: boolean; var Created: boolean): TMemDBRow;

    procedure CheckTableRowCount;
    procedure CheckChangedRowStructure(Reason: TMemDBTransReason);
    procedure BuildCheckPartialIndexes;
    procedure RowLocalPreCommit(Reason: TMemDbTransReason);

    procedure UpdateHelpersAndIndices(PinReason: TPinReason);
    procedure DimensionChangesets(PinReason: TPinReason);
    procedure ReGenChangesets(PinReason: TPinReason);

    procedure HandleHelperChangeRequest(Sender: TObject);

    procedure AdjustTableStructureForMetadata;

    procedure Init(Parent: TMemDBTablePersistent; const Tid: TTransactionId);

    procedure ToJournal(Stream: TStream);
    procedure ToScratch(Stream: TStream);
    procedure FromJournal(Stream: TStream);
    procedure FromScratch(Stream: TStream);

    procedure PreCommit(Reason: TMemDBTransReason);
    procedure Commit(Reason: TMemDBTransReason);
    procedure Rollback(Reason: TMemDBTransReason);

    procedure MetaIndexCommit(Reason: TMemDBTransReason);
    procedure MetaIndexRollback(Reason: TMemDBTransReason);
    procedure MetaIndexLocalRollback(Reason: TMemDBTransReason);

    procedure BuildValidateNewIndexesCommon(LocalIter: boolean);
    procedure BuildValidateNewIndexesOutsideCommitLock;
    procedure BuildValidateNewIndexesInsideCommitLock;

    procedure RowCPTidHandle(Sender: TObject; const Update: TTidUpdate);
  public
    constructor Create;
    destructor Destroy; override;

    procedure UpdateLayout(PinReason: TPinReason);

    function GetUserIndexRoot(var Idx: TMemIndexDef; IdxName: string): TMemDBIndex;
    
    //TODO - Ensure user can GetPinLatest for Tid without being
    //out of date with Index.
    function UserMoveToRowByIndexRoot(IRoot: TMemDbIndex;
                                      Cursor: TMemDBCursor;
                                      Pos: TMemAPIPosition): TMemDBCursor;

    //TODO - Ensure user can GetPinLatest for Tid without being
    //out of date with Index.
    function UserFindRowByIndexRoot(IndexDef: TMemIndexDef;
                                    FieldDefs: TMemFieldDefs;
                                    FieldAbsIdxs: TFieldOffsets;
                                    IRoot: TMemDbIndex;
                                    const DataRecs: TMemDbFieldDataRecs): TMemDbCursor;

    procedure UserDeleteRow(Cursor: TMemDbCursor);
    procedure UserWriteRowData(var Cursor:TMemDbCursor; FieldList: TMemStreamableList);

    property DataChanged: boolean read FDataChanged;
    property LayoutChangeRequired: boolean read FLayoutChangeRequired;
  end;

  TRowIndexNode = class(TIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  end;

  TRowSearchVal = class(TRowIndexNode)
  private
    FGuid: TGuid; //Row GUID.
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
    procedure Init(const RowGuid: TGuid);
  end;

  //Prohibits operations on all rows in a table whilst it is being modified.
  //Better to abort the silly little transactions changing individual rows
  //than one big one restructuring the table.
{$IFOPT C+}
  TRowChangeLock = class(TTrackable)
{$ELSE}
  TRowChangeLock = record
{$ENDIF}
    FSiblings: TDLEntry;
    Tid: TTransactionId;
    ProhibitAdd, ProhibitChange, ProhibitDelete: boolean;
  end;
{$IFOPT C+}
  PRowChangeLock = TRowChangeLock;
{$ELSE}
  PRowChangeLock = ^TRowChangeLock;
{$ENDIF}


  TMemDBTablePersistent = class(TMemDBEntity)
  private
    //TODO - Determine and check some locking order.
    FMasterRowLock: TCriticalSection;
    FAdditionLocks: TDLEntry;
    //Addition locks possibly the first of many smaller scale locks, but
    //at the moment, it's still doing FK's and indices under the
    //master commit lock. This area will be improved in future.

    FMasterRowList: TIndexedStoreO;

    //TODO - Consider dangling Tids in exception cases, and
    //how to clean up.
    FTidLocalLock: TCriticalSection;
    FTidLocalStructures: TDLEntry;

    FMetaIndexLock: TCriticalSection;
    FMasterIndexes: TReffedList;

{$IFDEF MEMDB2_TEMP_REMOVE}
    //TODO - Just get rid of all this.

    FCommitChangesMade: TCommitChangesMade;
    FTemporaryIndexLimit: integer; //Handle exceptions: 0 .. Lim-1 made temporary.

    FData: TMemDBTableList; //Indexed list of TMemDBRow.

    //TODO - Check helpers and change arrays either
    //protected by commit lock or ...

    //TODO TODO - One fine day, somehow make all this TID local *as well*,
    //so can mod tables in parallel?? Think not worth it at the moment.
    FIndexHelper, FFieldHelper: TMemDblBufListHelper;
    FIndexChangesets: TIndexChangeArray;
    FFieldChangesets: TFieldChangeArray;
    FTagDataList: TList;
    FIndexingChangeRequired, FDataChangeRequired: boolean;

    FEmptyList: TMemStreamableList;
{$ENDIF}
  protected
    //Returns newly added for this tid.
    function AddAllRowProhibitInCrit(const Tid: TTRansactionId;
      ProhibitAdd, ProhibitChange, ProhibitDelete: boolean): boolean;

    procedure GetProhibitions(const Tid: TTRansactionId;
                             var AddProhibited: boolean;
                             var ChangeProhibited: boolean;
                             var DeleteProhibited: boolean);

    function FindRowProhibitLock(const Tid: TTransactionId): PRowChangeLock;
    procedure FindRmRowProhibitLock(const Tid: TTransactionId);
    function NewRowProhibitLock: PRowChangeLock;
    procedure DisposeRowProhibitLock(Lock: PRowChangeLock);

    procedure RowDCPHandleAndForward(Sender: TObject; const Update: TReferenceUpdate);
    procedure RowCPTidHandleAndForward(Sender: TObject; const Update: TTidUpdate);

    procedure HandleAddRefForTidLocal(Sender: TObject);
    procedure HandleReleaseForTidLocal(Sender: TObject);

    procedure HandleAddRefForData(Sender: TObject);
    procedure HandleReleaseForData(Sender: TObject);

    function GetTidLocal(const Tid: TTransactionId): TTidLocal;

    //This is not entirely race free: Insertion into the initial list
    //is race free, but no guarantee fully init until initialising call
    //has returned. However. Assumption is that calls for a certain
    //Tid are in the same thread.
    function GetMakeTidLocal(const Tid: TTransactionId; PinReason: TPinReason): TTidLocal;

    function GetMakeListHelpers(const Tid: TTransactionId;
                                var FieldHelper: TMemDblBufListHelper;
                                var IndexHelper: TMemDblBufListHelper): TTidLocal;

    procedure GetCurrNxtMetaCopiesEx(const Tid: TTransactionId;
                                     var CC, NC: TMemTableMetadataItem;
                                     var CCFieldCount, CCIndexCount,
                                     NCFieldCount, NCINdexCount: integer;
                                     PinReason: TPinReason); //TODO - Do we need pin reason?

    procedure GetCurrNxtMetaCopies(const Tid: TTransactionId;
                                   var CC, NC: TMemTableMetadataItem;
                                   PinReason: TPinReason);  //TODO - Do we need pin reason?

    procedure LookaheadHelper(Stream: TStream;
                              var MetadataInStream: boolean;
                              var DataInStream: boolean);

    //IdxIdx here the same as changesets.
    //All errors here internal exceptions.
{$IFDEF MEMDB2_TEMP_REMOVE}

{$IFOPT C+}
    procedure CheckTagAgreesWithMetadata(IdxIdx: integer;
                            TagData: TMemDBITagData;
                            IndexState: TTagCheckIndexState;
                            ProgState: TTagCheckProgrammedState);
{$ELSE}
    procedure CheckTagAgreesWithMetadata(IdxIdx: integer;
                            TagData: TMemDBITagData;
                            IndexState: TTagCheckIndexState;
                            ProgState: TTagCheckProgrammedState); inline;
{$ENDIF}

    //Actions at change or commit check time.
    procedure HandleHelperChangeRequest(Sender: TObject);
    procedure UpdateHelpersAndIndices;
    procedure DimensionChangesets;
    procedure ReGenChangesets;
    //Actions made in commit / rollback.
    procedure AddNewTagStructs;
    procedure RemoveNewlyAddedTagStructs;
    procedure RemoveOldTagStructs(DeleteAll: boolean = false);
    procedure DeleteUnusedIndices(Reason: TMemDBTransReason);
    procedure ReinstateDeletedIndices;
    procedure AdjustTableStructureForMetadata;
    procedure ValidateIndexes(Reason: TMemDBTransReason);
    procedure CommitAdjustIndicesToTemporary(Reason: TMemDBTransReason);
    procedure CommitRestoreIndicesToPermanent(Reason: TMemDbTransReason);
    procedure RollbackRestoreIndicesToPermanent;

    procedure RevalidateEntireUserIndex(RawIndexDefNumber: integer);
    procedure CheckStreamedInTableRowCount;
    procedure CheckStreamedInRowStructure(Row: TMemDBRow; Ref1, Ref2: TObject;  TblList: TMemDbIndexedList);
    procedure CheckStreamedInRowIndexing;
    procedure CheckIndexForRow(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);

    property Data: TMemDBTableList read FData;
{$ENDIF}
  public
    constructor Create;
    destructor Destroy; override;

    procedure ToJournal(const Tid: TTransactionId; Stream: TStream); override;
    procedure ToScratch(const PseudoTid:TTransactionId; Stream: TStream); override;
    procedure FromJournal(const PseudoTid: TTransactionId; Stream: TStream); override;
    procedure FromScratch(const PseudoTid: TTransactionId; Stream: TStream); override;

    procedure PreCommit(const TId: TTransactionId; Reason: TMemDBTransReason); override;
    procedure Commit(const TId: TTransactionId; Reason: TMemDbTransReason); override;
    procedure Rollback(const TId: TTransactionId; Reason: TMemDbTransReason); override;

    function DataChangedForTid(const Tid:TTransactionId): boolean;
    function LayoutChangesRequiredForTid(const Tid: TTransactionId): boolean;
    function AnyChanges(const Tid:TTransactionId): boolean; override; //TODO - Check OK: Not atomic, meant for debug checking.
    function AnyChangesForTid(const Tid: TTransactionId): boolean; override; //TODO - Check OK: Only atomic from the thread processing that Tid.

    //TODO - Cache metadata for current access for API to reduce lock contention.
    function META_PinCurrent(const Tid: TTransactionId; Reason: TPinReason): TMemDBStreamable; override;
    function META_GetPinLatest(const Tid: TTransactionId;
                            var BufSelected: TAbSelType; Reason: TPinReason): TMemDBStreamable; override;
    procedure META_RequestChange(const Tid: TTransactionId); override;
    procedure META_Delete(const Tid: TTransactionId); override;
                            
    function META_FieldsByNames(const AB: TBufSelector; const Names: TMDBFieldNames; var AbsIdxs: TFieldOffsets): TMemFieldDefs;
    function META_FieldByName(const AB: TBufSelector; const Name: string; var AbsIndex: integer): TMemFieldDef;
    function META_IndexByName(const AB: TBufSelector; const Name: string; var AbsIndex: integer): TMemIndexDef;

    function GetUserTidLocalIndexRoot(const Tid: TTransactionId; var TidLocal: TTidLocal; var Idx: TMemIndexDef; IdxName: string): TMemDBIndex;
  end;

  TMemDBEntityChangeType = (
    mectNewTable,
    mectNewFK,
    mectChangedDeletedEntity,
    mectChangedDataTable
  );

  TMemDbEntityProxy = class(TReffedProxy)
  protected
    FSiblings:TDLEntry;
  public
    constructor Create;
  end;

  TMemDBDatabasePersistent = class(TMemDBJournalCreator)
  private
  protected
    FInterfaced: TMemDBAPIInterfacedObject;
    FEntityLock: TCriticalSection;
    FEntityList: TDLEntry;

    //TODO: Thorn in my side. Pre-commit commit lock currently vital
    //for ensuring consistency of indices and foreign keys.
    //Big TODO is eventually getting rid of it.
    FCommitLock: TCriticalSection;

    function HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;virtual;
    procedure LookaheadHelper(Stream: TStream;
                             var ChangeType: TMemDBEntityChangeType;
                             var EntityName: string);
    //Atomically get and assemble list of entities.
    function AssembleEntityList: TReffedList;
  public
    function EntitiesByName(const AB:TBufSelector; Name: string; PinReason: TPinReason): TMemDBEntity;

    procedure ToJournal(const Tid: TTransactionId; Stream: TStream); override;
    procedure ToScratch(const PseudoTid:TTransactionId; Stream: TStream); override;
    procedure FromJournal(const PseudoTid: TTransactionId; Stream: TStream); override;
    procedure FromScratch(const PseudoTid: TTransactionId; Stream: TStream); override;

    function AnyChangesForTid(const TId: TTransactionId): boolean; override;
    function AnyChanges(const Tid: TTransactionId): boolean; override;

    procedure PreCommit(const TId: TTransactionId; Reason: TMemDBTransReason); override;
    procedure Commit(const TId: TTransactionId; Reason: TMemDbTransReason); override;
    procedure Rollback(const TId: TTransactionId; Reason: TMemDbTransReason); override;

    constructor Create;
    destructor Destroy; override;

    procedure CheckAPITableDelete(const Sel: TBufSelector; TableName: string);
    procedure CheckAPIIndexDelete(const Sel: TBufSelector; IsoDeterminedTableName, IndexName: string);
    function HandleAPITableRename(const Sel: TBufSelector; OldName, NewName: string): boolean;
    function HandleAPIIndexRename(const Sel: TBufSelector; IsoDeterminedTableName, OldName, NewName: string): boolean;

    property Interfaced: TMemDBAPIInterfacedObject read FInterfaced;
    property CommitLock: TCriticalSection read FCommitLock;
  end;

  //Helper class to manage A/B buffered lists.
  //Handles two lists of "stuff", where one is in a current buffer,
  //and one is in a next buffer. Can request list clone,
  //Will create delete sentinels as necessary.

{$IFDEF USE_TRACKABLES}
  TMemDblBufListHelper = class(TTrackable)
{$ELSE}
  TMemDblBufListHelper = class
{$ENDIF}
  private
    FOnChangeRequest: TNotifyEvent;
  protected
    FCurrentList, FNextList: TMemStreamableList;
    procedure Changing;
    procedure SetABList(const AB: TAbSelType; New: TMemStreamableList);
    function GetABList(const AB: TAbSelType): TMemStreamableList;

    function GetNonDeletedCount: integer;
    function GetNonDeletedItem(Idx: integer): TMemDBStreamable;
    function GetCount: integer;
    function GetItem(Idx: integer): TMemDBStreamable;
    function ModifyInternal(Index: integer; var OldObj: boolean): TMemDBStreamable; //returns obj in list.
    procedure DeleteInternal(Index: integer; var OldObj: boolean); //deletes object as well
    function AddInternalNoRef(New: TMemDBStreamable; var OldObj: boolean; Index: integer = -1): integer; //returns raw index, takes ownership.

    procedure GetChildValidPtrs(Idx: integer; var Current: TMemDBStreamable; var Next: TMemDBStreamable);
    function GetChildAdded(Idx: integer): boolean;
    function GetChildModified(Idx: integer): boolean;
    function GetChildDeleted(Idx: integer): boolean;
    function GetChildNull(Idx: integer): boolean;
  public
    destructor Destroy; override;
    //These function always return the NEW object.
    //Be prepared for delete sentinels to appear in the list,
    //Hence the count will not necessarily change after delete calls.

    function Modify(Index: integer): TMemDBStreamable; //returns obj in list.
    //Don't allow arbitrary insertion, but can add back in previously deleted items.
    function AddNoRef(New: TMemDBStreamable; Index: integer = -1): integer; //returns raw index, takes ownership.
    procedure Delete(Index: integer); //deletes object as well

    function ModifyND(Index: integer): TMemDBStreamable; //returns obj in list.
    //Don't allow arbitrary insertion, but can add back in previously deleted items.
    function AddNDNoRef(New: TMemDBStreamable; Index: integer = -1): integer; //returns raw index, takes ownership.
    procedure DeleteND(Index: integer); //deletes object as well

    function RawIndexToNdIndex(Index: integer): integer;
    function NdIndexToRawIndex(NdIndex: integer): integer;

    procedure ClearLists;

    property Count: integer read GetCount;
    property Items[Idx: integer]: TMemDBStreamable read GetItem; //Gets most recent set.
    property NonDeletedCount: integer read GetNonDeletedCount;
    property NonDeletedItems[Idx: integer]: TMemDBStreamable read GetNonDeletedItem;

    //Consistency properties for children.
    //Indexing is raw (all items)
    property ChildAdded[Idx: integer]: boolean read GetChildAdded;
    property ChildModified[Idx:integer]: boolean read GetChildModified;
    property ChildDeleted[Idx: integer]: boolean read GetChildDeleted;
    property ChildNull[Idx: integer]: boolean read GetChildNull;

    //TODO - Handle pinning and parent MultiBuffered class.
    property List[const AB: TAbSelType]: TMemStreamableList read GetABList write SetABList;
    property OnChangeRequest: TNotifyEvent read FOnChangeRequest write FOnChangeRequest;
  end;

//Moved from TMemDBTableMetadata
function FieldsByNamesInt(MetadataCopy: TMemTableMetadataItem; const Names:TMDBFieldNames; var AbsIndexes: TFieldOffsets): TMemFieldDefs;
function FieldByNameInt(MetadataCopy: TMemTableMetadataItem; const Name:string; var AbsIndex: integer): TMemFieldDef;
function IndexByNameInt(MetadataCopy: TMemTableMetadataItem; const Name:string; var AbsIndex: integer): TMemIndexDef;

//Would like to put in Mics, but requires streamable list decl.

//TODO - Building multi data recs for index validation / comparison may prove
//to be a bit slow and costly. Consider refactoring in a way which does
//not need mem alloc.
function BuildMultiDataRecs(FieldList: TMemStreamableList;
                            const AbsFieldOffsets: TFieldOffsets): TMemDbFieldDataRecs;

function SameLayoutFieldsSame(A,B: TMemStreamableList; FieldOffsets: TFieldOffsets): boolean;

function DiffLayoutFieldsSame(A: TMemStreamableList; AOffsets: TFieldOffsets;
                              B: TMemStreamableList; BOffsets: TFieldOffsets): boolean;

function AllFieldsZero(FieldList: TMemStreamableList;
                            const AbsFieldOffsets: TFieldOffsets): boolean;

const
  S_TABLE_DATA_CHANGED = 'Cannot change table field layout when uncommitted data changes.';
  S_FIELD_LAYOUT_CHANGED = 'Cannot change table data when uncommited field layout changes.';
  S_QUICK_LIST_DUPLICATE_INSERTION = 'Quick lookaside lists: Duplicate insertion.';
  //Now indexes are CoW Tid local, a small amount of API code finds its way here.
  S_API_INDEX_NAME_NOT_FOUND = 'Index name not found.';

implementation

uses
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GlobalLog,
{$ENDIF}
  SysUtils, MemDB2, NullStream, MemDB2Api;

const
  S_REPLAY_CHECK_FAILED = 'Journal replay: pre or postcondition checks failed: ';
  S_JOURNAL_REPLAY_BAD_INST = 'Journal replay failed, missing class instance or type. ';
  S_PRE_COMMIT_INTERNAL = 'Pre-commit setup failed, internal error';
  S_COMMMIT_CONSISTENCY_OBJS = 'Commit consistency check failed: Duplicate table or foreign key names.';
  S_MISSING_FIELD_DEF = 'Field definition missing.';
  S_NO_FIELDS_IN_INDEX = 'No fields in index';
  S_FIELD_NAME_EMPTY = 'Field name empty.';
  S_DUP_FIELD_NAMES = 'Duplicate field names.';
  S_BAD_FIELD_INDEX = 'Bad field index.';
  S_MISSING_INDEX_DEF = 'Missing index def';
  S_INDEX_NAME_EMPTY = 'Index name empty';
  S_DUP_INDEX_NAMES = 'Duplicate index names';
  S_FIELD_CHANGED_TYPE = 'Fields may not change type after being created';
  S_PRE_COMMIT_CHECK_INTERNAL = 'Pre commit check internal error.';
  S_INDEX_FIELD_COUNT_SHOULD_BE_CONSTANT = 'Indexes should referencea constant number of fields once created.';
  S_INDEX_DOES_NOT_REFERENCE_FIELD = 'Index does not reference a valid field';
  S_INDEX_UNDERLYING_FIELD_CHANGED = 'Can''t change index to reference a different field.';
  S_INDEXES_INCONSISTENT = 'Index references inconsistent field names and offsets.';
  S_INDEXED_FIELD_CHANGED_TYPE = 'Can''t change field type if it is indexed.';
  S_INDEX_CHANGED_ATTRS = 'Index has changed attributes since created, not allowed.';
  S_INDEX_BAD_FIELD_TYPE = 'This type of field cannot be indexed (float or blob?)';
  S_INDEX_TAG_NOT_FOUND = 'Index tag not found';
  S_INDEX_TAG_DUPLICATE = 'Index tag already exists';
  S_INDEX_CONSTRAINTS_FIELDMOVE = 'Do not expect field numbers to be changing during partial index validation.';
  S_INDEX_CONSTRAINT_ZERO = 'Index attributes require fields not zero.';
  S_INDEX_CONSTRAINT_UNIQUE = 'Index attributes require fields unique.';
  S_COMMIT_ROLLBACK_FAILED_INDEXES_CORRUPTED = 'Commit or rollback for a DB row failed. Indexes or index tags corrupted.';
  S_FROM_SCRATCH_REQUIRES_EMPTY_OBJ = 'Journal replay from scratch must start with empty objects.';
  S_INTERNAL_CHECKING_STRUCTURE = 'Internal error checking changed row structure';
  S_INTERNAL_CHECKING_INDEXES = 'Internal error checking indexed fields';
  S_FIELDS_NOT_SAME_AS_META = 'Changed fields not the same as metadata';
  S_LIST_HELPER_INTERNAL = 'List helper internal exception';
  S_TABLE_WITH_NO_FIELDS_HAS_ROWS = 'Streamed in table has rows but no fields!';
  S_JOURNAL_REPLAY_NAMES_INCONSISTENT = 'Journal replay, names inconsistent.';
  S_FK_FIELD_MISSING = 'Foreign key relationship: required data is missing.';
  S_FK_REFERENCES_TABLE = 'Cannot delete table, it is referenced by a foreign key.';
  S_FK_REFERENCES_INDEX = 'Cannot delete index, it is referenced by a foreign key.';

  S_API_TRANSACTION_IS_RO = 'Write operation attempted in read-only transaction.';
  S_API_TRANSACTION_NOT_RWEX = 'Write-exclusive operation attempted in write-shared or read-only transaction.';
  S_API_NO_SUCH_API_OBJECT = 'No API object, bad handle, or API ID not supported.';
  S_API_MODIFYING_META_FOR_DELETED_TABLE = 'Trying to modify metadata for previously deleted table. Commit or rollback first.';
  S_INDEX_NOT_YET_CREATED = 'Row navigation not possible, index not yet created (next commit).';
  S_API_NEXT_PREV_REQUIRES_CURRENT_ROW =
    'Navigating to next or previous row requires that you be on a row to start with.';
  S_API_NEXT_PREV_INTERNAL_ERROR = 'Internal error, couldn''t find current row';
  S_API_SEARCH_VAL_BAD_TYPE = 'Search key data must be of same type as field.';
  S_API_SEARCH_BAD_FIELD_COUNT = 'Bad field count in search data, or indexed fields';
  S_API_SEARCH_NO_INDEX_SPECIFIED = 'No index tag specified for API search on user index';
  S_API_SEARCH_INTERNAL = 'Data A/B buffer type disagrees with transaction isolation level';
  S_API_UNDELETING_ROW_AT_POST_TIME = 'Can''t modify a record previously deleted in same transaction';
  S_API_POST_COMMITTED_OVERWRITE =
    'Can''t modify a record read with "ReadComitted" isolation' +
    ' level if a previous read-write operation has already changed it.';
  S_API_DELETE_OVERWRITE =
    'Can''t delete a record read with "ReadComitted" isolation' +
    ' level if a previous read-write operation has already changed it.';
  S_API_READ_ROW_INTERNAL = 'No data in A/B buffer trying to read a row.';
  S_API_POST_INTERNAL = 'Internal indexing error trying to post changes.';
  S_API_DELETE_INTERNAL = 'Internal indexing error trying to delete a record.';
  S_FK_TABLE_NOT_FOUND = 'Table not found checking foreign key relationship.';
  S_FK_INDEX_NOT_FOUND = 'Index not found checking foreign key relationship.';
  S_FK_ONLY_ON_SF_INDEXES = 'Foreign key relationship only allowed between single field indexes at the moment.';
  S_FK_INDEX_FIELD_INTERNAL = 'Internal error checking foreign keys: field/index rearrangement.';
  S_INDEX_FOR_FK_UNIQUE_ATTR = 'Index referenced by foreign key must have unique attr set.';
  S_FK_INDEXES_DIFF_FIELDCOUNT = 'Indexes in foreign key must have same number of associated fields.';
  S_FK_FIELDS_DIFFERENT_TYPES = 'Fields in foreign key relationship must have same types.';
  S_FK_INTERNAL = 'Foreign key validation, internal error.';
  S_FK_INTERNAL_INDEX_TAG = 'Foreign key validation, internal error: bad index tag';
  S_FK_NOT_IN_REFERRED_TABLE = 'Foreign key: trying to add a key not found in referred table.';
  S_FK_IN_REFERRING_TABLE = 'Foreign key: trying to delete a key found in referring table.';
  S_FK_INTERNAL_OVERWRITE = 'Internal error: multiple rename conflicts should have been caught before this point.';
  S_ROW_IDS_DISAGREE = 'Journal replay: RowID''s disagree';
  S_INDEXED_LIST_LOOKAHEAD_FAILED = 'Indexed list lookahead failed, tag: ';
  S_TABLE_LOOKAHEAD_FAILED = 'Table lookahead failed, tag: ';
  S_DATABASE_LOOKAHEAD_FAILED = 'Main DB lookahead failed, tag: ';
  S_ASYNC_PROCESS_CANCELLED = 'Asynchronous computation cancelled in pre-commit step';
  S_INTERNAL_PARALLEL_OP = 'Internal error scheduling parallel operation';
  S_CHECK_TAGDATA_NO_TAG = 'Check Index Tag Data: does not exist';
  S_CHECK_TAGDATA_ARRAYSIZE = 'Check Index Tag Data: Tag lookaside bad size or out of date.';
  S_CHECK_TAGDATA_NOT_EXPECTED = 'Check Index Tag Data: Index tag to be checked not the one we expected at this IndexIndex.';
  S_CHECK_TAGDATA_NO_META = 'Check Index Tag Data: Expected current or next metadata copy to be available at this time.';
  S_CHECK_TAGDATA_NO_FIELD = 'Check Index Tag Data: Expected to find field referenced by index at this time';
  S_TAG_FAILED_INDEX_CLASS_CHECK = 'Check Index Tag Data: Index tag did not have the expected index class set';
  S_TAG_FAILED_DEFAULT_OFFSET_CHECK = 'Check Index Tag Data: Default field index in tag does not agree with metadata';
  S_TAG_FAILED_EXTRA_OFFSET_CHECK = 'Check Index Tag Data: Extra field index in tag does not agree with metadata';
  S_TAG_INDEX_BUILT_NOT_AS_EXPECTED = 'Check Index Tag Data: Index has not been built or destroyed when expected.';
  S_ADJUST_INDICES_NO_INDEX = 'Internal error: No index metadata item found when adjusting indices';
  S_ADJUST_INDICES_NO_FIELD = 'Internal error: No field metadata item found when adjusting indices';
  S_FOREIGN_KEY_UNDERLYING_TABLE_CHANGED = 'Table underlying foreign key changed location. Should be constant in spite of multi-renames.';
  S_FOREIGN_KEY_UNDERLYING_INDEX_CHANGED = 'Index underlying foreign key changed location. Should be constant in spite of multi-renames.';
  S_FOREIGN_KEY_UNDERLYING_FIELD_CHANGED = 'Field underlying foreign key changed location. Should be constant in spite of multi-renames.';
  S_FIELD_LIST_BAD_FOR_COLLATION = 'Collating data for index: Field list bad.';
  S_ZERO_LENGTH_FIELD_LIST_COLLATING = 'Collating data for index: Field list empty.';
  S_EMPTY_FIELD_DATA_IN_INDEX_COLLATION = 'Collating data for index: Field data empty';
  S_FIELDS_DIFFERENT_FORMAT_IN_INDEX_COLLATION_1 = 'Collating data for index: Fields different format (1).';
  S_FIELDS_DIFFERENT_FORMAT_IN_INDEX_COLLATION_2 = 'Collating data for index: Fields different format (2).';
  S_FIELD_LIST_BAD_FOR_COMPARISON = 'Bad field list object comparing sets of fields.';
  S_ZERO_LENGTH_FIELD_LIST_COMPARING = 'Comparing sets of fields, given a zero length field list.';
  S_DIFF_LENGTH_FIELD_LIST_COMPARING = 'Comparing sets of fields, given different length field lists.';
  S_ASYNC_INDEX_OP_FAILED = 'Asynchronous index build failed. Indexes are probably toast.';
  S_CURSOR_HAS_NO_ROW_AT_MODIFY = 'User mod of fields broken: no cursor assigned.';
  S_CURSOR_NOT_ASSIGNED_AT_DELETE = 'User delete of row broken: no cursor assigned';
  S_CURSOR_HAS_NO_ROW_AT_DELETE = 'User delete of row broken: no row associated with cursor';
  S_INTERNAL_UNSTREAM_EDIT = 'Internal indexing error during unstream operation';
  S_INTERNAL_META_EDIT = 'Internal indexing error during metadata processing';
  S_COMMIT_DELETES_API_OBECTS = 'Commit deletes an object underlying some API''s. Free the API''s first please.';
  S_JOURNAL_ROW_NOT_NEW = 'Expected row to be newly added, but it isn''t.';
  S_MODIFIED_CONCURRENT_ABORT = 'Data modified concurrently by another transaction, aborting. This might work if you retry, or lock exclusive.';
  S_JOURNAL_REPLAY_DUP_INST_ENTITY = 'Journal replay failed, trying to overwrite duplicate named entity.';
  S_JOURNAL_REPLAY_DUP_INST_ID = 'Journal replay failed, duplicate Row ID.';
  S_NEW_TAGS_UNEXPECTED_COUNT = 'Unexpected count of tags checking tag arrays.';
  S_NEW_TAGS_UNEXPECTED_OBJECT = 'Unexpected object encountered checking tag arrays';
  S_INTERNAL_INDEXING_ERROR = 'Internal indexing error (RowId''s).';
  S_JOURNAL_DATA_WITH_LAYOUT_CHANGE = 'Not allowed: Journal entry has both field rearrangement and data.';
  S_DATA_AND_LAYOUT_CHANGE_TO_JOURNAL = 'Not allowed: Table has both changed data and layout to journal!';
  S_ROW_MAINT_INTERNAL = 'Row maintenance: Expect commit/rollback from lookaside in order.';
  S_CONCURRENT_MODIFY_DURING_REPLAY = 'Unexpected concurrent journal modification during replay.';
  S_MODIFIED_CONCURRENT_ROW_OP = 'Aborted. Concurrency conflict. (Changing rows whilst other tx changes layout). Retry?';
  S_INTERNAL_REGEN_CHANGESETS_1 = 'Internal error regenating changesets (1).';
  S_INTERNAL_REGEN_CHANGESETS_2 = 'Internal error regenating changesets (2).';
  S_INTERNAL_REGEN_CHANGESETS_3 = 'Internal error regenating changesets (3).';
  S_INTERNAL_REGEN_CHANGESETS_4 = 'Internal error regenating changesets (4).';
  S_INTERNAL_REGEN_CHANGESETS_5 = 'Internal error regenating changesets (5).';
  S_INTERNAL_REGEN_CHANGESETS_6 = 'Internal error regenating changesets (6).';
  S_INDEX_FIELD_COUNTS_BAD_1 = 'Regen changesets: Query fields for index gave bad data (1).';
  S_INDEX_FIELD_COUNTS_BAD_2 = 'Regen changesets: Query fields for index gave bad data (2).';
  S_INDEX_REARRANGE_BAD = 'Regen changesets: Abs/Rel index field check failed despite previous checks.';
  S_TABLE_ADJUST_CONCURRENCY_CONFLICT = 'Concurrency conflict adjusting table format.';
  S_TABLE_ADJUST_REPEAT_FAILED = 'Repeated attempt to adjust table structure failed, time to rollback and start again.';
  S_TABLE_ADJUST_FAILED_1 = 'Internal error adjusting table structure (1).';
  S_TABLE_ADJUST_FAILED_2 = 'Internal error adjusting table structure (2).';
  S_MOVE_ROW_NOT_VALID_POSITION = 'User move of row must specify a valid position/direction.';
  S_LOCAL_INDEX_DELETION_BAD = 'Local index deletion failed or duplicate.';
  S_LOCAL_INDEX_INSERTION_DUPLICATE = 'Local index insertion duplicate.';
  S_LOCAL_INDEX_INSERTION_BAD_1 = 'Local index insertion failed (1).';
  S_LOCAL_INDEX_INSERTION_BAD_2 = 'Local index insertion failed (2).';
  S_INDEX_PIN_COLLATION_BAD = 'Index pin collation bad: Unexpected number of local INodes.';
  S_NAV_TABLE_METADATA_NOT_COMMITED = 'No committed metadata for this table (nav level)';
  S_FIELD_LIST_BAD_FOR_SV_COMPARISON = 'Field list bad (searchval comparison)';
  S_ZERO_LENGTH_FIELD_LIST_COMPARING_SV = 'Field list empty (searchval comparison)';
  S_SV_FIELD_COUNT_INCONSISTENT = 'Field count inconsistent (searchval comparison)';
  S_SV_FIELD_OFFSET_OOR = 'Field offset out of range (searchval comparison)';
  S_SV_FIELD_BAD = 'Field empty or deleted (searchval comparison)';
  S_SV_FIELDS_DIFFERENT_FORMAT = 'Field formats do not match (seachval comparison)';
  S_TABLE_FORMAT_CONCURRENTLY_CHANGED_NAV_0 = 'Concurrency: Table format changed whilst iterating (Nav 0).';
  S_TABLE_FORMAT_CONCURRENTLY_CHANGED_NAV_1 = 'Concurrency: Table format changed whilst iterating (Nav 1).';
  S_TABLE_FORMAT_CONCURRENTLY_CHANGED_NAV_2 = 'Concurrency: Table format changed whilst iterating (Nav 2).';
  S_ROW_DELETED_NAV_0 = 'Row unexpectedly deleted (Nav 0).';
  S_ROW_DELETED_NAV_1 = 'Row unexpectedly deleted (Nav 1).';
  S_ROW_DELETED_NAV_2 = 'Row unexpectedly deleted (Nav 2).';
  S_FIELDS_DONT_MATCH_IN_SEARCH = 'Internal error: Search Succeeded. but fields don''t match.';
  S_NEXT_PREV_BY_IDX_NO_INODE = 'Next/Previous by Idx. Cursor, but no INode!';
  S_NEXT_PREV_BY_IDX_INODE_FIND_FAILED = 'Next/Previous by Idx. Couldn''t find INode for index.';
  S_ERROR_GETTING_OFFSETS_IN_INDEX_BUILD = 'Error getting field defs/offsets during index build';
  S_PIN_FAILED_DURING_INDEX_BUILD = 'Pin for INode failed during index build';
  S_PIN_FAILED_DURING_INDEX_BUILD_2 = 'Pin for INode failed during index build, despite change flags indicating possible.';
  S_INSERT_FAILED_DURING_INDEX_BUILD = 'Tree insertion failed during index build.';
  S_PIN_FAILURE_DURING_INDEX_VALIDATE = 'Pin for cursor failed during index validation.';
  S_PIN_FIELDS_FAILURE_DURING_INDEX_VALIDATE = 'Pin fields failed during index validate, despite OK cursor.';
  S_SPARSENESS_DIFFERS_CHECKING_INDEX = 'Row formats differ checking index (should be all sparse, or all compact).';
  S_ERROR_IPINS_MODIFYING_INDEX = 'Confusion with IPins modifying index.';
  S_ERROR_MODIFYING_INDEX_REMOVE = 'Failed to remove from tree during index modification.';
  S_ERROR_PINNING_INDEX_ADD = 'Error adding pin during index modification.';
  S_ERROR_MODIFYING_INDEX_ADD = 'Error adding to tree during index modification.';
  S_ERROR_GETTING_OFFSETS_IN_INDEX_VALIDATE = 'Error getting field defs/offsets during index validation';

{ Misc Functions }

function BuildMultiDataRecs(FieldList: TMemStreamableList;
                          const AbsFieldOffsets: TFieldOffsets): TMemDbFieldDataRecs;
var
  i, L: integer;
  F: TObject;
  FieldData: TMemFieldData;
begin
  if not Assigned(FieldList) then
    raise EMemDbInternalException.Create(S_FIELD_LIST_BAD_FOR_COLLATION);
  //Expect some fields, because this is for index traverse and validation.
  L := Length(AbsFieldOffsets);
  if L = 0 then
    raise EMemDbInternalException.Create(S_ZERO_LENGTH_FIELD_LIST_COLLATING);
  SetLength(result, L);
  for i := 0 to Pred(L) do
  begin
    F := FieldList.Items[AbsFieldOffsets[i]];
    if (not Assigned(F)) or (F is TMemDeleteSentinel) then
      raise EMemDbInternalException.Create(S_EMPTY_FIELD_DATA_IN_INDEX_COLLATION);
    FieldData := F as TMemFieldData;
    result[i]:= FieldData.FDataRec
  end;
end;

function AllFieldsZero(FieldList: TMemStreamableList;
                            const AbsFieldOffsets: TFieldOffsets): boolean;
var
  i: integer;
  ZeroRec: TMemDbFieldDataRec;
  F: TObject;
  FieldData: TMemFieldData;

begin
  if not Assigned(FieldList) then
    raise EMemDbInternalException.Create(S_FIELD_LIST_BAD_FOR_COLLATION);
  //Expect some fields, because this is for index traverse and validation.
  FillChar(ZeroRec, sizeof(ZeroRec), 0);
  result := true;
  for i := 0 to Pred(Length(AbsFieldOffsets)) do
  begin
    F := FieldList.Items[AbsFieldOffsets[i]];
    if (not Assigned(F)) or (F is TMemDeleteSentinel) then
      raise EMemDbInternalException.Create(S_EMPTY_FIELD_DATA_IN_INDEX_COLLATION);
    FieldData := F as TMemFieldData;
    ZeroRec.FieldType := FieldData.FDataRec.FieldType;
    if not DataRecsSame(ZeroRec, FieldData.FDataRec) then
    begin
      result := false;
      exit;
    end;
  end;
end;

function SearchValFieldsSame(SvRecs:TMemDbFieldDataRecs; Fields: TMemStreamableList; const FieldOffsets: TFieldOffsets): boolean;
var
  L, F: integer;
  S: TMemDBStreamable;
  Field: TMemFieldData;
begin
  result := true;
  if not (Assigned(SvRecs) and Assigned(Fields) and Assigned(FieldOffsets)) then
    raise EMemDbInternalException.Create(S_FIELD_LIST_BAD_FOR_SV_COMPARISON);
  L := Length(FieldOffsets);
  if (L = 0) then
    raise EMemDbInternalException.Create(S_ZERO_LENGTH_FIELD_LIST_COMPARING_SV);
  if L <> Length(SVRecs) then
    raise EMemDbInternalException.Create(S_SV_FIELD_COUNT_INCONSISTENT);
  for L := 0 to Pred(Length(SVRecs)) do
  begin
    F := FieldOffsets[L];
    if (F < 0) or (F >= Fields.Count) then
      raise EMemDbInternalException.Create(S_SV_FIELD_OFFSET_OOR);
    S := Fields[F];
    if NotAssignedOrSentinel(S) then
      raise EMemDbInternalException.Create(S_SV_FIELD_BAD);
    Field := S as TMemFieldData;
    if Field.FDataRec.FieldType <> SvRecs[L].FieldType then
      raise EMemDbInternalException.Create(S_SV_FIELDS_DIFFERENT_FORMAT);
    if not DataRecsSame(Field.FDataRec, SvRecs[L]) then
    begin
      result := false;
      exit;
    end;
  end;
end;

function SameLayoutFieldsSame(A,B: TMemStreamableList; FieldOffsets: TFieldOffsets): boolean;
var
  L,i: integer;
  OA, OB: TMemDBStreamable;
  FA,FB: TMemFieldData;
begin
  if not (Assigned(A) and Assigned(B))  then
    raise EMemDbInternalException.Create(S_FIELD_LIST_BAD_FOR_COMPARISON);
  L := Length(FieldOffsets);
  if L = 0 then
    raise EMemDbInternalException.Create(S_ZERO_LENGTH_FIELD_LIST_COMPARING);
  //Happy to have list range check errors as native exceptions.
  //Field offsets does not necessarily have to cover all the fields.
  result := true;
  for i := 0 to Pred(L) do
  begin
    OA := A.Items[FieldOffsets[i]];
    OB := B.Items[FieldOffsets[i]];
    if NotAssignedOrSentinel(OA) or NotAssignedOrSentinel(OB) then
      raise EMemDbInternalException.Create(S_EMPTY_FIELD_DATA_IN_INDEX_COLLATION);
    FA := OA as TMemFieldData;
    FB := OB as TMemFieldData;
    if not (FA.FDataRec.FieldType = FB.FDataRec.FieldType) then
      raise EMemDbInternalException.Create(S_FIELDS_DIFFERENT_FORMAT_IN_INDEX_COLLATION_1);
    if not DataRecsSame(FA.FDataRec, FB.FDataRec) then
    begin
      result := false;
      exit;
    end;
  end;
end;

function DiffLayoutFieldsSame(A: TMemStreamableList; AOffsets: TFieldOffsets;
                              B: TMemStreamableList; BOffsets: TFieldOffsets):boolean;
var
  LA,LB,i: integer;
  OA, OB: TMemDBStreamable;
  FA,FB: TMemFieldData;
begin
  if not (Assigned(A) and Assigned(B))  then
    raise EMemDbInternalException.Create(S_FIELD_LIST_BAD_FOR_COMPARISON);
  LA := Length(AOffsets);
  if LA = 0 then
    raise EMemDbInternalException.Create(S_ZERO_LENGTH_FIELD_LIST_COMPARING);
  LB := Length(BOffsets);
  if LA <> LB then
    raise EMemDbInternalException.Create(S_DIFF_LENGTH_FIELD_LIST_COMPARING);
  //Happy to have list range check errors as native exceptions.
  //Field offsets does not necessarily have to cover all the fields.
  result := true;
  for i := 0 to Pred(LA) do
  begin
    OA := A.Items[AOffsets[i]];
    OB := B.Items[BOffsets[i]];
    if NotAssignedOrSentinel(OA) or NotAssignedOrSentinel(OB) then
      raise EMemDbInternalException.Create(S_EMPTY_FIELD_DATA_IN_INDEX_COLLATION);
    FA := OA as TMemFieldData;
    FB := OB as TMemFieldData;
    if not (FA.FDataRec.FieldType = FB.FDataRec.FieldType) then
      raise EMemDbInternalException.Create(S_FIELDS_DIFFERENT_FORMAT_IN_INDEX_COLLATION_2);
    if not DataRecsSame(FA.FDataRec, FB.FDataRec) then
    begin
      result := false;
      exit;
    end;
  end;
end;

{ TMemDBAPI }

procedure TMemDBAPI.CheckWriteTransaction;
begin
  if not
    ((FAssociatedTransaction as TMemDBTransaction).Mode
     in [amWriteShared, amWriteExclusive]) then
    raise EMemDBAPIException.Create(S_API_TRANSACTION_IS_RO);
end;

function TMemDBAPI.GetApiObject(ID: TMemDBAPIId; RaiseIfNoAPI: boolean = true): TMemDBAPI;
begin
  result := FInterfacedObject.GetAPIObject(FAssociatedTransaction, ID);
  if (not Assigned(result)) and RaiseIfNoAPI then
    raise EMemDBAPIException.Create(S_API_NO_SUCH_API_OBJECT);
end;

destructor TMemDBAPI.Destroy;
begin
  FInterfacedObject.PutAPIObjectLocal(self);
  inherited;
end;

{ TMemDBAPIInterfacedObject }

constructor TMemDBAPIInterfacedObject.Create;
begin
  FAPIObjects := TList.Create;
  FAPIListLock := TCriticalSection.Create;
  inherited;
end;

destructor TMemDBAPIInterfacedObject.Destroy;
begin
  Assert(FAPIObjects.Count = 0);
  FAPIObjects.Free;
  FAPIListLock.Free;
  inherited;
end;

procedure TMemDBAPIInterfacedObject.DoAddRefForApi;
begin
  if Assigned(FOnAddRefForApi) then
    FOnAddRefForApi(self);
end;

procedure TMemDBAPIInterfacedObject.DoReleaseForApi;
begin
  if Assigned(FOnReleaseForApi) then
    FOnReleaseForApi(self);
end;

function TMemDBAPIInterfacedObject.DoAPIObjectRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;
begin
  if Assigned(FAPIObjectRequest) then
    result := FApiObjectRequest(Transaction, ID)
  else
    result := nil;
end;

function TMemDBAPIInterfacedObject.GetAPIObject(Transaction: TObject; ID: TMemDBAPIId; RaiseIfNoAPI: boolean = true): TMemDBAPI;
var
  DoAddRef: boolean;
begin
  DoAddRef := false;
  FAPIListLock.Acquire;
  try
    result := DoAPIObjectRequest(Transaction, ID);
    if Assigned(result) then
    begin
      result.FAssociatedTransaction := Transaction;
      result.FInterfacedObject := self;
      DoAddRef := FAPIObjects.Count = 0;
      FAPIObjects.Add(result);
      if Assigned(Transaction) then
        (Transaction as TMemDbTransaction).RegisterCreatedApi(result);
    end
    else
    begin
      if RaiseIfNoAPI then
        raise EMemDBAPIException.Create(S_API_NO_SUCH_API_OBJECT);
    end;
  finally
    FAPIListLock.Release;
  end;
  //This window is OK if other references exist on interfaced, which they should.
  if DoAddRef then
    DoAddRefForApi;
end;

procedure TMemDBAPIInterfacedObject.PutAPIObjectLocal(API:TMemDBAPI);
var
  Idx: integer;
  DoRelease: boolean;
begin
  FAPIListLock.Acquire;
  try
    Assert(Assigned(API));
    Assert(API.FInterfacedObject = self);
    Idx := FAPIObjects.IndexOf(API);
    Assert(Idx >= 0);
    FAPIObjects.Delete(Idx);
    if Assigned(API.FAssociatedTransaction) then
      (API.FAssociatedTransaction as TMemDBTransaction).DeregisterCreatedApi(API);
    DoRelease := FAPIObjects.Count = 0;
  finally
    FAPIListLock.Release;
  end;
  if DoRelease then
    DoReleaseForApi;
end;

{ TMemDBCursor }

procedure TMemDBCursor.SetRowPin(const Tid: TTransactionId; NewRow: TMemDBRow);
begin
  //This after initial pin from surrounding code.
  if (NewRow <> FRow) then
  begin
    if Assigned(FRow) then
      FRow.UnPinCursor(FPinTid);
    FPinTid := Tid;
    FRow := NewRow;
  end;
end;

destructor TMemDbCursor.Destroy;
begin
  SetRowPin(TTransactionId.Empty, nil);
  inherited;
end;

{ TMemDBIndexedList }


{$IFDEF MEMDB2_TEMP_REMOVE}

constructor TMemDBIndexedList.Create;
begin
  inherited;
  FStore := TIndexedStoreO.Create;
  DLList.DLItemInitList(@self.FRefChangedRows);
  DLList.DLItemInitList(@self.FRefEmptyRows);
end;

destructor TMemDBIndexedList.Destroy;
begin
  Assert(DLList.DlItemIsEmpty(@FRefChangedRows));
  Assert(DLList.DlItemIsEmpty(@FRefEmptyRows));
  FStore.Free;
  inherited;
end;

//This is just a refactor of Row consistency flags,
//hopefully more speedily, whilst not using the is
//operator too much.
function TMemDbIndexedList.WhichList(Row:TMemDbRow): PDLEntry;
begin
  if (Row.Added or Row.Changed or Row.Deleted) then
  begin
    Assert(not Row.Null);
    Result := @FRefChangedRows;
  end
  else if Row.Null then
  begin
    Assert(not (Row.Added or Row.Changed or Row.Deleted));
    Result := @FRefEmptyRows;
  end
  else
    Result := nil;
end;

procedure TMemDbIndexedList.RmRowQuickLists(Row: TMemDBRow; var LPos:PDLEntry; var LHead: PDLEntry);
begin
  //Allow removal if not already in any lists.
  Assert(DlItemIsEmpty(@Row.FQuickRef) = not Assigned(Row.FOwningListHead));
  if not DLItemIsEmpty(@Row.FQuickRef) then
  begin
    Assert(Row.FOwningListHead = WhichList(Row));
    LPos := Row.FQuickRef.BLink;
    LHead := Row.FOwningListHead;
    DLList.DLListRemoveObj(@Row.FQuickRef);
    Row.FOwningListHead := nil;
  end
  else
  begin
    LPos := nil;
    LHead := nil;
  end;
  Assert(Assigned(LPos) = Assigned(LHead));
  Assert(DlItemIsEmpty(@Row.FQuickRef) = not Assigned(Row.FOwningListHead));
end;

procedure TMemDBIndexedList.AddRowQuicklists(Row: TMemDbRow; LPos: PDLEntry; LHead: PDLEntry);
var
  NewLHead: PDLEntry;
begin
  //Do not allow addition if already in a list.
  Assert(Assigned(LPos) = Assigned(LHead));
  Assert(DlItemIsEmpty(@Row.FQuickRef) = not Assigned(Row.FOwningListHead));

  if not DLItemIsEmpty(@Row.FQuickRef) then
    raise EMemDBInternalException.Create(S_QUICK_LIST_DUPLICATE_INSERTION);
  //Where we previously removed it from.

  NewLHead := WhichList(Row);
  if Assigned(NewLHead) then
  begin
    //Insert it back into a list.
    if NewLHead = LHead then
      //Insert onto old list where it was.
      DLList.DLItemInsertAfter(LPos, @Row.FQuickRef)
    else
      //Insert into different list at the end.
      DLList.DLListInsertTail(NewLHead, @Row.FQuickRef);
    Row.FOwningListHead := NewLHead;
  end;
  //Else don't insert.
end;

procedure TMemDBIndexedList.ToScratch(Stream: TStream);
var
  IRec: TItemRec;
  Item: TMemDBJournalCreator;
  RV: TIsRetVal;
begin
  WrTag(Stream, mstIndexedListStart);
  IRec := FStore.GetAnItem;
  while Assigned(IRec) do
  begin
    Item := IRec.Item as TMemDBJournalCreator;
    Item.ToScratch(Stream);
    RV := FStore.GetAnotherItem(IRec);
    Assert(RV in [rvOK, rvNotFound]);
  end;
  WrTag(Stream, mstIndexedListEnd);
end;

procedure TMemDBIndexedList.FromJournal(Stream: TStream);
var
  Row: TMemDBRow;
  IRec: TItemRec;
  RV: TIsRetVal;
  LPos: PDLEntry;
  QLPos, QLHead: PDLEntry;
begin
  ExpectTag(Stream, mstIndexedListStart);
  Row := LookaheadHelper(Stream, IRec);
  while Assigned(Row) do
  begin
    if Assigned(IRec) then
    begin
      Row := IRec.Item as TMemDBRow;
      RV := FStore.RemoveItemInPlace(IRec, LPos);
      if RV <> rvOK then
        raise EMemDBInternalException.Create(S_INTERNAL_UNSTREAM_EDIT);
      Assert(RV = rvOK);
      Row.FStoreBackRec := nil;
      RmRowQuickLists(Row, QLPos, QLHead);
    end
    else
    begin
      LPos := nil;
      QLPos := nil;
      QLHead := nil;
    end;
    Row.FromJournal(Stream);
    RV := FStore.AddItemInPlace(Row, LPos, IRec);
    if RV <> rvOK then
      raise EMemDBInternalException.Create(S_INTERNAL_UNSTREAM_EDIT);
    Assert(Assigned(IRec));
    Row.FStoreBackRec := IRec;
    AddRowQuicklists(Row, QLPos, QLHead);
    Row := LookaheadHelper(Stream, IRec);
  end;
  ExpectTag(Stream, mstIndexedListEnd);
end;

procedure TMemDBIndexedList.FromScratch(Stream: TStream);
var
  Row: TMemDbRow;
  IRec: TItemRec;
  RV: TIsRetVal;
begin
  ExpectTag(Stream, mstIndexedListStart);
  Row := LookaheadHelper(Stream, IRec);
  while Assigned(Row) do
  begin
    if Assigned(IRec) then
      raise EMemDBException.Create(S_JOURNAL_REPLAY_DUP_INST);
    Row.FromScratch(Stream);
    RV := FStore.AddItem(Row, IRec);
    if RV <> rvOK then
      raise EMemDBInternalException.Create(S_INTERNAL_UNSTREAM_EDIT);
    AddRowQuicklists(Row, nil, nil);
    Assert(Assigned(IRec));
    Row.FStoreBackRec := IRec;
    Row := LookaheadHelper(Stream, IRec);
  end;
  ExpectTag(Stream, mstIndexedListEnd);
end;


function TMemDBIndexedList.LookaheadHelper(Stream: TStream; var IRec: TItemRec): TMemDBRow;
var
  Pos: Int64;
  Tag: TMemStreamTag;
  RowId: TGUID;
  RowIDString: string;
  SV: TMemDBIndexNodeSearchVal;
  RV: TISRetVal;
begin
  Pos := Stream.Position;
  Tag := RdTag(Stream);
  case Tag of
    DEPRECATED_mstRowStartV1, mstRowStartV2:
    begin
      if Tag = DEPRECATED_mstRowStartV1 then
      begin
        RowIdString := RdStreamString(Stream);
        RowId := StringToGUID(RowIdString);
      end
      else
      begin
        RowId := RdGuid(Stream);
      end;
      SV := TMemDBIndexNodeSearchVal.Create;
      try
        SV.IdSearchVal := RowId;
        RV := Store.FindByIndex(MDBInternalIndexRowId.TagStructs[sicCurrent], SV, IRec);
      finally
        SV.Free;
      end;
      if RV = rvOK then
        result := IRec.Item as TMemDBRow
      else
      begin
        Assert(not Assigned(IRec));
        result := TMemDBRow.Create;
        result.RowId := RowId;
      end;
    end;
    mstIndexedListEnd: result := nil;
  else
    raise EMemDBException.Create(S_INDEXED_LIST_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
  end;
  Stream.Seek(Pos, TSeekOrigin.soBeginning);
end;

{$ENDIF}

{ TMemDBTableList }

{$IFDEF MEMDB2_TEMP_REMOVE}

constructor TMemDBTableList.Create;
var
  RV: TIsRetVal;
begin
  inherited;
  RV := FStore.AddIndex(TMemDBIndexNode, MDBInternalIndexRowId.TagStructs[sicCurrent], false);
  Assert(RV = rvOK);
end;

destructor TMemDBTableList.Destroy;
var
  IRec: TItemRec;
  O: TObject;
  Tag: pointer;
  NodeClass: TIndexNodeClass;
  RV: TIsRetVal;
  Idx: integer;
begin
  for Idx := 0 to Pred(FStore.IndexCount) do
  begin
    RV := FStore.IndexInfoByOrdinal(Idx, Tag, NodeClass);
    Assert(RV = rvOK);
    RV := FStore.DeleteIndex(Tag, false);
    Assert(RV = rvOK);
  end;
  //Unlike other arbitrary indexed lists, we almost definitely own
  //the table rows.
  while FStore.Count > 0 do
  begin
    IRec := FStore.GetAnItem;
    O := IRec.Item;
    RV := FStore.RemoveItem(IRec);
    //Remove from quick lists performed by free.
    Assert(RV = rvOK);
    O.Free;
  end;
  inherited;
end;

function TMemDBTableList.AnyChangesAtAll:boolean;
begin
  result := inherited;
  result := result or (not DLItemIsEmpty(@Self.FRefChangedRows));
end;

procedure TMemDBTableList.ForEachRow(Handler: TSelectiveRowHandler;
                                     Ref1, Ref2: TObject);
var
  Current, Next: TItemRec;
  CurrentRow: TMemDBRow;
begin
  Current := FStore.GetAnItem;
  while Assigned(Current) do
  begin
    Next := Current;
    FStore.GetAnotherItem(Next);
{$IFOPT C+}
    CurrentRow := Current.Item as TMemDbRow;
{$ELSE}
    CurrentRow := TMemDbRow(Current.Item);
{$ENDIF}
    //If handler removes and reinserts, then it should use store and quicklist
    //"in-place" functions to ensure ordering in all three lists is kept same,
    //otherwise chaos will ensue.
    Handler(CurrentRow, Ref1, Ref2, self);
    Current := Next;
  end;
end;

procedure TMemDBTableList.ForEachChangedRow(Handler: TSelectiveRowHandler;
                            Ref1, Ref2: TObject);
var
  Current, Next: PDLEntry;
  CurrentRow: TMemDBRow;
begin
  Current := FRefChangedRows.FLink;
  while Assigned(Current.Owner) do
  begin
    Next := Current.FLink;
{$IFOPT C+}
    CurrentRow := Current.Owner as TMemDbRow;
{$ELSE}
    CurrentRow := TMemDbRow(Current.Owner);
{$ENDIF}
    //If handler removes and reinserts, then it should use store and quicklist
    //"in-place" functions to ensure ordering in all three lists is kept same,
    //otherwise chaos will ensue.
    Handler(CurrentRow, Ref1, Ref2, self);
    Current := Next;
  end;
end;

// If only use this when changes commited, then don't need delete sentinels.
// Don't use on uncomitted changes.
// As elsewhere in code, we are solving the "delete newly added" problem
// by saying "go away, and roll back the transaction".
procedure TMemDBTableList.ForEachEmptyRow(Handler: TSelectiveRowHandler;
                                              Ref1, Ref2: TObject);
var
  Current, Next: PDLEntry;
  CurrentRow: TMemDBRow;
begin
  Current := FRefEmptyRows.FLink;
  while Assigned(Current.Owner) do
  begin
    Next := Current.FLink;
{$IFOPT C+}
    CurrentRow := Current.Owner as TMemDbRow;
{$ELSE}
    CurrentRow := TMemDbRow(Current.Owner);
{$ENDIF}
    //If handler removes and reinserts, then it should use store and quicklist
    //"in-place" functions to ensure ordering in all three lists is kept same,
    //otherwise chaos will ensue.
    Handler(CurrentRow, Ref1, Ref2, self);
    Current := Next;
  end;
end;

procedure TMemDBTableList.WriteRowToJournal(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);
var
  Stream: TStream;
begin
{$IFOPT C+}
  Stream := Ref1 as TStream;
{$ELSE}
  Stream := TStream(Ref1);
{$ENDIF}
  Row.ToJournal(Stream);
end;


procedure TMemDbTableList.ToJournal(Stream: TStream);
begin
  if AnyChangesAtAll then
  begin
    WrTag(Stream, mstIndexedListStart);
    ForEachChangedRow(WriteRowToJournal, Stream, nil);
    WrTag(Stream, mstIndexedListEnd);
  end;
end;

procedure TMemDBTableList.CommitRollbackChangedRow(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);
var
  rv: TIsRetVal;
  LPos: PDLEntry;
  QLPos, QLHead: PDLEntry;
begin
  //Bad indexing will most likely show up here.
  rv := FStore.RemoveItemInPlace(Row.FStoreBackRec, LPos);
  if rv <> rvOK then
    raise EMemDBInternalException.Create(S_COMMIT_ROLLBACK_FAILED_INDEXES_CORRUPTED);
  Row.FStoreBackRec := nil;
  RmRowQuickLists(Row, QLPos, QLHead);
  //Remove before comitting (indexing)
  if LongBool(Ref1) then
    Row.Commit(TMemDbTransReason(Ref2))
  else
    Row.Rollback(TMemDBTransReason(Ref2));
  //And re-add after comitting (indexing)
  rv := FStore.AddItemInPlace(Row, LPos, Row.FStoreBackRec);
  if rv <> rvOK then
    raise EMemDBInternalException.Create(S_COMMIT_ROLLBACK_FAILED_INDEXES_CORRUPTED);
  Assert(Assigned(Row.FStoreBackRec));
  AddRowQuicklists(Row, QLPos, QLHead);
end;

procedure TMemDBTableList.RemoveEmptyRow(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);
begin
  FStore.RemoveItem(Row.FStoreBackRec);
  //Back ptr, quicklists cleared by free.
  Row.Free;
end;

//Optimised versions only do changed rows, and subsequent cleanup.
procedure TMemDBTableList.Commit(Reason: TMemDBTransReason);
begin
  inherited;
  //Replay from scratch optimization, stream directly into current copy.
  ForEachChangedRow(CommitRollbackChangedRow, TObject(true), TObject(Reason));
  ForEachEmptyRow(RemoveEmptyRow, nil, nil);
end;

procedure TMemDBTableList.Rollback(Reason: TMemDBTransReason);
begin
  inherited;
  ForEachChangedRow(CommitRollbackChangedRow, TObject(false), TObject(Reason));
  ForEachEmptyRow(RemoveEmptyRow, nil, nil);
end;

function TMemDBTableList.MetaEditFirst: TEditRec;
var
  IRec: TItemRec;
  rv: TISRetVal;
begin
  rv := FStore.FirstByIndex(MDBInternalIndexRowId.TagStructs[sicCurrent], IRec);
  Assert(rv in [rvOk, rvNotFound]);
  if Assigned(IRec) then
  begin
    result := TEditRec.Create;
    result.IRec := IRec;
    rv := FStore.NextByIndex(MDBInternalIndexRowId.TagStructs[sicCurrent], IRec);
    Assert(rv in [rvOk, rvNotFound]);
    result.NextIRec := IRec;
  end
  else
    result := nil;
end;

function TMemDBTableList.MetaEditNext(EditRec: TEditRec):TEditRec;
var
  IRec: TItemRec;
  rv: TISRetVal;
begin
  Assert(Assigned(EditRec));
  EditRec.IRec := EditRec.NextIRec;
  if Assigned(EditRec.IRec) then
  begin
    result := EditRec;
    IRec := EditRec.IRec;
    rv := FStore.NextByIndex(MDBInternalIndexRowId.TagStructs[sicCurrent], IRec);
    Assert(rv in [rvOk, rvNotFound]);
    result.NextIRec := IRec;
  end
  else
  begin
    EditRec.Free;
    result := nil;
  end;
end;

function TMemDBTableList.MetaRemoveRowForEdit(EditRec: TEditRec): TMemDBRow;
var
  rv: TISRetVal;
begin
  Assert(Assigned(EditRec));
  Assert(Assigned(EditRec.IRec));
  Assert(not Assigned(EditRec.RemovedItem));
  Assert(not Assigned(EditRec.Lpos));
  Assert(not Assigned(EditRec.QLpos));
  Assert(not Assigned(EditRec.QLhead));
  result := EditRec.IRec.Item as TMemDBRow;
  rv := FStore.RemoveItemInPlace(EditRec.IRec, EditRec.LPos);
  if RV <> rvOK then
    raise EMemDBInternalException.Create(S_INTERNAL_META_EDIT);
  EditRec.RemovedItem := result;
  result.FStoreBackRec := nil;
  RmRowQuickLists(result, EditRec.QLPos, EditRec.QLHead);
  EditRec.IRec := nil;
end;

procedure TMemDBTableList.MetaInsertRowAfterEdit(EditRec: TEditRec; Row: TMemDBRow);
var
  rv: TISRetVal;
begin
  Assert(Assigned(EditRec));
  Assert(Assigned(Row));
  Assert(EditRec.RemovedItem = Row);
  Assert(Assigned(EditRec.LPos));
  Assert(not Assigned(EditRec.IRec));
  rv := FStore.AddItemInPlace(Row, EditRec.LPos, EditRec.IRec);
  if RV <> rvOK then
    raise EMemDBInternalException.Create(S_INTERNAL_META_EDIT);
  Assert(Assigned(EditRec.IRec));
  Row.FStoreBackRec := EditRec.IRec;
  AddRowQuicklists(Row,EditRec.QLPos, EditRec.QLHead);
  EditRec.RemovedItem := nil;
  EditRec.LPos := nil;
  EditRec.QLPos := nil;
  EditRec.QLHead := nil;
  //But next item will be that previously determined, not that obtained
  //by a new "get next" call.
end;

procedure TMemDBTableList.ReadRowData(Row: TMemDBRow; Iso: TMDBIsolationLevel;
                      Fields: TMemStreamableList);
var
  AB: TABSelection;
  RowFields: TMemStreamableList;
begin
  Assert(Assigned(Row));
  AB := IsoToAB(Iso);
  if NotAssignedOrSentinel(Row.ABData[AB]) then
    raise EMemDbInternalException.Create(S_API_READ_ROW_INTERNAL);
  RowFields := Row.ABData[AB] as TMemStreamableList;
  Assert(Assigned(Fields));
  Fields.DeepAssign(RowFields);
end;


procedure TMemDBTableList.DeleteRow(var Cursor: TItemRec; Iso: TMDBIsolationLevel);
var
  IRet: TISRetVal;
  LPos: PDLEntry;
  Row: TMemDbRow;
  QLPos, QLHead: PDLEntry;
begin
  if not Assigned(Cursor) then
    raise EMemDbInternalException.Create(S_CURSOR_NOT_ASSIGNED_AT_DELETE);
{$IFOPT C+}
  Row := Cursor.Item as TMemDBRow;
{$ELSE}
  Row := TMemDBRow(Cursor.Item);
{$ENDIF}
  if not Assigned(Row) then
    raise EMemDbInternalException.Create(S_CURSOR_HAS_NO_ROW_AT_DELETE);
  if Assigned(Row.ABData[abNext]) then
  begin
    //Already deleted.
    if Row.ABData[abNext] is TMemDeleteSentinel then
      exit;
    //Modified, and we are deleting on the basis of older data.
    if Iso <> ilDirtyRead then
      raise EMemDBAPIException.Create(S_API_DELETE_OVERWRITE);
  end;
  IRet := Store.RemoveItemInPlace(Cursor, LPos);
  if IRet <> rvOK then
    raise EMemDBInternalException.Create(S_API_DELETE_INTERNAL);
  Row.FStoreBackRec := nil;
  RmRowQuickLists(Row, QLPos, QLHead);
  Row.Delete;
  IRet := Store.AddItemInPlace(Row, LPos, Cursor);
  if IRet <> rvOK then
    raise EMemDBInternalException.Create(S_API_DELETE_INTERNAL);
  Row.FStoreBackRec := Cursor;
  AddRowQuickLists(Row, QLPos, QLHead);
end;

{$ENDIF}

{ TMemDBEntity }

constructor TMemDBEntity.Create;
begin
  inherited;
  FInterfaced := TMemDBAPIInterfacedObject.Create;
  FInterfaced.FAPIObjectRequest := HandleInterfacedObjRequest;
  FInterfaced.FOnAddRefForApI := HandleAddRefForAPI;
  FInterfaced.FOnReleaseForAPI := HandleReleaseForAPI;
  FInterfaced.FParent := Self;
  FProxy := TMemDBEntityProxy.Create;
  FProxy.Proxy := self;
end;

destructor TMemDBEntity.Destroy;
begin
  FInterfaced.Free;
  //Proxy is freeing us.
  inherited;
end;

function TMemDBEntity.HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;
begin
  result := nil;
end;

procedure TMemDBEntity.HandleAddRefForAPI(Sender: TObject);
begin
  Assert(Sender = FInterfaced);
  Assert(Assigned(FProxy));
  FProxy.AddRef;
end;

procedure TMemDBEntity.HandleReleaseForAPI(Sender: TObject);
begin
  Assert(Sender = FInterfaced);
  Assert(Assigned(FProxy));
  FProxy.Release;
end;

procedure TMemDBEntity.MetadataDCPHandle(Sender: TObject; const Update:TReferenceUpdate);
begin
  Assert(Sender = FMetadata);
  Assert(Assigned(FProxy));
  if Update.Pre <> Update.Post then
  begin
    if Update.Post then
    begin
      FProxy.AddRef;
      //If DCP then in entity list, AddRef is for list ownership.
      FParentDB.FEntityLock.Acquire;
      try
        DLListInsertTail(@FParentDB.FEntityList, @FProxy.FSiblings);
      finally
        FParentDB.FEntityLock.Release;
      end;
    end
    else
    begin
      FParentDB.FEntityLock.Acquire;
      try
        DLListRemoveObj(@FProxy.FSiblings);
      finally
        FParentDB.FEntityLock.Release;
      end;
      FProxy.Release;
    end;
  end;
end;

procedure TMemDBEntity.PreCommit(const TId: TTransactionId; Reason: TMemDBTransReason);
begin
  inherited;
  FMetadata.PreCommit(Tid, Reason);
end;

procedure TMemDBEntity.Commit(const TId: TTransactionId; Reason: TMemDBTransReason);
begin
  inherited;
  FMetadata.Commit(Tid, Reason);
end;

procedure TMemDBEntity.Rollback(const TId: TTransactionId; Reason: TMemDBTransReason);
begin
  inherited;
  FMetadata.Rollback(Tid, Reason);
end;

procedure TMemDBEntity.Init(const Tid: TTransactionId; Parent: TObject; Name:string; DSName: boolean);
begin
  Assert(Assigned(Parent));
  Assert(Parent is TMemDBDatabasePersistent);
  FParentDB := Parent as TMemDBDatabasePersistent;
  FMetadata.Init(Tid, self, Name, DSName);
end;

function TMemDBEntity.META_PinCurrent(const Tid: TTransactionId; Reason: TPinReason): TMemDBStreamable;
begin
  result := FMetadata.PinCurrent(Tid, Reason);
end;

function TMemDBEntity.META_GetNext(const Tid: TTransactionId): TMemDBStreamable;
begin
  result := FMetadata.GetNext(Tid);
end;

function TMemDBEntity.META_GetPinLatest(const Tid: TTransactionId;
                        var BufSelected: TAbSelType; Reason: TPinReason): TMemDBStreamable;
begin
  result := FMetadata.GetPinLatest(Tid, BufSelected, Reason);
end;

procedure TMemDBEntity.META_RequestChange(const Tid: TTransactionId);
begin
  FMetadata.RequestChange(Tid, ilReadRepeatable);
end;

procedure TMemDbEntity.META_Delete(const Tid: TTransactionId);
begin
  FMetadata.Delete(Tid, ilReadRepeatable);
end;

{ TTidLocal }

procedure TTidLocal.HandleHelperChangeRequest(Sender: TObject);
begin
  FParentTable.FMetadata.RequestChange(FTid, ilReadRepeatable);
  UpdateHelpersAndIndices(pinEvolve);
end;

procedure TTidLocal.UpdateLayout(PinReason: TPinReason);
begin
  UpdateHelpersAndIndices(PinReason);
  DimensionChangesets(PinReason);
  ReGenChangesets(PinReason);
end;

procedure TTidLocal.UpdateHelpersAndIndices(PinReason: TPinReason);
var
  CC, NC: TMemTableMetadataItem;
  M: TMemDbStreamable;
  i: integer;
  MasterIndex: TMemDBIndex;
begin
  if not Assigned(FIndexHelper) then
  begin
    FIndexHelper :=  TMemDblBufListHelper.Create;
    FIndexHelper.OnChangeRequest := HandleHelperChangeRequest;
  end;
  if not Assigned(FFieldHelper) then
  begin
    FFieldHelper := TMemDblBufListHelper.Create;
    FFieldHelper.OnChangeRequest := HandleHelperChangeRequest;
  end;
  if not Assigned(FEmptyList) then
    FEmptyList := TMemStreamableList.Create;

  //First metadata pin for a Txion has to be atomic with index clone.
  if not FIndexInit then
  begin
    FParentTable.FMetaIndexLock.Acquire;
    try
      M := FParentTable.FMetadata.PinCurrent(FTid, PinReason);
      if Assigned(M) then
      begin
        FIndexInit := true;
        Assert(FLocalIndexCopies.Count = 0);
        for i := 0 to Pred(FParentTable.FMasterIndexes.Count) do
        begin
          MasterIndex := FParentTable.FMasterIndexes.Items[i] as TMemDBIndex;
          FLocalIndexCopies.AddNoRef(MasterIndex.Clone);
        end;
      end;
    finally
      FParentTable.FMetaIndexLock.Release;
    end;
  end
  else
    M := FParentTable.FMetadata.PinCurrent(FTid, PinReason);

  if AssignedNotSentinel(M) then
  begin
    CC := M as TMemTableMetadataItem;
    FFieldHelper.List[abCurrent] := CC.FieldDefs;
    FIndexHelper.List[abCurrent] := CC.IndexDefs;
  end
  else
  begin
    FFieldHelper.List[abCurrent] := FEmptyList;
    FIndexHelper.List[abCurrent] := FEmptyList;
  end;
  M := FParentTable.FMetadata.GetNext(FTid);
  if AssignedNotSentinel(M) then
  begin
    NC := M as TMemTableMetadataItem;
    FFieldHelper.List[abNext] := NC.FieldDefs;
    FIndexHelper.List[abNext] := NC.IndexDefs;
  end
  else
  begin
    FFieldHelper.List[abNext] := FEmptyList;
    FIndexHelper.List[abNext] := FEmptyList;
  end;
end;

procedure TTidLocal.DimensionChangesets(PinReason: TPinReason);
var
  i: integer;
  CC, NC: TMemDbStreamable;
  Added, Changed, Deleted, Null: boolean;
begin
  CC := FParentTable.FMetadata.PinCurrent(FTid, PinReason);
  NC := FParentTable.FMetadata.GetNext(FTid);
  FParentTable.ChangeFlagsFromPinned(CC, NC, Added, Changed, Deleted, Null);

  if Null then
  begin
    SetLength(FFieldChangesets, 0);
    SetLength(FIndexChangesets, 0);
  end
  else if Added or Changed then
  begin
    //Next list has delete sentinels at this point.
    SetLength(FFieldChangesets, FFieldHelper.FNextList.Count);
    SetLength(FIndexChangesets, FIndexHelper.FNextList.Count);
  end
  else if Deleted then
  begin
    //Next list likely to be empty, so current lists here.
    SetLength(FFieldChangesets, FFieldHelper.FCurrentList.Count);
    SetLength(FIndexChangesets, FIndexHelper.FCurrentList.Count);
  end
  else
  begin
    //Nothing changed.
    SetLength(FFieldChangesets, 0);
    SetLength(FIndexChangesets, 0);
  end;

  for i := 0 to Pred(Length(FFieldChangesets)) do
      FFieldChangesets[i] := [];
  for i := 0 to Pred(Length(FIndexChangesets)) do
      FIndexChangesets[i] := [];
end;

procedure TTidLocal.ReGenChangesets(PinReason: TPinReason);
var
  CC, NC: TMemTableMetadataItem;
  CCFieldCount, CCINdexCount, NCFieldCount, NCIndexCount: integer;
  i, j, NDIndex: integer;
  FieldDef1, FieldDef2: TMemFieldDef;
  IndexDef1, IndexDef2: TMemIndexDef;
  ff: integer; //Field number in index.

  AbsIndexes1, AbsIndexes2: TFieldOffsets;
  FieldDefs1, FieldDefs2: TMemFieldDefs;
  selCurrent, selNext: TBufSelector;

begin
  FParentTable.GetCurrNxtMetaCopiesEx(FTid, CC, NC, CCFieldCount, CCIndexCount, NCFieldCount, NCIndexCount, PinReason);

  //Expect NCFieldCount to be >= CCFieldCount or 0.
  if not ((NCFieldCount >= CCFieldCount) or (NCFieldCount = 0)) then
    raise EMemDbInternalException.Create(S_INTERNAL_REGEN_CHANGESETS_1);

  //Fields: Check definitions in new copy (if there are some).
  NDIndex := 0;
  for i := 0 to Pred(NCFieldCount) do
  begin
    //check unique field names.
    if not (Assigned(NC.FieldDefs.Items[i])) then
      raise EMemDbConsistencyException.Create(S_MISSING_FIELD_DEF);
    if not (NC.FieldDefs.Items[i] is TMemDeleteSentinel)  then
    begin
      FieldDef1 := NC.FieldDefs.Items[i] as TMemFieldDef;
      if FieldDef1.FieldName = '' then
        raise EMemDBConsistencyException.Create(S_FIELD_NAME_EMPTY);
      for j := Succ(i) to Pred(NCFieldCount) do
      begin
        if not (Assigned(NC.FieldDefs.Items[j])) then
          raise EMemDbConsistencyException.Create(S_MISSING_FIELD_DEF);
        if not (NC.FieldDefs.Items[j] is TMemDeleteSentinel)  then
        begin
          FieldDef2 := NC.FieldDefs.Items[j] as TMemFieldDef;
          if FieldDef1.FieldName = FieldDef2.FieldName then
            raise EMemDBConsistencyException.Create(S_DUP_FIELD_NAMES);
        end;
      end;
      //check field numbering correct with respect to deleted fields.
      if FieldDef1.FieldIndex <> NDIndex then
        raise EMemDBInternalException.Create(S_BAD_FIELD_INDEX);
      Assert(FFieldHelper.NdIndexToRawIndex(NDIndex) = i);
      Assert(FFieldHelper.RawIndexToNdIndex(i) = NDIndex);
      Inc(NDIndex);
    end;
  end;

  if not ((NCIndexCount >= CCIndexCount) or (NCIndexCount = 0)) then
    raise EMemDbInternalException.Create(S_INTERNAL_REGEN_CHANGESETS_2);

  //Indexes: Check definitions in new copy (if there are some).
  NDIndex := 0;
  for i := 0 to Pred(NCIndexCount) do
  begin
    if not (Assigned(NC.IndexDefs.Items[i])) then
      raise EMemDbConsistencyException.Create(S_MISSING_INDEX_DEF);
    if not (NC.IndexDefs.Items[i] is TMemDeleteSentinel) then
    begin
      IndexDef1:= NC.IndexDefs.Items[i] as TMemIndexDef;
      if IndexDef1.IndexName = '' then
        raise EMemDBConsistencyException.Create(S_INDEX_NAME_EMPTY);
      if IndexDef1.FieldNameCount = 0 then
        raise EMemDBConsistencyException.Create(S_NO_FIELDS_IN_INDEX);
      for ff := 0 to Pred(IndexDef1.FieldNameCount) do
      begin
        if IndexDef1.FieldNames[ff] = '' then
          raise EMemDBConsistencyException.Create(S_FIELD_NAME_EMPTY);
      end;
      for j := Succ(i) to Pred(NCIndexCount) do
      begin
        //Check unique index names.
        if not (Assigned(NC.IndexDefs.Items[j])) then
          raise EMemDbConsistencyException.Create(S_MISSING_INDEX_DEF);
        if not (NC.IndexDefs.Items[j] is TMemDeleteSentinel) then
        begin
          IndexDef2 := NC.IndexDefs.Items[j] as TMemIndexDef;
          if IndexDef1.IndexName = IndexDef2.IndexName then
            raise EMemDBConsistencyException.Create(S_DUP_INDEX_NAMES);
        end;
      end;
      //Indexes not numbered, might reconsider with foreign keys.
      Assert(FIndexHelper.NdIndexToRawIndex(NDIndex) = i);
      Assert(FIndexHelper.RawIndexToNdIndex(i) = NDIndex);
      Inc(NDIndex);
    end;
  end;

  //Check expected length of changeset arrays.
  if NCFieldCount = 0 then
  begin
    //Deleted or no change.
    if not ((Length(FFieldChangesets) = CCFieldCount)
      or (Length(FFieldChangesets) = 0)) then
      raise EMemDbInternalException.Create(S_INTERNAL_REGEN_CHANGESETS_3);
  end
  else
  begin
    //Added or changed.
    if Length(FFieldChangesets) <> NCFieldCount then
      raise EMemDbInternalException.Create(S_INTERNAL_REGEN_CHANGESETS_4);
  end;

  //Fields: Comparitive for changeset, and fill out *all* items
  //in field changesets, even for things like table deleted case.
  for i := 0 to Pred(Length(FFieldChangesets)) do
  begin
    if i < NCFieldCount then
    begin
      Assert(Assigned(NC.FieldDefs.Items[i]));
      if not (NC.FieldDefs.Items[i] is TMemDeleteSentinel) then
      begin
        FieldDef2 := NC.FieldDefs.Items[i] as TMemFieldDef;
        //Don't need to check against any other metadata at present time.
        if i < CCFieldCount then //Not an added field.
        begin
          if NotAssignedOrSentinel(CC.FieldDefs.Items[i]) then
            raise EMemDBInternalException.Create(S_PRE_COMMIT_CHECK_INTERNAL);
          FieldDef1 := CC.FieldDefs.Items[i] as TMemFieldDef;
          if FieldDef1.FieldType <> FieldDef2.FieldType then
            raise EMemDbInternalException.Create(S_FIELD_CHANGED_TYPE);
          // if FieldDef1.FieldName <> FieldDef2.FieldName then
          // Fields can be renamed without any extra lower
          // level processing being required.
          if FieldDef1.FieldIndex <> FieldDef2.FieldIndex then
            FFieldChangesets[i] := FFieldChangesets[i] + [fctChangedFieldNumber];
        end
        else
          FFieldChangesets[i] := [fctAdded];
      end
      else
        FFieldChangesets[i] := [fctDeleted]; //One individually deleted.
    end
    else
      FFieldChangesets[i] := [fctDeleted]; //All deleted.
  end;

  //Indexes: Comparitive for changeset, and fill out *all* items
  //in index changesets, even for things like table deleted case.

  //Check expected length of changeset arrays.
  if NCIndexCount = 0 then
  begin
    //Deleted or no change.
    if not ((Length(FIndexChangesets) = CCIndexCount)
      or (Length(FIndexChangesets) = 0)) then
      raise EMemDbInternalException.Create(S_INTERNAL_REGEN_CHANGESETS_5);
  end
  else
  begin
    //Added or changed.
    if Length(FIndexChangesets) <> NCIndexCount then
      raise EMemDbInternalException.Create(S_INTERNAL_REGEN_CHANGESETS_6);
  end;

  selCurrent := MakeCurrentBufSelector(FTid);
  selNext := MakeNextBufSelector(FTid);

  for i := 0 to Pred(Length(FIndexChangesets)) do
  begin
    if i < NCIndexCount then
    begin
      Assert(Assigned(NC.IndexDefs.Items[i]));
      if not (NC.IndexDefs.Items[i] is TMemDeleteSentinel) then //Not a deleted index.
      begin
        IndexDef1:= NC.IndexDefs.Items[i] as TMemIndexDef;
        if i < CCIndexCount then //Not an added index.
        begin
          if NotAssignedOrSentinel(CC.IndexDefs.Items[i]) then
            raise EMemDBInternalException.Create(S_PRE_COMMIT_CHECK_INTERNAL);
          IndexDef2 := CC.IndexDefs.Items[i] as TMemIndexDef;
          // if IndexDef1.IndexName <> IndexDef2.IndexName then
          // Indexes are allowed to change name without any extra
          // lower level processing being required.
          if IndexDef1.IndexAttrs <> IndexDef2.IndexAttrs then
            raise EMemDbInternalException.Create(S_INDEX_CHANGED_ATTRS);

          //Check indexes reference same underlying field:
          //Same abs position in array for field index,
          //and also, field number change is same for field, and index.
          FieldDefs1 := (FParentTable.FMetadata as TMemDBTableMetadata)
            .FieldsByNames(selNext, IndexDef1.FieldArray, AbsIndexes1, PinReason);
          FieldDefs2 := (FParentTable.FMetadata as TMemDBTableMetadata)
            .FieldsByNames(selCurrent, IndexDef2.FieldArray, AbsIndexes2, PinReason);

          if not ((Length(FieldDefs1) > 0) // i < NCIndexCount
            and (Length(FieldDefs2) > 0) // i < CCIndexCount
            and (Length(FieldDefs1) = Length(AbsIndexes1))
            and (Length(FieldDefs2) = Length(AbsIndexes2))) then
              raise EMemDbInternalException.Create(S_INDEX_FIELD_COUNTS_BAD_1);

          if Length(FieldDefs1) <> Length(FieldDefs2) then
            raise EMemDbConsistencyException.Create(S_INDEX_FIELD_COUNT_SHOULD_BE_CONSTANT);
          for ff := 0 to Pred(Length(FieldDefs1)) do
          begin
            //Check fields looked up by name are good.
            if not (Assigned(FieldDefs1[ff]) and Assigned(FieldDefs2[ff])) then
              raise EMemDBConsistencyException.Create(S_INDEX_DOES_NOT_REFERENCE_FIELD);
            //Check same underlying fields.
            if AbsIndexes1[ff] <> AbsIndexes2[ff] then
              raise EMemDbConsistencyException.Create(S_INDEX_UNDERLYING_FIELD_CHANGED);
            //Check field indexes for index are ok.
            if FieldDefs1[ff].FieldType <> FieldDefs2[ff].FieldType then
              raise EMemDBConsistencyException.Create(S_INDEXED_FIELD_CHANGED_TYPE);

            //Check indexed fields in correct locations,
            //(should have been verified by field number checking above).
            if not ((FieldDefs2[ff].FieldIndex = AbsIndexes2[ff]) //Current copy no delete sentinels
              and (FFieldHelper.NdIndexToRawIndex(FieldDefs1[ff].FieldIndex) = AbsIndexes1[ff])) then //Next copy sentinels agree.
              raise EMemDbInternalException.Create(S_INDEX_REARRANGE_BAD);

            if FieldDefs1[ff].FieldIndex <> FieldDefs2[ff].FieldIndex then
              FIndexChangesets[i] := FIndexChangesets[i] + [ictChangedFieldNumber];
          end;
        end
        else
        begin
          FieldDefs1 := (FParentTable.FMetadata as TMemDBTableMetadata)
            .FieldsByNames(selNext, IndexDef1.FieldArray, AbsIndexes1, PinReason);

          if not ((Length(FieldDefs1) > 0) // i < NCIndexCount
            and (Length(FieldDefs1) = Length(AbsIndexes1))) then
            raise EMemDbInternalException.Create(S_INDEX_FIELD_COUNTS_BAD_2);

          for ff := 0 to Pred(Length(FieldDefs1)) do
          begin
            if not Assigned(FieldDefs1[ff]) then
              raise EMemDBConsistencyException.Create(S_INDEX_DOES_NOT_REFERENCE_FIELD);
            if not (FieldDefs1[ff].FieldType in IndexableFieldTypes) then
              raise EMemDBConsistencyException.Create(S_INDEX_BAD_FIELD_TYPE);
          end;
          FIndexChangesets[i] := [ictAdded];
        end;
      end
      else
        FIndexChangesets[i] := [ictDeleted]; //One deleted.
    end
    else
      FIndexChangesets[i] := [ictDeleted]; //All deleted.
  end;

  FIndexingChangeRequired := false;
  FLayoutChangeRequired := false;
  for i := 0 to Pred(Length(FIndexChangesets)) do
    if (FIndexChangesets[i] * [ictAdded, ictDeleted, ictChangedFieldNumber])
      <> [] then
      FIndexingChangeRequired := true;

  for i := 0 to Pred(Length(FFieldChangesets)) do
    if (FFieldChangesets[i] * [fctAdded, fctDeleted, fctChangedFieldNumber])
      <> [] then
      FLayoutChangeRequired := true;
end;


procedure TTidLocal.AdjustTableStructureForMetadata;
var
  RNxt: TMemDbStreamable;
  CC, NC: TMemTableMetadataItem;
  Row: TMemDBRow;
  IRec: TItemRec;
  Delete: boolean;
  CCFieldCount: integer;
  i: integer;
  FieldData: TMemFieldData;
  RowFields: TMemRowFields;
  NxtFieldDef, CurFieldDef: TMemFieldDef;

begin
  RNxt := FParentTable.FMetadata.GetNext(FTid);
  Assert(Assigned(RNxt));
  Delete := (RNxt is TMemDeleteSentinel);
  if not Delete then
  begin
    FParentTable.GetCurrNxtMetaCopies(FTid, CC, NC, pinEvolve);
    Assert(Assigned(NC));
    Delete := NC.FieldDefs.Count = 0;
  end;

  //TODO - We have the row addition lock, so we can do this in batches
  //without holding the master row lock for all of it.
  FParentTable.FMasterRowLock.Acquire;
  try
    try
      //Need addition lock whether in batches or not - not atomic with commit/rollback.
      if not FParentTable.AddAllRowProhibitInCrit(FTid, true, true, true) then
        exit; //Already adjusted table structure, perhaps previous abortive commit attempt

      //NB. If/when traveral locks work, IRec is valid between master lock acquisitions.
      IRec := FParentTable.FMasterRowList.GetAnItem;
      while Assigned(IRec) do
      begin
        Row := IRec.Item as TMemDBRow;
        if Delete then
          Row.Delete(FTid, ilReadRepeatable)
        else
        begin
          //No guarantee other format changes aren't going on at the same time.

          //Basic format checks so algorithm doesnt die, but
          //"formal" consistency check is done with Tid comparison,
          //which will catch races one way round or another.
          if not Row.CheckFormatAgainstMeta(FTid, abCurrent, CC, pinEvolve) then
            raise EMemDBConcurrencyException.Create(S_TABLE_ADJUST_CONCURRENCY_CONFLICT);

          Row.RequestChange(FTid, ilReadRepeatable);

          //Catch previous abortive attempts to change structure.
          //Expect next copy to be same as current copy, and pass such tests...
          if not Row.CheckFormatAgainstMeta(FTid, abNext, CC, pinEvolve) then
            raise EMemDBConcurrencyException.Create(S_TABLE_ADJUST_REPEAT_FAILED);

          RowFields := Row.GetNext(FTid) as TMemRowFields;
          RowFields.Sparse := true;

          Assert(Assigned(NC));
          if NotAssignedOrSentinel(CC) then
            CCFieldCount := 0
          else
            CCFieldCount := CC.FieldDefs.Count;

          for i := 0 to Pred(NC.FieldDefs.Count) do
          begin
            //No delete sentinels in current or next at the moment....
            if i < CCFieldCount then
              FieldData := RowFields.Items[i] as TMemFieldData
            else
              FieldData := nil;

            if (NC.FieldDefs.Items[i]) is TMemDeleteSentinel then
              NxtFieldDef := nil
            else
              NxtFieldDef := NC.FieldDefs.Items[i] as TMemFieldDef;

            if i < CCFieldCount then
            begin
              Assert(not (CC.FieldDefs.Items[i] is TMemDeleteSentinel));
              CurFieldDef := CC.FieldDefs.Items[i] as TMemFieldDef
            end
            else
              CurFieldDef := nil;

            if not (((not Assigned(NxtFieldDef)) = (fctDeleted in FFieldChangesets[i]))
              and ((not Assigned(CurFieldDef)) = (fctAdded in FFieldChangesets[i]))) then
              raise EMemDBInternalException.Create(S_TABLE_ADJUST_FAILED_1);

            if fctDeleted in  FFieldChangesets[i] then
            begin
              FieldData.Release;
              RowFields.Items[i] := TMemDeleteSentinel.Create;
            end
            else if fctAdded in FFieldChangesets[i] then
            begin
              FieldData := TMemFieldData.Create;
              FieldData.FDataRec.FieldType :=
                (NC.FieldDefs.Items[i] as TMemFieldDef).FieldType;
              //Zero data OK, not explicitly representing NULL's.

              RowFields.AddNoRef(FieldData); //Ref count of 1 is what we want.
            end
          end;
          if not Row.CheckFormatAgainstMeta(FTid, abNext, NC, pinEvolve) then
            raise EMemDbInternalException.Create(S_TABLE_ADJUST_FAILED_2);
        end;
        FParentTable.FMasterRowList.GetAnotherItem(IRec);
      end;
    except
      //Might actually be in some nasty intermediate state, but
      //better that, (and it be rediscovered on next commit attempt),
      //than keep prohibit lock and hence not try to
      //adjust / finish adjusting structure when we should.
      FParentTable.FindRmRowProhibitLock(FTid);
      raise;
    end;
  finally
    FParentTable.FMasterRowLock.Release;
  end;
end;

constructor TTidLocal.Create;
begin
  inherited;
  DLItemInitObj(self, @FTidLocalLinks);
  FCPRows := TIndexedStoreO.Create;
  FLocalIndexCopies := TReffedList.Create;
end;

procedure TTidLocal.Init(Parent: TMemDBTablePersistent; const Tid: TTransactionId);
var
  AddRef: boolean;
begin
  FParentTable := Parent;
  FTid := Tid;
  FParentTable.FTidLocalLock.Acquire;
  try
    AddRef := DLList.DlItemIsEmpty(@FParentTable.FTidLocalStructures);
    DLList.DLListInsertTail(@FParentTable.FTidLocalStructures, @FTidLocalLinks);
  finally
    FParentTable.FTidLocalLock.Release;
  end;
  if AddRef then
    FParentTable.HandleAddRefForTidLocal(self);
end;

destructor TTidLocal.Destroy;
var
  Release: boolean;
begin
  Assert(FCPRows.Count = 0);
  FCPRows.Free;
  FParentTable.FTidLocalLock.Acquire;
  try
    DLList.DLListRemoveObj(@FTidLocalLinks);
    Release := DLList.DlItemIsEmpty(@FParentTable.FTidLocalStructures);
  finally
    FParentTable.FTidLocalLock.Release;
  end;
  if Release then
    FParentTable.HandleReleaseForTidLocal(self);

  FIndexHelper.Free;
  FFieldHelper.Free;
  FEmptyList.Release;
  FLocalIndexCopies.Release; //With contained indexes.
  FNewBuildIndices.Release;
  inherited;
end;

procedure TTidLocal.ToJournal(Stream: TStream);
var
  IRec: TItemRec;
  Row: TMemDBRow;
begin
  WrTag(Stream, mstIndexedListStart);
  IRec := FCPRows.GetAnItem;
  while Assigned(IRec) do
  begin
    Row := IRec.Item as TMemDBRow;
    if Row.AnyChangesForTid(self.FTid) then //Some Items are pinned not changed.
    begin
      Row.ToJournal(self.FTid, Stream);
    end;
    FCPRows.GetAnotherItem(IRec);
  end;
  WrTag(Stream, mstIndexedListEnd);
end;

procedure TTidLocal.ToScratch(Stream: TStream);
var
  IRec: TItemRec;
  Row: TMemDBRow;
begin
  WrTag(Stream, mstIndexedListStart);
  //TODO - Maybe do this in a way which doesn't lock the master list?
  //Checkpoints to tend to be pretty rapid ...
  FParentTable.FMasterRowLock.Acquire;
  try
    IRec := FParentTable.FMasterRowList.GetAnItem;
    while Assigned(IRec) do
    begin
      Row := IRec.Item as TMemDBRow;
      if AssignedNotSentinel(Row.PinCurrent(FTid, pinEvolve)
          as TMemDbStreamable) then
        Row.ToScratch(self.FTid, Stream);
      FParentTable.FMasterRowList.GetAnotherItem(IRec);
    end;
  finally
    FParentTable.FMasterRowLock.Release;
  end;
  WrTag(Stream, mstIndexedListEnd);
end;

function TTidLocal.LookaheadHelper(Stream: TStream; Scratch: boolean; var Created: boolean): TMemDBRow;
var
  Pos: Int64;
  Tag: TMemStreamTag;
  RowId: TGUID;
  SV: TRowSearchVal;
  RV: TISRetVal;
  IRec: TItemRec;
begin
  Created := false;
  Pos := Stream.Position;
  Tag := RdTag(Stream);
  case Tag of
    mstRowStartV2:
    begin
      RowId := RdGuid(Stream);
      if Scratch then
      begin
        result := TMemDBRow.Create;
        Created := true;
        result.Init(FParentTable, RowId);
      end //DCPHandler will insert into master list, and de-dupe guids.
      else
      begin
        SV := TRowSearchVal.Create;
        try
          SV.FGuid := RowId;
          FParentTable.FMasterRowLock.Acquire;
          try
            RV := FParentTable.FMasterRowList.FindByIndex(Pointer(1), SV, IRec);
            if RV = rvOK then
            begin
              result := IRec.Item as TMemDBRow;
              //From journal replay cases, we do not expect concurrent
              //modification of state
              if not Assigned(result.PinCurrent(self.FTid, TPinReason.pinEvolve)) then
                raise EMemDBInternalException.Create(S_CONCURRENT_MODIFY_DURING_REPLAY);
            end
            else
              Assert(RV = rvNotFound);
          finally
            FParentTable.FMasterRowLock.Release;
          end;
        finally
          SV.Free;
        end;
        if RV = rvOK then
          result := IRec.Item as TMemDBRow
        else
        begin
          Assert(not Assigned(IRec));
          result := TMemDBRow.Create;
          Created := true;
          result.Init(FParentTable, RowId);
        end;
      end;
    end;
    mstIndexedListEnd: result := nil;
  else
    raise EMemDBException.Create(S_INDEXED_LIST_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
  end;
  Stream.Seek(Pos, TSeekOrigin.soBeginning);
end;

procedure TTidLocal.FromJournal(Stream: TStream);
var
  Row: TMemDBRow;
  Created: boolean;
begin
  ExpectTag(Stream, mstIndexedListStart);
  Row := LookaheadHelper(Stream, false, Created);
  while Assigned(Row) do
  begin
    Row.FromJournal(FTid, Stream);
    if Created then
      Row.FProxy.Release;
    Row := LookaheadHelper(Stream, false, Created);
  end;
  ExpectTag(Stream, mstIndexedListEnd);
end;

procedure TTidLocal.FromScratch(Stream: TStream);
var
  Row: TMemDBRow;
  Created: boolean;
begin
  ExpectTag(Stream, mstIndexedListStart);
  Row := LookaheadHelper(Stream, true, Created);
  while Assigned(Row) do
  begin
    //Row DCP handler, Row CPTid handler handle reffing, and
    //all insertion except master list.
    Row.FromScratch(FTid, Stream);
    if Created then
      Row.FProxy.Release;
    Row := LookaheadHelper(Stream, true, Created);
  end;
  ExpectTag(Stream, mstIndexedListEnd);
end;

procedure TTidLocal.RowCPTidHandle(Sender: TObject; const Update: TTidUpdate);
var
  IRec: TItemRec;
begin
  Assert(Assigned(Sender));
  if Update.Ref.Post <> Update.Ref.Pre then
  begin
    Assert(Update.Tid = FTid);
    if Update.Ref.Post then
    begin
      FCPRows.AddItem(Sender, IRec);
      Assert(Assigned(IRec));
    end
    else
    begin
      //Take advantage of a neat trick whereby we commit/rollback rows in order,
      //and then we don't need an index, we know we're always doing it to
      //the first row.
      IRec := FCPRows.GetAnItem;
      if not (Assigned(IRec) and (IRec.Item = Sender)) then
        raise EMemDBInternalException.Create(S_ROW_MAINT_INTERNAL);
      FCPRows.RemoveItem(IRec);
    end;
  end;
  if Update.Ref.Post then
    FDataChanged := FDataChanged or (Sender as TMemDBRow).AnyChangesForTid(Update.Tid);
end;

procedure TTidLocal.CheckTableRowCount;
var
  LM: TMemDBStreamable;
  LatestMeta: TMemTableMetadataItem;
  FieldsNull: boolean;
  BufSel: TABSelType;
  RV: TISRetVal;
  IRec: TItemRec;
  Row: TMemDBRow;
  RowCur, RowNext: TMemDBStreamable;
  Added, Changed, Deleted, Null:boolean;
begin
  LM := FParentTable.Metadata.GetPinLatest(FTid, BufSel, pinFinalCheck);
  FieldsNull := NotAssignedOrSentinel(LM);
  if not FieldsNull then
  begin
    LatestMeta := LM as TMemTableMetadataItem;
    FieldsNull := LatestMeta.FieldDefs.Count = 0;
  end;
  if not FieldsNull then
    exit;
  //Need to check all rows in table are modded for our Tid, and have
  //delete sentinel. Sorry, need master row lock at this point.

  //Addition in other transactions prevented by addition lock.
  FParentTable.FMasterRowLock.Acquire;
  try
    IRec := FParentTable.FMasterRowList.GetAnItem;
    while Assigned(IRec) do
    begin
      Row := IRec.Item as TMemDBRow;
      RowCur := Row.PinCurrent(FTid, pinFinalCheck);
      RowNext := Row.GetNext(FTid);
      Row.ChangeFlagsFromPinned(RowCur, RowNext,
        Added, Changed, Deleted, Null);
      //Hmm. Can some of the rows be Null, not deleted?
      //Yes, because previous trans can have made rows null
      if not (Deleted or Null) then
        raise EMemDBConsistencyException.Create(S_TABLE_WITH_NO_FIELDS_HAS_ROWS);
      RV := FParentTable.FMasterRowList.GetAnotherItem(IRec);
      Assert(RV in [rvOK, rvNotFound]);
    end;
  finally
    FParentTable.FMasterRowLock.Release;
  end;
end;

procedure TTidLocal.CheckChangedRowStructure(Reason: TMemDBTransReason);
var
  IRec: TITemRec;
  Row: TMemDBRow;
  MRMeta: TMemTableMetadataItem;
  Streamable: TMemDbStreamable;
  bufSel: TABSelType;

begin
  //Noticeably since this involves all local (or pinned) structures,
  //this does not need to be protected by commit lock.
  Streamable := FParentTable.FMetadata.GetPinLatest(FTid, bufSel, pinFinalCheck);
  if not Assigned(Streamable) then
    raise EMemDBInternalException.Create(S_INTERNAL_CHECKING_STRUCTURE);
  if (Streamable is TMemDeleteSentinel) then
    exit; //Can get here in deleted case.

  MRMeta := Streamable as TMemTableMetadataItem;
  Assert(Assigned(MrMeta.FieldDefs));
  Assert(MrMeta.FieldDefs is TMemStreamableList);

  IRec := FCPRows.GetAnItem;
  while Assigned(IRec) do
  begin
    Row := IRec.Item as TMemDBRow;
    Streamable := Row.GetPinLatest(FTid, bufSel, pinFinalCheck);
    //Can have null rows? Not if you managed to pin it. If CPTid,
    //then you managed to pin or create next, and that shouldn't be undone until
    //commit / rollback, so should be assigned.
    Assert(Assigned(Streamable));
    if (Streamable is TMemDeleteSentinel) then
    begin
      //Next row.
      FCPRows.GetAnotherItem(IRec);
      continue;
    end;

    //Checks here same as before, except we don't check MetaFieldDef.FieldIndex,
    //but that should have been checked earlier in ReGenChangesets.
    //We also check similar arrangement of delete sentinels.
    if not Row.CheckFormatAgainstMeta(FTid, TABSelType.abLatest, MRMeta, pinFinalCheck) then
    begin
      if Reason = mtrUserOp then
        //Should never happen whilst we're generating these changes ourselves.
        raise EMemDBInternalException.Create(S_FIELDS_NOT_SAME_AS_META)
      else
        //Might possibly happen by reading bad data from file.
        raise EMemDBConsistencyException.Create(S_FIELDS_NOT_SAME_AS_META)
    end;
    FCPRows.GetAnotherItem(IRec);
  end;
end;


procedure TTidLocal.BuildValidateNewIndexesCommon(LocalIter: boolean);
var
  NewBuild: TReffedList;
  i, j: integer;
  NC: TMemTableMetadataItem;
  IDef: TMemIndexDef;
  FieldDefs: TMemFieldDefs;
  SparseOffsets: TFieldOffsets;
  CompactOffsets: TFieldOffsets;
  CheckOffsets: TFieldOffsets;
  NextBufSelector: TBufSelector;
  NewIndex:TMemDBIndex;
  INode, ValINode: TMemDbIndexLeaf;
  IRec: TItemRec;
  Row: TMemDBRow;
  PinRet, TreeRet: boolean;
{$IFOPT C+}
  Cur, Nxt: TMemDbStreamable;
  Added, Changed, Deleted, Null: boolean;
{$ENDIF}
  CurFields, PrevFields: TMemRowFields;
  PinFields: TMemDBStreamable;
  selType: TABSelType;
{$IFOPT C+}
  ProhibitLock: PRowChangeLock;
{$ENDIF}

begin
  if Assigned(FNewBuildIndices) then
    exit; //Inside lock case, may have previously been built outside lock.

  NextBufSelector := MakeNextBufSelector(FTid);

  NewBuild := nil;
  NC := nil;
  NewIndex := nil;
  INode := nil;
  PrevFields := nil;
  try
{$IFOPT C+}
    if LocalIter then
    begin
      //Check that we hold a concurrent row modification lock.
      ProhibitLock := FParentTable.FindRowProhibitLock(FTid);
      Assert(Assigned(ProhibitLock));
      Assert(ProhibitLock.ProhibitAdd);
      Assert(ProhibitLock.ProhibitChange);
      Assert(ProhibitLock.ProhibitDelete);
    end;
{$ENDIF}

    Assert(Assigned(FIndexChangesets) and (Length(FIndexChangesets) > 0));
    NewBuild := TReffedList.Create;
    NewBuild.Count := Length(FIndexChangesets);
    //Note that we create FNewBuildIndices array for later destruction
    //swizzling in MetaIndexCommit. Always create this even if it's
    //indexes / table being deleted.

    //TODO - Execute parallel for better speed.
    for i := 0 to Pred(Length(FIndexChangesets)) do
    begin
      if FIndexChangesets[i] * [ictAdded, ictChangedFieldNumber] <> [] then
      begin
        //Only get next metadata copy here, because del table -> No NC.
        if not Assigned(NC) then
        begin
          NC := FParentTable.Metadata.GetNext(FTid) as TMemTableMetadataItem;
          Assert(Assigned(NC));
        end;

        IDef := NC.IndexDefs[i] as TMemIndexDef;
        FieldDefs := (FParentTable.Metadata as TMemDbTableMetadata).FieldsByNames(NextBufSelector,
                                                                     IDef.FieldArray,
                                                                     SparseOffsets, pinEvolve); //TODO FinalCheck?
        Assert(Length(FieldDefs) = Length(SparseOffsets));
        if not IDef.FieldNameCount = Length(FieldDefs) then
          raise EMemDbInternalException.Create(S_ERROR_GETTING_OFFSETS_IN_INDEX_BUILD);

        SetLength(CompactOffsets, Length(SparseOffsets));
        for j := 0 to Pred(Length(CompactOffsets)) do
          CompactOffsets[j] := FieldDefs[j].FieldIndex;

        NewIndex := TMemDBIndex.Create;
        NewIndex.FinalFieldOffsets := CompactOffsets;
        NewIndex.SparseFieldOffsets := SparseOffsets;

        if LocalIter then
          IRec := FCPRows.GetAnItem
        else
          IRec := FParentTable.FMasterRowList.GetAnItem;
        while Assigned(IRec) do
        begin
          Row := IRec.Item as TMemDBRow;

          //Don't gratuitously create INodes if we don't need them.
          if not Assigned(INode) then
            INode := TMemDBIndexLeaf.Create;

          //First, pin.
          try
            if LocalIter then
            begin
              //Expect all rows modded and atomic view, try next bufsel.
              //Table deletion should not get here.
              PinRet := Row.PinForIndex(FTid, abNext, INode);
              if not PinRet then
                raise EMemDBInternalException.Create(S_PIN_FAILED_DURING_INDEX_BUILD);
            end
            else
            begin
              //Not necessarily all rows modded,
              //May have some deleted or NULL rows.
              //Also, Cur data may have changed behind our back,

              //However, ABLatest pin should not be able to resurrect data
              //(would fail concurrency check).
              PinRet := Row.PinForIndex(FTid, abLatest, INode);
            end;
          except
            INode.Release; //Node unpins if necessary.
            raise;
          end;
          //Then add to tree
          if PinRet then
          begin
            try
              TreeRet := NewIndex.Add(abCurrent, INode);
              if not TreeRet then
                raise EMemDBInternalException.Create(S_INSERT_FAILED_DURING_INDEX_BUILD);
              INode := nil;
            except
              INode.Release; //Will unpin if necessary.
              raise;
            end;
          end;
          //All done, next row.
          if LocalIter then
            FCPRows.GetAnotherItem(IRec)
          else
            FParentTable.FMasterRowList.GetAnotherItem(IRec);
        end;
        //Added all the rows.

        //Now validate the entire index if applicable.
        if IDef.IndexAttrs * [iaUnique , iaNotEmpty] <> [] then
        begin
          ValINode := NewIndex.Locate(abCurrent, ptFirst, nil);
          while Assigned(ValINode) do
          begin
            //Don't bother pinning code here, get straight from the INode.
            //We are validating what the index really is,
            //not any local pinned view of things.
            PinFields := ValINode.Pinned;

            if NotAssignedOrSentinel(PinFields) then
              raise EMemDBInternalException.Create(S_PIN_FIELDS_FAILURE_DURING_INDEX_VALIDATE);

            CurFields := PinFields as TMemRowFields;
            if CurFields.Sparse then
              CheckOffsets := SparseOffsets
            else
              CheckOffsets := CompactOffsets;
            //CurFields assigned.

            //OK, how do we get the subset of fields required for the index?
            if IDef.IndexAttrs * [iaNotEmpty] <> [] then
            begin
              if AllFieldsZero(CurFields, CheckOffsets) then
                raise EMemDBConsistencyException.Create(S_INDEX_CONSTRAINT_ZERO);
            end;
            if (IDef.IndexAttrs * [iaUnique] <> []) and Assigned(PrevFields) then
            begin
              //Do we expect sparseness to always be the same for the different rows?
              //Sparse if table format change, else compact, and we're supposed to
              //do all the rows in such cases.
              if CurFields.Sparse <> PrevFields.Sparse then
                raise EMemDbInternalException.Create(S_SPARSENESS_DIFFERS_CHECKING_INDEX);
              //We could handle such cases, but do not expect them.
              if SameLayoutFieldsSame(CurFields, PrevFields, CheckOffsets) then
                raise EMemDBConsistencyException.Create(S_INDEX_CONSTRAINT_UNIQUE);
            end;

            PrevFields := CurFields;
            ValINode := NewIndex.Locate(abCurrent, ptNext, ValINode);
          end
        end;

        //Validated OK, add to list of new build indices.
        NewBuild.Items[i] := NewIndex;
        NewIndex := nil;
      end; //If modded index.
    end; //For index changeset.

    FNewBuildIndices := NewBuild;
    NewBuild := nil;
  finally
    NewBuild.Release;
    NewIndex.Release;
    INode.Release;
  end
end;

procedure TTidLocal.BuildValidateNewIndexesOutsideCommitLock;
begin
  BuildValidateNewIndexesCommon(true);
end;

procedure TTidLocal.BuildValidateNewIndexesInsideCommitLock;
begin
  BuildValidateNewIndexesCommon(false);
end;

procedure TTidLocal.BuildCheckPartialIndexes;

  function GetIPinForIndex(Row: TMemDBRow; Index: TMemDbIndex): PMemDbIndexPin;
  var
    IPins: TList;
    j, fnd: integer;

  begin
    //Humm. We need to PinCurrent from the INode's point of view.
    //Bit clunky, but safer.
    result := nil;
    IPins := Row.MakeCandidateIPins(Index); //Probably only one, actually.
    try
      fnd := 0;
      for j := 0 to Pred(IPins.Count) do
      begin
        if Index.CheckPresent(abNext, PMemDbIndexPin(IPins[j]).INode) then
        begin
          result := PMemDbIndexPin(IPins[j]);
          Inc(fnd);
{$IFOPT C-}
          break;
{$ENDIF}
        end
      end;
      if fnd > 1 then
      begin
        result := nil;
        raise EMemDBInternalException.Create(S_ERROR_IPINS_MODIFYING_INDEX);
      end;
    finally
      for j := 0 to Pred(IPins.Count) do
        if IPins[j] <> result then
          Row.ReleaseIndexPinOutsideLock(IPins[j]);
      IPins.Free;
    end;
  end;

var
  i, k: integer;
  PartialIndex: boolean;
  Index: TMemDBIndex;
  IRec: TItemRec;
  Row: TMemDBRow;
  Cur, Nxt, Other: TMemDBStreamable;
  CurFields, OtherFields: TMemRowFields;
  Added, Deleted, Changed, Null: boolean;
  IPin: PMemDbIndexPin;
  NewINode: TMemDbIndexLeaf;
  OtherNode: TMemDBIndexLeaf;
  Pos: TMemAPIPosition;
  Ret: boolean;
  C: TMemDBStreamable;
  CC: TMemTableMetadataItem;
  CCDef: TMemIndexDef;
  CurrentBufSelector: TBufSelector;
  FieldDefs: TMemFieldDefs;
  CompactOffsets: TFieldOffsets;
  SparseOffsets: TFieldOffsets;

begin
  CC := nil;
  CurrentBufSelector := MakeCurrentBufSelector(FTid);

  for i := 0 to Pred(FParentTable.FMasterIndexes.Count) do
  begin
    if not FIndexingChangeRequired then
      PartialIndex := true
    else
    begin
      Assert(i < Length(FIndexChangesets));
      PartialIndex := FIndexChangesets[i]
        * [ictAdded, ictDeleted, ictChangedFieldNumber] = [];
    end;

    //TODO - Do this in parallel for greater speed.

    if PartialIndex then
    begin
      if not Assigned(CC) then
      begin
        //If partial index not added, deleted, changed then
        //CC definition is still current.
        C := FParentTable.Metadata.PinCurrent(FTid, pinFinalCheck);
        Assert(Assigned(C));
        Assert(C is TMemTableMetadataItem);
        CC := C as TMemTableMetadataItem;
      end;
      //First, index modification:
      Index := FParentTable.FMasterIndexes[i] as TMemDbIndex;
      Index.RootToNext; //All manipulation here in abNext copy of index.

      //TidLocal row set will not change under commit lock.
      IRec := FCPRows.GetAnItem;
      while Assigned(IRec) do
      begin
        Row := IRec.Item as TMemDBRow;

        IPin := GetIPinForIndex(Row, Index);
        Cur := nil;
        try
          if Assigned(IPin) then
          begin
            //Don't go thru standard pinning mechanism,
            //we're under commit lock, and we know these indexes evolve
            //in a thread-safe unitary fashion under that lock.
            Cur := IPin.INode.Pinned;
          end;
          //And after all that hassle for Cur, Nxt is a breeze.
          Nxt := Row.GetNext(FTid);
          Row.ChangeFlagsFromPinned(Cur, Nxt, Added, Changed, Deleted, Null);

          if Changed or Deleted then
          begin
            Assert(Assigned(IPin));
            Ret := Index.Remove(abNext, IPin.INode);
            if not Ret then
              raise EMemDBInternalException.Create(S_ERROR_MODIFYING_INDEX_REMOVE);
          end
          else
            Assert(not Assigned(IPin));
        finally
          if Assigned(IPin) then
          begin
            Assert(Assigned(Row));
            Row.ReleaseIndexPinOutsideLock(IPin);
            //Arguably, since we're in the commit lock here, shouldn't need to worry
            //about this either, and not encountering local pins, but,
            //hey ho play it safe.
          end;
        end;

        if Added or Changed then
        begin
          NewINode := TMemDbIndexLeaf.Create;
          try
            if not Row.PinForIndex(FTid, abNext, NewINode) then
              raise EMemDBInternalexception.Create(S_ERROR_PINNING_INDEX_ADD);

            if not Index.Add(abNext, NewINode) then
              raise EMemDBInternalexception.Create(S_ERROR_MODIFYING_INDEX_ADD);
          except
            NewINode.Release; //Will unpin if necessary.
            raise;
          end;
        end;

        FCPRows.GetAnotherItem(IRec);
      end; //Assigned IRec.

      //OK, index modified. Now for some validation.

      CCDef := CC.IndexDefs[i] as TMemIndexDef;
      if CCDef.IndexAttrs * [iaUnique, iaNotEmpty] <> [] then
      begin
        FieldDefs := (FParentTable.Metadata as TMemDbTableMetadata).
          FieldsByNames(CurrentBufSelector, CCDef.FieldArray, SparseOffsets, pinFinalCheck);

        Assert(Length(FieldDefs) = Length(SparseOffsets));
        if not CCDef.FieldNameCount = Length(FieldDefs) then
          raise EMemDbInternalException.Create(S_ERROR_GETTING_OFFSETS_IN_INDEX_VALIDATE);

        SetLength(CompactOffsets, Length(SparseOffsets));
        for k := 0 to Pred(Length(CompactOffsets)) do
          CompactOffsets[k] := FieldDefs[k].FieldIndex;

        IRec := FCPRows.GetAnItem;
        while Assigned(IRec) do
        begin
          Row := IRec.Item as TMemDBRow;
          //First off, is this row still in the index?
          IPin := GetIPinForIndex(Row, Index);
          try
            if Assigned(IPin) then
            begin
              //Again, no pinning with row here, just validating what's
              //well and truly in the index.

              Cur := IPin.INode.Pinned;
              Assert(Assigned(Cur));
              Assert(Cur is TMemRowFields);
              CurFields := Cur as TMemRowFields;
              Assert(not CurFields.Sparse); //We've previously checked sparseness building the index, just assert here.

              if CCDef.IndexAttrs * [iaNotEmpty] <> [] then
              begin
                if AllFieldsZero(CurFields, CompactOffsets) then
                  raise EMemDBConsistencyException.Create(S_INDEX_CONSTRAINT_ZERO);
              end;

              if CCDef.IndexAttrs * [iaUnique] <> [] then
              begin
                Assert(ptPrevious = Succ(ptNext));
                for Pos := ptNext to ptPrevious do
                begin
                  OtherFields := nil;
                  OtherNode := Index.Locate(abNext, Pos, IPin.INode);

                  if Assigned(OtherNode) then
                  begin
                    //No pinning with row here.
                    Other := OtherNode.Pinned;
                    Assert(Assigned(Other));
                    Assert(Other is TMemRowFields);
                    OtherFields := Other as TMemRowFields;
                    Assert(not OtherFields.Sparse);
                    //We've previously checked sparseness building the index, just assert here.
                  end;
                  if Assigned(OtherFields) then
                  begin
                    if SameLayoutFieldsSame(CurFields, OtherFields, CompactOffsets) then
                      raise EMemDBConsistencyException.Create(S_INDEX_CONSTRAINT_UNIQUE);
                  end;
                end; //Next/prev check.
              end; //Unique attr set..
            end; //Got IPin for index.
          finally
            if Assigned(IPin) then
            begin
              Assert(Assigned(Row));
              Row.ReleaseIndexPinOutsideLock(IPin);
            end;
          end;

          FCPRows.GetAnotherItem(IRec);
        end; //Round all rows.
      end; //Index has attrs.
    end; //Partial index.
  end;
end;

procedure TTidLocal.MetaIndexCommit(Reason: TMemDbTransReason);
var
  i: integer;
  AddOrRebuild: boolean;
  Delete: boolean;
  Index: TMemDbIndex;
begin
  if FIndexingChangeRequired then
  begin
    for i := 0 to Pred(Length(FIndexChangesets)) do
    begin
      //Index swizzling done here in sparse changeset array space.

      AddOrRebuild := FIndexChangesets[i] * [ictAdded, ictChangedFieldNumber] <> [];
      Delete := FIndexChangesets[i] * [ictDeleted] <> [];
      Assert(not (AddOrRebuild and Delete));
      if AddOrRebuild then
      begin
        //Local index copies always sparse.
        Assert(FNewBuildIndices.Count > i);
        if FParentTable.FMasterIndexes.Count <= i then
          FParentTable.FMasterIndexes.Count := Succ(i); //Extend if needed.

        //Swizzle new index onto master index copies.
        Assert(Assigned(FNewBuildIndices[i]));
        //FMasterIndexes might have a previous index here on re-build.
        Index := FParentTable.FMasterIndexes[i] as TMemDBIndex;
        FParentTable.FMasterIndexes[i] := FNewBuildIndices[i];
        FNewBuildIndices[i] := Index;
      end
      else if Delete then
      begin
        //Master indexes temporarily sparse...
        Assert(FParentTable.FMasterIndexes.Count > i);
        Assert(FNewBuildIndices.Count > i);
        //Swizzle old master index onto local list for later destruction.
        Assert(not Assigned(FNewBuildIndices[i]));
        Assert(Assigned(FParentTable.FMasterIndexes[i]));
        FNewBuildIndices[i] := FParentTable.FMasterIndexes[i];
        FParentTable.FMasterIndexes[i] := nil;
      end
      else
      begin
        //No large scale manipulation, commit index to next iteration.
        Index := FParentTable.FMasterIndexes[i] as TMemDbIndex;
        Index.CommitNextToRoot;
      end;
    end;
    //And pack master indexes.
    FParentTable.FMasterIndexes.Pack;
  end
  else
  begin
    for i := 0 to Pred(FParentTable.FMasterIndexes.Count) do
    begin
      Index := FParentTable.FMasterIndexes[i] as TMemDbIndex;
      Index.CommitNextToRoot;
    end;
  end;
end;

procedure TTidLocal.MetaIndexRollback(Reason: TMemDbTransReason);
var
  i: integer;
  Index: TMemDBIndex;
begin
  //FNewBuildIndices.Release;
  //Let destructor handle it.

  //TODO - Multi thread speedup / swizzle for later destruction?
  //Already in index/meta lock for parent table.
  Assert(Assigned(FParentTable.FMasterIndexes));
  for i := 0 to Pred(FParentTable.FMasterIndexes.Count) do
  begin
    Index := FParentTable.FMasterIndexes.Items[i] as TMemDBIndex;
    Index.DiscardNext;
  end;
end;

procedure TTidLocal.MetaIndexLocalRollback(Reason: TMemDBTransReason);
var
  i: integer;
  Index: TMemDBIndex;
begin
  FNewBuildIndices.Release;
  FNewBuildIndices := nil;

  //TODO - Multi thread speedup / swizzle for later destruction?
  FParentTable.FMetaIndexLock.Acquire;
  try
    Assert(Assigned(FParentTable.FMasterIndexes));
    for i := 0 to Pred(FParentTable.FMasterIndexes.Count) do
    begin
      Index := FParentTable.FMasterIndexes.Items[i] as TMemDBIndex;
      Index.DiscardNext;
    end;
  finally
    FParentTable.FMetaIndexLock.Release;
  end;
end;

procedure TTidLocal.RowLocalPreCommit(Reason: TMemDBTransReason);
var
  IRec: TItemRec;
  Row: TMemDBRow;
  Cur, Nxt: TMemDBStreamable;
  Added, Changed, Deleted, Null: boolean;
  NoAdd, NoChange, NoDelete: boolean;

begin
  FParentTable.GetProhibitions(FTid, NoAdd, NoChange, NoDelete);
  //No locking, this is TidLocal.
  IRec := FCPRows.GetAnItem;
  while Assigned(IRec) do
  begin
    Row := IRec.Item as TMemDBRow;
    Row.PreCommit(FTid, Reason);

    { If next assigned:
      - From pin which was up to date, and Nxt is still up to date (atomic).
      - Covers changed, deleted.
      If next not assigned,
      - Cur pin *might* be wrong.
        - Difference between Added and Null, except that we require
          that RequestChange / Delete does not resurrect a row.
          and we transitively check for atomicity, so is OK. }
    Cur := Row.PinCurrent(FTid, pinFinalCheck);
    Nxt := Row.GetNext(FTid);
    Row.ChangeFlagsFromPinned(Cur, Nxt, Added, Changed, Deleted, Null);
    if (Added and NoAdd) or (Changed and NoChange) or (Deleted and NoDelete) then
        raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_ROW_OP);
    FCPRows.GetAnotherItem(IRec);
  end;
end;

//TODO - Go thru all this, work out how much actually needs
//table row lock, and/or commit lock.
procedure TTidLocal.PreCommit(Reason: TMemDBTransReason);

begin
  //Check up-to-dateness of Nxt, Pins.
  RowLocalPreCommit(Reason);
  //Table row count for null cases and deleted fields.
  CheckTableRowCount;
  //Check structure of changes rows, however that happened.
  CheckChangedRowStructure(Reason);
  //Indexes not rebuit / revalidated.
  BuildCheckPartialIndexes;
end;

procedure TTidLocal.Commit(Reason: TMemDbTransReason);
var
  Row, LastRow: TMemDBRow;
  IRec: TITemRec;
begin
  //Commit all rows, changed or pinned (clears pins).
  LastRow := nil;
  IRec := FCPRows.GetAnItem;
  while Assigned(IRec) do
  begin
    Row := IRec.Item as TMemDBRow;
    Assert(Row <> LastRow); //Just check the commit is removing things as it should.
    Row.Commit(FTid, Reason);
    LastRow := Row;
    IRec := FCPRows.GetAnItem;
  end;
  FDataChanged := false;
  FLayoutChangeRequired := false;
end;

procedure TTidLocal.Rollback(Reason: TMemDbTransReason);
var
  Row, LastRow: TMemDBRow;
  IRec: TITemRec;
begin
  //Rollback all rows, changed or pinned (clears pins).
  LastRow := nil;
  IRec := FCPRows.GetAnItem;
  while Assigned(IRec) do
  begin
    Row := IRec.Item as TMemDBRow;
    Assert(Row <> LastRow); //Just check the rollback is removing things as it should.
    Row.Rollback(FTid, Reason);
    LastRow := Row;
    IRec := FCPRows.GetAnItem;
  end;
  FDataChanged := false;
  FLayoutChangeRequired := false;
end;


function TTidLocal.GetUserIndexRoot(var Idx: TMemIndexDef; IdxName: string): TMemDBIndex;
var
  Cur: TBufSelector;
  AbsIndex: integer;
  Def: TMemIndexDef;
begin
  Cur := MakeCurrentBufSelector(FTid);
  //Indexes are consistent with pinned current metadata.
  Def := (FParentTable.Metadata as TMemDBTableMetadata).IndexByName(Cur, IdxName, AbsIndex, pinEvolve);
  if not Assigned(Def) then
    raise EMemDBAPIException.Create(S_API_INDEX_NAME_NOT_FOUND);
  Assert(AbsIndex >= 0);
  Assert(AbsIndex < FLocalIndexCopies.Count);
  result := FLocalIndexCopies.Items[AbsIndex] as TMemDbIndex;
end;

function TTidLocal.UserMoveToRowByIndexRoot(IRoot: TMemDbIndex;
                                  Cursor: TMemDBCursor;
                                  Pos: TMemAPIPosition): TMemDbCursor;
var
  Row: TMemDBRow;
  IRec: TItemRec;
  INode: TMemDbIndexLeaf;
  RetryPos: TMemApiPosition;
  Retry: boolean;
  IPinList: TList;
  i, fndcnt, fndidx: integer;
  IPin: PMemDbIndexPin;
begin
  case Pos of
    ptFirst: RetryPos := ptNext;
    ptLast: RetryPos := ptPrevious;
    ptNext, ptPrevious: RetryPos := Pos;
  else
    raise EMemDBInternalException.Create(S_MOVE_ROW_NOT_VALID_POSITION);
  end;
  Row := nil;
  if not Assigned(IRoot) then
  begin
    INode := nil;
    //TODO - Not do all traversal / skipping of NULL under index lock?
    //It's not expensive until someone deletes the table / all the rows...
    FParentTable.FMasterRowLock.Acquire;
    try
      case Pos of
        ptFirst: begin
          IRec := FParentTable.FMasterRowList.GetAnItem;
        end;
        ptLast: begin
          IRec := FParentTable.FMasterRowList.GetLastItem;
        end;
        ptNext: begin
          if not (Assigned(Cursor) and Assigned(Cursor.Row)) then
            raise EMemDBAPIException.Create(S_API_NEXT_PREV_REQUIRES_CURRENT_ROW);
          IRec := Cursor.Row.FMasterRec;
          FParentTable.FMasterRowList.GetAnotherItem(IRec);
        end;
        ptPrevious: begin
          if not (Assigned(Cursor) and Assigned(Cursor.Row)) then
            raise EMemDBAPIException.Create(S_API_NEXT_PREV_REQUIRES_CURRENT_ROW);
          IRec := Cursor.Row.FMasterRec;
          FParentTable.FMasterRowList.GetPreviousItem(IRec);
        end;
        //ptInvalid won't happen here.
      end;
      //If the IRec is NULL, then we really are done searching.
      Retry := Assigned(IRec);
      while Retry do
      begin
        Row := IRec.Item as TMemDBRow;
        Retry := not Row.PinForCursor(FTid);
        //TODO - Number of acceptable retries before we raise concurrency exception?
        if Retry then
        begin
          //OK, no luck with that one, try next along the line.
          case RetryPos of
            ptNext: FParentTable.FMasterRowList.GetAnotherItem(IRec);
            ptPrevious: FParentTable.FMasterRowList.GetPreviousItem(IRec);
          else
            Assert(false);
          end;
          Retry := Assigned(IRec);
        end;
      end;
    finally
      FParentTable.FMasterRowLock.Release;
    end;
    if not Assigned(IRec) then
      Row := nil;
  end
  else
  begin
    IPin := nil;
    FndIdx := -1;
    case Pos of
      ptFirst, ptLast: begin
        INode := IRoot.Locate(abCurrent, Pos, nil);
      end;
      ptNext, ptPrevious: begin
        if not (Assigned(Cursor) and Assigned(Cursor.Row)) then
          raise EMemDBAPIException.Create(S_API_NEXT_PREV_REQUIRES_CURRENT_ROW);
        if Cursor.IterIndex = IRoot then
        begin
          INode := Cursor.FIterInode;
          if not (Assigned(INode)
            and IRoot.CheckPresent(abCurrent, INode)) then
            raise EMemDbInternalException.Create(S_NEXT_PREV_BY_IDX_NO_INODE);
        end
        else
        begin
          //Oh dear, we need to go on a hunt for the INode.
          //Provided MemDBAPI has handled the cursors OK (no cursor for deleted row),
          //then we should be able to find one.
          IPinList := Cursor.Row.MakeCandidateIPins(IRoot);
          try
            //The one which we actually find should be local index, so
            //guaranteed to persist, but the others are not.
            fndcnt := 0;
            for i := 0 to Pred(IPinList.Count) do
            begin
              IPin := PMemDBIndexPin(IPinList.Items[i]);
              Assert(Assigned(IPin));
              if IRoot.CheckPresent(abCurrent, IPin.INode) then
              begin
                fndidx := i;
                Inc(FndCnt);
{$IFOPT C+}
                break;
{$ENDIF}
              end;
            end;
            if FndCnt <> 1 then
              raise EMemDbInternalException.Create(S_NEXT_PREV_BY_IDX_INODE_FIND_FAILED);
            IPin := IPinList.Items[FndIdx]; //This one is not ephemeral.
          finally
            while IPinList.Count > 0 do
            begin
              //Cheeky releasing all of them, but the one we want is in the local index...
              Cursor.Row.ReleaseIndexPinOutsideLock(PMemDbIndexPin(IPinList.Items[Pred(IPinList.Count)]));
              IPinList.Delete(Pred(IPinList.Count));
            end;
            IPinList.Free;
          end;
          //OK, we have our INode.
          INode := IPin.INode;
          Assert(Assigned(INode));
        end;
      end;
    else
      raise EMemDBInternalException.Create(S_MOVE_ROW_NOT_VALID_POSITION);
    end;

    //Have our INode, do the initial Next/Prev.
    INode := IRoot.Locate(abCurrent, Pos, INode);
    Retry := Assigned(INode);
    while Retry do
    begin
      Row := INode.Row as TMemDBRow;
      IRec := Row.FMasterRec;
      Retry := not Row.PinForCursorFromInode(FTid, INode, pinEvolve);
      //TODO - Number of acceptable retries before we raise concurrency exception?
      if Retry then
      begin
        INode := IRoot.Locate(abCurrent, RetryPos, INode);
        Retry := Assigned(INode);
      end;
    end;
    if not Assigned(INode) then
    begin
      Row := nil;
      IRec := nil;
    end;
  end;

  Assert(Assigned(IRec) = Assigned(Row));
  if Assigned(Row) then
  begin
    //New row just been pinned. Always allocate new cursor.
    result := TMemDbCursor.Create;
    result.SetRowPin(FTid, Row); //Unpins old row.
    Assert(Assigned(IRoot) = Assigned(INode));
    result.FIterIndex := IRoot;
    result.FIterInode := INode;
    result.FIterInc := RetryPos;
    result.FTidLocal := self;
  end
  else
    result := nil;
end;

function TTidLocal.UserFindRowByIndexRoot(IndexDef: TMemIndexDef;
                                FieldDefs: TMemFieldDefs;
                                FieldAbsIdxs: TFieldOffsets;
                                IRoot: TMemDbIndex;
                                const DataRecs: TMemDbFieldDataRecs): TMemDbCursor;
var
  i: integer;
  SV: TMemDBIndexSearchVal;
  ILeaf: TMemDBIndexLeaf;
  Cursor, NewCursor: TMemDBCursor;
  Row: TMemDBRow;
  Cur: TMemTableMetadataItem;
  AllFieldDefs: TMemStreamableList;
  RowFields: TMemStreamableList;
  S: TMemDBStreamable;
  BufSel: TABSelType;
begin
  if not Assigned(IRoot) then
    raise EMemDbInternalException.Create(S_API_SEARCH_NO_INDEX_SPECIFIED);

  if (Length(FieldDefs) <> Length(DataRecs)) or (Length(DataRecs) = 0) then
    raise EMemDbInternalException.Create(S_API_SEARCH_BAD_FIELD_COUNT);
    //Internal, should have been spotted before now.
  for i := 0 to Pred(Length(FieldDefs)) do
  begin
    Assert(FieldDefs[i].FieldIndex = FieldAbsIdxs[i]); //No delete sentinels, fields compact.
    if not (FieldDefs[i].FieldType in IndexableFieldTypes) then
      raise EMemDBInternalException.Create(S_INDEX_BAD_FIELD_TYPE);
    if DataRecs[i].FieldType <> FieldDefs[i].FieldType then
      raise EMemDBAPIException.Create(S_API_SEARCH_VAL_BAD_TYPE);
  end;
  SV := TMemDbIndexSearchVal.Create;
  Cursor := nil;
  NewCursor := nil;
  result := nil;
  try
    SV.FFieldSearchVals := CopyDataRecs(DataRecs);
    ILeaf := IRoot.Find(abCurrent, SV);
    if Assigned(ILeaf) then
    begin
      Row := ILeaf.Row as TMemDBRow;
      Cursor := TMemDBCursor.Create;
      Cursor.SetRowPin(FTid, Row);
      Cursor.FIterIndex := IRoot;
      Cursor.FIterInode := ILeaf;
      Cursor.FIterInc := ptNext;
      Cursor.FTidLocal := self;

      S := FParentTable.Metadata.PinCurrent(FTid, pinEvolve);
      if NotAssignedOrSentinel(S) then
        raise EMemDBInternalException.Create(S_NAV_TABLE_METADATA_NOT_COMMITED);
      Cur := S as TMemTableMetadataItem;
      AllFieldDefs := Cur.FieldDefs;

      if Row.PinForCursorFromInode(FTid, ILeaf, pinEvolve) then
      begin
        //Do not need to check field defs etc, will be done by subsequent read.
{$IFOPT C+}
        //Unless debug build, in which case, check here for sanity.
        if not Cursor.Row.CheckFormatAgainstMetaDefs(
          FTid, abLatest, AllFieldDefs, pinEvolve) then
          raise EMemDBConcurrencyException.Create(S_TABLE_FORMAT_CONCURRENTLY_CHANGED_NAV_0);

        S := Cursor.Row.GetPinLatest(FTid, BufSel, pinEvolve);
        if NotAssignedOrSentinel(S) then
          raise EMemDbInternalException.Create(S_ROW_DELETED_NAV_0); //This should have been caught earlier.
        RowFields := S as TMemStreamableList;

        if not SearchValFieldsSame(DataRecs, RowFields, FieldAbsIdxs) then
          raise EMemDBInternalException.Create(S_FIELDS_DONT_MATCH_IN_SEARCH);
{$ENDIF}
        result := Cursor;
        Cursor := nil;
      end
      else
      begin
        //Pin failed. Try to pin adjacent, but need to check format is as expected
        //and that all fields are the same for it to be a good find.

        //Unfortunately, this means that when doing EdgeByIndex, we might
        //end up traversing back and forth a bit, but c'est la vie.
        NewCursor := UserMoveToRowByIndexRoot(IRoot, Cursor, Cursor.FIterInc);
        if Assigned(NewCursor) then
        begin
          if not NewCursor.Row.CheckFormatAgainstMetaDefs(
            FTid, abLatest, AllFieldDefs, pinEvolve) then
            raise EMemDBConcurrencyException.Create(S_TABLE_FORMAT_CONCURRENTLY_CHANGED_NAV_1);

          S := NewCursor.Row.GetPinLatest(FTid, BufSel, pinEvolve);
          if NotAssignedOrSentinel(S) then
            raise EMemDbInternalException.Create(S_ROW_DELETED_NAV_1); //This should have been caught earlier.
          RowFields := S as TMemStreamableList;

          if SearchValFieldsSame(DataRecs, RowFields, FieldAbsIdxs) then
          begin
            result := NewCursor;
            NewCursor := nil;
            exit;
          end;
        end;

        NewCursor.Free;
        Cursor.FIterInc := ptPrevious;

        NewCursor := UserMoveToRowByIndexRoot(IRoot, Cursor, Cursor.FIterInc);
        if Assigned(NewCursor) then
        begin
          if not NewCursor.Row.CheckFormatAgainstMetaDefs(
            FTid, abLatest, AllFieldDefs, pinEvolve) then
            raise EMemDBConcurrencyException.Create(S_TABLE_FORMAT_CONCURRENTLY_CHANGED_NAV_2);

          S := NewCursor.Row.GetPinLatest(FTid, BufSel, pinEvolve);
          if NotAssignedOrSentinel(S) then
            raise EMemDbInternalException.Create(S_ROW_DELETED_NAV_2); //This should have been caught earlier.
          RowFields := S as TMemStreamableList;

          if SearchValFieldsSame(DataRecs, RowFields, FieldAbsIdxs) then
          begin
            result := NewCursor;
            NewCursor := nil;
            exit;
          end;
        end;
      end;
    end;
  finally
    SV.Free;
    Cursor.Free;
    NewCursor.Free;
  end;
end;

procedure TTidLocal.UserDeleteRow(Cursor: TMemDbCursor);
var
  Row: TMemDbRow;
begin
  if not Assigned(Cursor) then
    raise EMemDbInternalException.Create(S_CURSOR_NOT_ASSIGNED_AT_DELETE);
  Row := Cursor.Row;
  Row.RemoveFromLocalIndices(self);
  //If next assigned, row delete will overwrite OK.
  //No new sorts of conflicts to deal with here.
  Row.Delete(FTid);
end;

procedure TTidLocal.UserWriteRowData(var Cursor:TMemDbCursor; FieldList: TMemStreamableList);
var
  Appending: boolean;
  Row: TMemDBRow;
  Next: TMemDBStreamable;
  NewData: TMemRowFields;
  Guid: TGuid;
  CurMeta: TMemTableMetadataItem;
  PinOK: boolean;
begin
  Appending := not Assigned(Cursor);
  Assert(Assigned(FieldList));
  //I am in TidLocal, so atomic Meta/Index pin has already happened.
  CurMeta := FParentTable.META_PinCurrent(FTid, pinEvolve) as TMemTableMetadataItem;
  TMemDBRow.StaticCheckFormatAgainstMeta(FieldList, CurMeta);

  if Appending then
  begin
    CreateGUID(Guid);
    Row := TMemDBRow.Create;
    Row.Init(FParentTable, Guid);
    NewData := TMemRowFields.Create;
    NewData.Sparse := false; //Running off current meta fields.
    NewData.DeepAssign(FieldList);
    Row.DirectSetNext(FTid, NewData);
    Row.FProxy.Release;

    Row.AddToLocalIndices(self);

    Cursor := TMemDBCursor.Create;
    Cursor.SetRowPin(FTid, Row);
    Cursor.FIterIndex := nil;
    Cursor.FIterInode := nil;
    Cursor.FIterInc := ptInvalid;
    Cursor.FTidLocal := self;
    PinOK := Row.PinForCursor(FTid);
    Assert(PinOK);
  end
  else
  begin
    Row := Cursor.Row; //Already Pinned.
    if not Assigned(Row) then
      raise EMemDbInternalException.Create(S_CURSOR_HAS_NO_ROW_AT_MODIFY);

    //RequestChange will barf if it tries to undelete.
    //Caught nicely in isolation >= RepeatableRead.
    //Caught less nicely in isolation CommittedRead
    Row.RemoveFromLocalIndices(self);

    Row.RequestChange(FTid);
    Next := Row.GetNext(FTid);
    Next.DeepAssign(FieldList);

    Row.AddToLocalIndices(self);
  end;
end;

(*

  CreateNext := not Assigned(Row.ABData[abNext]);
  if not CreateNext then
  begin
    //Already deleted.
    if Row.ABData[abNext] is TMemDeleteSentinel then
      raise EMemDBInternalException.Create(S_API_UNDELETING_ROW_AT_POST_TIME);
    //Modified after we read it.
    if Iso <> ilDirtyRead then
      raise EMemDBAPIException.Create(S_API_POST_COMMITTED_OVERWRITE);
  end;
  if not Appending then
  begin
    IRet := Store.RemoveItemInPlace(Cursor, LPos);
    if IRet <> rvOK then
      raise EMemDBInternalException.Create(S_API_POST_INTERNAL);
    Row.FStoreBackRec := nil;
    RmRowQuickLists(Row, QLPos, QLHead);
  end
  else
  begin
    LPos := nil;
    QLPos := nil;
    QLHead := nil;
  end;
  if CreateNext then
    Row.ABData[abNext] := TMemStreamableList.Create;
  Row.ABData[abNext].DeepAssign(Fields);
  IRet := Store.AddItemInPlace(Row, LPos, Cursor);
  if IRet <> rvOK then
    raise EMemDBInternalException.Create(S_API_POST_INTERNAL);
  Row.FStoreBackRec := Cursor;
  AddRowQuickLists(Row, QLPos, QLHead);
end;
*)

{ TRowIndexNode }

function TRowIndexNode.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode):integer;
var
   Own, Other: TMemDBRow;
begin
  Own := OwnItem as TMemDBRow;
  Other := OtherItem as TMemDBRow;
  result := CompareGuids(Other.FRowID, Own.FRowID);
end;

{ TRowSearchVal }

function TRowSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  Other: TMemDBRow;
begin
  Assert(not Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Other := OtherItem as TMemDBRow;
  result := CompareGuids(Other.FRowID, FGuid);
end;

procedure TRowSearchVal.Init(const RowGuid: TGuid);
begin
  FGuid := RowGuid;
end;

{ TMemDBTablePersistent }

function TMemDBTablePersistent.AddAllRowProhibitInCrit(const Tid: TTRansactionId;
  ProhibitAdd, ProhibitChange, ProhibitDelete: boolean): boolean;
var
  Lock: PRowChangeLock;
begin
  Lock := PRowChangeLock(FAdditionLocks.FLink.Owner);
  while Assigned(Lock) do
  begin
    if Lock.Tid = Tid then
    begin
      result := false;
      Lock.ProhibitAdd := Lock.ProhibitAdd or ProhibitAdd;
      Lock.ProhibitChange := Lock.ProhibitChange or ProhibitChange;
      Lock.ProhibitDelete := Lock.ProhibitDelete or ProhibitDelete;
      exit;
    end;
    Lock :=  PRowChangeLock(Lock.FSiblings.FLink.Owner);
  end;
  result := true;
  Lock := NewRowProhibitLock;
  Lock.ProhibitAdd := ProhibitAdd;
  Lock.ProhibitChange := ProhibitChange;
  Lock.ProhibitDelete := ProhibitDelete;
  Lock.Tid := Tid;
  DLListInsertTail(@FAdditionLocks, @Lock.FSiblings);
end;

procedure TMemDbTablePersistent.GetProhibitions(const Tid: TTRansactionId;
                         var AddProhibited: boolean;
                         var ChangeProhibited: boolean;
                         var DeleteProhibited: boolean);
var
  PLock: PRowChangeLock;
begin
  AddProhibited := false;
  ChangeProhibited := false;
  DeleteProhibited := false;
  FMasterRowLock.Acquire;
  try
    PLock := PRowChangeLock(FAdditionLocks.FLink.Owner);
    while Assigned(PLock) do
    begin
      if PLock.Tid <> Tid then
      begin
        AddProhibited := AddProhibited or PLock.ProhibitAdd;
        ChangeProhibited := ChangeProhibited or PLock.ProhibitChange;
        DeleteProhibited := DeleteProhibited or PLock.ProhibitDelete;
        exit;
      end;
      PLock :=  PRowChangeLock(PLock.FSiblings.FLink.Owner);
    end;
  finally
     FMasterRowLock.Release;
  end;
end;

function TMemDbTablePersistent.FindRowProhibitLock(const Tid: TTransactionId): PRowChangeLock;
var
  PLock: PRowChangeLock;
begin
  FMasterRowLock.Acquire;
  try
    PLock := PRowChangeLock(FAdditionLocks.FLink.Owner);
    while Assigned(PLock) do
    begin
      if PLock.Tid = Tid then
      begin
        result := PLock;
        exit;
      end;
      PLock :=  PRowChangeLock(PLock.FSiblings.FLink.Owner);
    end;
  finally
     FMasterRowLock.Release;
  end;
  result := nil;
end;

procedure TMemDBTablePersistent.FindRmRowProhibitLock(const Tid: TTransactionId);
var
  PLock: PRowChangeLock;
begin
  FMasterRowLock.Acquire;
  try
    PLock := PRowChangeLock(FAdditionLocks.FLink.Owner);
    while Assigned(PLock) do
    begin
      if PLock.Tid = Tid then
      begin
        DisposeRowProhibitLock(PLock);
        exit;
      end;
      PLock :=  PRowChangeLock(PLock.FSiblings.FLink.Owner);
    end;
  finally
     FMasterRowLock.Release;
  end;
end;

function TMemDbTablePersistent.NewRowProhibitLock: PRowChangeLock;
begin
{$IFOPT C+}
  result := TRowChangeLock.Create;
{$ELSE}
  New(Result);
{$ENDIF}
  DLItemInitObj(TObject(result), @result.FSiblings);
end;

procedure TMemDbTablePersistent.DisposeRowProhibitLock(Lock: PRowChangeLock);
begin
  DLListRemoveObj(@Lock.FSiblings);
{$IFOPT C+}
  Lock.Free;
{$ELSE}
  Dispose(Lock);
{$ENDIF}
end;


constructor TMemDBTablePersistent.Create;
var
  RV: TIsRetVal;
begin
  inherited;
  FMasterRowLock := TCriticalSection.Create;
  FMasterRowList := TIndexedStoreO.Create;
  RV := FMasterRowList.AddIndex(TRowIndexNode, Pointer(1), false);
  if RV <> rvOK then
    raise EMemDBInternalException.Create(S_INTERNAL_INDEXING_ERROR);

  FTidLocalLock := TCriticalSection.Create;
  FMetaIndexLock := TCriticalSection.Create;
  FMasterIndexes := TReffedList.Create;
  DLItemInitList(@FTidLocalStructures);
  DlItemInitList(@FAdditionLocks);
  FMetadata := TMemDbTableMetadata.Create;
end;

destructor TMemDBTablePersistent.Destroy;
begin
  //If referencing works correctly, then all should be empty by the time we destroy.
  FMasterRowLock.Acquire;
  try
    Assert(FMasterRowList.Count = 0);
    Assert(DLItemIsEmpty(@FAdditionLocks));
  finally
    FMasterRowLock.Release;
  end;
  FTidLocalLock.Acquire;
  try
    Assert(DlItemIsEmpty(@FTidLocalStructures));
  finally
    FTidLocalLock.Release;
  end;
  FMasterRowList.Free;
  FMasterRowLock.Free;
  FTidLocalLock.Free;
  FMetaIndexLock.Free;
  FMasterIndexes.Release;
  FMetadata.Free;

{$IFDEF MEMDB2_TEMP_REMOVE}
  RemoveOldTagStructs(true);
  FData.Free;
  FIndexHelper.Free;
  FFieldHelper.Free;
  FEmptyList.Free;
  FTagDataList.Free;
{$ENDIF}
  inherited;
end;

{$IFDEF MEMDB2_TEMP_REMOVE}

function TMemDbTablePersistent.GetDataChanged: boolean;
begin
  result := FData.AnyChangesAtAll;
end;

function TMemDBTablePersistent.HasTableData: boolean;
begin
  result := FData.Store.Count > 0;
end;

function TMemDBTablePersistent.AnyChangesAtAll: boolean;
begin
  result := inherited;
  result := result or FData.AnyChangesAtAll;
end;

procedure TMemDBTablePersistent.UpdateHelpersAndIndices;
var
  CC, NC: TMemTableMetadataItem;
begin
  if AssignedNotSentinel(FMetadata.ABData[abCurrent]) then
  begin
    CC := FMetaData.ABData[abCurrent] as TMemTableMetadataItem;
    FFieldHelper.List[abCurrent] := CC.FieldDefs;
    FIndexHelper.List[abCurrent] := CC.IndexDefs;
  end
  else
  begin
    FFieldHelper.List[abCurrent] := FEmptyList;
    FIndexHelper.List[abCurrent] := FEmptyList;
  end;
  if AssignedNotSentinel(FMetadata.ABData[abNext]) then
  begin
    NC := FMetaData.ABData[abNext] as TMemTableMetadataItem;
    FFieldHelper.List[abNext] := NC.FieldDefs;
    FIndexHelper.List[abNext] := NC.IndexDefs;
  end
  else
  begin
    FFieldHelper.List[abNext] := FEmptyList;
    FIndexHelper.List[abNext] := FEmptyList;
  end;
end;

procedure TMemDBTablePersistent.HandleHelperChangeRequest(Sender: TObject);
begin
  FMetadata.RequestChange;
  UpdateHelpersAndIndices;
end;

{$ENDIF}

procedure TMemDBTablePersistent.RowDCPHandleAndForward(Sender: TObject; const Update: TReferenceUpdate);
var
  RefChange: boolean;
  Res: TItemRec;
  RV: TIsRetVal;
  Item: TMemDBRow;
begin
  RefChange := false;
  Assert(Assigned(Sender) and (Sender is TMemDBRow));
  Item := Sender as TMemDBRow;
  if Update.Pre <> Update.Post then
  begin
    if Update.Post then
    begin
      Item.FProxy.AddRef;

      FMasterRowLock.Acquire;
      try
        RefChange := FMasterRowList.Count = 0;
        RV := FMasterRowList.AddItem(Item, Res);
        if RV <> rvOK then
          raise EMemDbInternalException.Create(S_JOURNAL_REPLAY_DUP_INST_ID);
          //TODO - Get some ideas of all the diverse ways this might percolate out.
        Item.FMasterRec := Res;
      finally
        FMasterRowLock.Release;
      end;
      if RefChange then
        HandleAddRefForData(self);
    end
    else
    begin
      FMasterRowLock.Acquire;
      try
        RV := FMasterRowList.RemoveItem(Item.FMasterRec);
        if RV <> rvOK then
          raise EMemDbInternalException.Create(S_INTERNAL_INDEXING_ERROR);
          //TODO - Get some ideas of all the diverse ways this might percolate out.
        Item.FMasterRec := nil;
        RefChange := FMasterRowList.Count = 0;
      finally
        FMasterRowLock.Release;
      end;
      Item.FProxy.Release;
      
      if RefChange then
        HandleReleaseForData(self);
      //No data, changes, or pins, not ref counted.
    end;
  end;
end;

procedure TMemDBTablePersistent.RowCPTidHandleAndForward(Sender: TObject; const Update: TTidUpdate);
var
  TidLocal: TTidLocal;
begin
  Assert(Assigned(Sender));
  if Update.Ref.Pre <> Update.Ref.Post then
  begin
    if Update.Ref.Post then
      TidLocal := GetMakeTidLocal(Update.Tid, pinEvolve)
    else
    begin
      TidLocal := GetTidLocal(Update.Tid);
      Assert(Assigned(TidLocal));
    end;
    TidLocal.RowCPTidHandle(Sender, Update);
  end;
end;

procedure TMemDBTablePersistent.HandleAddRefForTidLocal(Sender: TObject);
begin
  Assert(Sender is TTidLocal);
  Assert(Assigned(FProxy));
  FProxy.AddRef;
end;

procedure TMemDBTablePersistent.HandleReleaseForTidLocal(Sender: TObject);
begin
  Assert(Sender is TTidLocal);
  Assert(Assigned(FProxy));
  FProxy.Release;
end;

procedure TMemDBTablePersistent.HandleAddRefForData(Sender: TObject);
begin
  Assert(Sender  = Self);
  Assert(Assigned(FProxy));
  FProxy.AddRef;
end;

procedure TMemDbTablePersistent.HandleReleaseForData(Sender: TObject);
begin
  Assert(Sender  = Self);
  Assert(Assigned(FProxy));
  FProxy.Release;
end;

procedure TMemDBTablePersistent.ToJournal(const Tid: TTransactionId; Stream: TStream);
var
  TidLocal: TTidLocal;
  Meta: TMemDBStreamable;
  MetaChange, DataChange: boolean;
  tmpSelected: TABSelType;
begin
  TidLocal := GetMakeTidLocal(Tid, pinEvolve);
  TidLocal.UpdateLayout(pinEvolve);
  //TODO - Do I absolutely need to update layout here or not?
  //I don't think so if always updated on field change.
  MetaChange := Assigned(META_GetNext(Tid));

  if TidLocal.DataChanged and TidLocal.LayoutChangeRequired then
    raise EMemDBInternalException.Create(S_DATA_AND_LAYOUT_CHANGE_TO_JOURNAL);

  DataChange := TidLocal.DataChanged;
  if TidLocal.LayoutChangeRequired then
  begin
    TidLocal.AdjustTableStructureForMetadata;
    if TidLocal.FIndexingChangeRequired then
      TidLocal.BuildValidateNewIndexesOutsideCommitLock;
  end;

  if MetaChange or DataChange then
  begin
    WrTag(Stream, mstTableStart);
    if MetaChange then
      FMetadata.ToJournal(Tid, Stream)
    else
    begin
      WrTag(Stream, mstTableUnchangedName);
      Meta := META_GetPinLatest(Tid, tmpSelected, pinEvolve);
      WrStreamString(Stream, (Meta as TMemEntityMetadataItem).EntityName);
    end;
    if DataChange then
    begin
      TidLocal.ToJournal(Stream);
    end;
    WrTag(Stream, mstTableEnd);
  end;
end;

procedure TMemDBTablePersistent.ToScratch(const PseudoTid: TTransactionId; Stream: TStream);
var
  TidLocal:TTidLocal;
begin
  TidLocal := GetMakeTidLocal(PseudoTid, pinEvolve);
  TidLocal.UpdateLayout(pinEvolve);
  //TODO - Do I absolutely need to update layout here or not?
  //I don't think so if always updated on field change.

  //In Entity list if CPTids, however does not guarantee has data.
  //Stream only if current metdata indicates not NULL.
  // TODO - Do this for foreign keys too.
  if Assigned(META_PinCurrent(PseudoTid, pinEvolve)) then
  begin
    WrTag(Stream, mstTableStart);
    FMetadata.ToScratch(PseudoTid, Stream);
    TidLocal.ToScratch(Stream);
    WrTag(Stream, mstTableEnd);
  end;
end;

procedure TMemDBTablePersistent.LookaheadHelper(Stream: TStream;
                                                var MetadataInStream: boolean;
                                                var DataInStream: boolean);
var
  Pos: Int64;
  Tag: TMemStreamTag;
begin
  MetadataInStream := false;
  DataInStream := false;
  Pos := Stream.Position;
  Tag := RdTag(Stream);
  case Tag of
    mstTableUnchangedName, mstTableEnd: ;//Ignore, handled by ordering.
    mstDblBufferedStart: MetadataInStream := true;
    mstIndexedListStart: DataInStream := true;
  else
    raise EMemDBException.Create(S_TABLE_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
  end;
  Stream.Seek(Pos, TSeekOrigin.soBeginning);
end;

procedure TMemDBTablePersistent.FromJournal(const PseudoTid: TTransactionId; Stream: TStream);
var
  MetadataInStream, DataInStream: boolean;
  TblName: string;
  TidLocal: TTidLocal;
  Cur: TMemTableMetadataItem;
begin
  ExpectTag(Stream, mstTableStart);
  //Unfortunately, we don't have an up to date view of whether
  //layout change is required at this point, so just have
  //to unstream data anyway.

  TidLocal := GetMakeTidLocal(PseudoTid, pinEvolve);
  LookaheadHelper(Stream, MetadataInStream, DataInStream);
  if MetadataInStream then
  begin
    FMetadata.FromJournal(PseudoTid, Stream);
    TidLocal.UpdateLayout(pinEvolve);
  end
  else
  begin
    ExpectTag(Stream, mstTableUnchangedName);
    TblName := RdStreamString(Stream);
    Cur := META_PinCurrent(PseudoTid, PinEvolve) as TMemTableMetadataItem;
    if CompareStr(TblName, Cur.EntityName) <> 0 then
      raise EMemDBInternalException.Create(S_JOURNAL_REPLAY_NAMES_INCONSISTENT);
  end;
  LookaheadHelper(Stream, MetadataInStream, DataInStream);

  if DataInStream and TidLocal.LayoutChangeRequired then
    raise EMemDbException.Create(S_JOURNAL_DATA_WITH_LAYOUT_CHANGE);

  if DataInStream then
    TidLocal.FromJournal(Stream);

  ExpectTag(Stream, mstTableEnd);

  if TidLocal.LayoutChangeRequired then
  begin
    TidLocal.AdjustTableStructureForMetadata;
    if TidLocal.FIndexingChangeRequired then
      TidLocal.BuildValidateNewIndexesOutsideCommitLock;
  end;
end;

procedure TMemDBTablePersistent.FromScratch(const PseudoTid: TTransactionId; Stream: TStream);
var
  TidLocal:TTidLocal;
  Newly: boolean;
begin
  FMasterRowLock.Acquire;
  try
    if FMetadata.AnyChangesForTid(PseudoTid)
       or Assigned(Metadata.PinCurrent(PseudoTid, pinEvolve))
       or (FMasterRowList.Count > 0) then
      raise EMemDBInternalException.Create(S_FROM_SCRATCH_REQUIRES_EMPTY_OBJ);

      Newly := AddAllRowProhibitInCrit(PseudoTid, true, true, true);
      Assert(Newly);
  finally
    FMasterRowLock.Release;
  end;
  ExpectTag(Stream, mstTableStart);
  TidLocal := GetMakeTidLocal(PseudoTid, pinEvolve);
  FMetadata.FromScratch(PseudoTid, Stream);
  TidLocal.UpdateLayout(pinEvolve);
  TidLocal.FromScratch(Stream);
  ExpectTag(Stream, mstTableEnd);
  //Do not expect any contention and all rows modded this txion.
  if TidLocal.FIndexingChangeRequired then
    TidLocal.BuildValidateNewIndexesOutsideCommitLock;
end;

procedure TMemDBTablePersistent.PreCommit(const TId: TTransactionId; Reason: TMemDBTransReason);
var
  Cur, Next: TMemDBStreamable;
  Added, Changed, Deleted, Null: boolean;
  TidLocal: TTidLocal;
begin
  FMetadata.PreCommit(Tid, Reason);
  Cur := META_PinCurrent(Tid, pinFinalCheck);
  Next := META_GetNext(Tid);
  ChangeFlagsFromPinned(Cur, Next, Added, Changed, Deleted, Null);
  if Null then
    exit; //Deleted case, still check all the rows have been deleted.

  //ToJournal / FromJournal has handled the layout change case,
  //which will have created a TidLocal if necessary.
  //All the other row checking can be done there.

  //TODO - Build changed indexes later where early index build not possible.

  TidLocal := GetTidLocal(Tid);
  Assert(Assigned(TidLocal));
  try
    //In cases where "current" changes behind our back, better to concurrency check
    //before building / revalidating indices.
    TidLocal.PreCommit(Reason);
    if TidLocal.FIndexingChangeRequired then
      TidLocal.BuildValidateNewIndexesInsideCommitLock;
  except
    //It's possible (and silly) for commits to raise, and then
    //users to make mods before retrying the commit operation. In such cases,
    //we should roll back the index changes, so they dont get totally barfed.

    //TODO - What with pin reasons and all, it's debatable what changes
    //you should / should not be allowed to make after initial commit attempt.
    //Most people will just rollback.
    //TODO - Check pin reasons and re-attempted commits.

    //TODO - Freeing of stuff outside Commit lock for better performance.
    TidLocal.MetaIndexLocalRollback(Reason);

    raise;
  end;
end;


procedure TMemDBTablePersistent.Commit(const TId: TTransactionId; Reason: TMemDbTransReason);
var
  TidLocal: TTidLocal;
begin
  //Assume if metadata deleted or null, then changes have been made to data structs.
  //Either changed layout, or deleted all the fields. Need to always commit, because
  //CPTid clearing required.

  //Don't lock master row list, instead used Tid Local, even if it has more
  //lock acquisitions.
  TidLocal := GetTidLocal(Tid);
  if Assigned(TidLocal) then
    TidLocal.Commit(Reason);

  FMetaIndexLock.Acquire;
  try
    if Assigned(TidLocal) then
      TidLocal.MetaIndexCommit(Reason);
    inherited;
  finally
    FMetaIndexLock.Release;
  end;
  TidLocal.Free; //TODO - Free outside commit lock for better performance.

  FindRmRowProhibitLock(Tid);
end;

procedure TMemDBTablePersistent.Rollback(const TId: TTransactionId; Reason: TMemDbTransReason);
var
  TidLocal: TTidLocal;
begin
  //Assume if metadata deleted or null, then changes have been made to data structs.
  //Either changed layout, or deleted all the fields. Need to always commit, because
  //CPTid clearing required.

  //Don't lock master row list, instead used Tid Local, even if it has more
  //lock acquisitions.
  TidLocal := GetTidLocal(Tid);
  if Assigned(TidLocal) then
    TidLocal.Rollback(Reason);

  FMetaIndexLock.Acquire;
  try
    if Assigned(TidLocal) then
      TidLocal.MetaIndexRollback(Reason);
    inherited;
  finally
    FMetaIndexLock.Release;
  end;
  TidLocal.Free;  //TODO - Free outside commit lock for better performance.

  FindRmRowProhibitLock(Tid);
end;

{$IFDEF MEMDB2_TEMP_REMOVE}

procedure TMemDbTablePersistent.DimensionChangesets;
var
  i: integer;
begin
  if FMetadata.Null then
  begin
    SetLength(FFieldChangesets, 0);
    SetLength(FIndexChangesets, 0);
  end
  else if FMetadata.Added or FMetadata.Changed then
  begin
    SetLength(FFieldChangesets, FFieldHelper.FNextList.Count);
    SetLength(FIndexChangesets, FIndexHelper.FNextList.Count);
  end
  else if FMetadata.Deleted then
  begin
    SetLength(FFieldChangesets, FFieldHelper.FCurrentList.Count);
    SetLength(FIndexChangesets, FIndexHelper.FNextList.Count);
  end
  else
  begin
    //Nothing changed.
    SetLength(FFieldChangesets, 0);
    SetLength(FIndexChangesets, 0);
  end;

  for i := 0 to Pred(Length(FFieldChangesets)) do
      FFieldChangesets[i] := [];
  for i := 0 to Pred(Length(FIndexChangesets)) do
      FIndexChangesets[i] := [];
end;

{$ENDIF}

function TMemDbTablePersistent.AnyChanges(const Tid:TTransactionId): boolean;
begin
  result := FMetadata.AnyChanges(Tid);
  FTidLocalLock.Acquire;
  try
    result := result or not DLItemIsEmpty(@FTidLocalStructures);
  finally
    FTidLocalLock.Release;
  end;
end;

function TMemDbTablePersistent.AnyChangesForTid(const Tid: TTransactionId): boolean;
var
  TidLocal: TTidLocal;
begin
  TidLocal := GetTidLocal(Tid);
  result := FMetadata.AnyChangesForTid(Tid) or
   (Assigned(TidLocal) and TidLocal.DataChanged);
end;

function TMemDbTablePersistent.DataChangedForTid(const Tid:TTransactionId): boolean;
var
  TidLocal: TTidLocal;
begin
  TidLocal := GetTidLocal(Tid);
  result := Assigned(TidLocal) and TidLocal.DataChanged;
end;

function TMemDbTablePersistent.LayoutChangesRequiredForTid(const Tid: TTransactionId): boolean;
var
  TidLocal: TTidLocal;
begin
  TidLocal := GetTidLocal(Tid);
  result := Assigned(TidLocal) and TidLocal.LayoutChangeRequired;
end;

function TMemDbTablePersistent.GetTidLocal(const Tid: TTransactionId): TTidLocal;
begin
  FTidLocalLock.Acquire;
  try
    result := FTidLocalStructures.FLink.Owner as TTidLocal;
    while Assigned(result) do
    begin
      if result.FTid = Tid then
        break;
      result := result.FTidLocalLinks.FLink.Owner as TTidLocal;
    end;
  finally
    FTidLocalLock.Release;
  end;
end;

function TMemDBTablePersistent.GetMakeTidLocal(const Tid: TTransactionId; PinReason: TPinReason): TTidLocal;
var
  ToCreate: boolean;
begin
  FTidLocalLock.Acquire;
  try
    result := FTidLocalStructures.FLink.Owner as TTidLocal;
    while Assigned(result) do
    begin
      if result.FTid = Tid then
        break;
      result := result.FTidLocalLinks.FLink.Owner as TTidLocal;
    end;
    ToCreate := not Assigned(Result);
    if ToCreate then
    begin
      result := TTidLocal.Create;
      //Recursive lock acquire, but not a problem.
      result.Init(self, Tid);
    end;
  finally
    FTidLocalLock.Release;
  end;
  if ToCreate then
    result.UpdateLayout(PinReason);
end;

function TMemDBTablePersistent.GetMakeListHelpers(const Tid: TTransactionId;
                                                   var FieldHelper: TMemDblBufListHelper;
                                                   var IndexHelper: TMemDblBufListHelper): TTidLocal;
begin
  result := GetMakeTidLocal(Tid, pinEvolve);
  FieldHelper := result.FFieldHelper;
  IndexHelper := result.FIndexHelper;
end;

//TODO - Move this into TTidLocal?
procedure TMemDBTablePersistent.GetCurrNxtMetaCopiesEx(const Tid: TTransactionId;
                                           var CC, NC: TMemTableMetadataItem;
                                           var CCFieldCount, CCIndexCount,
                                           NCFieldCount, NCINdexCount: integer;
                                           PinReason: TPinReason); //TODO - Do we need pin reason.
var
  Current, Next: TMemDBStreamable;
begin
  Assert(Assigned(GetTidLocal(Tid)));
  Current := FMetadata.PinCurrent(Tid, PinReason);
  Next := FMetadata.GetNext(Tid);
  CC := nil;
  NC := nil;
  CCFieldCount := 0;
  CCIndexCount := 0;
  NCFieldCount := 0;
  NCINdexCount := 0;
  if AssignedNotSentinel(Current) then
    CC := Current as TMemTableMetadataItem;
  if AssignedNotSentinel(Next) then
    NC := Next as TMemTableMetadataItem;
  if Assigned(CC) then
  begin
    CCFieldCount := CC.FieldDefs.Count;
    CCIndexCount := CC.IndexDefs.Count;
  end;
  if Assigned(NC) then
  begin
    NCFieldCount := NC.FieldDefs.Count;
    NCIndexCount := NC.IndexDefs.Count;
  end;
end;

procedure TMemDBTablePersistent.GetCurrNxtMetaCopies(const Tid: TTransactionId;
                                                     var CC, NC: TMemTableMetadataItem;
                                                     PinReason: TPinReason);
var
  CCFieldCount, NCFIeldCount, CCIndexCount, NCIndexCount: integer;
begin
  GetCurrNxtMetaCopiesEx(Tid, CC,NC, CCFieldCount, NCFieldCount, CCIndexCount, NCIndexCount, PinReason);
end;

function TMemDbTablePersistent.META_PinCurrent(const Tid: TTransactionId; Reason: TPinReason): TMemDBStreamable;
begin
  GetMakeTidLocal(Tid, Reason);
  result := inherited;
end;

function TMemDbTablePersistent.META_GetPinLatest(const Tid: TTransactionId;
                        var BufSelected: TAbSelType; Reason: TPinReason): TMemDBStreamable;
begin
  GetMakeTidLocal(Tid, Reason);
  result := inherited;
end;

procedure TMemDbTablePersistent.META_RequestChange(const Tid: TTransactionId);
begin
  GetMakeTidLocal(Tid, pinEvolve);
  inherited;
end;

procedure TMemDbTablePersistent.META_Delete(const Tid: TTransactionId);
begin
  GetMakeTidLocal(Tid, pinEvolve);
  inherited;
end;

function TMemDbTablePersistent.META_FieldsByNames(const AB: TBufSelector; const Names: TMDBFieldNames; var AbsIdxs: TFieldOffsets): TMemFieldDefs;
begin
  if AB.SelType <> TAbSelType.abNext then
    GetMakeTidLocal(AB.TId, pinEvolve);
  result := (Metadata as TMemDbTableMetadata).FieldsByNames(AB, Names, AbsIdxs, pinEvolve);
end;

function TMemDbTablePersistent.META_FieldByName(const AB: TBufSelector; const Name: string; var AbsIndex: integer): TMemFieldDef;
begin
  if AB.SelType <> TAbSelType.abNext then
    GetMakeTidLocal(AB.TId, pinEvolve);
  result := (Metadata as TMemDBTableMetadata).FieldByName(AB, Name, AbsIndex, pinEvolve);
end;

function TMemDbTablePersistent.META_IndexByName(const AB: TBufSelector; const Name: string; var AbsIndex: integer): TMemIndexDef;
begin
  if AB.SelType <> TAbSelType.abNext then
    GetMakeTidLocal(AB.TId, pinEvolve);
  result := (Metadata as TMemDbTableMetadata).IndexByName(AB, Name, AbsIndex, pinEvolve);
end;

function TMemDbTablePersistent.GetUserTidLocalIndexRoot(const Tid: TTransactionId; var TidLocal: TTidLocal; var Idx: TMemIndexDef; IdxName: string): TMemDBIndex;
begin
  TidLocal := GetMakeTidLocal(Tid, pinEvolve);
  result := TidLocal.GetUserIndexRoot(Idx, IdxName);
end;

{$IFDEF MEMDB2_TEMP_REMOVE}

procedure TMemDBTablePersistent.CheckTagAgreesWithMetadata(IdxIdx: integer;
                        TagData: TMemDBITagData;
                        IndexState: TTagCheckIndexState;
                        ProgState: TTagCheckProgrammedState);
{$IFOPT C+}
var
  CC,NC: TMemTableMetadataItem;
  CCFieldCount, CCIndexCount, NCFieldCount, NCIndexCount: integer;
  IndexDefCC, IndexDefNC: TMemIndexDef;
  FieldDefsCC, FieldDefsNC: TMemFieldDefs;
  FieldAbsIndexesCC, FieldAbsIndexesNC: TFieldOffsets;
  MSTemp: TMemDBStreamable;
  NeedCurrent, NeedNext:boolean;
  i: integer;
{$ENDIF}
begin
{$IFOPT C+}
  //UpdateHelpersAndIndices - already done at all points where metadata undergoes
  //major rearrangement.
  GetCurrNxtMetaCopiesEx(CC, NC, CCFieldCount, CCIndexCount, NCFieldCount, NCIndexCount);
  //Check tag exists and is where we expect.
  if not Assigned(TagData) then
    raise EMemDBInternalException.Create(S_CHECK_TAGDATA_NO_TAG);
  if (IdxIdx < 0) or (IdxIdx >= FTagDataList.Count) then
    raise EMemDbInternalException.Create(S_CHECK_TAGDATA_ARRAYSIZE);
  if (TagData <> FTagDataList[IdxIdx]) then
    raise EMemDbInternalException.Create(S_CHECK_TAGDATA_NOT_EXPECTED);
  //Consistency with current/next metadata, indexes and fields,
  //specifically field numbers is checked elsewhere (re-gen changesets).
  //Assume field indexes (which are ND indexes) are esentially correct.

  SetLength(FieldDefsCC, 0);
  SetLength(FieldDefsNC, 0);
  NeedCurrent := false;
  NeedNext := false;
  case IndexState of
    tciPermanentAgreesCurrent: NeedCurrent := true;
    tciTemporaryAgreesNextOnly, tciPermanentAgreesNext: NeedNext := true;
    tciTemporaryAgreesBoth:
    begin
      NeedCurrent := true;
      NeedNext := true;
    end;
  else
    Assert(false);
  end;
  if NeedCurrent then
  begin
    if not Assigned(CC) or (IdxIdx >= CCIndexCount) then
      raise EMemDbInternalException.Create(S_CHECK_TAGDATA_NO_META);
    MSTemp := CC.IndexDefs.Items[IdxIdx];
    if NotAssignedOrSentinel(MSTemp) then
      raise EMemDbInternalException.Create(S_CHECK_TAGDATA_NO_META);
    IndexDefCC := MSTemp as TMemIndexDef;
    FieldDefsCC := FieldsByNamesInt(CC, IndexDefCC.FieldArray, FieldAbsIndexesCC);
    if Length(FieldDefsCC) = 0 then
      raise EMemDbInternalException.Create(S_CHECK_TAGDATA_NO_FIELD);
  end;
  if NeedNext then
  begin
    if not Assigned(NC) or (IdxIdx >= NCIndexCount) then
      raise EMemDbInternalException.Create(S_CHECK_TAGDATA_NO_META);
    MSTemp := NC.IndexDefs.Items[IdxIdx];
    if NotAssignedOrSentinel(MSTemp) then
      raise EMemDbInternalException.Create(S_CHECK_TAGDATA_NO_META);
    IndexDefNC := MSTemp as TMemIndexDef;
    FieldDefsNC := FieldsByNamesInt(NC, IndexDefNC.FieldArray, FieldAbsIndexesNC);
    if Length(FieldDefsNC) = 0 then
      raise EMemDbInternalException.Create(S_CHECK_TAGDATA_NO_FIELD);
  end;
  case IndexState of
    tciPermanentAgreesCurrent:
    //Permanent index, default field offset which agrees with CC.IndexDef.FieldNumber
    begin
      if TagData.MainIndexClass <> micPermanent then
        raise EMemDbInternalException.Create(S_TAG_FAILED_INDEX_CLASS_CHECK);

      if not (Length(TagData.DefaultFieldOffsets) = Length(FieldDefsCC)) then
        raise EMemDbInternalException.Create(S_TAG_FAILED_DEFAULT_OFFSET_CHECK);
      for i := 0 to Pred(Length(TagData.DefaultFieldOffsets)) do
      begin
        if TagData.DefaultFieldOffsets[i] <> FieldDefsCC[i].FieldIndex then
          raise EMemDbInternalException.Create(S_TAG_FAILED_DEFAULT_OFFSET_CHECK);
      end;
    end;
    tciPermanentAgreesNext:
    //Permanent index, default field offset which agrees with NC.IndexDef.FieldNumber
    begin
      if TagData.MainIndexClass <> micPermanent then
        raise EMemDbInternalException.Create(S_TAG_FAILED_INDEX_CLASS_CHECK);

      if not (Length(TagData.DefaultFieldOffsets) = Length(FieldDefsNC)) then
        raise EMemDbInternalException.Create(S_TAG_FAILED_DEFAULT_OFFSET_CHECK);
      for i := 0 to Pred(Length(TagData.DefaultFieldOffsets)) do
      begin
        if TagData.DefaultFieldOffsets[i] <> FieldDefsNC[i].FieldIndex then
          raise EMemDbInternalException.Create(S_TAG_FAILED_DEFAULT_OFFSET_CHECK);
      end;
    end;
    tciTemporaryAgreesBoth:
    //Temporary index, default field offset agreeds with NC.IndexDef.FieldNumber,
    //which should prob be the NDIndex of the field, extra field offset is the
    //"old" offset in CC.
    begin
      if TagData.MainIndexClass <> micTemporary then
        raise EMemDbInternalException.Create(S_TAG_FAILED_INDEX_CLASS_CHECK);

      if not (Length(TagData.DefaultFieldOffsets) = Length(FieldDefsNC)) then
        raise EMemDbInternalException.Create(S_TAG_FAILED_DEFAULT_OFFSET_CHECK);
      for i := 0 to Pred(Length(TagData.DefaultFieldOffsets)) do
      begin
        if TagData.DefaultFieldOffsets[i] <> FieldDefsNC[i].FieldIndex then
          raise EMemDbInternalException.Create(S_TAG_FAILED_DEFAULT_OFFSET_CHECK);
      end;

      if not (Length(TagData.ExtraFieldOffsets) = Length(FieldDefsCC)) then
        raise EMemDbInternalException.Create(S_TAG_FAILED_EXTRA_OFFSET_CHECK);
      for i := 0 to Pred(Length(TagData.ExtraFieldOffsets)) do
      begin
        if TagData.ExtraFieldOffsets[i] <> FieldDefsCC[i].FieldIndex then
          raise EMemDbInternalException.Create(S_TAG_FAILED_EXTRA_OFFSET_CHECK);
      end;
    end;
    //Temporary index, default field offset agreeds with NC.IndexDef.FieldNumber,
    //which should prob be the NDIndex of the field, extra field offset is the
    //ND index of the field before any possible rearrangement, which is academic,
    //cos the index didn't exist then.
    tciTemporaryAgreesNextOnly:
    begin
      if TagData.MainIndexClass <> micTemporary then
        raise EMemDbInternalException.Create(S_TAG_FAILED_INDEX_CLASS_CHECK);

      if not (Length(TagData.DefaultFieldOffsets) = Length(FieldDefsNC)) then
        raise EMemDbInternalException.Create(S_TAG_FAILED_DEFAULT_OFFSET_CHECK);
      for i := 0 to Pred(Length(TagData.DefaultFieldOffsets)) do
      begin
        if TagData.DefaultFieldOffsets[i] <> FieldDefsNC[i].FieldIndex then
          raise EMemDbInternalException.Create(S_TAG_FAILED_DEFAULT_OFFSET_CHECK);
      end;
    end;
  else
    Assert(false);
  end;
  case ProgState of
    tcpNotProgrammed:
      if TagData.IdxsSetToStore then
        raise EMemDbInternalException.Create(S_TAG_INDEX_BUILT_NOT_AS_EXPECTED);
    tcpProgrammed:
      if not TagData.IdxsSetToStore then
        raise EMemDbInternalException.Create(S_TAG_INDEX_BUILT_NOT_AS_EXPECTED);
    tcpDontCare: ;
  else
    Assert(false);
  end;
{$ENDIF}
end;

procedure TMemDBTablePersistent.ReGenChangesets;
var
  i, j, NDIndex, ff: integer;
  FieldDef1, FieldDef2: TMemFieldDef;
  FieldDefs1, FieldDefs2: TMemFieldDefs;
  IndexDef1, IndexDef2: TMemIndexDef;
  CC, NC: TMemTableMetadataItem;
  CCFieldCount, CCINdexCount, NCFieldCount, NCIndexCount: integer;
  AbsIndexes1, AbsIndexes2: TFieldOffsets;
begin
  UpdateHelpersAndIndices;
  GetCurrNxtMetaCopiesEx(CC, NC, CCFieldCount, CCIndexCount, NCFieldCount, NCIndexCount);
  DimensionChangesets;
  NDIndex := 0;
  for i := 0 to Pred(NCFieldCount) do
  begin
    //check unique field names.
    if not (Assigned(NC.FieldDefs.Items[i])) then
      raise EMemDbConsistencyException.Create(S_MISSING_FIELD_DEF);
    if not (NC.FieldDefs.Items[i] is TMemDeleteSentinel)  then
    begin
      FieldDef1 := NC.FieldDefs.Items[i] as TMemFieldDef;
      if FieldDef1.FieldName = '' then
        raise EMemDBConsistencyException.Create(S_FIELD_NAME_EMPTY);
      for j := Succ(i) to Pred(NCFieldCount) do
      begin
        if not (Assigned(NC.FieldDefs.Items[j])) then
          raise EMemDbConsistencyException.Create(S_MISSING_FIELD_DEF);
        if not (NC.FieldDefs.Items[j] is TMemDeleteSentinel)  then
        begin
          FieldDef2 := NC.FieldDefs.Items[j] as TMemFieldDef;
          if FieldDef1.FieldName = FieldDef2.FieldName then
            raise EMemDBConsistencyException.Create(S_DUP_FIELD_NAMES);
        end;
      end;
      //check field numbering correct with respect to deleted fields.
      if FieldDef1.FieldIndex <> NDIndex then
        raise EMemDBInternalException.Create(S_BAD_FIELD_INDEX);
      Assert(FFieldHelper.NdIndexToRawIndex(NDIndex) = i);
      Assert(FFieldHelper.RawIndexToNdIndex(i) = NDIndex);
      Inc(NDIndex);
    end;
  end;

  //Check indexes.
  NDIndex := 0;
  for i := 0 to Pred(NCIndexCount) do
  begin
    if not (Assigned(NC.IndexDefs.Items[i])) then
      raise EMemDbConsistencyException.Create(S_MISSING_INDEX_DEF);
    if not (NC.IndexDefs.Items[i] is TMemDeleteSentinel) then
    begin
      IndexDef1:= NC.IndexDefs.Items[i] as TMemIndexDef;
      if IndexDef1.IndexName = '' then
        raise EMemDBConsistencyException.Create(S_INDEX_NAME_EMPTY);
      if IndexDef1.FieldNameCount = 0 then
        raise EMemDBConsistencyException.Create(S_NO_FIELDS_IN_INDEX);
      for ff := 0 to Pred(IndexDef1.FieldNameCount) do
      begin
        if IndexDef1.FieldNames[ff] = '' then
          raise EMemDBConsistencyException.Create(S_FIELD_NAME_EMPTY);
      end;
      for j := Succ(i) to Pred(NCIndexCount) do
      begin
        //Check unique index names.
        if not (Assigned(NC.IndexDefs.Items[j])) then
          raise EMemDbConsistencyException.Create(S_MISSING_INDEX_DEF);
        if not (NC.IndexDefs.Items[j] is TMemDeleteSentinel) then
        begin
          IndexDef2 := NC.IndexDefs.Items[j] as TMemIndexDef;
          if IndexDef1.IndexName = IndexDef2.IndexName then
            raise EMemDBConsistencyException.Create(S_DUP_INDEX_NAMES);
        end;
      end;
      //Indexes not numbered, might reconsider with foreign keys.
      Assert(FIndexHelper.NdIndexToRawIndex(NDIndex) = i);
      Assert(FIndexHelper.RawIndexToNdIndex(i) = NDIndex);
      Inc(NDIndex);
    end;
  end;

  //Check changes between old and new metadata are consistent and valid.
  //First off, fields.
  for i := 0 to Pred(NCFieldCount) do
  begin
    Assert(Assigned(NC.FieldDefs.Items[i]));
    if not (NC.FieldDefs.Items[i] is TMemDeleteSentinel) then
    begin
      FieldDef2 := NC.FieldDefs.Items[i] as TMemFieldDef;
      //Don't need to check against any other metadata at present time.
      if i < CCFieldCount then //Not an added field.
      begin
        if NotAssignedOrSentinel(CC.FieldDefs.Items[i]) then
          raise EMemDBInternalException.Create(S_PRE_COMMIT_CHECK_INTERNAL);
        FieldDef1 := CC.FieldDefs.Items[i] as TMemFieldDef;
        if FieldDef1.FieldType <> FieldDef2.FieldType then
          raise EMemDbInternalException.Create(S_FIELD_CHANGED_TYPE);
        // if FieldDef1.FieldName <> FieldDef2.FieldName then
        // Fields can be renamed without any extra lower
        // level processing being required.
        if FieldDef1.FieldIndex <> FieldDef2.FieldIndex then
          FFieldChangesets[i] := FFieldChangesets[i] + [fctChangedFieldNumber];
      end
      else
        FFieldChangesets[i] := [fctAdded];
    end
    else
      FFieldChangesets[i] := [fctDeleted];
  end;

  //Second off, indexes.
  for i := 0 to Pred(NCIndexCount) do
  begin
    Assert(Assigned(NC.IndexDefs.Items[i]));
    if not (NC.IndexDefs.Items[i] is TMemDeleteSentinel) then //Not a deleted index.
    begin
      IndexDef1:= NC.IndexDefs.Items[i] as TMemIndexDef;
      if i < CCIndexCount then //Not an added index.
      begin
        if NotAssignedOrSentinel(CC.IndexDefs.Items[i]) then
          raise EMemDBInternalException.Create(S_PRE_COMMIT_CHECK_INTERNAL);
        IndexDef2 := CC.IndexDefs.Items[i] as TMemIndexDef;
        // if IndexDef1.IndexName <> IndexDef2.IndexName then
        // Indexes are allowed to change name without any extra
        // lower level processing being required.
        if IndexDef1.IndexAttrs <> IndexDef2.IndexAttrs then
          raise EMemDbInternalException.Create(S_INDEX_CHANGED_ATTRS);

        //Check indexes reference same underlying field:
        //Same abs position in array for field index,
        //and also, field number change is same for field, and index.
        FieldDefs1 := (FMetadata as TMemDBTableMetadata).FieldsByNames(abNext, IndexDef1.FieldArray, AbsIndexes1);
        FieldDefs2 := (FMetadata as TMemDBTableMetadata).FieldsByNames(abCurrent, IndexDef2.FieldArray, AbsIndexes2);

        Assert(Length(FieldDefs1) > 0); // i < NCIndexCount
        Assert(Length(FieldDefs2) > 0); // i < CCIndexCount
        Assert(Length(FieldDefs1) = Length(AbsIndexes1));
        Assert(Length(FieldDefs2) = Length(AbsIndexes2));

        if Length(FieldDefs1) <> Length(FieldDefs2) then
          raise EMemDbConsistencyException.Create(S_INDEX_FIELD_COUNT_SHOULD_BE_CONSTANT);
        for ff := 0 to Pred(Length(FieldDefs1)) do
        begin
          //Check fields looked up by name are good.
          if not (Assigned(FieldDefs1[ff]) and Assigned(FieldDefs2[ff])) then
            raise EMemDBConsistencyException.Create(S_INDEX_DOES_NOT_REFERENCE_FIELD);
          //Check same underlying fields.
          if AbsIndexes1[ff] <> AbsIndexes2[ff] then
            raise EMemDbConsistencyException.Create(S_INDEX_UNDERLYING_FIELD_CHANGED);
          //Check field indexes for index are ok.
          if FieldDefs1[ff].FieldType <> FieldDefs2[ff].FieldType then
            raise EMemDBConsistencyException.Create(S_INDEXED_FIELD_CHANGED_TYPE);

          //Check indexed fields in correct locations,
          //(should have been verified by field number checking above).
          Assert(FieldDefs2[ff].FieldIndex = AbsIndexes2[ff]);
          Assert(FFieldHelper.NdIndexToRawIndex(FieldDefs1[ff].FieldIndex) = AbsIndexes1[ff]);

          if FieldDefs1[ff].FieldIndex <> FieldDefs2[ff].FieldIndex then
            FIndexChangesets[i] := FIndexChangesets[i] + [ictChangedFieldNumber];
        end;
      end
      else
      begin
        FieldDefs1 := (FMetadata as TMemDBTableMetadata).FieldsByNames(abNext, IndexDef1.FieldArray, AbsIndexes1);
        Assert(Length(FieldDefs1) > 0); // i < NCIndexCount
        Assert(Length(FieldDefs1) = Length(AbsIndexes1));
        for ff := 0 to Pred(Length(FieldDefs1)) do
        begin
          if not Assigned(FieldDefs1[ff]) then
            raise EMemDBConsistencyException.Create(S_INDEX_DOES_NOT_REFERENCE_FIELD);
          if not (FieldDefs1[ff].FieldType in IndexableFieldTypes) then
            raise EMemDBConsistencyException.Create(S_INDEX_BAD_FIELD_TYPE);
        end;
        FIndexChangesets[i] := [ictAdded];
      end;
    end
    else
      FIndexChangesets[i] := [ictDeleted];
  end;

  FIndexingChangeRequired := false;
  FDataChangeRequired := false;
  for i := 0 to Pred(Length(FIndexChangesets)) do
    if (FIndexChangesets[i] * [ictAdded, ictDeleted, ictChangedFieldNumber])
      <> [] then
      FIndexingChangeRequired := true;

  for i := 0 to Pred(Length(FFieldChangesets)) do
    if (FFieldChangesets[i] * [fctAdded, fctDeleted, fctChangedFieldNumber])
      <> [] then
      FDataChangeRequired := true;
end;


procedure TMemDBTablePersistent.DeleteUnusedIndices(Reason: TMemDBTransReason);
var
  i: integer;
  TagData: TMemDBITagData;
begin
  if FIndexingChangeRequired then
  begin
    for i := 0 to Pred(Length(FIndexChangesets)) do
    begin
      if ictDeleted in FIndexChangesets[i] then
      begin
        TagData := FTagDataList[i];
        CheckTagAgreesWithMetadata(i, TagData, tciPermanentAgreesCurrent, tcpProgrammed);
        TagData.CommitRmIdxsFromStore;
        FCommitChangesMade := FCommitChangesMade + [ccmDeleteUnusedIndices];
        CheckTagAgreesWithMetadata(i, TagData, tciPermanentAgreesCurrent, tcpNotProgrammed);
      end;
    end;
  end;
end;

//Just create new tags.Don't need to set new fields or anything yet.
procedure TMemDBTablePersistent.AddNewTagStructs;
var
 CC,NC:TMemTableMetadataItem;
 CCFieldCount, CCIndexCount, NCFieldCount, NCINdexCount, i:integer;
 T: TObject;
begin
  if FIndexingChangeRequired then
  begin
    GetCurrNxtMetaCopiesEx(CC,NC, CCFieldCount, CCIndexCount, NCFieldCount, NCIndexCount);
    //These counts include items which are delete sentinels.
    Assert(NCIndexCount >= CCIndexCount);
    if FTagDataList.Count <> CCIndexCount then
      raise EMemDBInternalException.Create(S_NEW_TAGS_UNEXPECTED_COUNT);

    for i := 0 to Pred(CCIndexCount) do
    begin
      T := FTagDataList[i];
      if not (Assigned(T) and (T is TMemDBITagData)) then
        raise EMemDBInternalException.Create(S_NEW_TAGS_UNEXPECTED_OBJECT);
    end;
    if NCIndexCount > CCIndexCount then
    begin
      FTagDataList.Count := NCIndexCount; //Zero pads.
      FCommitChangesMade := FCommitChangesMade + [ccmAddedNewTagStructs];
      for i := CCIndexCount to Pred(NCIndexCount) do
      begin
        Assert(FIndexChangesets[i] = [ictAdded]);
        FTagDataList[i] := TMemDBITagData.Create;
      end;
      //No need to init fields here.
    end;
  end;
end;

procedure TMemDBTablePersistent.RemoveNewlyAddedTagStructs;
var
 CC,NC:TMemTableMetadataItem;
 CCFieldCount, CCIndexCount, NCFieldCount, NCINdexCount, i:integer;
 T: TObject;
begin
  GetCurrNxtMetaCopiesEx(CC,NC, CCFieldCount, CCIndexCount, NCFieldCount, NCIndexCount);
  Assert(NCIndexCount >= CCIndexCount);
  Assert(FTagDataList.Count = NCIndexCount);
  if NCIndexCount > CCIndexCount then
  begin
    for i := CCIndexCount to Pred(NCIndexCount) do
    begin
      Assert(FIndexChangesets[i] = [ictAdded]);
      T := FTagDataList[i];
      T.Free;
    end;
    FTagDataList.Count := CCIndexCount;
  end;
end;

procedure TMemDbTablePersistent.RemoveOldTagStructs(DeleteAll: boolean);
var
  i: Integer;
  TagStruct: TMemDBITagData;
  CC,NC:TMemTableMetadataItem;
  CCFieldCount, CCIndexCount, NCFieldCount, NCINdexCount:integer;

begin
  //If deleted, then very next commit which happens next will make it
  //null, table will be blown away.
  if FMetadata.Deleted or FMetadata.Null then
    DeleteAll := true;
  if DeleteAll then
  begin
    for i := 0 to Pred(FTagDataList.Count) do
    begin
      //Minimal fuss cleardown when next copy of metadata
      //blown away, as opposed to individual index rearrangement.
      TagStruct := FTagDataList[i];
      if TagStruct.IdxsSetToStore then
        TagStruct.RollbackRmIdxsFromStore;
      TagStruct.Free;
    end;
    FTagDataList.Count := 0;
  end
  else
  begin
    if FMetadata.Added or FMetadata.Changed then
    begin
      //List helpers are in fact valid at this point.
      //Was torn on whether to use them but we won't
      GetCurrNxtMetaCopiesEx(CC,NC, CCFieldCount, CCIndexCount, NCFieldCount, NCIndexCount);
      Assert(NCIndexCount >= CCIndexCount);
      for i := 0 to Pred(CCIndexCount) do
      begin
        if NC.IndexDefs.Items[i] is TMemDeleteSentinel then
        begin
          Assert(FIndexChangesets[i] = [ictDeleted]);
          TagStruct := FTagDataList[i];
          CheckTagAgreesWithMetadata(i, TagStruct, tciPermanentAgreesCurrent, tcpNotProgrammed);
          TagStruct.Free;
          FTagDataList[i] := nil;
        end;
      end;
      FTagDataList.Pack;
    end;
  end;
end;


procedure TMemDBTablePersistent.ReinstateDeletedIndices;
var
  i: integer;
  TagData: TMemDBITagData;
begin
  for i := 0 to Pred(Length(FIndexChangesets)) do
  begin
    if ictDeleted in FIndexChangesets[i] then
    begin
      TagData := FTagDataList[i];
      CheckTagAgreesWithMetadata(i, TagData, tciPermanentAgreesCurrent, tcpNotProgrammed);
      TagData.RollbackRestoreIdxsToStore(FData.Store);
      CheckTagAgreesWithMetadata(i, TagData, tciPermanentAgreesCurrent, tcpProgrammed);
    end;
  end;
end;


procedure TMemDBTablePersistent.ValidateIndexes(Reason: TMemDBTransReason);
var
  CC, NC: TMemTableMetadataItem;
  i: integer;
  IndexDef1: TMemIndexDef;

  function IndexNeedsValidate(idx:integer; Def: TMemIndexDef): boolean;
  begin
    result := false;
    if (FIndexChangesets[idx] * [ictAdded
{$IFOPT C+}
      ,ictChangedFieldNumber
      //Other index checking stringent enough do not need to revalidate,
      //if only a change in field number.
{$ENDIF}
      ]) <> [] then
    begin
      //Handle index validation.
      if Def.IndexAttrs * [iaUnique, iaNotEmpty] <> [] then
        result := true;
    end;
  end;

begin
  Assert(FIndexingChangeRequired);
  GetCurrNxtMetaCopies(CC,NC);

  for i := 0 to Pred(Length(FIndexChangesets)) do
  begin
    if ictDeleted in FIndexChangesets[i] then
      continue;
    IndexDef1 := NC.IndexDefs.Items[i] as TMemIndexDef;
    if IndexNeedsValidate(i, IndexDef1) then
    begin
      RevalidateEntireUserIndex(i);
    end;
  end;
end;

procedure TMemDbTablePersistent.CommitAdjustIndicesToTemporary(Reason: TMemDBTransReason);
var
  CC, NC: TMemTableMetadataItem;
  i: integer;
  IndexDef1: TMemIndexDef;
  FieldDefs1: TMemFieldDefs;
  FieldAbsIdxes: TFieldOffsets;
  NewOffsets: TFieldOffsets;
  TagData: TMemDbITagData;
  ff: integer;
begin
  if FIndexingChangeRequired then
  begin
    GetCurrNxtMetaCopies(CC,NC);
    FTemporaryIndexLimit := 0;

    for i := 0 to Pred(Length(FIndexChangesets)) do
    begin
      if ictDeleted in FIndexChangesets[i] then
      begin
        FTemporaryIndexLimit := Succ(i);
        continue; //Index already blown away.
      end;
      TagData := FTagDataList[i];
      //Revalidate indexes unique after changing field numbers.
      IndexDef1 := NC.IndexDefs.Items[i] as TMemIndexDef;
      if NotAssignedOrSentinel(IndexDef1) then
        raise EMemDBInternalException.Create(S_ADJUST_INDICES_NO_INDEX);
      FieldDefs1 := FieldsByNamesInt(NC, IndexDef1.FieldArray, FieldAbsIdxes);
      if Length(FieldDefs1) = 0 then
        raise EMemDBInternalException.Create(S_ADJUST_INDICES_NO_FIELD);
      for ff := 0 to Pred(Length(FieldDefs1)) do
        if NotAssignedOrSentinel(FieldDefs1[ff]) then
          raise EMemDBInternalException.Create(S_ADJUST_INDICES_NO_FIELD);

      FCommitChangesMade := FCommitChangesMade + [ccmIndexesToTemporary];
      SetLength(NewOffsets, Length(FieldDefs1));

      if ictChangedFieldNumber  in FIndexChangesets[i] then
      begin
        Assert(not (ictAdded in FIndexChangesets[i]));
        CheckTagAgreesWithMetadata(i, TagData, tciPermanentAgreesCurrent, tcpProgrammed);
        for ff := 0 to Pred(Length(FieldDefs1)) do
          NewOffsets[ff] := FieldDefs1[ff].FieldIndex;
        TagData.MakePermanentTemporary(NewOffsets);
        CheckTagAgreesWithMetadata(i, TagData, tciTemporaryAgreesBoth, tcpProgrammed);
      end
      else if ictAdded in FIndexChangesets[i] then
      begin
        //Newly created tag needs to be made temporary, and we create the
        //"previous" field index to take into account possible field rearrangements.
        for ff := 0 to Pred(Length(FieldDefs1)) do
        begin
          Assert(FieldDefs1[ff].FieldIndex <= FFieldHelper.NdIndexToRawIndex(FieldDefs1[ff].FieldIndex));
          NewOffsets[ff] := FFieldHelper.NdIndexToRawIndex(FieldDefs1[ff].FieldIndex);
        end;
        TagData.InitPermanent(NewOffsets);
        for ff := 0 to Pred(Length(FieldDefs1)) do
          NewOffsets[ff] := FieldDefs1[ff].FieldIndex;
        TagData.MakePermanentTemporary(NewOffsets);
        CheckTagAgreesWithMetadata(i, TagData, tciTemporaryAgreesNextOnly, tcpNotProgrammed);
        TagData.CommitAddIdxsToStore(FData.Store);
        CheckTagAgreesWithMetadata(i, TagData, tciTemporaryAgreesNextOnly, tcpProgrammed);
      end;
      FTemporaryIndexLimit := Succ(i);
    end;

    ValidateIndexes(Reason);
  end;
end;

procedure TMemDbTablePersistent.CommitRestoreIndicesToPermanent(Reason: TMemDbTransReason);
var
  i: integer;
  Limit: integer;
  TagData: TMemDBITagData;
begin
  //Adjust temporary indices back to permanent ones.
  //Temporary index tags are such that commit removal and addition should have
  //gone OK.
  Limit := FTemporaryIndexLimit;
  FTemporaryIndexLimit := 0;
  for i := 0 to Pred(Limit) do
  begin
    if ictDeleted in FIndexChangesets[i] then
      continue; //Index already blown away.

    TagData := FTagDataList[i];
    if ictChangedFieldNumber  in FIndexChangesets[i] then
    begin
      Assert(not (ictAdded in FIndexChangesets[i]));
      CheckTagAgreesWithMetadata(i, TagData, tciTemporaryAgreesBoth, tcpProgrammed);
      TagData.CommitRestoreToPermanent;
      CheckTagAgreesWithMetadata(i, TagData, tciPermanentAgreesNext, tcpProgrammed);
    end
    else if ictAdded in FIndexChangesets[i] then
    begin
      CheckTagAgreesWithMetadata(i, TagData, tciTemporaryAgreesNextOnly, tcpProgrammed);
      TagData.CommitRestoreToPermanent;
      CheckTagAgreesWithMetadata(i, TagData, tciPermanentAgreesNext, tcpProgrammed);
    end;
  end;
end;

procedure TMemDBTablePersistent.RollbackRestoreIndicesToPermanent;
var
  i: integer;
  Limit: integer;
  TagData: TMemDBITagData;
begin
  //Adjust temporary indices back to permanent ones.
  //Temporary index tags are such that commit removal and addition should have
  //gone OK.
  if FIndexingChangeRequired then
  begin
    Limit := FTemporaryIndexLimit;
    FTemporaryIndexLimit := 0;
    for i := 0 to Pred(Limit) do
    begin
      if ictDeleted in FIndexChangesets[i] then
        continue; //Index already blown away.

      TagData := FTagDataList[i];
      if ictChangedFieldNumber  in FIndexChangesets[i] then
      begin
        Assert(not (ictAdded in FIndexChangesets[i]));
        CheckTagAgreesWithMetadata(i, TagData, tciTemporaryAgreesBoth, tcpProgrammed);
        TagData.RollbackRestoreToPermanent;
        CheckTagAgreesWithMetadata(i, TagData, tciPermanentAgreesCurrent, tcpProgrammed);
      end
      else if ictAdded in FIndexChangesets[i] then
      begin
        CheckTagAgreesWithMetadata(i, TagData, tciTemporaryAgreesNextOnly, tcpProgrammed);
        TagData.RollbackRestoreToPermanent;
        TagData.RollbackRmIdxsFromStore;
        //No tag check here, because no "current" to roll back to.
      end;
    end;
  end;
end;

type
  TProcessRowChkIdxStruct = record
    FieldsForIndexDef: TMemFieldDefs;
    AbsFieldOffsets: TFieldOffsets;
    IndexAttrs: TMDBIndexAttrs;
    IdxIdx: integer;
    IdxAdded, IdxChangedFieldNumber: boolean;
  end;
  PProcessRowChkIdxStruct = ^TProcessRowChkIdxStruct;

procedure TMemDBTablePersistent.CheckStreamedInRowIndexing;
var
 CC, MRC: TMemTableMetadataItem;
 CCFieldsForIndexDef, MRCFieldsForIndexDef: TMemFieldDefs;
 CCIndexDef, MRCIndexDef: TMemIndexDef;
 IdxIdx: integer;
 FieldAbsIdxs: TFieldOffsets;
 RowStruct: TProcessRowChkIdxStruct;
 ff: integer;

begin
  MRC := FMetadata.ABData[abLatest] as TMemTableMetadataItem;
  if AssignedNotSentinel(FMetadata.ABData[abCurrent]) then
    CC := FMetadata.ABData[abCurrent] as TMemTableMetadataItem
  else
    CC := nil;
  for IdxIdx := 0 to Pred(MRC.IndexDefs.Count) do
  begin
    if AssignedNotSentinel(MRC.IndexDefs.Items[IdxIdx]) then
    begin
      MRCIndexDef := MRC.IndexDefs.Items[IdxIdx] as TMemIndexDef;

      if MRCIndexDef.IndexAttrs <> [] then
      begin
        RowStruct.IdxAdded := (Length(FIndexChangesets) > IdxIdx)
          and (ictAdded in FIndexChangesets[IdxIdx]);
        RowStruct.IdxChangedFieldNumber := (Length(FIndexChangesets) > IdxIdx)
          and (ictChangedFieldNumber in FIndexChangesets[IdxIdx]);

        Assert(not RowStruct.IdxChangedFieldNumber);
        //CheckStreamedInRowIndexing => !FDataChangeRequired
        // => No index or field changesets, so current index offsets fine.
        // => No delete sentinels or field rearrangement.

        RowStruct.IndexAttrs := MRCIndexDef.IndexAttrs;
        RowStruct.IdxIdx := IdxIdx;
        if RowStruct.IdxAdded then
        begin
          Assert(Assigned(MRCIndexDef));
          MRCFieldsForIndexDef := FieldsByNamesInt(MRC, MRCIndexDef.FieldArray, FieldAbsIdxs);
          if Length(MRCFieldsForIndexDef) = 0 then
            raise EMemDBInternalException.Create(S_INTERNAL_CHECKING_INDEXES);
          for ff := 0 to Pred(Length(MRCFieldsForIndexDef)) do
          begin
            if NotAssignedOrSentinel(MRCFieldsForIndexDef[ff]) then
              raise EMemDBInternalException.Create(S_INTERNAL_CHECKING_INDEXES);
            Assert(MRCFieldsForIndexDef[ff].FieldIndex = FieldAbsIdxs[ff]);
          end;
           //Not concerned about copy-by-reference here.
          RowStruct.FieldsForIndexDef := MRCFieldsForIndexDef;
          RowStruct.AbsFieldOffsets := FieldAbsIdxs;
        end
        else
        begin
          Assert(not RowStruct.IdxChangedFieldNumber);
          //CheckStreamedInRowIndexing => !FDataChangeRequired
          // => No index or field changesets, so current index offsets fine.
          // => No delete sentinels or field rearrangement.
          Assert(Assigned(CC));
          Assert(Assigned(CC.IndexDefs) and (IdxIdx < CC.IndexDefs.Count));
          CCIndexDef := CC.IndexDefs.Items[IdxIdx] as TMemIndexDef;
          Assert(Assigned(CCIndexDef));
          CCFieldsForIndexDef := FieldsByNamesInt(CC, CCIndexDef.FieldArray, FieldAbsIdxs);
          if Length(CCFieldsForIndexDef) = 0 then
            raise EMemDBInternalException.Create(S_INTERNAL_CHECKING_INDEXES);
          for ff := 0 to Pred(Length(CCFieldsForIndexDef)) do
          begin
            if NotAssignedOrSentinel(CCFieldsForIndexDef[ff]) then
              raise EMemDBInternalException.Create(S_INTERNAL_CHECKING_INDEXES);
            Assert(CCFieldsForIndexDef[ff].FieldIndex = FieldAbsIdxs[ff]);
          end;
           //Not concerned about copy-by-reference here.
           RowStruct.FieldsForIndexDef := CCFieldsForIndexDef;
           RowStruct.AbsFieldOffsets := FieldAbsIdxs;
        end;
        FData.ForEachChangedRow(CheckIndexForRow, @RowStruct, nil);
      end;
    end;
  end;
end;

procedure TMemDbTablePersistent.CheckIndexForRow(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);

  function MostRecentFieldsFromRow(Row: TMemDBRow): TMemStreamableList;
  begin
    result := nil;
    if Assigned(Row) then
    begin
      if AssignedNotSentinel(Row.ABData[abLatest]) then
        result := Row.ABData[abLatest] as TMemStreamableList;
    end;
  end;

var
  PProcRowStruct: PProcessRowChkIdxStruct;
  RowFields: TMemStreamableList;
  ZeroRec: TMemDBFieldDataRec;
  TagData: TMemDBITagData;
  TagStruct: PITagStruct;
  PrevRec, NextRec: TItemRec;
  RV: TIsRetVal;
  PrevRow, NextRow: TMemDBRow;
  PrevRowFields, NextRowFields: TMemStreamableList;

begin
  PProcRowStruct := PProcessRowChkIdxStruct(Ref1);
  if not (Row.ABData[abNext] is TMemDeleteSentinel) then //Not deleted row.
  begin
    FillChar(ZeroRec, sizeof(ZeroRec), 0);

    RowFields := Row.ABData[abNext] as TMemStreamableList;

    if iaNotEmpty in PProcRowStruct.IndexAttrs then
    begin
      //Very arguable here, but we'll just require that not all fields are empty,
      //Kinda like digits of an int not being zero ....
      if AllFieldsZero(RowFields, PProcRowStruct.AbsFieldOffsets) then
        raise EMemDBConsistencyException.Create(S_INDEX_CONSTRAINT_ZERO);
    end;
    if iaUnique in PProcRowStruct.IndexAttrs then
    begin
      //Work out what the index tag will be.

      //We can not get here if not FDataChangeRequired = true
      // => No fields moved.
      // => never IdxChangedFieldNumber.

      //However, we can definitely add new records, and then add an index.
      //Index change required -> True. Data change required -> false.

      //Asserted and old code commented out. Can be removed in a bit.
      TagData := FTagDataList[PProcRowStruct.IdxIdx];
      if PProcRowStruct.IdxAdded then
      begin
        Assert(not PProcRowStruct.IdxChangedFieldNumber);
        CheckTagAgreesWithMetadata(PProcRowStruct.IdxIdx, TagData, tciTemporaryAgreesNextOnly, tcpProgrammed);
      end
      else if PProcRowStruct.IdxChangedFieldNumber then
      begin
        raise EMemDBInternalException.Create(S_INDEX_CONSTRAINTS_FIELDMOVE);
      end
      else
        CheckTagAgreesWithMetadata(PProcRowStruct.IdxIdx, TagData, tciPermanentAgreesCurrent, tcpProgrammed);

      TagStruct := TagData.TagStructs[sicLatest];
      if not FData.Store.HasIndex(TagStruct) then
        raise EMemDBInternalException.Create(S_INDEX_TAG_NOT_FOUND);
      Assert(Assigned(Row.FStoreBackRec));
      PrevRec := Row.FStoreBackRec;
      NextRec := Row.FStoreBackRec;
      RV := TblList.Store.NextByIndex(TagStruct, NextRec);
      Assert(RV in [rvOK, rvNotFound]);
      RV := TblList.Store.PreviousByIndex(TagStruct, PrevRec);
      Assert(RV in [rvOK, rvNotFound]);
      if Assigned(PrevRec) then
        PrevRow := PrevRec.Item as TMemDbRow
      else
        PrevRow := nil;
      if Assigned(NextRec) then
        NextRow := NextRec.Item as TMemDBRow
      else
        NextRow := nil;
      //Index metadata copies used are
      //CC (Current Copies), but we are using the most recent fields.
      //OK if data changed, but no field number change.
      //Checked for above.
      PrevRowFields := MostRecentFieldsFromRow(PrevRow);
      NextRowFields := MostRecentFieldsFromRow(NextRow);
      //One or other field lists might be NULL.
      if Assigned(PrevRowFields) then
      begin
        if SameLayoutFieldsSame(PrevRowFields, RowFields, PProcRowStruct.AbsFieldOffsets) then
          raise EMemDBConsistencyException.Create(S_INDEX_CONSTRAINT_UNIQUE);
      end
      else
      begin
{$IFDEF DEBUG_INDEXING_CHECK}
        if Assigned(PrevRow) then
        begin
          //If previous / next row fields not assigned, then beginning
          //or end of table, just check we can go all the way to end of table,
          //and no rows have fields of that a/b type assigned.
          RV := Store.PreviousByIndex(TagStruct, PrevRec);
          Assert(RV in [rvOK, rvNotFound]);
          while Assigned(PrevRec) do
          begin
            PrevRow := PrevRec.Item as TMemDbRow;
            PrevRowFields := MostRecentFieldsFromRow(PrevRow);
            Assert(not Assigned(PrevRowFields));
            RV := Store.PreviousByIndex(TagStruct, PrevRec);
            Assert(RV in [rvOK, rvNotFound]);
          end;
        end;
  {$ENDIF}
      end;
      if Assigned(NextRowFields) then
      begin
        if SameLayoutFieldsSame(NextRowFields, RowFields, PProcRowStruct.AbsFieldOffsets) then
          raise EMemDBConsistencyException.Create(S_INDEX_CONSTRAINT_UNIQUE);
      end
      else
      begin
{$IFDEF DEBUG_INDEXING_CHECK}
        if Assigned(NextRow) then
        begin
          //If previous / next row fields not assigned, then beginning
          //or end of table, just check we can go all the way to end of table,
          //and no rows have fields of that a/b type assigned.
          RV := Store.NextByIndex(TagStruct, NextRec);
          Assert(RV in [rvOK, rvNotFound]);
          while Assigned(NextRec) do
          begin
            NextRow := NextRec.Item as TMemDBRow;
            NextRowFields := MostRecentFieldsFromRow(NextRow);
            Assert(not Assigned(NextRowFields));
            RV := Store.NextByIndex(TagStruct, NextRec);
            Assert(RV in [rvOK, rvNotFound]);
          end;
        end;
{$ENDIF}
      end;
    end;
  end;
end;

procedure TMemDbTablePersistent.RevalidateEntireUserIndex(
  RawIndexDefNumber: integer);
var
  IndexDef1, IndexDef2: TMemIndexDef;
  FieldDefs1, FieldDefs2: TMemFieldDefs;
  AbsIdxs1, AbsIdxs2: TFieldOffsets;
  Current, Next: TItemRec;
  CurRow, NextRow: TMemDBRow;
  IRet: TIsRetVal;
  ZeroRec: TMemDbFieldDataRec;
  CurRowFields, NextRowFields: TMemStreamableList;
  FieldNumbers: TFieldOffsets;
  CurrentMetadata, NextMetadata: TMemTableMetadataItem;
  CCFieldCount, CCIndexCount, NCFieldCount, NCIndexCount: integer;
  TagData: TMemDbITagData;
  ITagStruct: PITagStruct;
  ff: integer;

begin
  GetCurrNxtMetaCopiesEx(CurrentMetadata, NextMetadata,
    CCFieldCount, CCIndexCount, NCFieldCount, NCIndexCount);
  FillChar(ZeroRec, sizeof(ZeroRec), 0);
  Assert(NCIndexCount > RawIndexDefNumber);
  IndexDef1 := NextMetadata.IndexDefs.Items[RawIndexDefNumber] as TMemIndexDef;
  FieldDefs1 := FieldsByNamesInt(NextMetadata, IndexDef1.FieldArray, AbsIdxs1);
  if RawIndexDefNumber < CCIndexCount then
  begin
    IndexDef2 := CurrentMetadata.IndexDefs.Items[RawIndexDefNumber] as TMemIndexDef;
    FieldDefs2 := FieldsByNamesInt(CurrentMetadata, IndexDef2.FieldArray, AbsIdxs2);
  end
  else
  begin
    IndexDef2 := nil;
    SetLength(FieldDefs2, 0);
    SetLength(AbsIdxs2, 0);
  end;
  Assert(Assigned(IndexDef2) = not (ictAdded in FIndexChangesets[RawIndexDefNumber]));
  Assert(Assigned(IndexDef1) = (Length(FieldDefs1) > 0));
  Assert(Assigned(IndexDef2) = (Length(FieldDefs2) > 0));
  //Index tags currently in temporary state.
  TagData := FTagDataList[RawIndexDefNumber];
  if ictAdded in FIndexChangesets[RawIndexDefNumber] then
  begin
    Assert(not (ictChangedFieldNumber in FIndexChangesets[RawIndexDefNumber]));
    CheckTagAgreesWithMetadata(RawIndexDefNumber, TagData, tciTemporaryAgreesNextOnly, tcpProgrammed);
    SetLength(FieldNumbers, Length(FieldDefs1));
    //TODO - Use AbsIdxs1?
    for ff := 0 to Pred(Length(FieldNumbers)) do
    begin
      Assert(FFieldHelper.NdIndexToRawIndex(FieldDefs1[ff].FieldIndex) = AbsIdxs1[ff]);
      FieldNumbers[ff] := AbsIdxs1[ff];
    end;

    ITagStruct := TagData.TagStructs[sicLatest];
  end
  else if ictChangedFieldNumber in FIndexChangesets[RawIndexDefNumber] then
  begin
    CheckTagAgreesWithMetadata(RawIndexDefNumber, TagData, tciTemporaryAgreesBoth, tcpProgrammed);
    //If changes field number, we still have delete sentinels, the original
    //field index (which is the ND IndexDef1.FieldIndex) is just fine.
    SetLength(FieldNumbers, Length(FieldDefs2));
    Assert(Length(FieldDefs2) = Length(FieldDefs1));
    for ff := 0 to Pred(Length(FieldNumbers)) do
    begin
      FieldNumbers[ff] := FieldDefs2[ff].FieldIndex;
      Assert(FieldDefs2[ff].FieldIndex = FFieldHelper.NdIndexToRawIndex(FieldDefs1[ff].FieldIndex));
    end;
    ITagStruct := TagData.TagStructs[sicLatest];
  end
  else
  begin
    CheckTagAgreesWithMetadata(RawIndexDefNumber, TagData, tciPermanentAgreesCurrent, tcpProgrammed);
    SetLength(FieldNumbers, Length(FieldDefs1));
    for ff := 0 to Pred(Length(FieldNumbers)) do
      FieldNumbers[ff] := FieldDefs1[ff].FieldIndex;
    ITagStruct := TagData.TagStructs[sicLatest];
  end;
  if not FData.Store.HasIndex(ITagStruct) then
    raise EMemDBInternalException.Create(S_INDEX_TAG_NOT_FOUND);

  //NB. There's no guarantee that rows have valid latest data,
  //those just appear at the start or end of the series.
  IRet := FData.Store.FirstByIndex(ITagStruct, Current);
  Assert(IRet in [rvOK, rvNotFound]);
  if Assigned(Current) then
    CurRow := Current.Item as TMemDbRow
  else
    CurRow := nil;
  while Assigned(CurRow) and ((CurRow.Deleted) or (CurRow.Null)) do
  begin
    Assert(not CurRow.Null); //Do not expect totally NULL rows.
    Assert(NotAssignedOrSentinel(CurRow.ABData[abLatest]));

    IRet := FData.Store.NextByIndex(ITagStruct, Current);
    Assert(IRet in [rvOK, rvNotFound]);
    if Assigned(Current) then
      CurRow := Current.Item as TMemDbRow
    else
      CurRow := nil;
  end;

  while Assigned(CurRow) do
  begin
    Assert(not (CurRow.Deleted or CurRow.Null));
    CurRowFields := CurRow.ABData[abLatest] as TMemStreamableList;
    if iaNotEmpty in IndexDef1.IndexAttrs then
    begin
      if AllFieldsZero(CurRowFields, FieldNumbers) then
        raise EMemDBConsistencyException.Create(S_INDEX_CONSTRAINT_ZERO);
    end;

    Next := Current;
    IRet := FData.Store.NextByIndex(ITagStruct, Next);
    Assert(IRet in [rvOK, rvNotFound]);
    if Assigned(Next) then
      NextRow := Next.Item as TMemDbRow
    else
      NextRow := nil;
    while Assigned(NextRow) and ((NextRow.Deleted) or (NextRow.Null)) do
    begin
      Assert(NotAssignedOrSentinel(NextRow.ABData[abLatest]));
      IRet := FData.Store.NextByIndex(ITagStruct, Next);
      Assert(IRet in [rvOK, rvNotFound]);
      if Assigned(Next) then
        NextRow := Next.Item as TMemDbRow
      else
        NextRow := nil;
    end;

    if Assigned(NextRow) and (iaUnique in IndexDef1.IndexAttrs) then
    begin
      Assert(not (NextRow.Deleted or NextRow.Null));
      NextRowFields := NextRow.ABData[abLatest] as TMemStreamableList;
      if SameLayoutFieldsSame(NextRowFields, CurRowFields, FieldNumbers) then
        raise EMemDBConsistencyException.Create(S_INDEX_CONSTRAINT_UNIQUE);
    end;
    Current := Next;
    CurRow := NextRow;
  end;
end;


{$ENDIF}

{ TMemDBForeignKeyPersistent }

{$IFDEF MEMDB2_TEMP_REMOVE}

constructor TMemDBForeignKeyPersistent.Create;
begin
  inherited;
  FMetadata := TMemDBForeignKeyMetadata.Create;
end;

destructor TMemDBForeignKeyPersistent.Destroy;
begin
  FMetadata.Free;
  inherited;
end;

procedure TMemDBForeignKeyPersistent.ToJournal(Stream: TStream);
begin
  if FMetadata.AnyChangesAtAll then
  begin
    WrTag(Stream, mstFkStart);
    FMetadata.ToJournal(Stream);
    WrTag(Stream, mstFkEnd);
  end;
end;

procedure TMemDBForeignKeyPersistent.ToScratch(Stream: TStream);
begin
  //TODO - Only if metadata indicates not NULL.
  WrTag(Stream, mstFkStart);
  FMetadata.ToScratch(Stream);
  WrTag(Stream, mstFkEnd);
end;

procedure TMemDBForeignKeyPersistent.FromJournal(Stream: TStream);
begin
  //Premise is that this is being called given appropriate previous lookahead
  //and construction.
  ExpectTag(Stream, mstFkStart);
  FMetadata.FromJournal(Stream);
  ExpectTag(Stream, mstFkEnd);
end;

procedure TMemDBForeignKeyPersistent.FromScratch(Stream: TStream);
begin
  //Premise is that this is being called given appropriate previous lookahead
  //and construction.
  if not FMetadata.Null then
    raise EMemDBInternalException.Create(S_FROM_SCRATCH_REQUIRES_EMPTY_OBJ);
  ExpectTag(Stream, mstFkStart);
  FMetadata.FromScratch(Stream);
  ExpectTag(Stream, mstFkEnd);
end;

procedure TMemDBForeignKeyPersistent.PreCommit(Reason: TMemDBTransReason);
var
  EntityReferring, EntityReferred: TMemDBEntity;
  CCEntityReferring, CCEntityReferred: TMemDbEntity;
  FKM, FKMCC: TMemForeignKeyMetadataItem;
  M: TMemDBTableMetadata;
  Meta: TMemDbFKMeta;
  CCMeta: TMemDBFKMeta;
  ff: integer;

begin
  inherited;
  if FMetadata.Deleted or FMetadata.Null then
    exit;
  FillChar(Meta, sizeof(Meta), 0);
  //Very basic metadata checking.
  (Metadata as TMemDBForeignKeyMetadata).ConsistencyCheck;
  FKM := (Metadata as TMemDBForeignKeyMetadata).ABData[abLatest]
    as TMemForeignKeyMetadataItem;
  //Referring index.
  EntityReferring := ParentDB.EntitiesByName(abLatest, FKM.TableReferer, Meta.TableReferringIdx);
  if not (Assigned(EntityReferring) and (EntityReferring is TMemDBTablePersistent)) then
    raise EMemDBException.Create(S_FK_TABLE_NOT_FOUND);
  Meta.TableReferring := EntityReferring as TMemDBTablePersistent;
  M := Meta.TableReferring.Metadata as TMemDBTableMetadata;
  Meta.IndexDefReferring := M.IndexByName(abLatest, FKM.IndexReferer, Meta.IndexDefReferringAbsIdx);
  if not Assigned(Meta.IndexDefReferring) then
    raise EMemDBException.Create(S_FK_INDEX_NOT_FOUND);
  //..and fields (names should already have been checked, tables before fkeys).
  Meta.FieldDefsReferring := M.FieldsByNames(abLatest, Meta.IndexDefReferring.FieldArray, Meta.FieldDefsReferringAbsIdx);
  if Length(Meta.FieldDefsReferring) = 0 then
    raise EMemDBInternalException.Create(S_FK_INDEX_FIELD_INTERNAL);
  Assert(Length(Meta.FieldDefsReferring) = Length(Meta.FieldDefsReferringAbsIdx));

  //Referred index.
  EntityReferred := ParentDB.EntitiesByName(abLatest, FKM.TableReferred, Meta.TableReferredIdx);
  if not (Assigned(EntityReferred) and (EntityReferred is TMemDBTablePersistent)) then
    raise EMemDBException.Create(S_FK_TABLE_NOT_FOUND);
  Meta.TableReferred := EntityReferred as TMemDBTablePersistent;
  M := Meta.TableReferred.Metadata as TMemDBTableMetadata;
  Meta.IndexDefReferred := M.IndexByName(abLatest, FKM.IndexReferred, Meta.IndexDefReferredAbsIdx);
  if not Assigned(Meta.IndexDefReferred) then
    raise EMemDBException.Create(S_FK_INDEX_NOT_FOUND);
  //...And fields (names should already have been checked, tables before fkeys).
  Meta.FieldDefsReferred := M.FieldsByNames(abLatest, Meta.IndexDefReferred.FieldArray, Meta.FieldDefsReferredAbsIdx);
  if Length(Meta.FieldDefsReferred) = 0 then
    raise EMemDBInternalException.Create(S_FK_INDEX_FIELD_INTERNAL);
  Assert(Length(Meta.FieldDefsReferred) = Length(Meta.FieldDefsReferredAbsIdx));

  //Index uniqueness
  if not (iaUnique in Meta.IndexDefReferred.IndexAttrs) then
    raise EMemDBException.Create(S_INDEX_FOR_FK_UNIQUE_ATTR);

  //Indexes have same field count and type / order.
  if Length(Meta.FieldDefsReferring) <> Length(Meta.FieldDefsReferred) then
    raise EMemDbException.Create(S_FK_INDEXES_DIFF_FIELDCOUNT);

  for ff := 0 to Pred(Length(Meta.FieldDefsReferring)) do
  begin
    //Field type cross check.
    if Meta.FieldDefsReferring[ff].FieldType <> Meta.FieldDefsReferred[ff].FieldType then
      raise EMemDBException.Create(S_FK_FIELDS_DIFFERENT_TYPES);
  end;

  if not FMetadata.Added then
  begin
    //Whether changed or not, should be able to get hold of CCopy of index
    //and field defs, and check that they are the "same" (AbsIdx)
    //as the latest ones. Explicit check tables, indexes and fields have not
    //been moved around (too much) under our feet.

    //This a slightly more obvious and sensible check of potential multiple
    //index, field, table renames (which may have been renamed more than once).
    //These conditions were implicit, but not explicitly checked for previously.

    FillChar(CCMeta, sizeof(CCMeta), 0);
    FKMCC := (Metadata as TMemDBForeignKeyMetadata).ABData[abCurrent]
      as TMemForeignKeyMetadataItem;
    Assert(AssignedNotSentinel(FKMCC));
    //If we have a current copy (not added), then it must have referred to
    //existing table objects, and those existing table objects must have
    //been present in same copy as CC of our metadata.

    //Tables.
    CCEntityReferring := ParentDB.EntitiesByName(abCurrent, FKMCC.TableReferer, CCMeta.TableReferringIdx);
    CCEntityReferred := ParentDB.EntitiesByName(abCurrent, FKMCC.TableReferred, CCMeta.TableReferredIdx);

    //Tables actually have to be the same object in memory.
    if (EntityReferring <> CCEntityReferring)
      or (EntityReferred <> CCEntityReferred) then
      raise EMemDBInternalException.Create(S_FOREIGN_KEY_UNDERLYING_TABLE_CHANGED);
    CCMeta.TableReferring := CCEntityReferring as TMemDBTablePersistent;
    CCMeta.TableReferred := CCEntityReferred as TMemDbTablePersistent;

    //Indexes.
    M := CCMeta.TableReferring.Metadata as TMemDBTableMetadata;
    CCMeta.IndexDefReferring := M.IndexByName(abCurrent, FKMCC.IndexReferer, CCMeta.IndexDefReferringAbsIdx);
    M := CCMeta.TableReferred.Metadata as TMemDBTableMetadata;
    CCMeta.IndexDefReferred := M.IndexByName(abCurrent, FKMCC.IndexReferred, CCMeta.IndexDefReferredAbsIdx);
    //Index objs do not actually need to be the same object (copied on write),
    //and nor do they have to have the same name, or refer to the same fields,
    //but for them to be the "same" index, we expect the IndexIndex (which is AbsIndex, not NDIndex),
    //to be the same (hence why we have changeset arrays).
    if (CCMeta.IndexDefReferringAbsIdx <> Meta.IndexDefReferringAbsIdx)
      or (CCMeta.IndexDefReferredAbsIdx <> Meta.IndexDefReferredAbsIdx) then
      raise EMemDBInternalException.Create(S_FOREIGN_KEY_UNDERLYING_INDEX_CHANGED);

    //Fields.
    M := CCMeta.TableReferring.Metadata as TMemDBTableMetadata;
    CCMeta.FieldDefsReferring := M.FieldsByNames(abCurrent, CCMeta.IndexDefReferring.FieldArray, CCMeta.FieldDefsReferringAbsIdx);
    M := CCMeta.TableReferred.Metadata as TMemDBTableMetadata;
    CCMeta.FieldDefsReferred := M.FieldsByNames(abCurrent, CCMeta.IndexDefReferred.FieldArray, CCMeta.FieldDefsReferredAbsIdx);

    //Expect CC meta to be as consistent as NCMeta....
    if Length(CCMeta.FieldDefsReferring) <> Length(CCMeta.FieldDefsReferred) then
      raise EMemDBInternalException.Create(S_FOREIGN_KEY_UNDERLYING_FIELD_CHANGED);

    //And lengths between them also to be the same.
    if Length(CCMeta.FieldDefsReferring) <> Length(Meta.FieldDefsReferred) then
      raise EMemDBInternalException.Create(S_FOREIGN_KEY_UNDERLYING_FIELD_CHANGED);

    //And expect field defs to have same absIdx as before, even if field rearrangement.
    for ff := 0 to Pred(Length(CCMeta.FieldDefsReferring)) do
    begin
      //As with Index defs, field defs do not need to be the same object, but they need
      //to be (for each xaction changeset) at the same AbsIdx as previously,
      //hence changeset arrays.
      if (CCMeta.FieldDefsReferringAbsIdx[ff] <> Meta.FieldDefsReferringAbsIdx[ff])
        or (CCMeta.FieldDefsReferredAbsIdx[ff] <> Meta.FieldDefsReferredAbsIdx[ff]) then
        raise EMemDBInternalException.Create(S_FOREIGN_KEY_UNDERLYING_FIELD_CHANGED);
    end;
  end;

  //And now the grunt work of checking the relation.
  CreateCheckForeignKeyRowSets(Meta,Reason);
end;

type
  TRowProcessingAction = (rpaReferringAddedFieldsToList,
                          rpaReferredDeletedFieldsToList,
                          rpaReferredAddedFieldsToList);

  TProcessRowFKStruct = record
    OutList: TIndexedStoreO;
    PMeta: PMemDBFKMeta;
    TransReason: TMemDBTransReason;
    Action: TRowProcessingAction;
  end;
  PProcessRowFKStruct = ^TProcessRowFKStruct;

procedure TMemDBForeignKeyPersistent.ProcessRow(Row: TMemDBRow;
                     Ref1, Ref2: TObject; TblList: TMemDbIndexedList);
var
  PStruct: PProcessRowFKStruct;
  CurrentRowFields, NextRowFields: TMemStreamableList;

  RV: TIsRetVal;
  IRecAdd: TItemRec;
  AddToList: boolean;
begin
  PStruct := PProcessRowFKStruct(Ref1);
  case PStruct.Action of
    rpaReferringAddedFieldsToList:
    begin
      if not (Row.Deleted or Row.Null) then
      begin
        if Row.Changed then
        begin
          //Might be changing field layout if simultaneous
          //layout change and FK add.
          AddToList := FMetadata.Added;

          if not AddToList then
          begin
            //Latest field (delete sentinels still in place).
            //Only perform calc if we need to.
            NextRowFields := Row.ABData[abNext] as TMemStreamableList;
            //Not adding FK, check current field. Same field offsets
            //Delete sentinels not removed yet.
            CurrentRowFields := Row.ABData[abCurrent] as TMemStreamableList;

            AddToList := not SameLayoutFieldsSame(NextRowFields, CurrentRowFields,
              PStruct.PMeta.FieldDefsReferringAbsIdx);
          end;
          if AddToList then
          begin
            RV := PStruct.OutList.AddItem(Row, IRecAdd);
            Assert(RV in [rvOk]); //TODO - Handle failure, why dupKey?
          end;
        end
        else
        begin
          //If row unchanged then got here because initting for the first time
          Assert(FMetadata.Added or Row.Added);
          //Only changing layout if stream from scratch, in which
          //case indexes all added, none changed field number. Trim will not
          //happen.
          Assert((not PStruct.PMeta.TableReferring.LayoutChangeRequired)
            or (PStruct.TransReason = mtrReplayFromScratch));
          //In from scratch, no delete sentinels.
          //Handle rows not fields, no need for field offset.
          RV := PStruct.OutList.AddItem(Row, IRecAdd);
          Assert(RV in [rvOk]);
        end;
      end;
    end;
    rpaReferredDeletedFieldsToList:
    begin
      //These cases slightly easier, because table not changing structure,
      //and FK relationship not newly created.
      Assert((not PStruct.PMeta.TableReferred.LayoutChangeRequired)
        or (PStruct.TransReason = mtrReplayFromScratch));
      Assert(not FMetadata.Added);
      if not Row.Added then
      begin
        //FromScratch case should not get here.
        Assert(not PStruct.PMeta.TableReferred.LayoutChangeRequired);
        if Row.Deleted or Row.Changed then
        begin

          AddToList := Row.Deleted;
          if not AddToList then
          begin
            CurrentRowFields := Row.ABData[abCurrent] as TMemStreamableList;
            NextRowFields := Row.ABData[abNext] as TMemStreamableList;
            AddToList := not SameLayoutFieldsSame(NextRowFields, CurrentRowFields,
              PStruct.PMeta.FieldDefsReferredAbsIdx);
          end;
          if AddToList then
          begin
            RV := PStruct.OutList.AddItem(Row, IRecAdd);
            Assert(RV in [rvOk]);
          end;
        end
        else //Do not expect unchanged rows here.
          raise EMemDBInternalException.Create(S_FK_INTERNAL);
      end;
    end;
    rpaReferredAddedFieldsToList:
    begin
      //These cases slightly easier, because table not changing structure,
      //and FK relationship not newly created.
      Assert((not PStruct.PMeta.TableReferred.LayoutChangeRequired)
        or (PStruct.TransReason = mtrReplayFromScratch));
      Assert(not FMetadata.Added);
      if not Row.Deleted then
      begin
        if Row.Added or Row.Changed then
        begin
          //From scratch case does get here, but no delete sentinels.
          AddToList := Row.Added;
          if not AddToList then
          begin
            NextRowFields := Row.ABData[abNext] as TMemStreamableList;
            CurrentRowFields := Row.ABData[abCurrent] as TMemStreamableList;
            AddToList := not SameLayoutFieldsSame(CurrentRowFields, NextRowFields,
              PStruct.PMeta.FieldDefsReferredAbsIdx);
          end;
          if AddToList then
          begin
            RV := PStruct.OutList.AddItem(Row, IRecAdd);
            Assert(RV in [rvOk, rvDuplicateKey]);
          end;
        end
        else //Do not expect unchanged rows here.
          raise EMemDBInternalException.Create(S_FK_INTERNAL);
      end
    end;
  else
    Assert(false);
  end;
end;

procedure TMemDBForeignKeyPersistent.CreateReferringAddedList(const Meta: TMemDBFKMeta; Reason: TMemDBTransReason);
var
  PStruct: TProcessRowFKStruct;
  IRec: TItemRec;
  RV: TIsRetVal;
begin
  with PStruct do
  begin
    OutList := Meta.Lists.FReferringAdded;
    PMeta := @Meta;
    Action := rpaReferringAddedFieldsToList;
    TransReason := Reason;
  end;
  if FMetadata.Added then
  begin
    //All rows in referring table, even if layout change. (Arrrgh!)
    IRec := Meta.TableReferring.Data.Store.GetAnItem;
    while Assigned(IRec) do
    begin
      Assert((IRec.Item as TMemDBRow).FStoreBackRec = IRec);
      ProcessRow(IRec.Item as TMemDBRow, @PStruct, nil, Meta.TableReferring.Data);
      RV := Meta.TableReferring.Data.Store.GetAnotherItem(IRec);
      Assert(RV in [rvOK, rvNotFound]);
    end;
  end
  else
  begin
    Assert(Reason <> mtrReplayFromScratch);
    //Changed rows in referring table unless layout change.
    if not Meta.TableReferring.LayoutChangeRequired then
      Meta.TableReferring.Data.ForEachChangedRow(ProcessRow, @PStruct, nil);
  end;
end;

// N.B This trim is sorta optional:
// We create list of added rows, and trim those that were already there.
// For those which were already there, we know the FK relationships holds,
// and that value would be re-checked if deleted from referred table.
//
// Add after previous ambiguity.
// If we did not do this trim, it wouldn't affect the results of the FK check.
procedure TMemDBForeignKeyPersistent.TrimReferringAddedList(const Meta: TMemDBFKMeta; Reason: TMemDBTransReason);
var
  LookasideRV: TISRetVal;
  LookasideIRec, NextLookasideIRec: TItemRec;
  LookupRow: TMemDbRow;
  LookupFields: TMemStreamableList;
  LookupFieldRecs: TMemDbFieldDataRecs;
  SearchVal: TMemDBIndexNodeSearchVal;
  IndexChangeset: TIndexChangeSet;
  ITag: PITagStruct;
  DataRV: TISRetVal;
  DataIRec: TITemRec;
{$IFOPT C+}
  DataRow: TMemDBRow;
  DataFields: TMemStreamableList;
{$ENDIF}
begin
  if not FMetadata.Added then
  begin
    if not Meta.TableReferring.LayoutChangeRequired then
    begin
      //If table changing layout, no good information about what was there before.

      //We can trim from the list all those items that can be found
      //in the *current* index of the table.
      SearchVal := TMemDBIndexNodeSearchVal.Create;
      try
        //What's the index tag we need to search on?
        ITag := FindIndexTag(Meta.TableReferring,
                             Meta.IndexDefReferring,
                             IndexChangeset,
                             sicCurrent);
        if not Assigned(ITag) then
          raise EMemDBInternalException.Create(S_FK_INTERNAL_INDEX_TAG);

        if ictChangedFieldNumber in IndexChangeset then
          raise EMemDBInternalException.Create(S_FK_INTERNAL) //Layout changed, should not be here at all.
        else if ictAdded in IndexChangeset then
          //What a pity, index only newly calculated, can't use to trim, because
          //we don't know what was there before.
          exit;

        LookasideIRec := Meta.Lists.FReferringAdded.GetAnItem;
        while Assigned(LookasideIRec) do
        begin
          LookupRow := LookasideIRec.Item as TMemDbRow;
          LookupFields := LookupRow.ABData[abLatest] as  TMemStreamableList;
          //Added rows contain latest data always in latest.
          //And we're gonna be brave and use the AbsIdx we think is always correct.
          LookupFieldRecs := BuildMultiDataRecs(LookupFields,
            Meta.FieldDefsReferringAbsIdx);
          SearchVal.FieldSearchVals := LookupFieldRecs;
          DataRV := Meta.TableReferring.Data.Store.FindByIndex(
            ITag, SearchVal, DataIRec);
          if DataRV = rvOK then
          begin
{$IFOPT C+}
            //Let's just check.
            DataRow := DataIRec.Item as TMemDBRow;
            DataFields := DataRow.ABData[abCurrent] as TMemStreamableList;
            Assert(SameLayoutFieldsSame(LookupFields, DataFields,
              Meta.FieldDefsReferringAbsIdx));
{$ENDIF}
          end;
          NextLookasideIRec := LookasideIRec;
          LookasideRV := Meta.Lists.FReferringAdded.GetAnotherItem(NextLookasideIRec);
          Assert(LookasideRV in [rvOK, rvNotFound]);
          if DataRV = rvOK then
          begin
            LookasideRV := Meta.Lists.FReferringAdded.RemoveItem(LookasideIRec);
            Assert(LookasideRV in [rvOK]);
          end;
          LookasideIRec := NextLookasideIRec;
        end;
      finally
        SearchVal.Free;
      end;
    end;
  end;
end;


procedure TMemDBForeignKeyPersistent.CreateReferredDeletedList(const Meta: TMemDBFKMeta; Reason: TMemDBTransReason);
var
  PStruct: TProcessRowFKStruct;
begin
  if not FMetadata.Added then
  begin
    if not Meta.TableReferred.LayoutChangeRequired then
    begin
      with PStruct do
      begin
        OutList := Meta.Lists.FReferredDeleted;
        PMeta := @Meta;
        Action := rpaReferredDeletedFieldsToList;
        TransReason := Reason;
      end;
      Meta.TableReferred.Data.ForEachChangedRow(ProcessRow, @PStruct, nil);
    end;
  end;
end;

// N.B. This trim is not quite so optional.
//
// We make a note of all rows deleted, but then trim those, where another
// row with the same value has simultaneously been re-added into the table.
//
// If we removed this trim, then a simultaneous delete-readd would fail the FK
// relationship, where arguably, it shouldn't.
// Issue of semantics as to whether you allow a delete,re-add on rows which
// are depended on by another table.
procedure TMemDBForeignKeyPersistent.TrimReferredDeletedList(const Meta: TMemDBFKMeta; Reason: TMemDBTransReason);
var
  PStruct: TProcessRowFKStruct;
  DeletedRV, AddedRV: TISRetVal;
  DelRow:TMemDBRow;
  DeletedIRec, NextDeletedIRec, AddedIRec: TItemRec;
  DelDel: boolean;
  SearchVal: TMemDBRowLookasideSearchVal;
  DelFields: TMemDbFieldDataRecs;
{$IFOPT C+}
  AddRow: TMemDBRow;
{$ENDIF}
begin
  if not FMetadata.Added then
  begin
    if not Meta.TableReferred.LayoutChangeRequired then
    begin
      SearchVal := TMemDBRowLookasideSearchVal.Create;
      try
        //Create a referred added list if applicable, and difference the lists.
        with PStruct do
        begin
          OutList := Meta.Lists.FReferredAdded;
          PMeta := @Meta;
          Action := rpaReferredAddedFieldsToList;
          TransReason := Reason;
        end;
        Meta.TableReferred.Data.ForEachChangedRow(ProcessRow, @PStruct, nil);

        //Now do a bit of differencing.

        //For each set of "previous" fields in the deleted list,
        //we can trim the row if they are in the "next" fields in the added list.
        DeletedIRec := Meta.Lists.FReferredDeleted.GetAnItem;
        while Assigned(DeletedIRec) do
        begin
          DelRow := DeletedIRec.Item as TMemDBRow;
          DelFields := BuildMultiDataRecs(DelRow.ABData[abCurrent] as TMemStreamableList,
            Meta.FieldDefsReferredAbsIdx);
          SearchVal.FieldSearchVals := DelFields;
          AddedRV := Meta.Lists.FReferredAdded.FindByIndex(
            @Meta.Lists.TagReferredAddedNext, SearchVal, AddedIRec);
          Assert(AddedRV in [rvOK, rvNotFound]);
          DelDel := AddedRV = rvOK;

          NextDeletedIRec := DeletedIRec;
          DeletedRV := Meta.Lists.FReferredDeleted.GetAnotherItem(NextDeletedIRec);
          Assert(DeletedRV in [rvOK, rvNotFound]);
          if DelDel then
          begin
{$IFOPT C+}
            AddRow := AddedIRec.Item as TMemDBRow;
            Assert(SameLayoutFieldsSame(AddRow.ABData[abNext] as TMemStreamableList,
              DelRow.ABData[abCurrent] as TMemStreamableList, Meta.FieldDefsReferredAbsIdx));
{$ENDIF}
            DeletedRV := Meta.Lists.FReferredDeleted.RemoveItem(DeletedIRec);
            Assert(DeletedRV = rvOK);
          end;
          DeletedIRec := NextDeletedIRec;
        end;
      finally
        SearchVal.Free;
      end;
    end;
  end;
end;

function TMemDBForeignKeyPersistent.FindIndexTag(Table: TMemDbTablePersistent;
                          IndexDef: TMemIndexDef;
                          var OutChangeset: TIndexChangeset;
                          SubIndexClass: TSubIndexClass): PITagStruct;
var
  LatestTableMeta: TMemTableMetadataItem;
  idx: integer;
  IndexOffset: integer;
  TagData: TMemDBITagData;

begin
  //Calculate index tag for referring / referred.
  IndexOffset := -1;
  //Latest metadata even though current index.
  LatestTableMeta := Table.Metadata.ABData[abLatest] as TMemTableMetadataItem;
  //Find the absolute index offset
  for idx := 0 to Pred(LatestTableMeta.IndexDefs.Count) do
  begin
    if LatestTableMeta.IndexDefs.Items[idx] = IndexDef then
    begin
      IndexOffset := idx;
      break;
    end;
  end;
  if IndexOffset < 0 then
  begin
    OutChangeset := [];
    result := nil;
  end
  else
  begin
    //Get the index changeset attributes.
    Assert((Length(Table.FIndexChangesets) = 0)
      or (IndexOffset < Length(Table.FIndexChangesets)));

    if Length(Table.FIndexChangesets) > 0 then
      OutChangeset := Table.FIndexChangesets[IndexOffset]
    else
      OutChangeset := [];

    TagData := Table.FTagDataList[IndexOffset];
    if ictChangedFieldNumber in OutChangeset then
    begin
      Table.CheckTagAgreesWithMetadata(IndexOffset, TagData, tciTemporaryAgreesBoth, tcpProgrammed);
      result := TagData.TagStructs[SubIndexClass];
    end
    else if ictAdded in OutChangeset then
    begin
      Table.CheckTagAgreesWithMetadata(IndexOffset, TagData, tciTemporaryAgreesNextOnly, tcpProgrammed);
      result := TagData.TagStructs[SubIndexClass];
    end
    else
    begin
      Table.CheckTagAgreesWithMetadata(IndexOffset, TagData, tciPermanentAgreesCurrent, tcpProgrammed);
      result := TagData.TagStructs[SubIndexClass];
    end;
  end;
end;

procedure TMemDBForeignKeyPersistent.CheckOutstandingCrossRefs(const Meta: TMemDBFKMeta; Reason: TMemDBTransReason);
var
  SearchVal: TMemDBIndexNodeSearchVal;
  ListRetVal, DataRetVal: TISRetVal;
  ListIRec, DataIRec: TItemRec;
  ITag: PITagStruct;
  IndexChangeset: TIndexChangeset;
  ListRow: TMemDbRow;
  ListDataRecs: TMemDbFieldDataRecs;
{$IFOPT C+}
  DataRow: TMemDBRow;
{$ENDIF}

begin
  //OK, so we now need to check:
  //1. That for every item in Referrer added, there is an entry in referred table.
  //2. That for every entry in Referred deleted, there is no entry in referrer table.
  SearchVal := TMemDBIndexNodeSearchVal.Create;
  try
    //Check new vales in referrer exist in referred.
    ITag := FindIndexTag(Meta.TableReferred,
                         Meta.IndexDefReferred,
                         IndexChangeset,
                         sicLatest);
    if not Meta.TableReferred.Data.Store.HasIndex(ITag) then
      raise EMemDBInternalException.Create(S_FK_INTERNAL);
    ListIRec := Meta.Lists.FReferringAdded.GetAnItem;
    while Assigned(ListIRec) do
    begin
      ListRow := ListIRec.Item as TMemDbRow;
      //Referring added, new values in abLatest, cos can add from scratch too.
      ListDataRecs := BuildMultiDataRecs(ListRow.ABData[abLatest] as TMemStreamableList,
        Meta.FieldDefsReferringAbsIdx);
      SearchVal.FieldSearchVals := ListDataRecs;
      DataRetVal := Meta.TableReferred.Data.Store
        .FindByIndex(TObject(ITag), SearchVal, DataIRec);
      Assert(DataRetVal in [rvOK, rvNotFound]);
      if DataRetVal <> rvOK then
        raise EMemDBConsistencyException.Create(S_FK_NOT_IN_REFERRED_TABLE);
{$IFOPT C+}
        //Just check the fields really match.
        DataRow := DataIRec.Item as TMemDBRow;
        Assert(DiffLayoutFieldsSame(ListRow.ABData[abLatest] as TMemStreamableList,
                             Meta.FieldDefsReferringAbsIdx,
                             DataRow.ABData[abLatest] as TMemStreamableList,
                             Meta.FieldDefsReferredAbsIdx));
{$ENDIF}
      ListRetVal := Meta.Lists.FReferringAdded.GetAnotherItem(ListIRec);
      Assert(ListRetVal in [rvOK, rvNotFound]);
    end;
    //Check deleted values in referred not in referrer.
    ITag := FindIndexTag(Meta.TableReferring,
                         Meta.IndexDefReferring,
                         IndexChangeset, sicLatest);
    if not Meta.TableReferring.Data.Store.HasIndex(ITag) then
      raise EMemDBInternalException.Create(S_FK_INTERNAL);
    ListIRec := Meta.Lists.FReferredDeleted.GetAnItem;
    while Assigned(ListIRec) do
    begin
      ListRow := ListIrec.Item as TMemDBRow;
      //Referred deleted, old values in abCurrent.
      ListDataRecs := BuildMultiDataRecs(ListRow.ABData[abCurrent] as TMemStreamableList,
        Meta.FieldDefsReferredAbsIdx);
      SearchVal.FieldSearchVals := ListDataRecs;
      DataRetVal := Meta.TableReferring.Data.Store
        .FindByIndex(TObject(ITag), SearchVal, DataIRec);
      Assert(DataRetVal in [rvOK, rvNotFound]);
      if DataRetVal = rvOK then
      begin
{$IFOPT C+}
        //Just check the fields really match.
        DataRow := DataIRec.Item as TMemDBRow;
        Assert(DiffLayoutFieldsSame(ListRow.ABData[abCurrent] as TMemStreamableList,
            Meta.FieldDefsReferredAbsIdx,
            DataRow.ABData[abLatest] as TMemStreamableList,
            Meta.FieldDefsReferringAbsIdx));
{$ENDIF}
        raise EMemDBConsistencyException.Create(S_FK_IN_REFERRING_TABLE);
      end;
      ListRetVal := Meta.Lists.FReferredDeleted.GetAnotherItem(ListIRec);
      Assert(ListRetVal in [rvOK, rvNotFound]);
    end;
  finally
    SearchVal.Free;
  end;
end;

procedure TMemDbForeignKeyPersistent.SetupIndexes(var Meta: TMemDBFkMeta);
var
  RV: TIsRetVal;
begin
  with Meta.Lists do
  begin
    TagReferredAddedNext.abBuf := abNext;
    TagReferredAddedNext.FieldAbsIdxs := CopyFieldOffsets(Meta.FieldDefsReferredAbsIdx);
    SyncFastOffsets(TagReferredAddedNext.FieldAbsIdxs, TagReferredAddedNext.FieldAbsIdxsFast);

    FReferringAdded := TIndexedStoreO.Create;
    FReferredDeleted := TIndexedStoreO.Create;
    FReferredAdded := TIndexedStoreO.Create;
    RV := FReferredAdded.AddIndex(TMemDBRowLookasideIndexNode, @TagReferredAddedNext, false);
    Assert(RV = rvOK);
  end;
end;

procedure TMemDbForeignKeyPersistent.ClearIndexes(var Meta: TMemDBFkMeta);
begin
  with Meta.Lists do
  begin
    FReferringAdded.Free;
    FReferredDeleted.Free;
    FReferredAdded.Free;
    FReferringAdded := nil;
    FReferredDeleted := nil;
    FReferredAdded := nil;
    SetLength(TagReferredAddedNext.FieldAbsIdxs, 0);
    SyncFastOffsets(TagReferredAddedNext.FieldAbsIdxs, TagReferredAddedNext.FieldAbsIdxsFast);
  end;
end;
  //Referential integrity is violated when:
  //1. Rows / data are added to the Referring that do not exist in the referred.
  //2. Rows / data are deleted from the Referred that exist in the referring.

  //Initial list creation:

  //a) Change case:

  //a1)Referring Added:
  //Added / changed rows when new field data exists.
  //Can remove from added list if data previously in table, and unchanged, (not deleted)
  //Cannot remove from added list if another row with the data has been deleted.

  //a2)Referred deleted:
  //Changed / deleted rows: Original data changed or deleted.
  //If we have change-deleted in one place, then uniqueness constraints mean
  //the data is not to be found anywhere else in the table,
  //except where changed-added (no need to search unchanged rows).

  //b) From scratch case
  //Referring added list is all nondeleted rows, latest buffer.
  //Referrer deleted list is empty.

  //c) Handling the case where field indexes are changing.
  //i) Change case: No items in added or deleted for either table.
  //ii) From scratch case: Fine, just use new copy of fields.
  //iii) Table lookups by index, get the right index tag for the most recent table copy.

  //Final checking:

  //For each item left in referring added list, there should be a row in the referrer table.
  //For each item left in the referred deleted list, there should not be a row in the referring table.

procedure TMemDBForeignKeyPersistent.CreateCheckForeignKeyRowSets(var  Meta: TMemDBFKMeta; Reason: TMemDBTransReason);
begin
  //Create some "lookaside lists" of values that have been added / deleted.
  //The store contains TMemFieldData items.
  try
    //N.B. Indexed store gives us a "duplicate key" error code, which we should use.
    SetupIndexes(Meta);
    CreateReferringAddedList(Meta, Reason);
    TrimReferringAddedList(Meta, Reason);
    CreateReferredDeletedList(Meta, Reason);
    TrimReferredDeletedList(Meta, Reason);
    CheckOutstandingCrossRefs(Meta, Reason);
  finally
    ClearIndexes(Meta);
  end;
end;

{$ENDIF}

function TMemDBForeignKeyPersistent.HandleAPITableRename(const Sel: TBufSelector; OldName, NewName: string): boolean;
{$IFDEF MEMDB2_TEMP_REMOVE}
var
  FKMeta: TMemDBForeignKeyMetadata;
  AB:TABSelection;
begin
  result := false;
  AB := IsoToAB(Iso);
  if AssignedNotSentinel(Metadata.ABData[abLatest]) then
  begin
    FKMeta := Metadata as TMemDBForeignKeyMetadata;
    if OldName = FKMeta.Referer[fkTable, AB] then
    begin
      //Rename overwrites should already have been caught.
      if OldName <> FKMeta.Referer[fkTable, abLatest] then
        raise EMemDBInternalException.Create(S_FK_INTERNAL_OVERWRITE);
      FKMeta.SetReferer(fkTable, NewName);
      result :=  true;
    end;
    if OldName = FKMeta.Referred[fkTable, AB] then
    begin
      //Rename overwrites should already have been caught.
      if OldName <> FKMeta.Referred[fkTable, abLatest] then
        raise EMemDBInternalException.Create(S_FK_INTERNAL_OVERWRITE);
      FKMeta.SetReferred(fkTable, NewName);
      result := true;
    end;
  end;
{$ENDIF}
begin
  Assert(false); //TODO - write this.
  result := false;
end;

procedure TMemDBForeignKeyPersistent.CheckAPITableDelete(const Sel: TBufSelector; TableName: string);
{$IFDEF MEMDB2_TEMP_REMOVE}
var
  FKMeta: TMemDBForeignKeyMetadata;
  AB:TABSelection;
begin
  AB := IsoToAB(Iso);
  if AssignedNotSentinel(Metadata.ABData[abLatest]) then
  begin
    FKMeta := Metadata as TMemDBForeignKeyMetadata;
    //Looks funny, but foreign key references cannot be moved from one place
    //to another. After multiple renames, should still be able to refer
    //to a table by its old name....
    if (TableName = FKMeta.Referer[fkTable, AB]) or
       (TableName = FKMeta.Referred[fkTable, AB]) then
      raise EMemDBAPIException.Create(S_FK_REFERENCES_TABLE);
  end;
{$ENDIF}
begin
  Assert(false); //TODO - write this.
end;

function TMemDBForeignKeyPersistent.HandleAPIIndexRename(const Sel: TBufSelector; IsoDeterminedTableName, OldName, NewName: string): boolean;
{$IFDEF MEMDB2_TEMP_REMOVE}
var
  FKMeta: TMemDBForeignKeyMetadata;
  AB:TABSelection;
begin
  result := false;
  AB := IsoToAB(Iso);
  if AssignedNotSentinel(Metadata.ABData[abLatest]) then
  begin
    FKMeta := Metadata as TMemDBForeignKeyMetadata;
    if IsoDeterminedTableName = FKMeta.Referer[fkTable, AB] then
    begin
      if OldName = FKMeta.Referer[fkIndex, AB] then
      begin
        //Rename overwrites should already have been caught.
        if OldName <> FKMeta.Referer[fkIndex, abLatest] then
          raise EMemDBInternalException.Create(S_FK_INTERNAL_OVERWRITE);
        FKMeta.SetReferer(fkIndex, NewName);
        result :=  true;
      end;
    end;
    if IsoDeterminedTableName = FKMeta.Referred[fkTable, AB] then
    begin
      if OldName = FKMeta.Referred[fkIndex, AB] then
      begin
        //Rename overwrites should already have been caught.
        if OldName <> FKMeta.Referred[fkIndex, abLatest] then
          raise EMemDBInternalException.Create(S_FK_INTERNAL_OVERWRITE);
        FKMeta.SetReferred(fkIndex, NewName);
        result :=  true;
      end;
    end;
  end;
{$ENDIF}
begin
  Assert(false); //TODO - write this.
  result := false;
end;

procedure TMemDBForeignKeyPersistent.CheckAPIIndexDelete(const Sel: TBufSelector; IsoDeterminedTableName, IndexName: string);
{$IFDEF MEMDB2_TEMP_REMOVE}
var
  FKMeta: TMemDBForeignKeyMetadata;
  AB:TABSelection;
begin
  AB := IsoToAB(Iso);
  if AssignedNotSentinel(Metadata.ABData[abLatest]) then
  begin
    FKMeta := Metadata as TMemDBForeignKeyMetadata;
    //Looks funny, but foreign key references cannot be moved from one place
    //to another. After multiple renames, should still be able to refer
    //to a table by its old name....
    if IsoDeterminedTableName = FKMeta.Referer[fkTable, AB] then
    begin
      if IndexName = FKMeta.Referer[fkIndex, AB] then
        raise EMemDBAPIException.Create(S_FK_REFERENCES_INDEX);
    end;
    if IsoDeterminedTableName = FKMeta.Referred[fkTable, AB] then
    begin
      if IndexName = FKMeta.Referred[fkIndex, AB] then
        raise EMemDBAPIException.Create(S_FK_REFERENCES_INDEX);
    end;
  end;
{$ENDIF}
begin
  Assert(false); //TODO - write this.
end;

{ TMemDBRow }

constructor TMemDBRow.Create;
begin
  inherited;
  FProxy := TMemDBRowProxy.Create;
  FProxy.Proxy := self;
  SetListCreateClass(TMemRowFields);
end;

procedure TMemDBRow.Init(Table: TMemDBTablePersistent; const Guid: TGUID);
begin
  self.FTable := Table;
  self.FRowID := Guid;
end;

procedure TMemDBRow.DCPHandle(const Update: TReferenceUpdate);
begin
  FTable.RowDCPHandleAndForward(self, Update);
end;

procedure TMemDBRow.CPTidHandle(const Update: TTidUpdate);
begin
  FTable.RowCPTidHandleAndForward(self, Update);
end;

function TMemDBRow.CheckFormatAgainstMeta(const Tid: TTransactionId; AB: TAbSelType; MD: TMemTableMetadataItem; PinReason: TPinReason): boolean;
var
  MetaFieldDefs: TMemStreamableList;
begin
  if NotAssignedOrSentinel(MD) then
    MetaFieldDefs := nil
  else
    MetaFieldDefs := MD.FieldDefs;
  result := CheckFormatAgainstMetaDefs(Tid, AB, MetaFieldDefs, PinReason);
end;

class function TMemDBRow.StaticCheckFormatAgainstMeta(Data: TMemDBStreamable; MD: TMemTableMetadataItem): boolean;
var
  MetaFieldDefs: TMemStreamableList;
begin
  if NotAssignedOrSentinel(MD) then
    MetaFieldDefs := nil
  else
    MetaFieldDefs := MD.FieldDefs;
  result := StaticCheckFormatAgainstMetaDefs(Data, MetaFieldDefs);
end;

class function TMemDBRow.StaticCheckFormatAgainstMetaDefs(Data: TMemDBStreamable; MetaFieldDefs: TMemStreamableList): boolean;
var
  Def: TMemDBStreamable;
  DataFields: TMemStreamableList;
  DataCount,DefCount, i: integer;
  DataField: TMemFieldData;
  FieldDef: TMemFieldDef;
begin
  DataFields := nil;
  if NotAssignedorSentinel(Data) then
    DataCount := 0
  else
  begin
    DataFields := Data as TMemStreamableList;
    DataCount := DataFields.Count;
  end;
  if NotAssignedOrSentinel(MetaFieldDefs) then
    DefCount := 0
  else
    DefCount := MetaFieldDefs.Count;
  //Expect delete sentinel ordering etc to be the same between the two
  //sets if they exist, so gross counts and sentinel placement should be the same.
  if DataCount <> DefCount then
  begin
    result := false;
    exit;
  end;
  for i := 0 to Pred(DataCount) do
  begin
    Data := DataFields.Items[i];
    Def := MetaFieldDefs.Items[i];
    Assert(Assigned(Data));
    Assert(Assigned(Def));
    //Expect possible sentinels, but not NULL.
    if (Data is TMemDeleteSentinel) <> (Def is TMemDeleteSentinel) then
    begin
      result := false;
      exit;
    end;
    if not (Data is TMemDeleteSentinel) then
    begin
      DataField := Data as TMemFieldData;
      FieldDef := Def as TMemFieldDef;
      if DataField.FDataRec.FieldType <> FieldDef.FieldType then
      begin
        result := false;
        exit;
      end;
    end;
  end;
  result := true;
end;

function TMemDBRow.CheckFormatAgainstMetaDefs(const Tid: TTransactionId; AB: TAbSelType; MetaFieldDefs: TMemStreamableList; PinReason: TPinReason): boolean;
var
  Data: TMemDBStreamable;
  bufSel: TAbSelType;
begin
  Data := nil;
  case AB of
    abCurrent: Data := PinCurrent(Tid, PinReason);
    abNext: Data := GetNext(Tid);
    abLatest: Data := GetPinLatest(Tid, bufSel, PinReason);
  else
    Assert(false);
  end;
  result := StaticCheckFormatAgainstMetaDefs(Data, MetaFieldDefs);
end;

procedure TMemDBRow.ToJournal(const Tid: TTransactionId; Stream: TStream);
var
  Current, Next: TMemDbStreamable;
  Added, Changed, Deleted, Null: boolean;
begin
  Current := PinCurrent(Tid, pinEvolve);
  Next := GetNext(Tid);
  ChangeFlagsFromPinned(Current, Next, Added, Changed, Deleted, Null);
  if Added or Changed or Deleted then
  begin
    WrTag(Stream, mstRowStartV2);
    WrGuid(Stream, RowId);
    inherited;
    WrTag(Stream, mstRowEnd);
  end;
end;

procedure TMemDBRow.ToScratch(const PseudoTid:TTransactionId; Stream: TStream);
begin
  WrTag(Stream, mstRowStartV2);
  WrGuid(Stream, RowId);
  inherited;
  WrTag(Stream, mstRowEnd);
end;

procedure TMemDBRow.FromJournal(const PseudoTid: TTransactionId; Stream: TStream);
var
  StreamRowGuid: TGUID;
  CheckItemCurrent, CheckItemNext: TMemStreamableList;
  Cur, Nxt: TMemDbStreamable;
begin
  ExpectTag(Stream, mstRowStartV2);
  StreamRowGUID := RdGuid(Stream);
  if CompareGuids(RowId, StreamRowGUID) <> 0 then
    raise EMemDBInternalException.Create(S_ROW_IDS_DISAGREE);
  inherited;
  ExpectTag(Stream, mstRowEnd);
  Cur := PinCurrent(PseudoTid, pinEvolve);
  Nxt := GetNext(PseudoTid);
  if AssignedNotSentinel(Cur) then
    CheckItemCurrent := Cur as TMemStreamableList
  else
    CheckItemCurrent := nil;
  if AssignedNotSentinel(Nxt) then
    CheckItemNext := Nxt as TMemStreamableList
  else
    CheckItemNext := nil;
  CheckABStreamableListChange(CheckItemCurrent, CheckItemNext);
end;

procedure TMemDBRow.FromScratch(const PseudoTid: TTransactionId; Stream: TStream);
var
  StreamRowGUID: TGUID;
  Cur, Nxt: TMemDBStreamable;
begin
  ExpectTag(Stream, mstRowStartV2);
  StreamRowGUID := RdGuid(Stream);
  if CompareGuids(RowId, StreamRowGuid) <> 0 then
    raise EMemDBInternalException.Create(S_ROW_IDS_DISAGREE);
  inherited;
  ExpectTag(Stream, mstRowEnd);
  //Assuming no delete sentinels in A/B top level copies, and every row is an add.
  Cur := PinCurrent(PseudoTid, pinEvolve);
  Nxt := GetNext(PseudoTid);
  if AssignedNotSentinel(Cur)
    or (NotAssignedOrSentinel(Nxt)) then
    raise EMemDbInternalException.Create(S_JOURNAL_ROW_NOT_NEW);
  CheckABStreamableListChange(nil, Nxt as TMemStreamableList);
end;

//Assumption here is all stuff TidLocal is in local thread, so
//no concurrency to worry about, except with the nasty issue
//of index nodes in not-fully-local trees going away:

//We can't always unambiguously determine whether an INode is reachable from
//a tree without doing a top-down search.

//Also you'd think that we just have a pair of possible nodes to worry
//about, but that's not the case: Root index changing (cur / nxt) can be
//going on at the same time, so we'll end up with a candidate list of nodes,
//some possibly ephemeral, but we know that only one tree delete should succeed.
function TMemDBRow.MakeCandidateIPins(Index: TMemDBIndex): TList;
var
  GIndex: TMemDBIndex;
  Pin, MatchPin: PMemDbIndexPin;
  Match: boolean;
  i: integer;
begin
  Assert(Assigned(Index));
  GIndex := Index.ParentIndex;
  Result := TList.Create;
  try
    LockSelf;
    try
      Pin := PMemDBIndexPin(self.FIndexPins.FLink.Owner);
      while Assigned(Pin) do
      begin
        Match := (Pin.INode.OriginalIndex = Index) or
        (Assigned(GIndex) and (Pin.INode.OriginalIndex = GIndex));
        if Match then
        begin
          MatchPin := self.HoldIndexPinInLock(Pin);
          if Assigned(MatchPin) then
            Result.Add(MatchPin);
        end;
        Pin := PMemDbIndexPin(Pin.Link.FLink.Owner);
      end;
    finally
      UnlockSelf;
    end;
    //If local index search, then put the local one first,
    //else we have no idea which INode it is.
    if Assigned(GIndex) then
    begin
      Match := false;
      for i  := 0 to Pred(result.Count) do
      begin
        MatchPin := PMemDbIndexPin(result.Items[i]);
        if MatchPin.INode.OriginalIndex = Index then
        begin
          if Match then //Check its the only one.
            raise EMemDbInternalException.Create(S_INDEX_PIN_COLLATION_BAD);
          Match := true;
          if (i <> 0) and (result.Count > 1) then
          begin
            //Swap with item 0.
            result.Items[i] := result.Items[0];
            result.Items[0] := MatchPin;
          end;
        end;
      end;
    end;
  except
    while Result.Count > 0 do
    begin
      ReleaseIndexPinOutsideLock(PMemDbIndexPin(Result.Items[Pred(Result.Count)]));
      Result.Delete(Pred(Result.Count));
    end;
    Result.Free;
    raise;
  end;
end;

procedure TMemDBRow.RemoveFromLocalIndices(TidLocal: TTidLocal);
var
  Index: TMemDbIndex;
  INodeList: TList;
  i, j, DelCount: integer;
  Pin: PMemDbIndexPin;
begin
  for i := 0 to Pred(TidLocal.FLocalIndexCopies.Count) do
  begin
    Index := TidLocal.FLocalIndexCopies.Items[i] as TMemDbIndex;
    INodeList := MakeCandidateIPins(Index);
    try
      DelCount := 0;
      for j := 0 to Pred(INodeList.Count) do
      begin
        Pin := PMemDbIndexPin(INodeList.Items[i]);
        if Index.Remove(TABSelType.abCurrent, Pin.INode) then
        begin
          Inc(DelCount);
{$IFOPT C-}
          break;
{$ENDIF}
        end;
      end;
      if DelCount <> 1 then
        raise EMemDBInternalException.Create(S_LOCAL_INDEX_DELETION_BAD);
    finally
      while INodeList.Count > 0 do
      begin
        ReleaseIndexPinOutsideLock(PMemDbIndexPin(INodeList.Items[Pred(INodeList.Count)]));
        INodeList.Delete(Pred(INodeList.Count));
      end;
      INodeList.Free;
    end;
  end;
end;

procedure TMemDBRow.AddToLocalIndices(TidLocal: TTidLocal);
var
  Index: TMemDbIndex;
  INodeList: TList;
  Pin: PMemDbIndexPin;
  INode: TMemDbIndexLeaf;
  i: integer;
begin
  //Because of index de-duplication, we can't determine duplicate addition via
  //inode. However, because the index is local, we can be sure there
  //should not be any candidate IPins with our local index as the original index.
  for i := 0 to Pred(TidLocal.FLocalIndexCopies.Count) do
  begin
    Index := TidLocal.FLocalIndexCopies.Items[i] as TMemDbIndex;
    INodeList := MakeCandidateIPins(Index);
    try
      if INodeList.Count > 0 then
      begin
        Pin := PMemDbIndexPin(InodeList.Items[0]);
        if Pin.INode.OriginalIndex = Index then
          raise EMemDBInternalException.Create(S_LOCAL_INDEX_INSERTION_DUPLICATE);
      end;
    finally
      while INodeList.Count > 0 do
      begin
        ReleaseIndexPinOutsideLock(PMemDbIndexPin(INodeList.Items[Pred(INodeList.Count)]));
        INodeList.Delete(Pred(INodeList.Count));
      end;
      INodeList.Free;
    end;
    //OK, we're in with a chance of a good insertion.
    INode := TMemDbIndexLeaf.Create;
    try
      if not self.PinForIndex(TidLocal.FTid, TABSelType.abLatest, INode) then
        raise EMemDbInternalException.Create(S_LOCAL_INDEX_INSERTION_BAD_1);
      if not Index.Add(abCurrent, INode) then
        raise EMemDbInternalException.Create(S_LOCAL_INDEX_INSERTION_BAD_2);
    except
      on E: Exception do INode.Release;
    end;
  end;
end;

{ TMemDBEntityMetadata }

constructor TMemDbEntityMetadata.Create;
begin
  inherited;
  SetListCreateClass(TMemStreamableList);
end;

procedure TMemDBEntityMetadata.Init(const Tid: TTransactionId; Parent: TObject; Name:string; DSName: boolean);
begin
  FEntity := Parent as TMemDBEntity;
  inherited;
  //Child classes create and set initial metadata item, incrementing refs.
end;

procedure TMemDBEntityMetadata.DCPHandle(const Update: TReferenceUpdate);
begin
  FEntity.MetadataDCPHandle(self, Update);
  inherited;
end;

{ TMemDbTableMetadata }

procedure TMemDbTableMetadata.Init(const Tid: TTransactionId; Parent: TObject; Name:string; DSName: boolean);
var
  MTI: TMemTableMetadataItem;
begin
  inherited; //Parenting needs to be assigned first for reffing to work
  if DSName then
  begin
    MTI := TMemTableMetadataItem.Create;
    MTI.EntityName := Name;
    DirectSetNext(Tid, MTI);
  end;
end;

procedure TMemDbTableMetadata.CheckABListChanges(const Tid: TTransactionId);
var
  C, N: TMemDBStreamable;
  Cur, Nxt: TMemTableMetadataItem;
  CurF, NextF, CurI, NextI: TMemStreamableList;
begin
  C := PinCurrent(Tid, pinFinalCheck);
  N := GetNext(Tid);
  if AssignedNotSentinel(C) then
    Cur := C as TMemTableMetadataItem
  else
    Cur := nil;
  if AssignedNotSentinel(N) then
    Nxt := N as TMemTableMetadataItem
  else
    Nxt := nil;
  if Assigned(Cur) then
  begin
    CurF := Cur.FieldDefs;
    CurI := Cur.IndexDefs;
  end
  else
  begin
    CurF := nil;
    CurI := nil;
  end;
  if Assigned(Nxt) then
  begin
    NextF := Nxt.FieldDefs;
    NextI := Nxt.IndexDefs;
  end
  else
  begin
    NextF := nil;
    NextI := nil;
  end;
  //Assuming no delete sentinels in A/B top level copies, but delete
  //sentinels in lists.
  CheckABStreamableListChange(CurF, NextF);
  CheckABStreamableListChange(CurI, NextI);
end;

procedure TMemDbTableMetadata.PreCommit(const Tid: TTransactionId; Reason: TMemDbTransReason);
begin
  inherited;
  CheckABListChanges(Tid);
end;

function FieldsByNamesInt(MetadataCopy: TMemTableMetadataItem; const Names:TMDBFieldNames; var AbsIndexes: TFieldOffsets): TMemFieldDefs;
var
  i: integer;
  SomeData: boolean;
begin
  SetLength(result, Length(Names));
  SetLength(AbsIndexes, Length(Names));
  SomeData := false;
  for i := 0 to Pred(Length(Names)) do
  begin
    result[i] := FieldByNameInt(MetadataCopy, Names[i], Absindexes[i]);
    SomeData := SomeData or (Assigned(result[i]));
  end;
  if not SomeData then
  begin
    SetLength(result, 0);
    SetLength(AbsIndexes, 0);
  end;
end;

function FieldByNameInt(MetadataCopy: TMemTableMetadataItem; const Name:string; var AbsIndex: integer): TMemFieldDef;
var
  Field: TMemFieldDef;
  i: integer;
begin
  result := nil;
  AbsIndex := -1;
  for i := 0 to Pred(MetadataCopy.FieldDefs.Count) do
  begin
    Assert(Assigned(MetadataCopy.FieldDefs.Items[i]));
    if not (MetadataCopy.FieldDefs.Items[i] is TMemDeleteSentinel) then
    begin
      Field := MetadataCopy.FieldDefs.Items[i] as TMemFieldDef;
      if Field.FieldName = Name then
      begin
        AbsIndex := i;
        result := Field;
        exit;
      end;
    end;
  end;
end;

function IndexByNameInt(MetadataCopy: TMemTableMetadataItem; const Name:string; var AbsIndex: integer): TMemIndexDef;
var
  Index: TMemIndexDef;
  i: integer;
begin
  result := nil;
  for i := 0 to Pred(MetadataCopy.IndexDefs.Count) do
  begin
    Assert(Assigned(MetadataCopy.IndexDefs.Items[i]));
    if not (MetadataCopy.IndexDefs.Items[i] is TMemDeleteSentinel) then
    begin
      Index := MetadataCopy.IndexDefs.Items[i] as TMemIndexDef;
      if Index.IndexName = Name then
      begin
        AbsIndex := i;
        result := Index;
        exit;
      end;
    end;
  end;
end;

function TMemDBTableMetadata.FieldsByNames(const AB: TBufSelector; const Names: TMDBFieldNames; var AbsIdxs: TFieldOffsets; Reason: TPinReason): TMemFieldDefs;
var
  MC: TMemDbStreamable;
  bufSel: TABSelType;
begin
  MC := nil;
  case AB.SelType of
    abCurrent: MC := self.PinCurrent(AB.TId, Reason);
    abNext: MC := self.GetNext(AB.TId);
    abLatest: MC := self.GetPinLatest(AB.TId, bufSel, Reason);
  else
    Assert(false);
  end;
  if AssignedNotSentinel(MC) then
    result := FieldsByNamesInt(MC as TMemTableMetadataItem, Names, AbsIdxs)
  else
  begin
    SetLength(result, 0);
    SetLength(AbsIdxs, 0);
  end;
end;

function TMemDbTableMetadata.FieldByName(const AB: TBufSelector; const Name: string; var AbsIndex: integer; Reason: TPinReason): TMemFieldDef;
var
  MC: TMemDbStreamable;
  bufSel: TABSelType;
begin
  MC := nil;
  case AB.SelType of
    abCurrent: MC := self.PinCurrent(AB.TId, Reason);
    abNext: MC := self.GetNext(AB.TId);
    abLatest: MC := self.GetPinLatest(AB.TId, bufSel, Reason);
  else
    Assert(false);
  end;
  if AssignedNotSentinel(MC) then
    result := FieldByNameInt(MC as TMemTableMetadataItem, Name, AbsIndex)
  else
    result := nil;
end;

function TMemDBTableMetadata.IndexByName(const AB: TBufSelector; const Name: string; var AbsIndex: integer; Reason: TPinReason): TMemIndexDef;
var
  MC: TMemDbStreamable;
  bufSel: TABSelType;
begin
  MC := nil;
  case AB.SelType of
    abCurrent: MC := self.PinCurrent(AB.TId, Reason);
    abNext: MC := self.GetNext(AB.TId);
    abLatest: MC := self.GetPinLatest(AB.TId, bufSel, Reason);
  else
    Assert(false);
  end;
  if AssignedNotSentinel(MC) then
    result := IndexByNameInt(MC as TMemTableMetadataItem, Name, AbsIndex)
  else
    result := nil;
end;

{ TMemDBForeignKeyMetadata }

{$IFDEF MEMDB2_TEMP_REMOVE}

function TMemDbForeignKeyMetadata.GetReferer(RT: TMemDBFKRefType; AB: TABSelection):string;
var
  Meta: TMemForeignKeyMetadataItem;
begin
  Assert(AssignedNotSentinel(ABData[AB]));
  Meta := ABData[AB] as TMemForeignKeyMetadataItem;
  case RT of
    fkTable: result := Meta.TableReferer;
    fkIndex: result := Meta.IndexReferer;
  else
    Assert(false);
  end;
end;

function TMemDbForeignKeyMetadata.GetReferred(RT: TMemDBFKRefType; AB: TABSelection):string;
var
  Meta: TMemForeignKeyMetadataItem;
begin
  Assert(AssignedNotSentinel(ABData[AB]));
  Meta := ABData[AB] as TMemForeignKeyMetadataItem;
  case RT of
    fkTable: result := Meta.TableReferred;
    fkIndex: result := Meta.IndexReferred;
  else
    Assert(false);
  end;
end;

procedure TMemDBForeignKeyMetadata.ConsistencyCheck;
var
  Meta: TMemForeignKeyMetadataItem;
begin
  if AssignedNotSentinel(ABData[abLatest]) then
  begin
    Meta := ABData[abLatest] as TMemForeignKeyMetadataItem;
    if (Length(Meta.TableReferer) = 0)
      or (Length(Meta.TableReferred) = 0)
      or (Length(Meta.IndexReferer) = 0)
      or (Length(Meta.IndexReferred) = 0) then
      raise EMemDBInternalException.Create(S_FK_FIELD_MISSING);
  end;
  //Probably don't check whether refs etc are really valid here,
  //can do that in the pre-commit check when we need to use them.
end;

{$ENDIF}

procedure TMemDbForeignKeyMetadata.Init(const Tid: TTransactionId; Parent: TObject; Name:string; DSName: boolean);
var
  MTI: TMemForeignKeyMetadataItem;
begin
  inherited; //Parenting needs to be assigned first for reffing to work
  if DSName then
  begin
    MTI := TMemForeignKeyMetadataItem.Create;
    MTI.EntityName := Name;
    DirectSetNext(Tid, MTI);
  end;
end;

{$IFDEF MEMDB2_TEMP_REMOVE}

procedure TMemDbForeignKeyMetadata.SetReferer(RT: TMemDBFKRefType; Referer: string);
var
  Meta: TMemForeignKeyMetadataItem;
begin
  RequestChange;
  Meta := ABData[abNext] as TMemForeignKeyMetadataItem;
  case RT of
    fkTable: Meta.TableReferer := Referer;
    fkIndex: Meta.IndexReferer := Referer;
  else
    Assert(false);
  end;
end;

procedure TMemDbForeignKeyMetadata.SetReferred(RT: TMemDBFKRefType; Referred: string);
var
  Meta: TMemForeignKeyMetadataItem;
begin
  RequestChange;
  Meta := ABData[abNext] as TMemForeignKeyMetadataItem;
  case RT of
    fkTable: Meta.TableReferred := Referred;
    fkIndex: Meta.IndexReferred := Referred;
  else
    Assert(false);
  end;
end;

{$ENDIF}

{ TMemDBEntityProxy }

constructor TMemDBEntityProxy.Create;
begin
  inherited;
  DLItemInitObj(self, @FSiblings);
  //FLock, list, init done elsewhere.
end;

{ TMemDBDatabasePersistent }

procedure TMemDBDatabasePersistent.PreCommit(const TId: TTransactionId; Reason: TMemDBTransReason);
var
  i, j: integer;
  ProxI, ProxJ: TMemDBEntityProxy;
  ObjI, ObjJ: TMemDBEntity;
  EntityList: TReffedList;
  IAdded, IChanged, IDeleted, INull: boolean;
  JAdded, JChanged, JDeleted, JNull: boolean;
  ICur, INxt, JCur, JNxt: TMemDbStreamable;
  ILat, JLat: TMemEntityMetadataItem;
  SelType: TABSelType;
begin
  //Pre-commit check and no-pin goes from top to bottom.
  inherited;
  EntityList := AssembleEntityList;
  try
    //OK. Check no dup object names,
    for i := 0 to Pred(EntityList.Count) do
    begin
      ProxI := EntityList[i] as TMemDBEntityProxy;
      ObjI := ProxI.Proxy as TMemDbEntity;
      ICur := ObjI.META_PinCurrent(Tid, pinFinalCheck);
      INxt := ObjI.META_GetNext(Tid);
      ChangeFlagsFromPinned(ICur, INxt, IAdded, IChanged, IDeleted, INull);
      if not (INull or IDeleted) then
      begin
        for j := Succ(i) to Pred(EntityList.Count) do
        begin
          ProxJ := EntityList[j] as TMemDBEntityProxy;
          ObjJ := ProxJ.Proxy as TMemDBEntity;
          JCur := ObjJ.META_PinCurrent(Tid, pinFinalCheck);
          JNxt := ObjJ.META_GetNext(Tid);
          ChangeFlagsFromPinned(JCur, JNxt, JAdded, JChanged, JDeleted, JNull);
          if not (JNull or JDeleted) then
          begin
            ILat := ObjI.META_GetPinLatest(Tid, SelType, pinFinalCheck) as TMemEntityMetadataItem;
            JLat := ObjJ.META_GetPinLatest(Tid, SelType, pinFinalCheck) as TMemEntityMetadataItem;
            if ILat.EntityName = JLat.EntityName then
              raise EMemDBConsistencyException.Create(S_COMMMIT_CONSISTENCY_OBJS);
          end;
        end;
      end;
    end;

    //Then pre-commit tables.
    for i := 0 to Pred(EntityList.Count) do
    begin
      ProxI := EntityList[i] as TMemDBEntityProxy;
      ObjI := ProxI.Proxy as TMemDbEntity;
      if ObjI is TMemDBTablePersistent then
        ObjI.PreCommit(Tid, Reason);
    end;

    //Then pre-commit FKs.
    for i := 0 to Pred(EntityList.Count) do
    begin
      ProxI := EntityList[i] as TMemDBEntityProxy;
      ObjI := ProxI.Proxy as TMemDbEntity;
      Assert((ObjI is TMemDBTablePersistent) or (ObjI is TMemDBForeignKeyPersistent));
      if ObjI is TMemDBForeignKeyPersistent then
        ObjI.PreCommit(Tid, Reason);
    end;
  finally
    EntityList.Release;
  end;
end;


procedure TMemDBDatabasePersistent.LookaheadHelper(Stream: TStream;
                                                  var ChangeType: TMemDBEntityChangeType;
                                                  var EntityName: string);
var
  Pos: Int64;
  Tag: TMemStreamTag;
  StrChangeType: TMDBChangeType;
begin
  Pos := Stream.Position;
  Tag := RdTag(Stream);
  case Tag of
    mstTableStart:
    begin
      Tag := RdTag(Stream);
      case Tag of
        mstTableUnchangedName:
        begin
          EntityName := RdStreamString(Stream);
          ChangeType := mectChangedDataTable;
        end;
        mstDblBufferedStart:
        begin
          StrChangeType := RdStreamChangeType(Stream);
          case StrChangeType of
            mctAdd, mctChange, mctDelete:
            begin
              Tag := RdTag(Stream);
              if Tag <> mstTableMetadataStart then
                raise EMemDBException.Create(S_DATABASE_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
              Tag := RdTag(Stream);
              if Tag <> mstEntityMetadataStart then
                raise EMemDBException.Create(S_DATABASE_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
              //New entity name for add, existing name for change, delete.
              EntityName := RdStreamString(Stream);
              if StrChangeType = mctAdd then
                ChangeType := mectNewTable
              else
                ChangeType := mectChangedDeletedEntity;
            end;
          else
            raise EMemDBException.Create(S_DATABASE_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
          end;
        end;
      else
        raise EMemDBException.Create(S_DATABASE_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
      end;
    end;
    mstFkStart:
    begin
      Tag := RdTag(Stream);
      if Tag <> mstDblBufferedStart then
        raise EMemDBException.Create(S_DATABASE_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
      StrChangeType := RdStreamChangeType(Stream);
      case StrChangeType of
        mctAdd, mctChange, mctDelete:
        begin
          Tag := RdTag(Stream);
          if Tag <> mstFKMetadataStart then
            raise EMemDBException.Create(S_DATABASE_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
          Tag := RdTag(Stream);
          if Tag <> mstEntityMetadataStart then
            raise EMemDBException.Create(S_DATABASE_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
          //New entity name for add, existing name for change, delete.
          EntityName := RdStreamString(Stream);
          if StrChangeType = mctAdd then
            ChangeType := mectNewFK
          else
            ChangeType := mectChangedDeletedEntity;
        end;
      else
        raise EMemDBException.Create(S_DATABASE_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
      end;
    end;
  else
    raise EMemDBException.Create(S_DATABASE_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
  end;
  Stream.Seek(Pos, TSeekOrigin.soBeginning);
end;

procedure TMemDBDatabasePersistent.ToJournal(const Tid: TTransactionId; Stream: TStream);
var
  EntityList: TReffedList;
  Prox: TMemDBEntityProxy;
  Entity: TMemDBEntity;
  i: integer;
begin
  WrTag(Stream, mstDBStart);
  EntityList := AssembleEntityList;
  try
    for i := 0 to Pred(EntityList.Count) do
    begin
      Prox := EntityList.Items[i] as TMemDBEntityProxy;
      Entity := Prox.Proxy as TMemDBEntity;
      Entity.ToJournal(Tid, Stream);
    end;
  finally
    EntityList.Release;
  end;
  WrTag(Stream, mstDBEnd);
end;

procedure TMemDBDatabasePersistent.ToScratch(const PseudoTid:TTransactionId; Stream: TStream);
var
  EntityList: TReffedList;
  Proxy: TMemDbEntityProxy;
  Entity: TMemDbEntity;
  i: integer;
begin
  WrTag(Stream, mstDBStart);
  EntityList := AssembleEntityList;
  try
    for i := 0 to Pred(EntityList.Count) do
    begin
      Proxy := EntityList.Items[i] as TMemDbEntityProxy;
      Entity := Proxy.Proxy as TMemDbEntity;
      Entity.ToScratch(PseudoTid, Stream);
    end;
  finally
    EntityList.Release;
  end;
  WrTag(Stream, mstDBEnd);
end;

procedure TMemDBDatabasePersistent.FromJournal(const PseudoTid: TTransactionId; Stream: TStream);
var
  StrPos: int64;
  NxtTag: TMemStreamTag;
  ChangeType: TMemDBEntityChangeType;
  EntityName: string;
  DBU: TMemDBEntity;
  DBUMeta: TMemDBStreamable;
begin
  ExpectTag(Stream, mstDBStart);
  StrPos := Stream.Position;
  NxtTag := RdTag(Stream);
  while NxtTag <> mstDBEnd do
  begin
    Stream.Seek(StrPos, TSeekOrigin.soBeginning);
    LookaheadHelper(Stream, ChangeType, EntityName);
    case ChangeType of
      mectNewTable,
      mectNewFK:
      begin
        //In these cases, lookahead helper should be returning the
        //new entity name.
        DBU := EntitiesByName(MakeLatestBufSelector(PseudoTid), EntityName, pinEvolve);
        if Assigned(DBU) then
        begin
          DBU.FProxy.Release;
          raise EMemDbException.Create(S_JOURNAL_REPLAY_DUP_INST_ENTITY);
        end;

        if ChangeType = mectNewTable then
          DBU := TMemDBTable.Create
        else
          DBU := TMemDBForeignKey.Create;

        DBU.Init(PseudoTid, self, EntityName, false);
        DBU.FromJournal(PseudoTid, Stream); //should inc ref.
        Assert(Assigned(DBU.Metadata.GetNext(PseudoTid)));
        Assert(not (DBU.Metadata.GetNext(PseudoTid) is TMemDeleteSentinel));
        DBU.Proxy.Release;
      end;
      mectChangedDeletedEntity, mectChangedDataTable:
      begin
        //In these cases, lookahead helper should be returning the
        //current entity name.

        //Not all the renaming is checked here, we'll do that in the pre-commit check.
        DBU := EntitiesByName(MakeCurrentBufSelector(PseudoTid), EntityName, pinEvolve);
        if Assigned(DBU) then
        begin
          DBU.FromJournal(PseudoTid, Stream);
          DBUMeta := DBU.Metadata.GetNext(PseudoTid);
          Assert(Assigned(DBUMeta) = (ChangeType = mectChangedDeletedEntity));
          DBU.FProxy.Release;
        end
        else
          raise EMemDBException.Create(S_JOURNAL_REPLAY_NAMES_INCONSISTENT);
      end
    else
      raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
    end;
    StrPos := Stream.Position;
    NxtTag := RdTag(Stream);
  end;
end;

procedure TMemDBDatabasePersistent.FromScratch(const PseudoTid: TTransactionId; Stream: TStream);
var
  StrPos: int64;
  NxtTag: TMemStreamTag;
  ChangeType: TMemDBEntityChangeType;
  EntityName: string;
  DBU: TMemDBEntity;
  LatestSel: TBufSelector;
begin
  ExpectTag(Stream, mstDBStart);
  StrPos := Stream.Position;
  NxtTag := RdTag(Stream);
  while NxtTag <> mstDBEnd do
  begin
    Stream.Seek(StrPos, TSeekOrigin.soBeginning);
    LookaheadHelper(Stream, ChangeType, EntityName);
    case ChangeType of
      mectNewTable,
      mectNewFK:
      begin
        LatestSel := MakeLatestBufSelector(PseudoTid);
        DBU := EntitiesByName(LatestSel, EntityName, pinEvolve);
        if Assigned(DBU) then
        begin
          DBU.FProxy.Release;
          raise EMemDbException.Create(S_JOURNAL_REPLAY_DUP_INST_ENTITY);
        end;

        if ChangeType = mectNewTable then
          DBU := TMemDbTable.Create
        else
          DBU := TMemDBForeignKey.Create;

        DBU.Init(PseudoTid, self, EntityName, false);
        DBU.FromScratch(PseudoTid, Stream); //Should inc ref on proxy for us.
        Assert(Assigned(DBU.Metadata.GetNext(PseudoTid)));
        Assert(not (DBU.Metadata.GetNext(PseudoTid) is TMemDeleteSentinel));
        DBU.Proxy.Release;
      end
    else
      raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
    end;
    //Variable handling based on lookahead results.
    StrPos := Stream.Position;
    NxtTag := RdTag(Stream);
  end;
end;

function TMemDBDatabasePersistent.AnyChangesForTid(const TId: TTransactionId): boolean;
var
  EntityList: TReffedList;
  Proxy: TReffedProxy;
  Entity: TMemDbEntity;
  i: integer;
begin
  EntityList := AssembleEntityList;
  try
    result := false;
    for i := 0 to Pred(EntityList.Count) do
    begin
      Proxy := EntityList.Items[i] as TReffedProxy;
      Entity := Proxy.Proxy as TMemDBEntity;
      result := result or Entity.AnyChangesForTid(Tid);
    end;
  finally
    EntityList.Release;
  end;
end;

function TMemDBDatabasePersistent.AnyChanges(const TId: TTransactionId): boolean;
var
  EntityList: TReffedList;
  Proxy: TReffedProxy;
  Entity: TMemDbEntity;
  i: integer;
begin
  EntityList := AssembleEntityList;
  try
    result := false;
    for i := 0 to Pred(EntityList.Count) do
    begin
      Proxy := EntityList.Items[i] as TReffedProxy;
      Entity := Proxy.Proxy as TMemDBEntity;
      result := result or Entity.AnyChanges(Tid);
    end;
  finally
    EntityList.Release;
  end;
end;

procedure TMemDBDatabasePersistent.Commit(const TId: TTransactionId; Reason: TMemDbTransReason);
var
  EntityList: TReffedList;
  Proxy: TMemDBEntityProxy;
  Entity: TMemDbEntity;
  i: integer;
begin
  inherited;
  EntityList := AssembleEntityList;
  try
    for i := 0 to Pred(EntityList.Count) do
    begin
      Proxy := EntityList.Items[i] as TMemDBEntityProxy;
      Entity := Proxy.Proxy as TMemDBEntity;
      Entity.Commit(Tid, Reason);
    end;
  finally
    EntityList.Release;
  end;
end;

procedure TMemDBDatabasePersistent.Rollback(const TId: TTransactionId; Reason: TMemDbTransReason);
var
  EntityList: TReffedList;
  Proxy: TMemDBEntityProxy;
  Entity: TMemDbEntity;
  i: integer;
begin
  inherited;
  EntityList := AssembleEntityList;
  try
    for i := 0 to Pred(EntityList.Count) do
    begin
      Proxy := EntityList.Items[i] as TMemDBEntityProxy;
      Entity := Proxy.Proxy as TMemDBEntity;
      Entity.Rollback(Tid, Reason);
    end;
  finally
    EntityList.Release;
  end;
end;

function TMemDBDatabasePersistent.AssembleEntityList: TReffedList;
var
  Proxy: TMemDBEntityProxy;
begin
  //NB. Do not need PinForUnorderedContainer here, because we get all of them
  //at once. Some of them might be going away, but
  //later pins make them persistent enough for our transaction.
  result := TReffedList.Create;
  try
    FEntityLock.Acquire;
    try
      Proxy := FEntityList.FLink.Owner as TMemDBEntityProxy;
      while Assigned(Proxy) do
      begin
        result.AddNoRef(Proxy.AddRef);
        Proxy := Proxy.FSiblings.FLink.Owner as TMemDBEntityProxy;
      end;
    finally
      FEntityLock.Release;
    end;
  except
    on Exception do
    begin
      result.Release;
      raise;
    end;
  end;
end;

//TODO - Check all callers of this for addref/release.
function TMemDBDatabasePersistent.EntitiesByName(const AB:TBufSelector; Name: string; PinReason: TPinReason): TMemDBEntity;
var
  i: integer;
  Proxy: TMemDBEntityProxy;
  Entity: TMemDBEntity;
  List: TReffedList;
  SelType: TABSelType;
  EntMetaS: TMemDBStreamable;
  EntMetaItem: TMemEntityMetadataItem;
begin
  List := AssembleEntityList;
  try
    for i := 0 to Pred(List.Count) do
    begin
      //NB. Do not need PinForUnorderedContainer here, because we get all of them
      //at once, and use atomicity on the pins. The same is not the case for DB rows.
      Proxy := List.Items[i] as TMemDbEntityProxy;
      Assert(Assigned(Proxy.Proxy) and (Proxy.Proxy is TMemDbEntity));
      Entity := Proxy.Proxy as TMemDbEntity;
      case AB.SelType of
        abCurrent: EntMetaS := Entity.META_PinCurrent(AB.Tid, PinReason);
        abNext: EntMetaS := Entity.META_GetNext(AB.Tid);
        abLatest: EntMetaS := Entity.META_GetPinLatest(AB.Tid, SelType, PinReason);
      else
        Assert(false);
        EntMetaS := nil;
      end;
      if NotAssignedOrSentinel(EntMetaS) then
        continue;
      EntMetaItem := EntMetaS as TMemEntityMetadataItem;
      if EntMetaItem.EntityName = Name then
      begin
        result := Entity;
        Proxy.AddRef;
        exit;
      end;
    end;
  finally
    List.Release;
  end;
  result := nil;
end;

constructor TMemDBDatabasePersistent.Create;
begin
  inherited;
  FInterfaced := TMemDBAPIInterfacedObject.Create;
  FInterfaced.FAPIObjectRequest := HandleInterfacedObjRequest;
  //Don't hook AddRef / Release here, database always here until freed.
  FInterfaced.FParent := self;
  FEntityLock := TCriticalSection.Create;
  FCommitLock := TCriticalSection.Create;
  DLItemInitList(@FEntityList);
end;

destructor TMemDBDatabasePersistent.Destroy;
var
  PseudoTid: TTransactionId;
  EntityList: TReffedList;
  i: integer;
  Proxy: TMemDBEntityProxy;
  Entity: TMemDbEntity;
  NullStream: TNullStream;
  FKs, Del: boolean;
begin
  //OK, to check at some point:
  //All Ref reasons (transactions, API objects, pins etc deleted).

  //So all we have to do now is delete the metadata copies, do a commit,
  //and refs should return to zero.

  //TODO TODO - This is the slow path delete. just to check everything
  // clears down nicely. Make a fast path,
  //where we assume no concurrency issues, dec all ref counts until zero.
  NullStream := TNullStream.Create;
  try
    for FKs := True downto False do
    begin
      PseudoTid := TTransactionId.NewTransactionId(ilSerialisable);
      EntityList := AssembleEntityList;
      try
        for i := 0 to Pred(EntityList.Count) do
        begin
          Proxy := EntityList.Items[i] as TMemDBEntityProxy;
          Entity := Proxy.Proxy as TMemDBEntity;
          if FKs then
            Del := Entity is TMemDBForeignKeyPersistent
          else
            Del := Entity is TMemDBTablePersistent;
          if Del then
          begin
            Entity.Metadata.Delete(PseudoTid);
            Entity.ToJournal(PseudoTid, NullStream);
            Entity.PreCommit(PseudoTid, mtrUserOp);
            Entity.Commit(PseudoTid, mtrUserOp);
            //Should clear all pins and refs, assuming no oustanding txions.
          end;
        end;
      finally
        EntityList.Release;
      end;
    end;
  finally
    NullStream.Free;
  end;
  //I can see this assertion firing a few times.
  Assert(DlItemIsEmpty(@FEntityList));
  FInterfaced.Free;
  FEntityLock.Free;
  FCommitLock.Free;
  inherited;
end;

function TMemDBDatabasePersistent.HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;
begin
  case ID of
    APIInternalCommitRollback: result := TMemAPIDatabaseInternal.Create;
  else
    result := nil;
  end;
end;

function TMemDBDatabasePersistent.HandleAPITableRename(const Sel: TBufSelector; OldName, NewName: string): boolean;
var
  i: integer;
  EntityList: TReffedList;
  Proxy: TMemDBEntityProxy;
  Entity: TMemDBEntity;
begin
  result := false;
  EntityList := AssembleEntityList;
  try
    for i := 0 to Pred(EntityList.Count) do
    begin
      Proxy := EntityList.Items[i] as TMemDBEntityProxy;
      Entity := Proxy.Proxy as TMemDBEntity;
      if Entity is TMemDBForeignKeyPersistent then
        result := (Entity as TMemDBForeignKeyPersistent)
          .HandleAPITableRename(Sel, OldName, NewName) or result;
    end;
  finally
    EntityList.Release;
  end;
end;

procedure TMemDBDatabasePersistent.CheckAPITableDelete(const Sel: TBufSelector; TableName: string);
var
  i: integer;
  EntityList: TReffedList;
  Proxy: TMemDBEntityProxy;
  Entity: TMemDBEntity;
begin
  EntityList := AssembleEntityList;
  try
    for i := 0 to Pred(EntityList.Count) do
    begin
      Proxy := EntityList.Items[i] as TMemDBEntityProxy;
      Entity := Proxy.Proxy as TMemDBEntity;
      if Entity is TMemDBForeignKeyPersistent then
        (Entity as TMemDBForeignKeyPersistent)
        .CheckAPITableDelete(Sel, TableName);
    end;
  finally
    EntityList.Release;
  end;
end;

function TMemDBDatabasePersistent.HandleAPIIndexRename(const Sel: TBufSelector; IsoDeterminedTableName, OldName, NewName: string): boolean;
var
  i: integer;
  EntityList: TReffedList;
  Proxy: TMemDBEntityProxy;
  Entity: TMemDBEntity;
begin
  result := false;
  EntityList := AssembleEntityList;
  try
    for i := 0 to Pred(EntityList.Count) do
    begin
      Proxy := EntityList.Items[i] as TMemDBEntityProxy;
      Entity := Proxy.Proxy as TMemDBEntity;
      if Entity is TMemDBForeignKeyPersistent then
        result := (Entity as TMemDBForeignKeyPersistent)
        .HandleAPIIndexRename(Sel, IsoDeterminedTableName, OldName, NewName) or result;
    end;
  finally
    EntityList.Release;
  end;
end;


procedure TMemDBDatabasePersistent.CheckAPIIndexDelete(const Sel: TBufSelector; IsoDeterminedTableName, IndexName: string);
var
  i: integer;
  EntityList: TReffedList;
  Proxy: TMemDBEntityProxy;
  Entity: TMemDBEntity;
begin
  EntityList := AssembleEntityList;
  try
    for i := 0 to Pred(EntityList.Count) do
    begin
      Proxy := EntityList.Items[i] as TMemDBEntityProxy;
      Entity := Proxy.Proxy as TMemDBEntity;
      if Entity is TMemDBForeignKeyPersistent then
        (Entity as TMemDBForeignKeyPersistent)
        .CheckAPIIndexDelete(Sel, IsoDeterminedTableName, IndexName);
    end;
  finally
    EntityList.Release;
  end;
end;


{ TMemDblBufListHelper}

procedure TMemDblBufListHelper.SetABList(const AB: TAbSelType; New: TMemStreamableList);
begin
  case AB of
    abCurrent:
    begin
      FCurrentList.Release;
      FCurrentList := New.AddRef as TMemStreamableList;
    end;
    abNext:
    begin
      FNextList.Release;
      FNextList := New.AddRef as TMemStreamableList;
    end;
  else
    raise EMemDBInternalException.Create(S_BAD_AB_SELECTOR);
  end;
end;

function TMemDblBufListHelper.GetABList(const AB: TABSelType): TMemStreamableList;
begin
  case AB of
    abCurrent: result := FCurrentList;
    abNext: result := FNextList;
    abLatest:
    begin
      if Assigned(FNextList) then
        result := FNextList
      else
        result := FCurrentList;
    end;
  else
    raise EMemDBInternalException.Create(S_BAD_AB_SELECTOR);
  end;
end;

procedure TMemDblBufListHelper.Changing;
begin
  if Assigned(FOnChangeRequest) then
    FOnChangeRequest(Self);
end;

function TMemDblBufListHelper.GetNonDeletedCount: integer;
var
  RList: TMemStreamableList;
  Item: TMemDBStreamable;
  i: integer;
begin
  RList := List[abLatest];
  result := 0;
  if Assigned(RList) then
  begin
    for i := 0 to Pred(RList.Count) do
    begin
      Item := RList.Items[i];
      Assert(Assigned(Item));
      if not (Item is TMemDeleteSentinel) then
        Inc(result);
    end;
  end;
end;

function TMemDblBufListHelper.GetNonDeletedItem(Idx: integer): TMemDBStreamable;
var
  RList: TMemStreamableList;
  j: integer;
begin
  Assert(Idx >= 0);
  RList := List[abLatest];
  Assert(Assigned(RList));
  j := NdIndexToRawIndex(Idx);
  result := RList.Items[j];
  Assert(not (result is TMemDeleteSentinel));
end;

function TMemDblBufListHelper.GetCount: integer;
var
  RList: TMemStreamableList;
begin
  result := 0;
  RList := List[abLatest];
  if Assigned(RList) then
    result := RList.Count;
end;

function TMemDblBufListHelper.GetItem(Idx: integer): TMemDBStreamable;
var
  RList: TMemStreamableList;
begin
  RList := List[abLatest];
  Assert(Assigned(RList));
  result := RList.Items[Idx];
end;

function TMemDblBufListHelper.ModifyInternal(Index: integer; var OldObj: boolean): TMemDBStreamable;
begin
  Changing;
  OldObj := Assigned(FCurrentList) and (Index < FCurrentList.Count);
  result := FNextList.Items[Index];
  Assert(not (result is TMemDeleteSentinel));
end;

function TMemDblBufListHelper.AddInternalNoRef(New: TMemDBStreamable; var OldObj: boolean; Index: integer = -1): integer; //returns raw index, takkes object.
var
  Item: TMemDBStreamable;
begin
  Changing;
  Assert(Assigned(New));
  Assert(not (New is TMemDeleteSentinel));
  if Index < 0 then
    result := FNextList.AddNoRef(New)
  else
  begin
    //Re-added old item.
    Item := FNextList.Items[Index];
    Assert(Item is TMemDeleteSentinel);
    Item.Release;
    FNextList.Items[Index] := New;
    result := Index;
  end;
end;

procedure TMemDblBufListHelper.DeleteInternal(Index: integer; var OldObj: boolean);
var
  OldCount, NewCount: integer;
  Item: TMemDBStreamable;
begin
  Changing;
  if Assigned(FCurrentList) then
    OldCount := FCurrentList.Count
  else
    OldCount := 0;
  Assert(Assigned(FNextList));
  NewCount := FNextList.Count;
  Assert(NewCount >= OldCount);
  Item := FNextList.Items[Index];
  Item.Release;
  if Index < OldCount then
  begin
    //Replace with a delete sentinel.
    Item := TMemDeleteSentinel.Create;
    FNextList.Items[Index] := Item;
    OldObj := true;
  end
  else
  begin
    //Remove list entry.
    FNextList.Items[Index] := nil;
    FNextList.Pack;
    OldObj := false;
  end;
end;

function TMemDblBufListHelper.Modify(Index: integer): TMemDBStreamable;
var
  OldObj: boolean;
begin
  result := ModifyInternal(Index, OldObj);
end;

procedure TMemDblBufListHelper.Delete(Index: integer);
var
  OldObj: boolean;
begin
  DeleteInternal(Index, OldObj);
end;

function TMemDblBufListHelper.AddNoRef(New: TMemDBStreamable; Index: integer = -1): integer;
var
  OldObj: boolean;
begin
  result := AddInternalNoRef(New, OldObj, Index);
end;

function TMemDblBufListHelper.ModifyND(Index: integer): TMemDBStreamable;
begin
  result := Modify(NdIndexToRawIndex(Index));
end;

function TMemDblBufListHelper.AddNDNoRef(New: TMemDBStreamable; Index: integer = -1): integer;
begin
  Assert(Index < 0);
  result := RawIndexToNdIndex(AddNoRef(New, Index));
end;

procedure TMemDblBufListHelper.DeleteND(Index: integer);
begin
  Delete(NdIndexToRawIndex(Index));
end;

procedure TMemDblBufListHelper.GetChildValidPtrs(Idx: integer; var Current: TMemDBStreamable; var Next: TMemDBStreamable);
var
  CCount, NCount: integer;
begin
  CCount := 0;
  NCount := 0;
  Current := nil;
  Next := nil;
  if Assigned(FCurrentList) then
    CCount := FCurrentList.Count;
  if Assigned(FNextList) then
    NCount := FNextList.Count;
  //NCount can be less than CCount if all old, but is zero.
  //CCount can be less than NCount if all new.
  Assert((NCount >= CCount) or (NCount = 0));
  if Idx < NCount then
    Next := FNextList.Items[Idx];
  if Idx < CCount then
    Current := FCurrentList.Items[Idx];
  //Check the ordering of our arrays is correct.
  Assert(Assigned(Current) = (Idx < CCount));
  if Assigned(Current) then
    Assert(not (Current is TMemDeleteSentinel));
  Assert(Assigned(Next) = (Idx < Ncount));
  if Assigned(Next) and (Next is TMemDeleteSentinel) then
    Assert(Idx < CCount);
end;

function TMemDblBufListHelper.GetChildAdded(Idx: integer): boolean;
var
  Current, Next: TMemDBStreamable;
begin
  GetChildValidPtrs(Idx, Current, Next);
  result := NotAssignedOrSentinel(Current) and AssignedNotSentinel(Next);
end;

function TMemDblBufListHelper.GetChildModified(Idx: integer): boolean;
var
  Current, Next: TMemDBStreamable;
begin
  GetChildValidPtrs(Idx, Current, Next);
  result := AssignedNotSentinel(Current) and AssignedNotSentinel(Next);
end;

function TMemDblBufListHelper.GetChildDeleted(Idx: integer): boolean;
var
  Current, Next: TMemDBStreamable;
begin
  GetChildValidPtrs(Idx, Current, Next);
  result := AssignedNotSentinel(Current)
    and (Assigned(Next) and (Next is TMemDeleteSentinel));
end;

function TMemDblBufListHelper.GetChildNull(Idx: integer): boolean;
var
  Current, Next: TMemDBStreamable;
begin
  GetChildValidPtrs(Idx, Current, Next);
  result := NotAssignedOrSentinel(Current)
    and NotAssignedOrSentinel(Next);
  Assert(not (Current is TMemDeleteSentinel));
end;

//Can be called with index off the end of the list.
function TMemDblBufListHelper.RawIndexToNdIndex(Index: integer): integer;
var
  I, MaxI: integer;
  RList: TMemStreamableList;
begin
  RList := List[abLatest];
  Assert(Assigned(RList));
  result := Index;
  MaxI := Index;
  if RList.Count < MaxI then
    MaxI := RList.Count;

  Assert((Index >= RList.Count) or (not (RList.Items[Index] is TMemDeleteSentinel)));
  for I := 0 to Pred(MaxI) do
    if RList.Items[i] is TMemDeleteSentinel then
      Dec(result);
end;

function TMemDblBufListHelper.NdIndexToRawIndex(NdIndex: integer): integer;
var
  I: integer;
  RList: TMemStreamableList;
begin
  RList := List[abLatest];
  Assert(Assigned(RList));
  result := 0;
  I := Pred(0);
  while result < RList.Count do
  begin
    if not (RList.Items[result] is TMemDeleteSentinel) then
    begin
      Inc(I);
      if I = NdIndex then
      begin
        Assert(RawIndexToNdIndex(result) = NDIndex);
        exit;
      end;
    end;
    Inc(Result);
  end;
  //Given an index off the list!
  //However, can extrapolate!
  Inc(I); // Now count of nondeleted.
  result := NdIndex + RList.Count - I;
  Assert(RawIndexToNdIndex(result) = NDIndex);
end;

procedure TMemDblBufListHelper.ClearLists;
begin
  List[abCurrent] := nil;
  List[abNext] := nil;
end;

destructor TMemDblBufListHelper.Destroy;
begin
  ClearLists;
  inherited;
end;

end.
