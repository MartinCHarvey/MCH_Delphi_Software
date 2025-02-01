unit MemDBBuffered;
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
  In memory database.
  Classes and Datastructures which are A-B buffered, and create or consume journal
  item.
  Also main logic for table and DB handling.
}

interface

{$IFDEF DEBUG_DATABASE_DELETE}
{$DEFINE DEBUG_DATABASE_NAVIGATE}
{$ENDIF}

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  IndexedStore, MemDbStreamable, Classes, MemDBMisc, SyncObjs, Parallelizer,
  DLList;

type
  TMemDBTransReason = (mtrUserOp,
                       mtrReplayFromScratch,
                       mtrReplayFromJournal,
                       mtrUserOpMultiRollback);

  TMemDBAPIInterfacedObject = class;

{$IFDEF USE_TRACKABLES}
  TMemDBAPI = class(TTrackable)
{$ELSE}
  TMemDBAPI = class
{$ENDIF}
  protected
    FAssociatedTransaction: TObject;
    FInterfacedObject: TMemDBAPIInterfacedObject;
    FIsolation: TMDBIsolationLevel;
    procedure CheckReadWriteTransaction;
  public
    function GetApiObjectFromHandle(Handle: TMemDBHandle; ID: TMemDBAPIId; RaiseIfNoAPI: boolean = true): TMemDBAPI;
    function GetApiObject(ID: TMemDBAPIId; RaiseIfNoAPI: boolean = true): TMemDBAPI;
    destructor Destroy; override;
    property Isolation: TMDBIsolationLevel read FIsolation write FIsolation;
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

    procedure PutAPIObjectLocal(API:TMemDBAPI);
  public
    function GetAPIObject(Transaction: TObject; ID: TMemDBAPIId; RaiseIfNoAPI: boolean = true): TMemDBAPI;
    constructor Create;
    destructor Destroy; override;
    procedure CheckNoDanglingTransactionRefs(Transaction: TObject; CanThrow: boolean);
    procedure CheckNoAPIsBeforeDestruction(CanThrow: boolean);
    property Parent: TObject read FParent;
  end;

  //Object which can write changes between current and next version to journal.
{$IFDEF USE_TRACKABLES}
  TMemDBJournalCreator = class(TTrackable)
{$ELSE}
  TMemDBJournalCreator = class
{$ENDIF}
  protected
  public
    procedure ToJournal(FwdStream: TStream; InverseStream: TStream); virtual; abstract;
{$IFOPT C+}
    //N.B. This checks for "obvious" non-invertibility, (i.e. stupid typing errors).
    //however some things (like table deletes), where metadata delete -> all delete
    //need to be handled separately.

    //Is a class function just to make sure we don't rely on the internal state
    //of the class implementing it. Of course it's OK to create temporaries, and
    //check taks between streams.
    class procedure AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream); virtual; abstract;
{$ENDIF}
    procedure ToScratch(Stream: TStream); virtual; abstract;
    procedure FromJournal(Stream: TStream); virtual; abstract;
    procedure FromScratch(Stream: TStream); virtual; abstract;

    //In order of operation from one consistent state to another:
    //Check no A/B buffered changes. (Possibly debug only).
    //Then journal setup replay changes
    procedure CheckNoChanges;
    function AnyChangesAtAll: boolean; virtual;

    //Commit / rollback functions generally do not call up the inheritance
    //heirarchy, and are "flat"

    //Check A/B buffered changes consistent and logical.
    procedure PreCommit(Reason: TMemDBTransReason); virtual;
    //Save to journal here.
    //Make the changes.
    procedure Commit(Reason: TMemDbTransReason); virtual; abstract;
    procedure Rollback(Reason: TMemDbTransReason); virtual; abstract;
    procedure PostCommitCleanup(Reason: TMemDbTransReason); virtual;
    procedure PostRollbackCleanup(Reason: TMemDbTransReason); virtual;
  end;

  TMemDBConsistencyFlagged = class(TMemDbJournalCreator)
  protected
    function GetAdded: boolean; virtual; abstract;
    function GetChanged: boolean; virtual; abstract;
    function GetDeleted: boolean; virtual; abstract;
    function GetNull: boolean; virtual; abstract;

  public
    function AnyChangesAtAll: boolean; override;

    property Added: boolean read GetAdded;
    property Changed: boolean read GetChanged;
    property Deleted: boolean read GetDeleted;
    property Null: boolean read GetNull;
  end;

  TMemDBChangeable = class(TMemDBConsistencyFlagged)
  public
    procedure Delete; virtual; abstract;
    procedure RequestChange; virtual; abstract;
  end;

  TMemDBDoubleBuffered = class(TMemDBChangeable)
  private
  protected
    FCurrentCopy: TMemDBStreamable;
    FNextCopy: TMemDBStreamable;

    function GetAdded: boolean; override;
    function GetChanged: boolean; override;
    function GetDeleted: boolean; override;
    function GetNull: boolean; override;

{$IFOPT C+}
    function HasABData(AB: TABSelection): boolean;
    function GetABData(AB: TABSelection): TMemDBStreamable;
    procedure SetABData(AB: TABSelection; New: TMemDBStreamable);
{$ELSE}
    function HasABData(AB: TABSelection): boolean; inline;
    function GetABData(AB: TABSelection): TMemDBStreamable; inline;
    procedure SetABData(AB: TABSelection; New: TMemDBStreamable); inline;
{$ENDIF}
    //Can't use AB copies here because lists may be lower in object heirarchy.
    procedure CheckABStreamableListChange(Current, Next: TMemStreamableList);
    class procedure LookaheadHelperGeneric(Stream: TStream;var Copy: TMemDBStreamable);
    procedure LookaheadHelper(Stream: TStream);
    procedure LookaheadHelperOpt(Stream: TStream);
  public
    procedure Delete; override;
    procedure RequestChange; override;

    destructor Destroy; override;

    procedure ToJournal(FwdStream: TStream; InverseStream: TStream); override;
{$IFOPT C+}
    class procedure AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream); override;
{$ENDIF}
    procedure ToScratch(Stream: TStream); override;
    procedure FromJournal(Stream: TStream); override;
    procedure FromScratch(Stream: TStream); override;

    procedure Commit(Reason: TMemDbTransReason); override;
    procedure Rollback(Reason: TMemDbTransReason); override;

    //For situations where you need to create an setup initial current copy.
    //i.e. CreateTable, CreateForeignKey, InsertRow etc.
    procedure Init(Context: TObject; Name:string); virtual; abstract;

    property HasData[Sel: TAbSelection]:boolean read HasAbData;
    property ABData[Sel: TAbSelection]:TMemDBStreamable read GetAbData write SetAbData;
  end;

  //TODO - Combine TMemDBTableList and TMemDbIndexedList, once we're sure
  //indexed list not used in any other ways / places (queries?).

  //Indexed list is for large data lists (table and other datasets),
  //Not for metadata.

  TMemDBRow = class;

  //TODO - Collapse next two classes into one.
  TMemDBIndexedList = class(TMemDBJournalCreator)
  protected
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

    procedure ToScratch(Stream: TStream); override;
    procedure FromJournal(Stream: TStream); override;
    procedure FromScratch(Stream: TStream); override;

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
  TMemDBTableList = class(TMemDBIndexedList)
  protected

    //Handle child items when we know they're
    //TMemDBRow.
    procedure WriteRowToJournalV3(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);
    procedure CommitRollbackChangedRow(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);
    procedure RemoveEmptyRow(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);

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
    function MetaEditFirst: TEditRec;
    function MetaEditNext(EditRec: TEditRec):TEditRec;
    function MetaRemoveRowForEdit(EditRec: TEditRec): TMemDbRow;
    procedure MetaInsertRowAfterEdit(EditRec: TEditRec; Row: TMemDbRow);

    procedure ToJournal(FwdStream: TStream; InverseStream: TStream); override;
{$IFOPT C+}
    class procedure AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream); override;
{$ENDIF}

    function AnyChangesAtAll: boolean; override;

    procedure Commit(Reason: TMemDbTransReason); override;
    procedure Rollback(Reason: TMemDbTransReason); override;

    //Navigation and change functions for user API.
    function MoveToRowByIndexTag(Iso: TMDBIsolationLevel;
                                 TagStruct: PITagStruct;
                                 Cursor: TItemRec;
                                 Pos: TMemAPIPosition): TItemRec;
    function FindRowByIndexTag(Iso: TMDBIsolationLevel;
                               IndexDef: TMemIndexDef;
                               FieldDefs: TMemFieldDefs;
                               ITagStruct: PITagStruct;
                               const DataRecs: TMemDbFieldDataRecs): TItemRec;

    procedure ReadRowData(Row: TMemDBRow; Iso: TMDBIsolationLevel;
                          Fields: TMemStreamableList);
    //If row not assigned, then append.
    procedure WriteRowData(var Cursor: TItemRec; Iso: TMDBIsolationLevel;
                          Fields: TMemStreamableList);

    procedure DeleteRow(var Cursor: TItemRec; Iso: TMDBIsolationLevel);
  end;

  TMemDBRow = class(TMemDBDoubleBuffered)
  private
    FRowID: TGUID;
  protected
    FQuickRef: TDLEntry; //Quick-list link
    FOwningListHead: PDLEntry; //Quick list owner.
    FStoreBackRec: TItemRec;
    //Very unfortunate, but now using quick-lists, need the back ptr too.
  public
    constructor Create;
    destructor Destroy; override;

    procedure ToJournal(FwdStream: TStream; InverseStream: TStream); override;
{$IFOPT C+}
    class procedure AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream); override;
{$ENDIF}

    procedure ToScratch(Stream: TStream); override;
    procedure FromJournal(Stream: TStream); override;
    procedure FromScratch(Stream: TStream); override;

    procedure Commit(Reason: TMemDbTransReason); override;
    property RowId:TGUID read FRowId write FRowId;

    procedure Init(Context: TObject; Name:string); override;
  end;

  TMemDBEntityMetadata = class(TMemDbDoubleBuffered)
  protected
    function GetName(Sel: TABselection): string;
  public
    property Name[Sel: TABSelection]: string read GetName;
    procedure Init(Context: TObject; Name:string); override;
  end;

  //Internal maintenance operations at this level.
  TMemDBTableMetadata = class(TMemDBEntityMetadata)
  private
    //A/B buffer holds TMemTableMetadataItem
    procedure CheckABListChanges;
  public
    procedure FromJournal(Stream: TStream); override;
    procedure FromScratch(Stream: TStream); override;

    procedure PreCommit(Reason: TMemDbTransReason);override;
    procedure Commit(Reason: TMemDbTransReason); override;

    function FieldsByNames(AB: TAbSelection; const Names: TMDBFieldNames; var AbsIdxs: TFieldOffsets): TMemFieldDefs;
    function FieldByName(AB: TABSelection; const Name: string; var AbsIndex: integer): TMemFieldDef;
    function IndexByName(AB: TABSelection; const Name: string; var AbsIndex: integer): TMemIndexDef;
    procedure Init(Context: TObject; Name:string); override;
  end;

  TMemDBFKRefType = (fkTable, fkIndex);

  TMemDBForeignKeyMetadata = class(TMemDBEntityMetadata)
  protected
    //A/B buffer holds TMemForeignKeyMetadataItem
    function GetReferer(RT: TMemDBFKRefType; AB: TABSelection):string;
    function GetReferred(RT: TMemDBFKRefType; AB: TABSelection):string;
    procedure ConsistencyCheck;
  public
    procedure Init(Context: TObject; Name: string); override;

    property Referer[RT: TMemDBFKRefType; AB: TABSelection]: string read GetReferer;
    property Referred[RT: TMemDBFKRefType; AB: TABSelection]: string read GetReferred;

    procedure SetReferer(RT: TMemDBFKRefType; Referer: string);
    procedure SetReferred(RT: TMemDBFKRefType; Referred: string);
  end;

  //Inheritance here instead of containment - we don't
  //need to create another class yet.
  TMemDBDatabaseMetadata = class(TMemDbDoubleBuffered)
  end;

  TMemDBTablePersistent = class;
  TMemDBDatabasePersistent = class;

  TMemDBEntity = class(TMemDBChangeable)
  private
    FParentDB: TMemDBDatabasePersistent;
    FInterfaced: TMemDBAPIInterfacedObject;
  protected
    FMetadata: TMemDBEntityMetadata;

    function GetAdded: boolean; override;
    function GetChanged: boolean; override;
    function GetDeleted: boolean; override;
    function GetNull: boolean; override;
    function GetName(AB: TABSelection):string;
    function HasAbMetadata(AB: TABSelection):boolean;

    function HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;virtual;
    procedure CheckNoDanglingTransactionRefs(Transaction: TObject; CanThrow: boolean);
  public
    constructor Create;
    destructor Destroy; override;

    procedure Delete; override;
    procedure RequestChange; override;
    function AnyChangesAtAll: boolean; override;

    procedure PreCommit(Reason: TMemDBTransReason); override;
    procedure Commit(Reason: TMemDbTransReason); override;
    procedure Rollback(Reason: TMemDbTransReason); override;

    procedure GetStats(var Stats: TMemStats); virtual;

    property Metadata: TMemDBEntityMetadata read FMetadata;
    property HasMetaData[Sel: TAbSelection]:boolean read HasAbMetadata;
    property Name[AB: TABSelection]:string read GetName;
    procedure Init(Context: TObject; Name:string); virtual;
    property ParentDB: TMemDBDatabasePersistent read FParentDB;

    property Interfaced: TMemDBAPIInterfacedObject read FInterfaced;
  end;

  TMemDBFKMetaTags = record
    abBuf: TABSelection;
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

  TIndexChangeType = (ictAdded,
                      ictDeleted,
                      ictChangedFieldNumber);

  TIndexChangeSet = set of TIndexChangeType;

  TIndexChangeArray = array of TIndexChangeSet;

  TMemDblBufListHelper = class;

  TMemDBForeignKeyPersistent = class(TMemDBEntity)
  private
  protected
    function ReferringAddedHandler(Ref1, Ref2: Pointer):Pointer;
    function ReferredDeletedHandler(Ref1, Ref2: Pointer):Pointer;


    procedure SetupIndexes(var Meta: TMemDBFKMeta);
    procedure ClearIndexes(var Meta: TMemDbFkMeta);

    function FindIndexTag(Table: TMemDbTablePersistent;
                          IndexDef: TMemIndexDef;
                          var OutChangeset: TIndexChangeset;
                          SubIndexClass: TSubIndexClass): PITagStruct;

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
    procedure PreCommit(Reason: TMemDBTransReason); override;

    procedure ToJournal(FwdStream: TStream; InverseStream: TStream); override;
{$IFOPT C+}
    class procedure AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream); override;
{$ENDIF}
    procedure ToScratch(Stream: TStream); override;
    procedure FromJournal(Stream: TStream); override;
    procedure FromScratch(Stream: TStream); override;

    procedure CheckAPITableDelete(Iso: TMDBIsolationLevel; TableName: string);
    procedure CheckAPIIndexDelete(Iso: TMDBIsolationLevel; IsoDeterminedTableName, IndexName: string);
    function HandleAPITableRename(Iso: TMDBIsolationLevel; OldName, NewName: string): boolean;
    function HandleAPIIndexRename(Iso: TMDBIsolationLevel; IsoDeterminedTableName, OldName, NewName: string): boolean;
  end;

  TFieldChangeType = (fctAdded,
                      fctDeleted,
                      fctChangedFieldNumber);

  TFieldChangeSet = set of TFieldChangeType;
  TFieldChangeArray = array of TFieldChangeSet;

  //Could have had refcounted tags off metadata,
  //but this is more in keeping with with having the table internals properly
  //internal.

  //When data not modified, TagArray correspons to current copy of indices.
  //When modified, TagArray correspnds to index changeset indexing, i.e,
  //between modify and commit, added indexes at higher IdxIdx, and
  //delete sentinels for intermediately deleted indices.

  TCommitChangeMade = ( ccmAddedNewTagStructs,
                        ccmDeleteUnusedIndices,
                        ccmIndexesToTemporary);
  TCommitChangesMade = set of TCommitChangeMade;

  TMemDBTablePersistent = class(TMemDBEntity)
  private
    FCommitChangesMade: TCommitChangesMade;
    FTemporaryIndexLimit: integer; //Handle exceptions: 0 .. Lim-1 made temporary.
    FData: TMemDBTableList; //Indexed list of TMemDBRow.
    FIndexHelper, FFieldHelper: TMemDblBufListHelper;
    FIndexChangesets: TIndexChangeArray;
    FFieldChangesets: TFieldChangeArray;
    FTagDataList: TList;
    FIndexingChangeRequired, FDataChangeRequired: boolean;

    FEmptyList: TMemStreamableList;
  protected
    procedure GetCurrNxtMetaCopiesEx(var CC, NC: TMemTableMetadataItem;
                                           var CCFieldCount, CCIndexCount,
                                               NCFieldCount, NCINdexCount: integer);
    procedure GetCurrNxtMetaCopies(var CC, NC: TMemTableMetadataItem);
    function GetDataChanged: boolean;

    //IdxIdx here the same as changesets.
    //All errors here internal exceptions.
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
    procedure UpdateListHelpers;
    procedure DimensionChangesets;
    procedure ReGenChangesets;
    //Actions made in commit / rollback.
    procedure AddNewTagStructs;
    procedure RemoveNewlyAddedTagStructs;
    procedure RemoveOldTagStructs(DeleteAll: boolean = false);
    procedure DeleteUnusedIndices(Reason: TMemDBTransReason);
    procedure ReinstateDeletedIndices;
    procedure AdjustTableStructureForMetadata;
    function  ValidateIndexesParallelHandler(Ref1, Ref2: pointer):pointer;
    procedure ValidateIndexes(Reason: TMemDBTransReason);
    procedure CommitAdjustIndicesToTemporary(Reason: TMemDBTransReason);
    procedure CommitRestoreIndicesToPermanent(Reason: TMemDbTransReason);
    procedure RollbackRestoreIndicesToPermanent;

    procedure RevalidateEntireUserIndex(RawIndexDefNumber: integer);
    procedure CheckStreamedInTableRowCount;
    procedure CheckStreamedInRowStructure(Row: TMemDBRow; Ref1, Ref2: TObject;  TblList: TMemDbIndexedList);
    procedure CheckStreamedInRowIndexing;
    procedure CheckIndexForRow(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);

    procedure LookaheadHelper(Stream: TStream;
                              var MetadataInStream: boolean;
                              var DataInStream: boolean);

    property Data: TMemDBTableList read FData;
  public
    constructor Create;
    destructor Destroy; override;

    procedure ToJournal(FwdStream: TStream; InverseStream: TStream); override;
{$IFOPT C+}
    class procedure AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream); override;
{$ENDIF}
    procedure ToScratch(Stream: TStream); override;
    procedure FromJournal(Stream: TStream); override;
    procedure FromScratch(Stream: TStream); override;

    function AnyChangesAtAll: boolean; override;
    function HasTableData: boolean;

    procedure PreCommit(Reason: TMemDBTransReason); override;
    procedure Commit(Reason: TMemDbTransReason); override;
    procedure Rollback(Reason: TMemDbTransReason); override;

    procedure GetStats(var Stats: TMemStats); override;

    property DataChanged: boolean read GetDataChanged;
    property LayoutChangeRequired: boolean read FDataChangeRequired;
    property IndexHelper: TMemDblBufListHelper read FIndexHelper;
    property FieldHelper: TMemDblBufListHelper read FFieldHelper;
    property TagDataList:  TList read FTagDataList;
  end;

  TDBObjList = class(TList)
  protected
    function GetItem(Idx: integer): TMemDBEntity;
    procedure SetItem(Idx: integer; New: TMemDBEntity);
    function ParallelFree(Ref1, Ref2: Pointer): pointer;
  public
    destructor Destroy; override;
    property Items[Idx:integer]: TMemDBEntity read GetItem write SetItem;
  end;

  TMemDBEntityChangeType = (
    mectNewTable,
    mectNewFK,
    mectChangedDeletedEntity,
    mectChangedDataTable
  );

  TMemDBDatabasePersistent = class(TMemDBJournalCreator)
  private
  protected
    FUserObjs: TDBObjList;
    FInterfaced: TMemDBAPIInterfacedObject;
    procedure CleanupCommon(Reason: TMemDBTransReason);
    function HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;virtual;
    procedure LookaheadHelper(Stream: TStream;
                             var ChangeType: TMemDBEntityChangeType;
                             var EntityName: string);
    function PreCommitParallelHandler(Ref1, Ref2: pointer):pointer;
    procedure PreCommitParallel(Reason: TMemDBTransReason);
    function CommitParallelHandler(Ref1, Ref2: pointer):pointer;
    procedure CommitParallel(Reason: TMemDBTransReason);
  public
    function EntitiesByName(AB: TABSelection; Name: string; var Idx: integer): TMemDBEntity;

    procedure ToJournal(FwdStream: TStream; InverseStream: TStream); override;
{$IFOPT C+}
    class procedure AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream); override;
{$ENDIF}
    procedure ToScratch(Stream: TStream); override;
    procedure FromJournal(Stream: TStream); override;
    procedure FromScratch(Stream: TStream); override;

    function AnyChangesAtAll: boolean; override;

    //Replay journal here....
    //Check A/B buffered changes consistent and logical.
    procedure PreCommit(Reason: TMemDBTransReason); override;
    //Save to journal here.
    //Make the changes.
    procedure Commit(Reason: TMemDbTransReason); override;
    procedure Rollback(Reason: TMemDbTransReason); override;
    procedure PostCommitCleanup(Reason: TMemDbTransReason);  override;
    procedure PostRollbackCleanup(Reason: TMemDbTransReason);  override;

    constructor Create;
    destructor Destroy; override;

    procedure CheckNoDanglingTransactionRefs(Transaction: TObject; CanThrow: boolean);
    procedure CheckAPITableDelete(Iso: TMDBIsolationLevel; TableName: string);
    procedure CheckAPIIndexDelete(Iso: TMDBIsolationLevel; IsoDeterminedTableName, IndexName: string);
    function HandleAPITableRename(Iso: TMDBIsolationLevel; OldName, NewName: string): boolean;
    function HandleAPIIndexRename(Iso: TMDBIsolationLevel; IsoDeterminedTableName, OldName, NewName: string): boolean;

    procedure GetStats(var Stats: TMemStats);

    property Interfaced: TMemDBAPIInterfacedObject read FInterfaced;
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
    procedure SetABList(AB: TABSelection; New: TMemStreamableList);
    function GetABList(AB: TABSelection): TMemStreamableList;

    function GetNonDeletedCount: integer;
    function GetNonDeletedItem(Idx: integer): TMemDBStreamable;
    function GetCount: integer;
    function GetItem(Idx: integer): TMemDBStreamable;
    function ModifyInternal(Index: integer; var OldObj: boolean): TMemDBStreamable; //returns obj in list.
    procedure DeleteInternal(Index: integer; var OldObj: boolean); //deletes object as well
    function AddInternal(New: TMemDBStreamable; var OldObj: boolean; Index: integer = -1): integer; //returns raw index, takes ownership.

    procedure GetChildValidPtrs(Idx: integer; var Current: TMemDBStreamable; var Next: TMemDBStreamable);
    function GetChildAdded(Idx: integer): boolean;
    function GetChildModified(Idx: integer): boolean;
    function GetChildDeleted(Idx: integer): boolean;
    function GetChildNull(Idx: integer): boolean;
  public
    //These function always return the NEW object.
    //Be prepared for delete sentinels to appear in the list,
    //Hence the count will not necessarily change after delete calls.

    function Modify(Index: integer): TMemDBStreamable; //returns obj in list.
    //Don't allow arbitrary insertion, but can add back in previously deleted items.
    function Add(New: TMemDBStreamable; Index: integer = -1): integer; //returns raw index, takes ownership.
    procedure Delete(Index: integer); //deletes object as well

    function ModifyND(Index: integer): TMemDBStreamable; //returns obj in list.
    //Don't allow arbitrary insertion, but can add back in previously deleted items.
    function AddND(New: TMemDBStreamable; Index: integer = -1): integer; //returns raw index, takes ownership.
    procedure DeleteND(Index: integer); //deletes object as well

    function RawIndexToNdIndex(Index: integer): integer;
    function NdIndexToRawIndex(NdIndex: integer): integer;

    //And a helper function to prune delete sentinels if necessary.
    //If not both lists, expect only to have Current copy.
    procedure InvalidateLists; virtual;

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

    property List[AB: TABSelection]: TMemStreamableList read GetABList write SetABList;
    property OnChangeRequest: TNotifyEvent read FOnChangeRequest write FOnChangeRequest;
  end;

function AssignedNotSentinel(X: TMemDBStreamable): boolean; inline;
function NotAssignedOrSentinel(X: TMemDBStreamable): boolean; inline;

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

function SameLayoutFieldsSame(A,B: TMemStreamableList; FieldOffsets: TFieldOffsets;
                              AssertSameFormat: boolean = true): boolean;

function DiffLayoutFieldsSame(A: TMemStreamableList; AOffsets: TFieldOffsets;
                              B: TMemStreamableList; BOffsets: TFieldOffsets;
                              AssertSameFormat: boolean = true): boolean;

function AllFieldsZero(FieldList: TMemStreamableList;
                            const AbsFieldOffsets: TFieldOffsets): boolean;

const
  S_TABLE_DATA_CHANGED = 'Cannot change table field layout when uncommitted data changes.';
  S_FIELD_LAYOUT_CHANGED = 'Cannot change table data when uncommited field layout changes.';
  S_QUICK_LIST_DUPLICATE_INSERTION = 'Quick lookaside lists: Duplicate insertion.';

implementation

uses
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GlobalLog,
{$ENDIF}
  MemDBIndexing, MemDBApi, MemDB, SysUtils;

const
  S_REPLAY_CHECK_FAILED = 'Journal replay: pre or postcondition checks failed: ';
  S_JOURNAL_REPLAY_CHANGETYPE_DISAGREES = 'Journal replay failed, change type disagrees with data.';
  S_JOURNAL_REPLAY_BAD_INST = 'Journal replay failed, missing class instance or type. ';
  S_JOURNAL_REPLAY_DUP_INST = 'Journal replay failed, trying to overwrite duplicate changes.';
  S_CHANGESET_BAD_LISTS = 'Changeset has lists of items that are not consistent';
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
  S_FIELDS_NOT_SAME_AS_META = 'Streamed in field count not the same as metadata';
  S_FIELD_REARRANGEMENT_INCONSISTENT = 'Field data rearrangement inconsistent with metadata';
  S_FIELD_DIFFERENT_TYPE_FROM_META = 'Streamed in field type inconsistent with metadata';
  S_LIST_HELPER_INTERNAL = 'List helper internal exception';
  S_TABLE_WITH_NO_FIELDS_HAS_ROWS = 'Streamed in table has rows but no fields!';
  S_JOURNAL_REPLAY_NAMES_INCONSISTENT = 'Journal replay, names inconsistent.';
  S_FK_FIELD_MISSING = 'Foreign key relationship: required data is missing.';
  S_FK_REFERENCE_CIRCULAR = 'Foreign key relationship: A table cannot reference itself.';
  S_FK_REFERENCES_TABLE = 'Cannot delete table, it is referenced by a foreign key.';
  S_FK_REFERENCES_INDEX = 'Cannot delete index, it is referenced by a foreign key.';

  S_API_TRANSACTION_IS_RO = 'Write operation attempted in read-only transaction.';
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
  S_DBL_BUF_LOOKAHEAD_FAILED = 'Double buffered lookahead failed, tag: ';
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
  S_FIELD_LIST_BAD_FOR_COMPARISON = 'Bad field list object comparing sets of fields.';
  S_ZERO_LENGTH_FIELD_LIST_COMPARING = 'Comparing sets of fields, given a zero length field list.';
  S_DIFF_LENGTH_FIELD_LIST_COMPARING = 'Comparing sets of fields, given different length field lists.';
  S_ASYNC_INDEX_OP_FAILED = 'Asynchronous index build failed. Indexes are probably toast.';
  S_CURSOR_HAS_NO_ROW_AT_MODIFY = 'User mod of fields broken: no cursor assigned.';
  S_CURSOR_NOT_ASSIGNED_AT_DELETE = 'User delete of row broken: no cursor assigned';
  S_CURSOR_HAS_NO_ROW_AT_DELETE = 'User delete of row broken: no row associated with cursor';
  S_INTERNAL_UNSTREAM_EDIT = 'Internal indexing error during unstream operation';
  S_INTERNAL_META_EDIT = 'Internal indexing error during metadata processing';
  S_TRANSACTION_HAS_API_OBJECTS = 'Transaction has associated API objects. You should have freed them before commit/rollback';
  S_MINI_OR_COMMIT_DELETES_API_OBECTS = 'Mini-commit or commit deletes an object underlying some API''s. Free the API''s first please.';
  S_JOURNAL_ROW_NOT_NEW = 'Expected row to be newly added, but it isn''t.';

{ Misc Functions }

function OptimizationApplies(const OptLevel: TOptimizeLevel;
                             Reason: TMemDbTransReason): boolean;
begin
  result := (OptLevel = olAlways) or
  ((OptLevel >= olInitFirstTrans) and (Reason = mtrReplayFromScratch)) or
  ((OptLevel >= olInitAllTrans) and (Reason = mtrReplayFromJournal))
end;

function QuickBuildOptimizationApplies(Reason: TMemDBTransReason): boolean;
begin
  result := Optimizations.QuickBuildFirstTransaction and
    (Reason = TMemDbTransReason.mtrReplayFromScratch);
end;

function AssignedNotSentinel(X: TMemDBStreamable): boolean;
begin
  result := Assigned(X) and (not (X is TMemDeleteSentinel));
end;

function NotAssignedOrSentinel(X: TMemDBStreamable): boolean;
begin
  result := (not Assigned(X)) or (X is TMemDeleteSentinel);
end;

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

function SameLayoutFieldsSame(A,B: TMemStreamableList; FieldOffsets: TFieldOffsets;
                              AssertSameFormat: boolean = true): boolean;
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
    Assert((FA.FDataRec.FieldType = FB.FDataRec.FieldType)
      or not AssertSameFormat);
    if not DataRecsSame(FA.FDataRec, FB.FDataRec) then
    begin
      result := false;
      exit;
    end;
  end;
end;

function DiffLayoutFieldsSame(A: TMemStreamableList; AOffsets: TFieldOffsets;
                              B: TMemStreamableList; BOffsets: TFieldOffsets;
                              AssertSameFormat: boolean = true):boolean;
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
    Assert((FA.FDataRec.FieldType = FB.FDataRec.FieldType)
      or not AssertSameFormat);
    if not DataRecsSame(FA.FDataRec, FB.FDataRec) then
    begin
      result := false;
      exit;
    end;
  end;
end;

{ TMemDBAPI }

procedure TMemDBAPI.CheckReadWriteTransaction;
begin
  if not
    ((FAssociatedTransaction as TMemDBTransaction).Mode = amReadWrite) then
    raise EMemDBAPIException.Create(S_API_TRANSACTION_IS_RO);
end;

function TMemDBAPI.GetApiObjectFromHandle(Handle: TMemDBHandle; ID: TMemDBAPIId; RaiseIfNoAPI: boolean = true): TMemDBAPI;
begin
  if Assigned(Handle)
    and (TObject(Handle) is TMemDBAPIInterfacedObject) then
  begin
    result := TMemDBAPIInterfacedObject(Handle)
      .GetAPIObject(FAssociatedTransaction, ID, RaiseIfNoAPI);
  end
  else
    result := nil;
  if (not Assigned(result)) and RaiseIfNoAPI then
    raise EMemDBAPIException.Create(S_API_NO_SUCH_API_OBJECT);
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
  try
    Assert(FAPIObjects.Count = 0);
  except
    on E: EAssertionFailed do ;
  end;
  while FAPIObjects.Count > 0 do
    TObject(FAPIObjects.Items[0]).Free;
  FAPIObjects.Free;
  FAPIListLock.Free;
  inherited;
end;

function TMemDBAPIInterfacedObject.GetAPIObject(Transaction: TObject; ID: TMemDBAPIId; RaiseIfNoAPI: boolean = true): TMemDBAPI;
begin
  FAPIListLock.Acquire;
  try
    result := FAPIObjectRequest(Transaction, ID);
    if Assigned(result) then
    begin
      result.FAssociatedTransaction := Transaction;
      result.FInterfacedObject := self;
      if Assigned(Transaction) then
        result.FIsolation := (Transaction as TMemDBTransaction).Isolation
      else
        result.FIsolation := ilDirtyRead; //Internal API.
      FAPIObjects.Add(result)
    end
    else
    begin
      if RaiseIfNoAPI then
        raise EMemDBAPIException.Create(S_API_NO_SUCH_API_OBJECT);
    end;
  finally
    FAPIListLock.Release;
  end;
end;

procedure TMemDBAPIInterfacedObject.PutAPIObjectLocal(API:TMemDBAPI);
var
  Idx: integer;
begin
  FAPIListLock.Acquire;
  try
    Assert(Assigned(API));
    Assert(API.FInterfacedObject = self);
    Idx := FAPIObjects.IndexOf(API);
    if Idx >= 0 then
      FAPIObjects.Delete(Idx);
  finally
    FAPIListLock.Release;
  end;
end;

procedure TMemDBAPIInterfacedObject.CheckNoAPIsBeforeDestruction(CanThrow: boolean);
var
  Idx: integer;
  API:TMemDBAPI;
begin
  Idx := 0;
  FAPIListLock.Acquire;
  try
    while Idx <FAPIObjects.Count do
    begin
      API := TMemDBAPI(FAPIObjects.Items[Idx]);
      if CanThrow then
        raise EMemDBAPIException.Create(S_MINI_OR_COMMIT_DELETES_API_OBECTS)
      else
        Assert(false);
      API.Free;
    end;
  finally
    FAPIListLock.Release;
  end;
end;

procedure TMemDBAPIInterfacedObject.CheckNoDanglingTransactionRefs(Transaction: TObject; CanThrow: boolean);
var
  Idx: integer;
  API:TMemDBAPI;
begin
  FAPIListLock.Acquire;
  try
    Idx := 0;
    while Idx < FAPIObjects.Count do
    begin
      API := TMemDBAPI(FAPIObjects.Items[Idx]);
      if API.FAssociatedTransaction = Transaction then
      begin
        if CanThrow then
          raise EMemDBAPIException.Create(S_TRANSACTION_HAS_API_OBJECTS);
        API.Free;
      end
      else
        Inc(Idx);
    end;
    FAPIObjects.Pack;
  finally
    FAPIListLock.Release;
  end;
end;

{ TMemDBJournalCreator }

procedure TMemDBJournalCreator.PreCommit(Reason: TMemDBTransReason);
begin
end;

procedure TMemDBJournalCreator.PostCommitCleanup(Reason: TMemDbTransReason);
begin
end;

procedure TMemDBJournalCreator.PostRollbackCleanup(Reason: TMemDbTransReason);
begin
end;

function TMemDbJournalCreator.AnyChangesAtAll: boolean;
begin
  result := false;
end;

procedure TMemDBJournalCreator.CheckNoChanges;
begin
  if AnyChangesAtAll then
    raise EMemDBInternalException.Create(S_REPLAY_CHECK_FAILED + ClassName);
end;

{ TMemDBConsistencyFlagged }

function TMemDBConsistencyFlagged.AnyChangesAtAll: boolean;
begin
  result := inherited;
  result := result or Added or Changed or Deleted;
end;

{ TMemDBDoubleBuffered }

function TMemDBDoubleBuffered.GetAdded: boolean;
begin
  //Null to non null.
  result := NotAssignedOrSentinel(FCurrentCopy)
    and AssignedNotSentinel(FNextCopy);
  Assert(not (FCurrentCopy is TMemDeleteSentinel));
end;

function TMemDBDoubleBuffered.GetChanged: boolean;
begin
  //Non null to non Null
  result := AssignedNotSentinel(FCurrentCopy)
    and AssignedNotSentinel(FNextCopy);
  Assert(not (FCurrentCopy is TMemDeleteSentinel));
end;

function TMemDBDoubleBuffered.GetDeleted: boolean;
begin
  //Non null to delete sentinel.
  result := AssignedNotSentinel(FCurrentCopy)
    and (Assigned(FNextCopy) and (FNextCopy is TMemDeleteSentinel));
  Assert(not (FCurrentCopy is TMemDeleteSentinel));
end;

function TMemDBDoubleBuffered.GetNull: boolean;
begin
  //Null to Null.
  result := NotAssignedOrSentinel(FCurrentCopy) and NotAssignedOrSentinel(FNextCopy);
  Assert(not (FCurrentCopy is TMemDeleteSentinel));
end;

procedure TMemDBDoubleBuffered.Delete;
begin
  if AssignedNotSentinel(FNextCopy) then
  begin
    FNextCopy.Free;
    FNextCopy := nil;
  end;
  if not Assigned(FNextCopy) then
    FNextCopy := TMemDeleteSentinel.Create;
end;

procedure TMemDBDoubleBuffered.RequestChange;
begin
  if not Assigned(FNextCopy) then
  begin
    Assert(Assigned(FCurrentCopy)); //Let's not propagate NULL's thru the db.
    FNextCopy := TMemDBStreamable.DeepClone(FCurrentCopy);
  end
  else
    if FNextCopy is TMemDeleteSentinel then
      raise EMemDBException.Create(S_API_MODIFYING_META_FOR_DELETED_TABLE);
end;

destructor TMemDBDoubleBuffered.Destroy;
begin
  FCurrentCopy.Free;
  FNextCopy.Free;
  inherited;
end;

function TMemDBDoubleBuffered.HasABData(AB: TABSelection): boolean;
begin
  result := AssignedNotSentinel(ABData[AB]);
end;

function TMemDBDoubleBuffered.GetABData(AB: TABSelection): TMemDBStreamable;
begin
  case AB of
    abCurrent: result := FCurrentCopy;
    abNext: result := FNextCopy;
    abLatest:
    begin
      if Assigned(FNextCopy) then
        result := FNextCopy //Even if delete sentinel.
      else if Assigned(FCurrentCopy) then
        result := FCurrentCopy
      else
        result := nil;
    end
  else
    raise EMemDBInternalException.Create(S_BAD_AB_SELECTOR);
  end;
end;

procedure TMemDBDoubleBuffered.SetABData(AB: TABSelection; New: TMemDBStreamable);
begin
  case AB of
    abCurrent: FCurrentCopy := New;
    abNext: FNextCopy := New;
  else
    raise EMemDBInternalException.Create(S_BAD_AB_SELECTOR);
  end;
end;

procedure TMemDBDoubleBuffered.CheckABStreamableListChange(Current, Next: TMemStreamableList);
var
  Idx: integer;
begin
  if Assigned(Current) then
  begin
    //No delete sentinels or nils in current.
    for Idx := 0 to Pred(Current.Count) do
    begin
      if NotAssignedOrSentinel(Current.Items[idx]) then
        raise EMemDBException.Create(S_CHANGESET_BAD_LISTS);
    end;
  end;
  if Assigned(Next) then
  begin
    //No Nil's in next.
    for Idx := 0 to Pred(Next.Count) do
    begin
      if not Assigned(Next.Items[Idx]) then
        raise EMemDBException.Create(S_CHANGESET_BAD_LISTS);
    end;
    if Assigned(Current) then
    begin
      //Next has same or more items as current.
      if Next.Count < Current.Count then
        raise EMemDBException.Create(S_CHANGESET_BAD_LISTS);
      //No delete sentinels in *new* items unstreamed from disk?
      for Idx := Current.Count to Pred(Next.Count) do
      begin
        if Next.Items[Idx] is TMemDeleteSentinel then
          raise EMemDBException.Create(S_CHANGESET_BAD_LISTS);
      end;
    end;
  end;
end;

procedure TMemDBDoubleBuffered.ToJournal(FwdStream: TStream; InverseStream: TStream);
begin
  inherited;
  Assert(Added or Changed or Deleted);

  WrTag(FwdStream, mstDblBufferedStart);
  WrTag(InverseStream, mstDblBufferedStart);

  if NotAssignedOrSentinel(FCurrentCopy) then
  begin
    Assert(not (FNextCopy is TMemDeleteSentinel));
    Assert(Added);
    WrStreamChangeType(FwdStream, mctAdd);
    FNextCopy.ToStream(FwdStream);

    WrStreamChangeType(InverseStream, mctDelete);
    FNextCopy.ToStream(InverseStream);
  end
  else
  begin
    if FNextCopy is TMemDeleteSentinel then
    begin
      Assert(Deleted);
      WrStreamChangeType(FwdStream, mctDelete);
      FCurrentCopy.ToStream(FwdStream);

      WrStreamChangeType(InverseStream, mctAdd);
      FCurrentCopy.ToStream(InverseStream);
    end
    else
    begin
      Assert(Changed);
      WrStreamChangeType(FwdStream, mctChange);
      FCurrentCopy.ToStream(FwdStream);
      FNextCopy.ToStream(FwdStream);

      WrStreamChangeType(InverseStream, mctChange);
      FNextCopy.ToStream(InverseStream);
      FCurrentCopy.ToStream(InverseStream);
    end;
  end;
  WrTag(FwdStream, mstDblBufferedEnd);
  WrTag(InverseStream, mstDblBufferedEnd);
end;

procedure TMemDBDoubleBuffered.ToScratch(Stream: TStream);
begin
  inherited;
  Assert(not Assigned(FNextCopy));
  if AssignedNotSentinel(FCurrentCopy) then
  begin
    WrTag(Stream, mstDblBufferedStart);
    WrStreamChangeType(Stream, mctAdd);
    FCurrentCopy.ToStream(Stream);
    WrTag(Stream, mstDblBufferedEnd);
  end;
  //Else a NULL item, which we won't stream.
end;

class procedure TMemDBDoubleBuffered.LookaheadHelperGeneric(Stream: TStream;
                                                            var Copy: TMemDBStreamable);
var
  Pos: Int64;
  Tag: TMemStreamTag;
begin
  Pos := Stream.Position;
  Tag := RdTag(Stream);
  //No delete sentinels here.
  case Tag of
    mstStreamableListStart: Copy := TMemStreamableList.Create;
    mstTableMetadataStart: Copy := TMemTableMetadataItem.Create;
    mstFKMetadataStart: Copy := TMemForeignKeyMetadataItem.Create;
  else
    raise EMemDBException.Create(S_DBL_BUF_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
  end;
  Stream.Seek(Pos, TSeekOrigin.soBeginning);
end;


procedure TMemDBDoubleBuffered.LookaheadHelper(Stream: TStream);
begin
  LookaheadHelperGeneric(Stream, FNextCopy);
end;

//Optimize streaming of rows into FCurrentCopy, so never swizzle.
procedure TMemDBDoubleBuffered.LookaheadHelperOpt(Stream: TStream);
var
  Pos: Int64;
  Tag: TMemStreamTag;
begin
  Pos := Stream.Position;
  Tag := RdTag(Stream);
  //No delete sentinels here.
  case Tag of
    mstStreamableListStart: FCurrentCopy := TMemStreamableList.Create;
  else
    raise EMemDBException.Create(S_DBL_BUF_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
  end;
  Stream.Seek(Pos, TSeekOrigin.soBeginning);
end;

{$IFOPT C+}
type
  TMemDBDoubleBufferedDbg = class(TMemDBDoubleBuffered)
  public
    procedure Init(Context: TObject; Name:string); override;
  end;

procedure TMemDBDoubleBufferedDbg.Init(Context: TObject; Name:string);
begin
end;

class procedure  TMemDBDoubleBuffered.AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream);
var
  FwCurrCopy, FwNextCopy: TMemDBStreamable;
  ICurrCopy, INextCopy: TMemDBStreamable;
  FTag, ITag: TMemStreamTag;
  FwChange, IChange: TMDBChangeType;
begin
  inherited;
  //Assume mstDblBufferedStart already read.
  //On this function, we will *not* assume
  //This is to/from journal, not scratch, so follow streaming rules
  //accordingly.
  FwChange := RdStreamChangeType(FwdStream);
  IChange := RdStreamChangeType(InverseStream);
  case FwChange of
    mctAdd: Assert(IChange = mctDelete);
    mctChange: Assert(IChange = mctChange);
    mctDelete: Assert(IChange = mctAdd);
  else
    Assert(false);
  end;
  FwCurrCopy := nil;
  FwNextCopy := nil;
  ICurrCopy := nil;
  INextCopy := nil;
  try
    case FwChange of
      mctAdd: begin
        LookaheadHelperGeneric(FwdStream, FwNextCopy);
        FwNextCopy.FromStream(FwdStream);
      end;
      mctChange: begin
        LookaheadHelperGeneric(FwdStream, FwCurrCopy);
        FwCurrCopy.FromStream(FwdStream);
        LookaheadHelperGeneric(FwdStream, FwNextCopy);
        FwNextCopy.FromStream(FwdStream);
      end;
      mctDelete: begin
        LookaheadHelperGeneric(FwdStream, FwCurrCopy);
        FwCurrCopy.FromStream(FwdStream);
      end;
    else
      Assert(false);
    end;

    case IChange of
      mctAdd: begin
        LookaheadHelperGeneric(InverseStream, INextCopy);
        INextCopy.FromStream(InverseStream);
      end;
      mctChange: begin
        LookaheadHelperGeneric(InverseStream, ICurrCopy);
        ICurrCopy.FromStream(InverseStream);
        LookaheadHelperGeneric(InverseStream, INextCopy);
        INextCopy.FromStream(InverseStream);
      end;
      mctDelete: begin
        LookaheadHelperGeneric(InverseStream, ICurrCopy);
        ICurrCopy.FromStream(InverseStream);
      end;
    else
      Assert(false);
    end;

    case FwChange of
      mctAdd: begin
        Assert(FwNextCopy.Same(ICurrCopy));
        Assert(ICurrCopy.Same(FwNextCopy));
      end;
      mctChange: begin
        Assert(FwNextCopy.Same(ICurrCopy));
        Assert(ICurrCopy.Same(FwNextCopy));

        Assert(FwCurrCopy.Same(INextCopy));
        Assert(INextCopy.Same(FwCurrCopy));
      end;
      mctDelete: begin
        Assert(FwCurrCopy.Same(INextCopy));
        Assert(INextCopy.Same(FwCurrCopy));
      end;
    else
      Assert(false);
    end;

  finally
    FwCurrCopy.Free;
    FwNextCopy.Free;
    ICurrCopy.Free;
    INextCopy.Free;
  end;
  FTag := RdTag(FwdStream);
  ITag := RdTag(InverseStream);
  Assert(FTag = ITag);
  Assert(FTag = mstDblBufferedEnd);
end;
{$ENDIF}


procedure TMemDBDoubleBuffered.FromJournal(Stream: TStream);
var
  ChangeType: TMDBChangeType;
begin
  inherited;
  if Assigned(FNextCopy) then
    raise EMemDbInternalException.Create(S_JOURNAL_REPLAY_DUP_INST);
  ExpectTag(Stream, mstDblBufferedStart);
  ChangeType := RdStreamChangeType(Stream);
  case ChangeType of
    mctAdd:
    begin
      if AssignedNotSentinel(FCurrentCopy) then
        raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
      LookaheadHelper(Stream);
      FNextCopy.FromStream(Stream);
    end;
    mctChange:
    begin
      if NotAssignedOrSentinel(FCurrentCopy) then
        raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
      FCurrentCopy.CheckSameAsStream(Stream);
      LookaheadHelper(Stream);
      FNextCopy.FromStream(Stream);
    end;
    mctDelete:
    begin
      if NotAssignedOrSentinel(FCurrentCopy) then
        raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
      FCurrentCopy.CheckSameAsStream(Stream);
      FNextCopy := TMemDeleteSentinel.Create;
    end;
  else
    Assert(false);
  end;
  ExpectTag(Stream, mstDblBufferedEnd);
end;

procedure TMemDBDoubleBuffered.FromScratch(Stream: TStream);
var
  ChangeType: TMDBChangeType;
begin
  inherited;
  if Assigned(FNextCopy) then
    raise EMemDbInternalException.Create(S_JOURNAL_REPLAY_DUP_INST);
  ExpectTag(Stream, mstDblBufferedStart);

  ChangeType := RdStreamChangeType(Stream);
  case ChangeType of
    mctAdd:
    begin
      if AssignedNotSentinel(FCurrentCopy) then
        raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
      //Expect transaction reason is always mtrReplayFromScratch
      if QuickBuildOptimizationApplies(mtrReplayFromScratch)
        and (self is TMemDBRow) then
      begin
        LookaheadHelperOpt(Stream);
        FCurrentCopy.FromStream(Stream);
      end
      else
      begin
        LookaheadHelper(Stream);
        FNextCopy.FromStream(Stream);
      end;
    end;
  else
    raise EMemDBException.Create(S_JOURNAL_REPLAY_CHANGETYPE_DISAGREES);
  end;
  ExpectTag(Stream, mstDblBufferedEnd);
end;


procedure TMemDBDoubleBuffered.Commit;
begin
  inherited;
  if Assigned(FNextCopy) then
  begin
    FCurrentCopy.Free;
    if not (FNextCopy is TMemDeleteSentinel) then
      FCurrentCopy := FNextCopy
    else
    begin
      FNextCopy.Free;
      FCurrentCopy := nil;
    end;
  end;
  FNextCopy := nil;
end;

procedure TMemDBDoubleBuffered.Rollback;
begin
  inherited;
  FNextCopy.Free;
  FNextCopy := nil;
end;

{ TMemDBIndexedList }

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

{ TMemDBTableList }

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
  //Clear all the indexes in parallel before removing items (for speed).
  for Idx := 0 to Pred(FStore.IndexCount) do
  begin
    RV := FStore.IndexInfoByOrdinal(Idx, Tag, NodeClass);
    Assert(RV = rvOK);
    RV := FStore.DeleteIndex(Tag, true);
    Assert(RV = rvOK);
  end;
  RV := FStore.PerformAsyncActions(@MemDBXlateExceptions);
  Assert(RV = rvOK);
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

procedure TMemDBTableList.WriteRowToJournalV3(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);
var
  FwdStream, InverseStream: TStream;
begin
  FwdStream := Ref1 as TStream;
  InverseStream := Ref2 as TStream;
  Row.ToJournal(FwdStream, InverseStream);
end;


procedure TMemDbTableList.ToJournal(FwdStream: TStream; InverseStream: TStream);
begin
  if AnyChangesAtAll then
  begin
    WrTag(FwdStream, mstIndexedListStart);
    WrTag(InverseStream, mstIndexedListStart);
    ForEachChangedRow(WriteRowToJournalV3, FwdStream, InverseStream);
    WrTag(FwdStream, mstIndexedListEnd);
    WrTag(InverseStream, mstIndexedListEnd);
  end;
end;

{$IFOPT C+}
class procedure TMemDbTableList.AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream);
var
  FTag, ITag: TMemStreamTag;
  FPos, IPos: Int64;
begin
  //Assume already encountered mstIndexedListStart
  FPos := FwdStream.Position;
  IPos := InverseStream.Position;
  FTag := RdTag(FwdStream);
  ITag := RdTag(InverseStream);
  Assert(FTag = ITag);
  Assert(FTag in [DEPRECATED_mstRowStartV1, mstRowStartV2, mstIndexedListEnd]);

  while FTag in [DEPRECATED_mstRowStartV1, mstRowStartV2] do
  begin
    FwdStream.Seek(FPos, TSeekOrigin.soBeginning);
    InverseStream.Seek(IPos, TSeekOrigin.soBeginning);

    //For this specific call, rewind back to the start tag, so the called
    //function knows what format it is.
    TMemDBRow.AssertStreamsInverse(FwdStream, InverseStream);

    FPos := FwdStream.Position;
    IPos := InverseStream.Position;
    FTag := RdTag(FwdStream);
    ITag := RdTag(InverseStream);
    Assert(FTag = ITag);
    Assert(FTag in [DEPRECATED_mstRowStartV1, mstRowStartV2, mstIndexedListEnd]);
  end;
end;
{$ENDIF}

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
  if not QuickBuildOptimizationApplies(Reason) then
  begin
    ForEachChangedRow(CommitRollbackChangedRow, TObject(true), TObject(Reason));
    ForEachEmptyRow(RemoveEmptyRow, nil, nil);
  end
  else
  begin
    Assert(DlItemIsEmpty(@FRefChangedRows));
    Assert(DlItemIsEmpty(@FRefEmptyRows));
  end;
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

function TMemDBTableList.MoveToRowByIndexTag(Iso: TMDBIsolationLevel; TagStruct: PITagStruct;
                             Cursor: TItemRec;
                             Pos: TMemAPIPosition): TItemRec;
var
  RV: TIsRetVal;
  AB: TABSelection;
  Row: TMemDBRow;
begin
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  if Assigned(Cursor) then
  begin
    GLogLog(SV_INFO, 'MoveToRowByIndexTag, with starting row ' +
      MDBIsoStrings[Iso] + ' ' + MemAPIPositionStrings[Pos] +
      ' TagStruct: ' + IntToStr(NativeInt(TagStruct)));
  end
  else
  begin
    GLogLog(SV_INFO, 'MoveToRowByIndexTag, no starting row ' +
      MDBIsoStrings[Iso] + ' ' + MemAPIPositionStrings[Pos] +
      ' TagStruct: ' + IntToStr(NativeInt(TagStruct)));
  end;
{$ENDIF}
  RV := rvInternalError;
  case Pos of
    ptFirst:
    begin
      if Assigned(TagStruct) then
        RV := FStore.FirstByIndex(TagStruct, result)
      else
      begin
         result := FStore.GetAnItem;
         if Assigned(result) then
           RV := rvOK
         else
           RV := rvNotFound;
      end;
    end;
    ptLast:
    begin
      if Assigned(TagStruct) then
        RV := FStore.LastByIndex(TagStruct, result)
      else
      begin
         result := FStore.GetLastItem;
         if Assigned(result) then
           RV := rvOK
         else
           RV := rvNotFound;
      end;
    end;
    ptNext, ptPrevious:
    begin
      if not Assigned(Cursor) then
        raise EMemDBAPIException.Create(S_API_NEXT_PREV_REQUIRES_CURRENT_ROW);
      result := Cursor;
      if Assigned(TagStruct) then
      begin
        if Pos = ptNext then
          RV := FStore.NextByIndex(TagStruct, result)
        else
          RV := FStore.PreviousByIndex(TagStruct, result);
  {$IFDEF DEBUG_DATABASE_NAVIGATE}
          if RV = rvOK then
            GLogLog(SV_INFO, 'MoveToRowByIndexTag, move by 1 from previous cursor OK.')
          else
            GLogLog(SV_INFO, 'MoveToRowByIndexTag, move by 1 from previous cursor failed');
  {$ENDIF}
      end
      else
      begin
        if Pos = ptNext then
          RV := FStore.GetAnotherItem(result)
        else
          RV := FStore.GetPreviousItem(result);
      end;
    end;
  else
    Assert(false);
  end;
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  if RV = rvOK then
    GLogLog(SV_INFO, 'MoveToRowByIndexTag, move to row OK.')
  else
    GLogLog(SV_INFO, 'MoveToRowByIndexTag, move to row failed');
{$ENDIF}
  Assert(Assigned(result) = (rv = RVOK));
  if not (RV in [rvOK, rvNotFound]) then
  begin
    if RV = rvTagNotFound then
      raise EMemDBAPIException.Create(S_INDEX_NOT_YET_CREATED)
    else
      raise EMemDBInternalException.Create(S_API_NEXT_PREV_INTERNAL_ERROR);
  end;

  AB := IsoToAB(Iso);
  //Now, just because we have specified a current or most recent index
  //class does not mean that the current / next AB buffer will be valid,
  //it's just that they end up at the top (or maybe bottom) of the index...
  if Assigned(result) then
  begin
{$IFOPT C+}
    row := result.Item as TMemDBROw;
{$ELSE}
    row := TMemDbRow(result.Item);
{$ENDIF}
  end
  else
    row := nil;

  while Assigned(row) and NotAssignedOrSentinel(row.ABData[AB]) do
  begin
{$IFDEF DEBUG_DATABASE_NAVIGATE}
    GLogLog(SV_INFO, 'MoveToRowByIndexTag, row is sentinel, skip ' + MDBABStrings[AB]);
{$ENDIF}
    case Pos of
      ptFirst, ptNext:
      begin
        if Assigned(TagStruct) then
          RV := FStore.NextByIndex(TagStruct, result)
        else
          RV := FStore.GetAnotherItem(result);
      end;
      ptLast, ptPrevious:
      begin
        if Assigned(TagStruct) then
          RV := FStore.PreviousByIndex(TagStruct, result)
        else
          RV := FStore.GetPreviousItem(result);
      end;
    else
      Assert(false);
    end;
    Assert(Assigned(result) = (rv = RVOK));
    if RV = RVOK then
    begin
{$IFOPT C+}
      row := result.Item as TMemDBROw;
{$ELSE}
      row := TMemDbRow(result.Item);
{$ENDIF}
    end
    else
      row := nil;
    if not (RV in [rvOK, rvNotFound]) then
      raise EMemDBInternalException.Create(S_API_NEXT_PREV_INTERNAL_ERROR);
  end;
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  if Assigned(result) then
    GLogLog(SV_INFO, 'MoveToRowByIndexTag, final result found.')
  else
    GLogLog(SV_INFO, 'MoveToRowByIndexTag, final result failed.');
{$ENDIF}
end;

function TMemDBTableList.FindRowByIndexTag(Iso: TMDBIsolationLevel;
                                           IndexDef: TMemIndexDef;
                                           FieldDefs: TMemFieldDefs;
                                           ITagStruct: PITagStruct;
                                           const DataRecs: TMemDbFieldDataRecs): TItemRec;
var
  RV: TIsRetVal;
  SearchVal: TMemDBIndexNodeSearchVal;
  ABSel: TABSelection;
  i: integer;
begin
  if not Assigned(ITagStruct) then
    raise EMemDbInternalException.Create(S_API_SEARCH_NO_INDEX_SPECIFIED);
  if (Length(FieldDefs) <> Length(DataRecs)) or (Length(DataRecs) = 0) then
    raise EMemDbInternalException.Create(S_API_SEARCH_BAD_FIELD_COUNT);
    //Internal, should have been spotted before now.
  for i := 0 to Pred(Length(FieldDefs)) do
  begin
    if not (FieldDefs[i].FieldType in IndexableFieldTypes) then
      raise EMemDBInternalException.Create(S_INDEX_BAD_FIELD_TYPE);
    if DataRecs[i].FieldType <> FieldDefs[i].FieldType then
      raise EMemDBAPIException.Create(S_API_SEARCH_VAL_BAD_TYPE);
  end;
{$IFDEF DEBUG_DATABASE_NAVIGATE}
    GLogLog(SV_INFO, 'FindRowByIndexTag ' +
      MDBIsoStrings[Iso] + ' Tag: ' + IntToStr(NativeInt(ITagStruct)));
    for i := 0 to Pred(Length(DataRecs)) do
    begin
      if DataRecs[i].FieldType = ftUnicodeString then
      begin
        GLogLog(SV_INFO, 'FindRowByIndexTag SearchVal[' + InttoStr(i)+ ']: ' + DataRecs[i].sVal);
      end;
    end;
{$ENDIF}
  SearchVal := TMemDBIndexNodeSearchVal.Create;
  try
    SearchVal.FieldSearchVals := CopyDataRecs(DataRecs);
    RV := Store.FindByIndex(ITagStruct, SearchVal, result);
    if not (RV in [rvOK, rvNotFound]) then
    begin
      if RV = rvTagNotFound then
        raise EMemDBAPIException.Create(S_INDEX_NOT_YET_CREATED)
      else
        raise EMemDBInternalException.Create(S_API_SEARCH_INTERNAL);
    end;
  finally
    SearchVal.Free;
  end;
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  if RV = rvOK then
    GLogLog(SV_INFO, 'FindRowByIndexTag OK')
  else
    GLogLog(SV_INFO, 'FindRowByIndexTag failed');
{$ENDIF}
  Assert((RV = rvOk) = Assigned(result));
  if RV = rvOK then
  begin
    ABSel := IsoToAB(Iso);
    //We are searching on current metadata, and either current or latest
    //index. Do some quick checking of AB result based on isolation level.
    //We passed in some fields, so we should definitely have some result
    //fields in the required AB buffer.
    if NotAssignedOrSentinel((result.Item as TMemDBRow).ABData[ABSel]) then
      raise EMemDbInternalException.Create(S_API_SEARCH_INTERNAL);
  end
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

procedure TMemDBTableList.WriteRowData(var Cursor: TItemRec; Iso: TMDBIsolationLevel;
                      Fields: TMemStreamableList);
var
  Appending: boolean;
  CreateNext: boolean;
  IRet: TISRetVal;
  LPos: PDLEntry;
  Row: TMemDBRow;
  QLPos, QLHead: PDLEntry;
begin
  //If Row not assigned, then appending else modifying existing.
  Appending := not Assigned(Cursor);
  if Appending then
  begin
    Row := TMemDBRow.Create;
    Row.Init(nil, '');
  end
  else
  begin
    if not Assigned(Cursor.Item) then
      raise EMemDbInternalException.Create(S_CURSOR_HAS_NO_ROW_AT_MODIFY);
{$IFOPT C+}
    Row := Cursor.Item as TMemDBRow;
{$ELSE}
    Row := TMemDBRow(Cursor.Item);
{$ENDIF}
  end;

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

{ TMemDbDatabaseMetadata }

{ TMemDBEntity }

constructor TMemDBEntity.Create;
begin
  inherited;
  FInterfaced := TMemDBAPIInterfacedObject.Create;
  FInterfaced.FAPIObjectRequest := HandleInterfacedObjRequest;
  FInterfaced.FParent := Self;
end;

destructor TMemDBEntity.Destroy;
begin
  FInterfaced.Free;
  inherited;
end;

function TMemDBEntity.HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;
begin
  result := nil;
end;

procedure TMemDBEntity.CheckNoDanglingTransactionRefs(Transaction: TObject; CanThrow: boolean);
begin
  FInterfaced.CheckNoDanglingTransactionRefs(Transaction, CanThrow);
end;

function TMemDBEntity.HasAbMetadata(AB: TABSelection):boolean;
begin
  result := FMetadata.HasData[AB];
end;

function TMemDBEntity.GetName(AB: TABSelection): string;
begin
  result := FMetadata.Name[AB];
end;

function TMemDBEntity.GetAdded: boolean;
begin
  result := FMetadata.Added;
end;

function TMemDBEntity.GetChanged: boolean;
begin
  result := FMetadata.Changed;
end;

function TMemDBEntity.GetDeleted: boolean;
begin
  result := FMetadata.Deleted;
end;

function TMemDBEntity.GetNull: boolean;
begin
  result := FMetadata.Null;
end;

procedure TMemDBEntity.Delete;
begin
  FMetadata.Delete;
end;

procedure TMemDBEntity.RequestChange;
begin
  FMetadata.RequestChange;
end;

function TMemDBEntity.AnyChangesAtAll: boolean;
begin
  result := inherited;
  result := result or FMetadata.AnyChangesAtAll;
end;

procedure TMemDBEntity.PreCommit(Reason: TMemDBTransReason);
begin
  inherited;
  if Metadata.Deleted then
    FInterfaced.CheckNoAPIsBeforeDestruction(Reason = mtrUserOp); //Not mtrUserOpMultiRollback
end;

procedure TMemDBEntity.Commit(Reason: TMemDbTransReason);
begin
  inherited;
  FMetadata.Commit(Reason);
end;

procedure TMemDBEntity.Rollback(Reason: TMemDbTransReason);
begin
  inherited;
  FMetadata.Rollback(Reason);
end;

procedure TMemDBEntity.Init(Context: TObject; Name:string);
begin
  Assert(Assigned(Context));
  Assert(Context is TMemDBDatabasePersistent);
  FParentDB := Context as TMemDBDatabasePersistent;
  FMetadata.Init(Context, Name);
end;

procedure TMemDBEntity.GetStats(var Stats: TMemStats);
begin
  if not Assigned(Stats) then
    Stats := TMemDBEntityStats.Create;
  if AssignedNotSentinel(FMetadata.ABData[abLatest]) then
    (Stats as TMemDBEntityStats).AName := FMetadata.GetName(abLatest)
  else if AssignedNotSentinel(FMetadata.ABData[abCurrent]) then
    (Stats as TMemDBEntityStats).AName := FMetadata.GetName(abCurrent);
end;

{ TMemDBTablePersistent }

constructor TMemDBTablePersistent.Create;
begin
  inherited;
  FMetadata := TMemDbTableMetadata.Create;
  FData := TMemDBTableList.Create;
  FIndexHelper :=  TMemDblBufListHelper.Create;
  FIndexHelper.OnChangeRequest := HandleHelperChangeRequest;
  FFieldHelper := TMemDblBufListHelper.Create;
  FFieldHelper.OnChangeRequest := HandleHelperChangeRequest;
  FEmptyList := TMemStreamableList.Create;
  FTagDataList := TList.Create;
end;

destructor TMemDBTablePersistent.Destroy;
begin
  RemoveOldTagStructs(true);
  FMetadata.Free;
  FData.Free;
  FIndexHelper.Free;
  FFieldHelper.Free;
  FEmptyList.Free;
  FTagDataList.Free;
  inherited;
end;

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

procedure TMemDBTablePersistent.UpdateListHelpers;
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
  UpdateListHelpers;
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

procedure TMemDBTablePersistent.ToJournal(FwdStream: TStream; InverseStream: TStream);
begin
  if FMetadata.AnyChangesAtAll
    or (FData.AnyChangesAtAll and not LayoutChangeRequired) then
  begin
    WrTag(FwdStream, mstTableStart);
    WrTag(InverseStream, mstTableStart);
    if FMetadata.AnyChangesAtAll then
      FMetadata.ToJournal(FwdStream, InverseStream)
    else
    begin
      WrTag(FwdStream, mstTableUnchangedName);
      WrTag(InverseStream, mstTableUnchangedName);
      WrStreamString(FwdStream, FMetadata.Name[abLatest]);
      WrStreamString(InverseStream, FMetadata.Name[abLatest]);
    end;
    if FData.AnyChangesAtAll and not LayoutChangeRequired then
      FData.ToJournal(FwdStream, InverseStream);
    WrTag(FwdStream, mstTableEnd);
    WrTag(InverseStream, mstTableEnd);
  end;
end;

{$IFOPT C+}
class procedure TMemDbTablePersistent.AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream);
var
  FTag, ITag: TMemStreamTag;
  FPos, IPos: Int64;
  GotMeta, GotTblData: boolean;
  StrNameFwd, StrNameInverse: string;
begin
  //Assumes mstTableStart already read.
  FTag := mstTableStart;

  GotMeta := false;
  GotTblData := false;
  while true do
  begin

   FPos := FwdStream.Position;
   IPos := InverseStream.Position;
   FTag := RdTag(FwdStream);
   ITag := RdTag(InverseStream);
   Assert(FTag = ITag);
   Assert(FTag in [mstDblBufferedStart, mstTableUnchangedName, mstIndexedListStart, mstTableEnd]);

   //Metadata changes just streamed as raw dbl buffered changes.
   if FTag = mstDblBufferedStart then
   begin
     Assert(not GotMeta);
     GotMeta := true;
     TMemDBDoubleBuffered.AssertStreamsInverse(FwdStream, InverseStream);
   end
   else if FTag = mstTableUnchangedName then
   begin
     Assert(not GotMeta);
     GotMeta := true;
     StrNameFwd := RdStreamString(FwdStream);
     StrNameInverse := RdStreamString(InverseStream);
     Assert(StrNameFwd = StrNameInverse);
   end
   else if FTag = mstIndexedListStart then
   begin
     Assert(not GotTblData);
     GotTbldata := true;
     TMemDbTableList.AssertStreamsInverse(FwdStream, InverseStream);
   end
   else
     break;
  end;
  Assert(FTag = mstTableEnd);
end;
{$ENDIF}

procedure TMemDBTablePersistent.ToScratch(Stream: TStream);
begin
  WrTag(Stream, mstTableStart);
  FMetadata.ToScratch(Stream);
  FData.ToScratch(Stream);
  WrTag(Stream, mstTableEnd);
end;

procedure TMemDBTablePersistent.FromJournal(Stream: TStream);
var
  MetadataInStream, DataInStream: boolean;
  TblName: string;
begin
  //Assumption is that we're here, lookahead already having been performed.
  ExpectTag(Stream, mstTableStart);
  //Unfortunately, we don't have an up to date view of whether
  //layout change is required at this point, so just have
  //to unstream data anyway.
  LookaheadHelper(Stream, MetadataInStream, DataInStream);
  if MetadataInStream then
    FMetadata.FromJournal(Stream)
  else
  begin
    ExpectTag(Stream, mstTableUnchangedName);
    TblName := RdStreamString(Stream);
    if CompareStr(TblName, FMetadata.Name[abCurrent]) <> 0 then
      raise EMemDBInternalException.Create(S_JOURNAL_REPLAY_NAMES_INCONSISTENT);
  end;
  LookaheadHelper(Stream, MetadataInStream, DataInStream);
  if DataInStream then
    FData.FromJournal(Stream);
  ExpectTag(Stream, mstTableEnd);
end;

procedure TMemDBTablePersistent.FromScratch(Stream: TStream);
begin
  if not (FMetadata.Null and (FData.Store.Count = 0)) then
    raise EMemDBInternalException.Create(S_FROM_SCRATCH_REQUIRES_EMPTY_OBJ);

  ExpectTag(Stream, mstTableStart);
  FMetadata.FromScratch(Stream);
  FData.FromScratch(Stream);
  ExpectTag(Stream, mstTableEnd);
end;


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


procedure TMemDBTablePersistent.GetCurrNxtMetaCopiesEx(
                                           var CC, NC: TMemTableMetadataItem;
                                           var CCFieldCount, CCIndexCount,
                                               NCFieldCount, NCINdexCount: integer);

begin
  CC := nil;
  NC := nil;
  CCFieldCount := 0;
  CCIndexCount := 0;
  NCFieldCount := 0;
  NCINdexCount := 0;
  if AssignedNotSentinel(FMetadata.ABData[abCurrent]) then
    CC := FMetaData.ABData[abCurrent] as TMemTableMetadataItem;
  if AssignedNotSentinel(FMetadata.ABData[abNext]) then
    NC := FMetadata.ABData[abNext] as TMemTableMetadataItem;
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

procedure TMemDBTablePersistent.GetCurrNxtMetaCopies(var CC, NC: TMemTableMetadataItem);
var
  CCFieldCount, NCFIeldCount, CCIndexCount, NCIndexCount: integer;
begin
  GetCurrNxtMetaCopiesEx(CC,NC, CCFieldCount, NCFieldCount, CCIndexCount, NCIndexCount);
end;


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
  //UpdateListHelpers - already done at all points where metadata undergoes
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
  UpdateListHelpers;
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


procedure TMemDbTablePersistent.CheckStreamedInTableRowCount;
var
  LatestMeta: TMemTableMetadataItem;
  RV: TISRetVal;
  Res: TItemRec;
  Row: TMemDBRow;
begin
  LatestMeta := FMetadata.ABData[abLatest] as TMemTableMetadataItem;
  if LatestMeta.FieldDefs.Count = 0 then
  begin
    //If other checking has worked, expect not to have to go round this loop
    //at all. - Field change count should have implicit data changes.
    RV := FData.Store.FirstByIndex(MDBInternalIndexRowId.TagStructs[sicCurrent], Res);
    while (RV = rvOK) do
    begin
      Row := Res.Item as TMemDBRow;
      if not Row.Deleted then
        raise EMemDBConsistencyException.Create(S_TABLE_WITH_NO_FIELDS_HAS_ROWS);
      RV := FData.Store.NextByIndex(MDBInternalIndexRowId.TagStructs[sicCurrent], Res)
    end;
    Assert(RV = rvNotFound);
  end;
end;


procedure TMemDbTablePersistent.CheckStreamedInRowStructure(Row: TMemDBRow; Ref1, Ref2: TObject; TblList: TMemDbIndexedList);
var
  MostRecent: TMemTableMetadataItem;
  RowFields: TMemStreamableList;
  MetaFields: TMemStreamableList;
  RowField: TMemFieldData;
  MetaFieldDef: TMemFieldDef;
  i: integer;
  QuickBuildOptApplies: boolean;
begin
  QuickBuildOptApplies := Boolean(Ref2);

  Assert(AssignedNotSentinel(Ref1 as TMemDBStreamable));
  MostRecent := Ref1 as TMemTableMetadataItem;

  //Pattern of data and delete sentinels and types should be
  //the same in metadata as it is in row data.
  if not QuickBuildOptApplies then
  begin
    if not Assigned(Row.ABData[abNext]) then
      raise EMemDBInternalException.Create(S_INTERNAL_CHECKING_STRUCTURE);
  end
  else
  begin
    if not Assigned(Row.ABData[abCurrent]) then
      raise EMemDBInternalException.Create(S_INTERNAL_CHECKING_STRUCTURE);
  end;
  if not (Row.ABData[abNext] is TMemDeleteSentinel) then //Not deleted row.
  begin
    if not QuickBuildOptApplies then
      RowFields := Row.ABData[abNext] as TMemStreamableList
    else
      RowFields := Row.ABData[abCurrent] as TMemStreamableList;
    Assert(Assigned(MostRecent));
    Assert(Assigned(MostRecent.FieldDefs));
    Assert(MostRecent.FieldDefs is TMemStreamableList);
    MetaFields := MostRecent.FieldDefs as TMemStreamableList;
    //Same number of entries.
    if RowFields.Count <> MetaFields.Count then
      raise EMemDBConsistencyException.Create(S_FIELDS_NOT_SAME_AS_META);
    //Same structure with respect to delete sentinels.
    for i := 0 to Pred(RowFields.Count) do
    begin
      RowField := nil;
      MetaFieldDef := nil;
      if not (RowFields.Items[i] is TMemDeleteSentinel) then
        RowField := RowFields.Items[i] as TMemFieldData;
      if not (MetaFields.Items[i] is TMemDeleteSentinel) then
        MetaFieldDef := MetaFields.Items[i] as TMemFieldDef;
      if Assigned(RowField) <> Assigned(MetaFieldDef) then
        raise EMemDbConsistencyException.Create(S_FIELD_REARRANGEMENT_INCONSISTENT);
      if Assigned(RowField) then
      begin
        //Same field types.
        if MetaFieldDef.FieldIndex <> i then
          raise EMemDbInternalException.Create(S_BAD_FIELD_INDEX);
        if RowField.FDataRec.FieldType <> MetaFieldDef.FieldType then
          raise EMemDBConsistencyException.Create(S_FIELD_DIFFERENT_TYPE_FROM_META);
      end;
    end;
  end;
end;


procedure TMemDBTablePersistent.DeleteUnusedIndices(Reason: TMemDBTransReason);
var
  i: integer;
  TagData: TMemDBITagData;
  RV: TIsRetVal;
  Parallel: boolean;
begin
  Parallel := OptimizationApplies(Optimizations.IndexBuildParallel, Reason);
  if FIndexingChangeRequired then
  begin
    for i := 0 to Pred(Length(FIndexChangesets)) do
    begin
      if ictDeleted in FIndexChangesets[i] then
      begin
        TagData := FTagDataList[i];
        CheckTagAgreesWithMetadata(i, TagData, tciPermanentAgreesCurrent, tcpProgrammed);
        TagData.CommitRmIdxsFromStore(Parallel);
        FCommitChangesMade := FCommitChangesMade + [ccmDeleteUnusedIndices];
        CheckTagAgreesWithMetadata(i, TagData, tciPermanentAgreesCurrent, tcpNotProgrammed);
      end;
    end;
    if Parallel then
    begin
      RV := FData.Store.PerformAsyncActions(@MemDBXlateExceptions);
      if RV <> rvOK then
        raise EMemDbInternalException.Create(S_ASYNC_INDEX_OP_FAILED);
    end;
  end;
end;

const
  S_NEW_TAGS_UNEXPECTED_COUNT = 'Unexpected count of tags checking tag arrays.';
  S_NEW_TAGS_UNEXPECTED_OBJECT = 'Unexpected object encountered checking tag arrays';

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

procedure TMemDbTablePersistent.AdjustTableStructureForMetadata;
var
  CC, NC: TMemTableMetadataItem;
  Row: TMemDBRow;
  EditRec: TEditRec;
  NextFields: TMemStreamableList;
  NextField: TMemFieldData;
  FieldDef1, FieldDef2: TMemFieldDef;
  i: integer;
begin
  //GetCurrentNextMetadataCopies(CC, NC, CCFieldCount, CCIndexCount, NCFieldCount, NCIndexCount);
  GetCurrNxtMetaCopies(CC,NC);
  EditRec := FData.MetaEditFirst;
  while Assigned(EditRec) do
  begin
    Row := FData.MetaRemoveRowForEdit(EditRec);
    try
      //Go through all the fields.
      Row.RequestChange;
      NextFields := Row.ABData[abNext] as TMemStreamableList;
      if NC.FieldDefs.Count = 0 then
      begin
        //All fields deleted, delete row.
        Row.Delete;
      end
      else
      begin
        for i := 0 to Pred(NC.FieldDefs.Count) do
        begin
          //No delete sentinels in current or next at the moment....
          if i < CC.FieldDefs.Count then
          begin
            NextField := NextFields.Items[i] as TMemFieldData;
          end
          else
          begin
            NextField := nil;
          end;
          if (NC.FieldDefs.Items[i]) is TMemDeleteSentinel then
            FieldDef1 := nil
          else
            FieldDef1 := NC.FieldDefs.Items[i] as TMemFieldDef;
          if i < CC.FieldDefs.Count then
          begin
            Assert(not (CC.FieldDefs.Items[i] is TMemDeleteSentinel));
            FieldDef2 := CC.FieldDefs.Items[i] as TMemFieldDef
          end
          else
            FieldDef2 := nil;
          Assert((not Assigned(FieldDef1)) = (fctDeleted in FFieldChangesets[i]));
          Assert((not Assigned(FieldDef2)) = (fctAdded in FFieldChangesets[i]));
          if fctDeleted in  FFieldChangesets[i] then
          begin
            NextField.Free;
            NextFields.Items[i] := TMemDeleteSentinel.Create;
          end
          else if fctAdded in FFieldChangesets[i] then
          begin
            NextField := TMemFieldData.Create;
            NextField.FDataRec.FieldType :=
              (NC.FieldDefs.Items[i] as TMemFieldDef).FieldType;
            //Zero data OK, not explicitly representing NULL's.
            NextFields.Add(NextField);
          end
        end;
      end;
    finally
      FData.MetaInsertRowAfterEdit(EditRec, Row);
    end;
    EditRec := FData.MetaEditNext(EditRec);
  end;
end;

function TMemDBTablePersistent.ValidateIndexesParallelHandler(Ref1, Ref2: pointer):pointer;
var
  i: integer;
begin
  i := integer(Ref1);
  RevalidateEntireUserIndex(i);
  result := nil;
end;

procedure TMemDBTablePersistent.ValidateIndexes(Reason: TMemDBTransReason);
var
  CC, NC: TMemTableMetadataItem;
  i, j: integer;
  IndexDef1: TMemIndexDef;
  ValidateParallel: boolean;
  CntParallel: integer;
  Refs1, Refs2, Rets: TPHRefs;
  Handlers: TParallelHandlers;
  Excepts: TPHExcepts;
  Finished: boolean;

  function CheckHelper: boolean;
  begin
    result := false;
    if (FIndexChangesets[i] * [ictAdded
{$IFOPT C+}
      ,ictChangedFieldNumber
      //Other index checking stringent enough do not need to revalidate,
      //if only a change in field number.
{$ENDIF}
      ]) <> [] then
    begin

      //Handle index validation.
      if IndexDef1.IndexAttrs * [iaUnique, iaNotEmpty] <> [] then
        result := true;
    end;
  end;

begin
  Assert(FIndexingChangeRequired);
  GetCurrNxtMetaCopies(CC,NC);
  ValidateParallel := OptimizationApplies(Optimizations.IndexValidateParallel, Reason);
  CntParallel := 0;
  Finished := false;
  while not Finished do
  begin
    for i := 0 to Pred(Length(FIndexChangesets)) do
    begin
      if ictDeleted in FIndexChangesets[i] then
        continue;
      IndexDef1 := NC.IndexDefs.Items[i] as TMemIndexDef;
      if CheckHelper then
      begin
        if ValidateParallel then
          Inc(CntParallel)
        else
          RevalidateEntireUserIndex(i);
      end;
    end;
    if ValidateParallel then
    begin
      if CntParallel <= 1 then
        ValidateParallel := false //Finished := false;
      else
        Finished := true;
    end
    else
      Finished := true;
  end;

  if ValidateParallel then
  begin
    SetLength(Refs1, CntParallel);
    SetLength(Handlers, CntParallel);
    SetLength(Rets, CntParallel);
    SetLength(Excepts, CntParallel);

    j := 0;
    for i := 0 to Pred(Length(FIndexChangesets)) do
    begin
      if ictDeleted in FIndexChangesets[i] then
        continue;
      IndexDef1 := NC.IndexDefs.Items[i] as TMemIndexDef;
      if CheckHelper then
      begin
        Refs1[j] := Pointer(i);
        Handlers[j] := ValidateIndexesParallelHandler;
        Inc(j);
      end;
    end;
    Assert(j = CntParallel);
    ExecParallel(Handlers, Refs1, Refs2, Excepts, Rets, @MemDBXlateExceptions);
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
  RV: TIsRetVal;
  Parallel: boolean;
begin
  Parallel := OptimizationApplies(Optimizations.IndexBuildParallel, Reason);
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
        TagData.CommitAddIdxsToStore(FData.Store, Parallel);
        CheckTagAgreesWithMetadata(i, TagData, tciTemporaryAgreesNextOnly, tcpProgrammed);
      end;
      FTemporaryIndexLimit := Succ(i);
    end;

    if Parallel then
    begin
      RV := FData.Store.PerformAsyncActions(@MemDBXlateExceptions);
      if RV <> rvOK then
        raise EMemDbInternalException.Create(S_ASYNC_INDEX_OP_FAILED);
    end;

    ValidateIndexes(Reason);
  end;
end;

procedure TMemDbTablePersistent.CommitRestoreIndicesToPermanent(Reason: TMemDbTransReason);
var
  i: integer;
  Limit: integer;
  TagData: TMemDBITagData;
  Parallel: boolean;
  RV: TIsRetVal;

begin
  Parallel := OptimizationApplies(Optimizations.IndexBuildParallel, Reason);
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
  if Parallel then
  begin
    RV := FData.Store.PerformAsyncActions(@MemDBXlateExceptions);
    if RV <> rvOK then
      raise EMemDbInternalException.Create(S_ASYNC_INDEX_OP_FAILED);
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

procedure TMemDbTablePersistent.PreCommit(Reason: TMemDBTransReason);
begin
  inherited;
  if not (FMetadata.Deleted or FMetadata.Null) then
  begin
    //Set up changeset info, arguably should have been re-gen before, but
    ReGenChangesets;
    //Structure checking, no modifications yet.
    if FDataChangeRequired and (Reason <> mtrReplayFromScratch) then
    begin
      if FData.AnyChangesAtAll then
        raise EMemDBConsistencyException.Create(S_TABLE_DATA_CHANGED);
    end
    else
    begin
      CheckStreamedInTableRowCount;
      if not QuickBuildOptimizationApplies(Reason) then
      begin
        FData.ForEachChangedRow(CheckStreamedInRowStructure,
          FMetadata.ABData[abLatest], Pointer(false));
      end
      else
      begin
        FData.ForEachRow(CheckStreamedInRowStructure,
          FMetadata.ABData[abLatest], Pointer(true));
      end;
    end;

    //Modifications from here on in.
    //(Indices need to be set up to allow foreign key checking).

    AddNewTagStructs;    //Changes FCommitChangesMade
    DeleteUnusedIndices(Reason); //Changes FCommitChangesMade.

    //When we adjust table structure, add and delete should be OK
    //(we just need to consider deletion case).

    if FDataChangeRequired and (Reason <> mtrReplayFromScratch) then
      AdjustTableStructureForMetadata;

    CommitAdjustIndicesToTemporary(Reason); //Changes FCommitChangesMade

    //Partial validation of indexes. when data changes.
    //In other cases (fromScratch, indexAdd), we expect to revalidate
    //entire index.
    if (Reason <> mtrReplayFromScratch) and not FDataChangeRequired then
      CheckStreamedInRowIndexing;

    //At this point, ready for commit, final index checking for foreign keys
    //relationships can happen at this point.
  end;
end;


procedure TMemDbTablePersistent.Commit(Reason: TMemDBTransReason);
begin
  //This with temporary index tags.
  if not (FMetadata.Deleted or FMetadata.Null) then
  begin
    FData.Commit(Reason);
    if ccmIndexesToTemporary in FCommitChangesMade then
    begin
      CommitRestoreIndicesToPermanent(Reason);
      FCommitChangesMade := FCommitChangesMade - [ccmIndexesToTemporary];
    end;
  end;
  RemoveOldTagStructs;
  //And now commit metadata.
  inherited; //FMetadata.Commit(Reason);
  FCommitChangesMade := [];
  FDataChangeRequired := false;
  FIndexingChangeRequired := false;
  UpdateListHelpers;
end;

procedure TMemDbTablePersistent.Rollback(Reason: TMemDBTransReason);
begin
  //Can restore indices before rollback, because
  //of the subtlety that indexes that change field
  //number do not also change data values... I think...
  if ccmIndexesToTemporary in FCommitChangesMade then
  begin
    RollbackRestoreIndicesToPermanent;
    FCommitChangesMade := FCommitChangesMade - [ccmIndexesToTemporary];
  end;
  FData.Rollback(Reason); //Undo AdjustTableStructureForMetadata, amongst other things.
  if ccmDeleteUnusedIndices in FCommitChangesMade then
  begin
    ReinstateDeletedIndices;
    FCommitChangesMade := FCommitChangesMade - [ccmDeleteUnusedIndices];
  end;
  if ccmAddedNewTagStructs in FCommitChangesMade then
  begin
    RemoveNewlyAddedTagStructs;
    FCommitChangesMade := FCommitChangesMade - [ccmAddedNewTagStructs];
  end;
  inherited; //FMetadata.Rollback(Reason);
  Assert(FCommitChangesMade  = []);
  FDataChangeRequired := false;
  FIndexingChangeRequired := false;
  UpdateListHelpers; //Rollback may have changed A-B, don't need to re-gen changesets.
end;

procedure TMemDBTablePersistent.GetStats(var Stats: TMemStats);
var
  TableMeta: TMemTableMetadataItem;
begin
  if not Assigned(Stats) then
    Stats := TMemDBTableStats.Create;
  inherited;
  if AssignedNotSentinel(FMetadata.ABData[abLatest]) then
    TableMeta := FMetadata.ABData[abLatest] as TMemTableMetadataItem
  else if AssignedNotSentinel(FMetadata.ABData[abCurrent]) then
    TableMeta := FMetadata.ABData[abCurrent] as TMemTableMetadataItem
  else
    TableMeta := nil;
  with Stats as TMemDBTableStats do
  begin
    if Assigned(TableMeta) then
    begin
      with TableMeta do
      begin
        FieldCount := FieldDefs.Count;
        IndexCount := IndexDefs.Count;
      end;
    end;
    RowCount := Data.Store.Count;
  end;
end;

{
procedure TMemDBEntity.GetStats(var Stats: TMemStats);
begin
  if not Assigned(Stats) then
    Stats := TMemDBEntityStats.Create;
  if AssignedNotSentinel(FMetadata.ABData[abLatest]) then
    (Stats as TMemDBEntityStats).AName := FMetadata.GetName(abLatest)
  else if AssignedNotSentinel(FMetadata.ABData[abCurrent]) then
    (Stats as TMemDBEntityStats).AName := FMetadata.GetName(abCurrent);
end;
}

{ TMemDBForeignKeyPersistent }

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

procedure TMemDBForeignKeyPersistent.ToJournal(FwdStream: TStream; InverseStream: TStream);
begin
  if FMetadata.AnyChangesAtAll then
  begin
    WrTag(FwdStream, mstFkStart);
    WrTag(InverseStream, mstFkStart);
    FMetadata.ToJournal(FwdStream, InverseStream);
    WrTag(FwdStream, mstFkEnd);
    WrTag(InverseStream, mstFkEnd);
  end;
end;

{$IFOPT C+}
class procedure TMemDBForeignKeyPersistent.AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream);
var
  FTag, ITag: TMemStreamTag;
begin
  //mstFkStart already read.
  FTag := RdTag(FwdStream);
  ITag := RdTag(InverseStream);
  Assert(FTag = ITag);
  Assert(FTag = mstDblBufferedStart);
  TMemDbEntityMetadata.AssertStreamsInverse(FwdStream, InverseStream);
  FTag := RdTag(FwdStream);
  ITag := RdTag(InverseStream);
  Assert(FTag = ITag);
  Assert(FTag = mstFkEnd);
end;
{$ENDIF}

procedure TMemDBForeignKeyPersistent.ToScratch(Stream: TStream);
begin
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
          Assert(RV in [rvOk]); //TODO - Handle failure. Why Dup key?
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
            Assert(RV in [rvOk]); //TODO - Handle failure. Why Dup key?
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

function TMemDBForeignKeyPersistent.ReferringAddedHandler(Ref1, Ref2: Pointer):Pointer;
var
  P :PMemDBFKMeta;
  Reason: TMemDBTransReason;
begin
  P := PMemDBFKMeta(Ref1);
  Reason := TMemDBTransReason(Ref2);
  CreateReferringAddedList(P^, Reason);
  TrimReferringAddedList(P^, Reason);
  result := nil;
end;

function TMemDBForeignKeyPersistent.ReferredDeletedHandler(Ref1, Ref2: Pointer):Pointer;
var
  P :PMemDBFKMeta;
  Reason: TMemDBTransReason;
begin
  P := PMemDBFKMeta(Ref1);
  Reason := TMemDBTransReason(Ref2);
  CreateReferredDeletedList(P^, Reason);
  TrimReferredDeletedList(P^, Reason);
  result := nil;
end;

procedure TMemDBForeignKeyPersistent.CreateCheckForeignKeyRowSets(var  Meta: TMemDBFKMeta; Reason: TMemDBTransReason);

var
  Handlers:TParallelHandlers;
  Refs1, Refs2: TPHRefs;
  Excepts: TPHExcepts;
  Rets: TPHRefs;
  i: Integer;

begin
  //Create some "lookaside lists" of values that have been added / deleted.
  //The store contains TMemFieldData items.
  try
    //N.B. Indexed store gives us a "duplicate key" error code, which we should use.
    SetupIndexes(Meta);
    if OptimizationApplies(Optimizations.FKListsParallel, Reason) then
    begin
      SetLength(Handlers,2);
      SetLength(Refs1, 2);
      SetLength(Refs2, 2);
      for i := 0 to 1 do
      begin
        if i = 0 then
          Handlers[i] := ReferringAddedHandler
        else
          Handlers[i] := ReferredDeletedHandler;
        Refs1[i] := @Meta;
        Refs2[i] := Pointer(Reason);
      end;
      ExecParallel(Handlers, Refs1, Refs2, Excepts, Rets, @MemDBXlateExceptions);
    end
    else
    begin
      CreateReferringAddedList(Meta, Reason);
      TrimReferringAddedList(Meta, Reason);
      CreateReferredDeletedList(Meta, Reason);
      TrimReferredDeletedList(Meta, Reason);
    end;
    CheckOutstandingCrossRefs(Meta, Reason);
  finally
    ClearIndexes(Meta);
  end;
end;

function TMemDBForeignKeyPersistent.HandleAPITableRename(Iso: TMDBIsolationLevel; OldName, NewName: string): boolean;
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
end;

procedure TMemDBForeignKeyPersistent.CheckAPITableDelete(Iso: TMDBIsolationLevel; TableName: string);
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
end;

function TMemDBForeignKeyPersistent.HandleAPIIndexRename(Iso: TMDBIsolationLevel; IsoDeterminedTableName, OldName, NewName: string): boolean;
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
end;

procedure TMemDBForeignKeyPersistent.CheckAPIIndexDelete(Iso: TMDBIsolationLevel; IsoDeterminedTableName, IndexName: string);
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
end;

{ TMemDBRow }

constructor TMemDbRow.Create;
begin
  inherited;
  DLItemInitObj(self, @FQuickRef);
end;

destructor TMemDbRow.Destroy;
begin
  if not DlItemIsEmpty(@FQuickRef) then
    DLListRemoveObj(@FQuickRef);
  FOwningListHead := nil;
  inherited;
end;


procedure TMemDbRow.Init(Context: TObject; Name:string);
begin
  inherited;
  CreateGUID(FRowID);
end;

procedure TMemDbRow.ToJournal(FwdStream: TStream; InverseStream: TStream);
begin
  if Added or Changed or Deleted then
  begin
    WrTag(FwdStream, mstRowStartV2);
    WrTag(InverseStream, mstRowStartV2);
    WrGuid(FwdStream, RowId);
    WrGuid(InverseStream, RowId);
    inherited;
    WrTag(FwdStream, mstRowEnd);
    WrTag(InverseStream, mstRowEnd);
  end;
end;

{$IFOPT C+}
class procedure TMemDbRow.AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream);
var
  FTag, ITag: TMemStreamTag;
  FId, IId: string;
  FGid, IGid: TGUID;
begin
  //Assume NOT encountered mstRowStart(V2), so we know what sort of row to read.
  FTag := RdTag(FwdStream);
  ITag := RdTag(InverseStream);
  Assert(FTag = ITag);
  Assert(FTag in [mstRowStartV2, DEPRECATED_mstRowStartV1]);
  if FTag = DEPRECATED_mstRowStartV1 then
  begin
    FId := RdStreamString(FwdStream);
    IId := RdStreamString(InverseStream);
    Assert(FId = IId);
  end
  else
  begin
    FGid := RdGuid(FwdStream);
    IGid := RdGuid(InverseStream);
    Assert(CompareGuids(FGid, IGid) = 0);
  end;
  Assert(FId = IId);
  FTag := RdTag(FwdStream);
  ITag := RdTag(InverseStream);
  Assert(FTag = ITag);
  Assert(FTag = mstDBlBufferedStart);
  inherited;

  FTag := RdTag(FwdStream);
  ITag := RdTag(InverseStream);
  Assert(FTag = ITag);
  Assert(FTag = mstRowEnd);
end;
{$ENDIF}

procedure TMemDbRow.ToScratch(Stream: TStream);
begin
  WrTag(Stream, mstRowStartV2);
  WrGuid(Stream, RowId);
  inherited;
  WrTag(Stream, mstRowEnd);
end;

procedure TMemDbRow.FromJournal(Stream: TStream);
var
  StreamRowId: string;
  StreamRowGuid: TGUID;
  CheckItemCurrent, CheckItemNext: TMemStreamableList;
  Tag: TMemStreamTag;
begin
  Tag := RdTag(Stream);
  if not (Tag in [DEPRECATED_mstRowStartV1, mstRowStartV2]) then
    raise EMemDBException.Create(S_WRONG_TAG);
  if Tag = DEPRECATED_mstRowStartV1 then
  begin
    StreamRowId := RdStreamString(Stream);
    StreamRowGUID := StringToGUID(StreamRowId);
  end
  else
    StreamRowGUID := RdGuid(Stream);

  inherited;
  if CompareGuids(RowId, StreamRowGUID) <> 0 then
    raise EMemDBInternalException.Create(S_ROW_IDS_DISAGREE);
  ExpectTag(Stream, mstRowEnd);
  CheckItemCurrent := nil;
  CheckItemNext := nil;
  if AssignedNotSentinel(ABData[abCurrent]) then
    CheckItemCurrent := ABData[abCurrent] as TMemStreamableList;
  if AssignedNotSentinel(ABData[abNext]) then
    CheckItemNext := ABData[abNext] as TMemStreamableList;
  CheckABStreamableListChange(
    CheckItemCurrent,
    CheckItemNext);
end;

procedure TMemDbRow.FromScratch(Stream: TStream);
var
  StreamRowId: string;
  StreamRowGUID: TGUID;
  Tag: TMemStreamTag;
begin
  Tag := RdTag(Stream);
  if not (Tag in [DEPRECATED_mstRowStartV1, mstRowStartV2]) then
    raise EMemDBException.Create(S_WRONG_TAG);
  if Tag = DEPRECATED_mstRowStartV1 then
  begin
    StreamRowId := RdStreamString(Stream);
    StreamRowGUID := StringToGuid(StreamRowId);
  end
  else
    StreamRowGUID := RdGuid(Stream);

  inherited;
  if CompareGuids(RowId, StreamRowGuid) <> 0 then
    raise EMemDBInternalException.Create(S_ROW_IDS_DISAGREE);
  ExpectTag(Stream, mstRowEnd);
  if not QuickBuildOptimizationApplies(mtrReplayFromScratch) then
  begin
    //Assuming no delete sentinels in A/B top level copies, and every row is an add.
    if AssignedNotSentinel(ABData[abCurrent] as TMemStreamableList)
      or (NotAssignedOrSentinel(ABData[abNext] as TMemStreamableList)) then
      raise EMemDbInternalException.Create(S_JOURNAL_ROW_NOT_NEW);

    CheckABStreamableListChange(
      nil,
      ABData[abNext] as TMemStreamableList);
  end
  else
  begin
    //Assuming row freshly streamed into *current* copy.
    if NotAssignedOrSentinel(ABData[abCurrent] as TMemStreamableList)
      or (AssignedNotSentinel(ABData[abNext] as TMemStreamableList)) then
      raise EMemDbInternalException.Create(S_JOURNAL_ROW_NOT_NEW);

      CheckABStreamableListChange(
        ABData[abCurrent] as TMemStreamableList,
        nil);
  end;
end;

procedure TMemDbRow.Commit(Reason: TMemDbTransReason);
begin
  inherited;
  if Assigned(ABData[abCurrent]) then
    (ABData[abCurrent] as TMemStreamableList).RemoveDeleteSentinels;
end;

{ TMemDBEntityMetadata }

function TMemDBEntityMetadata.GetName(Sel: TABSelection):string;
begin
  result := (ABData[Sel] as TMemEntityMetadataItem).EntityName;
end;

procedure TMemDBEntityMetadata.Init(Context: TObject; Name:string);
begin
  (ABData[abNext] as TMemEntityMetadataItem).EntityName := Name;
end;

{ TMemDbTableMetadata }

procedure TMemDbTableMetadata.Init(Context: TObject; Name:string);
begin
  Assert(not Assigned(ABData[abNext]));
  ABData[abNext] := TMemTableMetadataItem.Create;
  inherited;
end;

procedure TMemDbTableMetadata.CheckABListChanges;
var
  Cur, Nxt: TMemTableMetadataItem;
  CurF, NextF, CurI, NextI: TMemStreamableList;
begin
  if AssignedNotSentinel(AbData[abCurrent]) then
    Cur := AbData[abCurrent] as TMemTableMetadataItem
  else
    Cur := nil;
  if AssignedNotSentinel(AbData[abNext]) then
    Nxt := AbData[abNext] as TMemTableMetadataItem
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


procedure TMemDbTableMetadata.FromJournal(Stream: TStream);
begin
  inherited;
  CheckABListChanges;
end;

procedure TMemDbTableMetadata.FromScratch(Stream: TStream);
begin
  inherited;
  CheckABListChanges;
end;

procedure TMemDbTableMetadata.PreCommit(Reason: TMemDbTransReason);
begin
  inherited;
  CheckABListChanges;
end;

procedure TMemDbTableMetadata.Commit(Reason: TMemDbTransReason);
var
  MD: TMemTableMetadataItem;
begin
  inherited;
  if Assigned(ABData[abCurrent]) then
  begin
    MD := ABData[abCurrent] as TMemTableMetadataItem;
    MD.FieldDefs.RemoveDeleteSentinels;
    MD.IndexDefs.RemoveDeleteSentinels;
  end;
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

function TMemDBTableMetadata.FieldsByNames(AB: TAbSelection; const Names: TMDBFieldNames; var AbsIdxs: TFieldOffsets): TMemFieldDefs;
var
  MetadataCopy: TMemTableMetadataItem;
begin
  if AssignedNotSentinel(ABData[AB]) then
  begin
    MetadataCopy := ABData[AB] as TMemTableMetadataItem;
    result := FieldsByNamesInt(MetadataCopy, Names, AbsIdxs);
  end
  else
  begin
    SetLength(result, 0);
    SetLength(AbsIdxs, 0);
  end;
end;

function TMemDBTableMetadata.FieldByName(AB: TABSelection; const Name: string; var AbsIndex: Integer): TMemFieldDef;
var
  MetadataCopy: TMemTableMetadataItem;
begin
  if AssignedNotSentinel(ABData[AB]) then
  begin
    MetadataCopy := ABData[AB] as TMemTableMetadataItem;
    result := FieldByNameInt(MetadataCopy, Name, AbsIndex);
  end
  else
    result := nil;
end;

function TMemDBTableMetadata.IndexByName(AB: TABSelection; const Name: string; var AbsIndex: Integer): TMemIndexDef;
var
  MetadataCopy: TMemTableMetadataItem;
begin
  if AssignedNotSentinel(ABData[AB]) then
  begin
    MetadataCopy := ABData[AB] as TMemTableMetadataItem;
    result := IndexByNameInt(MetadataCopy, Name, AbsIndex);
  end
  else
    result := nil;
end;

{ TMemDBForeignKeyMetadata }


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
    if Meta.TableReferer = Meta.TableReferred then
      raise EMemDBInternalException.Create(S_FK_REFERENCE_CIRCULAR);
  end;
  //Probably don't check whether refs etc are really valid here,
  //can do that in the pre-commit check when we need to use them.
end;

procedure TMemDbForeignKeyMetadata.Init(Context: TObject; Name: string);
begin
  Assert(not Assigned(ABData[abNext]));
  ABData[ABNext] := TMemForeignKeyMetadataItem.Create;
  inherited;
end;

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


{ TDBObjList }

procedure TDBObjList.SetItem(Idx: integer; New: TMemDBEntity);
begin
  TList(self).Items[idx] := New;
end;

function TDBObjList.GetItem(Idx: integer): TMemDBEntity;
begin
  result := TObject(TList(self).Items[Idx]) as TMemDBEntity;
end;

function TDBObjList.ParallelFree(Ref1: Pointer; Ref2: Pointer): pointer;
begin
  (TObject(Ref1) as TMemDBEntity).Free;
  result := nil;
end;

destructor TDBObjList.Destroy;
var
  Idx: integer;
  Refs1, Refs2:TPHRefs;
  Rets: TPHRefs;
  Handlers: TParallelHandlers;
  Excepts: TPHExcepts;
begin
  //Startup / shutdown optimization.
  if OptimizationApplies(Optimizations.TearDownParallel, mtrReplayFromScratch) then
  begin
    if Count > 0 then
    begin
      SetLength(Refs1, Count);
      SetLength(Handlers, Count);
      for Idx := 0 to Pred(Count) do
      begin
        Refs1[Idx] := Items[Idx];
        Handlers[Idx] := ParallelFree;
      end;
      Parallelizer.ExecParallel(Handlers, Refs1, Refs2, Excepts, Rets, @MemDBXlateExceptions);
    end;
  end
  else
  begin
    for Idx := 0 to Pred(Count) do
      Items[Idx].Free;
  end;
  inherited;
end;


{ TMemDBDatabasePersistent }

function TMemDBDatabasePersistent.AnyChangesAtAll;
var
  i: integer;
begin
  result := inherited;
  for i := 0 to Pred(FUserObjs.Count) do
    result := result or FUserObjs.Items[i].AnyChangesAtAll;
end;

function TMemDBDatabasePersistent.PreCommitParallelHandler(Ref1, Ref2: pointer):pointer;
var
  ObjI :TMemDBEntity;
begin
  ObjI := TMemDBEntity(Ref1);
  Assert(Assigned(ObjI));
  Assert(ObjI is TMemDbEntity);
  ObjI.PreCommit(TMemDbTransReason(Ref2));
  result := nil;
end;

procedure TMemDBDatabasePersistent.PreCommitParallel(Reason: TMemDBTransReason);
var
  Handlers: TParallelHandlers;
  Refs1, Refs2: TPHRefs;
  Rets: TPHRefs;
  Excepts: TPHExcepts;
  Count, i: integer;
  ObjI: TMemDbEntity;
  DoTables, Include: boolean;
begin
  for DoTables := True downto False do
  begin
    Count := 0;
    for i := 0 to Pred(FUserObjs.Count) do
    begin
      ObjI := FUserObjs[i];
      Assert((ObjI is TMemDBTablePersistent) or (ObjI is TMemDBForeignKeyPersistent));
      if DoTables then
        Include := ObjI is TMemDBTablePersistent
      else
        Include := ObjI is TMemDBForeignKeyPersistent;
      if Include then
        Inc(Count);
    end;
    if (Count > 0) then
    begin
      SetLength(Handlers, Count);
      SetLength(Refs1, Count);
      SetLength(Refs2, Count);
      Count := 0;
      for i := 0 to Pred(FUserObjs.Count) do
      begin
        ObjI := FUserObjs[i];
        Assert((ObjI is TMemDBTablePersistent) or (ObjI is TMemDBForeignKeyPersistent));
        if DoTables then
          Include := ObjI is TMemDBTablePersistent
        else
          Include := ObjI is TMemDBForeignKeyPersistent;
        if Include then
        begin
          Handlers[Count] := PreCommitParallelHandler;
          Refs1[Count] := Pointer(ObjI);
          Refs2[Count] := Pointer(Reason);
          Inc(Count)
        end;
      end;
      Parallelizer.ExecParallel(Handlers, Refs1, Refs2, Excepts, Rets, @MemDBXlateExceptions);
    end;
  end;
end;

procedure TMemDBDatabasePersistent.PreCommit(Reason: TMemDBTransReason);
var
  i, j: integer;
  ObjI, ObjJ: TMemDBEntity;
begin
  inherited;
  //Check no duplicate object names.
  for i := 0 to Pred(FUserObjs.Count) do
  begin
    ObjI := FUserObjs[i];
    if not (ObjI.Null or ObjI.Deleted) then
    begin
      for j := Succ(i) to Pred(FUserObjs.Count) do
      begin
        ObjJ := FUserObjs[j];
        if not (ObjJ.Null or ObjJ.Deleted) then
        begin
          if ObjI.Name[abLatest] = ObjJ.Name[abLatest] then
            raise EMemDBConsistencyException.Create(S_COMMMIT_CONSISTENCY_OBJS);
        end;
      end;
    end;
  end;
  //Do the pre-commit check for the tables before the foreign keys
  //(so changesets done, indexes in temporary state if applicable).

  //Can also do tables and foreign keys in parallel.
  if OptimizationApplies(Optimizations.PreAndCommitParallel, Reason) then
  begin
    PreCommitParallel(Reason);
  end
  else
  begin
    for i := 0 to Pred(FUserObjs.Count) do
    begin
      ObjI := FUserObjs[i];
      Assert((ObjI is TMemDBTablePersistent) or (ObjI is TMemDBForeignKeyPersistent));
      if ObjI is TMemDBTablePersistent then
        ObjI.PreCommit(Reason);
    end;
    for i := 0 to Pred(FUserObjs.Count) do
    begin
      ObjI := FUserObjs[i];
      if ObjI is TMemDBForeignKeyPersistent then
        ObjI.PreCommit(Reason);
    end;
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

procedure TMemDBDatabasePersistent.ToJournal(FwdStream: TStream; InverseStream: TStream);
var
  i: integer;
begin
  WrTag(FwdStream, mstDBStart);
  WrTag(InverseStream, mstDBStart);
  for i := 0 to Pred(FUserObjs.Count) do
    FUserObjs.Items[i].ToJournal(FwdStream, InverseStream);
  WrTag(FwdStream, mstDBEnd);
  WrTag(InverseStream, mstDBEnd);
end;

{$IFOPT C+}
class procedure TMemDBDatabasePersistent.AssertStreamsInverse(FwdStream: TStream; InverseStream: TStream);
var
  FTag, ITag: TMemStreamTag;
  FPos, IPos: Int64;
begin
  FTag := RdTag(FwdStream);
  ITag := RdTag(InverseStream);
  Assert(FTag = ITag);
  Assert(FTag = mstDbStart);

  FPos := FwdStream.Position;
  IPos := InverseStream.Position;

  FTag := RdTag(FwdStream);
  ITag := RdTag(InverseStream);
  Assert(FTag = ITag);
  Assert(FTag in [mstTableStart, mstFKStart, mstdbEnd]);
  while FTag <> mstDBEnd do
  begin
    case FTag of
       mstTableStart: TMemDBTablePersistent.AssertStreamsInverse(FwdStream, InverseStream);
       mstFkStart: TMemDBForeignKeyPersistent.AssertStreamsInverse(FwdStream, InverseStream);
    else
      Assert(false);
    end;
    FTag := RdTag(FwdStream);
    ITag := RdTag(InverseStream);
    Assert(FTag = ITag);
    Assert(FTag in [mstTableStart, mstFKStart, mstdbEnd]);
  end;
end;
{$ENDIF}


procedure TMemDBDatabasePersistent.ToScratch(Stream: TStream);
var
  i: integer;
begin
  WrTag(Stream, mstDBStart);
  for i := 0 to Pred(FUserObjs.Count) do
    FUserObjs.Items[i].ToScratch(Stream);
  WrTag(Stream, mstDBEnd);
end;

procedure TMemDBDatabasePersistent.FromJournal(Stream: TStream);
var
  StrPos: int64;
  NxtTag: TMemStreamTag;
  ChangeType: TMemDBEntityChangeType;
  EntityName: string;
  DBU: TMemDBEntity;
  TmpIdx: integer;
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
        if Assigned(EntitiesByName(abLatest, EntityName, TmpIdx)) then
          raise EMemDbException.Create(S_JOURNAL_REPLAY_DUP_INST);
        if ChangeType = mectNewTable then
          DBU := TMemDbTable.Create
        else
          DBU := TMemDBForeignKey.Create;
        DBU.FParentDB := self;
        FUserObjs.Add(DBU);
        DBU.FromJournal(Stream);
        Assert(DBU.Added);
      end;
      mectChangedDeletedEntity, mectChangedDataTable:
      begin
        //In these cases, lookahead helper should be returning the
        //current entity name.

        //Not all the renaming is checked here, we'll do that in the pre-commit check.
        DBU := EntitiesByName(abCurrent, EntityName, TmpIdx);
        if Assigned(DBU) then
        begin
          DBU.FromJournal(Stream);
          Assert((DBU.Changed or DBU.Deleted)
            = (ChangeType = mectChangedDeletedEntity));

          if ChangeType = mectChangedDataTable then
            Assert(not (DBU.Added or DBU.Changed or DBU.Deleted));
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

procedure TMemDBDatabasePersistent.FromScratch(Stream: TStream);
var
  StrPos: int64;
  NxtTag: TMemStreamTag;
  ChangeType: TMemDBEntityChangeType;
  EntityName: string;
  TmpIdx: integer;
  DBU: TMemDBEntity;
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
        if Assigned(EntitiesByName(abLatest, EntityName, TmpIdx)) then
          raise EMemDbException.Create(S_JOURNAL_REPLAY_DUP_INST);
        if ChangeType = mectNewTable then
          DBU := TMemDbTable.Create
        else
          DBU := TMemDBForeignKey.Create;
        DBU.FParentDB := self;
        FUserObjs.Add(DBU);
        DBU.FromScratch(Stream);
        Assert(DBU.Added);
      end
    else
      raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
    end;
    //Variable handling based on lookahead results.
    StrPos := Stream.Position;
    NxtTag := RdTag(Stream);
  end;
end;

function TMemDBDatabasePersistent.CommitParallelHandler(Ref1, Ref2: pointer):pointer;
var
  ObjI :TMemDBEntity;
begin
  ObjI := TMemDBEntity(Ref1);
  Assert(Assigned(ObjI));
  Assert(ObjI is TMemDbEntity);
  ObjI.Commit(TMemDbTransReason(Ref2));
  result := nil;
end;

procedure TMemDbDatabasePersistent.CommitParallel(Reason: TMemDBTransReason);
var
  Handlers: TParallelHandlers;
  Refs1, Refs2: TPHRefs;
  Excepts: TPHExcepts;
  Rets: TPHRefs;
  i: integer;
begin
  if FUserObjs.Count > 0 then
  begin
    SetLength(Handlers, FUserObjs.Count);
    SetLength(Refs1, FUserObjs.Count);
    SetLength(Refs2, FUserObjs.Count);
    for i := 0 to Pred(FUserObjs.Count) do
    begin
      Handlers[i] := CommitParallelHandler;
      Refs1[i] := FUserObjs.Items[i];
      Refs2[i] := Pointer(Reason);
    end;
    ExecParallel(Handlers, Refs1, Refs2, Excepts, Rets, @MemDBXlateExceptions);
  end;
end;

procedure TMemDBDatabasePersistent.Commit(Reason: TMemDbTransReason);
var
  i: integer;
begin
  inherited;
  if OptimizationApplies(Optimizations.PreAndCommitParallel, Reason) then
    CommitParallel(Reason)
  else
  begin
    for i := 0 to Pred(FUserObjs.Count) do
      FUserObjs.Items[i].Commit(Reason);
  end;
end;

procedure TMemDBDatabasePersistent.Rollback(Reason: TMemDbTransReason);
var
  i: integer;
begin
  inherited;
  for i := 0 to Pred(FUserObjs.Count) do
    FUserObjs.Items[i].Rollback(Reason);
end;

procedure TMemDBDatabasePersistent.CleanupCommon(Reason: TMemDBTransReason);
var
  i: integer;
  NullIdx: integer;
  Refs1, Refs2: TPHRefs;
  Rets: TPHRefs;
  Excepts: TPHExcepts;
  Handlers: TParallelHandlers;
begin
  //Unfortunately not quite the same at TDBObjList parallel free.
  if OptimizationApplies(Optimizations.TearDownParallel, Reason) then
  begin
    NullIdx := 0;
    for i := 0 to Pred(FuserObjs.Count) do
      if FUserObjs.Items[i].Null then
        Inc(NullIdx);
    if NullIdx > 0 then
    begin
      SetLength(Refs1, NullIdx);
      SetLength(Handlers, NullIdx);
      NullIdx := 0;
      for i := 0 to Pred(FuserObjs.Count) do
      begin
        if FUserObjs.Items[i].Null then
        begin
          Refs1[NullIdx] := FUserObjs.Items[i];
          FUserObjs.Items[i] := nil; //So can pack later.
          Handlers[NullIdx] := FUserObjs.ParallelFree;
          Inc(NullIdx);
        end;
      end;
      Parallelizer.ExecParallel(Handlers, Refs1, Refs2, Excepts, Rets, @MemDBXlateExceptions);
    end;
  end
  else
  begin
    for i := 0 to Pred(FUserObjs.Count) do
    begin
      if FUserObjs.Items[i].Null then
      begin
        FUserObjs.Items[i].Free;
        FUserObjs.Items[i] := nil;
      end;
    end;
  end;
  FUserObjs.Pack;
end;

procedure TMemDBDatabasePersistent.PostCommitCleanup(Reason: TMemDbTransReason);
var
  i: integer;
begin
  inherited;
  for i := 0 to Pred(FUserObjs.Count) do
    FUserObjs.Items[i].PostCommitCleanup(Reason);
  CleanupCommon(reason);
end;

procedure TMemDBDatabasePersistent.PostRollbackCleanup(Reason: TMemDbTransReason);
var
  i: integer;
begin
  inherited;
  for i := 0 to Pred(FUserObjs.Count) do
    FUserObjs.Items[i].PostRollbackCleanup(Reason);
  CleanupCommon(Reason);
end;

function TMemDBDatabasePersistent.EntitiesByName(AB:TABSelection; Name: string; var Idx: integer): TMemDBEntity;
var
  i: integer;
  UserObj: TMemDBEntity;
begin
  result := nil;
  Idx := -1;
  for i := 0 to Pred(FUserObjs.Count) do
  begin
    UserObj := FUserObjs.Items[i];
    if UserObj.HasMetadata[AB] then
    begin
      if Name = UserObj.Name[AB] then
      begin
        Idx := i;
        result := UserObj;
        exit;
      end;
    end;
  end;
end;


constructor TMemDBDatabasePersistent.Create;
begin
  inherited;
  FUserObjs := TDBObjList.Create;
  FInterfaced := TMemDBAPIInterfacedObject.Create;
  FInterfaced.FAPIObjectRequest := HandleInterfacedObjRequest;
  FInterfaced.FParent := self;
{$IFDEF PRE_COMMIT_PARALLEL}
  FPoolRec := GCommonPool.RegisterClient(Self, HandlePoolNormalCompletion, HandlePoolCancelledCompletion);
  FPoolEvent := TEvent.Create(nil, true, false, '');
{$ENDIF}
end;

destructor TMemDBDatabasePersistent.Destroy;
begin
{$IFDEF PRE_COMMIT_PARALLEL}
  GCommonPool.DeRegisterClient(FPoolRec);
  FPoolEvent.Free;
{$ENDIF}
  FInterfaced.Free;
  FUserObjs.Free;
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

procedure TMemDBDatabasePersistent.CheckNoDanglingTransactionRefs(Transaction: TObject; CanThrow: boolean);
var
  Idx: integer;
begin
  for Idx := 0 to Pred(FUserObjs.Count) do
    FUserObjs.Items[Idx].CheckNoDanglingTransactionRefs(Transaction, CanThrow);
  FInterfaced.CheckNoDanglingTransactionRefs(Transaction, CanThrow);
  inherited;
end;

function TMemDBDatabasePersistent.HandleAPITableRename(Iso: TMDBIsolationLevel; OldName, NewName: string): boolean;
var
  i: integer;
begin
  result := false;
  for i := 0 to Pred(FUserObjs.Count) do
  begin
    if FUserObjs.Items[i] is TMemDBForeignKeyPersistent then
      result := (FUserObjs.Items[i] as TMemDBForeignKeyPersistent)
        .HandleAPITableRename(Iso, OldName, NewName) or result;
  end;
end;

procedure TMemDBDatabasePersistent.CheckAPITableDelete(Iso: TMDBIsolationLevel; TableName: string);
var
  i: integer;
begin
  for i := 0 to Pred(FUserObjs.Count) do
  begin
    if FUserObjs.Items[i] is TMemDBForeignKeyPersistent then
      (FUserObjs.Items[i] as TMemDBForeignKeyPersistent)
        .CheckAPITableDelete(Iso, TableName);
  end;
end;

function TMemDBDatabasePersistent.HandleAPIIndexRename(Iso: TMDBIsolationLevel; IsoDeterminedTableName, OldName, NewName: string): boolean;
var
  i: integer;
begin
  result := false;
  for i := 0 to Pred(FUserObjs.Count) do
  begin
    if FUserObjs.Items[i] is TMemDBForeignKeyPersistent then
      result := (FUserObjs.Items[i] as TMemDBForeignKeyPersistent)
        .HandleAPIIndexRename(Iso, IsoDeterminedTableName, OldName, NewName) or result;
  end;
end;

procedure TMemDBDatabasePersistent.CheckAPIIndexDelete(Iso: TMDBIsolationLevel; IsoDeterminedTableName, IndexName: string);
var
  i: integer;
begin
  for i := 0 to Pred(FUserObjs.Count) do
  begin
    if FUserObjs.Items[i] is TMemDBForeignKeyPersistent then
      (FUserObjs.Items[i] as TMemDBForeignKeyPersistent)
        .CheckAPIIndexDelete(Iso, IsoDeterminedTableName, IndexName);
  end;
end;

procedure TMemDBDatabasePersistent.GetStats(var Stats: TMemStats);
var
  EntityStats: TMemStats;
  i: integer;
begin
  Assert(Assigned(Stats));
  Assert(Stats is TMemDBStats);
  for i := 0 to Pred(FUserObjs.Count) do
  begin
    EntityStats := nil;
    FUserObjs.Items[i].GetStats(EntityStats);
    Assert(Assigned(EntityStats));
    (Stats as TMemDBStats).EntityStatsList.Add(EntityStats);
  end;
end;

{ TMemDblBufListHelper}

procedure TMemDblBufListHelper.SetABList(AB: TABSelection; New: TMemStreamableList);
begin
  case AB of
    abCurrent: FCurrentList := New;
    abNext: FNextList := New;
  else
    raise EMemDBInternalException.Create(S_BAD_AB_SELECTOR);
  end;
end;

function TMemDblBufListHelper.GetABList(AB: TABSelection): TMemStreamableList;
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

function TMemDblBufListHelper.AddInternal(New: TMemDBStreamable; var OldObj: boolean; Index: integer = -1): integer; //returns raw index, takkes object.
var
  Item: TMemDBStreamable;
begin
  Changing;
  Assert(Assigned(New));
  Assert(not (New is TMemDeleteSentinel));
  if Index < 0 then
    result := FNextList.Add(New)
  else
  begin
    //Re-added old item.
    Item := FNextList.Items[Index];
    Assert(Item is TMemDeleteSentinel);
    Item.Free;
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
  Item.Free;
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

function TMemDblBufListHelper.Add(New: TMemDBStreamable; Index: integer = -1): integer;
var
  OldObj: boolean;
begin
  result := AddInternal(New, OldObj, Index);
end;

function TMemDblBufListHelper.ModifyND(Index: integer): TMemDBStreamable;
begin
  result := Modify(NdIndexToRawIndex(Index));
end;

function TMemDblBufListHelper.AddND(New: TMemDBStreamable; Index: integer = -1): integer;
begin
  Assert(Index < 0);
  result := RawIndexToNdIndex(Add(New, Index));
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

procedure TMemDblBufListHelper.InvalidateLists;
begin
  FCurrentList := nil;
  FNextList := nil;
end;

end.
