unit MemDBApi;
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

{ Unit containing most of the user API for MemDB.
  Basic DB and transaction setup accessible via the MemDB unit }

interface

{$IFDEF DEBUG_DATABASE_DELETE}
{$DEFINE DEBUG_DATABASE_NAVIGATE}
{$ENDIF}

uses
  MemDBBuffered, MemDbMisc, MemDbStreamable, MemDBIndexing, Classes,
  IndexedStore;

type
  TMemDBDatabase = class;
  TMemDBTable = class;
  TMemDBForeignKey = class;
  TMemDbBookMark = type integer;

  //API objects.
  //These hold per-transaction state for users as well
  //as permissions checking.

  //Database level operations.
  TMemAPIDBTopLevel = class(TMemDBAPI)
  protected
    function GetInterfacedObject: TMemDBDatabase;
    property DB: TMemDBDatabase read GetInterfacedObject;
  end;

  TMemAPIDatabaseInternal = class(TMemAPIDBTopLevel)
  protected
  public
    //Really don't call any of these functions if you're an end user.
    procedure JournalReplayCycleV3(JournalEntry: TStream; Initial: boolean);
    procedure UserCommitCycleV3;
    procedure UserRollbackCycleV3;
  end;

  TMemAPIUserTransactionControl = class(TMemAPIDbTopLevel)
  public
    //But calling these as a user inside a transaction is OK.
    //You can, however, tie yourself in knots, so be careful!
    procedure GetChangesetsInfo(var MiniChangesets: integer; var Buffered: boolean);
    procedure MiniCommit;
    procedure MiniRollback(RaiseIfNothing: boolean = true);
    function Bookmark: TMemDbBookMark;
    procedure MiniRollbackToBookmark(Bookmark: TMemDbBookmark);
  end;

  TMemAPIDatabase = class(TMemAPIDBTopLevel)
  public
    function CreateTable(Name: string): TMemDBHandle;
    function OpenTableOrKey(Name: string): TMemDBHandle;
    function CreateForeignKey(Name: string): TMemDBHandle;
    procedure RenameTableOrKey(OldName, NewName: string);
    procedure DeleteTableOrKey(Name: string; Force: boolean = false);
    function GetEntityNames: TStringList;
  end;

  TMemAPIDatabaseComposite = class(TMemAPIDBTopLevel)
    procedure ReversibleDeleteTable(Name: string);
  end;

  TMemAPITable = class(TMemDBAPI)
  protected
    function GetInterfacedObject: TMemDBTable;
    property Table: TMemDBTable read GetInterfacedObject;
  end;

  TMemAPITableMetadata = class(TMemAPITable)
  public
    procedure CreateField(Name: string; FieldType: TMDBFieldType);
    procedure DeleteField(Name: string);
    procedure RenameField(OldName: string; NewName: string);
    function FieldInfo(Name: string; var FieldType: TMDBFieldType): boolean;
    function GetFieldNames: TStringList;

    procedure CreateIndex(Name: string;
                          IndexedField: string;
                          IndexAttrs: TMDbIndexAttrs);
    procedure CreateMultiFieldIndex(Name: string;
                          IndexedFields: TMDBFieldNames;
                          IndexAttrs: TMDbIndexAttrs);
    procedure DeleteIndex(Name: string);
    procedure RenameIndex(OldName: string; NewName: string);
    function IndexInfo(Name: string;
                       var IndexedFields: TMDBFieldNames;
                       var IndexAttrs: TMDBIndexAttrs): boolean;
    function GetIndexNames: TStringList;

    //DO NOT ALLOW:
    //Changing objects once they are created.
    //Easier to add-dup, remove-old, rename.
    //Will create composite API to do this, such that we can do multiple
    //changes / renames / whatever with multi-changesets inside a user transaction.

    //That way we do not have to check multiple crazy change on change of
    //multiple types of items in the same xaction. Eventually one's sanity runs out.

    //Better to Dup state, and be sure it's correct rather than open the door
    //to subtle nasty bugs dependent on whether things happen in the same xaxtion
    //or not.
  end;

  //TODO - Can remove TMemAPIBufState, got there with a cursor
  //and an added variable.
  TMemAPIBufState = (bsUnknown,
                     bsLocated,
                     bsRead,
                     bsModified);

  TMemAPITableData = class(TMemAPITable)
  private
    FCursor: TItemRec;
    FAdding: boolean;
    FFieldList: TMemStreamableList;
    FFieldListDirty: boolean;
  protected
    procedure DiscardInternal;
  public
    constructor Create;
    destructor Destroy ;override;

    //Use as "First/Next (poss by index)"
    function Locate(Pos: TMemAPIPosition; IdxName: string = ''): boolean;

    function FindByIndex(IdxName: string;
                          const Data: TMemDbFieldDataRec): boolean;
    function FindByMultiFieldIndex(IdxName: string;
                          const DataRecs: TMemDbFieldDataRecs): boolean;

    //FindEdgeByIndex is a little hack to find the first / last
    //of a set of records with a certain field value.
    //It should allow us to iterate through a subset of records,
    //even though we don't yet have full query fuctionality
    //(i.e. "SELECT * FROM WHERE 'key_field' = ).

    function FindEdgeByIndex(Pos: TMemAPIPosition; IdxName: string;
                              const Data: TMemDbFieldDataRec): boolean;
    function FindEdgeByMultiFieldIndex(Pos: TMemAPIPosition; IdxName: string;
                              const DataRecs: TMemDbFieldDataRecs): boolean;
    procedure Append;
    procedure ReadField(FieldName: string;
                        var Data: TMemDbFieldDataRec);
    procedure WriteField(FieldName: string;
                         const Data: TMemDbFieldDataRec);
    procedure Post;
    procedure Discard;
    procedure Delete;
    function RowSelected: boolean;
  end;

  TMemAPITableComposite = class (TMemAPITable)
    procedure ReversibleClearRows;
  end;

  //Foreign key operations.
  TMemAPIForeignKey = class(TMemDBAPI)
  protected
    function GetInterfacedObject: TMemDBForeignKey;
    property ForeignKey: TMemDbForeignKey read GetInterfacedObject;
  public
    procedure SetReferencingChild(TableName: string;
                                  IndexName: string);
    procedure SetReferencedParent(TableName: string;
                                  IndexName: string);
    procedure GetReferencingChild(var TableName: string;
                                  var IndexName: string);
    procedure GetReferencedParent(var TableName: string;
                                  var IndexName: string);
  end;

  //TMemAPIForeignKeyComposite?

  ///////////////////////////////////////////////////////////////////


  //Extensions to double buffered persistent objects to perform
  //API operations. These perform consistency checking on the operations,
  //and do not maintain an per-transaction state.
  TMemDbDatabase = class(TMemDbDatabasePersistent)
  public
    function API_CreateTable(Name: string): TMemDBHandle;
    function API_OpenTableOrKey(Iso: TMDBIsolationLevel; Name: string): TMemDBHandle;
    function API_CreateForeignKey(Name: string): TMemDBHandle;
    procedure API_RenameTableOrKey(Iso: TMDBIsolationLevel; OldName, NewName: string);
    function API_DeleteTableOrKey(Iso: TMDBIsolationLevel; Name: string; DryRun: boolean = false): boolean;
    function API_GetEntityNames(Iso: TMDBIsolationLevel): TStringList;
    function HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;override;
  end;

  TMemDBTable = class(TMemDbTablePersistent)
  protected
    procedure CheckNoFieldLayoutChanges;
    procedure CheckNoUncommitedData;
    function GetFieldDefList: TMemStreamableList;
    function IndexNameToIndexAndTag(Iso: TMDBIsolationLevel;  var Idx:TMemIndexDef; Name: string): PITagStruct;
    procedure IndexDefToFieldDefs(IndexDef: TMemIndexDef; var FieldDefs: TMemFieldDefs; var FieldAbsIdxs: TFieldOffsets);
  public
    procedure API_CreateField(Name: string; FieldType: TMDBFieldType);
    procedure API_DeleteField(Iso: TMDBIsolationLevel; Name: string);
    procedure API_RenameField(Iso: TMDBIsolationLevel; OldName: string; NewName: string);
    function API_FieldInfo(Iso: TMDBIsolationLevel; Name: string; var FieldType: TMDBFieldType): boolean;
    function API_GetFieldNames(Iso: TMDBIsolationLevel): TStringList;

    procedure API_CreateIndex(Name: string;
                          IndexedFields: TMDBFieldNames;
                          IndexAttrs: TMDbIndexAttrs);
    procedure API_DeleteIndex(Iso: TMDBIsolationLevel; Name: string);
    procedure API_RenameIndex(Iso: TMDBIsolationLevel; OldName: string; NewName: string);
    function API_IndexInfo(Iso: TMDBIsolationLevel; Name: string;
                       var IndexedFields: TMDBFieldNames;
                       var IndexAttrs: TMDBIndexAttrs): boolean;
    function API_GetIndexNames(Iso: TMDBIsolationLevel): TStringList;

    function API_DataLocate(Iso: TMDBIsolationLevel;
                            Cursor: TItemRec;
                            Pos: TMemAPIPosition;
                            IdxName: string): TItemRec; //Returns cursor.

    function API_DataFindByIndex(Iso: TMDBIsolationLevel;
                                IdxName: string;
                                const DataRecs: TMemDbFieldDataRecs): TItemRec; //Returns cursor

    function API_DataFindEdgeByIndex(Iso: TMDBIsolationLevel;
                                     IdxName: string;
                                     const DataRecs: TMemDbFieldDataRecs;
                                     Pos: TMemAPIPosition): TItemRec; //Returns cursor.

    procedure API_DataReadRow(Iso: TMDBIsolationLevel;
                             Cursor: TItemRec;
                             FieldList: TMemStreamableList);

    procedure API_DataInitRowForAppend(Iso: TMDBIsolationLevel;
                                      Cursor: TItemRec;
                                      FieldList: TMemStreamableList);

    procedure API_DataRowModOrAppend(Iso: TMDBIsolationLevel;
                                    var Cursor: TItemRec;
                                    FieldList: TMemStreamableList);

    procedure API_DataRowDelete(Iso: TMDBIsolationLevel;
                                var Cursor: TItemRec);

    function API_DataGetFieldAbsIdx(Iso: TMDBIsolationLevel;
                                    FieldName: string): integer;

    function HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;override;
  end;

  TMemDBForeignKey = class(TMemDbForeignKeyPersistent)
  protected
    function LookupTableAndIndex(TableName: string; IndexName: string): TMemIndexDef;
  public
    function HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;override;

    procedure API_SetReferencingChild(TableName: string;
                                      IndexName: string);
    procedure API_SetReferencedParent(TableName: string;
                                      IndexName: string);
    procedure API_GetReferencingChild(Iso: TMDBIsolationLevel;
                                      var TableName: string;
                                      var IndexName: string);
    procedure API_GetReferencedParent(Iso: TMDBIsolationLevel;
                                      var TableName: string;
                                      var IndexName: string);
  end;

function MakeStream(StorageMode: TTempStorageMode): TStream;

implementation

uses
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GlobalLog,
{$ENDIF}
  MemDB, SysUtils, IoUtils, BufferedFileStream, Math, NullStream;

const
  S_API_POST_OR_DISCARD_BEFORE_NAVIGATING =
    'Data at cursor locally changed. Post changes or discard before navigating around table.';
  S_API_NO_FIELDS_DUP_POST_OR_DISCARD = 'No fields in field list. Duplicate post or discard?';
  S_API_BAD_FIELD_INDEX = 'Internal API error, bad field index. (IsoLevel confusion?)';
  S_API_CANNOT_CHANGE_FIELD_TYPES_DIRECTLY =
    'Don''t change field types in data records, use metadata functions instead.';
  S_API_RENAME_OVERWRITE =
    'Can''t rename something read with "ReadComitted" isolation if it has already been renamed.';
  S_API_BLOB_SIZE_INCONSISTENT_WITH_PTR =
    'Blob data: Size must be consistent with pointer validity.';
  S_API_FIND_EDGE_FIRST_OR_LAST = 'FindEdgeByIndex requires you to specify the first or last position.';
  S_API_NO_ROW_SELECTED = 'No row selected/located for read, write, or delete';
  S_API_TOO_MANY_INDICES = 'Too many indices referring to the same field';
  S_MINI_ROLLBACK_NOTHING_TO_DO = 'Mini-rollback nothing to do';
  S_API_MINI_COMMIT_OVER_IRREVERSIBLE = 'Cannot perform a mini-commit after a previous force delete,' +
                                        'changes are irreversible. Commit or rollback the transaction.';
  S_MINI_ROLLBACK_NOT_SEQUENTIAL = 'Bookmark provided seems not to be in the sequence of previous changesets in this transaction.';

  S_API_ENTITY_NAME_CONFLICT =
    'Something already exists with that name. Perhaps commit previous renames / deletions?';
  S_API_ENTITY_NAME_NOT_FOUND = 'Couldn''t find anything with that name.';
  S_API_FIELD_NAME_CONFLICT =
    'A field already exists with that name. Perhaps commit previous renames / deletions?';
  S_API_FIELD_NAME_NOT_FOUND = 'Field name not found.';
  S_API_FIELDS_IN_INDEX_MUST_BE_DISJOINT = 'Cannot specify a field name more than once in an index.';
  S_API_INDEX_REFERENCES_FIELD = 'Cannot delete field, it is referenced by an index.';
  S_API_INTERNAL_ERROR = 'API implementation internal error.';
  S_API_INDEX_NAME_CONFLICT = 'An index already exists with that name. Perhaps commit previous renames / deletions?';
  S_API_INDEX_MUST_HAVE_FIELDS = 'You must specify some fields to index on';
  S_API_INDEX_NAME_NOT_FOUND = 'Index name not found.';
  S_API_INTERNAL_TAG_DATA_BAD = 'Index does not correspond with a valid tag';
  S_API_INTERNAL_READING_ROW = 'Internal error reading row data.';
  S_API_TABLE_METADATA_NOT_COMMITED = 'No committed metadata for this table';
  S_API_INTERNAL_CHANGING_ROW = 'Internal error changing row data.';
  S_API_ROW_BAD_STRUCTURE = 'Error changing row, field layout different from table.';
  S_API_SEARCH_REQUIRES_INDEX = 'Bad index name, or index not found, comitted indices only.';
  S_API_SEARCH_REQURES_CORRECT_FIELD_COUNT = 'Number of data fields supplied for search must be same as number of fields indexed.';
  S_API_NO_FIELDS_IN_TABLE_AT_APPEND_TIME = 'Cannot append a record until you have added fields to the table.';
  S_API_NO_ROW_FOR_DELETE = 'You need to navigate to a row before you can delete it.';
  S_API_INDEX_FOR_FK_UNIQUE_ATTR = 'Foreign key: referenced index must have unique attribute set.';

function MakeStream(StorageMode: TTempStorageMode): TStream;
var
  TmpName: string;
begin
  case StorageMode of
    tsmDisk:
    begin
      TmpName := TPath.GetTempFileName();
      result := TMemDBTempFileStream.Create(TmpName);
    end;
  else
    result := TMemoryStream.Create;
  end;
end;

{ TMemAPIDBTopLevel }

function TMemAPIDBTopLevel.GetInterfacedObject: TMemDBDatabase;
begin
  result := FInterfacedObject.Parent as TMemDBDatabase;
end;

{ TMemAPIDatabaseInternal}

procedure TMemAPIDatabaseInternal.JournalReplayCycleV3(JournalEntry: TStream; Initial: boolean);
var
   Reason: TMemDBTransReason;
   Pos: int64;
   Tag: TMemStreamTag;
   Multi, Stop: boolean;
begin
  if Initial then
    Reason := mtrReplayFromScratch
  else
    Reason := mtrReplayFromJournal;

  Pos := JournalEntry.Position;
  Tag := RdTag(JournalEntry);
  if not (Tag in [mstDBStart, mstMultiChangesetsStart]) then
    raise EMemDBException.Create(S_WRONG_TAG);
  Multi := Tag = mstMultiChangesetsStart;
  if not Multi then
    JournalEntry.Seek(Pos, TSeekOrigin.soBeginning);

  repeat
    try
{$IFOPT C+}
      DB.CheckNoChanges;
{$ENDIF}
      if Initial then
        DB.FromScratchV3(JournalEntry)
      else
        DB.FromJournalV3(JournalEntry);
      DB.PreCommit(Reason);
      DB.Commit(Reason);
    finally
      DB.PostCommitCleanup(Reason);
    end;
    if Multi then
    begin
      //We want to be able to replay entire multi-changeset
      //transactions. Stopping here if EOS is an error, a truncated
      //stream should bomb out on a failed read.
      Pos := JournalEntry.Position;
      Tag := RdTag(JournalEntry);
      if not (Tag in [mstDBStart, mstMultiChangesetsEnd]) then
        raise EMemDBException.Create(S_WRONG_TAG);
      Stop := Tag = mstMultiChangesetsEnd;
      if not Stop then
        JournalEntry.Seek(Pos, TSeekOrigin.soBeginning);
    end
    else
      Stop := true;
  until Stop;
end;

procedure TMemAPIDatabaseInternal.UserCommitCycleV3;
var
  PreMarker, PostMarker: TStream;
  FinalPair, MiniSet: TMemDBMiniSet;
  T: TMemDBTransaction;
  MiniCnt: integer;
  SrcIdx, DstIdx: integer;
begin
  T := FAssociatedTransaction as TMemDBTransaction;
  Assert(Assigned(T));
  PreMarker := nil;
  PostMarker := nil;
  FinalPair := nil;
  MiniCnt := T.MiniChangesets.Count;
  try
    FinalPair := TMemDBMiniSet.Create;
    FinalPair.FwdStream := MakeStream(T.Session.TempStorageMode);
{$IFOPT C+}
    FinalPair.InverseStream := MakeStream(T.Session.TempStorageMode);
{$ELSE}
    FinalPair.InverseStream := TNullStream.Create;
{$ENDIF}

    PreMarker := TMemoryStream.Create;
    PostMarker := TMemoryStream.Create;
    WrTag(PreMarker,  mstMultiChangesetsStart);
    WrTag(PostMarker, mstMultiChangesetsEnd);

    Assert(Assigned(T.MiniChangesets));
    Assert(Assigned(T.FinalStreams));
    Assert(T.FinalStreams.Count = 0);

    T.FinalStreams.Count := T.MiniChangesets.Count + 3;
    try
      DB.PreCommit(mtrUserOp);
      DB.ToJournalV3(FinalPair.FwdStream, FinalPair.InverseStream);
{$IFOPT C+}
        FinalPair.FwdStream.Seek(0, TSeekOrigin.soBeginning);
        FinalPair.InverseStream.Seek(0, TSeekOrigin.soBeginning);
        DB.AssertStreamsInverseV3(FinalPair.FwdStream, FinalPair.InverseStream);
{$ENDIF}
      DB.Commit(mtrUserOp);
    finally
      DB.PostCommitCleanup(mtrUserOp);
    end;
    T.MiniChangesets.Add(FinalPair);
  except
    on E: Exception do
    begin
      FinalPair.Free;
      PreMarker.Free;
      PostMarker.Free;
      T.MiniChangesets.Count := MiniCnt;
      T.FinalStreams.Count := 0;
      raise;
    end;
  end;
  //This just a little list rearrangement, no mem alloc,
  //so expect no exception.
  T.FinalStreams.Items[0] := PreMarker;
  DstIdx := 1;
  for SrcIdx := 0 to Pred(T.MiniChangesets.Count) do
  begin
    MiniSet := TObject(T.MiniChangesets.Items[SrcIdx]) as TMemDBMiniSet;
    T.FinalStreams.Items[DstIdx] := MiniSet.FwdStream;
    MiniSet.FwdStream := nil;
    Inc(DstIdx);
  end;
  T.FinalStreams.Items[DstIdx] := PostMarker;
  Inc(DstIdx);
  Assert(DstIdx = T.FinalStreams.Count);
end;


procedure TMemAPIDatabaseInternal.UserRollbackCycleV3;
var
  T: TMemDBTransaction;
  i: integer;
  MiniSet: TMemDBMiniSet;
begin
  T := FAssociatedTransaction as TMemDBTransaction;
  Assert(Assigned(T));
  try
    DB.Rollback(mtrUserOp);
  finally
    DB.PostRollbackCleanup(mtrUserOp);
  end;
  //Exceptions here handled by DB state -> error.
  //Going back by going forward...
  for i := Pred(T.MiniChangesets.Count) downto 0 do
  begin
    MiniSet := TObject(T.MiniChangesets.Items[i]) as TMemDBMiniset;
    try
      DB.FromJournalV3(MiniSet.InverseStream);
      DB.PreCommit(mtrUserOpMultiRollback);
      DB.Commit(mtrUserOpMultiRollback);
    finally
      DB.PostCommitCleanup(mtrUserOpMultiRollback);
    end;
  end;
end;

{ TMemAPIUserTransactionControl }

procedure TMemAPIUserTransactionControl.GetChangesetsInfo(var MiniChangesets: integer; var Buffered: boolean);
var
  T: TMemDBTransaction;
begin
  T := FAssociatedTransaction as TMemDBTransaction;
  Assert(Assigned(T));
  T.Session.StartMiniOp(T);
  try
    MiniChangesets := T.MiniChangesets.Count;
    Buffered := DB.AnyChangesAtAll;
  finally
    T.Session.FinishMiniOp(T);
  end;
end;

procedure TMemAPIUserTransactionControl.MiniCommit;
var
  T: TMemDBTransaction;
  MiniPair: TMemDBMiniSet;
  ChgCnt: integer;
begin
  CheckReadWriteTransaction;
  T := FAssociatedTransaction as TMemDBTransaction;
  if T.BufChangesOneWay then
    raise EMemDBAPIException.Create(S_API_MINI_COMMIT_OVER_IRREVERSIBLE);

  T.Session.StartMiniOp(T);
  try
    MiniPair := nil;
    ChgCnt := T.MiniChangesets.Count;
    try
      MiniPair := TMemDBMiniSet.Create;
      MiniPair.FwdStream := MakeStream(T.Session.TempStorageMode);
      MiniPair.InverseStream := MakeStream(T.Session.TempStorageMode);
      T.MiniChangesets.Count := T.MiniChangesets.Count + 1;
      try
        DB.PreCommit(mtrUserOp);
        DB.ToJournalV3(MiniPair.FwdStream, MiniPair.InverseStream);
{$IFOPT C+}
        MiniPair.FwdStream.Seek(0, TSeekOrigin.soBeginning);
        MiniPair.InverseStream.Seek(0, TSeekOrigin.soBeginning);
        DB.AssertStreamsInverseV3(MiniPair.FwdStream, MiniPair.InverseStream);
{$ENDIF}
        DB.Commit(mtrUserOp);
      finally
        DB.PostCommitCleanup(mtrUserOp);
      end;
      T.MiniChangesets[Pred(T.MiniChangesets.Count)] := MiniPair;
    except
      on E: Exception do
      begin
        MiniPair.Free;
        T.MiniChangesets.Count := ChgCnt;
        raise;
      end;
    end;
  finally
    T.Session.FinishMiniOp(T);
  end;
end;

procedure TMemAPIUserTransactionControl.MiniRollback(RaiseIfNothing: boolean);
var
  T: TMemDBTransaction;
  MiniSet: TMemDBMiniSet;
  MiniCnt: integer;
begin
  CheckReadWriteTransaction;
  T := FAssociatedTransaction as TMemDbTransaction;
  Assert(Assigned(T));
  T.Session.StartMiniOp(T);
  try
    if DB.AnyChangesAtAll then
    begin
      try
        DB.Rollback(mtrUserOp);
      finally
        Db.PostRollbackCleanup(mtrUserOp);
      end;
    end
    else if T.MiniChangesets.Count > 0 then
    begin
      //Back by going forwards.
      //However, in this case, don't put DB in error state if rollback
      //fails, (although arguably we should). It *might* still be possible
      //to commit xaction, and if a "higher level" rollback fails, we can
      //"properly" error things there.
      //Just don't discard state until rollback is done.
      MiniCnt := Pred(T.MiniChangesets.Count);
      MiniSet := TObject(T.MiniChangesets.Items[MiniCnt]) as TMemDBMiniset;
      try
        DB.FromJournalV3(MiniSet.InverseStream);
        DB.PreCommit(mtrUserOpMultiRollback);
        DB.Commit(mtrUserOpMultiRollback);
      finally
        DB.PostCommitCleanup(mtrUserOpMultiRollback);
      end;
      MiniSet.Free;
      T.MiniChangesets.Count := MiniCnt;
    end
    else if RaiseIfNothing then
      raise EMemDBAPIException.Create(S_MINI_ROLLBACK_NOTHING_TO_DO);
  finally
    T.Session.FinishMiniOp(T);
  end;
end;

function TMemAPIUserTransactionControl.Bookmark: TMemDbBookMark;
var
  MiniCount: integer;
  Buffered: boolean;
begin
  MiniCommit;
  GetChangesetsInfo(MiniCount, Buffered);
  Assert(not Buffered);
  result := TMemDBBookMark(MiniCount);
end;

procedure TMemAPIUserTransactionControl.MiniRollbackToBookmark(Bookmark: TMemDbBookmark);
var
  MiniCount: integer;
  Buffered: boolean;
begin
  GetChangesetsInfo(MiniCount, Buffered);
  if (Integer(Bookmark) > MiniCount) or (Integer(Bookmark) < 0) then
    raise EMemDBAPIException.Create(S_MINI_ROLLBACK_NOT_SEQUENTIAL);
  if Buffered then
    MiniRollback;
  while MiniCount > Bookmark do
  begin
    MiniRollback;
    Dec(MiniCount);
  end;
end;

{ TMemAPIDatabase }

function TMemAPIDatabase.CreateTable(Name: string): TMemDBHandle;
begin
  CheckReadWriteTransaction;
  result := DB.API_CreateTable(Name);
end;

function TMemAPIDatabase.OpenTableOrKey(Name: string): TMemDBHandle;
begin
  result := DB.API_OpenTableOrKey(Isolation, Name);
end;

function TMemAPIDatabase.CreateForeignKey(Name: string): TMemDBHandle;
begin
  CheckReadWriteTransaction;
  result := DB.API_CreateForeignKey(Name);
end;

procedure TMemAPIDatabase.RenameTableOrKey(OldName, NewName: string);
begin
  CheckReadWriteTransaction;
  DB.API_RenameTableOrKey(Isolation, OldName, NewName);
end;

procedure TMemAPIDatabase.DeleteTableOrKey(Name: string; Force: boolean);
var
  NoReverse: boolean;
  CompAPI: TMemAPIDatabaseComposite;
  T: TMemDbTransaction;
  AB: TABSelection;
  Entity: TMemDBEntity;
  tmpIdx: integer;
begin
  CheckReadWriteTransaction;
  NoReverse := false;
  AB := IsoToAB(Isolation);
  //Exists.
  Entity := DB.EntitiesByName(AB, Name, tmpIdx);
  if not Assigned(Entity) then
    raise EMemDbAPIException.Create(S_API_ENTITY_NAME_NOT_FOUND);

  if Force or (not (Entity is TMemDBTablePersistent)) then
    NoReverse := DB.API_DeleteTableOrKey(Isolation, Name)
  else
  begin
    CompAPI := self.GetApiObject(APIDatabaseComposite) as TMemAPIDatabaseComposite;
    try
      CompAPI.ReversibleDeleteTable(Name);
    finally
      CompAPI.Free;
    end;
  end;

  if NoReverse then
  begin
    T := FAssociatedTransaction as TMemDBTransaction;
    T.BufChangesOneWay := true;
  end;
end;

function TMemAPIDatabase.GetEntityNames: TStringList;
begin
  result := DB.API_GetEntityNames(Isolation);
end;

{ TMemAPIDatabaseComposite }

procedure TMemAPIDatabaseComposite.ReversibleDeleteTable(Name: string);
var
  Handle: TMemDBHandle;
  NoReverse: boolean;
  TblComp: TMemAPITableComposite;
begin
  //Re-get object handles after mini-commit, they might have been deleted.
  Handle := DB.API_OpenTableOrKey(Isolation, Name);
  TblComp := GetApiObjectFromHandle(Handle, APITableComposite) as TMemAPITableComposite;
  try
    if TblComp.Table.Deleted then
      exit; //Metadata indicates this table will be going away at next (mini) commit.

    //Do a dry-run check (better than committing A/B to bookmark and then rolling
    // back to that bookmark).
    DB.API_DeleteTableOrKey(Isolation, Name, true);
    TblComp.ReversibleClearRows;
  finally
    TblComp.Free;
  end;
  NoReverse := DB.API_DeleteTableOrKey(Isolation, Name);
  Assert(not NoReverse);
end;

{ TMemAPITable }

function TMemAPITable.GetInterfacedObject: TMemDBTable;
begin
  result := FInterfacedObject.Parent as TMemDBTable;
end;

{ TMemAPITableMetadata }

procedure TMemAPITableMetadata.CreateField(Name: string; FieldType: TMDBFieldType);
begin
  CheckReadWriteTransaction;
  Table.CheckNoUncommitedData;
  Table.API_CreateField(Name, FieldType);
end;

procedure TMemAPITableMetadata.DeleteField(Name: string);
begin
  CheckReadWriteTransaction;
  Table.CheckNoUncommitedData;
  Table.API_DeleteField(Isolation, Name);
end;

procedure TMemAPITableMetadata.RenameField(OldName: string; NewName: string);
begin
  CheckReadWriteTransaction;
  Table.CheckNoUncommitedData;
  Table.API_RenameField(Isolation, OldName, NewName);
end;

function TMemAPITableMetadata.FieldInfo(Name: string; var FieldType: TMDBFieldType): boolean;
begin
  result := Table.API_FieldInfo(Isolation, Name, FieldType)
end;

function TMemAPITableMetadata.GetFieldNames: TStringList;
begin
  result := Table.API_GetFieldNames(Isolation);
end;

procedure TMemAPITableMetadata.CreateIndex(Name: string;
                                           IndexedField: string;
                                           IndexAttrs: TMDbIndexAttrs);
var
  Fields: TMDBFieldNames;
begin
  CheckReadWriteTransaction;
  SetLength(Fields,1);
  Fields[0] := IndexedField;
  Table.API_CreateIndex(Name, Fields, IndexAttrs);
end;

procedure TMemAPITableMetadata.CreateMultiFieldIndex(Name: string;
                                                     IndexedFields: TMDBFieldNames;
                                                     IndexAttrs: TMDbIndexAttrs);
begin
  CheckReadWriteTransaction;
  Table.API_CreateIndex(Name, IndexedFields, IndexAttrs);
end;


procedure TMemAPITableMetadata.DeleteIndex(Name: string);
begin
  CheckReadWriteTransaction;
  Table.API_DeleteIndex(Isolation, Name);
end;

procedure TMemAPITableMetadata.RenameIndex(OldName: string; NewName: string);
begin
  CheckReadWriteTransaction;
  Table.API_RenameIndex(Isolation, OldName, NewName);
end;

function TMemAPITableMetadata.IndexInfo(Name: string;
                                        var IndexedFields: TMDBFieldNames;
                                        var IndexAttrs: TMDBIndexAttrs): boolean;
begin
  result := Table.API_IndexInfo(Isolation, Name, IndexedFields, IndexAttrs);
end;

function TMemAPITableMetadata.GetIndexNames: TStringList;
begin
  result := Table.API_GetIndexNames(Isolation);
end;

{ TMemAPITableData }

procedure TMemAPITableData.DiscardInternal;
begin
  if FFieldListDirty then
    raise EMemDBAPIException.Create(S_API_POST_OR_DISCARD_BEFORE_NAVIGATING);
  FFieldList.FreeAndClear;
  FAdding := false;
end;

procedure TMemAPITableData.Discard;
begin
  FFieldListDirty := false;
  DiscardInternal;
end;

constructor TMemAPITableData.Create;
begin
  inherited;
  FFieldList := TMemStreamableList.Create;
end;

destructor TMemAPITableData.Destroy;
begin
  FFieldList.FreeAndClear;
  FFieldLIst.Free;
  inherited;
end;

function TMemAPITableData.Locate(Pos: TMemAPIPosition; IdxName: string = ''): boolean;
begin
  DiscardInternal;
  FCursor := Table.API_DataLocate(FIsolation, FCursor, Pos, IdxName);
  result := Assigned(FCursor);
  if result then
    Table.API_DataReadRow(FIsolation, FCursor, FFieldList);
end;

function TMemAPITableData.FindByIndex(IdxName: string;
                                      const Data: TMemDbFieldDataRec): boolean;
var
  DataRecs: TMemDbFieldDataRecs;
begin
  SetLength(DataRecs,1);
  DataRecs[0] := Data;
  result := FindByMultiFieldIndex(IdxName, DataRecs);
end;

function TMemAPITableData.FindByMultiFieldIndex(IdxName: string;
                                        const DataRecs: TMemDbFieldDataRecs): boolean;
begin
  DiscardInternal;
  FCursor := Table.API_DataFindByIndex(FIsolation, IdxName, DataRecs);
  result := Assigned(FCursor);
  if result then
    Table.API_DataReadRow(FIsolation, FCursor, FFieldList);
end;

function TMemAPITableData.FindEdgeByIndex(Pos: TMemAPIPosition; IdxName: string;
                                          const Data: TMemDbFieldDataRec): boolean;
var
  DataRecs: TMemDbFieldDataRecs;
begin
  SetLength(DataRecs,1);
  DataRecs[0] := Data;
  result := FindEdgeByMultiFieldIndex(Pos, IdxName, DataRecs);
end;

function TMemAPITableData.FindEdgeByMultiFieldIndex(Pos: TMemAPIPosition; IdxName: string;
                                            const DataRecs: TMemDbFieldDataRecs): boolean;
begin
  DiscardInternal;
  FCursor := Table.API_DataFindEdgeByIndex(FIsolation,
                                           IdxName, DataRecs, Pos);
  result := Assigned(FCursor);
  if result then
    Table.API_DataReadRow(FIsolation, FCursor, FFieldList);
end;

procedure TMemAPITableData.Append;
begin
  CheckReadWriteTransaction;
  DiscardInternal;
  FCursor := nil;
  FAdding := true;
  Table.API_DataInitRowForAppend(FIsolation, FCursor, FFieldList);
end;

procedure TMemAPITableData.ReadField(FieldName: string;
                    var Data: TMemDbFieldDataRec);
var
  FieldAbsIdx: integer;
  MemFieldData: TMemFieldData;
  NewSize: UInt64;
begin
  if not (Assigned(FCursor) or FAdding) then
    raise EMemDBAPIException.Create(S_API_NO_ROW_SELECTED);
  if FFieldList.Count = 0 then
    raise EMemDBAPiException.Create(S_API_NO_FIELDS_DUP_POST_OR_DISCARD);
  FieldAbsIdx := Table.API_DataGetFieldAbsIdx(FIsolation, FieldName);
  if (FieldAbsIdx < 0) or (FieldAbsIdx >= FFieldList.Count) then
    raise EMemDBInternalException.Create(S_API_BAD_FIELD_INDEX);
  MemFieldData := (FFieldList.Items[FieldAbsIdx] as TMemFieldData);
  Data.FieldType := MemFieldData.FDataRec.FieldType;
  if Data.FieldType = ftBlob then
  begin
  { Change in blob semantics here. If you give me:
    1. nil and zero size, I  will indicate size and return.
    2. nil and nonzero size, I will indicate size and return.
    2. non-nil and zero size, I will indicate copied 0 bytes.
    3. non-nil and too few bytes, I will copy only the amount you asked.
    4. non-nil and correct bytes, I will copy all.
    5. non-nil and too many bytes, I will indicate how much copied.
    N.B.
    6. Except if data field is zero, then I can potentially indicate
       zero bytes copied. Hopefully you knew how large your buffer was,
       or you're happy to free it.}
    if not Assigned(Data.Data) then
      Data.size := MemFieldData.FDataRec.size
    else
    begin
      if Assigned(MemFieldData.FDataRec.Data) then
      begin
        NewSize := Math.Min(Data.size, MemFieldData.FDataRec.size);
        Move(MemFieldData.FDataRec.Data^, Data.Data^, NewSize);
        Data.size := NewSize;
      end
      else
      begin
        Assert(MemFieldData.FDataRec.size = 0);
        Data.size := 0;
      end;
    end;
  end
  else
  begin
    Data := MemFieldData.FDataRec
  end;
end;

procedure TMemAPITableData.WriteField(FieldName: string;
                     const Data: TMemDbFieldDataRec);
var
  FieldAbsIdx: integer;
  Field: TMemFieldData;
  NewData: pointer;
  NewSize: UInt64;
begin
  CheckReadWriteTransaction;
  if not (Assigned(FCursor) or FAdding) then
    raise EMemDBAPIException.Create(S_API_NO_ROW_SELECTED);
  if FFieldList.Count = 0 then
    raise EMemDBAPiException.Create(S_API_NO_FIELDS_DUP_POST_OR_DISCARD);
  FieldAbsIdx := Table.API_DataGetFieldAbsIdx(FIsolation, FieldName);
  if (FieldAbsIdx < 0) or (FieldAbsIdx >= FFieldList.Count) then
    raise EMemDBInternalException.Create(S_API_BAD_FIELD_INDEX);
  Field := (FFieldList.Items[FieldAbsIdx] as TMemFieldData);
  if Data.FieldType <> Field.FDataRec.FieldType then
    raise EMemDBAPIException.Create(S_API_CANNOT_CHANGE_FIELD_TYPES_DIRECTLY);
  if Data.FieldType = ftBlob then
  begin
  { Change in blob semantics here. Instead of taking ownership of the memory buffer,
    I will allocate my own, and assume you're giving me new content on every write }
    if (Data.size > 0) <> Assigned(Data.Data) then
      raise EMemDBAPIException.Create(S_API_BLOB_SIZE_INCONSISTENT_WITH_PTR);
    NewData := nil;
    NewSize := 0;
    try
      if Data.size > 0 then
      begin
        GetMem(NewData, Data.size);
        Move(Data.Data^, NewData^, Data.size);
        NewSize := Data.size;
      end;
    except
      FreeMem(NewData); //Exception in alloc or copy (most likely here).
      raise;
    end;
    FreeMem(Field.FDataRec.Data);
    Field.FDataRec.Data := NewData;
    Field.FDataRec.size := NewSize;
  end
  else
  begin
    Field.FDataRec := Data;
  end;
  FFieldListDirty := true;
end;

procedure TMemAPITableData.Post;
begin
  CheckReadWriteTransaction;
  //Do the post.
  Table.API_DataRowModOrAppend(FIsolation, FCursor, FFieldList);
  Discard; //Discard temp data.
  //TODO - Arguably, we could re-read the data in and remove
  //exceptions about no fields in field list.
  //Table.API_DataReadRow(FIsolation, FCursor, FFieldList);
end;

procedure TMemAPITableData.Delete;
var
  NextCursor: TItemRec;
begin
  CheckReadWriteTransaction;
  if not Assigned(FCursor) then
    raise EMemDBAPIException.Create(S_API_NO_ROW_SELECTED);
  NextCursor := Table.API_DataLocate(FIsolation, FCursor, ptNext, '');
  Table.API_DataRowDelete(FIsolation, FCursor);
  if Isolation < TMDBIsolationLevel.ilCommittedRead  then
  begin
    Discard;
    FCursor := NextCursor;
    if Assigned(FCursor) then
      Table.API_DataReadRow(FIsolation, FCursor, FFieldList);
  end;
end;

function TMemAPITableData.RowSelected: boolean;
begin
  result := Assigned(FCursor);
end;

{ TMemAPITableComposite }

procedure TMemAPITableComposite.ReversibleClearRows;
var
  Trans: TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  UTC: TMemAPIUserTransactionControl;
  TblData: TMemAPITableData;
  DeletedStuff, OK: boolean;
begin
  Trans := self.FAssociatedTransaction as TMemDbTransaction;
  DBAPI := Trans.GetAPI;
  try
    UTC := DBAPI.GetApiObject(APIUserTransactionControl) as TMemAPIUserTransactionControl;
    try
      Assert(not Table.Deleted);
      //Can mini-commit without the table going away.
      if Table.AnyChangesAtAll then
        UTC.MiniCommit;
      //State of table rows is all fresh.
      TblData := GetAPIObject(APITableData) as TMemAPITableData;
      DeletedStuff := false;
      try
        case Isolation of
          ilDirtyRead:
          begin
            OK := TblData.Locate(ptFirst, '');
            while(OK) do
            begin
              TblData.Delete;
              DeletedStuff := true;
              OK := TblData.Locate(ptFirst, '');
            end;
          end;
          ilCommittedRead:
          begin
            OK := TblData.Locate(ptFirst, '');
            while(OK) do
            begin
              TblData.Delete;
              DeletedStuff := true;
              OK := TblData.Locate(ptNext, '');
            end;
          end;
        end;
      finally
        TblData.Free;
      end;
      if DeletedStuff then
        UTC.MiniCommit;
    finally
      UTC.Free;
    end;
  finally
    DBAPI.Free;
  end;
end;

{ TMemAPIForeignKey }

function TMemAPIForeignKey.GetInterfacedObject: TMemDBForeignKey;
begin
  result := FInterfacedObject.Parent as TMemDBForeignKey;
end;

procedure TMemAPIForeignKey.SetReferencingChild(TableName: string;
                              IndexName: string);
begin
  CheckReadWriteTransaction;
  ForeignKey.API_SetReferencingChild(TableName,IndexName);
end;

procedure TMemAPIForeignKey.SetReferencedParent(TableName: string;
                              IndexName: string);
begin
  CheckReadWriteTransaction;
  ForeignKey.API_SetReferencedParent(TableName, IndexName);
end;

procedure TMemAPIForeignKey.GetReferencingChild(var TableName: string;
                              var IndexName: string);
begin
  ForeignKey.API_GetReferencingChild(Isolation, TableName, IndexName);
end;

procedure TMemAPIForeignKey.GetReferencedParent(var TableName: string;
                              var IndexName: string);
begin
  ForeignKey.API_GetReferencedParent(Isolation, TableName, IndexName);
end;


//////////////////////////////////////////////////////////////////////

{ TMemDbDatabase }

//Generally, don't try to undelete, unrename etc etc in the same transaction.
function TMemDBDatabase.API_CreateTable(Name: string): TMemDBHandle;
var
  TmpIdx: integer;
  NewTable: TMemDbTable;
begin
  if Assigned(EntitiesByName(abLatest, Name, tmpIdx)) then
    raise EMemDBAPIException.Create(S_API_ENTITY_NAME_CONFLICT);
  NewTable := TMemDBTable.Create;
  NewTable.Init(self, Name);
  FUserObjs.Add(NewTable);
  Assert(NewTable.Added);
  result := TMemDBHandle(NewTable.Interfaced);
end;

function TMemDBDatabase.API_OpenTableOrKey(Iso: TMDBIsolationLevel; Name: string): TMemDBHandle;
var
  Entity: TMemDBEntity;
  TmpIdx: integer;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  Entity := EntitiesByName(AB, Name, TmpIdx);
  if Assigned(Entity) then
    result := TMemDBHandle(Entity.Interfaced)
  else
    result := nil;
end;

function TMemDBDatabase.API_CreateForeignKey(Name: string): TMemDBHandle;
var
  TmpIdx: integer;
  NewKey: TMemDbForeignKey;
begin
  if Assigned(EntitiesByName(abLatest, Name, tmpIdx)) then
    raise EMemDBAPIException.Create(S_API_ENTITY_NAME_CONFLICT);
  NewKey := TMemDBForeignKey.Create;
  NewKey.Init(self, Name);
  FUserObjs.Add(NewKey);
  Assert(NewKey.Added);
  result := TMemDBHandle(NewKey.Interfaced);
end;

procedure TMemDBDatabase.API_RenameTableOrKey(Iso: TMDBIsolationLevel; OldName, NewName: string);
var
  TmpIdx: integer;
  Entity: TMemDBEntity;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  Entity := EntitiesByName(AB, OldName, tmpIdx);
  if (not Assigned(Entity)) or Entity.Deleted then
    raise EMemDbAPIException.Create(S_API_ENTITY_NAME_NOT_FOUND);
  if Assigned(EntitiesByName(abLatest, NewName, tmpIdx)) then
    raise EMemDBAPIException.Create(S_API_ENTITY_NAME_CONFLICT);
  if AssignedNotSentinel(Entity.Metadata.ABData[abNext]) then
  begin
    //If latest rename is not what we're renaming from, then stop.
    if (Entity.Metadata.ABData[abNext] as TMemEntityMetadataItem)
      .EntityName <> OldName then
    raise EMemDBAPIException.Create(S_API_RENAME_OVERWRITE);
  end;
  Entity := EntitiesByName(abCurrent, OldName, tmpIdx);
  Entity.RequestChange;
  (Entity.Metadata.ABData[abNext] as TMemEntityMetadataItem)
    .EntityName := NewName;
  HandleAPITableRename(Iso, OldName, NewName);
end;

//This delete is pretty much a force delete.
//Returns whether op is noninvertible.
function TMemDBDatabase.API_DeleteTableOrKey(Iso: TMDBIsolationLevel; Name: string; DryRun: boolean): boolean;
var
  TmpIdx: integer;
  Entity: TMemDBEntity;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  //Exists.
  Entity := EntitiesByName(AB, Name, tmpIdx);
  if not Assigned(Entity) then
    raise EMemDbAPIException.Create(S_API_ENTITY_NAME_NOT_FOUND);
  //Referenced by anything else?
  if Entity is TMemDbTablePersistent then
  begin
    //Referenced, bomb out.
    CheckAPITableDelete(Iso, Name);
    //Does the table actually have any data in it?
    result := (Entity as TMemDbTablePersistent).HasTableData;
    //If so, this op is noninvertible.
  end
  else
    result := false;
  if not DryRun then
    Entity.Delete;
end;

function TMemDBDatabase.API_GetEntityNames(Iso: TMDBIsolationLevel): TStringList;
var
  AB: TABSelection;
  i: Integer;
  UserObj: TMemDBEntity;
begin
  AB := IsoToAB(Iso);
  result := TStringList.Create;
  for i := 0 to Pred(FUserObjs.Count) do
  begin
    UserObj := FUserObjs.Items[i];
    if UserObj.HasMetaData[AB] then
    begin
      result.Add(UserObj.Name[AB]);
    end;
  end;
end;

function TMemDBDatabase.HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;
begin
  result := inherited;
  if not Assigned(result) then
  begin
    if Assigned(Transaction) then
    begin
      case ID of
        APIDatabase: result := TMemAPIDatabase.Create;
        APIUserTransactionControl: result := TMemAPIUserTransactionControl.Create;
        APIDatabaseComposite: result := TMemAPIDatabaseComposite.Create;
      end;
    end;
  end;
end;

{ TMemDBTable }

procedure TMemDBTable.CheckNoFieldLayoutChanges;
begin
  if LayoutChangeRequired then
    raise EMemDBAPIException.Create(S_FIELD_LAYOUT_CHANGED);
end;

procedure TMemDBTable.CheckNoUncommitedData;
begin
  if DataChanged then
    raise EMemDBAPIException.Create(S_TABLE_DATA_CHANGED);
end;

procedure TMemDBTable.API_CreateField(Name: string; FieldType: TMDBFieldType);
var
  M: TMemDBTableMetadata;
  tmpIdx: integer;
  NewField: TMemFieldDef;
begin
  M := Metadata as TMemDBTableMetadata;
  if Assigned(M.FieldByName(abLatest, Name, tmpIdx)) then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_CONFLICT);
  //Do data manipulation with list helper.
  NewField := TMemFieldDef.Create;
  try
    NewField.FieldName := Name;
    NewField.FieldType := FieldType;
    tmpIdx := FieldHelper.Add(NewField);
    NewField.FieldIndex := FieldHelper.RawIndexToNdIndex(tmpIdx);
    NewField := nil;
  finally
    NewField.Free;
  end;
  ReGenChangesets;
end;

procedure TMemDBTable.API_DeleteField(Iso: TMDBIsolationLevel; Name: string);
var
  M: TMemDBTableMetadata;
  FieldIdx, FieldIdx2: integer;
  IndexIdx, IdxFIdx: integer;
  Field: TMemFieldDef;
  Index: TMemIndexDef;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  M := Metadata as TMemDBTableMetadata;
  Field := M.FieldByName(AB, Name, FieldIdx);
  if not Assigned(Field) then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_NOT_FOUND);
  //Check whether any indices reference this field.
  for IndexIdx := 0 to Pred(IndexHelper.NonDeletedCount) do
  begin
    Index := IndexHelper.NonDeletedItems[IndexIdx] as TMemIndexDef;
    for IdxFidx := 0 to Pred(Index.FieldNameCount) do
    begin
      if Index.FieldNames[IdxFIdx] = Name then
        raise EMemDBAPIException.Create(S_API_INDEX_REFERENCES_FIELD);
    end;
  end;
  //Delete field.
  FieldIdx2 := FieldHelper.RawIndexToNdIndex(FieldIdx);
  FieldHelper.Delete(FieldIdx);
  //Adjust field index for later fields (ND).
  while (FieldIdx2 < FieldHelper.NonDeletedCount) do
  begin
    Field := FieldHelper.ModifyND(FieldIdx2) as TMemFieldDef;
    Field.FieldIndex := Pred(Field.FieldIndex);
    Inc(FieldIdx2);
  end;
  ReGenChangesets;
end;

procedure TMemDBTable.API_RenameField(Iso: TMDBIsolationLevel; OldName: string; NewName: string);
var
  Field: TMemFieldDef;
  FieldIdx, tmpIdx: integer;
  M: TMemDBTableMetadata;
  IndexIdx, IdxFIdx: integer;
  Index: TMemIndexDef;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  M := Metadata as TMemDbTableMetadata;
  Field := M.FieldByName(AB, OldName, FieldIdx);
  if (not Assigned(Field)) or (FieldHelper.ChildDeleted[FieldIdx]) then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_NOT_FOUND);
  if Assigned(M.FieldByName(abLatest, NewName, tmpIdx)) then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_CONFLICT);
  if AssignedNotSentinel(M.ABData[abNext]) then
  begin
    //Check there has not been a previous conflicting rename.
    //If previously been deleted, ChildDeleted check above should fail.
    Assert (AssignedNotSentinel((M.ABData[abNext] as TMemTableMetadataItem).FieldDefs.Items[FieldIdx]));
    Field := (M.ABData[abNext] as TMemTableMetadataItem).FieldDefs.Items[FieldIdx]
      as TMemFieldDef;
    if Field.FieldName <> OldName then
      raise EMemDBAPIException.Create(S_API_RENAME_OVERWRITE);
  end;
  //Rename the field.
  Field := FieldHelper.Modify(FieldIdx) as TMemFieldDef;
  Field.FieldName := NewName;
  //See if any indexes reference the field
  //if so, update the name in them as well.
  for IndexIdx := 0 to Pred(IndexHelper.NonDeletedCount) do
  begin
    Index := IndexHelper.NonDeletedItems[IndexIdx] as TMemIndexDef;
    for IdxFIdx := 0 to Pred(Index.FieldNameCount) do
    begin
      if Index.FieldNames[IdxFIdx] = OldName then
      begin
        Index := IndexHelper.ModifyND(IndexIdx) as TMemIndexDef;
        Index.FieldNames[IdxFIdx] := Field.FieldName;
      end;
    end;
  end;
  ReGenChangesets;
end;

function TMemDBTable.API_FieldInfo(Iso: TMDBIsolationLevel; Name: string; var FieldType: TMDBFieldType): boolean;
var
  M: TMemDBTableMetadata;
  Field: TMemFieldDef;
  FieldIdx: integer;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  M := Metadata as TMemDBTableMetadata;
  Field := M.FieldByName(AB, Name, FieldIdx);
  result := Assigned(Field);
  if result then
    FieldType := Field.FieldType;
end;

function TMemDBTable.API_GetFieldNames(Iso: TMDBIsolationLevel): TStringList;
var
  M: TMemDBTableMetadata;
  AB: TABSelection;
  MetadataCopy: TMemTableMetadataItem;
  FieldDef: TMemFieldDef;
  idx: integer;
begin
  AB := IsoToAB(Iso);
  M := Metadata as TMemDBTableMetadata;
  result := TStringList.Create;
  if AssignedNotSentinel(M.ABData[AB]) then
  begin
    MetadataCopy := M.ABData[AB] as TMemTableMetadataItem;
    for idx := 0 to Pred(MetadataCopy.FieldDefs.Count) do
    begin
      if AssignedNotSentinel(MetadataCopy.FieldDefs.Items[idx]) then
      begin
        FieldDef := MetadataCopy.FieldDefs.Items[idx] as TMemFieldDef;
        result.Add(FieldDef.FieldName);
      end;
    end;
  end;
end;

procedure TMemDBTable.API_CreateIndex(Name: string;
                      IndexedFields: TMDBFieldNames;
                      IndexAttrs: TMDbIndexAttrs);
var
  M: TMemDbTableMetadata;
  Index: TMemIndexDef;
  IndexIdx: integer;
  Fields: TMemFieldDefs;
  FieldAbsIdxs: TFieldOffsets;
  x,y: integer;

begin
  M := Metadata as TMemDBTableMetadata;
  if Assigned(M.IndexByName(abLatest, Name, IndexIdx)) then
    raise EMemDBAPIException.Create(S_API_INDEX_NAME_CONFLICT);
  if Length(IndexedFields) = 0 then
    raise EMemDbApiException.Create(S_API_INDEX_MUST_HAVE_FIELDS);
  Fields := M.FieldsByNames(abLatest, IndexedFields, FieldAbsIdxs);
  if Length(Fields) = 0 then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_NOT_FOUND);
  Assert(Length(Fields) = Length(IndexedFields));
  for x := 0 to Pred(Length(IndexedFields)) do
  begin
    if (not Assigned(Fields[x])) or (FieldHelper.ChildDeleted[FieldAbsIdxs[x]]) then
      raise EMemDBAPIException.Create(S_API_FIELD_NAME_NOT_FOUND + IndexedFields[x]);
    for y := Succ(x) to Pred(Length(IndexedFields)) do
      if FieldAbsIdxs[x] = FieldAbsIdxs[y] then
        raise EMemDbAPIException.Create(S_API_FIELDS_IN_INDEX_MUST_BE_DISJOINT);
  end;
  Index := TMemIndexDef.Create;
  Index.IndexName := Name;
  Index.FieldNameCount := Length(IndexedFields);
  //FieldArray is R/O property, kinda want to keep it that way.
  for x := 0 to Pred(Length(IndexedFields)) do
    Index.FieldNames[x] := IndexedFields[x];
  Index.IndexAttrs := IndexAttrs;
  IndexIdx := IndexHelper.Add(Index);
  Assert(IndexHelper.ChildAdded[IndexIdx]);

  ReGenChangesets;
end;

procedure TMemDBTable.API_DeleteIndex(Iso: TMDBIsolationLevel; Name: string);
var
  M: TMemDBTableMetadata;
  Index: TMemIndexDef;
  IndexIdx: integer;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  M := Metadata as TMemDBTableMetadata;
  Index := M.IndexByName(AB, Name, IndexIdx);
  if not Assigned(Index) then
    raise EMemDbAPIException.Create(S_API_INDEX_NAME_NOT_FOUND);
  ParentDB.CheckAPIIndexDelete(Iso, self.Name[AB], Name);
  //Delete index.
  IndexHelper.Delete(IndexIdx);
  //Do not need to adjust anything in fields.
  ReGenChangesets;
end;

procedure TMemDBTable.API_RenameIndex(Iso: TMDBIsolationLevel; OldName: string; NewName: string);
var
  M: TMemDbTableMetadata;
  Index: TMemIndexDef;
  IndexIdx, tmpIdx: integer;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  M := Metadata as TMemDbTableMetadata;
  Index := M.IndexByName(AB, OldName, IndexIdx);
  if (not Assigned(Index)) or (IndexHelper.ChildDeleted[IndexIdx]) then
    raise EMemDBAPIException.Create(S_API_INDEX_NAME_NOT_FOUND);
  if Assigned(M.IndexByName(abLatest, NewName, tmpIdx)) then
    raise EMemDBAPIException.Create(S_API_INDEX_NAME_CONFLICT);
  if AssignedNotSentinel(M.ABData[abNext]) then
  begin
    //Check there has not been a previous conflicting rename.
    //If previously been deleted, ChildDeleted check above should fail.
    Assert(AssignedNotSentinel((M.ABData[abNext] as TMemTableMetadataItem).IndexDefs.Items[IndexIdx]));
    Index := (M.ABData[abNext] as TMemTableMetadataItem).IndexDefs.Items[IndexIdx]
      as TMemIndexDef;
    if Index.IndexName <> OldName then
      raise EMemDBAPIException.Create(S_API_RENAME_OVERWRITE);
  end;
  //Now rename the index.
  Index := IndexHelper.Modify(IndexIdx) as TMemIndexDef;
  Index.IndexName := NewName;
  ParentDB.HandleAPIIndexRename(Iso, self.Name[AB], OldName, NewName);
  ReGenChangesets;
end;

function TMemDBTable.API_IndexInfo(Iso: TMDBIsolationLevel; Name: string;
                   var IndexedFields: TMDBFieldNames;
                   var IndexAttrs: TMDBIndexAttrs): boolean;
var
  M: TMemDbTableMetadata;
  Index: TMemIndexDef;
  IndexIdx: integer;
  IdxFIdx: integer;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  M := Metadata as TMemDbTableMetadata;
  Index := M.IndexByName(AB, Name, IndexIdx);
  result := Assigned(Index);
  if result then
  begin
    SetLength(IndexedFields, Index.FieldNameCount);
    for IdxFIdx := 0 to Pred(Length(IndexedFields)) do
      IndexedFields[IdxFIdx] := Index.FieldNames[IdxFIdx];
    IndexAttrs := Index.IndexAttrs;
  end;
end;

function TMemDBTable.API_GetIndexNames(Iso: TMDBIsolationLevel): TStringList;
var
  M: TMemDBTableMetadata;
  AB: TABSelection;
  MetadataCopy: TMemTableMetadataItem;
  IndexDef: TMemIndexDef;
  idx: integer;
begin
  AB := IsoToAB(Iso);
  M := Metadata as TMemDBTableMetadata;
  result := TStringList.Create;
  if AssignedNotSentinel(M.ABData[AB]) then
  begin
    MetadataCopy := M.ABData[AB] as TMemTableMetadataItem;
    for idx := 0 to Pred(MetadataCopy.IndexDefs.Count) do
    begin
      if AssignedNotSentinel(MetadataCopy.IndexDefs.Items[idx]) then
      begin
        IndexDef := MetadataCopy.IndexDefs.Items[idx] as TMemIndexDef;
        result.Add(IndexDef.IndexName);
      end;
    end;
  end;
end;

function TMemDBTable.HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;
begin
  result := inherited;
  if not Assigned(result) then
  begin
    if Assigned(Transaction) then
    begin
      case ID of
        APITableMetadata: result := TMemAPITableMetadata.Create;
        APITableData: result := TMemAPITableData.Create;
        APITableComposite: result := TMemAPITableComposite.Create;
      end;
    end;
  end;
end;

function TMemDBTable.IndexNameToIndexAndTag(Iso: TMDBIsolationLevel;  var Idx:TMemIndexDef; Name: string): PITagStruct;
var
  M: TMemDBTableMetadata;
  IdxIdx: integer;
  Sic: TSubIndexClass;
  TagData: TMemDbITagData;
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  FieldDef: TMemFieldDef;
  FieldIdx: integer;
  i: integer;
{$ENDIF}
begin
  if Length(Name) > 0 then
  begin
    M := Metadata as TMemDbTableMetadata;
    //Always look in current metadata copy: Indexes don't change until the next commit.
    //However, can still use "current / next" index with respect to data changes.
    Idx := M.IndexByName(abCurrent, Name, IdxIdx);
    if not Assigned(Idx) then
      raise EMemDBAPIException.Create(S_API_INDEX_NAME_NOT_FOUND);
    TagData := TagDataList[IdxIdx];
    if not Assigned(TagData) then
      raise EMemDBInternalException.Create(S_API_INTERNAL_TAG_DATA_BAD);
    CheckTagAgreesWithMetadata(IdxIdx, TagData, tciPermanentAgreesCurrent, tcpProgrammed);
    //Indices are never in transient state in abCurrent, so no difference
    //between ND field index and absolute field index... we hope.
    Sic := IsoToSubIndexClass(Iso);

{$IFDEF DEBUG_DATABASE_NAVIGATE}
    for i := 0 to Pred(Idx.FieldNameCount) do
    begin
      FieldDef := M.FieldByName(abCurrent, Idx.FieldNames[i], FieldIdx);
      Assert(Assigned(FieldDef));
      Assert(FieldIdx >= 0);
      GLogLog(SV_INFO, 'Encode index tag (field ' + InttoStr(i) +'): ' +
      SubIndexClassStrings[Sic] + 'Field index: ' + IntToStr(FieldIdx));
    end;
{$ENDIF}
    result := TagData.TagStructs[Sic];
  end
  else
  begin
{$IFDEF DEBUG_DATABASE_NAVIGATE}
    GLogLog(SV_INFO, 'Encode index tag: InternalRowId.');
{$ENDIF}
    result := MDBInternalIndexRowId.TagStructs[sicCurrent];
    Idx := nil;
  end;
end;

procedure TMemDBTable.IndexDefToFieldDefs(IndexDef: TMemIndexDef; var FieldDefs: TMemFieldDefs; var FieldAbsIdxs: TFieldOffsets);
var
  M: TMemDBTableMetadata;
begin
  Assert(Assigned(IndexDef));
  Assert(IndexDef.FieldNameCount > 0);
  M := Metadata as TMemDbTableMetadata;
  FieldDefs := M.FieldsByNames(abCurrent, IndexDef.FieldArray, FieldAbsIdxs);
end;

function TMemDBTable.API_DataLocate(Iso: TMDBIsolationLevel;
                        Cursor: TItemRec;
                        Pos: TMemAPIPosition;
                        IdxName: string): TItemRec; //Returns cursor.
var
  PTagStruct: PITagStruct;
  Idx: TMemIndexDef;
begin
  if Length(IdxName) = 0 then
    PTagStruct := nil
  else
  begin
    PTagStruct := IndexNameToIndexAndTag(Iso, Idx, IdxName);
    if not Assigned(Idx) then
      raise EMemDBAPIException.Create(S_API_SEARCH_REQUIRES_INDEX);
  end;
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  if Assigned(Cursor) then
  begin
    GLogLog(SV_INFO, 'API_DataLocate (with prev cursor): '
      + MDBIsoStrings[Iso] + ' ' + MemApiPositionStrings[Pos]
      + ' Index name: ' + IdxName);
  end
  else
  begin
    GLogLog(SV_INFO, 'API_DataLocate (no prev cursor): '
      + MDBIsoStrings[Iso] + ' ' + MemApiPositionStrings[Pos]
      + ' Index name: ' + IdxName);
  end;
{$ENDIF}
  result := Data.MoveToRowByIndexTag(Iso, PTagStruct,
                                     Cursor,
                                     Pos);
end;

function TMemDBTable.API_DataFindByIndex(Iso: TMDBIsolationLevel;
                            IdxName: string;
                            const DataRecs: TMemDbFieldDataRecs): TItemRec; //Returns cursor
var
  ITagStruct: PITagStruct;
  Idx: TMemIndexDef;
  FieldDefs: TMemFieldDefs;
  FieldAbsIdxs: TFieldOffsets;
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  i: integer;
{$ENDIF}
begin
  //Cannot find by internal index.
  if Length(IdxName) = 0 then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQUIRES_INDEX);
  ITagStruct := IndexNameToIndexAndTag(Iso, Idx, IdxName);
  if not Assigned(Idx) then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQUIRES_INDEX);
  IndexDefToFieldDefs(Idx, FieldDefs, FieldAbsIdxs);
  if Length(DataRecs)<> Length(FieldDefs) then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQURES_CORRECT_FIELD_COUNT);
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GLogLog(SV_INFO, 'FIND by index, IndexName: ' + Idx.IndexName);
  for i := 0 to Pred(Length(FieldDefs)) do
  begin
    GLogLog(SV_INFO, 'FieldName[' + InttoStr(i) + ']: ' + FieldDefs[i].FieldName + ' FieldNDIndex: ' +
    IntToStr(FieldDefs[i].FieldIndex) + ' FieldAbsIdx: ' + IntToStr(FieldAbsIdxs[i]));
  end;
{$ENDIF}
  result := self.Data.FindRowByIndexTag(Iso, Idx, FieldDefs, ITagStruct, DataRecs);
end;

function TMemDBTable.API_DataFindEdgeByIndex(Iso: TMDBIsolationLevel;
                                             IdxName: string;
                                             const DataRecs: TMemDbFieldDataRecs;
                                             Pos: TMemAPIPosition): TItemRec;
var
  ITagStruct: PITagStruct;
  Idx: TMemIndexDef;
  FieldDefs: TMemFieldDefs;
  FieldAbsIdxs: TFieldOffsets;
  Next: TItemRec;
  NextFields, ResultFields: TMemStreamableList;
  AB: TABSelection;
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  i: integer;
{$ENDIF}
begin
  if Length(IdxName) = 0 then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQUIRES_INDEX);
  ITagStruct := IndexNameToIndexAndTag(Iso, Idx, IdxName);
  if not Assigned(Idx) then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQUIRES_INDEX);
  if not (Pos in [ptFirst, ptLast]) then
    raise EMemDBAPIException.Create(S_API_FIND_EDGE_FIRST_OR_LAST);
  IndexDefToFieldDefs(Idx, FieldDefs, FieldAbsIdxs);
  if Length(DataRecs) <> Length(FieldDefs) then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQURES_CORRECT_FIELD_COUNT);
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GLogLog(SV_INFO, 'EDGE by index, IndexName: ' + Idx.IndexName);
  for i := 0 to Pred(Length(FieldDefs)) do
  begin
    GLogLog(SV_INFO,  ' FieldName['+ IntToStr(i) +']: ' + FieldDefs[i].FieldName + ' FieldNDIndex: ' +
      IntToStr(FieldDefs[i].FieldIndex) + ' FieldAbsIdx: ' + IntToStr(FieldAbsIdxs[i]));
  end;
{$ENDIF}
  Result := self.Data.FindRowByIndexTag(Iso, Idx, FieldDefs, ITagStruct, DataRecs);
  AB := IsoToAB(Iso);
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  if Assigned(Result) then
  begin
    GLogLog(SV_INFO, 'EDGE by Index, initial find '
      + MemAPIPositionStrings[Pos] + ' ' + MDBIsoStrings[Iso] + ' OK');
  end
  else
  begin
    GLogLog(SV_INFO, 'EDGE by Index, initial find '
      + MemAPIPositionStrings[Pos] + ' ' + MDBIsoStrings[Iso] + ' Failed');
  end;
{$ENDIF}
  if Assigned(Result) then
  begin
    case Pos of
      ptFirst: Pos := ptPrevious;
      ptLast: Pos := ptNext;
    else
      Assert(false);
    end;
{$IFDEF DEBUG_DATABASE_NAVIGATE}
    GLogLog(SV_INFO, 'EDGE by Index, move extremity ' + MemAPIPositionStrings[Pos]);
{$ENDIF}
    repeat
      Next := self.Data.MoveToRowByIndexTag(Iso, ITagStruct,
                                             result,
                                             Pos);
      if Assigned(Next) then
      begin
{$IFDEF DEBUG_DATABASE_NAVIGATE}
    GLogLog(SV_INFO, 'EDGE by Index, found ' + MemAPIPositionStrings[Pos] + ' row');
{$ENDIF}

        //Pretty sure we shouldn't have any sentinels at this point.
        NextFields := (Next.Item as TMemDBRow).ABData[AB] as TMemStreamableList;
        ResultFields := (result.Item as TMemDBRow).ABData[AB] as TMemStreamableList;

{$IFDEF DEBUG_DATABASE_NAVIGATE}
        //Compare all fields.
        if SameLayoutFieldsSame(NextFields, ResultFields, FieldAbsIdxs) then
        begin
          GLogLog(SV_INFO, 'EDGE by Index ' + MemAPIPositionStrings[Pos] + ' row has same fields.');
          for i := 0 to Pred(Length(FieldDefs)) do
          begin
            if (NextFields.Items[FieldAbsIdxs[i]] as TMemFieldData).FDataRec.FieldType = ftUnicodeString then
              GLogLog(SV_INFO, 'Field['+ InttoStr(i) +'] is: ' +
                (NextFields.Items[FieldAbsIdxs[i]] as TMemFieldData).FDataRec.sVal);
          end;
        end
        else
        begin
          GLogLog(SV_INFO, 'EDGE by Index ' + MemAPIPositionStrings[Pos] + ' row differs.');
          for i := 0 to Pred(Length(FieldDefs)) do
          begin
            if (NextFields.Items[FieldAbsIdxs[i]] as TMemFieldData).FDataRec.FieldType = ftUnicodeString then
              GLogLog(SV_INFO, 'Field['+ InttoStr(i) +'] is: ' +
                (NextFields.Items[FieldAbsIdxs[i]] as TMemFieldData).FDataRec.sVal);
          end;
        end;
{$ENDIF}
        if not SameLayoutFieldsSame(NextFields, ResultFields, FieldAbsIdxs) then
          Next := nil;
      end;
      if Assigned(Next) then
        result := Next;
    until not Assigned(Next);
  end;
end;

procedure TMemDBTable.API_DataReadRow(Iso: TMDBIsolationLevel;
                         Cursor: TItemRec;
                         FieldList: TMemStreamableList);
var
  Row: TMemDBRow;
  RowFields: TMemStreamableList;
begin
  if not (Assigned(Cursor) and Assigned(FieldList)) then
    raise EMemDBInternalException.Create(S_API_INTERNAL_READING_ROW);
  FieldList.FreeAndClear;
  Row := Cursor.Item as TMemDBRow;
  RowFields := Row.ABData[IsoToAB(Iso)] as TMemStreamableList;
  FieldList.DeepAssign(RowFields);
end;

function TMemDBTable.GetFieldDefList: TMemStreamableList;
var
  M: TMemDBTableMetadata;
  CurMetadata: TMemTableMetadataItem;
begin
  M := Metadata as TMemDbTableMetadata;
  //No Current ABCopy. (maybe handled by no field layout changes).
  if M.Added or M.Null then
    raise EMemDBAPIException.Create(S_API_TABLE_METADATA_NOT_COMMITED);
  //Always looking in current for reasons specified above??
  CurMetadata := M.ABData[abCurrent] as TMemTableMetadataItem;
  result := CurMetadata.FieldDefs;
end;

procedure TMemDBTable.API_DataInitRowForAppend(Iso: TMDBIsolationLevel;
                                  Cursor: TItemRec;
                                  FieldList: TMemStreamableList);
var
  MFields: TMemStreamableList;
  MField: TMemFieldDef;
  MData: TMemFieldData;
  Idx: Integer;
begin
   CheckNoFieldLayoutChanges;
  //Create field list here based on existing metadata.
  if Assigned(Cursor) or (not Assigned(FieldList)) then
    raise EMemDBInternalException.Create(S_API_INTERNAL_READING_ROW);
  FieldList.FreeAndClear;
  MFields := GetFieldDefList;
  if MFields.Count = 0 then
    raise EMemDBAPIException.Create(S_API_NO_FIELDS_IN_TABLE_AT_APPEND_TIME);
  for Idx := 0 to Pred(MFields.Count) do
  begin
    MField := MFields.Items[idx] as TMemFieldDef;
    MData := TMemFieldData.Create;
    MData.FDataRec.FieldType := MField.FieldType;
    Assert(MField.FieldIndex = Idx); //No ND.
    FieldList.Add(MData);
  end;
end;

procedure TMemDBTable.API_DataRowModOrAppend(Iso: TMDBIsolationLevel;
                                var Cursor: TItemRec;
                                FieldList: TMemStreamableList);
var
  MFields: TMemStreamableList;
  MField: TMemFieldDef;
  MData: TMemFieldData;
  Idx: Integer;
begin
   CheckNoFieldLayoutChanges;
  //We will validate new data format here, against existing metadata.
  if not Assigned(FieldList) then
    raise EMemDBInternalException.Create(S_API_INTERNAL_CHANGING_ROW);
  MFields := GetFieldDefList;
  if FieldList.Count <> MFields.Count then
    raise EMemDBInternalException.Create(S_API_ROW_BAD_STRUCTURE);
  for Idx := 0 to Pred(MFields.Count) do
  begin
    MField := MFields.Items[idx] as TMemFieldDef;
    MData := FieldList.Items[idx] as TMemFieldData;
    if MField.FieldType <> MData.FDataRec.FieldType then
      raise EMemDBInternalException.Create(S_API_ROW_BAD_STRUCTURE);
  end;
  self.Data.WriteRowData(Cursor, Iso, FieldList);
end;

procedure TMemDBTable.API_DataRowDelete(Iso: TMDBIsolationLevel;
                                        var Cursor: TItemRec);
begin
  //In an ideal world we could prob allow the row to be deleted anyway...
  //but not sure what would happen with journal replay,
  //and/or index changes, so will play safe.
  CheckNoFieldLayoutChanges;
  if not Assigned(Cursor) then
    raise EMemDBAPIException.Create(S_API_NO_ROW_FOR_DELETE);
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GLogLog(SV_INFO, 'API_DataRowDelete: Actual deletion.');
{$ENDIF}
  Data.DeleteRow(Cursor, Iso);
end;


function TMemDBTable.API_DataGetFieldAbsIdx(Iso: TMDBIsolationLevel;
                                FieldName: string): integer;
var
  M: TMemDbTableMetadata;
  FDef: TMemFieldDef;
begin
  M := Metadata as TMemDbTableMetadata;
  //Always look in current metadata copy: Field layout doesn't change until
  //the next commit.
  FDef := M.FieldByName(abCurrent, FieldName, result);
  if not Assigned(FDef) then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_NOT_FOUND);
end;

{ TMemDBForeignKey}

function TMemDBForeignKey.HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;
begin
  result := inherited;
  if not Assigned(result) then
  begin
    if Assigned(Transaction) then
    begin
      case ID of
        APIForeignKey: result := TMemAPIForeignKey.Create;
      end;
    end;
  end;
end;

function TMemDBForeignKey.LookupTableAndIndex(TableName: string; IndexName: string): TMemIndexDef;
var
  Entity: TMemDBEntity;
  Table: TMemDBTablePersistent;
  tmpIdx: Integer;
  M: TMemDBTableMetadata;
begin
  with ParentDB do
  begin
    //Latest entities, this checks specified params are valid.
    Entity := EntitiesByName(abLatest, TableName, tmpIdx);
    if not (Assigned(Entity) and (Entity is TMemDBTablePersistent)) then
      raise EMemDBAPIException.Create(S_API_ENTITY_NAME_NOT_FOUND);
    Table := Entity as TMemDBTablePersistent;
    M := Table.Metadata as TMemDBTableMetadata;
    result := M.IndexByName(abLatest, IndexName, tmpIdx);
    if not Assigned(result) then
      raise EMemDBAPIException.Create(S_API_INDEX_NAME_NOT_FOUND);
  end;
end;

procedure TMemDBForeignKey.API_SetReferencingChild(TableName: string;
                                  IndexName: string);
var
  M: TMemDBForeignKeyMetadata;
  MI: TMemForeignKeyMetadataItem;
begin
  LookupTableAndIndex(TableName, IndexName);
  //Referencing field/index does not need to be unique.
  //Referenced field/index does.
  M := Metadata as TMemDBForeignKeyMetadata;
  M.RequestChange;
  Assert(AssignedNotSentinel(M.ABData[abLatest]));
  MI := M.ABData[abLatest] as TMemForeignKeyMetadataItem;
  MI.TableReferer := TableName;
  MI.IndexReferer := IndexName;
  //Do not need to re-gen any internal structures.
end;

procedure TMemDBForeignKey.API_SetReferencedParent(TableName: string;
                                  IndexName: string);
var
  IdxDef: TMemIndexDef;
  M: TMemDBForeignKeyMetadata;
  MI: TMemForeignKeyMetadataItem;
begin
  IdxDef := LookupTableAndIndex(TableName, IndexName);
  //Referencing field/index does not need to be unique.
  //Referenced field/index does.
  if not (iaUnique in IdxDef.IndexAttrs) then
    raise EMemDbAPIException.Create(S_API_INDEX_FOR_FK_UNIQUE_ATTR);
  M := Metadata as TMemDBForeignKeyMetadata;
  M.RequestChange;
  Assert(AssignedNotSentinel(M.ABData[abLatest]));
  MI := M.ABData[abLatest] as TMemForeignKeyMetadataItem;
  MI.TableReferred := TableName;
  MI.IndexReferred := IndexName;
  //Do not need to re-gen any internal structures.
end;

procedure TMemDBForeignKey.API_GetReferencingChild(Iso: TMDBIsolationLevel;
                                  var TableName: string;
                                  var IndexName: string);
var
  M: TMemDBForeignKeyMetadata;
  MI: TMemForeignKeyMetadataItem;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  M := Metadata as TMemDBForeignKeyMetadata;
  if M.HasData[AB] then
  begin
    MI := M.ABData[AB] as TMemForeignKeyMetadataItem;
    TableName := MI.TableReferer;
    IndexName := MI.IndexReferer;
  end;
end;

procedure TMemDBForeignKey.API_GetReferencedParent(Iso: TMDBIsolationLevel;
                                  var TableName: string;
                                  var IndexName: string);
var
  M: TMemDBForeignKeyMetadata;
  MI: TMemForeignKeyMetadataItem;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  M := Metadata as TMemDBForeignKeyMetadata;
  if M.HasData[AB] then
  begin
    MI := M.ABData[AB] as TMemForeignKeyMetadataItem;
    TableName := MI.TableReferred;
    IndexName := MI.IndexReferred;
  end;
end;


end.
