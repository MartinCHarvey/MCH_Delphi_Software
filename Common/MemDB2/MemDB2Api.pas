unit MemDB2Api;
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

{ Unit containing most of the user API for MemDB.
  Basic DB and transaction setup accessible via the MemDB unit }

interface

{$IFDEF DEBUG_DATABASE_DELETE}
{$DEFINE DEBUG_DATABASE_NAVIGATE}
{$ENDIF}

uses
  MemDB2Buffered, MemDb2Misc, MemDb2Streamable, MemDB2Indexing, Classes,
  IndexedStore;

type
  TMemDBDatabase = class;
  TMemDBTable = class;
  TMemDBForeignKey = class;

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
    function UserCommitCycleV3: TStream;
    procedure UserRollbackCycleV3;
  end;

  TMemAPIDatabase = class(TMemAPIDBTopLevel)
  public
    procedure CreateTable(Name: string);
    procedure CreateForeignKey(Name: string);
    function GetAPIObjectFromEntity(Name: string; API: TMemDBAPIId): TMemDBAPI;
    procedure RenameTableOrKey(OldName, NewName: string);
    procedure DeleteTableOrKey(Name: string);
    function GetEntityNames: TStringList;
  end;

  TMemAPITable = class(TMemDBAPI)
  protected
    procedure CheckNoDataChanges;
    procedure CheckNoLayoutChanges;

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
  end;

  TMemAPITableData = class(TMemAPITable)
  private
    FCursor: TMemDBCursor;
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

    //FindEdgeByIndex lets you find first/last entries with a given value.
    // With GetNext / GetPrevious
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

    //Row deletion.
    //Optionally autoincrements on the most recent index/container you
    //were iterating through.
    procedure Delete(AutoInc: boolean = true);
    function RowSelected: boolean;
  end;

  //Foreign key operations.
  TMemAPIForeignKey = class(TMemDBAPI)
  protected
{$IFDEF MEMDB2_TEMP_REMOVE}
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
{$ENDIF}
  end;

  //TODO TODO - Make sure no metadata pinning here
  //unless tidLocal initialized.

  //Extensions to double buffered persistent objects to perform
  //API operations. These perform consistency checking on the operations,
  //and do not maintain an per-transaction state.
  TMemDbDatabase = class(TMemDbDatabasePersistent)
  public
    procedure API_CreateTable(T: TObject; Name: string);
    procedure API_CreateForeignKey(T: TObject; Name: string);
    procedure API_RenameTableOrKey(T: TObject; OldName, NewName: string);
    procedure API_DeleteTableOrKey(T: TObject; Name: string);
    function API_GetEntityNames(T: TObject): TStringList;
    function API_GetAPIObjectFromEntity(T: TObject; Name: string; API: TMemDBAPIId): TMemDBAPI;
    function HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;override;
  end;

  TMemDBTable = class(TMemDbTablePersistent)
  protected

    //Unfortunately, very first touch/pin of metadata needs to be by TidLocal, so that
    //CC fields/indexes are in sync with TidLocal index roots, hence all the wrapping here.

    //TODO - Optimise the use of these META functions, so that we don't need
    // to repeatedly query for the TidLocal structure.

    procedure META_CurIndexDefToFieldDefs(const Tid: TTransactionId; IndexDef: TMemIndexDef; var FieldDefs: TMemFieldDefs; var FieldAbsIdxs: TFieldOffsets);
    function META_CurFieldDefList(const Tid: TTransactionId): TMemStreamableList;
  public
    procedure API_CreateField(T: TObject; Name: string; FieldType: TMDBFieldType);
    procedure API_DeleteField(T: TObject; Name: string);
    procedure API_RenameField(T: TObject; OldName: string; NewName: string);
    function API_FieldInfo(T: TObject; Name: string; var FieldType: TMDBFieldType): boolean;
    function API_GetFieldNames(T: TObject): TStringList;

    procedure API_CreateIndex(T: TObject;
                          Name: string;
                          IndexedFields: TMDBFieldNames;
                          IndexAttrs: TMDbIndexAttrs);
    procedure API_DeleteIndex(T: TObject; Name: string);
    procedure API_RenameIndex(T: TObject; OldName: string; NewName: string);
    function API_IndexInfo(T: TObject; Name: string;
                       var IndexedFields: TMDBFieldNames;
                       var IndexAttrs: TMDBIndexAttrs): boolean;
    function API_GetIndexNames(T: TObject): TStringList;

    function API_DataLocate(T:TObject;
                            Cursor: TMemDBCursor;
                            Pos: TMemAPIPosition;
                            IdxName: string): TMemDBCursor; //Returns cursor.

    function API_DataFindByIndex(T: TObject;
                                IdxName: string;
                                const DataRecs: TMemDbFieldDataRecs): TMemDBCursor; //Returns cursor

    function API_DataFindEdgeByIndex(T:TObject;
                                     IdxName: string;
                                     const DataRecs: TMemDbFieldDataRecs;
                                     Pos: TMemAPIPosition): TMemDBCursor; //Returns cursor.

    procedure API_DataReadRow(T:TObject;
                             Cursor: TMemDBCursor;
                             FieldList: TMemStreamableList);

    procedure API_DataInitRowForAppend(T: TObject;
                                      Cursor: TMemDBCursor;
                                      FieldList: TMemStreamableList);

    function API_DataRowModOrAppend(T: TObject;
                                    Cursor: TMemDBCursor;
                                    FieldList: TMemStreamableList): TMemDBCursor;

    function API_DataRowDelete(T: TObject;
                               Cursor: TMemDBCursor;
                               AutoInc: boolean): TMemDBCursor;

    function API_DataGetFieldAbsIdx(T: TObject;
                                    FieldName: string): integer;

public
    function HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;override;
  end;

  TMemDBForeignKey = class(TMemDbForeignKeyPersistent)
{$IFDEF MEMDB2_TEMP_REMOVE}
  protected
    function LookupTableAndIndex(TableName: string; IndexName: string): TMemIndexDef;
  public

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
{$ENDIF}
public
    function HandleInterfacedObjRequest(Transaction: TObject; ID: TMemDBAPIId): TMemDBAPI;override;
  end;

function MakeStream(StorageMode: TTempStorageMode): TStream;

implementation

uses
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GlobalLog,
{$ENDIF}
  MemDB2, SysUtils, IoUtils, BufferedFileStream, Math, NullStream, MemDB2BufBase,
  Reffed;

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
  S_API_ENTITY_NAME_CONFLICT =
    'Something already exists with that name. Perhaps commit previous renames / deletions?';
  S_API_ENTITY_NAME_NOT_FOUND = 'Couldn''t find anything with that name.';
  S_API_FIELD_NAME_CONFLICT =
    'A field already exists with that name. Perhaps commit previous renames / deletions?';
  S_API_FIELD_NAME_NOT_FOUND = 'Field name not found.';
  S_API_FIELDS_IN_INDEX_MUST_BE_DISJOINT = 'Cannot specify a field name more than once in an index.';
  S_API_INDEX_REFERENCES_FIELD = 'Cannot delete field, it is referenced by an index.';
  S_API_INTERNAL_ERROR = 'API implementation internal error.';
  S_API_INDEX_NAME_NULL = 'Index name should be non-empty.';
  S_API_INDEX_NAME_CONFLICT = 'An index already exists with that name. Perhaps commit previous renames / deletions?';
  S_API_INDEX_MUST_HAVE_FIELDS = 'You must specify some fields to index on';
  S_API_INTERNAL_TAG_DATA_BAD = 'Index does not correspond with a valid tag';
  S_API_INTERNAL_READING_ROW = 'Internal error reading row data.';
  S_API_TABLE_METADATA_NOT_COMMITED = 'No committed metadata for this table (api level)';
  S_API_INTERNAL_CHANGING_ROW = 'Internal error changing row data.';
  S_API_ROW_BAD_STRUCTURE = 'Error changing row, field layout different from table.';
  S_API_SEARCH_REQUIRES_INDEX = 'Bad index name, or index not found, comitted indices only.';
  S_API_SEARCH_REQURES_CORRECT_FIELD_COUNT = 'Number of data fields supplied for search must be same as number of fields indexed.';
  S_API_NO_FIELDS_IN_TABLE_AT_APPEND_TIME = 'Cannot append a record until you have added fields to the table.';
  S_API_NO_ROW_FOR_DELETE = 'You need to navigate to a row before you can delete it.';
  S_API_INDEX_FOR_FK_UNIQUE_ATTR = 'Foreign key: referenced index must have unique attribute set.';
  S_FROM_SCRATCH_NOT_ALLOWED_MULTI = 'Checkpoint and first init changesets should not be in a multi-transaction';
  S_TABLE_FORMAT_CONCURRENTLY_CHANGED_1 = 'Concurrency: Table format changed whilst iterating (1).';
  S_TABLE_FORMAT_CONCURRENTLY_CHANGED_2 = 'Concurrency: Table format changed whilst iterating (2).';
  S_TABLE_FORMAT_CONCURRENTLY_CHANGED_3 = 'Concurrency: Table format changed whilst reading (3).';

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
   PseudoTid: TTransactionId;
begin
  if Initial then
    Reason := mtrReplayFromScratch
  else
    Reason := mtrReplayFromJournal;

  PseudoTid := TTransactionId.NewTransactionID(ilSerialisable); //if only writer, should be serialisable.

  try
    Assert(not DB.AnyChanges(PseudoTid));
    if Initial then
      DB.FromScratch(PseudoTid, JournalEntry)
    else
      DB.FromJournal(PseudoTid, JournalEntry);

    DB.CommitLock.Acquire; //TODO - We will get rid of the commit lock eventually.
    try
      DB.PreCommit(PseudoTid, Reason);
      DB.Commit(PseudoTid, Reason);
    finally
      DB.CommitLock.Release;
    end;
  except
    //Clear pins etc, although in this case, it's
    //a non-start for the database.
    //However, final deletion of entities should still work
    //and delete everything provided we clear the pins.
    DB.Rollback(PseudoTid, Reason);
    raise;
  end;
end;

function TMemAPIDatabaseInternal.UserCommitCycleV3: TStream;
var
  T: TMemDBTransaction;
begin
  T := FAssociatedTransaction as TMemDBTransaction;
  Assert(Assigned(T));
  Assert(not Assigned(T.Changeset));
  result := MakeStream(T.Session.TempStorageMode);
  try
    //TODO - Check Journalling before pre-commit works as we expect.
    DB.ToJournal(T.Tid, result);

    DB.CommitLock.Acquire; //TODO - We will get rid of the commit lock eventually.
    try
      DB.PreCommit(T.Tid, mtrUserOp);
      DB.Commit(T.Tid, mtrUserOp);
    finally
      DB.CommitLock.Release;
    end;
  except
    on Exception do
    begin
      result.Free;
      raise; //Rely on later rollback etc to clear pins and such.
    end;
  end;
end;

procedure TMemAPIDatabaseInternal.UserRollbackCycleV3;
var
  T: TMemDBTransaction;
begin
  T := FAssociatedTransaction as TMemDBTransaction;
  Assert(Assigned(T));
  //Should not raise exceptions.
  DB.Rollback(T.Tid, mtrUserOp);
end;

{ TMemAPIDatabase }

procedure TMemAPIDatabase.CreateTable(Name: string);
begin
  CheckWriteTransaction;
  DB.API_CreateTable(FAssociatedTransaction, Name);
end;

procedure TMemAPIDatabase.CreateForeignKey(Name: string);
begin
  CheckWriteTransaction;
  DB.API_CreateForeignKey(FAssociatedTransaction, Name);
end;

procedure TMemAPIDatabase.RenameTableOrKey(OldName, NewName: string);
begin
  CheckWriteTransaction;
  DB.API_RenameTableOrKey(FAssociatedTransaction, OldName, NewName);
end;

procedure TMemAPIDatabase.DeleteTableOrKey(Name: string);
begin
  CheckWriteTransaction;
  DB.API_DeleteTableOrKey(FAssociatedTransaction, Name);
end;

function TMemAPIDatabase.GetEntityNames: TStringList;
begin
  result := DB.API_GetEntityNames(FAssociatedTransaction);
end;

function TMemAPIDatabase.GetAPIObjectFromEntity(Name: string; API: TMemDBAPIId): TMemDBAPI;
begin
  result := DB.API_GetApiObjectFromEntity(FAssociatedTransaction,Name, API);
end;

{ TMemAPITable }

function TMemAPITable.GetInterfacedObject: TMemDBTable;
begin
  result := FInterfacedObject.Parent as TMemDBTable;
end;

procedure TMemAPITable.CheckNoDataChanges;
var
  T: TMemDbTable;
  Tr: TMemDbTransaction;
begin
  T := Table;
  Tr := self.FAssociatedTransaction as TMemDBTransaction;
  if T.DataChangedForTid(Tr.Tid) then
    raise EMemDBAPIException.Create(S_FIELD_LAYOUT_CHANGED);
end;

procedure TMemAPITable.CheckNoLayoutChanges;
var
  T: TMemDbTable;
  Tr: TMemDbTransaction;
begin
  T := Table;
  Tr := self.FAssociatedTransaction as TMemDBTransaction;
  if T.LayoutChangesRequiredForTid(TR.Tid) then
    raise EMemDBAPIException.Create(S_FIELD_LAYOUT_CHANGED);
end;

{ TMemAPITableMetadata }

procedure TMemAPITableMetadata.CreateField(Name: string; FieldType: TMDBFieldType);
begin
  CheckWriteTransaction;
  CheckNoDataChanges;
  Table.API_CreateField(FAssociatedTransaction, Name, FieldType);
end;

procedure TMemAPITableMetadata.DeleteField(Name: string);
begin
  CheckWriteTransaction;
  CheckNoDataChanges;
  Table.API_DeleteField(FAssociatedTransaction, Name);
end;

procedure TMemAPITableMetadata.RenameField(OldName: string; NewName: string);
begin
  CheckWriteTransaction;
  CheckNoDataChanges;
  Table.API_RenameField(FAssociatedTransaction, OldName, NewName);
end;

function TMemAPITableMetadata.FieldInfo(Name: string; var FieldType: TMDBFieldType): boolean;
begin
  result := Table.API_FieldInfo(FAssociatedTransaction, Name, FieldType)
end;

function TMemAPITableMetadata.GetFieldNames: TStringList;
begin
  result := Table.API_GetFieldNames(FAssociatedTransaction);
end;

procedure TMemAPITableMetadata.CreateIndex(Name: string;
                                           IndexedField: string;
                                           IndexAttrs: TMDbIndexAttrs);
var
  Fields: TMDBFieldNames;
begin
  CheckWriteTransaction;
  SetLength(Fields,1);
  Fields[0] := IndexedField;
  Table.API_CreateIndex(FAssociatedTransaction, Name, Fields, IndexAttrs);
end;

procedure TMemAPITableMetadata.CreateMultiFieldIndex(Name: string;
                                                     IndexedFields: TMDBFieldNames;
                                                     IndexAttrs: TMDbIndexAttrs);
begin
  CheckWriteTransaction;
  Table.API_CreateIndex(FAssociatedTransaction, Name, IndexedFields, IndexAttrs);
end;


procedure TMemAPITableMetadata.DeleteIndex(Name: string);
begin
  CheckWriteTransaction;
  Table.API_DeleteIndex(FAssociatedTransaction, Name);
end;

procedure TMemAPITableMetadata.RenameIndex(OldName: string; NewName: string);
begin
  CheckWriteTransaction;
  Table.API_RenameIndex(FAssociatedTransaction, OldName, NewName);
end;

function TMemAPITableMetadata.IndexInfo(Name: string;
                                        var IndexedFields: TMDBFieldNames;
                                        var IndexAttrs: TMDBIndexAttrs): boolean;
begin
  result := Table.API_IndexInfo(FAssociatedTransaction, Name, IndexedFields, IndexAttrs);
end;

function TMemAPITableMetadata.GetIndexNames: TStringList;
begin
  result := Table.API_GetIndexNames(FAssociatedTransaction);
end;


{ TMemAPITableData }

procedure TMemAPITableData.DiscardInternal;
begin
  if FFieldListDirty then
    raise EMemDBAPIException.Create(S_API_POST_OR_DISCARD_BEFORE_NAVIGATING);
  FFieldList.ReleaseAndClear;
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
  FFieldList.Release;
  FCursor.Free;
  inherited;
end;

function TMemAPITableData.Locate(Pos: TMemAPIPosition; IdxName: string = ''): boolean;
var
  NCursor:TMemDBCursor;
begin
  DiscardInternal;
  NCursor := Table.API_DataLocate(FAssociatedTransaction, FCursor, Pos, IdxName);
  if NCursor <> FCursor then
  begin
    FCursor.Free;
    FCursor := NCursor;
  end;
  result := Assigned(FCursor);
  if result then
    Table.API_DataReadRow(FAssociatedTransaction, FCursor, FFieldList);
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
var
  NCursor:TMemDBCursor;
begin
  DiscardInternal;
  NCursor := Table.API_DataFindByIndex(FAssociatedTransaction, IdxName, DataRecs);
  if NCursor <> FCursor then
  begin
    FCursor.Free;
    FCursor := NCursor;
  end;
  result := Assigned(FCursor);
  if result then
    Table.API_DataReadRow(FAssociatedTransaction, FCursor, FFieldList);
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
var
  NCursor:TMemDBCursor;
begin
  DiscardInternal;
  NCursor := Table.API_DataFindEdgeByIndex(FAssociatedTransaction,
                                           IdxName, DataRecs, Pos);
  if NCursor <> FCursor then
  begin
    FCursor.Free;
    FCursor := NCursor;
  end;
  result := Assigned(FCursor);
  if result then
    Table.API_DataReadRow(FAssociatedTransaction, FCursor, FFieldList);
end;

procedure TMemAPITableData.Append;
begin
  CheckWriteTransaction;
  DiscardInternal;
  FCursor.Free;
  FCursor := nil;
  FAdding := true;
  Table.API_DataInitRowForAppend(FAssociatedTransaction, FCursor, FFieldList);
  //No point reading, fields all zero.
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
  FieldAbsIdx := Table.API_DataGetFieldAbsIdx(FAssociatedTransaction, FieldName);
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
  CheckWriteTransaction;
  if not (Assigned(FCursor) or FAdding) then
    raise EMemDBAPIException.Create(S_API_NO_ROW_SELECTED);
  if FFieldList.Count = 0 then
    raise EMemDBAPiException.Create(S_API_NO_FIELDS_DUP_POST_OR_DISCARD);
  FieldAbsIdx := Table.API_DataGetFieldAbsIdx(FAssociatedTransaction, FieldName);
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
var
  NewCursor: TMemDBCursor;
begin
  CheckWriteTransaction;
  //Do the post.
  NewCursor := Table.API_DataRowModOrAppend(FAssociatedTransaction, FCursor, FFieldList);
  Discard;
  Assert(NewCursor <> FCursor);
  FCursor.Free;
  FCursor := NewCursor;
end;

procedure TMemAPITableData.Delete(AutoInc: boolean);
var
  NextCursor: TMemDBCursor;
begin
  CheckWriteTransaction;
  if not Assigned(FCursor) then
    raise EMemDBAPIException.Create(S_API_NO_ROW_SELECTED);

  Discard;
  NextCursor := Table.API_DataRowDelete(FAssociatedTransaction, FCursor, AutoInc);
  Assert(NextCursor <> FCursor);
  FCursor.Free;
  FCursor := NextCursor;
  if Assigned(FCursor) then
    Table.API_DataReadRow(FAssociatedTransaction, FCursor, FFieldList);
end;

function TMemAPITableData.RowSelected: boolean;
begin
  result := Assigned(FCursor);
end;

{ TMemAPIForeignKey }

{$IFDEF MEMDB2_TEMP_REMOVE}

function TMemAPIForeignKey.GetInterfacedObject: TMemDBForeignKey;
begin
  result := FInterfacedObject.Parent as TMemDBForeignKey;
end;

procedure TMemAPIForeignKey.SetReferencingChild(TableName: string;
                              IndexName: string);
begin
  CheckWriteExclusiveTransaction;
  ForeignKey.API_SetReferencingChild(TableName,IndexName);
end;

procedure TMemAPIForeignKey.SetReferencedParent(TableName: string;
                              IndexName: string);
begin
  CheckWriteExclusiveTransaction;
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

{$ENDIF}

//////////////////////////////////////////////////////////////////////

{ TMemDbDatabase }

//Generally, don't try to undelete, unrename etc etc in the same transaction.
procedure TMemDBDatabase.API_CreateTable(T:TObject; Name: string);
var
  Ent: TMemDBEntity;
  NewTable: TMemDbTable;
  Tr: TMemDBTransaction;
  BS: TBufSelector;
begin
  Tr := T as TMemDBTransaction;
  BS := MakeLatestBufSelector(Tr.Tid);
  //These checks are not definitive, not until pre-commit can we be sure we're OK.
  //Checking next buffers is however, definitive.
  Ent := EntitiesByName(BS, Name, pinEvolve);
  if Assigned(Ent) then
  begin
    Ent.Proxy.Release;
    raise EMemDBAPIException.Create(S_API_ENTITY_NAME_CONFLICT);
  end;
  NewTable := TMemDBTable.Create;
  NewTable.Init(Tr.Tid, self, Name, true);
  NewTable.Proxy.Release;
end;

procedure TMemDBDatabase.API_CreateForeignKey(T: TObject; Name: string);
var
  Ent: TMemDBEntity;
  NewKey: TMemDbForeignKey;
  Tr: TMemDBTransaction;
  BS: TBufSelector;
begin
  Tr := T as TMemDbTransaction;
  BS := MakeLatestBufSelector(Tr.Tid);
  //These checks are not definitive, not until pre-commit can we be sure we're OK.
  //Checking next buffers is however, definitive.
  Ent := EntitiesByName(BS, Name, pinEvolve);
  if Assigned(Ent) then
  begin
    Ent.Proxy.Release;
    raise EMemDBAPIException.Create(S_API_ENTITY_NAME_CONFLICT);
  end;
  NewKey := TMemDBForeignKey.Create;
  NewKey.Init(Tr.Tid, self, Name, true);
  NewKey.Proxy.Release;
end;

procedure TMemDBDatabase.API_RenameTableOrKey(T: TObject; OldName, NewName: string);
var
  Entity: TMemDBEntity;
  NNEntity: TMemDBEntity;
  Tr: TMemDBTransaction;
  AB: TBufSelector;
  SelLatest: TBufSelector;
  RefNext: TMemDBStreamable;
  NextM : TMemDBStreamable;
begin
  Tr := T as TMemDBTransaction;
  AB := MakeLatestBufSelector(Tr.Tid);
  SelLatest := MakeLatestBufSelector(Tr.Tid);

  //These checks are not definitive, not until pre-commit can we be sure we're OK.
  //Checking next buffers is however, definitive.
  Entity := EntitiesByName(AB, OldName, pinEvolve);
  if not Assigned(Entity) then
    raise EMemDbAPIException.Create(S_API_ENTITY_NAME_NOT_FOUND);

  try
    //AB sel might be current (committed), not next.
    RefNext := Entity.META_GetNext(Tr.Tid);
    if Assigned(RefNext) and (RefNext is TMemDeleteSentinel) then
      raise EMemDbAPIException.Create(S_API_ENTITY_NAME_NOT_FOUND);

    //Check new name not used.
    NNEntity := EntitiesByName(SelLatest, NewName, pinEvolve);
    if Assigned(NNEntity) then
    begin
      NNEntity.Proxy.Release;
      raise EMemDBAPIException.Create(S_API_ENTITY_NAME_CONFLICT);
    end;

    //Check renaming from existing name (if changes already present).
    if AssignedNotSentinel(RefNext as TMemDbStreamable) then
    begin
      //If latest rename is not what we're renaming from, then stop.
      if (RefNext as TMemEntityMetadataItem)
        .EntityName <> OldName then
      raise EMemDBAPIException.Create(S_API_RENAME_OVERWRITE);
    end;

    Entity.META_RequestChange(Tr.Tid);
    NextM := Entity.META_GetNext(Tr.Tid);
    Assert((not Assigned(RefNext)) or (RefNext = NextM));
    Assert(not (NextM is TMemDeleteSentinel));
    (NextM as TMemEntityMetadataItem).EntityName := NewName;
    HandleAPITableRename(AB, OldName, NewName);
  finally
    Entity.Proxy.Release;
  end;
end;

procedure TMemDBDatabase.API_DeleteTableOrKey(T: TObject; Name: string);
var
  Tr: TMemDbTransaction;
  Entity: TMemDBEntity;
  AB: TBufSelector;
begin
  Tr := T as TMemDBTransaction;
  AB := MakeLatestBufSelector(Tr.Tid);
  //Exists.
  Entity := EntitiesByName(AB, Name, pinEvolve);
  if not Assigned(Entity) then
    raise EMemDbAPIException.Create(S_API_ENTITY_NAME_NOT_FOUND);
  try
    //Referenced by anything else?
    if Entity is TMemDbTablePersistent then
      CheckAPITableDelete(AB, Name);
    Entity.META_Delete(Tr.Tid);
  finally
    Entity.Proxy.Release;
  end;
end;

function TMemDBDatabase.API_GetEntityNames(T: TObject): TStringList;
var
  Tr: TMemDBTransaction;
  AB: TBufSelector;
  EntityList: TReffedList;
  Proxy: TMemDBEntityProxy;
  Entity: TMemDBEntity;
  i: integer;
  MDReffed: TMemDBStreamable;
  MDItem: TMemEntityMetadataItem;
  tmpBufSel: TABSelType;
begin
  Tr := T as TMemDbTransaction;
  AB := MakeLatestBufSelector(Tr.Tid);
  result := TStringList.Create;
  EntityList := AssembleEntityList;
  try
    for i := 0 to Pred(EntityList.Count) do
    begin
      Proxy := EntityList.Items[i] as TMemDBEntityProxy;
      Entity := Proxy.Proxy as TMemDBEntity;
      case AB.SelType of
        abCurrent: MDReffed := Entity.META_PinCurrent(AB.TId, pinEvolve);
        abNext: MDReffed := Entity.META_GetNext(AB.TId);
        abLatest: MDReffed := Entity.META_GetPinLatest(AB.TId, tmpBufSel, pinEvolve);
      else
        Assert(false);
        MdReffed := nil;
      end;
      if AssignedNotSentinel(MDReffed as TMemDbStreamable) then
      begin
        MdItem := MDReffed as TMemEntityMetadataItem;
        result.Add(MDItem.EntityName);
      end;
    end;
  finally
    EntityList.Release;
  end;
end;

function TMemDBDatabase.API_GetAPIObjectFromEntity(T: TObject; Name: string; API: TMemDBAPIId): TMemDBAPI;
var
  Tr: TMemDbTransaction;
  AB: TBufSelector;
  Entity: TMemDbEntity;
begin
  Tr := T as TMemDBTransaction;
  AB := MakeLatestBufSelector(Tr.Tid);
  Entity := EntitiesByName(AB, Name, pinEvolve);
  result := nil;
  if Assigned(Entity) then
  begin
    try
      result := Entity.Interfaced.GetAPIObject(Tr, API, false);
    finally
      Entity.Proxy.Release;
    end;
  end
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
      end;
    end;
  end;
end;

{ TMemDBTable }

procedure TMemDBTable.API_CreateField(T: TObject; Name: string; FieldType: TMDBFieldType);
var
  tmpIdx: integer;
  NewField: TMemFieldDef;
  Sel: TBufSelector;
  Tr: TMemDBTransaction;
  TidLocal: TTidLocal;
  FieldHelper, IndexHelper: TMemDblBufListHelper;
begin
  Tr := T as TMemDbTransaction;
  Sel := MakeLatestBufSelector(Tr.Tid);
  if Assigned(META_FieldByName(Sel, Name, tmpIdx)) then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_CONFLICT);
  //Do data manipulation with list helper.
  TidLocal := GetMakeListHelpers(Tr.Tid, FieldHelper, IndexHelper);
  NewField := TMemFieldDef.Create;
  try
    NewField.FieldName := Name;
    NewField.FieldType := FieldType;
    tmpIdx := FieldHelper.AddNoRef(NewField);
    NewField.FieldIndex := FieldHelper.RawIndexToNdIndex(tmpIdx);
    NewField := nil;
  finally
    NewField.Release;
  end;
  TidLocal.UpdateLayout(pinEvolve);
end;

procedure TMemDBTable.API_DeleteField(T: TObject; Name: string);
var
  FieldIdx, FieldIdx2: integer;
  IndexIdx, IdxFIdx: integer;
  Field: TMemFieldDef;
  Index: TMemIndexDef;
  Sel: TBufSelector;
  Tr: TMemDBTransaction;
  TidLocal: TTidLocal;
  FieldHelper, IndexHelper: TMemDblBufListHelper;
begin
  Tr := T as TMemDbTransaction;
  Sel := MakeLatestBufSelector(Tr.Tid);
  Field := META_FieldByName(Sel, Name, FieldIdx);
  if not Assigned(Field) then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_NOT_FOUND);
  TidLocal := GetMakeListHelpers(Tr.Tid, FieldHelper, IndexHelper);
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
  TidLocal.UpdateLayout(pinEvolve);
end;

procedure TMemDBTable.API_RenameField(T: TObject; OldName: string; NewName: string);
var
  Field: TMemFieldDef;
  FieldIdx, tmpIdx: integer;
  IndexIdx, IdxFIdx: integer;
  Index: TMemIndexDef;
  Sel: TBufSelector;
  Tr: TMemDBTransaction;
  FieldHelper, IndexHelper: TMemDblBufListHelper;
  TidLocal: TTidLocal;
begin
  Tr := T as TMemDbTransaction;
  Sel := MakeLatestBufSelector(Tr.Tid);
  Field := META_FieldByName(Sel, OldName, FieldIdx);
  TidLocal := GetMakeListHelpers(Tr.Tid, FieldHelper, IndexHelper);
  if (not Assigned(Field)) or (FieldHelper.ChildDeleted[FieldIdx]) then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_NOT_FOUND);
  if Assigned(META_FieldByName(Sel, NewName, tmpIdx)) then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_CONFLICT);
  if AssignedNotSentinel(META_GetNext(Tr.Tid)) then
  begin
    //Check there has not been a previous conflicting rename.
    //If previously been deleted, ChildDeleted check above should fail.
    Assert (AssignedNotSentinel((META_GetNext(Tr.Tid) as TMemTableMetadataItem).FieldDefs.Items[FieldIdx]));
    Field := (META_GetNext(Tr.Tid) as TMemTableMetadataItem).FieldDefs.Items[FieldIdx]
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
  TidLocal.UpdateLayout(pinEvolve);
end;

function TMemDBTable.API_FieldInfo(T: TObject;  Name: string; var FieldType: TMDBFieldType): boolean;
var
  Field: TMemFieldDef;
  FieldIdx: integer;
  Sel: TBufSelector;
  Tr: TMemDbTransaction;
begin
  Tr := T as TMemDbTransaction;
  Sel := MakeLatestBufSelector(Tr.Tid);
  Field := META_FieldByName(Sel, Name, FieldIdx);
  result := Assigned(Field);
  if result then
    FieldType := Field.FieldType;
end;

function TMemDBTable.API_GetFieldNames(T: TObject): TStringList;
var
  MetadataCopy: TMemTableMetadataItem;
  FieldDef: TMemFieldDef;
  idx: integer;
  Tr: TMemDBTransaction;
  Selected: TABSelType;
begin
  Tr := T as TMemDbTransaction;
  result := TStringList.Create;
  if AssignedNotSentinel(META_GetPinLatest(Tr.Tid, Selected, pinEvolve)) then
  begin
    MetadataCopy := META_GetPinLatest(Tr.Tid, Selected, pinEvolve) as TMemTableMetadataItem;
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

procedure TMemDBTable.API_CreateIndex(T: TObject; Name: string;
                      IndexedFields: TMDBFieldNames;
                      IndexAttrs: TMDbIndexAttrs);
var
  Index: TMemIndexDef;
  IndexIdx: integer;
  Fields: TMemFieldDefs;
  FieldAbsIdxs: TFieldOffsets;
  x,y: integer;
  Sel: TBufSelector;
  Tr: TMemDbTransaction;
  TidLocal: TTidLocal;
  FieldHelper, IndexHelper: TMemDblBufListHelper;
begin
  Tr := T as TMemDbTransaction;
  Sel := MakeLatestBufSelector(Tr.Tid);
  if Length(Name) = 0 then
    raise EMemDBAPIException.Create(S_API_INDEX_NAME_NULL);
  if Assigned(META_IndexByName(Sel, Name, IndexIdx)) then
    raise EMemDBAPIException.Create(S_API_INDEX_NAME_CONFLICT);
  if Length(IndexedFields) = 0 then
    raise EMemDbApiException.Create(S_API_INDEX_MUST_HAVE_FIELDS);
  Sel := MakeLatestBufSelector(Tr.Tid);
  Fields := META_FieldsByNames(Sel, IndexedFields, FieldAbsIdxs);
  if Length(Fields) = 0 then
    raise EMemDBAPIException.Create(S_API_FIELD_NAME_NOT_FOUND);
  Assert(Length(Fields) = Length(IndexedFields));
  TidLocal := GetMakeListHelpers(Tr.Tid, FieldHelper, IndexHelper);
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
  IndexIdx := IndexHelper.AddNoRef(Index);
  Assert(IndexHelper.ChildAdded[IndexIdx]);
  TidLocal.UpdateLayout(pinEvolve);
end;

procedure TMemDBTable.API_DeleteIndex(T: TObject; Name: string);
var
  MT: TMemTableMetadataItem;
  Index: TMemIndexDef;
  IndexIdx: integer;
  Tr: TMemDbTransaction;
  TidLocal: TTidLocal;
  Sel: TBufSelector;
  Selected: TABSelType;
  FieldHelper, IndexHelper: TMemDblBufListHelper;
begin
  Tr := T as TMemDbTransaction;
  Sel := MakeLatestBufSelector(Tr.Tid);
  TidLocal := GetMakeListHelpers(Tr.Tid, FieldHelper, IndexHelper);
  Index := META_IndexByName(Sel, Name, IndexIdx);
  if not Assigned(Index) then
    raise EMemDbAPIException.Create(S_API_INDEX_NAME_NOT_FOUND);
  Assert(AssignedNotSentinel(META_GetPinLatest(Tr.Tid, Selected, pinEvolve)));
  MT := META_GetPinLatest(Tr.Tid, Selected, pinEvolve) as TMemTableMetadataItem;
  ParentDB.CheckAPIIndexDelete(sel, MT.EntityName, Name);
  //Delete index.
  IndexHelper.Delete(IndexIdx);
  //Do not need to adjust anything in fields.
  TidLocal.UpdateLayout(pinEvolve);
end;

procedure TMemDBTable.API_RenameIndex(T: TObject; OldName: string; NewName: string);
var
  Index: TMemIndexDef;
  IndexIdx, tmpIdx: integer;
  Tr: TMemDbTransaction;
  Sel: TBufSelector;
  selected: TABSelType;
  TidLocal: TTidLocal;
  FieldHelper, IndexHelper: TMemDblBufListHelper;
begin
  Tr := T as TMemDBTransaction;
  Sel := MakeLatestBufSelector(Tr.Tid);
  Index := META_IndexByName(Sel, OldName, IndexIdx);
  TidLocal := GetMakeListHelpers(Tr.Tid, FieldHelper, IndexHelper);
  if (not Assigned(Index)) or (IndexHelper.ChildDeleted[IndexIdx]) then
    raise EMemDBAPIException.Create(S_API_INDEX_NAME_NOT_FOUND);
  if Assigned(META_IndexByName(Sel, NewName, tmpIdx)) then
    raise EMemDBAPIException.Create(S_API_INDEX_NAME_CONFLICT);
  if AssignedNotSentinel(META_GetNext(Tr.Tid)) then
  begin
    //Check there has not been a previous conflicting rename.
    //If previously been deleted, ChildDeleted check above should fail.
    Assert(AssignedNotSentinel((META_GetNext(Tr.Tid) as TMemTableMetadataItem).IndexDefs.Items[IndexIdx]));
    Index := (META_GetNext(Tr.Tid) as TMemTableMetadataItem).IndexDefs.Items[IndexIdx]
      as TMemIndexDef;
    if Index.IndexName <> OldName then
      raise EMemDBAPIException.Create(S_API_RENAME_OVERWRITE);
  end;
  //Now rename the index.
  Index := IndexHelper.Modify(IndexIdx) as TMemIndexDef;
  Index.IndexName := NewName;
  Assert(AssignedNotSentinel(META_GetPinLatest(Tr.Tid, selected, pinEvolve)));
  ParentDB.HandleAPIIndexRename(Sel,
    (META_GetPinLatest(Tr.Tid, selected, pinEvolve) as TMemTableMetadataItem).EntityName,
    OldName, NewName);
  TidLocal.UpdateLayout(pinEvolve);
end;

function TMemDBTable.API_IndexInfo(T: TObject; Name: string;
                   var IndexedFields: TMDBFieldNames;
                   var IndexAttrs: TMDBIndexAttrs): boolean;
var
  Index: TMemIndexDef;
  IndexIdx: integer;
  IdxFIdx: integer;
  Tr: TMemDBTransaction;
  Sel: TBufSelector;
begin
  Tr := T as TMemDbTransaction;
  Sel := MakeLatestBufSelector(Tr.Tid);
  Index := META_IndexByName(Sel, Name, IndexIdx);
  result := Assigned(Index);
  if result then
  begin
    SetLength(IndexedFields, Index.FieldNameCount);
    for IdxFIdx := 0 to Pred(Length(IndexedFields)) do
      IndexedFields[IdxFIdx] := Index.FieldNames[IdxFIdx];
    IndexAttrs := Index.IndexAttrs;
  end;
end;

function TMemDBTable.API_GetIndexNames(T: TObject): TStringList;
var
  selected: TABSelType;
  Tr: TMemDbTransaction;
  MetadataCopy: TMemTableMetadataItem;
  IndexDef: TMemIndexDef;
  idx: integer;
begin
  result := TStringList.Create;
  Tr := T as TMemDbTransaction;
  if AssignedNotSentinel(META_GetPinLatest(Tr.Tid, selected, pinEvolve)) then
  begin
    MetadataCopy := META_GetPinLatest(Tr.Tid, selected, pinEvolve) as TMemTableMetadataItem;
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
      end;
    end;
  end;
end;

{$IFDEF MEMDB2_TEMP_REMOVE}

//TODO - Transplant to GetUserTidLocalIndexRoot.
function TMemDBTable.IndexNameToIndexAndTag(Iso: TMDBIsolationLevel;  var Idx:TMemIndexDef; Name: string): PITagStruct;
var
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

{$ENDIF}

procedure TMemDBTable.META_CurIndexDefToFieldDefs(const Tid: TTransactionId; IndexDef: TMemIndexDef; var FieldDefs: TMemFieldDefs; var FieldAbsIdxs: TFieldOffsets);
var
  Sel: TBufSelector;
begin
  Assert(Assigned(IndexDef));
  Assert(IndexDef.FieldNameCount > 0);
  Sel := MakeCurrentBufSelector(Tid);
  FieldDefs := META_FieldsByNames(Sel, IndexDef.FieldArray, FieldAbsIdxs);
end;

function TMemDBTable.META_CurFieldDefList(const Tid: TTransactionId): TMemStreamableList;
var
  CB: TMemDBStreamable;
  CurMetadata: TMemTableMetadataItem;
begin
  CB := META_PinCurrent(Tid, pinEvolve);
  if NotAssignedOrSentinel(CB) then
    raise EMemDBAPIException.Create(S_API_TABLE_METADATA_NOT_COMMITED);
  CurMetadata := CB as TMemTableMetadataItem;
  result := CurMetadata.FieldDefs;
end;

//TODO - Ensure debug / logging builds do in fact build.
function TMemDBTable.API_DataLocate(T:TObject;
                        Cursor: TMemDBCursor;
                        Pos: TMemAPIPosition;
                        IdxName: string): TMemDBCursor;
var
  IRoot: TMemDBIndex;
  Idx: TMemIndexDef;
  Tr: TMemDBTransaction;
  TidLocal: TTidLocal;
begin
  Tr := T as TMemDBTransaction;
  if Length(IdxName) = 0 then
  begin
    IRoot := nil;
    TidLocal := GetMakeTidLocal(Tr.Tid, pinEvolve);
  end
  else
  begin
    IRoot := GetUserTidLocalIndexRoot(Tr.Tid, TidLocal, Idx, IdxName);
    if not Assigned(IRoot) then
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
  result := TidLocal.UserMoveToRowByIndexRoot(IRoot, Cursor, Pos);
end;


function TMemDBTable.API_DataFindByIndex(T:TObject;
                            IdxName: string;
                            const DataRecs: TMemDbFieldDataRecs): TMemDbCursor; //Returns cursor
var
  IRoot: TMemDBIndex;
  Idx: TMemIndexDef;
  Tr: TMemDBTransaction;
  TidLocal: TTidLocal;
  FieldDefs: TMemFieldDefs;
  FieldAbsIdxs: TFieldOffsets;

{$IFDEF DEBUG_DATABASE_NAVIGATE}
  i: integer;
{$ENDIF}
begin
  Tr := T as TMemDBTransaction;
  if Length(IdxName) = 0 then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQUIRES_INDEX);

  IRoot := GetUserTidLocalIndexRoot(Tr.Tid, TidLocal, Idx, IdxName);
  if not Assigned(IRoot) then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQUIRES_INDEX);

  Assert(Assigned(Idx));
  Assert(Assigned(TidLocal));
  META_CurIndexDefToFieldDefs(Tr.Tid, Idx, FieldDefs, FieldAbsIdxs);
  if Length(DataRecs)<> Length(FieldDefs) then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQURES_CORRECT_FIELD_COUNT);
  //More detailed check of field format later on.
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GLogLog(SV_INFO, 'FIND by index, IndexName: ' + Idx.IndexName);
  for i := 0 to Pred(Length(FieldDefs)) do
  begin
    GLogLog(SV_INFO, 'FieldName[' + InttoStr(i) + ']: ' + FieldDefs[i].FieldName + ' FieldNDIndex: ' +
    IntToStr(FieldDefs[i].FieldIndex) + ' FieldAbsIdx: ' + IntToStr(FieldAbsIdxs[i]));
  end;
{$ENDIF}
  result := TidLocal.UserFindRowByIndexRoot(Idx, FieldDefs, FieldAbsIdxs, IRoot, DataRecs);
end;

function TMemDBTable.API_DataFindEdgeByIndex(T: TObject;
                                             IdxName: string;
                                             const DataRecs: TMemDbFieldDataRecs;
                                             Pos: TMemAPIPosition): TMemDBCursor;
var
  IRoot: TMemDBIndex;
  Tr: TMemDBTransaction;
  Idx: TMemIndexDef;
  TidLocal: TTidLocal;
  IndexFieldDefs: TMemFieldDefs;
  AllFieldDefs: TMemStreamableList;
  FieldAbsIdxs: TFieldOffsets;
  Cur,Next: TMemDBCursor;
  NextFields, ResultFields: TMemStreamableList;
  bufSel: TABSelType;
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  i: integer;
{$ENDIF}
begin
  if Length(IdxName) = 0 then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQUIRES_INDEX);
  Tr := T as TMemDBTransaction;
  IRoot := GetUserTidLocalIndexRoot(Tr.Tid, TidLocal, Idx, IdxName);
  if not Assigned(Idx) then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQUIRES_INDEX);
  if not (Pos in [ptFirst, ptLast]) then
    raise EMemDBAPIException.Create(S_API_FIND_EDGE_FIRST_OR_LAST);
  META_CurIndexDefToFieldDefs(Tr.Tid, Idx, IndexFieldDefs, FieldAbsIdxs);
  AllFieldDefs := META_CurFieldDefList(Tr.Tid);

  if Length(DataRecs) <> Length(IndexFieldDefs) then
    raise EMemDBAPIException.Create(S_API_SEARCH_REQURES_CORRECT_FIELD_COUNT);
  //More detailed check of field format later on.
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GLogLog(SV_INFO, 'EDGE by index, IndexName: ' + Idx.IndexName);
  for i := 0 to Pred(Length(FieldDefs)) do
  begin
    GLogLog(SV_INFO,  ' FieldName['+ IntToStr(i) +']: ' + FieldDefs[i].FieldName + ' FieldNDIndex: ' +
      IntToStr(FieldDefs[i].FieldIndex) + ' FieldAbsIdx: ' + IntToStr(FieldAbsIdxs[i]));
  end;
{$ENDIF}
  Cur := TidLocal.UserFindRowByIndexRoot(Idx, IndexFieldDefs, FieldAbsIdxs, IRoot, DataRecs);
  Next := nil;
  result := nil;
  try
{$IFDEF DEBUG_DATABASE_NAVIGATE}
    if Assigned(Cur) then
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
    if Assigned(Cur) then
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
        Next := TidLocal.UserMoveToRowByIndexRoot(IRoot, Cur, Pos);
        if Assigned(Next) then
        begin
{$IFDEF DEBUG_DATABASE_NAVIGATE}
      GLogLog(SV_INFO, 'EDGE by Index, found ' + MemAPIPositionStrings[Pos] + ' row');
{$ENDIF}
          //Isolation be damned, it's gonna break.
          if not Cur.Row.CheckFormatAgainstMetaDefs(Tr.Tid, abLatest, AllFieldDefs, pinEvolve) then
            raise EMemDBConcurrencyException.Create(S_TABLE_FORMAT_CONCURRENTLY_CHANGED_1);
            //Might get raised if buggy format, but normally only concurrency.
          ResultFields := Cur.Row.GetPinLatest(Tr.Tid, bufSel, pinEvolve) as TMemStreamableList;

          //Isolation be damned, it's gonna break.
          if not Next.Row.CheckFormatAgainstMetaDefs(Tr.Tid, abLatest, AllFieldDefs, pinEvolve) then
            raise EMemDBConcurrencyException.Create(S_TABLE_FORMAT_CONCURRENTLY_CHANGED_2);
            //Might get raised if buggy format, but normally only concurrency.
          NextFields := Next.Row.GetPinLatest(Tr.Tid, bufSel, pinEvolve) as TMemStreamableList;

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
          begin
            Next.Free;
            Next := nil
          end;
        end;
        if Assigned(Next) then
        begin
          Cur.Free;
          Cur := Next;
          Next := nil;
        end;
      until not Assigned(Next);
      result := Cur;
      Cur := nil;
    end
  finally
    Cur.Free;
    Next.Free;
  end;
end;

procedure TMemDBTable.API_DataReadRow(T:TObject;
                         Cursor: TMemDBCursor;
                         FieldList: TMemStreamableList);
var
  RowFields: TMemStreamableList;
  AllFieldDefs: TMemStreamableList;
  Tr:  TMemDbTransaction;
  bufSel: TABSelType;
begin
  Tr := T as TMemDBTransaction;
  if not (Assigned(Cursor) and Assigned(FieldList)) then
    raise EMemDBInternalException.Create(S_API_INTERNAL_READING_ROW);
  FieldList.ReleaseAndClear;
  AllFieldDefs := META_CurFieldDefList(TR.Tid);

  if not Cursor.Row.CheckFormatAgainstMetaDefs(Tr.Tid, abLatest, AllFieldDefs, pinEvolve) then
    raise EMemDBConcurrencyException.Create(S_TABLE_FORMAT_CONCURRENTLY_CHANGED_3);
    //Might get raised if buggy format, but normally only concurrency.

  RowFields := Cursor.Row.GetPinLatest(Tr.Tid, bufSel, pinEvolve) as TMemStreamableList;
  FieldList.DeepAssign(RowFields);
end;


procedure TMemDBTable.API_DataInitRowForAppend(T: TObject;
                                  Cursor: TMemDBCursor;
                                  FieldList: TMemStreamableList);
var
  AllFieldDefs: TMemStreamableList;
  MField: TMemFieldDef;
  MData: TMemFieldData;
  Idx: Integer;
  Tr: TMemDBTransaction;
  TidLocal: TTidLocal;
begin
  Tr := T as TMemDbTransaction;
  TidLocal := GetMakeTidLocal(Tr.Tid, pinEvolve);
  if TidLocal.LayoutChangeRequired then
    raise EMemDBAPIException.Create(S_FIELD_LAYOUT_CHANGED);

  //Create field list here based on existing metadata.
  if Assigned(Cursor) or (not Assigned(FieldList)) then
    raise EMemDBInternalException.Create(S_API_INTERNAL_READING_ROW);

  FieldList.ReleaseAndClear;

  AllFieldDefs := META_CurFieldDefList(Tr.Tid);
  if AllFieldDefs.Count = 0 then
    raise EMemDBAPIException.Create(S_API_NO_FIELDS_IN_TABLE_AT_APPEND_TIME);

  for Idx := 0 to Pred(AllFieldDefs.Count) do
  begin
    MField := AllFieldDefs.Items[idx] as TMemFieldDef;
    MData := TMemFieldData.Create;
    MData.FDataRec.FieldType := MField.FieldType;
    Assert(MField.FieldIndex = Idx); //No ND.
    FieldList.AddNoRef(MData);
  end;
end;


function TMemDBTable.API_DataRowModOrAppend(T: TObject;
                                Cursor: TMemDBCursor;
                                FieldList: TMemStreamableList): TMemDBCursor;
var
  AllFieldDefs: TMemStreamableList;
  MField: TMemFieldDef;
  MData: TMemFieldData;
  Idx: Integer;
  Tr: TMemDBTransaction;
  TidLocal: TTidLocal;
begin
  Tr := T as TMemDBTransaction;
  TidLocal := GetMakeTidLocal(Tr.Tid, pinEvolve);
  if TidLocal.LayoutChangeRequired then
    raise EMemDBAPIException.Create(S_FIELD_LAYOUT_CHANGED);

  //We will validate new data format here, against existing metadata.
  if not Assigned(FieldList) then
    raise EMemDBInternalException.Create(S_API_INTERNAL_CHANGING_ROW);
  AllFieldDefs := META_CurFieldDefList(Tr.Tid);
  if FieldList.Count <> AllFieldDefs.Count then
    raise EMemDBInternalException.Create(S_API_ROW_BAD_STRUCTURE);
  for Idx := 0 to Pred(AllFieldDefs.Count) do
  begin
    MField := AllFieldDefs.Items[idx] as TMemFieldDef;
    MData := FieldList.Items[idx] as TMemFieldData;
    if MField.FieldType <> MData.FDataRec.FieldType then
      raise EMemDBInternalException.Create(S_API_ROW_BAD_STRUCTURE);
  end;
  //If we write row data that disagrees with a format change then
  //either a) CheckChangedRowStructure will barf, or
  // b) Write-after-write conflict detection will catch the error.
  // We'll check it against the Cur metadata in the TidLocal row write.
  result := TidLocal.UserWriteRowData(Cursor, FieldList);
end;

function TMemDBTable.API_DataRowDelete(T: TObject;
                                        Cursor: TMemDBCursor;
                                        AutoInc: boolean): TMemDBCursor;
var
  Tr: TMemDBTransaction;
  TidLocal: TTidLocal;
begin
  Tr := T as TMemDBTransaction;
  TidLocal := GetMakeTidLocal(Tr.Tid, pinEvolve);
  if TidLocal.LayoutChangeRequired then
    raise EMemDBAPIException.Create(S_FIELD_LAYOUT_CHANGED);
  if not Assigned(Cursor) then
    raise EMemDBAPIException.Create(S_API_NO_ROW_FOR_DELETE);
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GLogLog(SV_INFO, 'API_DataRowDelete: Actual deletion.');
{$ENDIF}
  result := TidLocal.UserDeleteRow(Cursor, AutoInc);
end;

function TMemDBTable.API_DataGetFieldAbsIdx(T: TObject;
                                            FieldName: string): integer;
var
  FDef: TMemFieldDef;
  Tr: TMemDBTransaction;
  BufSel: TBufSelector;
begin
  //Always look in current metadata copy: Field layout doesn't change until
  //the next commit.
  Tr := T as TMemDBTransaction;
  BufSel := MakeCurrentBufSelector(Tr.Tid);
  FDef := META_FieldByName(BufSel, FieldName, result);
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

{$IFDEF MEMDB2_TEMP_REMOVE}

function TMemDBForeignKey.LookupTableAndIndex(TableName: string; IndexName: string): TMemIndexDef;
var
  Entity: TMemDBEntity;
  Table: TMemDBTablePersistent;
  tmpIdx: Integer;
begin
  with ParentDB do
  begin
    //Latest entities, this checks specified params are valid.
    Entity := EntitiesByName(abLatest, TableName, tmpIdx);
    if not (Assigned(Entity) and (Entity is TMemDBTablePersistent)) then
      raise EMemDBAPIException.Create(S_API_ENTITY_NAME_NOT_FOUND);
    Table := Entity as TMemDBTablePersistent;
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

{$ENDIF}

end.
