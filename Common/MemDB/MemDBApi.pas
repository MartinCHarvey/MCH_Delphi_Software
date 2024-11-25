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

{
  TODO - No queries yet. They will also be a complete mind job.
}

{$IFDEF DEBUG_DATABASE_DELETE}
{$DEFINE DEBUG_DATABASE_NAVIGATE}
{$ENDIF}

uses
  MemDBBuffered, MemDbMisc, MemDbStreamable, MemDBIndexing, Classes;

type
  TMemDBDatabase = class;
  TMemDBTable = class;
  TMemDBForeignKey = class;

  //API objects.
  //These hold per-transaction state for users as well
  //as permissions checking.

  //Database level operations.
  TMemAPIDB = class(TMemDBAPI)
  protected
    function GetInterfacedObject: TMemDBDatabase;
    property DB: TMemDBDatabase read GetInterfacedObject;
  end;

  TMemAPIDatabaseInternal = class(TMemAPIDB)
  public
    procedure JournalReplayCycleV2(JournalEntry: TStream; Initial: boolean);
    function UserCommitCycleV2(StorageMode: TTempStorageMode):TStream;
    procedure UserRollbackCycle;
  end;

  TMemAPIDatabase = class(TMemAPIDB)
  public
    function CreateTable(Name: string): TMemDBHandle;
    function OpenTableOrKey(Name: string): TMemDBHandle;
    function CreateForeignKey(Name: string): TMemDBHandle;
    procedure RenameTableOrKey(OldName, NewName: string);
    procedure DeleteTableOrKey(Name: string);
    function GetEntityNames: TStringList;
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
    //Cursor actually MemDBRow, but not to be modified directly here
    //Use table object API calls.
    //Poss create row object API calls.
    //Field list always exists.
    FCursor: TObject;
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
    procedure API_DeleteTableOrKey(Iso: TMDBIsolationLevel; Name: string);
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
                            Cursor: TObject;
                            Pos: TMemAPIPosition;
                            IdxName: string): TObject; //Returns cursor.

    function API_DataFindByIndex(Iso: TMDBIsolationLevel;
                                IdxName: string;
                                const DataRecs: TMemDbFieldDataRecs): TObject; //Returns cursor

    function API_DataFindEdgeByIndex(Iso: TMDBIsolationLevel;
                                     IdxName: string;
                                     const DataRecs: TMemDbFieldDataRecs;
                                     Pos: TMemAPIPosition): TObject; //Returns cursor.

    procedure API_DataReadRow(Iso: TMDBIsolationLevel;
                             Cursor: TObject;
                             FieldList: TMemStreamableList);

    procedure API_DataInitRowForAppend(Iso: TMDBIsolationLevel;
                                      Cursor: TObject;
                                      FieldList: TMemStreamableList);

    procedure API_DataRowModOrAppend(Iso: TMDBIsolationLevel;
                                    Cursor: TObject;
                                    FieldList: TMemStreamableList);

    procedure API_DataRowDelete(Iso: TMDBIsolationLevel;
                                Cursor: TObject);

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

implementation

uses
{$IFDEF DEBUG_DATABASE_NAVIGATE}
  GlobalLog,
{$ENDIF}
  MemDB, SysUtils, IoUtils, BufferedFileStream;

const
  S_API_POST_OR_DISCARD_BEFORE_NAVIGATING =
    'Data at cursor locally changed. Post changes or discard before navigating around table.';
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

{ TMemAPIDB }

function TMemAPIDB.GetInterfacedObject: TMemDBDatabase;
begin
  result := FInterfacedObject.Parent as TMemDBDatabase;
end;

{ TMemAPIDatabaseInternal}

//Stream already provided.
procedure TMemAPIDatabaseInternal.JournalReplayCycleV2(JournalEntry: TStream; Initial: boolean);
var
   Reason: TMemDBTransReason;
begin
  if Initial then
    Reason := mtrReplayFromScratch
  else
    Reason := mtrReplayFromJournal;
  try
    if Initial then
      DB.FromScratchV2(JournalEntry)
    else
      DB.FromJournalV2(JournalEntry);
    DB.PreCommit(Reason);
    DB.Commit(Reason);
  finally
    DB.PostCommitCleanup(Reason);
  end;
end;

function TMemAPIDatabaseInternal.UserCommitCycleV2(StorageMode: TTempStorageMode):TStream;

  function MakeStream: TStream;
  var
    TmpName: string;
  begin
    case StorageMode of
      tsmDisk:
      begin
        TmpName := TPath.GetTempFileName();
        result := TMemDBWriteCachedFileStream.Create(TmpName);
      end;
    else
      result := TMemoryStream.Create;
    end;
  end;

begin
  try
    result := MakeStream;
    try
      DB.PreCommit(mtrUserOp);
      DB.ToJournalV2(result);
      DB.Commit(mtrUserOp);
    except
      on E: Exception do
      begin
        result.Free; //Handle error in Commit function (index revalidate?)
        raise;
      end;
    end;
  finally
    DB.PostCommitCleanup(mtrUserOp);
  end;
end;

procedure TMemAPIDatabaseInternal.UserRollbackCycle;
begin
  try
    DB.Rollback(mtrUserOp);
  finally
    DB.PostRollbackCleanup(mtrUserOp);
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

procedure TMemAPIDatabase.DeleteTableOrKey(Name: string);
begin
  CheckReadWriteTransaction;
  DB.API_DeleteTableOrKey(Isolation, Name);
end;

function TMemAPIDatabase.GetEntityNames: TStringList;
begin
  result := DB.API_GetEntityNames(Isolation);
end;


function TMemAPITable.GetInterfacedObject: TMemDBTable;
begin
  result := FInterfacedObject.Parent as TMemDBTable;
end;

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
begin
  if not (Assigned(FCursor) or FAdding) then
    raise EMemDBAPIException.Create(S_API_NO_ROW_SELECTED);
  FieldAbsIdx := Table.API_DataGetFieldAbsIdx(FIsolation, FieldName);
  if (FieldAbsIdx < 0) or (FieldAbsIdx >= FFieldList.Count) then
    raise EMemDBInternalException.Create(S_API_BAD_FIELD_INDEX);
  //Blobs: Treat the memory as read-only, but you can re-write the pointer & size,
  //DB engine will free existing memory, and grab your new buffer.
  Data := (FFieldList.Items[FieldAbsIdx] as TMemFieldData).FDataRec
end;

procedure TMemAPITableData.WriteField(FieldName: string;
                     const Data: TMemDbFieldDataRec);
var
  FieldAbsIdx: integer;
  Field: TMemFieldData;
begin
  CheckReadWriteTransaction;
  if not (Assigned(FCursor) or FAdding) then
    raise EMemDBAPIException.Create(S_API_NO_ROW_SELECTED);
  FieldAbsIdx := Table.API_DataGetFieldAbsIdx(FIsolation, FieldName);
  if (FieldAbsIdx < 0) or (FieldAbsIdx >= FFieldList.Count) then
    raise EMemDBInternalException.Create(S_API_BAD_FIELD_INDEX);
  Field := (FFieldList.Items[FieldAbsIdx] as TMemFieldData);
  if Data.FieldType <> FIeld.FDataRec.FieldType then
    raise EMemDBAPIException.Create(S_API_CANNOT_CHANGE_FIELD_TYPES_DIRECTLY);
  //Blobs: Treat the memory as read-only, but you can re-write the pointer & size,
  //DB engine will free existing memory, and grab your new buffer.
  if Data.FieldType = ftBlob then
  begin
    if (Data.size > 0) <> Assigned(Data.Data) then
      raise EMemDBAPIException.Create(S_API_BLOB_SIZE_INCONSISTENT_WITH_PTR);
    if Field.FDataRec.Data <> Data.Data then
    begin
      //OK, you've changed the buffer, I'll grab the memory.
      if Assigned(Field.FDataRec.Data) then
        FreeMem(Field.FDataRec.Data);
    end;
    //else you've not changed the buffer, it's still my own one.
  end;
  Field.FDataRec := Data;
  FFieldListDirty := true;
end;

procedure TMemAPITableData.Post;
begin
  CheckReadWriteTransaction;
  //Do the post.
  Table.API_DataRowModOrAppend(FIsolation, FCursor, FFieldList);
  Discard; //Discard temp data.
end;

procedure TMemAPITableData.Delete;
var
  NextCursor: TObject;
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


procedure TMemAPITableData.Discard;
begin
  FFieldListDirty := false;
  DiscardInternal;
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

const
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

procedure TMemDBDatabase.API_DeleteTableOrKey(Iso: TMDBIsolationLevel; Name: string);
var
  TmpIdx: integer;
  Entity: TMemDBEntity;
  AB: TABSelection;
begin
  AB := IsoToAB(Iso);
  Entity := EntitiesByName(AB, Name, tmpIdx);
  if not Assigned(Entity) then
    raise EMemDbAPIException.Create(S_API_ENTITY_NAME_NOT_FOUND);
  CheckAPITableDelete(Iso, Name);
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
        APITableData:
        begin
          result := TMemAPITableData.Create;
        end;
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
                        Cursor: TObject;
                        Pos: TMemAPIPosition;
                        IdxName: string): TObject; //Returns cursor.
var
  PTagStruct: PITagStruct;
  Idx: TMemIndexDef;
begin
  if Length(IdxName) = 0 then
    PTagStruct := MDBInternalIndexRowId.TagStructs[sicCurrent]
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
                                     Cursor as TMemDBRow,
                                     Pos);
end;

function TMemDBTable.API_DataFindByIndex(Iso: TMDBIsolationLevel;
                            IdxName: string;
                            const DataRecs: TMemDbFieldDataRecs): TObject; //Returns cursor
var
  ITagStruct: PITagStruct;
  Idx: TMemIndexDef;
  FieldDefs: TMemFieldDefs;
  FieldAbsIdxs: TFieldOffsets;
  i: integer;
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
                                             Pos: TMemAPIPosition): TObject;
var
  ITagStruct: PITagStruct;
  Idx: TMemIndexDef;
  FieldDefs: TMemFieldDefs;
  FieldAbsIdxs: TFieldOffsets;
  Next: TObject;
  NextFieldDataRecs,
  ResultFieldDataRecs: TMemDbFieldDataRecs;
  NextFields, ResultFields: TMemStreamableList;
  AB: TABSelection;
  i: integer;
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
  SetLength(NextFieldDataRecs, Length(FieldDefs));
  SetLength(ResultFieldDataRecs, Length(FieldDefs));
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
                                         result as TMemDBRow,
                                         Pos);
      if Assigned(Next) then
      begin
{$IFDEF DEBUG_DATABASE_NAVIGATE}
    GLogLog(SV_INFO, 'EDGE by Index, found ' + MemAPIPositionStrings[Pos] + ' row');
{$ENDIF}

        //Pretty sure we shouldn't have any sentinels at this point.
        NextFields := (Next as TMemDBRow).ABData[AB] as TMemStreamableList;
        NextFieldDataRecs := BuildMultiDataRecs(NextFields, FieldAbsIdxs);
        ResultFields := (result as TMemDBRow).ABData[AB] as TMemStreamableList;
        ResultFieldDataRecs := BuildMultiDataRecs(ResultFields, FieldAbsIdxs);

{$IFDEF DEBUG_DATABASE_NAVIGATE}
        //Compare all fields.
        if MultiDataRecsSame(NextFieldDataRecs, ResultFieldDataRecs) then
        begin
          GLogLog(SV_INFO, 'EDGE by Index ' + MemAPIPositionStrings[Pos] + ' row has same fields.');
          for i := 0 to Pred(Length(FieldDefs)) do
          begin
            if NextFieldDataRecs[i].FieldType = ftUnicodeString then
              GLogLog(SV_INFO, 'Field['+ InttoStr(i) +'] is: ' + NextFieldDataRecs[i].sVal);
          end;
        end
        else
        begin
          GLogLog(SV_INFO, 'EDGE by Index ' + MemAPIPositionStrings[Pos] + ' row differs.');
          for i := 0 to Pred(Length(FieldDefs)) do
          begin
            if NextFieldDataRecs[i].FieldType = ftUnicodeString then
              GLogLog(SV_INFO, 'Field['+ InttoStr(i) +'] is: ' + NextFieldDataRecs[i].sVal);
          end;
        end;
{$ENDIF}
        if not MultiDataRecsSame(NextFieldDataRecs, ResultFieldDataRecs) then
          Next := nil;
      end;
      if Assigned(Next) then
        result := Next;
    until not Assigned(Next);
  end;
end;

procedure TMemDBTable.API_DataReadRow(Iso: TMDBIsolationLevel;
                         Cursor: TObject;
                         FieldList: TMemStreamableList);
var
  Row: TMemDBRow;
  RowFields: TMemStreamableList;
begin
  if not (Assigned(Cursor) and Assigned(FieldList)) then
    raise EMemDBInternalException.Create(S_API_INTERNAL_READING_ROW);
  FieldList.FreeAndClear;
  Row := Cursor as TMemDBRow;
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
                                  Cursor: TObject;
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
                                Cursor: TObject;
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
  self.Data.WriteRowData(Cursor as TMemDBRow, Iso, FieldList);
end;

procedure TMemDBTable.API_DataRowDelete(Iso: TMDBIsolationLevel;
                                Cursor: TObject);
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
  Data.DeleteRow(Cursor as TMemDBRow, Iso);
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
