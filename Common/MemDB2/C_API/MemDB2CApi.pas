unit MemDB2CApi;
{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

{
  C callable API over MemDB2.

  This unit only declares and implements the functions. It does not export
  them: a DLL project does that by including MemDB2CApi_Exports.inc.
  The matching C declarations are in memdb2.h, which also documents the
  rules (handles, errors, strings, ownership, threading) in detail.

  Summary of the design:

  * Calling convention: cdecl for every function.
  * Errors: every function (except the version/error getters) returns an
    Integer status: MDB_OK (0) or a negative MDB_ERR_xxx code. No exception
    ever crosses the boundary. The message for the most recent failure on
    the calling thread is available from mdb_last_error_message.
  * Handles: every Delphi object is hidden behind a 64 bit handle. Handles
    carry a type tag and a generation number, so stale, closed, or wrong
    type handles are rejected with an error rather than crashing.
    Closing a handle closes its children first (database -> sessions ->
    transactions -> table / foreign key objects). Closing a transaction
    without committing rolls it back. A failed commit leaves the
    transaction open, as MemDB2 does: roll it back or retry.
  * Strings: UTF-8, NUL terminated, in both directions. Output strings and
    blobs use caller supplied buffers: pass a NULL buffer to query the size.
  * Blobs: copied in both directions; no memory ownership crosses the API.
    Blobs can be read whole or as a leading part, and used in search keys.
}

interface

uses
  SysUtils;

const
  //memdb2.h calls this MDB_API_VERSION. Pascal is case-insensitive, so that
  //name would clash with the function mdb_api_version.
  //1: first release. 2: mdb_data_read_blob_prefix, mdb_key_add_blob.
  //3: mdb_db_open_ex, mdb_db_checkpoint_ex.
  MDB_CAPI_VERSION = 3;

  //Status codes.
  MDB_OK = 0;
  MDB_ERR_INVALID_ARG = -1;
  MDB_ERR_BAD_HANDLE = -2;
  MDB_ERR_WRONG_HANDLE_TYPE = -3;
  MDB_ERR_NOT_FOUND = -4;
  MDB_ERR_BUFFER_TOO_SMALL = -5;
  MDB_ERR_TYPE_MISMATCH = -6;
  MDB_ERR_DB_STATE = -7;
  MDB_ERR_HANDLE_BUSY = -8;
  MDB_ERR_API = -10;
  MDB_ERR_CONCURRENCY = -11;
  MDB_ERR_CONSISTENCY = -12;
  MDB_ERR_INTERNAL = -13;
  MDB_ERR_DB = -14;
  MDB_ERR_OUT_OF_MEMORY = -15;
  MDB_ERR_UNEXPECTED = -99;

  //Field types (mdb_field_type).
  MDB_FT_INT32 = 0;
  MDB_FT_UINT32 = 1;
  MDB_FT_INT64 = 2;
  MDB_FT_UINT64 = 3;
  MDB_FT_STRING = 4;
  MDB_FT_DOUBLE = 5;
  MDB_FT_BLOB = 6;
  MDB_FT_GUID = 7;

  //Transaction access modes.
  MDB_ACCESS_READ_SHARED = 0;
  MDB_ACCESS_WRITE_EXCLUSIVE = 1;
  MDB_ACCESS_READ_WRITE_SHARED = 2;

  //Transaction sync modes.
  MDB_SYNC_LAZY_WRITE = 0;
  MDB_SYNC_FLUSH_BUFFERS = 1;

  //Transaction isolation levels.
  MDB_ISO_READ_COMMITTED = 0;
  MDB_ISO_REPEATABLE_READ = 1;
  MDB_ISO_SNAPSHOT = 2;
  MDB_ISO_SERIALISABLE = 3;

  //Navigation positions.
  MDB_POS_FIRST = 0;
  MDB_POS_LAST = 1;
  MDB_POS_NEXT = 2;
  MDB_POS_PREVIOUS = 3;

  //Index attribute flags.
  MDB_INDEX_UNIQUE = 1;
  MDB_INDEX_NOT_EMPTY = 2;

  //Session temporary storage.
  MDB_TEMP_MEMORY = 0;
  MDB_TEMP_DISK = 1;

  //Database phases (mdb_db_state).
  MDB_PHASE_NULL = 0;
  MDB_PHASE_INIT = 1;
  MDB_PHASE_INIT_CHECKPOINT = 2;
  MDB_PHASE_RUNNING = 3;
  MDB_PHASE_RUNNING_ERROR = 4;
  MDB_PHASE_CLOSING_WAIT_CLIENTS = 5;
  MDB_PHASE_CLOSING_WAIT_CLIENTS_ERROR = 6;
  MDB_PHASE_CLOSING_WAIT_PERSIST = 7;
  MDB_PHASE_CLOSED = 8;
  MDB_PHASE_CLOSED_ERROR = 9;

  //mdb_db_open_ex flags.
  MDB_OPEN_ASYNC = 1;
  MDB_OPEN_NO_AUTO_CHECKPOINT = 2;

  //mdb_db_checkpoint_ex flags.
  MDB_CHECKPOINT_ASYNC = 1;

type
  TMDBHandle = UInt64;
  PMDBHandle = ^TMDBHandle;
  TMDBSize = NativeUInt;
  PMDBSize = ^TMDBSize;
  PMDBInt32 = ^Integer;
  PMDBUInt32 = ^Cardinal;
  PMDBInt64 = ^Int64;
  PMDBUInt64 = ^UInt64;
  PMDBDouble = ^Double;
  PMDBGuid = ^TGUID;
  TMDBNameArray = array[0..$07FFFFFF] of PAnsiChar;
  PMDBNameArray = ^TMDBNameArray;

{ General }
function mdb_api_version: Integer; cdecl;
function mdb_last_error_code: Integer; cdecl;
function mdb_last_error_message(buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer; cdecl;
function mdb_close(handle: TMDBHandle): Integer; cdecl;
function mdb_shutdown: Integer; cdecl;

{ Database }
function mdb_db_open(path: PAnsiChar; out_db: PMDBHandle): Integer; cdecl;
function mdb_db_open_ex(path: PAnsiChar; flags: Integer; out_db: PMDBHandle): Integer; cdecl;
function mdb_db_close(db: TMDBHandle): Integer; cdecl;
function mdb_db_checkpoint(db: TMDBHandle): Integer; cdecl;
function mdb_db_checkpoint_ex(db: TMDBHandle; flags: Integer): Integer; cdecl;
function mdb_db_state(db: TMDBHandle; out_phase: PMDBInt32): Integer; cdecl;
function mdb_db_last_error(db: TMDBHandle; buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer; cdecl;

{ Session }
function mdb_session_open(db: TMDBHandle; out_session: PMDBHandle): Integer; cdecl;
function mdb_session_close(session: TMDBHandle): Integer; cdecl;
function mdb_session_get_temp_storage(session: TMDBHandle; out_mode: PMDBInt32): Integer; cdecl;
function mdb_session_set_temp_storage(session: TMDBHandle; mode: Integer): Integer; cdecl;

{ Transaction }
function mdb_txn_begin(session: TMDBHandle; access: Integer; sync: Integer;
  isolation: Integer; out_txn: PMDBHandle): Integer; cdecl;
function mdb_txn_commit(txn: TMDBHandle): Integer; cdecl;
function mdb_txn_rollback(txn: TMDBHandle): Integer; cdecl;
function mdb_txn_create_table(txn: TMDBHandle; name: PAnsiChar): Integer; cdecl;
function mdb_txn_create_foreign_key(txn: TMDBHandle; name: PAnsiChar): Integer; cdecl;
function mdb_txn_rename_entity(txn: TMDBHandle; old_name, new_name: PAnsiChar): Integer; cdecl;
function mdb_txn_delete_entity(txn: TMDBHandle; name: PAnsiChar): Integer; cdecl;
function mdb_txn_entity_names(txn: TMDBHandle; out_list: PMDBHandle): Integer; cdecl;
function mdb_txn_open_table_meta(txn: TMDBHandle; name: PAnsiChar; out_meta: PMDBHandle): Integer; cdecl;
function mdb_txn_open_table_data(txn: TMDBHandle; name: PAnsiChar; out_data: PMDBHandle): Integer; cdecl;
function mdb_txn_open_foreign_key(txn: TMDBHandle; name: PAnsiChar; out_fk: PMDBHandle): Integer; cdecl;

{ Table metadata }
function mdb_meta_create_field(meta: TMDBHandle; name: PAnsiChar; field_type: Integer): Integer; cdecl;
function mdb_meta_delete_field(meta: TMDBHandle; name: PAnsiChar): Integer; cdecl;
function mdb_meta_rename_field(meta: TMDBHandle; old_name, new_name: PAnsiChar): Integer; cdecl;
function mdb_meta_field_type(meta: TMDBHandle; name: PAnsiChar; out_type: PMDBInt32): Integer; cdecl;
function mdb_meta_field_names(meta: TMDBHandle; out_list: PMDBHandle): Integer; cdecl;
function mdb_meta_create_index(meta: TMDBHandle; name, field_name: PAnsiChar; attrs: Integer): Integer; cdecl;
function mdb_meta_create_multi_index(meta: TMDBHandle; name: PAnsiChar;
  field_names: PMDBNameArray; field_count: Integer; attrs: Integer): Integer; cdecl;
function mdb_meta_delete_index(meta: TMDBHandle; name: PAnsiChar): Integer; cdecl;
function mdb_meta_rename_index(meta: TMDBHandle; old_name, new_name: PAnsiChar): Integer; cdecl;
function mdb_meta_index_info(meta: TMDBHandle; name: PAnsiChar;
  out_field_list: PMDBHandle; out_attrs: PMDBInt32): Integer; cdecl;
function mdb_meta_index_names(meta: TMDBHandle; out_list: PMDBHandle): Integer; cdecl;

{ Table data: navigation and editing }
function mdb_data_locate(data: TMDBHandle; pos: Integer; index_name: PAnsiChar; out_found: PMDBInt32): Integer; cdecl;
function mdb_data_find(data: TMDBHandle; index_name: PAnsiChar; key: TMDBHandle; out_found: PMDBInt32): Integer; cdecl;
function mdb_data_find_edge(data: TMDBHandle; pos: Integer; index_name: PAnsiChar;
  key: TMDBHandle; out_found: PMDBInt32): Integer; cdecl;
function mdb_data_row_selected(data: TMDBHandle; out_selected: PMDBInt32): Integer; cdecl;
function mdb_data_append(data: TMDBHandle): Integer; cdecl;
function mdb_data_post(data: TMDBHandle): Integer; cdecl;
function mdb_data_discard(data: TMDBHandle): Integer; cdecl;
function mdb_data_delete(data: TMDBHandle; auto_inc: Integer): Integer; cdecl;

{ Table data: field values }
function mdb_data_read_int32(data: TMDBHandle; field: PAnsiChar; out_value: PMDBInt32): Integer; cdecl;
function mdb_data_read_uint32(data: TMDBHandle; field: PAnsiChar; out_value: PMDBUInt32): Integer; cdecl;
function mdb_data_read_int64(data: TMDBHandle; field: PAnsiChar; out_value: PMDBInt64): Integer; cdecl;
function mdb_data_read_uint64(data: TMDBHandle; field: PAnsiChar; out_value: PMDBUInt64): Integer; cdecl;
function mdb_data_read_double(data: TMDBHandle; field: PAnsiChar; out_value: PMDBDouble): Integer; cdecl;
function mdb_data_read_guid(data: TMDBHandle; field: PAnsiChar; out_value: PMDBGuid): Integer; cdecl;
function mdb_data_read_string(data: TMDBHandle; field: PAnsiChar; buf: PAnsiChar;
  buf_size: TMDBSize; out_len: PMDBSize): Integer; cdecl;
function mdb_data_read_blob(data: TMDBHandle; field: PAnsiChar; buf: Pointer;
  buf_size: TMDBSize; out_len: PMDBSize): Integer; cdecl;
function mdb_data_read_blob_prefix(data: TMDBHandle; field: PAnsiChar; buf: Pointer;
  buf_size: TMDBSize; out_copied: PMDBSize; out_total: PMDBUInt64): Integer; cdecl;
function mdb_data_write_int32(data: TMDBHandle; field: PAnsiChar; value: Integer): Integer; cdecl;
function mdb_data_write_uint32(data: TMDBHandle; field: PAnsiChar; value: Cardinal): Integer; cdecl;
function mdb_data_write_int64(data: TMDBHandle; field: PAnsiChar; value: Int64): Integer; cdecl;
function mdb_data_write_uint64(data: TMDBHandle; field: PAnsiChar; value: UInt64): Integer; cdecl;
function mdb_data_write_double(data: TMDBHandle; field: PAnsiChar; value: Double): Integer; cdecl;
function mdb_data_write_guid(data: TMDBHandle; field: PAnsiChar; value: PMDBGuid): Integer; cdecl;
function mdb_data_write_string(data: TMDBHandle; field: PAnsiChar; value: PAnsiChar): Integer; cdecl;
function mdb_data_write_blob(data: TMDBHandle; field: PAnsiChar; value: Pointer; len: TMDBSize): Integer; cdecl;

{ Search keys }
function mdb_key_create(out_key: PMDBHandle): Integer; cdecl;
function mdb_key_clear(key: TMDBHandle): Integer; cdecl;
function mdb_key_count(key: TMDBHandle; out_count: PMDBInt32): Integer; cdecl;
function mdb_key_add_int32(key: TMDBHandle; value: Integer): Integer; cdecl;
function mdb_key_add_uint32(key: TMDBHandle; value: Cardinal): Integer; cdecl;
function mdb_key_add_int64(key: TMDBHandle; value: Int64): Integer; cdecl;
function mdb_key_add_uint64(key: TMDBHandle; value: UInt64): Integer; cdecl;
function mdb_key_add_double(key: TMDBHandle; value: Double): Integer; cdecl;
function mdb_key_add_guid(key: TMDBHandle; value: PMDBGuid): Integer; cdecl;
function mdb_key_add_string(key: TMDBHandle; value: PAnsiChar): Integer; cdecl;
function mdb_key_add_blob(key: TMDBHandle; value: Pointer; len: TMDBSize): Integer; cdecl;

{ Foreign keys }
function mdb_fk_set_referencing_child(fk: TMDBHandle; table_name, index_name: PAnsiChar): Integer; cdecl;
function mdb_fk_set_referenced_parent(fk: TMDBHandle; table_name, index_name: PAnsiChar): Integer; cdecl;
function mdb_fk_get_child_table(fk: TMDBHandle; buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer; cdecl;
function mdb_fk_get_child_index(fk: TMDBHandle; buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer; cdecl;
function mdb_fk_get_parent_table(fk: TMDBHandle; buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer; cdecl;
function mdb_fk_get_parent_index(fk: TMDBHandle; buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer; cdecl;

{ String lists }
function mdb_strlist_count(list: TMDBHandle; out_count: PMDBInt32): Integer; cdecl;
function mdb_strlist_get(list: TMDBHandle; index: Integer; buf: PAnsiChar;
  buf_size: TMDBSize; out_len: PMDBSize): Integer; cdecl;

implementation

uses
  Classes, LockAbstractions, MemDB2, MemDB2Api, MemDB2Misc, MemDB2Buffered;

const
  ERR_MSG_MAX = 1024; //Bytes including NUL.

type
  { Errors raised inside this unit, carrying their status code. }
  EMDBCApiError = class(Exception)
  private
    FCode: Integer;
  public
    constructor Create(ACode: Integer; const Msg: string);
    property Code: Integer read FCode;
  end;

  THandleKind = (hkNone, hkDB, hkSession, hkTxn, hkTableMeta, hkTableData,
                 hkForeignKey, hkKey, hkStrList);

  { Per transaction state: the transaction plus its (lazily created) database
    level API object, which MemDB2 creates per call to GetAPI. }
  TTxnRec = class
  public
    Txn: TMemDBTransaction;
    DBAPI: TMemAPIDatabase;
  end;

  { Search key: field values in index order.
    Blob values are copies owned by the key, freed by Clear and Destroy. }
  TKeyRec = class
  public
    Recs: TMemDbFieldDataRecs;
    destructor Destroy; override;
    procedure Add(const Rec: TMemDbFieldDataRec);
    procedure AddBlob(Value: Pointer; Len: TMDBSize);
    procedure Clear;
  end;

  THandleEntry = record
    Obj: TObject;
    Kind: THandleKind;
    Gen: Cardinal;
    Parent: TMDBHandle;
    NextFree: Integer;
    Busy: boolean; //Set while a commit is in progress on this handle.
  end;

  THandleArray = array of TMDBHandle;

  { Thread safe table of live handles.
    Handle value: high 32 bits generation, low 32 bits slot index + 1.
    Zero is never a valid handle. }
  THandleTable = class
  private
    FLock: TCriticalSection;
    FEntries: array of THandleEntry;
    FFreeHead: Integer;
    FNextGen: Cardinal;
    function SlotOf(H: TMDBHandle): Integer; //Lock held. -1 if not live.
  public
    constructor Create;
    destructor Destroy; override;
    function Add(Kind: THandleKind; Obj: TObject; Parent: TMDBHandle): TMDBHandle;
    function Get(H: TMDBHandle; Kind: THandleKind): TObject;
    function KindOf(H: TMDBHandle): THandleKind;
    function Children(H: TMDBHandle): THandleArray;
    function Roots: THandleArray;
    procedure SetBusy(H: TMDBHandle; Value: boolean);
    function Take(H: TMDBHandle; out Kind: THandleKind; AllowBusy: boolean = false): TObject;
  end;

var
  Handles: THandleTable;

threadvar
  GErrCode: Integer;
  GErrLen: Integer;
  GErrMsg: array[0..ERR_MSG_MAX - 1] of AnsiChar;

{ EMDBCApiError }

constructor EMDBCApiError.Create(ACode: Integer; const Msg: string);
begin
  inherited Create(Msg);
  FCode := ACode;
end;

{ TKeyRec }

destructor TKeyRec.Destroy;
begin
  Clear;
  inherited;
end;

procedure TKeyRec.Add(const Rec: TMemDbFieldDataRec);
begin
  SetLength(Recs, Succ(Length(Recs)));
  Recs[High(Recs)] := Rec;
end;

procedure TKeyRec.AddBlob(Value: Pointer; Len: TMDBSize);
var
  Rec: TMemDbFieldDataRec;
  P: Pointer;
begin
  //MemDB2 expects Data = nil exactly when size = 0.
  P := nil;
  if Len > 0 then
    GetMem(P, Len);
  try
    if Len > 0 then
      Move(Value^, P^, Len);
    Rec.FieldType := ftBlob;
    Rec.Data := P;
    Rec.size := Len;
    Add(Rec);
  except
    FreeMem(P);
    raise;
  end;
end;

procedure TKeyRec.Clear;
var
  i: integer;
begin
  for i := 0 to High(Recs) do
    if Recs[i].FieldType = ftBlob then
    begin
      FreeMem(Recs[i].Data);
      Recs[i].Data := nil;
      Recs[i].size := 0;
    end;
  SetLength(Recs, 0);
end;

{ THandleTable }

constructor THandleTable.Create;
begin
  inherited;
  FLock := TCriticalSection.Create;
  FFreeHead := -1;
  FNextGen := 1;
end;

destructor THandleTable.Destroy;
begin
  //Live objects are deliberately not freed here: this runs at DLL unload,
  //under the OS loader lock, where stopping database threads could deadlock.
  FLock.Free;
  inherited;
end;

function THandleTable.SlotOf(H: TMDBHandle): Integer;
var
  Idx: UInt64;
begin
  result := -1;
  Idx := H and $FFFFFFFF;
  if (Idx = 0) or (Idx > UInt64(Length(FEntries))) then
    exit;
  if (FEntries[Idx - 1].Kind = hkNone)
    or (FEntries[Idx - 1].Gen <> Cardinal(H shr 32)) then
    exit;
  result := Integer(Idx - 1);
end;

function THandleTable.Add(Kind: THandleKind; Obj: TObject; Parent: TMDBHandle): TMDBHandle;
var
  Slot: Integer;
begin
  Assert(Kind <> hkNone);
  FLock.Acquire;
  try
    if FFreeHead >= 0 then
    begin
      Slot := FFreeHead;
      FFreeHead := FEntries[Slot].NextFree;
    end
    else
    begin
      Slot := Length(FEntries);
      SetLength(FEntries, Slot + 1);
    end;
    FEntries[Slot].Obj := Obj;
    FEntries[Slot].Kind := Kind;
    FEntries[Slot].Gen := FNextGen;
    FEntries[Slot].Parent := Parent;
    FEntries[Slot].NextFree := -1;
    FEntries[Slot].Busy := false;
    result := (UInt64(FNextGen) shl 32) or UInt64(Slot + 1);
    Inc(FNextGen);
    if FNextGen = 0 then
      FNextGen := 1;
  finally
    FLock.Release;
  end;
end;

function THandleTable.Get(H: TMDBHandle; Kind: THandleKind): TObject;
var
  Slot: Integer;
begin
  result := nil;
  FLock.Acquire;
  try
    Slot := SlotOf(H);
    if Slot < 0 then
      raise EMDBCApiError.Create(MDB_ERR_BAD_HANDLE, 'Invalid or closed handle.');
    if FEntries[Slot].Kind <> Kind then
      raise EMDBCApiError.Create(MDB_ERR_WRONG_HANDLE_TYPE, 'Handle is the wrong type for this function.');
    if FEntries[Slot].Busy then
      raise EMDBCApiError.Create(MDB_ERR_HANDLE_BUSY, 'Handle is busy (a commit is in progress).');
    result := FEntries[Slot].Obj;
  finally
    FLock.Release;
  end;
end;

function THandleTable.KindOf(H: TMDBHandle): THandleKind;
var
  Slot: Integer;
begin
  result := hkNone;
  FLock.Acquire;
  try
    Slot := SlotOf(H);
    if Slot < 0 then
      raise EMDBCApiError.Create(MDB_ERR_BAD_HANDLE, 'Invalid or closed handle.');
    result := FEntries[Slot].Kind;
  finally
    FLock.Release;
  end;
end;

function THandleTable.Children(H: TMDBHandle): THandleArray;
var
  i: integer;
begin
  SetLength(result, 0);
  FLock.Acquire;
  try
    for i := 0 to Pred(Length(FEntries)) do
    begin
      if (FEntries[i].Kind <> hkNone) and (FEntries[i].Parent = H) then
      begin
        SetLength(result, Succ(Length(result)));
        result[High(result)] := (UInt64(FEntries[i].Gen) shl 32) or UInt64(i + 1);
      end;
    end;
  finally
    FLock.Release;
  end;
end;

procedure THandleTable.SetBusy(H: TMDBHandle; Value: boolean);
var
  Slot: Integer;
begin
  FLock.Acquire;
  try
    Slot := SlotOf(H);
    if Slot < 0 then
      raise EMDBCApiError.Create(MDB_ERR_BAD_HANDLE, 'Invalid or closed handle.');
    if Value and FEntries[Slot].Busy then
      raise EMDBCApiError.Create(MDB_ERR_HANDLE_BUSY, 'Handle is busy (a commit is in progress).');
    FEntries[Slot].Busy := Value;
  finally
    FLock.Release;
  end;
end;

function THandleTable.Roots: THandleArray;
begin
  result := Children(0); //Databases, keys and string lists have no parent.
end;

function THandleTable.Take(H: TMDBHandle; out Kind: THandleKind; AllowBusy: boolean): TObject;
var
  Slot: Integer;
begin
  result := nil;
  Kind := hkNone;
  FLock.Acquire;
  try
    Slot := SlotOf(H);
    if Slot < 0 then
      raise EMDBCApiError.Create(MDB_ERR_BAD_HANDLE, 'Invalid or closed handle.');
    if FEntries[Slot].Busy and not AllowBusy then
      raise EMDBCApiError.Create(MDB_ERR_HANDLE_BUSY, 'Handle is busy (a commit is in progress).');
    result := FEntries[Slot].Obj;
    Kind := FEntries[Slot].Kind;
    FEntries[Slot].Obj := nil;
    FEntries[Slot].Kind := hkNone;
    FEntries[Slot].Parent := 0;
    FEntries[Slot].Busy := false;
    FEntries[Slot].NextFree := FFreeHead;
    FFreeHead := Slot;
  finally
    FLock.Release;
  end;
end;

{ Error handling }

procedure SetLastErr(Code: Integer; const Msg: string);
var
  U: UTF8String;
  L: Integer;
begin
  GErrCode := Code;
  U := UTF8Encode(Msg);
  L := Length(U);
  if L > ERR_MSG_MAX - 1 then
  begin
    L := ERR_MSG_MAX - 1;
    //Don't cut a multi byte UTF-8 sequence in half.
    while (L > 0) and ((Byte(U[L + 1]) and $C0) = $80) do
      Dec(L);
  end;
  if L > 0 then
    Move(PAnsiChar(U)^, GErrMsg[0], L);
  GErrMsg[L] := #0;
  GErrLen := L;
end;

function Ok: Integer;
begin
  GErrCode := MDB_OK;
  GErrLen := 0;
  GErrMsg[0] := #0;
  result := MDB_OK;
end;

{ Map an exception to a status code, record its message, return the code.
  Most derived classes first. }
function Fail(E: TObject): Integer;
var
  Msg: string;
begin
  if E is Exception then
    Msg := Exception(E).Message
  else if Assigned(E) then
    Msg := E.ClassName
  else
    Msg := 'Unknown error.';

  if E is EMDBCApiError then
    result := EMDBCApiError(E).Code
  else if E is EMemDBConcurrencyException then
    result := MDB_ERR_CONCURRENCY
  else if E is EMemDBAPIException then
    result := MDB_ERR_API
  else if E is EMemDBConsistencyException then
    result := MDB_ERR_CONSISTENCY
  else if E is EMemDBInternalException then
    result := MDB_ERR_INTERNAL
  else if E is EMemDBException then
    result := MDB_ERR_DB
  else if E is EOutOfMemory then
    result := MDB_ERR_OUT_OF_MEMORY
  else if E is EAssertionFailed then
    result := MDB_ERR_INTERNAL
  else
    result := MDB_ERR_UNEXPECTED;

  if (E is Exception) and not (E is EMDBCApiError) then
    Msg := E.ClassName + ': ' + Msg;
  SetLastErr(result, Msg);
end;

procedure RaiseCApi(Code: Integer; const Msg: string);
begin
  raise EMDBCApiError.Create(Code, Msg);
end;

{ Argument helpers }

procedure CheckOut(P: Pointer; const ArgName: string);
begin
  if not Assigned(P) then
    RaiseCApi(MDB_ERR_INVALID_ARG, ArgName + ' must not be NULL.');
end;

function InStr(P: PAnsiChar; const ArgName: string): string;
begin
  if not Assigned(P) then
    RaiseCApi(MDB_ERR_INVALID_ARG, ArgName + ' must not be NULL.');
  result := UTF8ToUnicodeString(P);
end;

function InStrOpt(P: PAnsiChar): string;
begin
  if Assigned(P) then
    result := UTF8ToUnicodeString(P)
  else
    result := '';
end;

{ Copy a string out as UTF-8. Buf = nil: size query only. }
procedure OutStr(const S: string; Buf: PAnsiChar; BufSize: TMDBSize; OutLen: PMDBSize);
var
  U: UTF8String;
  L: TMDBSize;
begin
  CheckOut(OutLen, 'out_len');
  U := UTF8Encode(S);
  L := TMDBSize(Length(U));
  OutLen^ := L;
  if not Assigned(Buf) then
    exit;
  if BufSize < L + 1 then
    RaiseCApi(MDB_ERR_BUFFER_TOO_SMALL,
      Format('Buffer too small: %d bytes needed including the terminating NUL.', [L + 1]));
  if L > 0 then
    Move(PAnsiChar(U)^, Buf^, L);
  Buf[L] := #0;
end;

{ Enum conversions, C values -> MemDB2 }

function ToFieldType(V: Integer): TMDBFieldType;
begin
  case V of
    MDB_FT_INT32: result := ftInteger;
    MDB_FT_UINT32: result := ftCardinal;
    MDB_FT_INT64: result := ftInt64;
    MDB_FT_UINT64: result := ftUint64;
    MDB_FT_STRING: result := ftUnicodeString;
    MDB_FT_DOUBLE: result := ftDouble;
    MDB_FT_BLOB: result := ftBlob;
    MDB_FT_GUID: result := ftGuid;
  else
    RaiseCApi(MDB_ERR_INVALID_ARG, 'Unknown field type.');
    result := ftInteger; //Placate compiler.
  end;
end;

function FromFieldType(FT: TMDBFieldType): Integer;
begin
  case FT of
    ftInteger: result := MDB_FT_INT32;
    ftCardinal: result := MDB_FT_UINT32;
    ftInt64: result := MDB_FT_INT64;
    ftUint64: result := MDB_FT_UINT64;
    ftUnicodeString: result := MDB_FT_STRING;
    ftDouble: result := MDB_FT_DOUBLE;
    ftBlob: result := MDB_FT_BLOB;
    ftGuid: result := MDB_FT_GUID;
  else
    RaiseCApi(MDB_ERR_INTERNAL, 'Unknown MemDB2 field type.');
    result := -1;
  end;
end;

function ToAccessMode(V: Integer): TMDBAccessMode;
begin
  case V of
    MDB_ACCESS_READ_SHARED: result := amReadShared;
    MDB_ACCESS_WRITE_EXCLUSIVE: result := amWriteExclusive;
    MDB_ACCESS_READ_WRITE_SHARED: result := amReadWriteShared;
  else
    RaiseCApi(MDB_ERR_INVALID_ARG, 'Unknown access mode.');
    result := amReadShared;
  end;
end;

function ToSyncMode(V: Integer): TMDBSyncMode;
begin
  case V of
    MDB_SYNC_LAZY_WRITE: result := amLazyWrite;
    MDB_SYNC_FLUSH_BUFFERS: result := amFlushBuffers;
  else
    RaiseCApi(MDB_ERR_INVALID_ARG, 'Unknown sync mode.');
    result := amLazyWrite;
  end;
end;

function ToIsolation(V: Integer): TMDBIsolationLevel;
begin
  case V of
    MDB_ISO_READ_COMMITTED: result := ilReadComitted;
    MDB_ISO_REPEATABLE_READ: result := ilReadRepeatable;
    MDB_ISO_SNAPSHOT: result := ilSnapshot;
    MDB_ISO_SERIALISABLE: result := ilSerialisable;
  else
    RaiseCApi(MDB_ERR_INVALID_ARG, 'Unknown isolation level.');
    result := ilReadRepeatable;
  end;
end;

function ToPosition(V: Integer; EdgeOnly: boolean): TMemAPIPosition;
begin
  case V of
    MDB_POS_FIRST: result := ptFirst;
    MDB_POS_LAST: result := ptLast;
    MDB_POS_NEXT: result := ptNext;
    MDB_POS_PREVIOUS: result := ptPrevious;
  else
    RaiseCApi(MDB_ERR_INVALID_ARG, 'Unknown position.');
    result := ptInvalid;
  end;
  if EdgeOnly and not (result in [ptFirst, ptLast]) then
    RaiseCApi(MDB_ERR_INVALID_ARG, 'Position must be MDB_POS_FIRST or MDB_POS_LAST.');
end;

function ToIndexAttrs(V: Integer): TMDBIndexAttrs;
begin
  if (V and not (MDB_INDEX_UNIQUE or MDB_INDEX_NOT_EMPTY)) <> 0 then
    RaiseCApi(MDB_ERR_INVALID_ARG, 'Unknown index attribute flags.');
  result := [];
  if (V and MDB_INDEX_UNIQUE) <> 0 then
    Include(result, iaUnique);
  if (V and MDB_INDEX_NOT_EMPTY) <> 0 then
    Include(result, iaNotEmpty);
end;

function FromIndexAttrs(A: TMDBIndexAttrs): Integer;
begin
  result := 0;
  if iaUnique in A then
    result := result or MDB_INDEX_UNIQUE;
  if iaNotEmpty in A then
    result := result or MDB_INDEX_NOT_EMPTY;
end;

function FromPhase(P: TMemDBPhase): Integer;
begin
  case P of
    mdbNull: result := MDB_PHASE_NULL;
    mdbInit: result := MDB_PHASE_INIT;
    mdbInitCheckpoint: result := MDB_PHASE_INIT_CHECKPOINT;
    mdbRunning: result := MDB_PHASE_RUNNING;
    mdbRunningError: result := MDB_PHASE_RUNNING_ERROR;
    mdbClosingWaitClients: result := MDB_PHASE_CLOSING_WAIT_CLIENTS;
    mdbClosingWaitClientsError: result := MDB_PHASE_CLOSING_WAIT_CLIENTS_ERROR;
    mdbClosingWaitPersist: result := MDB_PHASE_CLOSING_WAIT_PERSIST;
    mdbClosed: result := MDB_PHASE_CLOSED;
    mdbClosedError: result := MDB_PHASE_CLOSED_ERROR;
  else
    RaiseCApi(MDB_ERR_INTERNAL, 'Unknown MemDB2 phase.');
    result := -1;
  end;
end;

{ Typed handle lookups }

function GetDB(H: TMDBHandle): TMemDB;
begin
  result := Handles.Get(H, hkDB) as TMemDB;
end;

function GetSession(H: TMDBHandle): TMemDBSession;
begin
  result := Handles.Get(H, hkSession) as TMemDBSession;
end;

function GetTxn(H: TMDBHandle): TTxnRec;
begin
  result := Handles.Get(H, hkTxn) as TTxnRec;
end;

function GetTxnAPI(H: TMDBHandle): TMemAPIDatabase;
var
  Rec: TTxnRec;
begin
  Rec := GetTxn(H);
  if not Assigned(Rec.DBAPI) then
    Rec.DBAPI := Rec.Txn.GetAPI;
  result := Rec.DBAPI;
end;

function GetMeta(H: TMDBHandle): TMemAPITableMetadata;
begin
  result := Handles.Get(H, hkTableMeta) as TMemAPITableMetadata;
end;

function GetData(H: TMDBHandle): TMemAPITableData;
begin
  result := Handles.Get(H, hkTableData) as TMemAPITableData;
end;

function GetFK(H: TMDBHandle): TMemAPIForeignKey;
begin
  result := Handles.Get(H, hkForeignKey) as TMemAPIForeignKey;
end;

function GetKey(H: TMDBHandle): TKeyRec;
begin
  result := Handles.Get(H, hkKey) as TKeyRec;
end;

function GetStrList(H: TMDBHandle): TStringList;
begin
  result := Handles.Get(H, hkStrList) as TStringList;
end;

{ Closing. Children first, newest first. A transaction closed this way is
  rolled back. Every object is freed even if an earlier one failed; the first
  failure is re-raised at the end. }

procedure FreeObject(Kind: THandleKind; Obj: TObject);
var
  Rec: TTxnRec;
begin
  case Kind of
    hkTxn:
      begin
        Rec := TTxnRec(Obj);
        try
          FreeAndNil(Rec.DBAPI);
          if Assigned(Rec.Txn) then
            Rec.Txn.RollbackAndFree;
        finally
          Rec.Free;
        end;
      end;
    hkDB:
      begin
        try
          TMemDB(Obj).StopDB;
        finally
          Obj.Free;
        end;
      end;
  else
    Obj.Free;
  end;
end;

{ Returns true if H was detached from the table (it is gone, even if freeing
  the object then failed). A handle that cannot be detached (busy) stays
  open, and so do its ancestors: a parent is never freed under a live child. }
function CloseTree(H: TMDBHandle; var FirstCode: Integer): boolean;
var
  Kids: THandleArray;
  i: integer;
  Kind: THandleKind;
  Obj: TObject;
  AllKidsClosed: boolean;
begin
  result := false;
  AllKidsClosed := true;
  Kids := Handles.Children(H);
  for i := High(Kids) downto 0 do
    if not CloseTree(Kids[i], FirstCode) then
      AllKidsClosed := false;
  if not AllKidsClosed then
    exit; //Error already recorded by the child.
  try
    Obj := Handles.Take(H, Kind);
  except
    if FirstCode = MDB_OK then
      FirstCode := Fail(ExceptObject);
    exit;
  end;
  result := true;
  try
    FreeObject(Kind, Obj);
  except
    if FirstCode = MDB_OK then
      FirstCode := Fail(ExceptObject);
  end;
end;

{ Close H and everything under it. Returns a status, never raises. }
function CloseHandleTree(H: TMDBHandle): Integer;
var
  FirstCode: Integer;
begin
  FirstCode := MDB_OK;
  CloseTree(H, FirstCode);
  if FirstCode = MDB_OK then
    result := Ok
  else
    result := FirstCode; //Message already recorded by Fail.
end;

{ Close only the children of H (used before commit). Raises on failure. }
procedure CloseChildren(H: TMDBHandle);
var
  Kids: THandleArray;
  i: integer;
  FirstCode: Integer;
begin
  FirstCode := MDB_OK;
  Kids := Handles.Children(H);
  for i := High(Kids) downto 0 do
    CloseTree(Kids[i], FirstCode);
  if FirstCode <> MDB_OK then
    //Keep the original failure's message (recorded by Fail).
    RaiseCApi(FirstCode, 'Could not close objects belonging to the transaction: '
      + UTF8ToUnicodeString(PAnsiChar(@GErrMsg[0])));
end;

{ Field value helpers }

function ReadRec(H: TMDBHandle; Field: PAnsiChar; Expect: TMDBFieldType): TMemDbFieldDataRec;
begin
  Assert(Expect <> ftBlob);
  result.Data := nil;
  result.size := 0;
  GetData(H).ReadField(InStr(Field, 'field'), result);
  if result.FieldType <> Expect then
    RaiseCApi(MDB_ERR_TYPE_MISMATCH, Format('Field is type %d, not the type requested.',
      [FromFieldType(result.FieldType)]));
end;

procedure WriteRec(H: TMDBHandle; Field: PAnsiChar; const Rec: TMemDbFieldDataRec);
begin
  GetData(H).WriteField(InStr(Field, 'field'), Rec);
end;

procedure AddKey(H: TMDBHandle; const Rec: TMemDbFieldDataRec);
begin
  GetKey(H).Add(Rec);
end;

function DoFind(Data: TMemAPITableData; Edge: boolean; Pos: TMemAPIPosition;
  const IdxName: string; Key: TKeyRec): boolean;
begin
  if Length(Key.Recs) = 0 then
    RaiseCApi(MDB_ERR_INVALID_ARG, 'Search key has no values.');
  if Edge then
  begin
    if Length(Key.Recs) = 1 then
      result := Data.FindEdgeByIndex(Pos, IdxName, Key.Recs[0])
    else
      result := Data.FindEdgeByMultiFieldIndex(Pos, IdxName, Key.Recs);
  end
  else
  begin
    if Length(Key.Recs) = 1 then
      result := Data.FindByIndex(IdxName, Key.Recs[0])
    else
      result := Data.FindByMultiFieldIndex(IdxName, Key.Recs);
  end;
end;

{ ---------------------------------------------------------------------------
  General
  --------------------------------------------------------------------------- }

function mdb_api_version: Integer;
begin
  result := MDB_CAPI_VERSION;
end;

function mdb_last_error_code: Integer;
begin
  result := GErrCode;
end;

function mdb_last_error_message(buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer;
var
  L: TMDBSize;
begin
  //Does not change the recorded error, so it can be called repeatedly.
  if not Assigned(out_len) then
  begin
    result := MDB_ERR_INVALID_ARG;
    exit;
  end;
  L := TMDBSize(GErrLen);
  out_len^ := L;
  if not Assigned(buf) then
  begin
    result := MDB_OK;
    exit;
  end;
  if buf_size < L + 1 then
  begin
    result := MDB_ERR_BUFFER_TOO_SMALL;
    exit;
  end;
  if L > 0 then
    Move(GErrMsg[0], buf^, L);
  buf[L] := #0;
  result := MDB_OK;
end;

function mdb_close(handle: TMDBHandle): Integer;
begin
  try
    Handles.KindOf(handle); //Validates.
    result := CloseHandleTree(handle);
  except
    result := Fail(ExceptObject);
  end;
end;

{ Close every open handle: databases (and everything beneath them, rolling
  back open transactions), keys and string lists. Call before unloading the
  DLL: database journal threads must be stopped while the DLL is still
  loaded, and this cannot safely be done during DLL unload itself. }
function mdb_shutdown: Integer;
var
  Roots: THandleArray;
  i: integer;
  FirstCode: Integer;
begin
  try
    FirstCode := MDB_OK;
    Roots := Handles.Roots;
    for i := High(Roots) downto 0 do
      CloseTree(Roots[i], FirstCode);
    if FirstCode = MDB_OK then
      result := Ok
    else
      result := FirstCode; //Message recorded by Fail.
  except
    result := Fail(ExceptObject);
  end;
end;

{ ---------------------------------------------------------------------------
  Database
  --------------------------------------------------------------------------- }

function mdb_db_open(path: PAnsiChar; out_db: PMDBHandle): Integer;
begin
  result := mdb_db_open_ex(path, 0, out_db);
end;

function mdb_db_open_ex(path: PAnsiChar; flags: Integer; out_db: PMDBHandle): Integer;
var
  DB: TMemDB;
  Root: string;
  Msg: string;
  Async, AutoChk: boolean;
begin
  try
    CheckOut(out_db, 'out_db');
    out_db^ := 0;
    if (flags and not (MDB_OPEN_ASYNC or MDB_OPEN_NO_AUTO_CHECKPOINT)) <> 0 then
      RaiseCApi(MDB_ERR_INVALID_ARG, 'Unknown flags in mdb_db_open_ex.');
    Async := (flags and MDB_OPEN_ASYNC) <> 0;
    AutoChk := (flags and MDB_OPEN_NO_AUTO_CHECKPOINT) = 0;
    Root := InStr(path, 'path');
    DB := TMemDB.Create;
    try
      //Asynchronous InitDB returns false once loading has started: that is
      //not a failure. The outcome shows up later in the DB phase.
      if not DB.InitDB(Root, jtV2, Async, AutoChk) and not Async then
      begin
        Msg := DB.LastError;
        if Length(Msg) = 0 then
          Msg := 'Could not open database at ' + Root;
        RaiseCApi(MDB_ERR_DB_STATE, Msg);
      end;
    except
      DB.Free;
      raise;
    end;
    out_db^ := Handles.Add(hkDB, DB, 0);
    if (not Async) and (DB.DBState = mdbRunningError) then
      //Open, but commits will fail. The handle is valid: read, or close it.
      RaiseCApi(MDB_ERR_DB_STATE, 'Database opened in an error state: ' + DB.LastError);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_db_close(db: TMDBHandle): Integer;
begin
  try
    GetDB(db);
    result := CloseHandleTree(db);
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_db_checkpoint(db: TMDBHandle): Integer;
begin
  result := mdb_db_checkpoint_ex(db, 0);
end;

function mdb_db_checkpoint_ex(db: TMDBHandle; flags: Integer): Integer;
var
  MemDB: TMemDB;
begin
  try
    MemDB := GetDB(db);
    if (flags and not MDB_CHECKPOINT_ASYNC) <> 0 then
      RaiseCApi(MDB_ERR_INVALID_ARG, 'Unknown flags in mdb_db_checkpoint_ex.');
    //False only when the DB is not in a state to checkpoint;
    //failures while checkpointing raise.
    if not MemDB.Checkpoint((flags and MDB_CHECKPOINT_ASYNC) <> 0) then
      RaiseCApi(MDB_ERR_DB_STATE, 'Checkpoint: database not running. ' + MemDB.LastError);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_db_state(db: TMDBHandle; out_phase: PMDBInt32): Integer;
begin
  try
    CheckOut(out_phase, 'out_phase');
    out_phase^ := FromPhase(GetDB(db).DBState);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_db_last_error(db: TMDBHandle; buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer;
begin
  try
    OutStr(GetDB(db).LastError, buf, buf_size, out_len);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

{ ---------------------------------------------------------------------------
  Session
  --------------------------------------------------------------------------- }

function mdb_session_open(db: TMDBHandle; out_session: PMDBHandle): Integer;
var
  Session: TMemDBSession;
begin
  try
    CheckOut(out_session, 'out_session');
    out_session^ := 0;
    Session := GetDB(db).StartSession;
    try
      out_session^ := Handles.Add(hkSession, Session, db);
    except
      Session.Free;
      raise;
    end;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_session_close(session: TMDBHandle): Integer;
begin
  try
    GetSession(session);
    result := CloseHandleTree(session);
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_session_get_temp_storage(session: TMDBHandle; out_mode: PMDBInt32): Integer;
begin
  try
    CheckOut(out_mode, 'out_mode');
    case GetSession(session).TempStorageMode of
      tsmMemory: out_mode^ := MDB_TEMP_MEMORY;
      tsmDisk: out_mode^ := MDB_TEMP_DISK;
    else
      RaiseCApi(MDB_ERR_INTERNAL, 'Unknown temp storage mode.');
    end;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_session_set_temp_storage(session: TMDBHandle; mode: Integer): Integer;
var
  S: TMemDBSession;
begin
  try
    S := GetSession(session);
    case mode of
      MDB_TEMP_MEMORY: S.TempStorageMode := tsmMemory;
      MDB_TEMP_DISK: S.TempStorageMode := tsmDisk;
    else
      RaiseCApi(MDB_ERR_INVALID_ARG, 'Unknown temp storage mode.');
    end;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

{ ---------------------------------------------------------------------------
  Transaction
  --------------------------------------------------------------------------- }

function mdb_txn_begin(session: TMDBHandle; access: Integer; sync: Integer;
  isolation: Integer; out_txn: PMDBHandle): Integer;
var
  Rec: TTxnRec;
  S: TMemDBSession;
begin
  try
    CheckOut(out_txn, 'out_txn');
    out_txn^ := 0;
    S := GetSession(session);
    Rec := TTxnRec.Create;
    try
      Rec.Txn := S.StartTransaction(ToAccessMode(access), ToSyncMode(sync), ToIsolation(isolation));
      out_txn^ := Handles.Add(hkTxn, Rec, session);
    except
      if Assigned(Rec.Txn) then
        Rec.Txn.RollbackAndFree;
      Rec.Free;
      raise;
    end;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

{ Commit, keeping MemDB2's semantics:
  * Success: MemDB2 frees the transaction; the handle is closed.
  * Failure: MemDB2 leaves the transaction open, and so does this API. The
    handle stays valid; the caller decides whether to roll back or retry.
  Either way, the transaction's table / foreign key handles are closed first:
  MemDB2 will not commit while API objects are outstanding. The transaction's
  database level API object is recreated on demand if it stays open. }
function mdb_txn_commit(txn: TMDBHandle): Integer;
var
  Rec: TTxnRec;
  Kind: THandleKind;
begin
  try
    Rec := GetTxn(txn);
    CloseChildren(txn);
    FreeAndNil(Rec.DBAPI);
    //While MemDB2 commits, no other call may use, open objects on, or
    //close this transaction.
    Handles.SetBusy(txn, true);
    try
      Rec.Txn.CommitAndFree;
    except
      //MemDB2 has left the transaction open: so does the handle.
      Handles.SetBusy(txn, false);
      raise;
    end;
    //Committed, and freed by MemDB2. Drop the handle and the record.
    Rec.Txn := nil;
    Handles.Take(txn, Kind, true);
    Rec.Free;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_txn_rollback(txn: TMDBHandle): Integer;
begin
  try
    GetTxn(txn);
    result := CloseHandleTree(txn);
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_txn_create_table(txn: TMDBHandle; name: PAnsiChar): Integer;
begin
  try
    GetTxnAPI(txn).CreateTable(InStr(name, 'name'));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_txn_create_foreign_key(txn: TMDBHandle; name: PAnsiChar): Integer;
begin
  try
    GetTxnAPI(txn).CreateForeignKey(InStr(name, 'name'));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_txn_rename_entity(txn: TMDBHandle; old_name, new_name: PAnsiChar): Integer;
begin
  try
    GetTxnAPI(txn).RenameTableOrKey(InStr(old_name, 'old_name'), InStr(new_name, 'new_name'));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_txn_delete_entity(txn: TMDBHandle; name: PAnsiChar): Integer;
begin
  try
    GetTxnAPI(txn).DeleteTableOrKey(InStr(name, 'name'));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_txn_entity_names(txn: TMDBHandle; out_list: PMDBHandle): Integer;
var
  SL: TStringList;
begin
  try
    CheckOut(out_list, 'out_list');
    out_list^ := 0;
    SL := GetTxnAPI(txn).GetEntityNames;
    try
      out_list^ := Handles.Add(hkStrList, SL, 0);
    except
      SL.Free;
      raise;
    end;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function OpenApiObject(txn: TMDBHandle; name: PAnsiChar; Out_H: PMDBHandle;
  API: TMemDBAPIId; Kind: THandleKind; const What: string): Integer;
var
  Obj: TMemDBAPI;
  Name_: string;
begin
  try
    CheckOut(Out_H, 'out handle');
    Out_H^ := 0;
    Name_ := InStr(name, 'name');
    //nil if no such entity, or it is the other kind (table vs foreign key).
    Obj := GetTxnAPI(txn).GetAPIObjectFromEntity(Name_, API, false);
    if not Assigned(Obj) then
      RaiseCApi(MDB_ERR_NOT_FOUND, 'No ' + What + ' named "' + Name_ + '".');
    try
      Out_H^ := Handles.Add(Kind, Obj, txn);
    except
      Obj.Free;
      raise;
    end;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_txn_open_table_meta(txn: TMDBHandle; name: PAnsiChar; out_meta: PMDBHandle): Integer;
begin
  result := OpenApiObject(txn, name, out_meta, APITableMetadata, hkTableMeta, 'table');
end;

function mdb_txn_open_table_data(txn: TMDBHandle; name: PAnsiChar; out_data: PMDBHandle): Integer;
begin
  result := OpenApiObject(txn, name, out_data, APITableData, hkTableData, 'table');
end;

function mdb_txn_open_foreign_key(txn: TMDBHandle; name: PAnsiChar; out_fk: PMDBHandle): Integer;
begin
  result := OpenApiObject(txn, name, out_fk, APIForeignKey, hkForeignKey, 'foreign key');
end;

{ ---------------------------------------------------------------------------
  Table metadata
  --------------------------------------------------------------------------- }

function mdb_meta_create_field(meta: TMDBHandle; name: PAnsiChar; field_type: Integer): Integer;
begin
  try
    GetMeta(meta).CreateField(InStr(name, 'name'), ToFieldType(field_type));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_meta_delete_field(meta: TMDBHandle; name: PAnsiChar): Integer;
begin
  try
    GetMeta(meta).DeleteField(InStr(name, 'name'));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_meta_rename_field(meta: TMDBHandle; old_name, new_name: PAnsiChar): Integer;
begin
  try
    GetMeta(meta).RenameField(InStr(old_name, 'old_name'), InStr(new_name, 'new_name'));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_meta_field_type(meta: TMDBHandle; name: PAnsiChar; out_type: PMDBInt32): Integer;
var
  FT: TMDBFieldType;
  Name_: string;
begin
  try
    CheckOut(out_type, 'out_type');
    Name_ := InStr(name, 'name');
    if not GetMeta(meta).FieldInfo(Name_, FT) then
      RaiseCApi(MDB_ERR_NOT_FOUND, 'No field named "' + Name_ + '".');
    out_type^ := FromFieldType(FT);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_meta_field_names(meta: TMDBHandle; out_list: PMDBHandle): Integer;
var
  SL: TStringList;
begin
  try
    CheckOut(out_list, 'out_list');
    out_list^ := 0;
    SL := GetMeta(meta).GetFieldNames;
    try
      out_list^ := Handles.Add(hkStrList, SL, 0);
    except
      SL.Free;
      raise;
    end;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_meta_create_index(meta: TMDBHandle; name, field_name: PAnsiChar; attrs: Integer): Integer;
begin
  try
    GetMeta(meta).CreateIndex(InStr(name, 'name'), InStr(field_name, 'field_name'), ToIndexAttrs(attrs));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_meta_create_multi_index(meta: TMDBHandle; name: PAnsiChar;
  field_names: PMDBNameArray; field_count: Integer; attrs: Integer): Integer;
var
  Names: TMDBFieldNames;
  i: integer;
begin
  try
    if field_count <= 0 then
      RaiseCApi(MDB_ERR_INVALID_ARG, 'field_count must be at least 1.');
    CheckOut(field_names, 'field_names');
    SetLength(Names, field_count);
    for i := 0 to Pred(field_count) do
      Names[i] := InStr(field_names^[i], Format('field_names[%d]', [i]));
    GetMeta(meta).CreateMultiFieldIndex(InStr(name, 'name'), Names, ToIndexAttrs(attrs));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_meta_delete_index(meta: TMDBHandle; name: PAnsiChar): Integer;
begin
  try
    GetMeta(meta).DeleteIndex(InStr(name, 'name'));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_meta_rename_index(meta: TMDBHandle; old_name, new_name: PAnsiChar): Integer;
begin
  try
    GetMeta(meta).RenameIndex(InStr(old_name, 'old_name'), InStr(new_name, 'new_name'));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_meta_index_info(meta: TMDBHandle; name: PAnsiChar;
  out_field_list: PMDBHandle; out_attrs: PMDBInt32): Integer;
var
  Fields: TMDBFieldNames;
  Attrs: TMDBIndexAttrs;
  SL: TStringList;
  Name_: string;
  i: integer;
begin
  try
    CheckOut(out_field_list, 'out_field_list');
    CheckOut(out_attrs, 'out_attrs');
    out_field_list^ := 0;
    Name_ := InStr(name, 'name');
    if not GetMeta(meta).IndexInfo(Name_, Fields, Attrs) then
      RaiseCApi(MDB_ERR_NOT_FOUND, 'No index named "' + Name_ + '".');
    SL := TStringList.Create;
    try
      for i := 0 to Pred(Length(Fields)) do
        SL.Add(Fields[i]);
      out_field_list^ := Handles.Add(hkStrList, SL, 0);
    except
      SL.Free;
      raise;
    end;
    out_attrs^ := FromIndexAttrs(Attrs);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_meta_index_names(meta: TMDBHandle; out_list: PMDBHandle): Integer;
var
  SL: TStringList;
begin
  try
    CheckOut(out_list, 'out_list');
    out_list^ := 0;
    SL := GetMeta(meta).GetIndexNames;
    try
      out_list^ := Handles.Add(hkStrList, SL, 0);
    except
      SL.Free;
      raise;
    end;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

{ ---------------------------------------------------------------------------
  Table data: navigation and editing
  --------------------------------------------------------------------------- }

function mdb_data_locate(data: TMDBHandle; pos: Integer; index_name: PAnsiChar; out_found: PMDBInt32): Integer;
begin
  try
    CheckOut(out_found, 'out_found');
    out_found^ := Ord(GetData(data).Locate(ToPosition(pos, false), InStrOpt(index_name)));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_find(data: TMDBHandle; index_name: PAnsiChar; key: TMDBHandle; out_found: PMDBInt32): Integer;
begin
  try
    CheckOut(out_found, 'out_found');
    out_found^ := Ord(DoFind(GetData(data), false, ptFirst,
      InStr(index_name, 'index_name'), GetKey(key)));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_find_edge(data: TMDBHandle; pos: Integer; index_name: PAnsiChar;
  key: TMDBHandle; out_found: PMDBInt32): Integer;
begin
  try
    CheckOut(out_found, 'out_found');
    out_found^ := Ord(DoFind(GetData(data), true, ToPosition(pos, true),
      InStr(index_name, 'index_name'), GetKey(key)));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_row_selected(data: TMDBHandle; out_selected: PMDBInt32): Integer;
begin
  try
    CheckOut(out_selected, 'out_selected');
    out_selected^ := Ord(GetData(data).RowSelected);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_append(data: TMDBHandle): Integer;
begin
  try
    GetData(data).Append;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_post(data: TMDBHandle): Integer;
begin
  try
    GetData(data).Post;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_discard(data: TMDBHandle): Integer;
begin
  try
    GetData(data).Discard;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_delete(data: TMDBHandle; auto_inc: Integer): Integer;
begin
  try
    GetData(data).Delete(auto_inc <> 0);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

{ ---------------------------------------------------------------------------
  Table data: field values
  --------------------------------------------------------------------------- }

function mdb_data_read_int32(data: TMDBHandle; field: PAnsiChar; out_value: PMDBInt32): Integer;
begin
  try
    CheckOut(out_value, 'out_value');
    out_value^ := ReadRec(data, field, ftInteger).i32Val;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_read_uint32(data: TMDBHandle; field: PAnsiChar; out_value: PMDBUInt32): Integer;
begin
  try
    CheckOut(out_value, 'out_value');
    out_value^ := ReadRec(data, field, ftCardinal).u32Val;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_read_int64(data: TMDBHandle; field: PAnsiChar; out_value: PMDBInt64): Integer;
begin
  try
    CheckOut(out_value, 'out_value');
    out_value^ := ReadRec(data, field, ftInt64).i64Val;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_read_uint64(data: TMDBHandle; field: PAnsiChar; out_value: PMDBUInt64): Integer;
begin
  try
    CheckOut(out_value, 'out_value');
    out_value^ := ReadRec(data, field, ftUint64).u64Val;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_read_double(data: TMDBHandle; field: PAnsiChar; out_value: PMDBDouble): Integer;
begin
  try
    CheckOut(out_value, 'out_value');
    out_value^ := ReadRec(data, field, ftDouble).dVal;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_read_guid(data: TMDBHandle; field: PAnsiChar; out_value: PMDBGuid): Integer;
begin
  try
    CheckOut(out_value, 'out_value');
    out_value^ := ReadRec(data, field, ftGuid).gVal;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_read_string(data: TMDBHandle; field: PAnsiChar; buf: PAnsiChar;
  buf_size: TMDBSize; out_len: PMDBSize): Integer;
begin
  try
    CheckOut(out_len, 'out_len');
    OutStr(ReadRec(data, field, ftUnicodeString).sVal, buf, buf_size, out_len);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_read_blob(data: TMDBHandle; field: PAnsiChar; buf: Pointer;
  buf_size: TMDBSize; out_len: PMDBSize): Integer;
var
  D: TMemAPITableData;
  FieldName: string;
  Rec: TMemDbFieldDataRec;
  Needed: UInt64;
begin
  try
    CheckOut(out_len, 'out_len');
    D := GetData(data);
    FieldName := InStr(field, 'field');
    //First pass: size query (MemDB2 copies nothing when Data is nil).
    Rec.Data := nil;
    Rec.size := 0;
    D.ReadField(FieldName, Rec);
    if Rec.FieldType <> ftBlob then
      RaiseCApi(MDB_ERR_TYPE_MISMATCH, 'Field is not a blob.');
    Needed := Rec.size;
    if Needed > High(TMDBSize) then
      RaiseCApi(MDB_ERR_BUFFER_TOO_SMALL, 'Blob too large for this process.');
    out_len^ := TMDBSize(Needed);
    if (not Assigned(buf)) or (Needed = 0) then
    begin
      result := Ok;
      exit;
    end;
    if buf_size < Needed then
      RaiseCApi(MDB_ERR_BUFFER_TOO_SMALL,
        Format('Buffer too small: %d bytes needed.', [Needed]));
    //Second pass: copy.
    Rec.Data := buf;
    Rec.size := Needed;
    D.ReadField(FieldName, Rec);
    out_len^ := TMDBSize(Rec.size);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_read_blob_prefix(data: TMDBHandle; field: PAnsiChar; buf: Pointer;
  buf_size: TMDBSize; out_copied: PMDBSize; out_total: PMDBUInt64): Integer;
var
  D: TMemAPITableData;
  FieldName: string;
  Rec: TMemDbFieldDataRec;
  Total: UInt64;
begin
  try
    CheckOut(out_copied, 'out_copied');
    out_copied^ := 0;
    if Assigned(out_total) then
      out_total^ := 0;
    if (buf_size > 0) and not Assigned(buf) then
      RaiseCApi(MDB_ERR_INVALID_ARG, 'buf must not be NULL when buf_size > 0.');
    D := GetData(data);
    FieldName := InStr(field, 'field');
    //Size query (MemDB2 copies nothing when Data is nil).
    Rec.Data := nil;
    Rec.size := 0;
    D.ReadField(FieldName, Rec);
    if Rec.FieldType <> ftBlob then
      RaiseCApi(MDB_ERR_TYPE_MISMATCH, 'Field is not a blob.');
    Total := Rec.size;
    if Assigned(out_total) then
      out_total^ := Total;
    if (buf_size = 0) or (Total = 0) then
    begin
      result := Ok;
      exit;
    end;
    //MemDB2 copies min(buf_size, blob size) bytes from the start of the blob.
    Rec.Data := buf;
    Rec.size := buf_size;
    D.ReadField(FieldName, Rec);
    out_copied^ := TMDBSize(Rec.size);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_write_int32(data: TMDBHandle; field: PAnsiChar; value: Integer): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftInteger;
    Rec.i32Val := value;
    WriteRec(data, field, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_write_uint32(data: TMDBHandle; field: PAnsiChar; value: Cardinal): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftCardinal;
    Rec.u32Val := value;
    WriteRec(data, field, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_write_int64(data: TMDBHandle; field: PAnsiChar; value: Int64): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftInt64;
    Rec.i64Val := value;
    WriteRec(data, field, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_write_uint64(data: TMDBHandle; field: PAnsiChar; value: UInt64): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftUint64;
    Rec.u64Val := value;
    WriteRec(data, field, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_write_double(data: TMDBHandle; field: PAnsiChar; value: Double): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftDouble;
    Rec.dVal := value;
    WriteRec(data, field, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_write_guid(data: TMDBHandle; field: PAnsiChar; value: PMDBGuid): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    CheckOut(value, 'value');
    Rec.FieldType := ftGuid;
    Rec.gVal := value^;
    WriteRec(data, field, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_write_string(data: TMDBHandle; field: PAnsiChar; value: PAnsiChar): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftUnicodeString;
    Rec.sVal := InStr(value, 'value');
    WriteRec(data, field, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_data_write_blob(data: TMDBHandle; field: PAnsiChar; value: Pointer; len: TMDBSize): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    if (len > 0) and not Assigned(value) then
      RaiseCApi(MDB_ERR_INVALID_ARG, 'value must not be NULL when len > 0.');
    Rec.FieldType := ftBlob;
    if len > 0 then
      Rec.Data := value
    else
      Rec.Data := nil;
    Rec.size := len;
    //MemDB2 copies the bytes; the caller keeps ownership of value.
    WriteRec(data, field, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

{ ---------------------------------------------------------------------------
  Search keys
  --------------------------------------------------------------------------- }

function mdb_key_create(out_key: PMDBHandle): Integer;
var
  K: TKeyRec;
begin
  try
    CheckOut(out_key, 'out_key');
    out_key^ := 0;
    K := TKeyRec.Create;
    try
      out_key^ := Handles.Add(hkKey, K, 0);
    except
      K.Free;
      raise;
    end;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_key_clear(key: TMDBHandle): Integer;
begin
  try
    GetKey(key).Clear;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_key_count(key: TMDBHandle; out_count: PMDBInt32): Integer;
begin
  try
    CheckOut(out_count, 'out_count');
    out_count^ := Length(GetKey(key).Recs);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_key_add_int32(key: TMDBHandle; value: Integer): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftInteger;
    Rec.i32Val := value;
    AddKey(key, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_key_add_uint32(key: TMDBHandle; value: Cardinal): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftCardinal;
    Rec.u32Val := value;
    AddKey(key, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_key_add_int64(key: TMDBHandle; value: Int64): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftInt64;
    Rec.i64Val := value;
    AddKey(key, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_key_add_uint64(key: TMDBHandle; value: UInt64): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftUint64;
    Rec.u64Val := value;
    AddKey(key, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_key_add_double(key: TMDBHandle; value: Double): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftDouble;
    Rec.dVal := value;
    AddKey(key, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_key_add_guid(key: TMDBHandle; value: PMDBGuid): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    CheckOut(value, 'value');
    Rec.FieldType := ftGuid;
    Rec.gVal := value^;
    AddKey(key, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_key_add_string(key: TMDBHandle; value: PAnsiChar): Integer;
var
  Rec: TMemDbFieldDataRec;
begin
  try
    Rec.FieldType := ftUnicodeString;
    Rec.sVal := InStr(value, 'value');
    AddKey(key, Rec);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_key_add_blob(key: TMDBHandle; value: Pointer; len: TMDBSize): Integer;
begin
  try
    if (len > 0) and not Assigned(value) then
      RaiseCApi(MDB_ERR_INVALID_ARG, 'value must not be NULL when len > 0.');
    //The key keeps its own copy; the caller keeps ownership of value.
    GetKey(key).AddBlob(value, len);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

{ ---------------------------------------------------------------------------
  Foreign keys
  --------------------------------------------------------------------------- }

function mdb_fk_set_referencing_child(fk: TMDBHandle; table_name, index_name: PAnsiChar): Integer;
begin
  try
    GetFK(fk).SetReferencingChild(InStr(table_name, 'table_name'), InStr(index_name, 'index_name'));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_fk_set_referenced_parent(fk: TMDBHandle; table_name, index_name: PAnsiChar): Integer;
begin
  try
    GetFK(fk).SetReferencedParent(InStr(table_name, 'table_name'), InStr(index_name, 'index_name'));
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function FKPart(fk: TMDBHandle; Child, WantTable: boolean; buf: PAnsiChar;
  buf_size: TMDBSize; out_len: PMDBSize): Integer;
var
  TableName, IndexName: string;
begin
  try
    if Child then
      GetFK(fk).GetReferencingChild(TableName, IndexName)
    else
      GetFK(fk).GetReferencedParent(TableName, IndexName);
    if WantTable then
      OutStr(TableName, buf, buf_size, out_len)
    else
      OutStr(IndexName, buf, buf_size, out_len);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_fk_get_child_table(fk: TMDBHandle; buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer;
begin
  result := FKPart(fk, true, true, buf, buf_size, out_len);
end;

function mdb_fk_get_child_index(fk: TMDBHandle; buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer;
begin
  result := FKPart(fk, true, false, buf, buf_size, out_len);
end;

function mdb_fk_get_parent_table(fk: TMDBHandle; buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer;
begin
  result := FKPart(fk, false, true, buf, buf_size, out_len);
end;

function mdb_fk_get_parent_index(fk: TMDBHandle; buf: PAnsiChar; buf_size: TMDBSize; out_len: PMDBSize): Integer;
begin
  result := FKPart(fk, false, false, buf, buf_size, out_len);
end;

{ ---------------------------------------------------------------------------
  String lists
  --------------------------------------------------------------------------- }

function mdb_strlist_count(list: TMDBHandle; out_count: PMDBInt32): Integer;
begin
  try
    CheckOut(out_count, 'out_count');
    out_count^ := GetStrList(list).Count;
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

function mdb_strlist_get(list: TMDBHandle; index: Integer; buf: PAnsiChar;
  buf_size: TMDBSize; out_len: PMDBSize): Integer;
var
  SL: TStringList;
begin
  try
    SL := GetStrList(list);
    if (index < 0) or (index >= SL.Count) then
      RaiseCApi(MDB_ERR_INVALID_ARG, 'index out of range.');
    OutStr(SL[index], buf, buf_size, out_len);
    result := Ok;
  except
    result := Fail(ExceptObject);
  end;
end;

initialization
  Handles := THandleTable.Create;
finalization
  Handles.Free;
end.
