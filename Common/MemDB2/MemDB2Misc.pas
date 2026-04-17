unit MemDB2Misc;
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
  Misc helper functions and definitions.
}

interface

uses
  SysUtils, Classes, BufferedFileStream, IndexedStore
{$IFDEF USE_TRACKABLES}
  , Trackables
{$ENDIF}
  , Parallelizer, RWWLock
  ;


type
  //Exceptions, modes, indexing.
  EMemDBException = class(Exception);
  EMemDBInternalException = class(EMemDBException);
  EMemDBConsistencyException = class(EMemDBException);
  EMemDBAPIException = class(EMemDBException);
  EMemDBConcurrencyException = class(EMemDBException);
  //N.B. If you add to this exception list, then
  //please update MemDBXLateExceptions

  TMDBAccessMode = (amReadShared, amWriteExclusive, amWriteShared);

const //Compatibility modes - map onto new modes, but deprecated for
      //new DB code.
  amRead = amReadShared;
  amReadWrite = amWriteExclusive;

type
  TMDBSyncMode = (amLazyWrite, amFlushBuffers);
  TMemDBJournalType = (jtV2);

  //Mapping to SQL standard isolations, ilCommittedRead here
  //also implies repeatable read, and serializable.

  //TODO - Need to re-work this with respect to query results and row reading.
  //TODO - Snapshot isolation support.
  //TODO - Snapshot index read at Txion start when snapshot or serialisable.
  TMDBIsolationLevel = (ilReadComitted, ilReadRepeatable, ilSerialisable);

const
  //Old "Isolation" values. All transactions now read their own local data
  //preferentially.
  ilCommittedRead = ilReadRepeatable;
  ilDirtyRead = ilReadRepeatable;

type
  TMDBFieldType = (ftInteger, ftCardinal, ftInt64, ftUint64,
                   ftUnicodeString, ftDouble, ftBlob, ftGuid);
  TMDBFieldTypeSet = set of TMDBFieldType;

  TMDBIndexAttr = (iaUnique, iaNotEmpty);
  TMDBIndexAttrs = set of TMDBIndexAttr;
  TMDBFieldNames = array of string;

  TMDBChangeType = (mctNone, mctAdd, mctChange, mctDelete);

  //TODO - Proper checking for new invalid value.
  TMemAPIPosition = (ptFirst, ptLast, ptNext, ptPrevious, ptInvalid);

  TABSelType = (abCurrent, abNext, abLatest);

  TTransactionID = packed record
    G: TGuid;
    Iso: TMDBIsolationLevel;
    class operator Equal(const Left, Right: TTransactionID): Boolean;
    class operator NotEqual(const Left, Right: TTransactionID): Boolean;
    class function Empty: TTransactionID; static;
    class function NewTransactionID(Iso: TMDBIsolationLevel): TTransactionID; static;
  end;

  TBufSelector = record
    TId: TTransactionId;
    SelType: TABSelType;
    class operator Equal(const Left, Right: TBufSelector): Boolean;
    class operator NotEqual(const Left, Right: TBufSelector): Boolean;
  end;

  TTempStorageMode = (tsmMemory, tsmDisk);

  //The actual data records for cells.
  TMemDbFieldDataRec = record
    sVal: string; //Unfortunately no long strings in variant record parts.
  case FieldType: TMDBFieldType of
    ftUnicodeString: (); //Nothing.
    ftInteger: (i32Val: integer);
    ftCardinal: (u32Val: cardinal);
    ftInt64: (i64Val: int64);
    ftUint64: (u64Val: uint64);
    ftDouble: (dVal: double);
    ftBlob: (size: UInt64; Data: Pointer);
    ftGuid: (gVal: TGUID);
  end;

  TMemDbFieldDataRecs = array of TMemDbFieldDataRec;

  TFieldOffset = integer;
  TFieldOffsets = array of TFieldOffset;
  PFieldOffset = ^TFieldOffset;

  TFieldOffsetsFast = record
    PtrFirst: PFieldOffset;
    Count: integer;
  end;

  function DataRecsSame(const S, O: TMemDBFieldDataRec): boolean;
  function CopyFieldNames(const S: TMDBFieldNames): TMDBFieldNames;
  function FieldNamesSame(const A, B: TMDBFieldNames): boolean;
  function CopyDataRecs(const A:TMemDbFieldDataRecs): TMemDbFieldDataRecs;

  function CopyFieldOffsets(A: TFieldOffsets): TFieldOffsets;

  function DBAccessModeToLockReason(Am: TMDBAccessMode): TRWWLockReason;

  //TODO - Don't really think buf selectors are gonna work the same way.
  //esp now have separate get/pin functions.
  function MakeCurrentBufSelector(const Tid: TTransactionId): TBufSelector;
  function MakeNextBufSelector(const Tid: TTransactionId): TBufSelector;
  function MakeLatestBufSelector(const Tid: TTransactionId): TBufSelector;
type
  //API object numbering.
  TMemDBAPIID = (APIInternalCommitRollback,
                 APIDatabase,
                 APITableMetadata,
                 APITableData,
                 APIForeignKey);

  TMDBIsoStrings = array[TMDBIsolationLevel] of string;
  TMDBABSelTypeStrings = array[TABSelType] of string;
  TMemAPIPositionStrings = array[TMemAPIPosition] of string;

  TMemDBTempFileStream = class(TWriteCachedFileStream)
  private
    FFileName: string;
{$IFDEF USE_TRACKABLES}
    FProxy: TTrackable;
{$ENDIF}
  public
    constructor Create(const FileName: string);
    destructor Destroy; override;
    property FileName:string read FFileName;
  end;

  TMemDBPhase = (mdbNull, mdbInit, mdbRunning,
    mdbClosingWaitClients, mdbClosingWaitPersist, mdbClosed,
    mdbError);

const
  TMemDBPhaseStrings: array[TMemDBPhase] of string =
    ('Null', 'Initialising', 'Running',
    'Closing (WaitClients)', 'Closing (WaitPersist)', 'Closed',
    'Error');

  ONE_MEG = 1024*1024;
  FILE_CACHE_SIZE = ONE_MEG; //Let's not mess about with small cache sizes.

  S_API_NOT_IMPLEMENTED = 'API not yet implemented.';

type
{$IFDEF USE_TRACKABLES}
  TMemStats = class(TTrackable)
{$ELSE}
  TMemStats = class
{$ENDIF}
  //TODO - Possibly "to-string" or other serialization.
  end;

const
  IndexableFieldTypes: TMDBFieldTypeSet = [ftInteger,
                                           ftCardinal,
                                           ftInt64,
                                           ftUint64,
                                           ftUnicodeString,
                                           ftDouble,
                                           ftGuid,
                                           ftBlob];
  MDBIsoStrings: TMDBIsoStrings = ('ilReadComitted', 'ilReadRepeatable', 'ilSerialisable');
  MDBABStrings: TMDBABSelTypeStrings = ('abCurrent', 'abNext', 'abLatest');
  //TODO - check all cases where this is passed around. (New value).
  MemAPIPositionStrings:TMemAPIPositionStrings
    = ('ptFirst', 'ptLast', 'ptNext', 'ptPrevious', 'ptInvalid');

var
  AllIndexAttrs: TMDBIndexAttrs;
  MemDBXlateExceptions: TExceptionHandlerChain;

procedure AppendTrailingDirSlash(var Path: string);

{$IFOPT C+}
function CompareGuids(const OtherGuid, OwnGuid: TGUID): integer;
{$ELSE}
function CompareGuids(const OtherGuid, OwnGuid: TGUID): integer; inline;
{$ENDIF}

implementation

uses
  MemDB2Indexing;

const
  S_TAG_FREED_IDX_SET = 'Index tag data freed whilst index still set.';
  S_IDXTAG_BAD_REFCOUNT = 'Index tag reference counting bad.';
  S_IDXTAG_BAD_INIT = 'Bad or duplicate initialization of index tag data.';
  S_IDXTAG_NOT_PERMANENT = 'Index tag not user permanent index, cannot make temporary.';
  S_IDXTAG_NOT_TEMPORARY = 'Index tag not temporary reindex, cannot make permanent.';
  S_IDXS_ALREADY_SET = 'Index tag link to store already set.';
  S_IDXS_NOT_SET = 'Index tag link to store not set.';
  S_INDEX_ADD_FAILED = 'Failed to add an index.';
  S_INDEX_DELETE_FAILED = 'Failed to delete an index';
  S_IDXS_NOT_USER = 'Index tag data not for a user index, cannot add/remove in this way.';
  S_IDXS_NOT_INTERNAL = 'Index tag data not for an internal index, cannot add/remove in this way';
  S_INTERNAL_INDEX_HAS_STORE_LINK = 'Internal index tagdata attached to specific db';
  S_INDEXTAG_DATA_NOT_VALID = 'Data in index tag not valid for this index type';
  S_OPTIMIZED_INDEX_SWIZZLE_FAILED = 'Internal error, optimised index building failed';

{ Misc functions }

function DBAccessModeToLockReason(Am: TMDBAccessMode): TRWWLockReason;
begin
  case Am of
    amReadShared: result := lrSharedRead;
    amWriteExclusive: result := lrExclusiveWrite;
    amWriteShared: result := lrSharedWrite;
  else
    Assert(false);
    result := lrExclusiveWrite;
  end;
end;

function MakeCurrentBufSelector(const Tid: TTransactionId): TBufSelector;
begin
  result.Tid := Tid;
  result.SelType := abCurrent;
end;

function MakeNextBufSelector(const Tid: TTRansactionId): TBufSelector;
begin
  result.Tid := Tid;
  result.SelType := abNext;
end;

function MakeLatestBufSelector(const Tid: TTRansactionId): TBufSelector;
begin
  result.Tid := Tid;
  result.SelType := abLatest;
end;

{ TTransactionId }

class operator TTransactionId.Equal(const Left, Right: TTransactionID): Boolean;
begin
  result := (Left.G = Right.G);
end;

class operator TTransactionId.NotEqual(const Left, Right: TTransactionID): Boolean;
begin
  result := (Left.G <> Right.G);
end;

class function TTransactionId.Empty: TTransactionID;
begin
  FillChar(result, sizeof(Result), 0);
  result.G := result.G.Empty;
  result.Iso := TMDBIsolationlevel(0);
end;

class function TTransactionId.NewTransactionID(Iso: TMDBIsolationLevel): TTransactionID;
begin
  CreateGUID(result.G);
  result.Iso := Iso;
end;

{ TBufSelector }

class operator TBufSelector.Equal(const Left, Right: TBufSelector): Boolean;
begin
  result := (Left.Tid = Right.Tid) and (Left.SelType = Right.SelType);
end;

class operator TBufSelector.NotEqual(const Left, Right: TBufSelector): Boolean;
begin
  result := (Left.Tid <> Right.Tid) or (Left.SelType <> Right.SelType);
end;

{ TMemDBTempFileStream }

constructor TMemDBTempFileStream.Create(const FileName: string);
begin
  FFileName := FileName;
  inherited Create(FileName, FILE_CACHE_SIZE);
{$IFDEF USE_TRACKABLES}
  FProxy := TTrackable.Create;
{$ENDIF}
end;

destructor TMemDBTempFileStream.Destroy;
var
  Tmp: string;
begin
  Tmp := FFileName;
  try
{$IFDEF USE_TRACKABLES}
    FProxy.Free;
{$ENDIF}
    inherited;
  finally
    if Length(Tmp) > 0 then
      DeleteFile(Tmp);
  end;
end;

{ Misc functions }

function CopyFieldNames(const S: TMDBFieldNames): TMDBFieldNames;
var
  i: integer;
begin
  SetLength(result, Length(S));
  for i := 0 to Pred(Length(Result)) do
    result[i] := S[i];
end;

function FieldNamesSame(const A, B: TMDBFieldNames): boolean;
var
  i: integer;
begin
  result := Length(A) = Length(B);
  if result then
    for i := 0 to Pred(Length(A)) do
      if not (A[i] = B[i]) then
      begin
        result := false;
        exit;
      end;
end;


function CopyDataRecs(const A:TMemDbFieldDataRecs): TMemDbFieldDataRecs;
var
  i: integer;
begin
  SetLength(result, Length(A));
  for i := 0 to Pred(Length(Result)) do
    result[i] := A[i];
end;

function DataRecsSame(const S, O: TMemDBFieldDataRec): boolean;
begin
  result := (S.FieldType = O.FieldType);
  if result then
  begin
    case S.FieldType of
      ftInteger: result := S.i32Val = O.i32Val;
      ftCardinal: result := S.u32Val = O.u32Val;
      ftInt64: result := S.i64Val = O.i64Val;
      ftUint64: result := S.u64Val = O.u64Val;
      ftUnicodeString: result := S.sVal = O.sVal;
      ftDouble: result := S.dVal = O.dVal;
      ftBlob:
      begin
        Assert(Assigned(S.Data) = (S.size > 0));
        Assert(Assigned(O.Data) = (O.size > 0));
        result := (S.size = O.size)
          and (Assigned(S.Data) = Assigned(O.Data))
          and (not Assigned(S.Data))
            or  (CompareMem(S.Data, O.Data, S.size));
      end;
      ftGuid: result := S.gVal = O.gVal;
    else
      Assert(false);
      result := false;
    end;
  end;
end;

function GetAllIndexAttrs: TMDBIndexAttrs;
var
  A: TMDBIndexAttr;
begin
  result := [];
  for A := Low(A) to High(A) do
    result := result + [A];
end;

function CopyFieldOffsets(A: TFieldOffsets): TFieldOffsets;
var
  i: integer;
begin
  SetLength(result, Length(A));
  for i := 0 to Pred(Length(A)) do
    result[i] := A[i];
end;

{$IFDEF MSWINDOWS}
const
  PathSep = '\';
{$ELSE}
const
  PathSep = '/';
{$ENDIF}

procedure AppendTrailingDirSlash(var Path: string);
begin
  if Length(Path) > 0 then
  begin
    if (Path[Length(Path)] <> PathSep) then
      Path := Path + PathSep;
  end;
end;

function CompareGuids(const OtherGuid, OwnGuid: TGUID): integer;
var
  i: integer;
begin
{
    D1: LongWord;
    D2: Word;
    D3: Word;
    D4: array[0..7] of Byte;
}
  if OtherGuid.D1 > OwnGuid.D1 then
    result := 1
  else if OtherGuid.D1 < OwnGuid.D1 then
    result := -1
  else if OtherGuid.D2 > OwnGuid.D2 then
    result := 1
  else if OtherGuid.D2 < OwnGuid.D2 then
    result := -1
  else if OtherGuid.D3 > OwnGuid.D3 then
    result := 1
  else if OtherGuid.D3 < OwnGuid.D3 then
    result := -1
  else
  begin
    result := 0;
    for i := 0 to 7 do
    begin
      if OtherGuid.D4[i] > OwnGuid.D4[i] then
      begin
        result := 1;
        break;
      end
      else if OtherGuid.D4[i] < OwnGuid.D4[i] then
      begin
        result := -1;
        break;
      end;
    end;
  end
end;

{ TMemDBStats }

function MemDBGeneralXlateExceptions(EClass: SysUtils.ExceptClass;
                              EMsg: string): boolean;
begin
  while Assigned(EClass) do
  begin
    if EClass = EMemDBInternalException then
      raise EMemDBInternalException.Create(EMsg);
    if EClass = EMemDBConsistencyException then
      raise EMemDBConsistencyException.Create(EMsg);
    if EClass = EMemDBAPIException then
      raise EMemDBAPIException.Create(EMsg);
    if EClass = EMemDBConcurrencyException then
      raise EMemDBConcurrencyException.Create(EMsg);
    if EClass = EMemDBException then
      raise EMemDbException.Create(EMsg);
    EClass := SysUtils.ExceptClass(EClass.ClassParent);
  end;
  result := false;
end;

initialization
  AllIndexAttrs := GetAllIndexAttrs;
  with MemDBXlateExceptions do
  begin
    Func := MemDBGeneralXlateExceptions;
    Next := nil;
  end;
end.
