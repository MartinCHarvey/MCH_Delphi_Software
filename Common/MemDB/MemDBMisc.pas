unit MemDBMisc;
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
  Misc helper functions and definitions.
}

interface

{$IFOPT C+}
{$DEFINE USE_TRACKABLES_LOCAL_MEMDBMISC}
{$ENDIF}
{$IFDEF USE_TRACKABLES}
{$DEFINE USE_TRACKABLES_LOCAL_MEMDBMISC}
{$ENDIF}

uses
  SysUtils, Classes, BufferedFileStream, IndexedStore
{$IFDEF USE_TRACKABLES_LOCAL_MEMDBMISC}
  , Trackables
{$ENDIF}
  ;


type
  //Exceptions, modes, indexing.
  EMemDBException = class(Exception);
  EMemDBInternalException = class(EMemDBException);
  EMemDBConsistencyException = class(EMemDBException);
  EMemDBAPIException = class(EMemDBException);

  TMDBAccessMode = (amRead, amReadWrite);
  TMDBSyncMode = (amLazyWrite, amFlushBuffers);
  TMemDBJournalType = (jtV2);

  //Mapping to SQL standard isolations, ilCommittedRead here
  //also implies repeatable read, and serializable.
  TMDBIsolationLevel = (ilDirtyRead, ilCommittedRead);

  TMDBFieldType = (ftInteger, ftCardinal, ftInt64, ftUint64,
                   ftUnicodeString, ftDouble, ftBlob, ftGuid);
  TMDBFieldTypeSet = set of TMDBFieldType;

  TMDBIndexAttr = (iaUnique, iaNotEmpty);
  TMDBIndexAttrs = set of TMDBIndexAttr;
  TMDBFieldNames = array of string;

  TMDBChangeType = (mctNone, mctAdd, mctChange, mctDelete);

  TMemDBHandle = type Pointer;

  TMemAPIPosition = (ptFirst, ptLast, ptNext, ptPrevious);

  TABSelection = (abCurrent, abNext, abLatest);

  TSubIndexClass = (sicCurrent, sicLatest);
  TMainIndexClass = (micPermanent, micTemporary, micInternal);
  TInternalIndexClass = (iicPtr, iicRowId, iicCurCopy, iicNextCopy);
  TTempStorageMode = (tsmMemory, tsmDisk);

  TTagCheckIndexState = (tciPermanentAgreesCurrent, tciPermanentAgreesNext,
                         tciTemporaryAgreesBoth, tciTemporaryAgreesNextOnly);
  TTagCheckProgrammedState = (tcpNotProgrammed, tcpProgrammed, tcpDontCare);
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

  TOldFieldOffset = 0.. $3FFF; //14 bits.
  TFieldOffset = integer;
  TFieldOffsets = array of TFieldOffset;
  //Field offsets used for two slightly different things: Standard ND field offsets
  //and also a list of field absolute indexes. They're close enough the same now.

  function IsoToAB(Iso: TMDBIsolationLevel): TABSelection;
  function ABToSubIndexClass(AB: TABSelection): TSubIndexClass;
  function IsoToSubIndexClass(Iso: TMDBIsolationLevel): TSubIndexClass;
  function DataRecsSame(const S, O: TMemDBFieldDataRec): boolean;
  function CopyFieldNames(const S: TMDBFieldNames): TMDBFieldNames;
  function FieldNamesSame(const A, B: TMDBFieldNames): boolean;
  function CopyDataRecs(const A:TMemDbFieldDataRecs): TMemDbFieldDataRecs;
  // Removed MCH hg changeset 1201, 25/11/24
  //function MultiDataRecsSame(const A,B: TMemDbFieldDataRecs; AssertSameFormat:boolean = true): boolean;
  function CopyFieldOffsets(A: TFieldOffsets): TFieldOffsets;
  // Removed MCH hg changeset 1201, 25/11/24
  //function FieldOffsetsSame(A,B: TFieldOffsets; AssertSameFormat:boolean = true): boolean;

type
  //API object numbering.
  TMemDBAPIID = (APIInternalCommitRollback,
                 APIDatabase,
                 APITableMetadata,
                 APITableData,
                 APIForeignKey);

  TMDBIsoStrings = array[TMDBIsolationLevel] of string;
  TMDBABStrings = array[TABSelection] of string;
  TMemAPIPositionStrings = array[TMemAPIPosition] of string;
  TMainIndexClassStrings = array[TMainIndexClass] of string;
  TInternalIndexClassStrings = array[TInternalIndexClass] of string;
  TSubIndexClassStrings = array[TSubIndexClass] of string;

  TMemDBWriteCachedFileStream = class(TWriteCachedFileStream)
  private
    FFileName: string;
{$IFOPT C+}
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

type
{$IFDEF USE_TRACKABLES}
  TMemStats = class(TTrackable)
{$ELSE}
  TMemStats = class
{$ENDIF}
  //TODO - Possibly "to-string" or other serialization.
  end;

  TMemDBStats = class(TMemStats)
  private
    FEntityStatsList: TList;
    FPhase: TMemDBPhase;
  public
    constructor Create;
    destructor Destroy; override;
    property EntityStatsList: TList read FEntityStatsList;
    property Phase: TMemDBPhase read FPhase write FPhase;
  end;

  TMemDBEntityStats = class(TMemStats)
  private
    FAName: string;
  public
    property AName:string read FAName write FAName;
  end;

  TMemDBTableStats = class(TMemDBEntityStats)
  private
    FFieldCount: Int64;
    FIndexCount: Int64;
    FRowCount: Int64;
  public
    property FieldCount: Int64 read FFieldCount write FFieldCount;
    property IndexCount: Int64 read FIndexCount write FIndexCount;
    property RowCount: Int64 read FRowCount write FRowCount;
  end;

  TMemDBITagData = class;

  TITagStruct = record
    TagData: TMemDBITagData;
    SubIndexClass: TSubIndexClass; //Need two structs for each index, current
                                   //and most recent.
  end;
  PITagStruct = ^TITagStruct;

{$IFDEF USE_TRACKABLES}
  TMemDBITagData = class(TTrackable)
{$ELSE}
  TMemDBITagData = class
{$ENDIF}
  private
    //Do not put tag structs at the start of the class,
    //so we can check that we're not erroneously using them instead of
    //PITagStructs to indexed store calls.
    FIndexClass: TMainIndexClass;
    FInternalIndexClass:TInternalIndexClass;
    //Do not need encap index class.
    FDefaultFieldOffsets: TFieldOffsets;
    FExtraFieldOffsets: TFieldOffsets;
    FInit: boolean;
    FStoreIdxSet: TIndexedStoreO;

    FCurrentTagStruct: TITagStruct;
    FLatestTagStruct: TITagStruct;
  protected
{$IFDEF USE_TRACKABLES}
    function GetExtraInfoText: string; override;
{$ENDIF}
    function GetIdxsSetToStore: boolean;
    function GetInternalIndexClass: TInternalIndexClass;
    function GetTagStructBySubClass(SubClass: TSubINdexClass): PITagStruct;

    function GetDefaultFieldOffsets: TFieldOffsets;
    function GetExtraFieldOffsets: TFieldOffsets;
  public
    constructor Create;
    destructor Destroy; override;

    //For user indexes, classes created dynamically.
    procedure InitPermanent(DefaultFieldOffsets: TFieldOffsets);
    procedure MakePermanentTemporary(NewOffsets: TFieldOffsets);
    procedure CommitRestoreToPermanent;
    procedure RollbackRestoreToPermanent;

    procedure CommitAddIdxsToStore(Store: TIndexedStoreO);
    procedure CommitRmIdxsFromStore;
    procedure RollbackRestoreIdxsToStore(Store: TIndexedStoreO);
    procedure RollbackRmIdxsFromStore;

    //For internal indexes, classes shared between all instances.
    procedure InitInternal(InternalClass: TInternalIndexClass);
    procedure AddInternalIndexToStore(Store:TIndexedStoreO);

    property IdxsSetToStore: boolean read GetIdxsSetToStore;

    property MainIndexClass: TMainIndexClass read FIndexClass;
    property InternalIndexClass:TInternalIndexClass read GetInternalIndexClass;

    property TagStructs[S:TSubIndexClass]:PITagStruct read GetTagStructBySubClass;

    property DefaultFieldOffsets: TFieldOffsets read GetDefaultFieldOffsets;
    property ExtraFieldOffsets: TFieldOffsets read GetExtraFieldOffsets;

  end;


const
  IndexableFieldTypes: TMDBFieldTypeSet = [ftInteger,
                                           ftCardinal,
                                           ftInt64,
                                           ftUint64,
                                           ftUnicodeString,
                                           ftDouble,
                                           ftGuid];
  MDBIsoStrings: TMDBIsoStrings = ('ilDirtyRead', 'ilCommittedRead');
  MDBABStrings: TMDBABStrings = ('abCurrent', 'abNext', 'abLatest');
  MemAPIPositionStrings:TMemAPIPositionStrings
    = ('ptFirst', 'ptLast', 'ptNext', 'ptPrevious');
  MainIndexClassStrings: TMainIndexClassStrings
    = ('micPermanent', 'micTemporary', 'micInternal');
  SubIndexClassStrings: TSubIndexClassStrings
    = ('sicCurrent', 'sicLatest');
  InternalIndexClassStrings: TInternalIndexClassStrings
   = ('iicPtr', 'iicRowId', 'iicCurCopy', 'iicNextCopy');

var
  AllIndexAttrs: TMDBIndexAttrs;

procedure AppendTrailingDirSlash(var Path: string);

{$IFOPT C+}
function CompareGuids(const OtherGuid, OwnGuid: TGUID): integer;
{$ELSE}
function CompareGuids(const OtherGuid, OwnGuid: TGUID): integer; inline;
{$ENDIF}

implementation

uses
  MemDBIndexing;

const
  S_NOT_IMPLEMENTED = 'Change field type not yet implemented.';
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

{  TMemDBITagData }

{$IFDEF USE_TRACKABLES}

function TMemDbITagData.GetExtraInfoText: string;
var
  r1, r2,r3, r4: string;
  i: integer;
begin
  r1 := inherited;
  r3 := '(';
  for i  := 0 to Pred(Length(FDefaultFieldOffsets)) do
    r3 := r3 + IntToStr(FDefaultFieldOffsets[i]) + ', ';
  r3 := r3 + ')';
  r4 := '(';
  for i  := 0 to Pred(Length(FExtraFieldOffsets)) do
    r4 := r4 + IntToStr(FExtraFieldOffsets[i]) + ', ';
  r4 := r4 + ')';

  r2 := MainIndexClassStrings[FIndexClass] + ' ' +
        InternalIndexClassStrings[FInternalIndexClass] + ' ' +
        r3 + ' ' +
        r4 + ' ' +
        BoolToStr(FInit, true) + ' ' +
        BoolToStr(Assigned(FStoreIdxSet), true);
  result := r1 + r2;
end;
{$ENDIF}


function TMemDbITagData.GetIdxsSetToStore: boolean;
begin
  result := Assigned(FStoreIdxSet);
end;

function TMemDbITagData.GetInternalIndexClass: TInternalIndexClass;
begin
  if FIndexClass <> micInternal then
    raise EMemDBInternalException.Create(S_INDEXTAG_DATA_NOT_VALID);
  result := FInternalIndexClass;
end;

function TMemDbITagData.GetDefaultFieldOffsets: TFieldOffsets;
begin
  if not (FIndexClass in [micPermanent, micTemporary]) then
    raise EMemDBInternalException.Create(S_INDEXTAG_DATA_NOT_VALID);
  result := FDefaultFieldOffsets;
end;

function TMemDbITagData.GetExtraFieldOffsets: TFieldOffsets;
begin
  if FIndexClass <> micTemporary then
    raise EMemDBInternalException.Create(S_INDEXTAG_DATA_NOT_VALID);
  result := FExtraFieldOffsets;
end;

function TMemDBITagData.GetTagStructBySubClass(SubClass: TSubINdexClass): PITagStruct;
begin
  case SubClass of
    sicCurrent: result := @FCurrentTagStruct;
    sicLatest: result := @FLatestTagStruct;
  else
    Assert(false);
    result := nil;
  end;
end;

constructor TMemDbITagData.Create;
begin
  inherited;
  with FCurrentTagStruct do
  begin
    TagData := self;
    SubIndexClass := sicCurrent;
  end;
  with FLatestTagStruct do
  begin
    TagData := self;
    SubIndexClass := sicLatest;
  end;
end;

destructor TMemDBITagData.Destroy;
begin
  if IdxsSetToStore then
    raise EMemDBInternalException.Create(S_TAG_FREED_IDX_SET);
  inherited;
end;

procedure TMemDbITagData.InitPermanent(DefaultFieldOffsets: TFieldOffsets);
begin
 if FInit then
   raise EMemDBInternalException.Create(S_IDXTAG_BAD_INIT); //Overwriting prev init?
  FIndexClass := micPermanent;
  FDefaultFieldOffsets := CopyFieldOffsets(DefaultFieldOffsets);
  FInit := true;
end;

procedure TMemDbITagData.MakePermanentTemporary(NewOffsets: TFieldOffsets);
begin
  if (not FInit) or (FIndexClass <> micPermanent) then
    raise EMemDbInternalException.Create(S_IDXTAG_NOT_PERMANENT);
  FIndexClass := micTemporary;
  FExtraFieldOffsets := FDefaultFieldOffsets;
  FDefaultFieldOffsets := CopyFieldOffsets(NewOffsets);
end;

procedure TMemDBITagData.CommitRestoreToPermanent;
begin
  if (not FInit) or (FIndexClass <> micTemporary) then
    raise EMemDBInternalException.Create(S_IDXTAG_NOT_TEMPORARY);
  //Default field offset unchanged as new offset.
  SetLength(FExtraFieldOffsets, 0);
  FIndexClass := micPermanent;
end;

procedure TMemDBITagData.RollbackRestoreToPermanent;
begin
  Assert((FInit) and (FIndexClass = micTemporary));
  FDefaultFieldOffsets := CopyFieldOffsets(FExtraFieldOffsets); //Rollback default changes.
  SetLength(FExtraFieldOffsets, 0);
  FIndexClass := micPermanent;
end;

procedure TMemDBITagData.InitInternal(InternalClass: TInternalIndexClass);
begin
  if FInit then
    raise EMemDBInternalException.Create(S_IDXTAG_BAD_INIT); //Overwriting prev init?
  FIndexClass := micInternal;
  FInternalIndexClass := InternalClass;
  FInit := true;
end;

procedure TMemDBITagData.CommitAddIdxsToStore(Store: TIndexedStoreO);
var
  rv: TIsRetVal;
begin
  if not (FIndexClass in [micPermanent, micTemporary]) then
    raise EMemDBInternalException.Create(S_IDXS_NOT_USER);
  if IdxsSetToStore then
    raise EMemDBInternalException.Create(S_IDXS_ALREADY_SET); //Overwriting prev init?

  if Store.AddIndex(TMemDBIndexNode, @FCurrentTagStruct) <> rvOK then
    raise EMemDbInternalException.Create(S_INDEX_ADD_FAILED);
  if Store.AddIndex(TMemDBIndexNode, @FLatestTagStruct) <> rvOK then
  begin
    rv := Store.DeleteIndex(@FCurrentTagStruct);
    Assert(rv = rvOK);
    raise EMemDbInternalException.Create(S_INDEX_ADD_FAILED);
  end;
  FStoreIdxSet := Store;
end;

procedure TMemDBITagData.CommitRmIdxsFromStore;
var
  rv: TIsRetVal;
begin
  if not (FIndexClass in [micPermanent, micTemporary]) then
    raise EMemDBInternalException.Create(S_IDXS_NOT_USER);
  if not IdxsSetToStore then
    raise EMemDBInternalException.Create(S_IDXS_NOT_SET); //Overwriting prev init?
  if (FStoreIdxSet.DeleteIndex(@FCurrentTagStruct) <> rvOK) then
    raise EMemDbInternalException.Create(S_INDEX_DELETE_FAILED);
  if FStoreIdxSet.DeleteIndex(@FLatestTagStruct) <> rvOK then
  begin
    rv := FStoreIdxSet.AddIndex(TMemDBIndexNode, @FCurrentTagStruct);
    Assert(rv = rvOK);
    raise EMemDbInternalException.Create(S_INDEX_DELETE_FAILED);
  end;
  FStoreIdxSet := nil;
end;

procedure TMemDBITagData.RollbackRestoreIdxsToStore(Store: TIndexedStoreO);
var
  rv: TIsRetVal;
begin
  Assert(FIndexClass in [micPermanent, micTemporary]);
  Assert(not Assigned(FStoreIdxSet) or (FStoreIdxSet = Store));
  rv := Store.AddIndex(TMemDBIndexNode, @FCurrentTagStruct);
  Assert(rv = rvOK);
  if (FIndexClass <> micInternal) then
  begin
    rv := Store.AddIndex(TMemDBIndexNode, @FLatestTagStruct);
    Assert(rv = rvOK);
  end;
  FStoreIdxSet := Store;
end;

procedure TMemDBITagData.RollbackRmIdxsFromStore;
var
  rv: TIsRetVal;
begin
  Assert(FIndexClass in [micPermanent, micTemporary]);
  Assert(IdxsSetToStore);
  rv := FStoreIdxSet.DeleteIndex(@FCurrentTagStruct);
  Assert(rv = rvOK);
  rv := FStoreIdxSet.DeleteIndex(@FLatestTagStruct);
  Assert(rv = rvOK);
  FStoreIdxSet := nil;
end;

procedure TMemDBITagData.AddInternalIndexToStore(Store:TIndexedStoreO);
begin
  if FIndexClass <> micInternal then
    raise EMemDBInternalException.Create(S_IDXS_NOT_INTERNAL);
  if IdxsSetToStore then
    raise EMemDBInternalException.Create(S_INTERNAL_INDEX_HAS_STORE_LINK);
  if Store.AddIndex(TMemDBIndexNode, @FCurrentTagStruct) <> rvOK then
    raise EMemDBInternalException.Create(S_INDEX_ADD_FAILED);
end;

{ TMemDBWriteCachedFileStream }

constructor TMemDBWriteCachedFileStream.Create(const FileName: string);
begin
  FFileName := FileName;
  inherited Create(FileName, FILE_CACHE_SIZE);
{$IFOPT C+}
  FProxy := TTrackable.Create;
{$ENDIF}
end;

destructor TMemDBWriteCachedFileStream.Destroy;
begin
{$IFOPT C+}
  FProxy.Free;
{$ENDIF}
  inherited;
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


function IsoToAB(Iso: TMDBIsolationLevel): TABSelection;
begin
  case Iso of
    ilDirtyRead: result := abLatest;
    ilCommittedRead: result := abCurrent;
  else
    Assert(false);
    result := Low(TABSelection);
  end;
end;

function ABToSubIndexClass(AB: TABSelection): TSubIndexClass;
begin
  case AB of
    abCurrent: result := sicCurrent;
    abLatest: result := sicLatest;
  else
    Assert(false);
    result := Low(TSubIndexClass);
  end;
end;

function IsoToSubIndexClass(Iso: TMDBIsolationLevel): TSubIndexClass;
begin
  result := ABToSubIndexClass(IsoToAB(Iso));
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

constructor TMemDBStats.Create;
begin
  inherited;
  FEntityStatsList := TList.Create;
end;

destructor TMemDBStats.Destroy;
var
  i: integer;
begin
  for i := 0 to Pred(FEntityStatsList.Count) do
    TObject(FEntityStatsList.Items[i]).Free;
  FEntityStatsList.Free;
  inherited;
end;



initialization
  AllIndexAttrs := GetAllIndexAttrs;
end.
