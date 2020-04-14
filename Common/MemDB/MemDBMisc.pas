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
  SysUtils, Classes, BufferedFileStream
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

  //TODO - Attribute that says must be unique only if nonzero.
  TMDBIndexAttr = (iaUnique, iaNotEmpty);
  TMDBIndexAttrs = set of TMDBIndexAttr;

  TMDBChangeType = (mctNone, mctAdd, mctChange, mctDelete);

  TMemDBHandle = type TObject;

  TMemAPIPosition = (ptFirst, ptLast, ptNext, ptPrevious);

  TABSelection = (abCurrent, abNext, abLatest);

  TIndexClass = (icCurrent, icMostRecent, icTemporary, icInternal);

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

  procedure JoinLists(Joined, ToAppend: TList);

  procedure ConvertField(const CurField: TMemDbFieldDataRec;
                         var NextField: TMemDbFieldDataRec);

  function IsoToAB(Iso: TMDBIsolationLevel): TABSelection;
  function ABToIndexClass(AB: TABSelection): TIndexClass;
  function IsoToIndexClass(Iso: TMDBIsolationLevel): TIndexClass;

  function DataRecsSame(const S, O: TMemDBFieldDataRec): boolean;

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
  TIndexClassStrings = array[TIndexClass] of string;

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
  IndexClassStrings: TIndexClassStrings
    = ('icCurrent', 'icMostRecent', 'icTemporary', 'icInternal');

var
  AllIndexAttrs: TMDBIndexAttrs;

procedure AppendTrailingDirSlash(var Path: string);

{$IFOPT C+}
function CompareGuids(const OtherGuid, OwnGuid: TGUID): integer;
{$ELSE}
function CompareGuids(const OtherGuid, OwnGuid: TGUID): integer; inline;
{$ENDIF}

implementation


const
  S_NOT_IMPLEMENTED = 'Change field type not yet implemented.';

{ TMemDBWriteCachedFileStream }

constructor TMemDBWriteCachedFileStream.Create(const FileName: string);
begin
  FFileName := FileName;
  inherited Create(FileName);
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


procedure JoinLists(Joined, ToAppend: TList);
var
  JoinedCount, ToAppendCount: integer;
  i: integer;
begin
  JoinedCount := Joined.Count;
  ToAppendCount := ToAppend.Count;
  Joined.Count := JoinedCount + ToAppendCount;
  for i := JoinedCount to Pred(Joined.Count) do
    Joined.Items[i] := ToAppend.Items[i - JoinedCount];
end;

procedure ConvertField(const CurField: TMemDbFieldDataRec;
                       var NextField: TMemDbFieldDataRec);
begin
  raise EMemDBInternalException.Create(S_NOT_IMPLEMENTED);
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

function ABToIndexClass(AB: TABSelection): TIndexClass;
begin
  case AB of
    abCurrent: result := icCurrent;
    abLatest: result := icMostRecent;
  else
    Assert(false);
    result := Low(TIndexClass);
  end;
end;

function IsoToIndexClass(Iso: TMDBIsolationLevel): TIndexClass;
begin
  result := ABToIndexClass(IsoToAB(Iso));
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
