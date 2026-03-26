unit MemDb2Streamable;
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

{
  In memory database.
  Classes for streamable and/or ref counted datastructures.
  No "algorithmic" code here, just data defs, and what's required for
  streaming / copying.
}

interface

uses
  SysUtils,
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  MemDb2Misc, MemDb2Indexing, Classes;

type
  //DB Reffed: Multi-buffered classes,
  //allowing for pinning, ref counting, atomic switchover, etc

  //Streamables now subclasses of this.

{$IFDEF USE_TRACKABLES}
  TMemDBReffed = class(TTrackable)
{$ELSE}
  TMemDBReffed = class
{$ENDIF}
  private
    //TODO - If refs always modified under a double buffered lock,
    //could use non-interlocked.
    FRef: integer;
  public
    constructor Create; virtual;

    procedure Assign(Source: TMemDBReffed); virtual;
    procedure DeepAssign(Source: TMemDBReffed); virtual;
    function Same(Other: TMemDBReffed):boolean; virtual;
    procedure CommitPack; virtual;

    class function Clone(Source: TMemDBReffed):TMemDBReffed;
    class function DeepClone(Source: TMemDBReffed):TMemDBReffed;

    function AddRef: TMemDBReffed;
    function Release: boolean;
    procedure Free;
  end;

  TMemDBReffedClass = class of TMemDBReffed;

  TMemDBReffedList = class(TMemDBReffed)
  private
    FList: TList;
  protected
    function GetCount: integer;
{$IFOPT C+}
    function GetItem(Idx: integer): TMemDBReffed;
    procedure SetItem(Idx: integer; Item: TMemDBReffed);
{$ELSE}
    function GetItem(Idx: integer): TMemDBReffed; inline;
    procedure SetItem(Idx: integer; Item: TMemDBReffed); inline;
{$ENDIF}
  public
    constructor Create; override;
    destructor Destroy; override;
    procedure Assign(Source: TMemDBReffed); override;
    procedure DeepAssign(Source: TMemDBReffed); override;

    function Same(Other: TMemDBReffed):boolean; override;
    procedure CommitPack; override;
    procedure ReleaseAndClear;
    //No delete sentinels in these lists - just allowing for
    //dbl buffered, atomic, ref counted switchover between two sets of stuff.
    function AddNoRef(Item: TMemDBReffed): integer;
    procedure Clear;
    procedure Pack;
    property Count: Integer read GetCount;
    property Items[idx:integer]: TMemDbReffed read GetItem write SetItem; default;
  end;

  TMemDBReffedProxy = class(TMemDBReffed)
  private
    FProxy: TObject;
  public
    procedure Assign(Source: TMemDBReffed); override;
    procedure DeepAssign(Source: TMemDBReffed); override;

    destructor Destroy; override;
    property Proxy: TObject read FProxy write FProxy;
  end;

  TMemDBStreamable = class(TMemDBReffed)
  public
    constructor Create; override;

    procedure ToStream(Stream: TStream); virtual; abstract;
    procedure FromStream(Stream: TStream); virtual; abstract;
    procedure CheckSameAsStream(Stream: TStream); virtual; abstract;
  end;

  TMemStreamableList = class(TMemDBStreamable)
  private
    FList: TList;
  protected
    function LookaheadHelper(Stream: TStream): TMemDBStreamable;
    function GetCount: integer;
{$IFOPT C+}
    function GetItem(Idx: integer): TMemDBStreamable;
    procedure SetItem(Idx: integer; Item: TMemDBStreamable);
{$ELSE}
    function GetItem(Idx: integer): TMemDBStreamable; inline;
    procedure SetItem(Idx: integer; Item: TMemDBStreamable); inline;
{$ENDIF}
  public
    constructor Create; override;
    destructor Destroy; override;
    procedure Assign(Source: TMemDBReffed); override;
    procedure DeepAssign(Source: TMemDBReffed); override;
    function Same(Other: TMemDBReffed):boolean; override;
    procedure CommitPack; override;
    procedure ReleaseAndClear;

    procedure ToStream(Stream: TStream); override;
    procedure FromStream(Stream: TStream); override;
    procedure CheckSameAsStream(Stream: TStream); override;
    function AddNoRef(Item: TMemDBStreamable): integer;
    procedure Clear;
    procedure Pack;
    property Count: Integer read GetCount;
    property Items[idx:integer]: TMemDBSTreamable read GetItem write SetItem; default;
  end;

  //Delete sentinels *not* streamed to disk for
  //A/B buffer changes (handled by changetype).
  //But they *are* streamed to disk for vacancies in list structures.

  TMemDeleteSentinel = class(TMemDBStreamable)
  public
    procedure CommitPack; override;

    procedure ToStream(Stream: TStream); override;
    procedure FromStream(Stream: TStream); override;
    procedure CheckSameAsStream(Stream: TStream); override;
  end;

  TMemFieldDef = class(TMemDBStreamable)
  private
    FFieldType: TMDBFieldType;
    FFieldName: string;
    FFieldIndex: TFieldOffset;
  public
    procedure Assign(Source: TMemDbReffed); override;
    procedure DeepAssign(Source: TMemDbReffed); override;
    function Same(Other: TMemDbReffed):boolean; override;
    procedure ToStream(Stream: TStream); override;
    procedure FromStream(Stream: TStream); override;
    procedure CheckSameAsStream(Stream: TStream); override;
    property FieldType: TMDBFieldType read FFieldType write FFieldType;
    property FieldName: string read FFieldName write FFieldName;
    property FieldIndex: TFieldOffset read FFieldIndex write FFieldIndex;
  end;

  TMemFieldDefs = array of TMemFieldDef;

  TMemFieldData = class(TMemDBStreamable)
  protected
    procedure RecFromStream(Stream: TStream; var Rec: TMemDbFieldDataRec);
    procedure AssignCommon(Source: TMemDBReffed);
  public
    FDataRec: TMemDbFieldDataRec;
    constructor Create; override;
    destructor Destroy; override;

    procedure Assign(Source: TMemDbReffed); override;
    procedure DeepAssign(Source: TMemDbReffed); override;
    function Same(Other: TMemDbReffed):boolean; override;
    procedure ToStream(Stream: TStream); override;
    procedure FromStream(Stream: TStream); override;
    procedure CheckSameAsStream(Stream: TStream); override;
  end;

  TMemIndexDef = class(TMemDBStreamable)
  private
    FIndexName: string;
    FFieldNames: TMDBFieldNames;
    FIndexAttrs: TMDBIndexAttrs;
  protected
    function GetFieldName(i: integer): string;
    procedure SetFieldName(i: integer; const s:string);
    function GetFieldNameCount: integer;
    procedure SetFieldNameCount(i: integer);
  public
    procedure Assign(Source: TMemDbReffed); override;
    procedure DeepAssign(Source: TMemDbReffed); override;
    function Same(Other: TMemDbReffed):boolean; override;
    procedure ToStream(Stream: TStream); override;
    procedure FromStream(Stream: TStream); override;
    procedure CheckSameAsStream(Stream: TStream); override;
    property IndexName: string read FIndexName write FIndexName;
    property FieldNames[i: integer]: string read GetFieldName write SetFieldName;
    property FieldNameCount: integer read GetFieldNameCount write SetFieldNameCount;
    property FieldArray: TMDBFieldNames read FFieldNames; //Try to use this as a const parameter only ....
    property IndexAttrs: TMDBIndexAttrs read FIndexAttrs write FIndexAttrs;
  end;

  TMemEntityMetadataItem = class(TMemDBStreamable)
  private
    FEntityName: string;
  public
    procedure Assign(Source: TMemDbReffed); override;
    procedure DeepAssign(Source: TMemDBReffed); override;
    function Same(Other: TMemDbReffed):boolean; override;
    procedure ToStream(Stream: TStream); override;
    procedure FromStream(Stream: TStream); override;
    procedure CheckSameAsStream(Stream: TStream); override;
    property EntityName: string read FEntityName write FEntityName;
  end;

  TMemTableMetadataItem = class(TMemEntityMetadataItem)
  private
    FFieldDefs: TMemStreamableList;
    FIndexDefs: TMemStreamableList;
  public
    procedure Assign(Source: TMemDbReffed); override;
    procedure DeepAssign(Source: TMemDbReffed); override;
    procedure CommitPack; override;

    constructor Create; override;
    destructor Destroy; override;
    function Same(Other: TMemDbReffed):boolean; override;
    procedure ToStream(Stream: TStream); override;
    procedure FromStream(Stream: TStream); override;
    procedure CheckSameAsStream(Stream: TStream); override;
    property FieldDefs: TMemStreamableList read FFieldDefs write FFieldDefs;
    property IndexDefs: TMemStreamableList read FIndexDefs write FIndexDefs;
  end;

  TMemForeignKeyMetadataItem = class(TMemEntityMetadataItem)
  private
    FTableReferer: string;
    FIndexReferer: string;
    FTableReferred: string;
    FIndexReferred: string;
  public
    procedure Assign(Source: TMemDbReffed); override;
    procedure DeepAssign(Source: TMemDbReffed); override;
    function Same(Other: TMemDbReffed):boolean; override;
    procedure ToStream(Stream: TStream); override;
    procedure FromStream(Stream: TStream); override;
    procedure CheckSameAsStream(Stream: TStream); override;
    property TableReferer: string read FTableReferer write FTableReferer;
    property IndexReferer: string read FIndexReferer write FIndexReferer;
    property TableReferred: string read FTableReferred write FTableReferred;
    property IndexReferred: string read FIndexReferred write FIndexReferred;
  end;

const
  S_BAD_AB_SELECTOR = 'A/B buffered data, bad selector for this operation.';
  S_BAD_TAG = 'Bad tag data in stream. Corrupted input file?';
  S_WRONG_TAG = 'Read wrong tag from data stream. Corrupted input file?';
  S_BAD_CHANGETYPE = 'Bad changetype data in stream. Corrupted input file?';
  S_JOURNAL_REPLAY_INCONSISTENT = 'Journal replay failed, state inconsistent. ';


  //V2 streaming definitions.
type
  TMemStreamTag = (
    mstDBStart,
    mstDBEnd,
    mstTableStart,
    mstTableEnd,
    mstFkStart,
    mstFkEnd,
    mstStringStart,
    mstStringEnd,
    mstTableUnchangedName,
    mstItemChangeType,
    mstDblBufferedStart,
    mstDblBufferedEnd,
    mstStreamableListStart,
    mstStreamableListEnd,
    DEPRECATED_previous1,
    mstFieldDefEnd,
    mstFieldDataStart,
    mstFieldDataEnd,
    DEPRECATED_previous2,
    mstIndexDefEnd,
    DEPRECATED_previous3,
    mstRowEnd,
    mstIndexedListStart,
    mstIndexedListEnd,
    mstDeleteSentinel,
    mstEntityMetadataStart,
    mstEntityMetadataEnd,
    mstTableMetadataStart,
    mstTableMetadataEnd,
    mstFKMetadataStart,
    mstFKMetadataEnd,
    DEPRECATED_previous4,
    mstFieldDefStartV2,
    DEPRECATED_previous5,
    DEPRECATED_previous6,
    mstIndexDefStartV4,
    DEPRECATED_previous7,
    DEPRECATED_previous8,
    mstRowStartV2,
    mstGuidStart,
    mstGuidEnd,
    mstReservedForEscape,
    mstReservedForEscape2 //NB, can add after these. No longer FE, FF.
  );

{$IFOPT C+}
  procedure WrTag(Stream: TStream; Tag: TMemStreamTag);
  function RdTag(Stream: TStream): TMemStreamTag;
  procedure ExpectTag(Stream: TStream; Tag: TMemStreamTag);
  procedure WrStreamString(Stream: TStream; const S: string);
  function RdStreamString(Stream: TStream): string;
  procedure WrStreamChangeType(Stream: TStream; Changetype: TMDBChangeType);
  function RdStreamChangeType(Stream: TStream): TMDBChangeType;
  procedure WrGuid(Stream:TStream; const G:TGUID);
  function RdGuid(Stream:TStream): TGUID;
{$ELSE}
  procedure WrTag(Stream: TStream; Tag: TMemStreamTag); inline;
  function RdTag(Stream: TStream): TMemStreamTag; inline;
  procedure ExpectTag(Stream: TStream; Tag: TMemStreamTag); inline;
  procedure WrStreamString(Stream: TStream; const S: string); inline;
  function RdStreamString(Stream: TStream): string; inline;
  procedure WrStreamChangeType(Stream: TStream; Changetype: TMDBChangeType); inline;
  function RdStreamChangeType(Stream: TStream): TMDBChangeType; inline;
  procedure WrGuid(Stream:TStream; const G:TGUID); inline;
  function RdGuid(Stream:TStream): TGUID; inline;
{$ENDIF}

function AssignedNotSentinel(X: TMemDBStreamable): boolean; inline;
function NotAssignedOrSentinel(X: TMemDBStreamable): boolean; inline;

implementation

uses
  LockAbstractions;

const
  S_MEM_DB_FIELD_TYPE = 'FieldType';
  S_MEM_DB_FIELD_VAL = 'FieldVal';
  S_MEM_DB_FIELD_LEN = 'FieldLen';
  S_DBL_BUFFERED_EQUALITY_CHECK = 'Comparing two complicated items.' +
    ' A bug, or comparing journal entries? Perhaps you wanted to compare the' +
    ' A buffer with the B buffer, or do other pre-commit checks?';
  S_BLOB_SIZES_INCONSISTENT = 'For MemDB, blob sizes must be exactly right.';
  S_BLOB_TOO_LARGE = 'Cannot allocate a blob > 2GB.';
  S_MEM_DB_UNSTREAM_OOR = 'Unstream value: out of range for this type. Corrupted stream?';
  S_STREAMABLE_LIST_LOOKAHEAD_FAILED = 'Streamable list lookahead failed, tag: ';
  S_INDEX_DEF_LOOKAHEAD_FAILED = 'Index def lookahead failed, tag: ';

{ TMemDBReffed }

constructor TMemDBReffed.Create;
begin
  inherited;
  FRef := 1;
end;

function TMemDBReffed.AddRef: TMemDbReffed;
begin
  if Assigned(Self) then
    InterlockedIncrement(FRef);
  result := Self;
end;

function TMemDBReffed.Release: boolean;
begin
  result := false;
  if Assigned(self) then
  begin
    if InterlockedDecrement(FRef) = 0 then
    begin
      result := true;
      inherited Free;
    end;
  end;
end;

procedure TMemDBReffed.Free;
begin
  Assert(false);
  Assert(FRef = 1);
  Release;
end;

procedure TMemDBReffed.Assign(Source: TMemDBReffed);
begin
  //Nothing. Make assign and deep assign totally separate.
end;

procedure TMemDBReffed.DeepAssign(Source: TMemDBReffed);
begin
  //Nothing. Make assign and deep assign totally separate.
end;

procedure TMemDbReffed.CommitPack;
begin
  //Nothing, override for container classes.
end;

function TMemDBReffed.Same(Other: TMemDBReffed):boolean;
begin
  result := Assigned(Other) = Assigned(Self);
  if Assigned(Other) and Assigned(Self) then
    result := (Other is Self.ClassType) and (Self is Other.ClassType);
end;

class function TMemDBReffed.Clone(Source: TMemDBReffed):TMemDBReffed;
var
  SrcClass: TMemDBReffedClass;
begin
  if not Assigned(Source) then
    result := nil
  else
  begin
    Assert(Source.ClassType.InheritsFrom(TMemDBStreamable));
    SrcClass := TMemDBReffedClass(Source.ClassType);
    result := SrcClass.Create;
    result.Assign(Source);
  end;
end;

class function TMemDBReffed.DeepClone(Source: TMemDBReffed):TMemDBReffed;
var
  SrcClass: TMemDBReffedClass;
begin
  if not Assigned(Source) then
    result := nil
  else
  begin
    Assert(Source.ClassType.InheritsFrom(TMemDBStreamable));
    SrcClass := TMemDBReffedClass(Source.ClassType);
    result := SrcClass.Create;
    result.DeepAssign(Source);
  end;
end;

{ TMemDBReffedList }

constructor TMemDBReffedList.Create;
begin
  inherited;
  FList := TList.Create;
end;

destructor TMemDBReffedList.Destroy;
begin
  ReleaseAndClear;
  FList.Free;
  inherited;
end;

function TMemDBReffedList.GetCount: integer;
begin
  result := FList.Count;
end;

function TMemDBReffedList.GetItem(Idx: integer): TMemDBReffed;
begin
  result := FList.Items[idx];
end;

procedure TMemDBReffedList.SetItem(Idx: integer; Item: TMemDBReffed);
begin
  FList.Items[idx] := Item;
end;

function TMemDBReffedList.AddNoRef(Item: TMemDBReffed): integer;
begin
  result := FList.Add(Item);
end;

procedure TMemDBReffedList.Clear;
begin
  FList.Clear;
end;

procedure TMemDBReffedList.Pack;
begin
  FList.Pack;
end;

procedure TMemDBReffedList.Assign(Source: TMemDBReffed);
var
  i: integer;
  S: TMemDBReffedList;
begin
  inherited;
  ReleaseAndClear;
  S := Source as TMemDBReffedList;
  for i := 0 to Pred(S.Count) do
    AddNoRef(S.Items[i].AddRef as TMemDbReffed);
end;

procedure TMemDBReffedList.DeepAssign(Source: TMemDBReffed);
var
  i: integer;
  S: TMemDBReffedList;
begin
  inherited;
  ReleaseAndClear;
  S := Source as TMemDBReffedList;
  for i := 0 to Pred(S.Count) do
    AddNoRef(DeepClone(S.Items[i]) as TMemDBReffed);
end;

procedure TMemDbReffedList.CommitPack;
var
  i: integer;
begin
  inherited;
  for i := 0 to Pred(Count) do
  begin
    if Assigned(Items[i]) then
    begin
      if Items[i] is TMemDeleteSentinel then
      begin
        Items[i].Release;
        Items[i] := nil;
      end
      else
        Items[i].CommitPack;
    end;
    Pack;
  end;
end;

function TMemDBReffedList.Same(Other: TMemDBReffed):boolean;
var
 O: TMemDBReffedList;
 I: integer;
begin
  result := inherited;
  if Result then
  begin
    O := Other as TMemDBReffedList;
    result := (O.Count = Count);
    if result then
    begin
      I := 0;
      while (I < Count) and result do
      begin
        result :=
          (Items[I] as TMemDBStreamable).Same(O.Items[I] as TMemDBStreamable);
        Inc(I);
      end;
    end;
  end;
end;

procedure TMemDBReffedList.ReleaseAndClear;
var
  i: integer;
begin
  for i := 0 to Pred(Count) do
    Items[i].Release;
  Clear;
end;


{ TMemDbReffedProxy }

//Generally the proxies will be used to allow atomic /
//protected access to sets of things via pinned lists,
//so we don't expect direct assignment, only referencing
//via lists.
procedure TMemDbReffedProxy.Assign(Source: TMemDBReffed);
begin
  Assert(false);
end;

procedure TMemDbReffedProxy.DeepAssign(Source: TMemDBReffed);
begin
  Assert(false);
end;

destructor TMemDbReffedProxy.Destroy;
begin
  FProxy.Free;
  inherited;
end;

{ TMemDeleteSentinel }

procedure TMemDeleteSentinel.ToStream(Stream: TStream);
begin
  WrTag(Stream, mstDeleteSentinel);
end;

procedure TMemDeleteSentinel.FromStream(Stream: TStream);
begin
  ExpectTag(Stream, mstDeleteSentinel);
end;

procedure TMemDeleteSentinel.CheckSameAsStream(Stream: TStream);
begin
  ExpectTag(Stream, mstDeleteSentinel);
end;

procedure TMemDeleteSentinel.CommitPack;
begin
  Assert(false);
end;

{ TMemDBStreamable }

constructor TMemDBStreamable.Create;
begin
  inherited;
end;

{ TMemStreamableList }

constructor TMemStreamableList.Create;
begin
  inherited;
  FList := TList.Create;
end;

destructor TMemStreamableList.Destroy;
begin
  ReleaseAndClear;
  FList.Free;
  inherited;
end;

function TMemStreamableList.GetCount: integer;
begin
  result := FList.Count;
end;

function TMemStreamableList.GetItem(Idx: integer): TMemDBStreamable;
begin
  result := FList.Items[idx];
end;

procedure TMemStreamableList.SetItem(Idx: integer; Item: TMemDBStreamable);
begin
  FList.Items[idx] := Item;
end;

function TMemStreamableList.AddNoRef(Item: TMemDBStreamable): integer;
begin
  result := FList.Add(Item);
end;

procedure TMemStreamableList.Clear;
begin
  FList.Clear;
end;

procedure TMemStreamableList.Pack;
begin
  FList.Pack;
end;

function TMemStreamableList.LookaheadHelper(Stream: TStream): TMemDBStreamable;
var
  Pos: Int64;
  Tag: TMemStreamTag;
begin
  Pos := Stream.Position;
  Tag := RdTag(Stream);
  //All the tags we can have for something in a list.
  case Tag of
    mstFieldDefStartV2: result := TMemFieldDef.Create;
    mstFieldDataStart: result := TMemFieldData.Create;
    mstIndexdefStartV4: result := TMemIndexDef.Create;
    mstDeleteSentinel: result := TMemDeleteSentinel.Create;
  else
    raise EMemDBException.Create(S_STREAMABLE_LIST_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
  end;
  Stream.Seek(Pos, TSeekOrigin.soBeginning);
end;


procedure TMemStreamableList.ToStream(Stream: TStream);
var
  Len, idx: integer;
begin
  inherited;
  WrTag(Stream, mstStreamableListStart);
  Len := self.Count;
  Stream.Write(Len, sizeof(Len));
  for idx := 0 to Pred(Len) do
    self.Items[idx].ToStream(Stream);
  WrTag(Stream, mstStreamableListEnd);
end;

procedure TMemStreamableList.FromStream(Stream: TStream);
var
  Len, idx: integer;
  NewItem: TMemDBStreamable;
begin
  inherited;
  ExpectTag(Stream, mstStreamableListStart);
  Stream.Read(Len, sizeof(Len));
  ReleaseAndClear;
  for idx := 0 to Pred(Len) do
  begin
    NewItem := LookaheadHelper(Stream);
    NewItem.FromStream(Stream);
    AddNoRef(NewItem);
  end;
  ExpectTag(Stream, mstStreamableListEnd);
end;

procedure TMemStreamableList.CheckSameAsStream(Stream: TStream);
var
  Len, Idx: integer;
begin
  inherited;
  ExpectTag(Stream, mstStreamableListStart);
  Stream.Read(Len, sizeof(Len));
  if Len <> Count then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
  for Idx := 0 to Pred(Count) do
    Items[idx].CheckSameAsStream(Stream);
  ExpectTag(Stream, mstStreamableListEnd);
end;

procedure TMemStreamableList.Assign(Source: TMemDBReffed);
var
  i: integer;
  S: TMemStreamableList;
begin
  inherited;
  ReleaseAndClear;
  S := Source as TMemStreamableList;
  for i := 0 to Pred(S.Count) do
    AddNoRef(S.Items[i].AddRef as TMemDbStreamable);
end;

procedure TMemStreamableList.DeepAssign(Source: TMemDBReffed);
var
  i: integer;
  S: TMemStreamableList;
begin
  inherited;
  ReleaseAndClear;
  S := Source as TMemStreamableList;
  for i := 0 to Pred(S.Count) do
    AddNoRef(DeepClone(S.Items[i]) as TMemDBStreamable);
end;

procedure TMemStreamableList.CommitPack;
var
  i: integer;
begin
  inherited;
  for i := 0 to Pred(Count) do
  begin
    if Assigned(Items[i]) then
    begin
      if Items[i] is TMemDeleteSentinel then
      begin
        Items[i].Release;
        Items[i] := nil;
      end
      else
        Items[i].CommitPack;
    end;
  end;
  Pack;
end;

function TMemStreamableList.Same(Other: TMemDBReffed):boolean;
var
 O: TMemStreamableList;
 I: integer;
begin
  result := inherited;
  if Result then
  begin
    O := Other as TMemStreamableList;
    result := (O.Count = Count);
    if result then
    begin
      I := 0;
      while (I < Count) and result do
      begin
        result :=
          (Items[I] as TMemDBStreamable).Same(O.Items[I] as TMemDBStreamable);
        Inc(I);
      end;
    end;
  end;
end;

procedure TMemStreamableList.ReleaseAndClear;
var
  i: integer;
begin
  for i := 0 to Pred(Count) do
    Items[i].Release;
  Clear;
end;

{ TMemFieldDef }

procedure TMemFieldDef.ToStream(Stream: TStream);
begin
  WrTag(Stream, mstFieldDefStartV2);
  Stream.Write(FFieldType, sizeof(FFieldType));
  WrStreamString(Stream, FFieldName);
  Stream.Write(FFieldIndex, sizeof(FFieldIndex));
  WrTag(Stream, mstFieldDefEnd);
end;

procedure TMemFieldDef.FromStream(Stream: TStream);
begin
  ExpectTag(Stream, mstFieldDefStartV2);
  Stream.Read(FFieldType, sizeof(FFieldType));
  if not ((FFieldType >= Low(FFieldType))  and (FFieldType <= High(FFieldType))) then
    raise EMemDBException.Create(S_MEM_DB_UNSTREAM_OOR);
  FFieldName := RdStreamString(Stream);
  Stream.Read(FFieldIndex, sizeof(FFieldIndex));
  ExpectTag(Stream, mstFieldDefEnd);
end;

procedure TMemFieldDef.CheckSameAsStream(Stream: TStream);
var
  StreamFieldType: TMDBFieldType;
  StreamFieldName: string;
  StreamFieldIndex: TFieldOffset;
begin
  ExpectTag(Stream, mstFieldDefStartV2);
  Stream.Read(StreamFieldType, sizeof(StreamFieldType));
  if StreamFieldType <> FFieldType then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
  StreamFieldName := RdStreamString(Stream);
  if CompareStr(StreamFieldName, FieldName) <> 0 then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
  Stream.Read(StreamFieldIndex, sizeof(StreamFieldIndex));
  if StreamFieldIndex <> FFieldIndex then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);

  ExpectTag(Stream, mstFieldDefEnd);
end;

procedure TMemFieldDef.Assign(Source: TMemDBReffed);
var
  S: TMemFieldDef;
begin
  inherited;
  S := Source as TMemFieldDef;
  FFieldType := S.FFieldType;
  FFieldName := S.FFieldName;
  FFieldIndex := S.FFieldIndex;
end;

procedure TMemFieldDef.DeepAssign(Source: TMemDBReffed);
var
  S: TMemFieldDef;
begin
  inherited;
  S := Source as TMemFieldDef;
  FFieldType := S.FFieldType;
  FFieldName := S.FFieldName;
  FFieldIndex := S.FFieldIndex;
end;

function TMemFieldDef.Same(Other: TMemDBReffed): boolean;
var
  O: TMemFieldDef;
begin
  result := inherited;
  if result then
  begin
    O := Other as TMemFieldDef;
    result :=
      (FFieldType = O.FFieldType) and
      (FFieldName = O.FFieldName) and
      (FFieldIndex = O.FFieldIndex);
  end;
end;

{ TMemIndexDef }

function TMemIndexDef.GetFieldName(i: integer): string;
begin
  result := FFieldNames[i];
end;

procedure TMemIndexDef.SetFieldName(i: integer; const s:string);
begin
  FFieldNames[i] := s;
end;

function TMemIndexDef.GetFieldNameCount: integer;
begin
  result := Length(FFieldNames);
end;

procedure TMemIndexDef.SetFieldNameCount(i: integer);
begin
  SetLength(FFieldNames, i);
end;

procedure TMemIndexDef.ToStream(Stream: TStream);
var
  i:integer;
begin
  WrTag(Stream, mstIndexDefStartV4);
  WrStreamString(Stream, FIndexName);
  i := Length(FFieldNames);
  Stream.Write(i, sizeof(i));
  for i := 0 to Pred(Length(FFieldNames)) do
    WrStreamString(Stream, FFieldNames[i]);
  Stream.Write(FIndexAttrs, sizeof(FIndexAttrs));
  WrTag(Stream, mstIndexDefEnd);
end;

procedure TMemIndexDef.FromStream(Stream: TStream);
var
  i: integer;
begin
  ExpectTag(Stream, mstIndexDefStartV4);
  FIndexName := RdStreamString(Stream);
  Stream.Read(i, sizeof(i));
  SetLength(FFieldNames, i);
  for i := 0 to Pred(Length(FFieldNames)) do
    FFieldNames[i] := RdStreamString(Stream);

  Stream.Read(FIndexAttrs, sizeof(FIndexAttrs));
  if FIndexAttrs - AllIndexAttrs <> [] then
    raise EMemDBException.Create(S_MEM_DB_UNSTREAM_OOR);

    ExpectTag(Stream, mstIndexDefEnd);
end;

procedure TMemIndexDef.CheckSameAsStream(Stream: TStream);
var
  LclIndexName: string;
  LclFieldNames: TMDBFieldNames;
  LclIndexAttrs: TMDBIndexAttrs;
  i: integer;
begin
  ExpectTag(Stream, mstIndexDefStartV4);
  LclIndexName := RdStreamString(Stream);
  if CompareStr(FIndexName, LclIndexName) <> 0 then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);

  Stream.Read(i, sizeof(i));
  SetLength(LclFieldNames, i);
  for i := 0 to Pred(Length(LclFieldNames)) do
    LclFieldNames[i] := RdStreamString(Stream);

  if not FieldNamesSame(FFieldNames, LclFieldNames) then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);

  Stream.Read(LclIndexAttrs, sizeof(LclIndexAttrs));
  if LclIndexAttrs <> FIndexAttrs then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);

  ExpectTag(Stream, mstIndexDefEnd);
end;

procedure TMemIndexDef.Assign(Source: TMemDBReffed);
var
  S: TMemIndexDef;
begin
  inherited;
  S := Source as TMemIndexDef;
  FIndexName := S.FIndexName;
  FFieldNames := CopyFieldNames(S.FFieldNames);
  FIndexAttrs := S.FIndexAttrs;
end;

procedure TMemIndexDef.DeepAssign(Source: TMemDBReffed);
var
  S: TMemIndexDef;
begin
  inherited;
  S := Source as TMemIndexDef;
  FIndexName := S.FIndexName;
  FFieldNames := CopyFieldNames(S.FFieldNames);
  FIndexAttrs := S.FIndexAttrs;
end;

function TMemIndexDef.Same(Other: TMemDBReffed): boolean;
var
  O: TMemIndexDef;
begin
  result := inherited;
  if result then
  begin
    O := Other as TMemIndexDef;
    result :=
      (FIndexName = O.FIndexName) and
      FieldNamesSame(FFieldNames, O.FFieldNames) and
      (FIndexAttrs = O.FIndexAttrs);
  end;
end;

{ TMemFieldData }

procedure TMemFieldData.ToStream(Stream: TStream);
begin
  WrTag(Stream, mstFieldDataStart);
  Stream.Write(FDataRec.FieldType, sizeof(FDataRec.FieldType));
  case FDataRec.FieldType of
    ftInteger: Stream.Write(FDataRec.i32Val, sizeof(FDataRec.i32Val));
    ftCardinal: Stream.Write(FDataRec.u32Val, sizeof(FDataRec.u32Val));
    ftInt64: Stream.Write(FDataRec.i64Val, sizeof(FDataRec.i64Val));
    ftUint64: Stream.Write(FDataRec.u64Val, sizeof(FDataRec.u64Val));
    ftUnicodeString: WrStreamString(Stream, FDataRec.sVal);
    ftDouble: Stream.Write(FDataRec.dVal, sizeof(FDataRec.dVal));
    ftBlob:
    begin
      Stream.Write(FDataRec.size, sizeof(FDataRec.size));
      Stream.Write(FDataRec.Data^, FDataRec.size);
    end;
    ftGuid: Stream.Write(FDataRec.gVal, sizeof(FDataRec.gVal));
  else
    Assert(false);
  end;
  WrTag(Stream, mstFieldDataEnd);
end;

procedure TMemFieldData.RecFromStream(Stream: TStream; var Rec: TMemDbFieldDataRec);
begin
  ExpectTag(Stream, mstFieldDataStart);
  Stream.Read(Rec.FieldType, sizeof(Rec.FieldType));
  case Rec.FieldType of
    ftInteger: Stream.Read(Rec.i32Val, sizeof(Rec.i32Val));
    ftCardinal: Stream.Read(Rec.u32Val, sizeof(Rec.u32Val));
    ftInt64: Stream.Read(Rec.i64Val, sizeof(Rec.i64Val));
    ftUint64: Stream.Read(Rec.u64Val, sizeof(Rec.u64Val));
    ftUnicodeString: Rec.sVal := RdStreamString(Stream);
    ftDouble: Stream.Read(Rec.dVal, sizeof(Rec.dVal));
    ftBlob:
    begin
      if Assigned(Rec.Data) then
      begin
        FreeMem(Rec.Data);
        Rec.Data := nil;
      end;
      Stream.Read(Rec.size, sizeof(Rec.size));
      if Rec.size > 0 then
      begin
        GetMem(Rec.Data, Rec.size);
        Stream.Read(Rec.Data^, Rec.size);
      end;
    end;
    ftGuid: Stream.Read(Rec.gVal, sizeof(Rec.gVal));
  else
    raise EMemDBException.Create(S_MEM_DB_UNSTREAM_OOR);
  end;
  ExpectTag(Stream, mstFieldDataEnd);
end;


procedure TMemFieldData.FromStream(Stream: TStream);
begin
  RecFromStream(Stream, FDataRec);
end;

procedure TMemFieldData.CheckSameAsStream(Stream: TStream);
var
  LocalRec: TMemDbFieldDataRec;
  OK: boolean;
begin
  FillChar(LocalRec, sizeof(LocalRec), 0);
  RecFromStream(Stream, LocalRec);
  try
    OK := DataRecsSame(FDataRec, LocalRec);
  finally
    if (LocalRec.FieldType = ftBlob) then
      FreeMem(LocalRec.Data);
  end;
  if not OK then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
end;

procedure TMemFieldData.AssignCommon(Source: TMemDBReffed);
var
  S: TMemFieldData;
begin
  inherited;
  S := Source as TMemFieldData;
  if FDataRec.FieldType = ftBlob then
  begin
    Assert(Assigned(FDataRec.Data) = (FDataRec.size > 0));
    if Assigned(FDataRec.Data) then
      FreeMem(FDataRec.Data);
    FDataRec.Data := nil;
    FDataRec.size := 0;
  end;
  FDataRec.FieldType := S.FDataRec.FieldType;
  if FDataRec.FieldType = ftBlob then
  begin
    Assert(Assigned(S.FDataRec.Data) = (S.FDataRec.size > 0));
    try
      if Assigned(S.FDataRec.Data) then
      begin
        GetMem(FDataRec.Data, S.FDataRec.size);
        Move(S.FDataRec.Data^, FDataRec.Data^, S.FDataRec.size);
        FDataRec.size := S.FDataRec.size;
      end
      else
      begin
        FDataRec.Data := nil;
        FDataRec.size := 0;
      end;
    except
      FreeMem(FDataRec.Data);
      FDataRec.size := 0;
      raise;
    end;
  end
  else
    FDataRec := S.FDataRec;
end;

procedure TMemFieldData.Assign(Source: TMemDBReffed);
begin
  inherited;
  AssignCommon(Source);
end;

procedure TMemFieldData.DeepAssign(Source: TMemDbReffed);
begin
  inherited;
  AssignCommon(Source);
end;

constructor TMemFieldData.Create;
begin
  inherited;
  FillChar(FDataRec, sizeof(FDataRec), 0);
end;

destructor TMemFieldData.Destroy;
begin
  if FDataRec.FieldType = ftBlob then
    FreeMem(FDataRec.Data);
  inherited;
end;


function TMemFieldData.Same(Other: TMemDBReffed): boolean;
var
  O: TMemFieldData;
begin
  result := inherited;
  if result then
  begin
    O := Other as TMemFieldData;
    result := DataRecsSame(FDataRec, O.FDataRec);
  end;
end;

{ TMemEntityMetadataItem }

procedure TMemEntityMetadataItem.Assign(Source: TMemDBReffed);
var
  S: TMemEntityMetadataItem;
begin
  inherited;
  S:= Source as TMemEntityMetadataItem;
  FEntityName := S.FEntityName;
end;

procedure TMemEntityMetadataItem.DeepAssign(Source: TMemDBReffed);
var
  S: TMemEntityMetadataItem;
begin
  inherited;
  S:= Source as TMemEntityMetadataItem;
  FEntityName := S.FEntityName;
end;

function TMemEntityMetadataItem.Same(Other: TMemDBReffed):boolean;
var
  O: TMemEntityMetadataItem;
begin
  result := inherited;
  if result then
  begin
    O := Other as TMemEntityMetadataItem;
    result := result and (FEntityName = O.FEntityName);
  end;
end;

procedure TMemEntityMetadataItem.ToStream(Stream: TStream);
begin
  WrTag(Stream, mstEntityMetadataStart);
  WrStreamString(Stream, FEntityName);
  WrTag(Stream, mstEntityMetadataEnd);
end;

procedure TMemEntityMetadataItem.FromStream(Stream: TStream);
begin
  ExpectTag(Stream, mstEntityMetadataStart);
  FEntityName := RdStreamString(Stream);
  ExpectTag(Stream, mstEntityMetadataEnd);
end;

procedure TMemEntityMetadataItem.CheckSameAsStream(Stream: TStream);
var
  SS: string;
begin
  ExpectTag(Stream, mstEntityMetadataStart);
  SS := RdStreamString(Stream);
  if CompareStr(SS, FEntityName) <> 0 then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
  ExpectTag(Stream, mstEntityMetadataEnd);
end;

{ TMemTableMetadataItem }

procedure TMemTableMetadataItem.Assign(Source: TMemDBReffed);
begin
  Assert(false);
  //not clear where / at what point we need to make the assignment,
  //and where we ref as opposed to cloning.
  //Shallow ref of lists? Shallow ref of list items??
  inherited;
end;

procedure TMemTableMetadataItem.DeepAssign(Source: TMemDBReffed);
var
  S: TMemTableMetadataItem;
begin
  inherited;
  S := Source as TMemTableMetadataItem;
  FFieldDefs.Release;
  FIndexDefs.Release;
  FFieldDefs := DeepClone(S.FFieldDefs) as TMemStreamableList;
  FIndexDefs := DeepClone(S.FIndexDefs) as TMemStreamableList;
end;

procedure TMemTableMetadataItem.CommitPack;
begin
  inherited;
  FFieldDefs.CommitPack;
  FIndexDefs.CommitPack;
end;

function TMemTableMetadataItem.Same(Other: TMemDBReffed):boolean;
var
  O: TMemTableMetadataItem;
begin
  result := inherited;
  if result then
  begin
    O := Other as TMemTableMetadataItem;
    result := FFieldDefs.Same(O.FFieldDefs) and FIndexDefs.Same(O.FIndexDefs);
  end;
end;

constructor TMemTableMetadataItem.Create;
begin
  inherited;
  FFieldDefs := TMemStreamableList.Create;
  FIndexDefs := TMemStreamableList.Create;
end;

destructor TMemTableMetadataItem.Destroy;
begin
  FFieldDefs.Release;
  FIndexDefs.Release;
  inherited;
end;

procedure TMemTableMetadataItem.ToStream(Stream: TStream);
begin
  WrTag(Stream, mstTableMetadataStart);
  inherited;
  FFieldDefs.ToStream(Stream);
  FIndexDefs.ToStream(Stream);
  WrTag(Stream, mstTableMetadataEnd);
end;

procedure TMemTableMetadataItem.FromStream(Stream: TStream);
begin
  ExpectTag(Stream, mstTableMetadataStart);
  inherited;
  FFieldDefs.FromStream(Stream);
  FIndexDefs.FromStream(Stream);
  ExpectTag(Stream, mstTableMetadataEnd);
end;

procedure TMemTableMetadataItem.CheckSameAsStream(Stream: TStream);
begin
  ExpectTag(Stream, mstTableMetadataStart);
  inherited;
  FFieldDefs.CheckSameAsStream(Stream);
  FIndexDefs.CheckSameAsStream(Stream);
  ExpectTag(Stream, mstTableMetadataEnd);
end;

{ TMemForeignKeyMetadataItem }

procedure TMemForeignKeyMetadataItem.Assign(Source: TMemDBReffed);
var
  S: TMemForeignKeyMetadataItem;
begin
  inherited;
  S:= Source as TMemForeignKeyMetadataItem;
  FTableReferer := S.FTableReferer;
  FIndexReferer := S.FIndexReferer;
  FTableReferred := S.FTableReferred;
  FIndexReferred := S.FIndexReferred;
end;

procedure TMemForeignKeyMetadataItem.DeepAssign(Source: TMemDBReffed);
var
  S: TMemForeignKeyMetadataItem;
begin
  inherited;
  S:= Source as TMemForeignKeyMetadataItem;
  FTableReferer := S.FTableReferer;
  FIndexReferer := S.FIndexReferer;
  FTableReferred := S.FTableReferred;
  FIndexReferred := S.FIndexReferred;
end;

function TMemForeignKeyMetadataItem.Same(Other: TMemDBReffed):boolean;
var
  O: TMemForeignKeyMetadataItem;
begin
  result := inherited;
  if result then
  begin
    O := Other as TMemForeignKeyMetadataItem;
    result := result and (FTableReferer = O.FTableReferer)
      and (FTableReferred = O.FTableReferred)
      and (FIndexReferer = O.FIndexReferer)
      and (FIndexReferred = O.FIndexReferred);
  end;
end;

procedure TMemForeignKeyMetadataItem.ToStream(Stream: TStream);
begin
  WrTag(Stream, mstFKMetadataStart);
  inherited;
  WrStreamString(Stream, FTableReferer);
  WrStreamString(Stream, FIndexReferer);
  WrStreamString(Stream, FTableReferred);
  WrStreamString(Stream, FIndexReferred);
  WrTag(Stream, mstFKMetadataEnd);
end;

procedure TMemForeignKeyMetadataItem.FromStream(Stream: TStream);
begin
  ExpectTag(Stream, mstFKMetadataStart);
  inherited;
  FTableReferer := RdStreamString(Stream);
  FIndexReferer := RdStreamString(Stream);
  FTableReferred := RdStreamString(Stream);
  FIndexReferred := RdStreamString(Stream);
  ExpectTag(Stream, mstFKMetadataEnd);
end;

procedure TMemForeignKeyMetadataItem.CheckSameAsStream(Stream: TStream);
var
  LclTableReferer,
  LclIndexReferer,
  LclTableReferred,
  LclIndexReferred: string;
begin
  ExpectTag(Stream, mstFKMetadataStart);
  inherited;
  LclTableReferer := RdStreamString(Stream);
  LclIndexReferer := RdStreamString(Stream);
  LclTableReferred := RdStreamString(Stream);
  LclIndexReferred := RdStreamString(Stream);
  if (CompareStr(LclTableReferer, FTableReferer) <> 0) or
    (CompareStr(LclIndexReferer, FIndexReferer) <> 0) or
    (CompareStr(LclTableReferred, FTableReferred) <> 0) or
    (CompareStr(LclIndexReferred, FIndexReferred) <> 0) then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
  ExpectTag(Stream, mstFKMetadataEnd);
end;

//V2 streaming functions and helpers.

procedure WrTag(Stream: TStream; Tag: TMemStreamTag);
begin
  //Extra range checking...
  Assert((Tag >= Low(Tag)) and (Tag <= High(Tag)));
  Stream.Write(Tag, sizeof(Tag));
end;

function RdTag(Stream: TStream): TMemStreamTag;
begin
  Stream.Read(result, sizeof(result));
  if not ((result >= Low(result)) and (result <= High(result))) then
    raise EMemDBException.Create(S_BAD_TAG);
end;

procedure ExpectTag(Stream: TStream; Tag: TMemStreamTag);
var
  RdTag: TMemStreamTag;
begin
  Stream.Read(RdTag, sizeof(RdTag));
  if not ((RdTag >= Low(RdTag)) and (RdTag <= High(RdTag))) then
    raise EMemDBException.Create(S_BAD_TAG);
  if RdTag <> Tag then
    raise EMemDBException.Create(S_WRONG_TAG);
end;

//Would like not to have string tags in the stream as well,
//but reading a bad length marker could be a bit terminal.

procedure WrGuid(Stream:TStream; const G:TGUID);
begin
  WrTag(Stream, mstGuidStart);
  Stream.Write(G, sizeof(G));
  WrTag(Stream, mstGuidEnd);
end;

function RdGuid(Stream:TStream): TGUID;
begin
  ExpectTag(Stream, mstGuidStart);
  Stream.Read(result, sizeof(result));
  ExpectTag(Stream, mstGuidEnd);
end;

procedure WrStreamString(Stream: TStream; const S: string);
var
  Len: integer;
begin
  WrTag(Stream, mstStringStart);
  Len := S.Length;
  Stream.Write(Len, sizeof(Len));
  if Len > 0 then
    Stream.Write(S[1], Len * sizeof(S[1]));
  WrTag(Stream, mstStringEnd);
end;

function RdStreamString(Stream: TStream): string;
var
  Len: integer;
begin
  ExpectTag(Stream, mstStringStart);
  Stream.Read(Len, sizeof(Len));
  SetLength(result, Len);
  if Len > 0 then
    Stream.Read(result[1], Len * sizeof(result[1]));
  ExpectTag(Stream, mstStringEnd);
end;

procedure WrStreamChangeType(Stream: TStream; Changetype: TMDBChangeType);
begin
  WrTag(Stream, mstItemChangeType);
  Assert((Changetype >= Low(Changetype)) and (Changetype <= High(Changetype)));
  Stream.Write(Changetype, sizeof(Changetype));
end;

function RdStreamChangeType(Stream: TStream): TMDBChangeType;
begin
  ExpectTag(Stream, mstItemChangeType);
  Stream.Read(result, sizeof(result));
  if not ((result >= Low(result)) and (result <= High(result))) then
    raise EMemDBException.Create(S_BAD_CHANGETYPE);
end;

function AssignedNotSentinel(X: TMemDBStreamable): boolean;
begin
  result := Assigned(X) and (not (X is TMemDeleteSentinel));
end;

function NotAssignedOrSentinel(X: TMemDBStreamable): boolean;
begin
  result := (not Assigned(X)) or (X is TMemDeleteSentinel);
end;

end.
