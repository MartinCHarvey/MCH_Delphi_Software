unit MemDbStreamable;
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
  Classes for streamable datastructures. No "algorithmic" code here,
  just data defs, and what's required for streaming / copying.

  These classes are used to persist table state, and maintain the journal.
}

interface

//TODO - We should be able to phase out most of the V1 streaming classes.

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  MemDbMisc, MemDbIndexing, Classes;

type

  //V1 streamable classes.
{$IFDEF USE_TRACKABLES}
  TMemDBStreamable = class(TTrackable)
{$ELSE}
  TMemDBStreamable = class
{$ENDIF}
  public
    constructor Create; virtual;
    procedure Assign(Source: TMemDBStreamable); virtual;
    procedure DeepAssign(Source: TMemDBStreamable); virtual;
    //TODO - Do we need the "same" function any more?
    function Same(Other: TMemDBStreamable):boolean; virtual;
    procedure ToStreamV2(Stream: TStream); virtual; abstract;
    procedure FromStreamV2(Stream: TStream); virtual; abstract;
    procedure CheckSameAsStreamV2(Stream: TStream); virtual; abstract;
    class function Clone(Source: TMemDBStreamable):TMemDBStreamable;
    class function DeepClone(Source: TMemDBStreamable):TMemDBStreamable;
  end;

  TMemDBStreamableClass = class of TMemDBStreamable;

  //TODO - Add more implementation to this as required by code.
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
    procedure Assign(Source: TMemDBStreamable); override;
    procedure DeepAssign(Source: TMemDBStreamable); override;
    //TODO - Remove same functions.
    function Same(Other: TMemDBStreamable):boolean; override;
    procedure FreeAndClear;
    procedure RemoveDeleteSentinels;
    procedure ToStreamV2(Stream: TStream); override;
    procedure FromStreamV2(Stream: TStream); override;
    procedure CheckSameAsStreamV2(Stream: TStream); override;
    function Add(Item: TMemDBStreamable): integer;
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
    procedure ToStreamV2(Stream: TStream); override;
    procedure FromStreamV2(Stream: TStream); override;
    procedure CheckSameAsStreamV2(Stream: TStream); override;
  end;

  TMemFieldDef = class(TMemDBStreamable)
  private
    FFieldType: TMDBFieldType;
    FFieldName: string;
    FFieldIndex: TFieldOffset;
  public
    procedure Assign(Source: TMemDBStreamable); override;
    function Same(Other: TMemDBStreamable):boolean; override;
    procedure ToStreamV2(Stream: TStream); override;
    procedure FromStreamV2(Stream: TStream); override;
    procedure CheckSameAsStreamV2(Stream: TStream); override;
    property FieldType: TMDBFieldType read FFieldType write FFieldType;
    property FieldName: string read FFieldName write FFieldName;
    property FieldIndex: TFieldOffset read FFieldIndex write FFieldIndex;
  end;

  TMemFieldData = class(TMemDBStreamable)
  protected
    procedure RecFromStream(Stream: TStream; var Rec: TMemDbFieldDataRec);
  public
    FDataRec: TMemDbFieldDataRec;

    procedure Assign(Source: TMemDBStreamable); override;
    function Same(Other: TMemDBStreamable):boolean; override;
    procedure ToStreamV2(Stream: TStream); override;
    procedure FromStreamV2(Stream: TStream); override;
    procedure CheckSameAsStreamV2(Stream: TStream); override;
  end;

  TMemIndexDef = class(TMemDBStreamable)
  private
    FIndexName: string;
    FFieldName: string;
    FFieldIndex: TFieldOffset;
    FIndexAttrs: TMDBIndexAttrs;
  public
    procedure Assign(Source: TMemDBStreamable); override;
    function Same(Other: TMemDBStreamable):boolean; override;
    procedure ToStreamV2(Stream: TStream); override;
    procedure FromStreamV2(Stream: TStream); override;
    procedure CheckSameAsStreamV2(Stream: TStream); override;
    property IndexName: string read FIndexName write FIndexName;
    property FieldName: string read FFieldName write FFieldName;
    property FieldIndex: TFieldOffset read FFieldIndex write FFieldIndex;
    property IndexAttrs: TMDBIndexAttrs read FIndexAttrs write FIndexAttrs;
  end;

  TMemEntityMetadataItem = class(TMemDBStreamable)
  private
    FEntityName: string;
  public
    procedure Assign(Source: TMemDBStreamable); override;
    function Same(Other: TMemDBStreamable):boolean; override;
    procedure ToStreamV2(Stream: TStream); override;
    procedure FromStreamV2(Stream: TStream); override;
    procedure CheckSameAsStreamV2(Stream: TStream); override;
    property EntityName: string read FEntityName write FEntityName;
  end;

  TMemTableMetadataItem = class(TMemEntityMetadataItem)
  private
    FFieldDefs: TMemStreamableList;
    FIndexDefs: TMemStreamableList;
  public
    procedure DeepAssign(Source: TMemDBStreamable); override;
    constructor Create; override;
    destructor Destroy; override;
    function Same(Other: TMemDBStreamable):boolean; override;
    procedure ToStreamV2(Stream: TStream); override;
    procedure FromStreamV2(Stream: TStream); override;
    procedure CheckSameAsStreamV2(Stream: TStream); override;
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
    procedure Assign(Source: TMemDBStreamable); override;
    function Same(Other: TMemDBStreamable):boolean; override;
    procedure ToStreamV2(Stream: TStream); override;
    procedure FromStreamV2(Stream: TStream); override;
    procedure CheckSameAsStreamV2(Stream: TStream); override;
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
    DEPRECATED_mstFieldDefStart_V1,
    mstFieldDefEnd,
    mstFieldDataStart,
    mstFieldDataEnd,
    DEPRECATED_mstIndexDefStart_V1,
    mstIndexDefEnd,
    mstRowStart,
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
    DEPRECATED_mstIndexFieldDeconflict,
    mstFieldDefStartV2,
    mstIndexDefStartV2
  );

{$IFOPT C+}
  procedure WrTag(Stream: TStream; Tag: TMemStreamTag);
  function RdTag(Stream: TStream): TMemStreamTag;
  procedure ExpectTag(Stream: TStream; Tag: TMemStreamTag);
  procedure WrStreamString(Stream: TStream; const S: string);
  function RdStreamString(Stream: TStream): string;
  procedure WrStreamChangeType(Stream: TStream; Changetype: TMDBChangeType);
  function RdStreamChangeType(Stream: TStream): TMDBChangeType;
{$ELSE}
  procedure WrTag(Stream: TStream; Tag: TMemStreamTag); inline;
  function RdTag(Stream: TStream): TMemStreamTag; inline;
  procedure ExpectTag(Stream: TStream; Tag: TMemStreamTag); inline;
  procedure WrStreamString(Stream: TStream; const S: string); inline;
  function RdStreamString(Stream: TStream): string; inline;
  procedure WrStreamChangeType(Stream: TStream; Changetype: TMDBChangeType); inline;
  function RdStreamChangeType(Stream: TStream): TMDBChangeType; inline;
{$ENDIF}

implementation

uses SysUtils;

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

{ TMemDeleteSentinel }

procedure TMemDeleteSentinel.ToStreamV2(Stream: TStream);
begin
  WrTag(Stream, mstDeleteSentinel);
end;

procedure TMemDeleteSentinel.FromStreamV2(Stream: TStream);
begin
  ExpectTag(Stream, mstDeleteSentinel);
end;

procedure TMemDeleteSentinel.CheckSameAsStreamV2(Stream: TStream);
begin
  ExpectTag(Stream, mstDeleteSentinel);
end;


{ TMemDBStreamable }

constructor TMemDBStreamable.Create;
begin
  inherited;
end;

procedure TMemDBStreamable.Assign(Source: TMemDBStreamable);
begin
  //Don't call inherited (Indirectly AssignTo)
  //  if Source <> nil then Source.AssignTo(Self) else AssignError(nil);
end;

procedure TMemDBStreamable.DeepAssign(Source: TMemDBStreamable);
begin
  Assign(Source);
end;

function TMemDBStreamable.Same(Other: TMemDBStreamable):boolean;
begin
  result := Assigned(Other) = Assigned(Self);
  if Assigned(Other) and Assigned(Self) then
    result := Other is Self.ClassType;
end;

class function TMemDBStreamable.Clone(Source: TMemDBStreamable):TMemDBStreamable;
var
  SrcClass: TMemDBStreamableClass;
begin
  if not Assigned(Source) then
    result := nil
  else
  begin
    Assert(Source.ClassType.InheritsFrom(TMemDBStreamable));
    SrcClass := TMemDBStreamableClass(Source.ClassType);
    result := SrcClass.Create;
    result.Assign(Source);
  end;
end;

class function TMemDBStreamable.DeepClone(Source: TMemDBStreamable):TMemDBStreamable;
var
  SrcClass: TMemDBStreamableClass;
begin
  if not Assigned(Source) then
    result := nil
  else
  begin
    Assert(Source.ClassType.InheritsFrom(TMemDBStreamable));
    SrcClass := TMemDBStreamableClass(Source.ClassType);
    result := SrcClass.Create;
    result.DeepAssign(Source);
  end;
end;

{ TMemStreamableList }

constructor TMemStreamableList.Create;
begin
  inherited;
  FList := TList.Create;
end;

destructor TMemStreamableList.Destroy;
begin
  FreeAndClear;
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

function TMemStreamableList.Add(Item: TMemDBStreamable): integer;
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
    DEPRECATED_mstFieldDefStart_V1, mstFieldDefStartV2: result := TMemFieldDef.Create;
    mstFieldDataStart: result := TMemFieldData.Create;
    DEPRECATED_mstIndexDefStart_V1, mstIndexDefStartV2: result := TMemIndexDef.Create;
    mstDeleteSentinel: result := TMemDeleteSentinel.Create;
  else
    raise EMemDBException.Create(S_STREAMABLE_LIST_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
  end;
  Stream.Seek(Pos, soFromBeginning);
end;


procedure TMemStreamableList.ToStreamV2(Stream: TStream);
var
  Len, idx: integer;
begin
  inherited;
  WrTag(Stream, mstStreamableListStart);
  Len := self.Count;
  Stream.Write(Len, sizeof(Len));
  for idx := 0 to Pred(Len) do
    self.Items[idx].ToStreamV2(Stream);
  WrTag(Stream, mstStreamableListEnd);
end;

procedure TMemStreamableList.FromStreamV2(Stream: TStream);
var
  Len, idx: integer;
  NewItem: TMemDBStreamable;
begin
  inherited;
  ExpectTag(Stream, mstStreamableListStart);
  Stream.Read(Len, sizeof(Len));
  FreeAndClear;
  for idx := 0 to Pred(Len) do
  begin
    NewItem := LookaheadHelper(Stream);
    NewItem.FromStreamV2(Stream);
    Add(NewItem);
  end;
  ExpectTag(Stream, mstStreamableListEnd);
end;

procedure TMemStreamableList.CheckSameAsStreamV2(Stream: TStream);
var
  Len, Idx: integer;
begin
  inherited;
  ExpectTag(Stream, mstStreamableListStart);
  Stream.Read(Len, sizeof(Len));
  if Len <> Count then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
  for Idx := 0 to Pred(Count) do
    Items[idx].CheckSameAsStreamV2(Stream);
  ExpectTag(Stream, mstStreamableListEnd);
end;

procedure TMemStreamableList.Assign(Source: TMemDBStreamable);
var
  i: integer;
  S: TMemStreamableList;
begin
  inherited;
  FreeAndClear;
  S := Source as TMemStreamableList;
  for i := 0 to Pred(S.Count) do
    Add(Clone(S.Items[i]));
end;

procedure TMemStreamableList.DeepAssign(Source: TMemDBStreamable);
var
  i: integer;
  S: TMemStreamableList;
begin
  inherited;
  FreeAndClear;
  S := Source as TMemStreamableList;
  for i := 0 to Pred(S.Count) do
    Add(DeepClone(S.Items[i]));
end;


function TMemStreamableList.Same(Other: TMemDBStreamable):boolean;
var
 O: TMemStreamableList;
 I: integer;
begin
  result := Assigned(Other) = Assigned(Self);
  if Assigned(Other) and Assigned(Self) then
    result := Other is Self.ClassType;
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

procedure TMemStreamableList.FreeAndClear;
var
  i: integer;
begin
  for i := 0 to Pred(Count) do
    Items[i].Free;
  Clear;
end;

procedure TMemStreamableList.RemoveDeleteSentinels;
var
  i: integer;
  T: TObject;
begin
  for i := 0 to Pred(Count) do
  begin
    T := TObject(Items[i]);
    Assert(Assigned(T));
    if T is TMemDeleteSentinel then
    begin
      T.Free;
      Items[i] := nil;
    end;
  end;
  Pack;
end;

{ TMemFieldDef }

procedure TMemFieldDef.ToStreamV2(Stream: TStream);
begin
  WrTag(Stream, mstFieldDefStartV2);
  Stream.Write(FFieldType, sizeof(FFieldType));
  WrStreamString(Stream, FFieldName);
  Stream.Write(FFieldIndex, sizeof(FFieldIndex));
  WrTag(Stream, mstFieldDefEnd);
end;

procedure TMemFieldDef.FromStreamV2(Stream: TStream);
var
  Tag: TMemStreamTag;
  OldFieldIndex: TOldFieldOffset;
begin
  Tag := RdTag(Stream);
  if not (Tag in [mstFieldDefStartV2, DEPRECATED_mstFieldDefStart_V1]) then
    raise EMemDBException.Create(S_WRONG_TAG);

  Stream.Read(FFieldType, sizeof(FFieldType));
  if not ((FFieldType >= Low(FFieldType))  and (FFieldType <= High(FFieldType))) then
    raise EMemDBException.Create(S_MEM_DB_UNSTREAM_OOR);
  FFieldName := RdStreamString(Stream);

  if Tag = DEPRECATED_mstFieldDefStart_V1 then
  begin
    Stream.Read(OldFieldIndex, sizeof(OldFieldIndex));
    FFieldIndex := OldFieldIndex;
  end
  else
    Stream.Read(FFieldIndex, sizeof(FFieldIndex));

  ExpectTag(Stream, mstFieldDefEnd);
end;

procedure TMemFieldDef.CheckSameAsStreamV2(Stream: TStream);
var
  StreamFieldType: TMDBFieldType;
  StreamFieldName: string;
  StreamFieldIndex: TFieldOffset;
  StreamOldFieldIndex: TOldFieldOffset;
  Tag: TMemStreamTag;
begin
  Tag := RdTag(Stream);
  if not (Tag in [mstFieldDefStartV2, DEPRECATED_mstFieldDefStart_V1]) then
    raise EMemDBException.Create(S_WRONG_TAG);

  Stream.Read(StreamFieldType, sizeof(StreamFieldType));
  if StreamFieldType <> FFieldType then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
  StreamFieldName := RdStreamString(Stream);
  if CompareStr(StreamFieldName, FieldName) <> 0 then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);

  if Tag = DEPRECATED_mstFieldDefStart_V1 then
  begin
    Stream.Read(StreamOldFieldIndex, sizeof(StreamOldFieldIndex));
    StreamFieldIndex := StreamOldFieldIndex;
  end
  else
    Stream.Read(StreamFieldIndex, sizeof(StreamFieldIndex));

  if StreamFieldIndex <> FFieldIndex then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);

  ExpectTag(Stream, mstFieldDefEnd);
end;

procedure TMemFieldDef.Assign(Source: TMemDBStreamable);
var
  S: TMemFieldDef;
begin
  inherited;
  S := Source as TMemFieldDef;
  FFieldType := S.FFieldType;
  FFieldName := S.FFieldName;
  FFieldIndex := S.FFieldIndex;
end;

function TMemFieldDef.Same(Other: TMemDBStreamable): boolean;
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

procedure TMemIndexDef.ToStreamV2(Stream: TStream);
begin
  WrTag(Stream, mstIndexDefStartV2);
  WrStreamString(Stream, FIndexName);
  WrStreamString(Stream, FFieldName);
  Stream.Write(FFieldIndex, sizeof(FFieldIndex));
  Stream.Write(FIndexAttrs, sizeof(FIndexAttrs));
  WrTag(Stream, mstIndexDefEnd);
end;

procedure TMemIndexDef.FromStreamV2(Stream: TStream);
var
  CurPos: Int64;
  DepTag, InitialTag: TMemStreamTag;
  DEPRECATED_Deconflict: byte;
  OldFieldIndex: TOldFieldOffset;
begin
  InitialTag := RdTag(Stream);
  if not (InitialTag in [mstIndexDefStartV2, DEPRECATED_mstIndexDefStart_V1]) then
    raise EMemDBException.Create(S_WRONG_TAG);

  FIndexName := RdStreamString(Stream);
  FFieldName := RdStreamString(Stream);

  if InitialTag = DEPRECATED_mstIndexDefStart_V1 then
  begin
    Stream.Read(OldFieldIndex, sizeof(OldFieldIndex));
    FFieldIndex := OldFieldIndex;
  end
  else
    Stream.Read(FFieldIndex, sizeof(FFieldIndex));

  Stream.Read(FIndexAttrs, sizeof(FIndexAttrs));
  if FIndexAttrs - AllIndexAttrs <> [] then
    raise EMemDBException.Create(S_MEM_DB_UNSTREAM_OOR);
  //Lookahead - field deconflict currently optional.
  //Remove deconflict from stream rd/wr in a while.
  CurPos := Stream.Position;
  DepTag := RdTag(Stream);
  if not (DepTag in [mstIndexDefEnd, DEPRECATED_mstIndexFieldDeconflict]) then
    raise EMemDBException.Create(S_INDEX_DEF_LOOKAHEAD_FAILED + IntToStr(Ord(DepTag)));

  if DepTag = DEPRECATED_mstIndexFieldDeconflict then
    Stream.Read(DEPRECATED_Deconflict, sizeof(DEPRECATED_Deconflict))
  else
    Stream.Seek(CurPos, soFromBeginning);

    ExpectTag(Stream, mstIndexDefEnd);
end;

procedure TMemIndexDef.CheckSameAsStreamV2(Stream: TStream);
var
  LclIndexName: string;
  LclFieldName: string;
  OldLclFieldIndex: TOldFieldOffset;
  LclFieldIndex: TFieldOffset;
  LclIndexAttrs: TMDBIndexAttrs;
  Tag: TMemStreamTag;
  CurPos: Int64;
  DEPRECATED_Deconflict: byte;
  InitialTag: TMemStreamTag;
begin
  InitialTag := RdTag(Stream);
  if not (InitialTag in [mstIndexDefStartV2, DEPRECATED_mstIndexDefStart_V1]) then
    raise EMemDBException.Create(S_WRONG_TAG);

  LclIndexName := RdStreamString(Stream);
  if CompareStr(FIndexName, LclIndexName) <> 0 then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
  LclFieldName := RdStreamString(Stream);
  if CompareStr(FFieldName, LclFieldName) <> 0 then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);

  if InitialTag = DEPRECATED_mstIndexDefStart_V1 then
  begin
    Stream.Read(OldLclFieldIndex, sizeof(OldLclFieldIndex));
    LclFieldIndex := OldLclFieldIndex;
  end
  else
  Stream.Read(LclFieldIndex, sizeof(LclFieldIndex));

  if LclFieldIndex <> FFieldIndex then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
  Stream.Read(LclIndexAttrs, sizeof(LclIndexAttrs));
  if LclIndexAttrs <> FIndexAttrs then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
  //Lookahead - field deconflict currently optional.
  CurPos := Stream.Position;
  Tag := RdTag(Stream);
  if not (Tag in [mstIndexDefEnd, DEPRECATED_mstIndexFieldDeconflict]) then
    raise EMemDBException.Create(S_INDEX_DEF_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
  if Tag = DEPRECATED_mstIndexFieldDeconflict then
    Stream.Read(DEPRECATED_Deconflict, sizeof(DEPRECATED_Deconflict))
  else
    Stream.Seek(CurPos, soFromBeginning);
  ExpectTag(Stream, mstIndexDefEnd);
end;

procedure TMemIndexDef.Assign(Source: TMemDBStreamable);
var
  S: TMemIndexDef;
begin
  inherited;
  S := Source as TMemIndexDef;
  FIndexName := S.FIndexName;
  FFieldName := S.FFieldName;
  FFieldIndex := S.FFieldIndex;
  FIndexAttrs := S.FIndexAttrs;
end;

function TMemIndexDef.Same(Other: TMemDBStreamable): boolean;
var
  O: TMemIndexDef;
begin
  result := inherited;
  if result then
  begin
    O := Other as TMemIndexDef;
    result :=
      (FIndexName = O.FIndexName) and
      (FFieldName = O.FFieldName) and
      (FFieldIndex = O.FFieldIndex) and
      (FIndexAttrs = O.FIndexAttrs);
  end;
end;

{ TMemFieldData }

procedure TMemFieldData.ToStreamV2(Stream: TStream);
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


procedure TMemFieldData.FromStreamV2(Stream: TStream);
begin
  RecFromStream(Stream, FDataRec);
end;

procedure TMemFieldData.CheckSameAsStreamV2(Stream: TStream);
var
  LocalRec: TMemDbFieldDataRec;
  OK: boolean;
begin
  RecFromStream(Stream, LocalRec);
  OK := DataRecsSame(FDataRec, LocalRec);
  if (LocalRec.FieldType = ftBlob)
    and Assigned(LocalRec.Data) then
    FreeMem(LocalRec.Data);
  if not OK then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
end;

procedure TMemFieldData.Assign(Source: TMemDBStreamable);
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
  end;
  FDataRec := S.FDataRec;
  if FDataRec.FieldType = ftBlob then
  begin
    Assert(Assigned(FDataRec.Data) = (FDataRec.size > 0));
    if Assigned(FDataRec.Data) then
    begin
      if FDataRec.size > High(Integer) then
        raise EMemDBInternalException.Create(S_BLOB_TOO_LARGE);
      GetMem(FDataRec.Data, Integer(FDataRec.size));
      Move(S.FDataRec.Data, FDataRec.Data, FDataRec.size);
    end;
  end;
end;

function TMemFieldData.Same(Other: TMemDBStreamable): boolean;
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

procedure TMemEntityMetadataItem.Assign(Source: TMemDBStreamable);
var
  S: TMemEntityMetadataItem;
begin
  inherited;
  S:= Source as TMemEntityMetadataItem;
  FEntityName := S.FEntityName;
end;

function TMemEntityMetadataItem.Same(Other: TMemDBStreamable):boolean;
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

procedure TMemEntityMetadataItem.ToStreamV2(Stream: TStream);
begin
  WrTag(Stream, mstEntityMetadataStart);
  WrStreamString(Stream, FEntityName);
  WrTag(Stream, mstEntityMetadataEnd);
end;

procedure TMemEntityMetadataItem.FromStreamV2(Stream: TStream);
begin
  ExpectTag(Stream, mstEntityMetadataStart);
  FEntityName := RdStreamString(Stream);
  ExpectTag(Stream, mstEntityMetadataEnd);
end;

procedure TMemEntityMetadataItem.CheckSameAsStreamV2(Stream: TStream);
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

procedure TMemTableMetadataItem.DeepAssign(Source: TMemDBStreamable);
var
  S: TMemTableMetadataItem;
begin
  inherited;
  S := Source as TMemTableMetadataItem;
  FFieldDefs.Free;
  FIndexDefs.Free;
  FFieldDefs := DeepClone(S.FFieldDefs) as TMemStreamableList;
  FIndexDefs := DeepClone(S.FIndexDefs) as TMemStreamableList;
end;

function TMemTableMetadataItem.Same(Other: TMemDBStreamable):boolean;
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
  FFieldDefs.Free;
  FIndexDefs.Free;
  inherited;
end;

procedure TMemTableMetadataItem.ToStreamV2(Stream: TStream);
begin
  WrTag(Stream, mstTableMetadataStart);
  inherited;
  FFieldDefs.ToStreamV2(Stream);
  FIndexDefs.ToStreamV2(Stream);
  WrTag(Stream, mstTableMetadataEnd);
end;

procedure TMemTableMetadataItem.FromStreamV2(Stream: TStream);
begin
  ExpectTag(Stream, mstTableMetadataStart);
  inherited;
  FFieldDefs.FromStreamV2(Stream);
  FIndexDefs.FromStreamV2(Stream);
  ExpectTag(Stream, mstTableMetadataEnd);
end;

procedure TMemTableMetadataItem.CheckSameAsStreamV2(Stream: TStream);
begin
  ExpectTag(Stream, mstTableMetadataStart);
  inherited;
  FFieldDefs.CheckSameAsStreamV2(Stream);
  FIndexDefs.CheckSameAsStreamV2(Stream);
  ExpectTag(Stream, mstTableMetadataEnd);
end;

{ TMemForeignKeyMetadataItem }

procedure TMemForeignKeyMetadataItem.Assign(Source: TMemDBStreamable);
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

function TMemForeignKeyMetadataItem.Same(Other: TMemDBStreamable):boolean;
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

procedure TMemForeignKeyMetadataItem.ToStreamV2(Stream: TStream);
begin
  WrTag(Stream, mstFKMetadataStart);
  inherited;
  WrStreamString(Stream, FTableReferer);
  WrStreamString(Stream, FIndexReferer);
  WrStreamString(Stream, FTableReferred);
  WrStreamString(Stream, FIndexReferred);
  WrTag(Stream, mstFKMetadataEnd);
end;

procedure TMemForeignKeyMetadataItem.FromStreamV2(Stream: TStream);
begin
  ExpectTag(Stream, mstFKMetadataStart);
  inherited;
  FTableReferer := RdStreamString(Stream);
  FIndexReferer := RdStreamString(Stream);
  FTableReferred := RdStreamString(Stream);
  FIndexReferred := RdStreamString(Stream);
  ExpectTag(Stream, mstFKMetadataEnd);
end;

procedure TMemForeignKeyMetadataItem.CheckSameAsStreamV2(Stream: TStream);
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

end.
