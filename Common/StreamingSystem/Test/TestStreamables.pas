unit TestStreamables;
{
  Copyright (c) Martin Harvey 2005.

  Test streamable classes.
}

interface

uses SSStreamables, StreamingSystem;

type
  TMyShortStr = string[20];

  TTestStreamable = class(TObjStreamable)
  private
    FTSmall: smallint;
    FTShort: shortint;
    FTByte: byte;
    FTWord: word;
    FTInt: integer;
    FTSStr: TMyShortStr;
    FTAStr: AnsiString;
    FTLStr: string;
    FTUStr: UnicodeString;
    FTBool: boolean;
    FTCard: cardinal;
    FTI64: Int64;
    FTU64: Uint64;
    FTI64Hi, FTI64Lo: Int64;
    FTU64Hi, FTU64Lo: UInt64;
  public
    function Compare(Other: TTestStreamable): boolean; virtual;
    procedure SetupTestData; virtual;
  published
    property TInt: integer read FTInt write FTInt;
    property TSmall: smallint read FTSmall write FTSmall;
    property TShort: shortint read FTShort write FTShort;
    property TByte: byte read FTByte write FTByte;
    property TWord: word read FTWord write FTWord;
    property TSStr: TMyShortStr read FTSStr write FTSStr;
    property TAStr: AnsiString read FTAStr write FTAStr;
    property TLStr: string read FTLStr write FTLStr;
    property TUStr: UnicodeString read FTUStr write FTUStr;
    property TBool: boolean read FTBool write FTBool;
    property TCard: cardinal read FTCard write FTCard;
    property TI64: Int64 read FTI64 write FTI64;
    property TU64: Uint64 read FTU64 write FTU64;
    property I64Hi: Int64 read FTI64Hi write FTI64Hi;
    property I64Lo: Int64 read FTI64Lo write FTI64Lo;
    property U64Hi: UInt64 read FTU64Hi write FTU64Hi;
    property U64Lo: Uint64 read FTU64Lo write FTU64Lo;
  end;

  TTestRec = record
    RecInt: integer;
  end;

  TTestStreamable3 = class(TObjStreamable)
  end;

  TArrayIdx = 0..19;

  TTestArray = array[TArrayIdx] of integer;

  TTestStreamable2 = class(TTestStreamable)
  private
    FTS1: TTestStreamable;
    FTS2: TTestStreamable3;
    FBool2: boolean;
    FTestArray: TTestArray;
    FTBlob: pointer;
    FTBlobSize: UInt64;
  protected
    procedure CustomMarshal(Sender: TDefaultSSController); override;
    procedure CustomUnmarshal(Sender: TDefaultSSController); override;
  public
    TestRec: TTestRec;
    procedure SetupTestData; override;
    function Compare(Other: TTestStreamable): boolean; override;
    constructor Create; override;
    destructor Destroy; override;
    property Blob: Pointer read FTBlob write FTBlob;
  published
    property TS1: TTestStreamable read FTS1 write FTS1;
    property TS2: TTestStreamable3 read FTS2 write FTS2;
    property Bool2: boolean read FBool2 write FBool2;
    property BlobSize: UInt64 read FTBlobSize write FTBlobSize;
  end;

function GetTestHeirarchy: THeirarchyInfo;

implementation

uses
  Classes, SysUtils;

function GetTestHeirarchy: THeirarchyInfo;
begin
  result := TObjDefaultHeirarchy;
  SetLength(result.MemberClasses, 3);
  result.MemberClasses[0] := TTestStreamable;
  result.MemberClasses[1] := TTestStreamable2;
  result.MemberClasses[2] := TTestStreamable3;
end;

(************************************
 * TTestStreamable                  *
 ************************************)

function TTestStreamable.Compare(Other: TTestStreamable): boolean;
begin
  result := Assigned(Self) = Assigned(Other);
  result := result and (Self.Tint = Other.Tint);
  result := result and (Self.tsmall = Other.tsmall);
  result := result and (Self.tshort = Other.tshort);
  result := result and (Self.tbyte = Other.tbyte);
  result := result and (Self.tword = Other.tword);
  result := result and (Self.tsstr = Other.tsstr);
  result := result and (Self.tlstr = Other.tlstr);
  result := result and (Self.TUStr = Other.TUStr);
  result := result and (Self.tbool = Other.tbool);
  result := result and (Self.tcard = Other.tcard);
  result := result and (Self.TI64 = Other.TI64);
  result := result and (Self.TU64 = Other.TU64);
  result := result and (Self.I64Hi = Other.I64Hi);
  result := result and (Self.I64Lo = Other.I64Lo);
  result := result and (Self.U64Hi = Other.U64Hi);
  result := result and (Self.U64Lo = Other.U64Lo);
end;

procedure TTestStreamable.SetupTestData;
begin
  FTint := Random(High(Integer));
  if Random(1) <> 0 then
    FTInt := -FTInt;
  FTSmall := Random(High(smallint));
  FTShort := Random(High(shortint));
  FTByte := Random(High(byte));
  FTWord := Random(High(word));
  FTSStr := 'A test short string';
  FTLStr := 'Longstr '''' ""  & a bit more <tag> </tag>';
  FTUStr := 'And a really unicode Norwegian: жше. French: кий string.';
  FTUStr := FTUStr + #345;
  FTUstr := FTUStr + #3677;
  FTBool := Random(Ord(High(boolean))) <> 0;
  FTCard := Random(High(Integer));
  FTI64 := Random(High(Integer));
  FTI64 := (FTI64 shl 32) + Random(High(Integer));
  if Random(1) <> 0 then
    FTI64 := -FTI64;
  FTU64 := Random(High(Integer));
  FTU64 := (FTU64 shl 32) + Random(High(Integer));
  FTI64Hi := High(Int64);
  FTI64Lo := Low(Int64);
  FTU64Hi := High(Uint64);
  FTU64Lo := Low(Uint64);
end;

(************************************
 * TTestStreamable2                 *
 ************************************)

procedure TTestStreamable2.SetupTestData;
var
  Idx: TArrayIdx;
begin
  FBool2 := Random(Ord(High(boolean))) <> 0;
  TestRec.RecInt := Random(High(integer));
  for Idx := Low(TArrayIdx) to High(TArrayIdx) do
    FTestArray[Idx] := Random(High(integer));
  inherited;
end;

function TTestStreamable2.Compare(Other: TTestStreamable): boolean;
var
  Idx: TArrayIdx;
  Other2: TTestStreamable2;
begin
  result := inherited Compare(Other);
  result := result and (Other.ClassType = Self.ClassType);
  if not result then exit;
  Other2 := TTestStreamable2(Other);
  result := result and (Assigned(FTS1) = Assigned(Other2.FTS1));
  if Assigned(FTS1) then
    result := result and FTS1.Compare(Other2.FTS1);
  result := result and FBool2 = Other2.FBool2;
  result := result and (TestRec.RecInt = Other2.TestRec.RecInt);
  for Idx := Low(Idx) to High(Idx) do
    result := result and (FTestArray[Idx] = Other2.FTestArray[Idx]);
  result := result and (FTBlobSize = Other2.FTBlobSize);
  if FTBlobSize > 0 then
    result := result and CompareMem(FTBlob, Other2.FTBlob, FTBlobSize);
end;

procedure TTestStreamable2.CustomMarshal(Sender: TDefaultSSController);
var
  Idx: TArrayIdx;
begin
  Sender.StreamRecordStart('TestRec');
  Sender.StreamSLong('RecInt', TestRec.RecInt);
  Sender.StreamRecordEnd('TestRec');
  Sender.StreamArrayStart('TestArray');
  for Idx := Low(Idx) to High(Idx) do
    Sender.StreamSLong('', FTestArray[Idx]);
  Sender.StreamArrayEnd('TestArray');
  Assert(Assigned(FTBlob) = (FtBlobSize > 0));
  Assert(Assigned(FTBlob) = (FTBlobSize > 0));
  if Assigned(FTBlob) then
  begin
    Sender.StreamBlob('Blob', FTBlobSize, FTBlob);
  end;
  inherited;
end;

procedure TTestStreamable2.CustomUnmarshal(Sender: TDefaultSSController);
var
  TempInt: integer;
  Idx: TArrayIdx;
  BlobStream: TStream;
begin
  if Sender.UnStreamRecordStart('TestRec') then
  begin
    Sender.UnStreamSLong('RecInt', TestRec.RecInt);
    Sender.UnStreamRecordEnd('TestRec');
  end;
  if Sender.UnstreamArrayStart('TestArray', TempInt) then
  begin
    if Pred(TempInt) = High(Idx) then
    begin
      for Idx := Low(Idx) to High(Idx) do
        Sender.UnstreamSLong('', FTestArray[Idx]);
    end;
    Sender.UnstreamArrayEnd('TestArray');
  end;
  if FTBlobSize > 0 then
  begin
    if Sender.UnstreamBlob('Blob', FTBlobSize, BlobStream) then
    begin
      try
        if Assigned(FTBlob) then
        begin
          FreeMem(FTBlob);
          FTBlob := GetMemory(FtBlobSize);
          BlobStream.Seek(0, soFromBeginning);
          BlobStream.ReadData(FTBlob, FTBlobSize);
        end;
      finally
        BlobStream.Free;
      end;
    end;
  end;
  Assert(Assigned(FTBlob) = (FtBlobSize > 0));
  inherited;
end;

constructor TTestStreamable2.Create;
var
  idx: integer;
  PB: PByte;
begin
  inherited;
  FTBlobSize := $10000;
  FTBlob := GetMemory(FTBlobSize);
  PB := PByte(FTBlob);
  for idx := 0 to Pred(FTBlobSize) do
  begin
    PB^ := Byte(Idx) xor $42;
    Inc(PB);
  end;
end;

destructor TTestStreamable2.Destroy;
begin
  FTS1.Free;
  FTS2.Free;
  FreeMemory(FTBlob);
  inherited;
end;


initialization
  Randomize;
end.

