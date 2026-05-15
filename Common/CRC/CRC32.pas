unit CRC32;

{
  Copyright Martin Harvey 30/11/2003

  - Amassed and ported from a various different C sources,
    and slightly munged to be the way I want it to be :-)

}

interface

uses Windows;

function StartCRC32: longword; inline;
function UpdateCrc32(const Buf; const Size: longword; const OldCRC: longword =
  0): longword;
function FinishCRC32(CRC: longword): longword; inline;

implementation

const
  TabSize = 256;

var
  Crc32Table: array[0..Pred(TabSize)] of longword;

procedure InitCrc32;
var
  Seed: longword;
  TabIndex: longword;
  BitNum: longword;
  CRC: longword;
  TempCRC: longword;
begin
  Seed := $EDB88320;
  for TabIndex := 0 to Pred(TabSize) do
  begin
    CRC := TabIndex;
    for BitNum := 0 to 7 do
    begin
      TempCrc := Crc shr 1;
      if (Crc and 1) <> 0 then
        Crc := TempCrc xor Seed
      else
        Crc := TempCrc;
    end;
    Crc32Table[TabIndex] := Crc;
  end;
end;

function UpdateCrc32(const Buf; const Size: longword; const OldCRC: longword =
  0): longword;
var
  BufPtr: PByte;
  DataOfs: longword;
  ByteVal: byte;
  CRC: longword;
  AccValue: longword;
  Index: longword;
  Q: longword;
begin
  CRC := OldCRC;
  BufPtr := @Buf;
  for DataOfs := 0 to Pred(Size) do
  begin
    ByteVal := BufPtr^;
    AccValue := CRC shr 8;
    Index := Crc and $FF;
    Index := Index xor ByteVal;
    Q := Crc32Table[Index];
    CRC := AccValue xor Q;
    Inc(BufPtr);
  end;
  Result := CRC;
end;

function StartCRC32: longword;
begin
  result := 0;
  result := not result;
end;

function FinishCRC32(CRC: longword): longword;
begin
  result := not CRC;
end;

initialization
  InitCRC32;
end.
