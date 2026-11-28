unit CRC32;
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
  - Amassed and ported from a various different C sources,
    and slightly munged to be the way I want it to be :-)

}

interface


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
  if Size > 0 then
  begin
    BufPtr := @Buf;
    for DataOfs := 0 to Pred(Size) do //Longword - Pred(Size) wraps.
    begin
      ByteVal := BufPtr^;
      AccValue := CRC shr 8;
      Index := Crc and $FF;
      Index := Index xor ByteVal;
      Q := Crc32Table[Index];
      CRC := AccValue xor Q;
      Inc(BufPtr);
    end;
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
