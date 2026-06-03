unit PatchFreeMem;

{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

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

interface

{
  If you have to ask what this does, or what it is for, then you probably
  don't need it in your code.
}

function GoPatchFreeMem: boolean;

implementation

uses
  SysUtils
{$IFDEF MSWINDOWS}
  ,Windows
{$ENDIF}
  ;

{$IF Defined(VER250) and Defined(WIN64)}
  // XE4 64-bit only
function GoPatchFreeMem: boolean;

const
  SAMPLE_OFFSET = $4070A0;
  FREEMEM_LENGTH = $4072C0 - SAMPLE_OFFSET;

  START_CHUNK_OFFSET = $40713E - SAMPLE_OFFSET;
  END_CHUNK_OFFSET = $407152 - SAMPLE_OFFSET;
  CHUNK_SIZE = END_CHUNK_OFFSET - START_CHUNK_OFFSET;

{
00000000`0040713e f00fb023        lock cmpxchg byte ptr [rbx],ah
00000000`00407142 7480            je      MemDB2Test+0x70c4 (00000000`004070c4)
00000000`00407144 f390            pause
00000000`00407146 488d0568b86b00  lea     rax,[MemDB2Test+0x6c29b5 (00000000`00ac29b5)]
00000000`0040714d f60000          test    byte ptr [rax],0
00000000`00407150 75e7            jne     MemDB2Test+0x7139 (00000000`00407139)
}

  COMPARE_DATA: array [0..Pred(CHUNK_SIZE)] of byte
    = ($F0, $0F, $B0, $23, $74, $80, $F3, $90, $48, $8D, $05, $68, $B8, $6B, $00, $F6, $00, $00, $75, $E7);

{
00000000`0040713e f00fb023        lock cmpxchg byte ptr [rbx],ah
00000000`00407142 7480            je      MemDB2Test+0x70c4 (00000000`004070c4)
00000000`00407144 f390            pause
00000000`00407146 488d0568b86b00  lea     rax,[MemDB2Test+0x6c29b5 (00000000`00ac29b5)]
00000000`0040714d 803800          cmp    byte ptr [rax],0
00000000`00407150 75e7            jne     MemDB2Test+0x7139 (00000000`00407139)
}

  MODDED_DATA:  array [0..Pred(CHUNK_SIZE)] of byte
    = ($F0, $0F, $B0, $23, $74, $80, $F3, $90, $48, $8D, $05, $68, $B8, $6B, $00, $80, $38, $00, $75, $E7);

{
  lea     rax,[MemDB2Test+0x6c29b5 (00000000`00ac29b5)]

  Contains a relative address. We need to skip it, and copy it thru to the final patched
  code.
}

  LEA_RELADDR_CHUNK_OFFSET = 11;
  LEA_RELADDR_SIZE = 4; //4 byte relative offset.

type
  TChunkBuf = array[0.. Pred(CHUNK_SIZE)] of byte;

var
  FreeMemAddr, ChunkAddr: PByte;
  PPtrs: PPointer;
  MemMgr: TMemoryManager;
  OldProtect: DWORD;
  ChunkBuf: TChunkBuf;
  ProcHandle: THandle;
  BytesRW: NativeUInt;
  i: integer;
begin
  result := false;
  ProcHandle := GetCurrentProcess;
  GetMemoryManager(MemMgr);
  PPtrs := @MemMgr;
  Inc(PPtrs);
  FreeMemAddr := PPtrs^;
  if VirtualProtect(FreeMemAddr, FREEMEM_LENGTH, PAGE_EXECUTE_READWRITE , OldProtect) then
  begin
    try
      ChunkAddr := FreeMemAddr;
      Inc(ChunkAddr, START_CHUNK_OFFSET);
      //Read.
      if not ReadProcessMemory(ProcHandle, ChunkAddr, @ChunkBuf[0], CHUNK_SIZE, BytesRW) then
        exit;
      if BytesRW <> CHUNK_SIZE then
        exit;
      //Check, minus reladdr.
      for i := 0 to Pred(CHUNK_SIZE) do
      begin
        if (i < LEA_RELADDR_CHUNK_OFFSET) or (i >= LEA_RELADDR_CHUNK_OFFSET + LEA_RELADDR_SIZE) then
        begin
          if ChunkBuf[i] <> COMPARE_DATA[i] then
            exit;
        end;
      end;
      //OK. We have a good match.
      for i := 0 to Pred(CHUNK_SIZE) do
      begin
        //Replace minus reladdr.
        if (i < LEA_RELADDR_CHUNK_OFFSET) or (i >= LEA_RELADDR_CHUNK_OFFSET + LEA_RELADDR_SIZE) then
          ChunkBuf[i] := MODDED_DATA[i];
      end;
      if not WriteProcessMemory(ProcHandle, ChunkAddr, @ChunkBuf[0], CHUNK_SIZE, BytesRW) then
        exit;
      if BytesRW <> CHUNK_SIZE then
        exit;
      //Flush ICache.
      FlushInstructionCache(GetCurrentProcess(), FreeMemAddr, FREEMEM_LENGTH);
      result := true;
    finally
      //Restore page protections.
      VirtualProtect(FreeMemAddr, FREEMEM_LENGTH, OldProtect, OldProtect);
    end;
  end;
end;

{$ELSE}
function GoPatchFreeMem: boolean;
begin
  //Do nothing - not required for version or architecture.
  result := true;
end;
{$IFEND}

end.
