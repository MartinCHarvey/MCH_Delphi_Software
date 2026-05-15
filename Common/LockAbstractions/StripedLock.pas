unit StripedLock;
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
interface

uses
  CRC32,
{$IFOPT C+}
  Trackables,
{$ENDIF}
  SyncObjs;

type
{$IFOPT C+}
  TStripedLock = class(TTrackable)
{$ELSE}
  TStripedLock = class
{$ENDIF}
  private
    FKey: Word;
{$IFOPT C+}
    FInit: boolean;
{$ENDIF}
  public
    procedure Init(const Buf; Size: integer);
    procedure Acquire;
    procedure Release;
  end;

implementation

uses
  Windows;

const
  LOCK_ARRAY_SIZE = Succ(High(Word));
  A_LONGER_SPIN = 12000;

type
  TSpinModCriticalSection = class(TCriticalSection)
  public
    procedure SetSpinCount(Count: cardinal);
  end;

type
  LockArray = array [0..Pred(LOCK_ARRAY_SIZE)] of TSPinModCriticalSection;

var
  Locks: LockArray;

{ TStripedLock }

procedure TSPinModCriticalSection.SetSpinCount(Count: cardinal);
var
  Ret: DWord;
begin
  Ret := SetCriticalSectionSpinCount(FSection, Count);
end;

{ TStripedLock }

procedure TStripedLock.Init(const Buf; Size: integer);
var
  ChkSum: longword;
begin
  ChkSum := StartCRC32;
  ChkSum := UpdateCRC32(Buf, Size, ChkSum);
  ChkSum := FinishCRC32(ChkSum);
  Assert(Sizeof(ChkSum) = 2 * SizeOf(FKey));
  FKey := ((ChkSum and $FFFF0000) shr 16) xor (ChkSum and $FFFF);
{$IFOPT C+}
  FInit := true;
{$ENDIF}
end;

procedure TStripedLock.Acquire;
begin
{$IFOPT C+}
  Assert(FInit);
{$ENDIF}
  Locks[FKey].Acquire;
end;

procedure TStripedLock.Release;
begin
{$IFOPT C+}
  Assert(FInit);
{$ENDIF}
  Locks[FKey].Release;
end;

{ Misc init }

procedure InitLocks;
var
  i: Integer;
begin
  for i := Low(LockArray) to High(LockArray) do
  begin
    Locks[i] := TSpinModCriticalSection.Create;
    Locks[i].SetSpinCount(A_LONGER_SPIN);
  end;
end;

procedure FiniLocks;
var
  i: Integer;
begin
  for i := Low(LockArray) to High(LockArray) do
    Locks[i].Free;
end;

initialization
  InitLocks;
finalization
  FiniLocks;
end.

