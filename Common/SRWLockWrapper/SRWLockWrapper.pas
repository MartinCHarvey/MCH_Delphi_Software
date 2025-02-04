unit SRWLockWrapper;

{

Copyright © 2025 Martin Harvey <martin_c_harvey@hotmail.com>

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

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  Winapi.Windows;

(*
typedef struct _RTL_SRWLOCK {
        PVOID Ptr;
} RTL_SRWLOCK, *PRTL_SRWLOCK;
*)

type
  RTL_SRWLOCK = record
    Ptr: pointer;
  end;

//typedef RTL_SRWLOCK SRWLOCK, *PSRWLOCK;
  SRWLOCK = RTL_SRWLOCK;
  PSRWLOCK = ^SRWLOCK;

  { N.B. Note that this SRWLock is *not* recursive.
    It also doesn't allow promotion of readers to writers. }
{$IFDEF USE_TRACKABLES}
  TSRWLock = class(TTrackable)
{$ELSE}
  TSRWLock = class
{$ENDIF}
  private
    FLock: RTL_SRWLOCK;
  public
    constructor Create;
    //No destructor needed.
    procedure ReleaseExclusive;
    procedure ReleaseShared;
    procedure AcquireExclusive;
    procedure AcquireShared;
    function TryAcquireExclusive: boolean;
    function TryAcquireShared: boolean;

    //And now some aliases to make it a drop in replacement for Delphi MRSW lock.
    { IReaderWriterLock }
    procedure BeginRead; inline;
    procedure EndRead; inline;
    procedure BeginWrite; inline;
    procedure EndWrite; inline;
  end;

implementation

{ Linking functions }

(*
WINBASEAPI
VOID
WINAPI
InitializeSRWLock(
    _Out_ PSRWLOCK SRWLock
    );
*)
procedure WINBASE_InitializeSRWLock(pLock: PSRWLOCK);
stdcall; external Winapi.Windows.Kernel32 name 'InitializeSRWLock';

(*
WINBASEAPI
_Releases_exclusive_lock_(*SRWLock)
VOID
WINAPI
ReleaseSRWLockExclusive(
    _Inout_ PSRWLOCK SRWLock
    );
*)

procedure WINBASE_ReleaseSRWLockExclusive(pLock: PSRWLOCK);
stdcall; external Winapi.Windows.Kernel32 name 'ReleaseSRWLockExclusive';

(*
WINBASEAPI
_Releases_shared_lock_(*SRWLock)
VOID
WINAPI
ReleaseSRWLockShared(
    _Inout_ PSRWLOCK SRWLock
    );

*)

procedure WINBASE_ReleaseSRWLockShared(pLock: PSRWLOCK);
stdcall; external Winapi.Windows.Kernel32 name 'ReleaseSRWLockShared';

(*
WINBASEAPI
_Acquires_exclusive_lock_(*SRWLock)
VOID
WINAPI
AcquireSRWLockExclusive(
    _Inout_ PSRWLOCK SRWLock
    );

*)

procedure WINBASE_AcquireSRWLockExclusive(pLock: PSRWLOCK);
stdcall; external WinApi.Windows.Kernel32 name 'AcquireSRWLockExclusive';

(*
WINBASEAPI
_Acquires_shared_lock_(*SRWLock)
VOID
WINAPI
AcquireSRWLockShared(
    _Inout_ PSRWLOCK SRWLock
    );
*)

procedure WINBASE_AcquireSRWLockShared(pLock: PSRWLock);
stdcall; external WinApi.Windows.Kernel32 name 'AcquireSRWLockShared';

(*
WINBASEAPI
_When_(return!=0, _Acquires_exclusive_lock_(*SRWLock))
BOOLEAN
WINAPI
TryAcquireSRWLockExclusive(
    _Inout_ PSRWLOCK SRWLock
    );
*)

function WINBASE_TryAcquireSRWLockExclusive(pLock: PSRWLock):boolean;
stdcall; external WinApi.Windows.Kernel32 name 'TryAcquireSRWLockExclusive';

(*
WINBASEAPI
_When_(return!=0, _Acquires_shared_lock_(*SRWLock))
BOOLEAN
WINAPI
TryAcquireSRWLockShared(
    _Inout_ PSRWLOCK SRWLock
    );
*)

function WINBASE_TryAcquireSRWLockShared(pLock: PSRWLock): boolean;
stdcall; external WinApi.Windows.Kernel32 name 'TryAcquireSRWLockShared';

{ TSRWLock }

constructor TSRWLock.Create;
begin
  inherited;
  WINBASE_InitializeSRWLock(@FLock);
end;

procedure TSRWLock.ReleaseExclusive;
begin
  WINBASE_ReleaseSRWLockExclusive(@FLock);
end;

procedure TSRWLock.ReleaseShared;
begin
  WINBASE_ReleaseSRWLockShared(@FLock);
end;

procedure TSRWLock.AcquireExclusive;
begin
  WINBASE_AcquireSRWLockExclusive(@FLock);
end;

procedure TSRWLock.AcquireShared;
begin
  WINBASE_AcquireSRWLockShared(@FLock);
end;

function TSRWLock.TryAcquireExclusive: boolean;
begin
  result := WINBASE_TryAcquireSRWLockExclusive(@FLock);
end;

function TSRWLock.TryAcquireShared: boolean;
begin
  result := WINBASE_TryAcquireSRWLockShared(@FLock);
end;

procedure TSRWLock.BeginRead;
begin
  AcquireShared;
end;

procedure TSRWLock.EndRead;
begin
  ReleaseShared;
end;

procedure TSRWLock.BeginWrite;
begin
  AcquireExclusive;
end;

procedure TSRWLock.EndWrite;
begin
  ReleaseExclusive;
end;

end.
