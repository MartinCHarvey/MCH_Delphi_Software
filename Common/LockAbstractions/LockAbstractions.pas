unit LockAbstractions;

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

{$IFDEF USE_WINDOWS_LOCKS}
uses Windows, SRWLockWrapper, SyncObjs;
{$ELSE}
uses SyncObjs, SysUtils;
{$ENDIF}

type
{$IFDEF USE_WINDOWS_LOCKS}
  TMRSWLock = SRWLockWrapper.TSRWLock;
{$ELSE}
  TMRSWLock = SysUtils.TMultiReadExclusiveWriteSynchronizer;
{$ENDIF}
  TCriticalSection = SyncObjs.TCriticalSection;
  TEvent = SyncObjs.TEvent;

function InterlockedCompareExchange(var Destination: Integer; Exchange: Integer; Comparand: Integer): Integer;
function InterlockedCompareExchange64(var Destination: Int64; Exchange: Int64; Comparand: Int64): Int64;
function InterlockedCompareExchangePointer(var Destination: Pointer; Exchange: Pointer; Comparand: Pointer): Pointer;
function InterlockedDecrement(var Addend: Integer): Integer;
function InterlockedExchange(var Target: Integer; Value: Integer): Integer;
{$IFDEF USE_WINDOWS_LOCKS}
{$IFDEF WIN32}
function InterlockedExchangeAdd(Addend: PInteger; Value: Integer): Integer; overload;
{$ENDIF}
function InterlockedExchangeAdd(var Addend: Integer; Value: Integer): Integer; overload;
{$ENDIF}
function InterlockedExchangePointer(var Target: Pointer; Value: Pointer): Pointer;
function InterlockedIncrement(var Addend: Integer): Integer;

implementation

function InterlockedCompareExchange(var Destination: Integer; Exchange: Integer; Comparand: Integer): Integer;
begin
{$IFDEF USE_WINDOWS_LOCKS}
  result := Windows.InterlockedCompareExchange(Destination, Exchange, Comparand);
{$ELSE}
  result := SyncObjs.TInterlocked.CompareExchange(Destination, Exchange, Comparand);
{$ENDIF}
end;

function InterlockedCompareExchange64(var Destination: Int64; Exchange: Int64; Comparand: Int64): Int64;
begin
{$IFDEF USE_WINDOWS_LOCKS}
  result := Windows.InterlockedCompareExchange64(Destination, Exchange, Comparand);
{$ELSE}
  result := SyncObjs.TInterlocked.CompareExchange(Destination, Exchange, Comparand);
{$ENDIF}
end;

function InterlockedCompareExchangePointer(var Destination: Pointer; Exchange: Pointer; Comparand: Pointer): Pointer;
begin
{$IFDEF USE_WINDOWS_LOCKS}
  result := Windows.InterlockedCompareExchangePointer(Destination, Exchange, Comparand);
{$ELSE}
  result := SyncObjs.TInterlocked.CompareExchange(Destination, Exchange, Comparand);
{$ENDIF}
end;

function InterlockedDecrement(var Addend: Integer): Integer;
begin
{$IFDEF USE_WINDOWS_LOCKS}
  result := Windows.InterlockedDecrement(Addend);
{$ELSE}
  result := SyncObjs.TInterlocked.Decrement(Addend);
{$ENDIF}
end;

function InterlockedExchange(var Target: Integer; Value: Integer): Integer;
begin
{$IFDEF USE_WINDOWS_LOCKS}
  result := Windows.InterlockedExchange(Target, Value);
{$ELSE}
  result := SyncObjs.TInterlocked.Exchange(Target, Value);
{$ENDIF}
end;

{$IFDEF USE_WINDOWS_LOCKS}
{$IFDEF WIN32}
function InterlockedExchangeAdd(Addend: PInteger; Value: Integer): Integer; overload;
begin
  result := Windows.InterlockedExchangeAdd(Addend, Value);
end;
{$ENDIF}

function InterlockedExchangeAdd(var Addend: Integer; Value: Integer): Integer; overload;
begin
  result := Windows.InterlockedExchangeAdd(Addend, Value);
end;
{$ENDIF}

function InterlockedExchangePointer(var Target: Pointer; Value: Pointer): Pointer;
begin
{$IFDEF USE_WINDOWS_LOCKS}
  result := Windows.InterlockedExchangePointer(Target, Value);
{$ELSE}
  result := SyncObjs.TInterlocked.Exchange(Target, Value);
{$ENDIF}
end;

function InterlockedIncrement(var Addend: Integer): Integer;
begin
{$IFDEF USE_WINDOWS_LOCKS}
  result := Windows.InterlockedIncrement(Addend);
{$ELSE}
  result := SyncObjs.TInterlocked.Increment(Addend);
{$ENDIF}
end;

end.
