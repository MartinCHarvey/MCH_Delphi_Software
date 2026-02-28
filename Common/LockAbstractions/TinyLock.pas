unit TinyLock;

{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

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

type
  TTinyLock = type integer;

procedure AcquireTinyLock(var Lock: TTinyLock);
procedure ReleaseTinyLock(var Lock: TTinyLock);

implementation

uses LockAbstractions, SysUtils;

const
  SPIN_COUNT = 200;

procedure AcquireTinyLock(var Lock: TTinyLock);
var
  Tries: integer;
begin
  while True do
  begin
    Tries := SPIN_COUNT;
    while Tries > 0 do
    begin
      if LockAbstractions.InterlockedCompareExchange(Integer(Lock), 1, 0) = 0 then
        exit;
      Dec(Tries);
    end;
    Sleep(0); //Yield.
  end;
end;

procedure ReleaseTinyLock(var Lock: TTinyLock);
var
  PrevLocked: integer;
begin
  PrevLocked := LockAbstractions.InterlockedExchange(Integer(Lock), 0);
  Assert(PrevLocked = 1);
end;

end.
