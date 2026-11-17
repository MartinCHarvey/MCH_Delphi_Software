unit LockedSList;
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

interface

{
  I need an interlocked SList implementation.
  Windows gives you via (Ex)Interlocked(*)EntrySList, but
  there is no Android RTL equivalent accessible from Delphi, so
  I am writing my own here.
}

uses
  LockAbstractions, SysUtils;

type
  PSLEntry = ^TSLEntry;
  TSLEntry = record
    FLink: PSLEntry;
    Owner: TObject;
  end;
  { Interlocked CmpExchPtr requires a pointer for its var parameter,
    hence the alias }
  PSLEntryAlias = ^TSLEntryAlias;
  TSLEntryAlias = record
    FLink: pointer;
    Owner: TObject;
  end;

  PSLListHead = ^TSLListHead;
  TSLListHead = record
    Entry:TSLEntry;
    PopLock: integer;
  end;

{ Basic init, push pop routines. Do not maintain a count at the moment }

procedure SLItemInitObj(Item: PSLEntry; Obj: TObject);
procedure SLItemInitList(Item: PSLListHead);

procedure SLListLockedPushItem(List: PSLListHead; Item:PSLEntry);
function SLListLockedPopItem(List: PSLListHead): PSLEntry;

function SLItemIsList(Item:PSLEntry): boolean;
function SlItemIsObj(Item:PSLEntry): boolean;

implementation

uses
  PComp;

procedure SLItemInitObj(Item: PSLEntry; Obj: TObject);
begin
  Assert(Assigned(Item));
  Assert(Assigned(Obj));
  Item.FLink := nil;
  Item.Owner := Obj;
end;

procedure SLItemInitList(Item: PSLListHead);
begin
  Assert(Assigned(Item));
  Item.Entry.FLink := nil;
  Item.Entry.Owner := nil;
  Item.PopLock := 0;
end;

procedure SLListLockedPushItem(List: PSLListHead; Item:PSLEntry);
var
  GoodExchange: boolean;
  LFL: PSLEntry;
begin
  Assert(Assigned(List));
  Assert(SLItemIsList(@List.Entry));
  Assert(Assigned(Item));
  Assert(SlItemIsObj(Item));
  Assert(not Assigned(Item.FLink));
  repeat
    LFL := List.Entry.FLink;
    Item.FLink := LFL;
    GoodExchange := LockAbstractions.InterlockedCompareExchangePointer
      (PSLEntryAlias(@List.Entry).FLink, Item, LFL) = LFL;
  until GoodExchange;
end;


function SLListLockedPopItem(List: PSLListHead): PSLEntry;
var
  Next: PSLEntry;
  GoodExchange: boolean;
begin
  Assert(Assigned(List));
  Assert(SLItemIsList(@List.Entry));
  repeat
    //Only take cache line exclusively if it looks like we're going to
    //succeed.
    GoodExchange := (List.PopLock = 0) and
      (LockAbstractions.InterlockedCompareExchange(List.PopLock, 1, 0) = 0);
    //Only pause after a failed attempt: the uncontended pop pays nothing.
    if not GoodExchange then
      CpuRelax;
  until GoodExchange;
  try
    repeat
      result := List.Entry.FLink;
      if Assigned(result) then
      begin
        Next := result.FLink;
        GoodExchange := LockAbstractions.InterlockedCompareExchangePointer
          (PSLEntryAlias(@List.Entry).FLink, Next, result) = result;
      end
      else
      begin
        result := nil;
        GoodExchange := true;
      end;
    until GoodExchange;
    if Assigned(result) then
    begin
      Assert(SLItemIsObj(result));
      result.FLink := nil;
    end;
  finally
    LockAbstractions.InterlockedExchange(List.PopLock, 0);
  end;
end;

function SLItemIsList(Item:PSLEntry): boolean;
begin
  Assert(Assigned(Item));
  result := not Assigned(Item.Owner);
end;

function SlItemIsObj(Item:PSLEntry): boolean;
begin
  result := not SLItemIsList(Item);
end;

initialization
  Assert(SizeOf(TSLEntry) = SizeOf(TSLEntryAlias));
  Assert(NativeUint(@PSLEntry(nil).FLink)
    = NativeUint(@PSLEntryAlias(nil).FLink));
end.
