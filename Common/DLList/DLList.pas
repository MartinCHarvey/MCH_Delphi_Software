unit DLList;
{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

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

{
  A unit to provide easily embeddable and fast doubly linked lists.
  Meant to be quicker and more efficient for storage & cache behaviour than
  TList. Of course, does not necessarily allow arbitrary access and indexing
}

interface

type
  PDLEntry = ^TDLEntry;
  TDLEntry = record
    Owner: TObject;
    FLink: PDlEntry;
    BLink: PDLEntry;
  end;

procedure DLItemInitObj(Owner: TObject; Entry: PDLEntry);
procedure DLItemInitList(Entry: PDLEntry);

function DLItemIsList(Entry: PDLEntry): boolean;
function DlItemIsEmpty(Entry: PDLEntry): boolean;

procedure DLItemInsertAfter(Entry: PDLEntry; NewEntry: PDLEntry);
procedure DLItemInsertBefore(Entry: PDLEntry; NewEntry: PDLEntry);

function DLItemRemoveBefore(Entry: PDLEntry): PDLEntry;
function DLItemRemoveAfter(Entry: PDLEntry): PDLEntry;

procedure DLListInsertHead(List: PDLEntry; NewEntry: PDlEntry);
procedure DLListInsertTail(List: PDLEntry; NewEntry: PDlEntry);

function DLListRemoveHead(List: PDLEntry): PDLEntry;
function DLListRemoveTail(List: PDLEntry): PDLEntry;

procedure DLListRemoveObj(Entry: PDlEntry);
procedure DLListRemoveList(List: PDLEntry); //Remove list head ptr from list.

implementation

procedure DLItemInitObj(Owner: TObject; Entry: PDLEntry);
begin
  Assert(Assigned(Entry));
  Assert(Assigned(Owner));
  Entry.Owner := Owner;
  Entry.FLink := Entry;
  Entry.Blink := Entry;
end;

procedure DLItemInitList(Entry: PDLEntry);
begin
  Assert(Assigned(Entry));
  Entry.Owner := nil;
  Entry.FLink := Entry;
  Entry.Blink := Entry;
end;

function DLItemIsList(Entry: PDLEntry): boolean;
begin
  Assert(Assigned(Entry));
  result := not Assigned(Entry.Owner);
end;

function DLItemIsEmpty(Entry: PDLEntry): boolean;
begin
  Assert(Assigned(Entry));
  Assert((Entry.Flink = Entry) = (Entry.Blink = Entry));
  result := (Entry.Flink = Entry);
end;

procedure DLItemInsertAfter(Entry: PDLEntry; NewEntry: PDLEntry);
begin
  Assert(Assigned(Entry));
  Assert(Assigned(NewEntry));
  Assert(Assigned(Entry.Flink));
  Assert(Assigned(Entry.BLink));
  Assert((Entry.Flink = Entry) = (Entry.Blink = Entry));
  Assert(DLItemIsEmpty(NewEntry)); //Remove poss of list cross linking.
  NewEntry.FLink := Entry.FLink;
  NewEntry.BLink := Entry;
  Entry.FLink.BLink := NewEntry;
  Entry.FLink := NewEntry;
end;

procedure DLItemInsertBefore(Entry: PDLEntry; NewEntry: PDLEntry);
begin
  Assert(Assigned(Entry));
  Assert(Assigned(NewEntry));
  Assert(Assigned(Entry.Flink));
  Assert(Assigned(Entry.BLink));
  Assert((Entry.Flink = Entry) = (Entry.Blink = Entry));
  Assert(DLItemIsEmpty(NewEntry)); //Remove poss of list cross linking.
  NewEntry.BLink := Entry.BLink;
  NewEntry.FLink := Entry;
  Entry.BLink.FLink := NewEntry;
  Entry.BLink := NewEntry;
end;

function DLItemRemoveBefore(Entry: PDLEntry): PDLEntry;
begin
  if not DlItemIsEmpty(Entry) then
  begin
    result := Entry.BLink;
    DLListRemoveObj(result);
  end
  else
    result := nil;
end;

function DLItemRemoveAfter(Entry: PDLEntry): PDLEntry;
begin
  if not DlItemIsEmpty(Entry) then
  begin
    result := Entry.FLink;
    DLListRemoveObj(result);
  end
  else
    result := nil;
end;

procedure DLListInsertHead(List: PDLEntry; NewEntry: PDlEntry);
begin
  Assert(DlItemIsList(List));
  DlItemInsertAfter(List, NewEntry);
end;

procedure DLListInsertTail(List: PDLEntry; NewEntry: PDlEntry);
begin
  Assert(DlItemIsList(List));
  DlItemInsertBefore(List, NewEntry);
end;

function DLListRemoveHead(List: PDLEntry): PDLEntry;
begin
  Assert(DlItemisList(List));
  result := DLItemRemoveAfter(List);
end;

function DLListRemoveTail(List: PDLEntry): PDLEntry;
begin
  Assert(DlItemIsList(List));
  result := DLItemRemoveBefore(List);
end;

procedure DLListRemoveObj(Entry: PDlEntry);
begin
  Assert(not DlItemIsempty(Entry));
  Assert(not DlItemIsList(Entry));
  Entry.FLink.BLink := Entry.BLink;
  Entry.BLink.FLink := Entry.FLink;
  DLItemInitObj(Entry.Owner, Entry);
end;

procedure DLListRemoveList(List: PDlEntry);
begin
  Assert(DlItemIsList(List));
  List.FLink.BLink := List.BLink;
  List.BLink.FLink := List.FLink;
end;

end.
