unit DLThreadQueue;
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

  A thread-ish safe list object,
  using "nicer" DL list objs.

  This list expects you to lock it yourself before
  calling its methods, allowing you to do atomic compares,
  moves of items between lists, etc etc

  N.B This object good for queuing things that are *not*
  TWorkItem. For things that are TWorkItem or descendant,
  use TOwnLockWorkQueue.
}

interface

uses DLList, LockAbstractions;

type
  TOwnLockDLThreadQueue = class;
  TDLThreadQueueable = class;

  //Queue for items which cannot have a DLLLink set in them.
  //Extra cost for memory allocation overhead in this case
  //when adding to or removing from lists.

  //Could make this shared lock in future if need be....
  TDLProxyThreadQueue = class
  private
    FRealQueue: TOwnLockDLThreadQueue;
  protected
    function GetCount: integer;
    function GetLock: TCriticalSection;
    procedure SetLock(Lock: TCriticalSection);
    property Lock:TCriticalSection read GetLock write SetLock;
  public
    constructor Create;
    destructor Destroy; override;
    function AddHeadObj(Obj: TObject): TDLThreadQueueable;
    //Returns proxy object created if you need to use RemoveObj function
    function AddTailobj(Obj: TObject): TDLThreadQueueable;
    //Returns proxy object created if you need to use RemoveObj function
    function RemoveHeadObj: TObject;
    function RemoveTailObj: TObject;
    function PeekHeadObj: TObject;
    function PeekTailObj: TObject;
    procedure RemoveObj(Obj: TObject; Proxied: TDLThreadQueueable);
    procedure AcquireLock;
    procedure ReleaseLock;
    property Count: integer read GetCount;
  end;

  TDLProxyThreadQueuePublicLock = class(TDLProxyThreadQueue)
  public
    property Lock;
  end;

  TDLThreadQueueable = class
  protected
    FEntry: TDLEntry;
    FProxy: TObject;
    procedure SetProxy(Proxy: TObject);
    function GetProxy: TObject;
    property Proxy: TObject read GetProxy write SetProxy;
  public
    constructor Create;
  end;

  TDLThreadQueue = class
  private
    FDebugLocked: boolean;
    FCount: integer;
    FHead: TDLEntry;
    FLock: TCriticalSection;
  protected
    function GetCount: integer;
    property Lock: TCriticalSection read FLock write FLock;
    procedure CheckDebugLocked;
    procedure CheckDebugUnlocked;
    function Peek(Head: boolean): TDLThreadQueueable;
  public
    constructor Create;
    destructor Destroy; override;
    procedure AddHeadObj(Obj: TDLThreadQueueable);
    procedure AddTailobj(Obj: TDLThreadQueueable);
    function RemoveHeadObj: TDLThreadQueueable;
    function RemoveTailObj: TDLThreadQueueable;
    function PeekHeadObj: TDLThreadQueueable;
    function PeekTailObj: TDLThreadQueueable;
    procedure RemoveObj(Obj: TDLThreadQueueable);
    procedure AcquireLock;
    procedure ReleaseLock;

    property Count: integer read GetCount;
  end;

  TOwnLockDLThreadQueue = class (TDLThreadQueue)
  private
    FOwnLock: TCriticalSection;
  public
    constructor Create;
    destructor Destroy; override;
  end;

implementation

{ TProxyDLThreadQueue }

function TDLProxyThreadQueue.GetLock: TCriticalSection;
begin
  result := FRealQueue.Lock;
end;

procedure TDLProxyThreadQueue.SetLock(Lock: TCriticalSection);
begin
  FRealQueue.Lock := Lock;
end;

function TDLProxyThreadQueue.GetCount: integer;
begin
  result := FRealQueue.Count;
end;

constructor TDLProxyThreadQueue.Create;
begin
  inherited;
  FRealQueue := TOwnLockDLThreadQueue.Create;
end;

destructor TDLProxyThreadQueue.Destroy;
begin
  FRealQueue.Free;
  inherited;
end;

function TDLProxyThreadQueue.AddHeadObj(Obj: TObject): TDLThreadQueueable;
var
  NewItem: TDLThreadQueueable;
begin
  NewItem := TDLThreadQueueable.Create;
  NewItem.Proxy := Obj;
  FRealQueue.AddHeadObj(NewItem);
  result := NewItem;
end;

function TDLProxyThreadQueue.AddTailObj(Obj: TObject): TDLThreadQueueable;
var
  NewItem: TDLThreadQueueable;
begin
  NewItem := TDLThreadQueueable.Create;
  NewItem.Proxy := Obj;
  FRealQueue.AddTailObj(NewItem);
  result := NewItem;
end;

function TDLProxyThreadQueue.RemoveHeadObj: TObject;
var
  ItemLink: TDLThreadQueueable;
begin
  ItemLink := FRealQueue.RemoveHeadObj;
  if Assigned(ItemLink) then
  begin
    result := ItemLink.Proxy;
    ItemLink.Free;
  end
  else
    result := nil;
end;

function TDLProxyThreadQueue.PeekHeadObj: TObject;
var
  ItemLink: TDLThreadQueueable;
begin
  ItemLink := FRealQueue.PeekHeadObj;
  if Assigned(ItemLink) then
    result := ItemLink.Proxy
  else
    result := nil;
end;

function TDLProxyThreadQueue.PeekTailObj: TObject;
var
  ItemLink: TDLThreadQueueable;
begin
  ItemLink := FRealQueue.PeekTailObj;
  if Assigned(ItemLink) then
    result := ItemLink.Proxy
  else
    result := nil;
end;

function TDLProxyThreadQueue.RemoveTailObj: TObject;
var
  ItemLink: TDLThreadQueueable;
begin
  ItemLink := FRealQueue.RemoveTailObj;
  if Assigned(ItemLink) then
  begin
    result := ItemLink.Proxy;
    ItemLink.Free;
  end
  else
    result := nil;
end;

procedure TDLProxyThreadQueue.RemoveObj(Obj: TObject; Proxied: TDLThreadQueueable);
begin
  Assert(Proxied.Proxy = Obj);
  FRealQueue.RemoveObj(Proxied);
  Proxied.Proxy := nil;
  Proxied.Free;
end;

procedure TDLProxyThreadQueue.AcquireLock;
begin
  FRealQueue.AcquireLock;
end;

procedure TDLProxyThreadQueue.ReleaseLock;
begin
  FRealQueue.ReleaseLock;
end;

{ TDLThreadQueueable }

constructor TDLThreadQueueable.Create;
begin
  inherited;
  DLItemInitObj(self, @FEntry);
end;

procedure TDLThreadQueueable.SetProxy(Proxy: TObject);
begin
  FProxy := Proxy;
end;

function TDLThreadQueueable.GetProxy;
begin
  result := FProxy;
end;

{ TDLThreadQueue }

procedure TDLThreadQueue.CheckDebugLocked;
begin
  if Assigned(FLock) then
    Assert(FDebugLocked);
end;

procedure TDLThreadQueue.CheckDebugUnlocked;
begin
  if Assigned(FLock) then
    Assert(not FDebugLocked);
end;

function TDLThreadQueue.GetCount: integer;
begin
  CheckDebugLocked();
  result := FCount;
end;

constructor TDLThreadQueue.Create;
begin
  inherited;
  DLItemInitList(@FHead);
end;

destructor TDLThreadQueue.Destroy;
begin
  CheckDebugUnlocked;
  Assert(FCount = 0);
  Assert(DLItemIsEmpty(@FHead));
  inherited;
end;

procedure TDLThreadQueue.AddHeadObj(Obj: TDLThreadQueueable);
begin
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
  CheckDebugLocked();
  DLListInsertHead(@FHead, @Obj.FEntry);
  Inc(FCount);
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
end;

procedure TDLThreadQueue.AddTailobj(Obj: TDLThreadQueueable);
begin
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
  CheckDebugLocked();
  DLListInsertTail(@FHead, @Obj.FEntry);
  Inc(FCount);
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
end;

function TDLThreadQueue.RemoveHeadObj: TDLThreadQueueable;
var
  ItemLink: PDLEntry;
begin
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
  CheckDebugLocked();
  ItemLink := DLListRemoveHead(@FHead);
  if Assigned(ItemLink) then
  begin
    result := TDLThreadQueueable(ItemLink.Owner);
    Assert(result is TDLThreadQueueable);
    Dec(FCount);
  end
  else
    result := nil;
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
end;

function TDLThreadQueue.RemoveTailObj: TDLThreadQueueable;
var
  ItemLink: PDLEntry;
begin
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
  CheckDebugLocked();
  ItemLink := DLListRemoveTail(@FHead);
  if Assigned(ItemLink) then
  begin
    result := TDLThreadQueueable(ItemLink.Owner);
    Assert(result is TDLThreadQueueable);
    Dec(FCount);
  end
  else
    result := nil;
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
end;


function TDLThreadQueue.Peek(Head: boolean): TDLThreadQueueable;
var
  ItemLink: PDLEntry;
begin
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
  CheckDebugLocked();
  if not DlItemIsEmpty(@FHead) then
  begin
    if Head then
      ItemLink := FHead.FLink
    else
      ItemLink := FHead.BLink;
  end
  else
    ItemLink := nil;
  if Assigned(ItemLink) then
  begin
    result := TDLThreadQueueable(ItemLink.Owner);
    Assert(result is TDLThreadQueueable);
  end
  else
    result := nil;
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
end;

function TDLThreadQueue.PeekHeadObj: TDLThreadQueueable;
begin
  result := Peek(true);
end;

function TDLThreadQueue.PeekTailObj: TDLThreadQueueable;
begin
  result := Peek(false);
end;


procedure TDLThreadQueue.RemoveObj(Obj: TDLThreadQueueable);
begin
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
  CheckDebugLocked();
  DLListRemoveObj(@Obj.FEntry);
  Assert(Obj.FEntry.Owner = Obj);
  Assert(Obj.FEntry.Owner is TDLThreadQueueable);
  Dec(FCount);
  Assert(DLItemIsEmpty(@FHead) = (FCount = 0));
end;

procedure TDLThreadQueue.AcquireLock;
begin
  if Assigned(FLock) then
  begin
    FLock.Acquire;
    FDebugLocked := true;
  end;
end;

procedure TDLThreadQueue.ReleaseLock;
begin
  if Assigned(FLock) then
  begin
    FDebugLocked := false;
    FLock.Release;
  end;
end;

{ TOwnLockDLThreadQueue }

constructor TOwnLockDLThreadQueue.Create;
begin
  inherited;
  FOwnLock := TCriticalSection.Create;
  FLock := FOwnLock;
end;

destructor TOwnLockDLThreadQueue.Destroy;
begin
  FOwnLock.Free;
  FLock := nil;
  inherited;
end;

end.
