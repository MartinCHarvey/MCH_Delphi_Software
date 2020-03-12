unit CommonPool;
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

interface

uses Workitems, DLThreadQueue, SyncObjs, Classes, Trackables, DLList;

type
  TCommonPool = class;

  TClientRec  = class;

  TCommonPoolWorkItem = class(TWorkItem)
  private
    FParentPool: TCommonPool;
    FClientRec: TClientRec;
  protected
    procedure CompletionCommon(Normal: boolean);
    procedure DoNormalCompletion; override;
    procedure DoCancelledCompletion; override;
  public
  end;
  PCommonPoolWorkItem = ^TCommonPoolWorkItem;

{$IFDEF USE_TRACKABLES}
  TClientRec = class(TTrackable)
{$ELSE}
  TClientRec = class
{$ENDIF}
  private
    FAllClientsLink: TDLEntry;
  public
    Client: TObject;
    ClientNormalCompletion: TNotifyEvent;
    ClientCancelledCompletion: TNotifyEvent;
    ClientRefCount: integer;
    ClientWait: TEvent;
    ClientQuitting: boolean;
    constructor Create;
    destructor Destroy; override;
  end;

{$IFDEF USE_TRACKABLES}
  TCommonPool = class(TTrackable)
{$ELSE}
  TCommonPool = class
{$ENDIF}
  private
    FClients: TDLEntry;
    FFarm: TWorkFarm;
    FLock: TCriticalSection;
  protected
{$IFOPT C+}
    function FindClientRec(Client: TObject): TClientRec;
{$ENDIF}
    function DeRegisterClientInternalLocked(ClientRec: TClientRec;
                                            var MyThreadFrees: Boolean): boolean;
  public
    function AddWorkItem(ClientRec: TClientRec; Item: TCommonPoolWorkItem): boolean;
    function AddWorkItemBatch(ClientRec: TClientRec; PItem0: PCommonPoolWorkItem; Count: integer): boolean;
    constructor Create;
    destructor Destroy; override;
    //Register client checks for no duplicate addition only in debug mode.
    function RegisterClient(Client: TObject;
                            ClientNormalCompletion, ClientCancelledCompletion: TNotifyEvent): TClientRec;
    function DeRegisterClient(ClientRec: TClientRec): boolean;
    //TODO - cancel for a specific client...., doing a more selective flush and wait.
  end;

var
  GCommonPool: TCommonPool;

implementation

{ TCommonPoolWorkItem }

procedure TCommonPoolWorkItem.CompletionCommon(Normal: boolean);
var
  Signal: boolean;
  ProxyCall: TNotifyEvent;
begin
  Assert(Assigned(FParentPool));
  Assert(Assigned(FClientRec));
  FParentPool.FLock.Acquire;
  try
    Dec(FClientRec.ClientRefCount);
    Assert(FClientRec.ClientRefCount >= 0);
    Signal := (FClientRec.ClientRefCount = 0) and FClientRec.ClientQuitting;
    if Normal then
      ProxyCall := FClientRec.ClientNormalCompletion
    else
      ProxyCall := FCLientRec.ClientCancelledCompletion;
  finally
    FParentPool.FLock.Release;
  end;
  if Assigned(ProxyCall) then
    ProxyCall(self);
  if Signal then
    FClientRec.ClientWait.SetEvent;
end;

procedure TCommonPoolWorkItem.DoCancelledCompletion;
begin
  CompletionCommon(false);
end;

procedure TCommonPoolWorkItem.DoNormalCompletion;
begin
  CompletionCommon(true);
end;

{ TClientRec }

constructor TClientRec.Create;
begin
  inherited;
  DLItemInitObj(self, @FAllClientsLink);
end;

destructor TClientRec.Destroy;
begin
  Assert(DlItemIsEmpty(@FAllClientsLink));
  inherited;
end;

{ TCommonPool }

function TCommonPool.AddWorkItem(ClientRec: TClientRec; Item: TCommonPoolWorkItem): boolean;
begin
  result := false;
  Assert(Assigned(ClientRec));
  FLock.Acquire;
  try
    if not ClientRec.ClientQuitting then
    begin
      Item.FParentPool := self;
      Item.FClientRec := ClientRec;
      result := FFarm.AddWorkItem(Item);
      if result then Inc(ClientRec.ClientRefCount);
    end;
  finally
    FLock.Release;
  end;
end;

function TCommonPool.AddWorkItemBatch(ClientRec: TClientRec; PItem0: PCommonPoolWorkItem; Count: integer): boolean;
begin
  result := false;
  Assert(Assigned(ClientRec));
  FLock.Acquire;
  try
    if not ClientRec.ClientQuitting then
    begin
      result := FFarm.AddWorkItemBatch(PWorkItem(PItem0), Count);
      if result then Inc(ClientRec.ClientRefCount, Count);
    end;
  finally
    FLock.Release;
  end;
end;

{$IFOPT C+}
function TCommonPool.FindClientRec(Client: TObject): TClientRec;
begin
  result := FClients.FLink.Owner as TClientRec;
  while Assigned(result) do
  begin
    if result.Client = Client then
      exit;
    result := result.FAllClientsLink.FLink.Owner as TClientRec;
  end;
end;
{$ENDIF}

constructor TCommonPool.Create;
begin
  inherited;
  DLItemInitList(@FClients);
  FLock := TCriticalSection.Create;
  FFarm := TWorkFarm.Create;
end;

function TCommonPool.RegisterClient(Client: TObject;
                        ClientNormalCompletion, ClientCancelledCompletion: TNotifyEvent): TClientRec;
begin
  FLock.Acquire;
  try
{$IFOPT C+}
    result := FindClientRec(Client);
    if Assigned(result) then
    begin
      result := nil;
      Assert(false);
      exit;
    end;
{$ENDIF}
    result := TClientRec.Create;
    result.Client := Client;
    result.ClientNormalCompletion := ClientNormalCompletion;
    result.ClientCancelledCompletion := ClientCancelledCompletion;
    result.ClientRefCount := 0;
    result.ClientWait := TEvent.Create(nil, true, false, '');
    DLListInsertTail(@FClients, @result.FAllClientsLink);
  finally
    FLock.Release;
  end;
end;

function TCommonPool.DeRegisterClientInternalLocked(ClientRec: TClientRec;
                                                    var MyThreadFrees: Boolean): boolean;
begin
  result := Assigned(ClientRec);
  if result then
  begin
    MyThreadFrees := not ClientRec.ClientQuitting;
    ClientRec.ClientQuitting := true;
    if ClientRec.ClientRefCount > 0 then
    begin
      FLock.Release;
      ClientRec.ClientWait.WaitFor(INFINITE);
      FLock.Acquire;
    end;
    if MyThreadFrees then
    begin
      DLListRemoveObj(@ClientRec.FAllClientsLink);
      ClientRec.Free;
    end;
  end;
end;

function TCommonPool.DeRegisterClient(ClientRec: TClientRec): boolean;
var
  MyThreadFrees: boolean;
begin
  FLock.Acquire;
  try
    result := DeRegisterClientInternalLocked(ClientRec, MyThreadFrees);
  finally
    FLock.Release;
  end;
end;

destructor TCommonPool.Destroy;
var
  ClientRec,PrevClientRec: TClientRec;
  MyThreadFrees: boolean;
begin
  //TODO - Might be able to start flush of underlying
  //threadpool here if quit is slow.
  FLock.Acquire;
  try
    ClientRec := FClients.FLink.Owner as TClientRec;
    while Assigned(ClientRec) do
    begin
      DeRegisterClientInternalLocked(ClientRec, MyThreadFrees);
      PrevClientRec := ClientRec;
      //Interesting error cases when mem freeing sets
      //memory to all 0xCD ....
      if MyThreadFrees then
        ClientRec := FClients.FLink.Owner as TClientRec
      else
        ClientRec := ClientRec.FAllClientsLink.FLink.Owner as TClientRec;
      if PrevClientRec = ClientRec then
        break; //Belt and braces, let's just quit.
    end;
  finally
    FLock.Release;
  end;
  FFarm.Free;
  Assert(DlItemIsEmpty(@FClients));
  FLock.Free;
  inherited;
end;

procedure CreateGlobals;
begin
  Assert(not Assigned(GCommonPool));
  GCommonPool := TCommonPool.Create;
end;

procedure DestroyGlobals;
begin
  Assert(Assigned(GCommonPool));
  GCommonPool.Free;
  GCommonPool := nil;
end;

initialization
  CreateGlobals;
finalization
  DestroyGlobals;
end.
