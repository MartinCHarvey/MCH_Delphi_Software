unit WorkItems;
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
  A unit to provide workitem and worker thread behaviour similar to that
  available in the windows kernel. Supposed to be reasonably lightweight.
}

interface

{$WARN SYMBOL_PLATFORM OFF}

{$IF Defined(MSWINDOWS)}

{$IFDEF USE_TRACKABLES}
uses
  DLList, Trackables, Classes, Windows, SyncObjs;
{$ELSE}
uses
  DLList, Classes, Windows, SyncObjs;
{$ENDIF}

{$ELSEIF Defined(POSIX)}

{$IFDEF USE_TRACKABLES}
uses
  DLList, Trackables, Classes, SyncObjs;
{$ELSE}
uses
  DLList, Classes, SyncObjs;
{$ENDIF}

{$ENDIF}

const
  E_WORKITEM_NOT_IMPL = -1;

type
  TWorkItemState = (wisStarting,
    wisExecuting,
    wisCancelling,
    wisCompHandler,
    wisCancelHandler,
    wisDone);
  TWorkItemStateSet = set of TWorkItemState;

  PWorkerThreadRec = ^TWorkerThreadRec;
  PWorkItem = ^TWorkItem;
{$IFDEF USE_TRACKABLES}
  TWorkItem = class(TTrackable)
{$ELSE}
  TWorkItem = class
{$ENDIF}
  private
    FDLLink: TDLEntry;
    FWorkItemState: integer; //Used as TWorkItemState, but interlocked ops.
    FFreeRefCount: integer;
    FCancelled: boolean;
    FRetCode: integer;
    //Added or changed for workitem recycle ability - chapter 5.
    FFreeInThread: boolean;
    FAutoReset: boolean;
    FSpecificFreeCalled: boolean;
    //Pointer to thread rec in situations where overriden code needs
    //to access original thread
    FThreadRec: PWorkerThreadRec;
  protected
    procedure IncFreeRefCount;
    procedure DecFreeRefCount;

    //Like a reset, but used internally to allow workitem recycling.
    //Not callable by external code.
    procedure ResetStateMachine;

    procedure DoCancel; virtual; //Prompt cancel call.
    function DoWork: integer; virtual;
    procedure DoNormalCompletion; virtual; //Completion cleanup.
    procedure DoCancelledCompletion; virtual; //Cancel cleanup.
    function CMovState(StartSet: TWorkItemStateSet;
      EndState: TWorkItemState): boolean;
    function GetWorkItemState: TWorkItemState;
    //Added or changed for workitem recycle ability - chapter 5.
    property CanAutoFree: boolean read FFreeInThread write FFreeInThread;
    property CanAutoReset: boolean read FAutoReset write FAutoReset;
    //
    property WorkItemState: TWorkItemState read GetWorkItemState;
    property ThreadRec: PWorkerThreadRec read FThreadRec;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Free;
    procedure Reset; virtual;
    function Cancel: boolean;
    procedure WorkInThread;
    procedure CompleteInThread;
    //Added or changed for workitem recycle ability - chapter 5.
    procedure AutoFree;
    procedure AutoReset;
    //
    property Cancelled: boolean read FCancelled;
    property RetCode: integer read FRetCode;
  end;

  TWorkContainerState = (wcsNormal, wcsFlushing, wcsFlushFinished);
  TContainerStateEvent = procedure(Sender: TObject;
    NewState: TWorkContainerState) of object;

{$IFDEF USE_TRACKABLES}
  TWorkItemContainer = class(TTrackable)
{$ELSE}
  TWorkItemContainer = class
{$ENDIF}
  private
    FFlushState: TWorkContainerState;
    FLock: TCriticalSection;
    FStateChanged: TContainerStateEvent;
    FDebugLocked: boolean;
  protected
    procedure AcquireLock;
    procedure ReleaseLock;
    property Lock: TCriticalSection read FLock write FLock;
    procedure StartFlushInternal(CancelWork: boolean); virtual;
    procedure ResetFlushInternal; virtual;
    procedure FlushFinishedInternal; virtual;
    function EvalFlushFinished: boolean; virtual;
    procedure CheckFlushFinished;
    function GetFlushStateLocked: TWorkContainerState;
    function GetFlushState: TWorkContainerState;
    procedure SetFlushStateLocked(NewState: TWorkContainerState);
    property DebugLocked: boolean read FDebugLocked;
  public
    property IntLock: TCriticalSection read FLock;
    function StartFlush(CancelWork: boolean): boolean;
    function ResetFinishedFlush: boolean;
    property FlushState: TWorkContainerState read GetFlushState;
    property OnStateChanged: TContainerStateEvent
      read FStateChanged write FStateChanged;
  end;

  TWorkQueue = class(TWorkItemContainer)
  private
    FDLHead: TDLEntry;
    FEntryCount: integer;
  protected
    function GetEntryCount: integer;
    function GetEntryCountLocked: integer;
    procedure StartFlushInternal(CancelWork: boolean); override;
    function EvalFlushFinished: boolean; override;
  public
    function AddWorkItem(Item: TWorkItem): boolean;
    function AddWorkItemBatch(PItem0: PworkItem; Count: integer): boolean;
    function RemoveWorkItem: TWorkItem;
    constructor Create;
    destructor Destroy; override;
    property EntryCountLocked: integer read GetEntryCountLocked;
  end;

  TOwnLockWorkQueue = class(TWorkQueue)
  private
  protected
  public
    constructor Create;
    destructor Destroy; override;
  end;

  TWorkFarm = class;

  TWorkerThread = class(TThread)
  private
    FThreadRec: PWorkerThreadRec;
  protected
    procedure Execute; override;
  public
    procedure Queue(AMethod: TThreadMethod);
    property ThreadRec: PWorkerThreadRec read FThreadRec write FThreadRec;
  end;

  TWorkerThreadRec = record
    Farm: TWorkFarm;
    Thread: TWorkerThread;
    CurItem: TWorkItem;
  end;

  TWorkFarm = class(TWorkItemContainer)
  private
    FQueue: TWorkQueue;
    FWorkers: array of TWorkerThreadRec;
    FWorkAvail: TEvent;
    FWorkAvailSet: boolean;
    FThreadsQuitting: TEvent;
    FFlushFinished: TEvent;
{$IF Defined(MSWINDOWS)}
    FPriority: TThreadPriority;
{$ELSEIF Defined(POSIX)}
    FPriority: integer;
{$ENDIF}
  protected
    procedure StartFlushInternal(CancelWork: boolean); override;
    procedure ResetFlushInternal; override;
    procedure FlushFinishedInternal; override;
    function EvalFlushFinished: boolean; override;
    procedure CheckWorkAvail;
    procedure WorkerThreadFunc(Sender: TObject; Rec: PWorkerThreadRec);
    function GetThreadCount: integer;
    procedure SetThreadCount(NewCount: integer);
    //InitThreadRec Added or changed for NUMA / Group support (Chapter 8).
    procedure InitThreadRec(var Rec: TWorkerThreadRec); virtual;
    procedure WindupThreads;
{$IF Defined(MSWINDOWS)}
    function GetThreadPriority: TThreadPriority;
    procedure SetThreadPriority(NewPriority: TThreadPriority);
{$ELSEIF Defined(POSIX)}
    function GetThreadPriority: integer;
    procedure SetThreadPriority(NewPriority: integer);
{$ENDIF}
  public
    function AddWorkItem(Item: TWorkItem): boolean;
    function AddWorkItemBatch(PItem0: PWorkItem; Count: integer): boolean;
    constructor Create;
    destructor Destroy; override;
    property ThreadCount: integer read GetThreadCount write SetThreadCount;
    procedure FlushAndWait(CancelWork: boolean);
{$IF Defined(MSWINDOWS)}
    property ThreadPriority: TThreadPriority
      read GetThreadPriority write SetThreadPriority;
{$ELSEIF Defined(POSIX)}
    property ThreadPriority: integer
      read GetThreadPriority write SetThreadPriority;
{$ENDIF}
  end;

implementation

uses
  SysUtils;

{ TWorkItem }

procedure TWorkItem.DoCancel;
begin
  FCancelled := true;
end;

function TWorkItem.DoWork: integer;
begin
  result := E_WORKITEM_NOT_IMPL;
end;

procedure TWorkItem.DoNormalCompletion;
begin
end;

procedure TWorkItem.DoCancelledCompletion;
begin
end;

function TWorkItem.CMovState(StartSet: TWorkItemStateSet;
  EndState: TWorkItemState): boolean;
var
  LStart, LDest: integer;
begin
  result := false;
  LDest := Integer(EndState);
  LStart := FWorkItemState;
  while (TWorkItemState(LStart) in StartSet) and not result do
  begin
    result := TInterlocked.CompareExchange(FWorkItemState, LDest, LStart) = LStart;
    LStart := FWorkItemState;
  end;
end;

function TWorkItem.GetWorkItemState: TWorkItemState;
begin
  result := TWorkItemState(FWorkItemState);
end;

constructor TWorkItem.Create;
begin
  inherited;
  TInterlocked.Increment(FFreeRefCount);
  DLItemInitObj(Self, @FDLLink);
  FWorkItemState := Ord(wisStarting);
end;

//Only resets state machine, does not touch queue fields, to allow
//for re-queueing.
procedure TWorkItem.ResetStateMachine;
{$IFOPT C+}
var
  Handled: boolean;
{$ENDIF}
begin
  FCancelled := false;
{$IFOPT C+}
  Handled := CMovState([wisDone, wisStarting], wisStarting);
  Assert(Handled);
{$ELSE}
  CMovState([wisDone, wisStarting], wisStarting);
{$ENDIF}
end;

//Added or changed for workitem recycle ability - chapter 5.
//Resets both queue fields, and also internal state.
procedure TWorkItem.Reset;
begin
  Assert(DLItemIsEmpty(@FDLLink));
  //Assert(not FAutoReset); Include if you want to check
  //for spurious resets on auto-reset workitems.
  ResetStateMachine;
  DLItemInitObj(Self, @FDLLink);
end;

destructor TWorkItem.Destroy;
{$IFOPT C+}
var
  StateCopy : TWorkItemState;
{$ENDIF}
begin
  Assert(FSpecificFreeCalled);
  Assert(DlItemIsEmpty(@FDLLink));
{$IFOPT C+}
  StateCopy := TWorkItemState(FWorkItemState);
  Assert((StateCopy = WisStarting) or (StateCopy = WisDone),
    'Work item destroying in state ' + IntToStr(Ord(StateCopy)) +
    ' expected state ' + IntToStr(Ord(WisStarting)) +
    ' or state ' + IntToStr(Ord(WisDone)));
{$ENDIF}
  inherited;
end;

procedure TWorkItem.Free;
begin
  if Assigned(Self) then
    DecFreeRefCount;
end;

procedure TWorkItem.IncFreeRefCount;
begin
  TInterlocked.Increment(FFreeRefCount);
end;

procedure TWorkItem.DecFreeRefCount;
begin
  if TInterlocked.Decrement(FFreeRefCount) <= 0 then
  begin
    FSpecificFreeCalled := true;
    inherited Free;
  end;
end;

function TWorkItem.Cancel: boolean;
begin
  result := CMovState([wisStarting, wisExecuting], wisCancelling);
  if result then
    DoCancel;
end;

procedure TWorkItem.WorkInThread;
var
  Handled: boolean;
  StateCopy: TWorkItemState;
begin
  repeat
    Handled := CMovState([wisStarting], wisExecuting);
    if Handled then
      //Obvious case.
      try
        FRetCode := DoWork
      except
        Cancel;
      end
    else
    begin
      //Cannot immediately start work. Why not?
      StateCopy := TWorkItemState(FWorkItemState);
      if StateCopy = wisCancelling then
      begin
        //Manual or auto-reset workitem cancelled before we could execute it.
        //Skip work, and do the cancellation handler.
        Handled := true;
      end
      else if FAutoReset and (StateCopy in [wisCompHandler, wisCancelHandler,
                         wisDone, wisStarting]) then
      begin
        //Wait for other thread in threadpool to change workitem state.
        //We expect it to move back into starting state.
        Sleep(0);
      end
      else
      begin
        //Re-queued to a threadpool from in main worker function
        //or not auto-reset, would result in hang or double work being done.
        //Re-queue workitems only from completion handlers, and either
        //reset them from VCL thread or make auto-reset before doing so.
        Assert(false);
        Cancel;
        Handled := true;
      end;
    end;
  until Handled;
end;

procedure TWorkItem.CompleteInThread;
{$IFOPT C+}
var
  Handled: boolean;
{$ENDIF}
begin
{$IFOPT C+}
  Handled := false;
{$ENDIF}
  if CMovState([wisExecuting], wisCompHandler) then
  begin
    try
      DoNormalCompletion;
    except
      try
        Assert(false, 'Please don''t throw exceptions at underlying threadpool code');
      except
      end;
    end;
{$IFOPT C+}
    Handled := CMovState([wisCompHandler], wisDone);
    Assert(Handled);
{$ELSE}
    CMovState([wisCompHandler], wisDone);
{$ENDIF}
  end;
  //If couldn't do the executing cmov, then must be cancelling.
  if CMovState([wisCancelling], wisCancelHandler) then
  begin
    try
      DoCancelledCompletion;
    except
      try
        Assert(false, 'Please don''t throw exceptions at underlying threadpool code');
      except
      end;
    end;
{$IFOPT C+}
    Handled := CMovState([wisCancelHandler], wisDone);
    Assert(Handled);
{$ELSE}
    CMovState([wisCancelHandler], wisDone);
{$ENDIF}
  end;
{$IFOPT C+}
  Assert(Handled);
{$ENDIF}
end;

//Added or changed for workitem recycle ability - chapter 5.
procedure TWorkItem.AutoFree;
begin
  if FFreeInThread then
    Free;
end;

//Added or changed for workitem recycle ability - chapter 5.
procedure TWorkItem.AutoReset;
begin
  if FAutoReset then
    ResetStateMachine;
end;

{ TWorkItemContainer }

procedure TWorkItemContainer.AcquireLock;
begin
  if Assigned(FLock) then
  begin
    FLock.Acquire;
  end;
  FDebugLocked := true;
end;

procedure TWorkItemContainer.ReleaseLock;
begin
  FDebugLocked := false;
  if Assigned(FLock) then
  begin
    FLock.Release;
  end;
end;

procedure TWorkItemContainer.StartFlushInternal(CancelWork: boolean);
begin
  Assert(FFlushState = wcsNormal);
  Assert(DebugLocked);
  SetFlushStateLocked(wcsFlushing);
end;

procedure TWorkItemContainer.ResetFlushInternal;
begin
  Assert(FFlushState = wcsFlushFinished);
  Assert(DebugLocked);
  SetFlushStateLocked(wcsNormal);
end;

procedure TWorkItemContainer.FlushFinishedInternal;
begin
  Assert(FFlushState = wcsFlushing);
  Assert(DebugLocked);
  SetFlushStateLocked(wcsFlushFinished);
end;

function TWorkItemContainer.EvalFlushFinished: boolean;
begin
  { Check not already finished - can have multiple
    calls to check flush finished under the same lock,
    so don't want to duplicate flush finishes }
  result := FFlushState = wcsFlushing;
end;

procedure TWorkItemContainer.CheckFlushFinished;
begin
  Assert(DebugLocked);
  if EvalFlushFinished then
    FlushFinishedInternal;
end;

function TWorkItemContainer.GetFlushState: TWorkContainerState;
begin
  AcquireLock;
  result := FFlushState;
  ReleaseLock
end;

function TWorkItemContainer.GetFlushStateLocked: TWorkContainerState;
begin
  Assert(DebugLocked);
  result := FFlushState;
end;

function TWorkItemContainer.StartFlush(CancelWork: boolean): boolean;
begin
  AcquireLock;
  result := FFlushState = wcsNormal;
  if result then
  begin
    StartFlushInternal(CancelWork);
    CheckFlushFinished;
  end;
  ReleaseLock;
end;

function TWorkItemContainer.ResetFinishedFlush: boolean;
begin
  AcquireLock;
  result := FFlushState = wcsFlushFinished;
  if result then
    ResetFlushInternal;
  ReleaseLock;
end;

procedure TWorkItemContainer.SetFlushStateLocked(NewState: TWorkContainerState);
begin
  Assert(DebugLocked);
  FFlushState := NewState;
  if Assigned(FStateChanged) then
    FStateChanged(Self, NewState);
end;

{ TWorkQueue }

function TWorkQueue.GetEntryCount: integer;
begin
  AcquireLock;
  result := FEntryCount;
  ReleaseLock;
end;

function TWorkQueue.GetEntryCountLocked: integer;
begin
  result := FEntryCount;
end;

function TWorkQueue.AddWorkItem(Item: TWorkItem): boolean;
begin
  AcquireLock;
  result := GetFlushStateLocked = wcsNormal;
  if result then
  begin
    Inc(FEntryCount);
    DLListInsertHead(@FDLHead, @Item.FDLLink);
    Assert((FEntryCount > 0) = not DLItemIsEmpty(@FDLHead));
  end;
  ReleaseLock;
end;

function TWorkQueue.AddWorkItemBatch(PItem0: PworkItem; Count: integer):
  boolean;
var
  Idx: integer;
  PItem: PWorkItem;
begin
  AcquireLock;
  result := GetFlushStateLocked = wcsNormal;
  if result then
  begin
    Idx := 0;
    PItem := pItem0;
    while Idx < Count do
    begin
      Inc(FEntryCount);
      DLListInsertHead(@FDLHead, @PItem.FDLLink);
      Assert((FEntryCount > 0) = not DLItemIsEmpty(@FDLHead));
      Inc(Idx);
      Inc(PItem);
    end;
  end;
  ReleaseLock;
end;

function TWorkQueue.RemoveWorkItem: TWorkItem;
var
  ResItem: PDLEntry;
begin
  AcquireLock;
  ResItem := DLListRemoveTail(@FDLHead);
  if Assigned(ResItem) then
  begin
    result := ResItem.Owner as TWorkItem;
    Dec(FEntryCount);
    Assert((FEntryCount > 0) = not DLItemIsEmpty(@FDLHead));
    CheckFlushFinished;
  end
  else
    result := nil;
  ReleaseLock;
end;

procedure TWorkQueue.StartFlushInternal(CancelWork: boolean);
var
  Entry: PDLEntry;
  Item: TWorkItem;
begin
  inherited;
  if CancelWork then
  begin
    Entry := FDLHead.FLink;
    while Entry <> @FDLHead do
    begin
      Item := Entry.Owner as TWorkItem;
      Item.Cancel;
      Entry := Entry.FLink;
    end;
  end;
end;

function TWorkQueue.EvalFlushFinished: boolean;
begin
  result := inherited EvalFlushFinished and (FEntryCount = 0);
end;

constructor TWorkQueue.Create;
begin
  inherited;
  DLItemInitList(@FDLHead);
end;

destructor TWorkQueue.Destroy;
begin
  Assert(FEntryCount = 0);
  Assert(DLItemIsEmpty(@FDLHead));
  inherited;
end;

{ TOwnLockWorkQueue }

constructor TOwnLockWorkQueue.Create;
begin
  inherited;
  FLock := TCriticalSection.Create;
end;

destructor TOwnLockWorkQueue.Destroy;
begin
  FLock.Free;
  inherited;
end;

{ TWorkerThread }

procedure TWorkerThread.Queue(AMethod: TThreadMethod);
begin
  inherited Queue(AMethod);
end;

procedure TWorkerThread.Execute;
begin
  FThreadRec.Farm.WorkerThreadFunc(Self, FThreadRec);
end;

{ TWorkFarm }

procedure TWorkFarm.StartFlushInternal(CancelWork: boolean);
var
  idx: integer;
  Rec: PWorkerThreadRec;
begin
  inherited;
  Assert(DebugLocked);
  FFlushFinished.ResetEvent;
  FQueue.StartFlush(CancelWork);
  if CancelWork then
  begin
    for Idx := 0 to High(FWorkers) do
    begin
      Rec := @FWorkers[Idx];
      if Assigned(Rec.CurItem) then
      begin
        try
          Rec.CurItem.Cancel;
        except
          on E: Exception do ;
          //Not much we can do here except swallow the exception and keep going.
        end;
      end;
    end;
  end;
end;

procedure TWorkFarm.ResetFlushInternal;
{$IFOPT C+}
var
  QReset: boolean;
{$ENDIF}
begin
  inherited;
  Assert(DebugLocked);
{$IFOPT C+}
  QReset := FQueue.ResetFinishedFlush;
  Assert(QReset);
{$ELSE}
  FQueue.ResetFinishedFlush;
{$ENDIF}
  FFlushFinished.ResetEvent;
end;

procedure TWorkFarm.FlushFinishedInternal;
begin
  inherited;
{$IFOPT C+}
  Assert(DebugLocked);
{$ENDIF}
  FFlushFinished.SetEvent;
end;

function TWorkFarm.EvalFlushFinished: boolean;
var
  Idx: integer;
  Rec: PWorkerThreadRec;
begin
  Assert(DebugLocked);
  result := inherited EvalFlushFinished;
  if result then
    result := result and (FQueue.FlushState = wcsFlushFinished);
  if result then
    for Idx := 0 to High(FWorkers) do
    begin
      Rec := @FWorkers[Idx];
      if Assigned(Rec.CurItem) then
      begin
        result := false;
        break;
      end;
    end;
end;

procedure TWorkFarm.CheckWorkAvail;
var
  NewWorkAvailSet: boolean;
begin
  Assert(DebugLocked);
  NewWorkAvailSet := FQueue.EntryCountLocked > 0;
  if (NewWorkAvailSet <> FWorkAvailSet) then
  begin
    if NewWorkAvailSet then
      FWorkAvail.SetEvent
    else
      FWorkAvail.ResetEvent;
    FWorkAvailSet := NewWorkAvailSet;
  end;
end;

procedure TWorkFarm.WorkerThreadFunc(Sender: TObject; Rec: PWorkerThreadRec);
var
  ret: TWaitResult;
{$IF Defined(MSWINDOWS)}
  WHandles: THandleObjectArray;
  Signalled: THandleObject;
{$ENDIF}
  GotWork: boolean;

begin
{$IF Defined(MSWINDOWS)}
  SetLength(WHandles, 2);
  WHandles[0] := FWorkAvail;
  WHandles[1] := FThreadsQuitting;
{$ENDIF}
  while true do
  begin
{$IF Defined(MSWINDOWS)}
    ret := THandleObject.WaitForMultiple(WHandles, INFINITE, false, Signalled);
    if not (ret = wrSignaled) then
    begin
      Assert(false);
      exit;
    end;
    if (Signalled <> FWorkAvail) then
      exit; //thread quitting case.
{$ELSEIF Defined(POSIX)}
    //TODO - this is a very nasty hack, but will do for the time being.
    ret := wrTimeout;
    while ret <> wrSignaled do
    begin
      ret := FWorkAvail.WaitFor(10);
      if (ret <> wrSignaled) and (ret <> wrTimeout) then
      begin
        Assert(false);
        exit;
      end;

      if (ret <> wrSignaled) then
      begin
        ret := FThreadsQuitting.WaitFor(10);
        if (ret <> wrSignaled) and (ret <> wrTimeout) then
          Assert(false);
        if ret = wrSignaled then
          exit;
      end;
    end;
{$ENDIF}

    //OK, we have work available.
    Assert(Rec.Farm = Self);
    Assert(Rec.Thread = Sender);
    Assert(not Assigned(Rec.CurItem));

    AcquireLock;
    repeat //No need to make a kernel call until have exhausted queue.
      Rec.CurItem := FQueue.RemoveWorkItem;
      GotWork := Assigned(Rec.CurItem);
      if GotWork then
      begin
        CheckWorkAvail;
        ReleaseLock;

        Rec.CurItem.IncFreeRefCount;
        Rec.CurItem.FThreadRec := Rec;
        Rec.CurItem.WorkInThread;
        Rec.CurItem.CompleteInThread;
        Rec.CurItem.FThreadRec := nil;

        Rec.CurItem.AutoReset;
        Rec.CurItem.AutoFree;
        Rec.CurItem.DecFreeRefCount;

        AcquireLock;
      end
    until not GotWork;
    CheckFlushFinished;
    ReleaseLock;
  end;
end;

function TWorkFarm.AddWorkItem(Item: TWorkItem): boolean;
begin
  AcquireLock;
  result := FQueue.AddWorkItem(Item);
  CheckWorkAvail;
  ReleaseLock;
end;

function TWorkFarm.AddWorkItemBatch(PItem0: PWorkItem; Count: integer): boolean;
begin
  AcquireLock;
  result := FQueue.AddWorkItemBatch(PItem0, Count);
  CheckWorkAvail;
  ReleaseLock;
end;

constructor TWorkFarm.Create;
{$IF Defined(MSWINDOWS)}
var
  SysInfo: TSystemInfo;
{$ENDIF}
begin
  inherited;
  FLock := TCriticalSection.Create;
  FQueue := TWorkQueue.Create;
  FWorkAvail := TEvent.Create(nil, true, false, '');
  FThreadsQuitting := TEvent.Create(nil, true, false, '');
  FFlushFinished := TEvent.Create(nil, true, false, '');
{$IF Defined(MSWINDOWS)}
  FPriority := tpNormal;
  GetSystemInfo(SysInfo);
  SetThreadCount(SysInfo.dwNumberOfProcessors);
{$ELSE}
  //TODO - find out how to determine # of CPU's on an appropriate platform.
  FPriority := 0;
  SetThreadCount(8);
{$ENDIF}
end;

destructor TWorkFarm.Destroy;
begin
  WindupThreads;
  FWorkAvail.Free;
  FThreadsQuitting.Free;
  FFlushFinished.Free;
  FQueue.Free;
  FLock.Free;
  inherited;
end;

function TWorkFarm.GetThreadCount: integer;
begin
  result := Length(FWorkers);
end;

procedure TWorkFarm.WindupThreads;
var
  idx: integer;
{$IFOPT C+}
  ret2: TWaitResult;
{$ENDIF}
begin
  StartFlush(true);
{$IFOPT C+}
  ret2 := FFlushFinished.WaitFor(INFINITE);
  Assert(ret2 = wrSignaled);
{$ELSE}
  FFlushFinished.WaitFor(INFINITE);
{$ENDIF}
  FThreadsQuitting.SetEvent;
  for idx := 0 to High(FWorkers) do
  begin
    FWorkers[idx].Thread.WaitFor;
    FWorkers[idx].Thread.Free;
    Assert(not Assigned(FWorkers[idx].CurItem));
  end;
  SetLength(FWorkers, 0);
  FThreadsQuitting.ResetEvent;
end;

//InitThreadRec Added or changed for NUMA / Group support (Chapter 8).
procedure TWorkFarm.InitThreadRec(var Rec: TWorkerThreadRec);
begin
  Rec.Farm := self;
  Rec.Thread := TWorkerThread.Create(true);
  Rec.Thread.Priority := FPriority;
  Rec.Thread.ThreadRec := @Rec;
  Rec.CurItem := nil;
end;

procedure TWorkFarm.SetThreadCount(NewCount: integer);
var
  idx: integer;
{$IFOPT C+}
  rval: boolean;
{$ENDIF}
begin
  if NewCount <= 0 then
  begin
    Assert(false);
    exit;
  end;
  WindupThreads;
  SetLength(FWorkers, NewCount);
  for idx := 0 to High(FWorkers) do
  begin
    InitThreadRec(FWorkers[idx]);
    FWorkers[idx].Thread.Start;
  end;
{$IFOPT C+}
  rval := ResetFinishedFlush;
  Assert(rval);
{$ELSE}
  ResetFinishedFlush;
{$ENDIF}
end;

procedure TWorkFarm.FlushAndWait(CancelWork: boolean);
{$IFOPT C+}
var
  ret: TWaitResult;
  ret2: boolean;
{$ENDIF}
begin
{$IFOPT C+}
  ret2 := StartFlush(CancelWork);
  Assert(ret2);
  ret := FFlushFinished.WaitFor(INFINITE);
  Assert(ret = wrSignaled);
  ret2 := ResetFinishedFlush;
  Assert(ret2);
{$ELSE}
  StartFlush(CancelWork);
  FFlushFinished.WaitFor(INFINITE);
  ResetFinishedFlush;
{$ENDIF}
end;

{$IF Defined(MSWINDOWS)}
function TWorkFarm.GetThreadPriority: TThreadPriority;
{$ELSEIF Defined(POSIX)}
function TWorkFarm.GetThreadPriority: integer;
{$ENDIF}
begin
  result := FPriority;
end;

{$IF Defined(MSWINDOWS)}
procedure TWorkFarm.SetThreadPriority(NewPriority: TThreadPriority);
{$ELSEIF Defined(POSIX)}
procedure TWorkFarm.SetThreadPriority(NewPriority: integer);
{$ENDIF}
var
  idx: integer;
begin
  FPriority := NewPriority;
  for idx := Low(FWorkers) to High(FWorkers) do
    FWorkers[idx].Thread.Priority := NewPriority;
end;

end.
