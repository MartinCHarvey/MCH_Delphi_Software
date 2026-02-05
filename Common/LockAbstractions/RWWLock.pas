unit RWWLock;

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

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SysUtils, SyncObjs;

type
  TRWWLockReason = (lrSharedRead, lrSharedWrite, lrExclusiveWrite);

  TRWWLockState = (lsIdle,
                  lsSharedRead,
                  lsSharedReadExit,
                  lsSharedWrite,
                  lsSharedWriteExit,
                  lsExclusiveWrite,
                  lsExclusiveWriteExit);

  TRWThreadCounts = record
    Active, Waiting, Admitted: Uint32;
  end;

  // All counts zero <=> lock state is idle.

  TRWCounts = array[TRWWLockReason] of TRWThreadCounts;

  TRWWaits = array[TRWWLockReason] of TEvent;

  TRWRatios = array[TRWWLockReason] of Uint32;

  //This lock is fair, but it does let threads through in batches, and
  //wait for the previous batch to finish before re-assessing whether
  //to let another batch through, or change mode.

{$IFDEF USE_TRACKABLES}
  TRWWLock = class(TTrackable)
{$ELSE}
  TRWWLock = class
{$ENDIF}
  private
    FCrit: TCriticalSection;
    FState: TRWWLockState;
    FCounts: TRWCounts;
    FThreadWaits: TRWWaits;
    FDestroyed: TEvent;
    FRatios: TRWRatios;
    FDestroy: boolean;

    FExclusiveCrit: TCriticalSection;
  protected
    function CanReturnIdle: boolean;
    function CalcNextState(Reason: TRWWLockReason; var NewReason:TRWWLockReason): TRWWLockState;
    function OnlyActiveAdmittedCurReason(Reason: TRWWLockReason): boolean;

    function GetRatio(Reason:TRWWLockReason):Uint32;
    procedure SetRatio(Reason:TRWWLockReason; Val: Uint32);
  public
    constructor Create;
    destructor Destroy; override;

    procedure Acquire(Reason: TRWWLockReason);
    function TryAcquire(Reason: TRWWLockReason): boolean;
    procedure Release(Reason: TRWWLockReason);

    property Ratios[Reason: TRWWLockReason]:Uint32 read GetRatio write SetRatio;
  end;

function IncRWWReason(Reason: TRWWLockReason): TRWWLockReason;

implementation

function ActiveState(Reason: TRWWLockReason): TRWWLockState;
begin
  case Reason of
    lrSharedRead: result := lsSharedRead;
    lrSharedWrite: result := lsSharedWrite;
    lrExclusiveWrite: result := lsExclusiveWrite;
  else
    Assert(false);
    result := TRWWLockState(-1);
  end;
end;

function ExitState(Reason: TRWWLockReason): TRWWLockState;
begin
  case Reason of
    lrSharedRead: result := lsSharedReadExit;
    lrSharedWrite: result := lsSharedWriteExit;
    lrExclusiveWrite: result := lsExclusiveWriteExit;
  else
    Assert(false);
    result := TRWWLockState(-1);
  end;
end;

constructor TRWWLock.Create;
var
  Reason: TRWWLockReason;
begin
  inherited;
  FCrit := TCriticalSection.Create;
  FState := lsIdle;
  FillChar(FCounts, sizeof(FCounts), 0);
  FDestroyed := TEvent.Create(nil, true, false, '');
  for Reason := Low(Reason) to High(Reason) do
    FThreadWaits[Reason] := TEvent.Create(nil, true, false, '');
  FExclusiveCrit := TCriticalSection.Create;
  FRatios[lrSharedRead] := 64;
  FRatios[lrSharedWrite] := 8;
  FRatios[lrExclusiveWrite] := 1;
end;

destructor TRWWLock.Destroy;
var
  DoWait: boolean;
  Reason: TRWWLockReason;
begin
  FCrit.Acquire;
  try
    DoWait := FState <> lsIdle;
    FDestroy := true;
    for Reason := Low(Reason) to High(Reason) do
      FThreadWaits[Reason].SetEvent;
  finally
    FCrit.Release;
  end;
  if DoWait then
    FDestroyed.WaitFor(INFINITE);
  Assert(CanReturnIdle);
  //Now fully destroyed.
  for Reason := Low(Reason) to High(Reason) do
    FThreadWaits[Reason].Free;
  FDestroyed.Free;
  FCrit.Free;
  FExclusiveCrit.Free;
  inherited;
end;

function TRWWLock.GetRatio(Reason:TRWWLockReason):Uint32;
begin
  FCrit.Acquire;
  try
    result := FRatios[Reason];
  finally
    FCrit.Release;
  end;
end;

procedure TRWWLock.SetRatio(Reason:TRWWLockReason; Val: Uint32);
begin
  if Val = 0 then
    raise Exception.Create('RWW Lock ratio must be > 0');
  FCrit.Acquire;
  try
    FRatios[Reason] := Val;
  finally
    FCrit.Release;
  end;
end;

function TRWWLock.CanReturnIdle:boolean;
var
   Reason: TRWWLockReason;
begin
  result := true;
  for Reason := Low(Reason) to High(Reason) do
  begin
    result := result and (FCounts[Reason].Active = 0)
      and (FCounts[Reason].Waiting = 0);
  end;
end;

procedure TRWWLock.Acquire(Reason: TRWWLockReason);
begin
  if not TryAcquire(Reason) then
    raise Exception.Create('RWW Lock is being destroyed');
end;

function TRWWLock.TryAcquire(Reason: TRWWLockReason): boolean;
var
  Waiting, Acquired: boolean;
begin
  //Acquired := false;
  Waiting := false;

  repeat
    FCrit.Acquire;
    try
      if Waiting then
      begin
        Assert(FCounts[Reason].Waiting > 0);
        Dec(FCounts[Reason].Waiting);
      end;

      if FDestroy then
      begin
        if FState <> lsIdle then
        begin
          if CanReturnIdle then
          begin
            FState := lsIdle;
            FDestroyed.SetEvent;
          end;
        end;
        result := false;
        exit;
      end;

      if FState = lsIdle then
      begin
        Assert(CanReturnIdle); //Check all counts zero as expected.
        FState := ActiveState(Reason);
        FThreadWaits[Reason].SetEvent;
        //Can immediately lock with given reason,
        //counts are zero.
      end;

      //If in active or exit states for this reason, check
      //no active anywhere else.
      Assert((FState <> ActiveState(Reason))
        or (FState <> ExitState(Reason))
        or (OnlyActiveAdmittedCurReason(Reason)));

      if FState = ActiveState(Reason) then
      begin
        Waiting := false;
        Acquired := true;
        Inc(FCounts[Reason].Active);
        Inc(FCounts[Reason].Admitted);
        if FCounts[Reason].Admitted = FRatios[Reason] then
        begin
          FState := ExitState(Reason);
          FThreadWaits[Reason].ResetEvent;
          //Will block all incoming thread before
          //all of current batch have finished,
          //but allow this thread to continue, so we get teardown logic
          //at release time to re-init or change states.
        end;
      end
      else
      begin
        Waiting := true;
        Acquired := false;
        Inc(FCounts[Reason].Waiting);
      end;
    finally
      FCrit.Release;
    end;
    if Waiting then
      FThreadWaits[Reason].WaitFor(INFINITE);
  until Acquired;
  result := true;
  if Reason = lrExclusiveWrite then
    FExclusiveCrit.Acquire;
end;

function TRWWLock.OnlyActiveAdmittedCurReason(Reason: TRWWLockReason): boolean;
var
  ReasonIter: TRWWLockReason;
begin
  for ReasonIter := Low(ReasonIter) to High(ReasonIter) do
  begin
    if ReasonIter <> Reason then
    begin
      if (FCounts[ReasonIter].Active <> 0)
        or (FCounts[ReasonIter].Admitted <> 0) then
      begin
        result := false;
        exit;
      end;
    end;
  end;
  result := true;
end;

function IncRWWReason(Reason: TRWWLockReason): TRWWLockReason;
begin
  case Reason of
    lrSharedRead: result := lrSharedWrite;
    lrSharedWrite: result := lrExclusiveWrite;
    lrExclusiveWrite: result := lrSharedRead;
  else
    Assert(false);
    result := TRWWLockReason(-1);
  end;
end;

function TRWWLock.CalcNextState(Reason: TRWWLockReason; var NewReason: TRWWLockReason): TRWWLockState;
var
  ReasonIter: TRWWLockReason;

begin
  //First of all, can't leave current state until active threads
  //in current state have finished.
  Assert((FState = ActiveState(Reason)) or (FState = ExitState(Reason)));
  NewReason := Reason;
  if FCounts[Reason].Active > 0 then
    result := FState
  else
  begin
    //Can leave current state.

    //Only have waiting for our reason if in the exit state for our reason.
    //else only threads in our state are active (there are none).

    //Hence ... OK to check any other threads waiting for some other reason.
    //Should leave current state if waiting threads for any other active state...
    ReasonIter := IncRWWReason(Reason);
    while ReasonIter <> Reason do
    begin
      if FCounts[ReasonIter].Waiting <> 0 then
      begin
        result := ActiveState(ReasonIter);
        NewReason := ReasonIter;
        exit;
      end;
      ReasonIter := IncRWWReason(ReasonIter);
    end;

    //However, if we have no waiting threads for any other state, and we have
    //gone into our own WaitExit state, and we have waiting threads,
    //then go back to our own active state... (otherwise things will grind to a halt).
    if FCounts[Reason].Waiting <> 0 then
    begin
      result := ActiveState(Reason);
      exit;
    end;
    //However, if nothing waiting at all, then back to idle.
    result := lsIdle;
  end;
end;

procedure TRWWLock.Release(Reason: TRWWLockReason);
var
  NewState: TRWWLockState;
  NewReason: TRWWLockReason;
begin
  if Reason = lrExclusiveWrite then
    FExclusiveCrit.Release;

  FCrit.Acquire;
  try
    Assert((FState = ActiveState(Reason)) or (FState = ExitState(Reason)));
    Assert(OnlyActiveAdmittedCurReason(Reason));

    Assert(FCounts[Reason].Active > 0);
    Dec(FCounts[Reason].Active);
    if FDestroy then
    begin
      if CanReturnIdle then
      begin
        FState := lsIdle;
        FDestroyed.SetEvent;
      end;
      exit;
    end;

    NewState := CalcNextState(Reason, NewReason);
    if NewState <> FState then
    begin
      //Leaving current active or exit state, event might already
      //be reset, but reset it anyway.
      Assert(FCounts[Reason].Active = 0);
      FCounts[Reason].Admitted := 0;

      if NewState = lsIdle then //To Idle.
      begin
        if FState = ActiveState(Reason) then
          FThreadWaits[Reason].ResetEvent; //Optimise - no reset needed from exit state.
        FState := NewState;
      end
      else if NewReason <> Reason then //To some other lock reason
      begin
        if FState = ActiveState(Reason) then
          FThreadWaits[Reason].ResetEvent; //Optimise - no reset needed from exit state.
        FState := NewState;
        FThreadWaits[NewReason].SetEvent;
      end
      else //Recycle back to active, same reason.
      begin
        Assert(NewState = ActiveState(Reason));
        Assert(FState = ExitState(Reason));
        FState := NewState;
        FThreadWaits[Reason].SetEvent;
      end;
    end;
  finally
    FCrit.Release;
  end;
end;


end.
