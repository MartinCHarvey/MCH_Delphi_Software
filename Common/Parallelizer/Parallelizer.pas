unit Parallelizer;
{

Copyright © 2024 Martin Harvey <martin_c_harvey@hotmail.com>

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
  SysUtils;

type
  TParallelHandler = function(Ref1, Ref2: pointer):pointer of object;
  TParallelHandlers = array of TParallelHandler;
  TPHRefs = array of pointer;

  TExceptionFilterAction = (
    tetaIgnore, //Try next handler in chain, might fall off end
    tetaSwallowException,
    tetaRecordException,
    tetaThrowOnceAllRecorded );

  //This function filters exceptions from threads.
  TExceptionFilter     = function(EClass: SysUtils.ExceptClass;
                                   EMsg: string): TExceptionFilterAction;
  //This function rethrows if it can.
  TExceptionTranslator = procedure(EClass: SysUtils.ExceptClass;
                                   EMsg: string);

  EParallelException = class(Exception);

  PExceptionHandlerChain = ^TExceptionHandlerChain;
  TExceptionHandlerChain = record
    FilterFunc: TExceptionFilter;
    XlatorFunc: TExceptionTranslator;
    Next: PExceptionHandlerChain;
  end;

  TPHExceptRecord = record
    HandledBy: PExceptionHandlerChain; //Actually just the immediate handler.
    EClass: SysUtils.ExceptClass;
    EMsg: string;
    RaiseFinally: boolean;
  end;

  TPHExcepts = array of TPHExceptRecord;
  TPHFinalAction = (
    //In order of importance
    tfaBadThrow, //Bad throw in exception handler mechanism.
    tfaUnhFwd, //Unhandled exception, forwarding
    tfaFilterFwd, //Filtered / handled exception, forwarding.
    tfaNone);

procedure ExecParallel(Handlers: TParallelHandlers;
                       const Refs1: TPHRefs;
                       const Refs2: TPHRefs;
                       var  Excepts: TPHExcepts;
                       var  Rets: TPHRefs;
                       const Xlators: PExceptionHandlerChain = nil);

function ParalleliserFilterCommonExceptions(EClass: SysUtils.ExceptClass;
                                             EMsg: string):TExceptionFilterAction;
procedure ParalleliserForwardCommonExceptions(EClass: SysUtils.ExceptClass;
                                             EMsg: string);

implementation

uses
  Classes
{$IFDEF USE_WINDOWS_LOCKS}
  , Windows
{$ENDIF}
  ;

const
  S_UNEXPECTED_WAIT_RET = 'Unexpected return from parallel wait: ';
  S_FWD_EXCEPTION = 'Unhandled exception forwarded: ';
  S_BAD_THROW_IN_FILTER = 'Exception filter itself raised: ';
  S_XLATOR_DID_NOT_RE_RAISE = 'Exception translator failed to re-raise: ';

type
  TParallelHandlerThread = class(TThread)
  private
  protected
    FRaised: boolean;
    FExceptClass:SysUtils.ExceptClass;
    FExceptMsg: string;

    FRef1, FRef2: pointer;
    FRet: pointer;
    FHandler: TParallelHandler;
  public
    procedure Execute; override;
  end;

  //Not same as SyncObjs THandleObjectArray,that's architecture astronautix.
  THandleArray = array of THandle;
  TThreadArray = array of TParallelHandlerThread;

procedure TParallelHandlerThread.Execute;
begin
  try
    FRet := FHandler(FRef1, FRef2);
  except
    on E: Exception do
    begin
      FRaised := true;
      FExceptClass := ExceptClass(E.ClassType);
      FExceptMsg := E.Message;
    end;
  end;
end;

function ParalleliserFilterCommonExceptions(EClass: SysUtils.ExceptClass;
                                             EMsg: string):TExceptionFilterAction;
begin
  while Assigned(EClass) do
  begin
    if (EClass = EOutOfMemory)
      or (EClass = EAccessViolation)
      or (EClass = EListError)
      or (EClass = EIntError)
      or (EClass = EMathError)
      or (EClass = EStackOverflow)
      or (EClass = EThread) then
    begin
        result := tetaThrowOnceAllRecorded;
        exit;
    end;
    EClass := ExceptClass(EClass.ClassParent);
  end;
  result := tetaIgnore;
end;

procedure ParalleliserForwardCommonExceptions(EClass: SysUtils.ExceptClass;
                                             EMsg: string);
begin
  //The EExternalError classes here are not going to have the full Win32
  //exception information, but this is good enough for the moment.
  while Assigned(EClass) do
  begin
    if (EClass = EOutOfMemory) then
      raise EOutOfMemory.Create(EMsg)
    else if (EClass = EAccessViolation) then
      raise EAccessViolation.Create(EMsg)
    else if (EClass = EListError) then
      raise EListError.Create(EMsg)
    else if (EClass = EIntError) then
      raise EIntError.Create(EMsg)
    else if (EClass = EMathError) then
      raise EMathError.Create(EMsg)
    else if (EClass = EStackOverflow) then
      raise EStackOverflow.Create(EMsg)
    else if (EClass = EThread) then
      raise EThread.Create(EMsg);
    EClass := ExceptClass(EClass.ClassParent);
  end;
end;


procedure ExecParallel(Handlers: TParallelHandlers;
                       const Refs1: TPHRefs;
                       const Refs2: TPHRefs;
                       var  Excepts: TPHExcepts;
                       var  Rets: TPHRefs;
                       const Xlators: PExceptionHandlerChain = nil);
var
  L, i: integer;
{$IFDEF USE_WINDOWS_LOCKS}
  BatchL, BatchOfs: integer;
{$ENDIF}
  Threads: TThreadArray;
{$IFDEF USE_WINDOWS_LOCKS}
  WaitHandles: THandleArray;
  WaitRet: DWORD;
{$ENDIF}
  PExcChain: PExceptionHandlerChain;
  FilterHandled: boolean;
  FilterRet: TExceptionFilterAction;
  FinalMsg: string;
  FinalAction: TPHFinalAction;

begin
  FinalAction := tfaNone;

  L := Length(Handlers);
  Assert(L > 0);
  Assert((Length(Refs1) = L) or (Length(Refs1) = 0));
  Assert((Length(Refs2) = L) or (Length(Refs2) = 0));
  SetLength(Excepts, L);
  for i := Low(Excepts) to High(Excepts) do
  begin
    Assert(not Assigned(Excepts[i].HandledBy));
    Assert(not Assigned(Excepts[i].EClass));
    Assert(Length(Excepts[i].EMsg) = 0);
  end;
  SetLength(Rets, L);
  PExcChain := XLators;
  while Assigned(PExcChain) do
  begin
    Assert(Assigned(PExcChain.FilterFunc));
    Assert(Assigned(PExcChain.XlatorFunc));
    PExcChain := PExcChain.Next;
  end;
{$IFDEF USE_WINDOWS_LOCKS}
  SetLength(WaitHandles, L);
{$ENDIF}
  SetLength(Threads, L);
  try
    for i := 0 to Pred(L) do
    begin
      Threads[i] := TParallelHandlerThread.Create(true);
      Threads[i].FreeOnTerminate := false;
{$IFDEF USE_WINDOWS_LOCKS}
      WaitHandles[i] := Threads[i].Handle;
{$ENDIF}
      with Threads[i] do
      begin
        if Length(Refs1) > 0 then
          FRef1 := Refs1[i];
        if Length(Refs2) > 0 then
          FRef2 := Refs2[i];
        FHandler := Handlers[i];
        Start;
      end;
    end;
{$IFDEF USE_WINDOWS_LOCKS}
    BatchOfs := 0;
    while BatchOfs < L do
    begin
      if (L - BatchOfs) < MAXIMUM_WAIT_OBJECTS then
        BatchL := (L - BatchOfs)
      else
        BatchL := MAXIMUM_WAIT_OBJECTS;
      WaitRet := Windows.WaitForMultipleObjects(BatchL, @WaitHandles[BatchOfs], true, INFINITE);
      if not ((WaitRet >= WAIT_OBJECT_0) and (WaitRet < (WAIT_OBJECT_0 + MAXIMUM_WAIT_OBJECTS))) then
        raise EParallelException.Create(S_UNEXPECTED_WAIT_RET + IntToStr(WaitRet));
      Inc(BatchOfs, BatchL);
    end;
{$ELSE}
    for i := 0 to Pred(L) do
      Threads[i].WaitFor;
{$ENDIF}
    for i := 0 to Pred(L) do
    begin
      if Threads[i].FRaised then
      begin
        PExcChain := Xlators;
        FilterHandled := false;
        while Assigned(PExcChain) and not FilterHandled do
        begin
          try
            FilterRet := PExcChain.FilterFunc(Threads[i].FExceptClass, Threads[i].FExceptMsg);
          except
            on E: Exception do
            begin
              if FinalAction > tfaBadThrow then
              begin
                FinalAction := tfaBadThrow;
                FinalMsg := S_BAD_THROW_IN_FILTER + E.ClassName + ' ' + E.Message;
                FilterRet := tetaSwallowException;
              end;
            end;
          end;
          case FilterRet of
            tetaSwallowException: FilterHandled := true;
            tetaRecordException: begin
              FilterHandled := true;
              Excepts[i].HandledBy := PExcChain;
              Excepts[i].EClass := Threads[i].FExceptClass;
              Excepts[i].EMsg := Threads[i].FExceptMsg;
              Excepts[i].RaiseFinally := false;
            end;
            tetaThrowOnceAllRecorded: begin
              FilterHandled := true;
              Excepts[i].HandledBy := PExcChain;
              Excepts[i].EClass := Threads[i].FExceptClass;
              Excepts[i].EMsg := Threads[i].FExceptMsg;
              Excepts[i].RaiseFinally := true;
              if FinalAction > tfaFilterFwd then
                FinalAction := tfaFilterFwd;
            end;
            tetaIgnore: { NOP } ;
          else
            {NOP}
          end;
          PExcChain := PExcChain.Next;
        end;
        if not FilterHandled then
        begin
          if FinalAction > tfaUnhFwd then
          begin
            FinalAction := tfaUnhFwd;
            FinalMsg := S_FWD_EXCEPTION + ' (' +
              Threads[i].FExceptClass.ClassName + ') ' + Threads[i].FExceptMsg;
          end;
        end;
      end;
      Rets[i] := Threads[i].FRet;
    end;
    case FinalAction of
      tfaBadThrow, tfaUnhFwd: raise EParallelException.Create(FinalMsg);
      tfaFilterFwd: begin
        //Expect translator / filter function to work together and re-throw the EClass
        //we recorded.
        for i := 0 to Pred(L) do
        begin
          if Assigned(Excepts[i].HandledBy) and (Excepts[i].RaiseFinally) then
          begin
            //Expect this function to re-raise, but if it doesn't we'll
            //raise that it didn't raise ...
            Excepts[i].HandledBy.XlatorFunc(Excepts[i].EClass, Excepts[i].EMsg);
            raise EParallelException.Create(S_XLATOR_DID_NOT_RE_RAISE
              + Excepts[i].EClass.ClassName + ' ' + Excepts[i].EMsg);
          end;
        end;
      end;
      tfaNone: {NOP};
    else
      { NOP }
    end;
  finally
    for i := 0 to Pred(L) do
      Threads[i].Free;
  end;
end;

end.
