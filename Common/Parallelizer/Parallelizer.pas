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
  TPHExcepts = array of SysUtils.ExceptClass;
  TExceptionTranslator = function(EClass: SysUtils.ExceptClass;
                                   EMsg: string): boolean;
  EParallelException = class(Exception);

  PExceptionHandlerChain = ^TExceptionHandlerChain;
  TExceptionHandlerChain = record
    Func: TExceptionTranslator;
    Next: PExceptionHandlerChain;
  end;

procedure ExecParallel(Handlers: TParallelHandlers;
                       const Refs1: TPHRefs;
                       const Refs2: TPHRefs;
                       var  Excepts: TPHExcepts;
                       var  Rets: TPHRefs;
                       const Xlators: PExceptionHandlerChain = nil);

implementation

uses
  Classes, Windows;

const
  S_UNEXPECTED_WAIT_RET = 'Unexpected return from parallel wait: ';
  S_FWD_EXCEPTION = 'Exception forwaded between threads (untranslated): ';

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
  published
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


procedure ExecParallel(Handlers: TParallelHandlers;
                       const Refs1: TPHRefs;
                       const Refs2: TPHRefs;
                       var  Excepts: TPHExcepts;
                       var  Rets: TPHRefs;
                       const Xlators: PExceptionHandlerChain = nil);
var
  L, BatchL, BatchOfs, i: integer;
  Threads: TThreadArray;
  WaitHandles: THandleArray;
  WaitRet: DWORD;
  ExceptHandled: boolean;
  PExcChain: PExceptionHandlerChain;
begin
  L := Length(Handlers);
  Assert(L > 0);
  Assert((Length(Refs1) = L) or (Length(Refs1) = 0));
  Assert((Length(Refs2) = L) or (Length(Refs2) = 0));
  SetLength(Excepts, L);
  SetLength(Rets, L);
  SetLength(WaitHandles, L);
  SetLength(Threads, L);
  try
    for i := 0 to Pred(L) do
    begin
      Threads[i] := TParallelHandlerThread.Create(true);
      Threads[i].FreeOnTerminate := false;
      WaitHandles[i] := Threads[i].Handle;
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
    BatchOfs := 0;
    while BatchOfs < L do
    begin
      if L < MAXIMUM_WAIT_OBJECTS then
        BatchL := L
      else
        BatchL := MAXIMUM_WAIT_OBJECTS;
      WaitRet := Windows.WaitForMultipleObjects(BatchL, @WaitHandles[BatchOfs], true, INFINITE);
      if not ((WaitRet >= WAIT_OBJECT_0) and (WaitRet < (WAIT_OBJECT_0 + MAXIMUM_WAIT_OBJECTS))) then
        raise EParallelException.Create(S_UNEXPECTED_WAIT_RET + IntToStr(WaitRet));
      Inc(BatchOfs, BatchL);
    end;
    for i := 0 to Pred(L) do
    begin
      if Threads[i].FRaised then
      begin
        ExceptHandled := false;
        PExcChain := Xlators;
        while Assigned(PExcChain) do
        begin
          ExceptHandled := PExcChain.Func(Threads[i].FExceptClass, Threads[i].FExceptMsg);
          if ExceptHandled then
            break
          else
            PExcChain := PExcChain.Next;
        end;
        if not ExceptHandled then
          raise EParallelException.Create(S_FWD_EXCEPTION + ' (' +
            Threads[i].FExceptClass.ClassName + ') ' + Threads[i].FExceptMsg);
        Excepts[i] := Threads[i].FExceptClass;
      end;
      Rets[i] := Threads[i].FRet;
    end;
  finally
    for i := 0 to Pred(L) do
      Threads[i].Free;
  end;
end;

end.
