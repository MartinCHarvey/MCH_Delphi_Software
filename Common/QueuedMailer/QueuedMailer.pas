unit QueuedMailer;
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

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SyncObjs, DLThreadQueue, IdMessage, CommonPool, IdExplicitTLSClientServerBase;

  //TODO - Persisting items in mail queue on shutdown, currently
  //they just get thrown in the bin. Wait for mail sends to complete,
  //in which case batching strategy needs changing?

  //TODO - Some kind of feedback indication for mails sent / not sent /
  //connection error.

type
{$IFDEF USE_TRACKABLES}
  TQueuedMailer = class(TTrackable)
{$ELSE}
  TQueuedMailer = class
{$ENDIF}
  private
    FQuitFlag: boolean;
    FWorkItemCount: integer;
    FStateLock: TCriticalSection;
    FMailQueue: TDLProxyThreadQueuePublicLock; //Queue of IdMessages.
    FCommonPoolRec: TClientRec;

    FServer: string;
    FPort: integer;
    FUsername: string;
    FPassword: string;
    FUseTLS: TIdUseTLS;
  protected
    function GetMailQueueOccupancy: boolean;
    function QueueEmail(Mail: TIdMessage): boolean;
    procedure CondStartWorkItemLocked;
    procedure PoolOKCompletion(Sender: TObject);
    procedure PoolCancelCompletion(Sender: TObject);
    procedure WorkInThread;
  public
    constructor Create;
    destructor Destroy; override;

    property Server: string read FServer write FServer;
    property Port: integer read FPort write FPort;
    property Username: string read FUsername write FUserName;
    property Password: string read FPassword write FPassword;
    property UseTLS: TIdUseTLS read FUseTLS write FUseTLS;
    property MailsInQueue: boolean read GetMailQueueOccupancy;
  end;

  TQueuedMailerWorkItem = class(TCommonPoolWorkItem)
  private
    FMailer: TQueuedMailer;
  protected
    function DoWork:integer; override;
  end;

implementation

uses
  SysUtils, GlobalLog, IdSMTP, IdSSLOpenSSL, IdIOHandler, IdIOHandlerStack;

const
  MailsPerSession = 128;
  S_MAILER_EXCEPTION = 'Exception sending e-mail: ';

{ TQueuedMailer }

function TQueuedMailer.QueueEmail(Mail: TIdMessage): boolean;
begin
  if not FQuitFlag then
  begin
    FMailQueue.AcquireLock;
    try
      result := Assigned(FMailQueue.AddTailobj(Mail));
      CondStartWorkItemLocked;
    finally
      FMailQueue.ReleaseLock;
    end;
  end
  else
    result := false;
end;

function TQueuedMailer.GetMailQueueOccupancy: boolean;
begin
  FMailQueue.AcquireLock;
  try
    result := FMailQueue.Count > 0;
  finally
    FMailQueue.ReleaseLock;
  end;
end;

procedure TQueuedMailer.CondStartWorkItemLocked;
var
  WorkItem: TQueuedMailerWorkItem;
begin
  if (FWorkItemCount = 0) and not FQuitFlag then
  begin
    if FMailQueue.Count > 0 then
    begin
      WorkItem := TQueuedMailerWorkItem.Create;
      WorkItem.CanAutoFree := true;
      WorkItem.CanAutoReset := false;

      WorkItem.FMailer := self;
      if GCommonPool.AddWorkItem(FCommonPoolRec, WorkItem) then
        Inc(FWorkItemCount)
      else
        WorkItem.Free;
    end;
  end;
end;

procedure TQueuedMailer.PoolOKCompletion(Sender: TObject);
begin
  FMailQueue.AcquireLock;
  try
    Dec(FWorkItemCount);
    Assert(FWorkItemCount >= 0);
    CondStartWorkItemLocked;
  finally
    FMailQueue.ReleaseLock;
  end;
end;

procedure TQueuedMailer.PoolCancelCompletion(Sender: TObject);
begin
  FMailQueue.AcquireLock;
  try
    Dec(FWorkItemCount);
    Assert(FWorkItemCount >= 0);
    //We don't even cancel workitems manually, so this
    //is the app close / pool quit case.
  finally
    FMailQueue.ReleaseLock;
  end;
end;

constructor TQueuedMailer.Create;
begin
  inherited;
  FStateLock := TCriticalSection.Create;
  FMailQueue := TDLProxyThreadQueuePublicLock.Create;
  FMailQueue.Lock := FStateLock;
  FCommonPoolRec := GCommonPool.RegisterClient(self, PoolOkCompletion, PoolCancelCompletion);
end;

destructor TQueuedMailer.Destroy;
var
  Obj: TObject;
begin
  FQuitFlag := true;

  //This waits on oustanding workitems, but we have
  //items in queue which do not yet have a workitem queued.
  GCommonPool.DeRegisterClient(FCommonPoolRec);

  FMailQueue.AcquireLock;
  try
    Obj := FMailQueue.RemoveHeadObj;
    while Assigned(Obj) do
    begin
      Obj.Free;
      Obj := FMailQueue.RemoveHeadObj;
    end;
  finally
    FMailQueue.ReleaseLock;
  end;
  FMailQueue.Lock := nil;
  FMailQueue.Free;
  FStateLock.Free;
  inherited;
end;

procedure TQueuedMailer.WorkInThread;
var
  MailsLeft: integer;
  SMTP: TIdSMTP;
  Msg: TIdMessage;
begin
  if not FQuitFlag then
  begin
    MailsLeft := MailsPerSession;
    SMTP := TIDSMTP.Create(nil);
    try
      if FUseTLS <> utNoTLSSupport then
      begin
        SMTP.CreateIoHandler(TIdIoHandlerClass(TIdSSLIOHandlerSocketOpenSSL));
        with SMTP.IOHandler as TIdSSLIOHandlerSocketOpenSSL do
        begin
          SSLOptions.Method := sslvTLSv1_2;
          SSLOptions.SSLVersions := [sslvTLSv1, sslvTLSv1_1, sslvTLSv1_2];
        end;
      end;
      SMTP.UseTLS := FUseTLS;

      SMTP.Username := FUserName;
      SMTP.Password := FPassword;
      SMTP.Host := FServer;
      SMTP.Port := FPort;
      SMTP.Connect;
      while (not FQuitFlag) and (MailsLeft > 0) do
      begin
        FMailQueue.AcquireLock;
        try
          Msg := FMailQueue.RemoveHeadObj as TIdMessage;
        finally
          FMailQueue.ReleaseLock;
        end;
        try
          if Assigned(Msg) then
          begin
            SMTP.Send(Msg);
            Dec(MailsLeft);
          end
          else
            MailsLeft := 0;
        finally
          Msg.Free;
        end;
      end;
    except
      on E:Exception do
      begin
        GLogLog(SV_FAIL, S_MAILER_EXCEPTION + E.ClassName + ' ' + E.Message);
        //At this point, prevent further e-mail sending so we can debug.
        //TODO - Consider retry stratgies in future.
        FQuitFlag := true;
      end;
    end;
    SMTP.Free;
  end;
end;

{ TQueuedMailerWorkItem }

function TQueuedMailerWorkItem.DoWork:integer;
begin
  FMailer.WorkInThread;
  result := 0;
end;

initialization
  TIdSSLIOHandlerSocketOpenSSL.SetDefaultClass;
  TIdIOHandlerStack.SetDefaultClass;
end.
