unit CheckInServiceCoOrdinator;

interface

function ChkInSrvStarted: boolean;
procedure CheckInSrvStart;
procedure CheckInSrvStop;
procedure DoPeriodic;

implementation

uses
  CheckInMailer, CheckInAppLogic, CheckInAppConfig, CheckInAudit, SysUtils,
  HTTPServerDispatcher, HTTPServerPageProducer, CheckInPageProducer,
  GlobalLog, Classes, SyncObjs;

type
  TServicePeriodicThread = class(TThread)
  protected
    FPeriodicQuit: TEvent;
    procedure Execute; override;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Terminate;
  end;

var
  Started, Stopping: boolean;
  Dispatcher: THTTPServerDispatcher;
  PeriodicThread: TServicePeriodicThread;

{ Service periodic thread }

procedure TServicePeriodicThread.Terminate;
begin
  inherited;
  FPeriodicQuit.SetEvent;
end;

procedure TServicePeriodicThread.Execute;
begin
  while not Terminated do
  begin
    DoPeriodic;
    FPeriodicQuit.WaitFor(1000); //1 sec.
  end;
end;

constructor TServicePeriodicThread.Create;
begin
  inherited;
  FPeriodicQuit := TEvent.Create(nil, true, false, '');
end;

destructor TServicePeriodicThread.Destroy;
begin
  Terminate;
  WaitFor;
  FPeriodicQuit.Free;
  inherited;
end;


{ Misc functions }

function ChkInSrvStarted: boolean;
begin
  result := Started;
end;

procedure CheckInSrvStart;
begin
  if not Started then
  begin
    try
      GLogLog(SV_INFO, 'Starting');
      CheckInAppConfig.UnitInit;
      GLogLog(SV_INFO, 'AppConfig init Root dir setup as: ' + CheckInAppConfig.RootDir);
      CheckInAudit.UnitInit;
      GLogLog(SV_INFO, 'AuditInit');
      CheckInMailer.UnitInit;
      GLogLog(SV_INFO, 'MailerInit');
      CheckInAppLogic.UnitInit;
      GLogLog(SV_INFO, 'AppLogicInit');

      Dispatcher := THTTPServerDispatcher.Create;
      Dispatcher.SessionClass := TPageProducerSession;
      Dispatcher.LogonInfoClass := TCheckInLogonInfo;
      Dispatcher.PageProducerClass := TCheckInPageProducer;
      Dispatcher.Active := true;

      GLogLog(SV_INFO, 'Dispatcher running');

      GCheckInMailer.SendAdminMessage(amtServerUp);
      Started := true;

      PeriodicThread := TServicePeriodicThread.Create;

      GLogLog(SV_INFO, 'Started');
    except
      on E: Exception do
      begin
        GLogLog(SV_CRIT, E.ClassName + ' ' + E.Message);
        raise;
      end;
    end;
  end;
end;

procedure CheckInSrvStop;
var
  i: integer;

const
  RetryMs = 100;
  RetryTimes = 50;
begin
  if Started then
  begin
    Stopping := true;
    try
      try
        GLogLog(SV_INFO, 'Stopping');
        PeriodicThread.Terminate;
        PeriodicThread.WaitFor;

        Dispatcher.Free;
        Dispatcher := nil;

        GLogLog(SV_INFO, 'Dispatcher stopped');

        GCheckInMailer.SendAdminMessage(amtServerDown);
        //Give the mailer thread 5 seconds to send any remaining messages including
        //this one.
        i := RetryTimes;
        while GCheckInMailer.MailsInQueue and (i > 0) do
        begin
          Sleep(RetryMs);
          Dec(i);
        end;

        GLogLog(SV_INFO, 'Mailer stopped');

        CheckInAppLogic.UnitFini;
        GLogLog(SV_INFO, 'AppLogic Fini');
        CheckInMailer.UnitFini;
        GLogLog(SV_INFO, 'Mailer Fini');
        CheckInAudit.UnitFini;
        GLogLog(SV_INFO, 'Audit Fini');
        CheckInAppConfig.UnitFini;
        GLogLog(SV_INFO, 'Config Fini');

        PeriodicThread.Free;
        PeriodicThread := nil;
        Started := false;
        GLogLog(SV_INFO, 'Stopped');
      except
        on E: Exception do
        begin
          GLogLog(SV_CRIT, E.ClassName + ' ' + E.Message);
          raise;
        end;
      end;
    finally
      Stopping := false;
    end;
  end;
end;

procedure DoPeriodic;
begin
  if Started and not Stopping then
  begin
    GCheckInApp.DoPeriodic;
    GAuditLog.DoPeriodic;
  end;
end;
end.
