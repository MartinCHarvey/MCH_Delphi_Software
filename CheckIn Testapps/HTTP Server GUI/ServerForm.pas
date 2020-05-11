unit ServerForm;

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, IdBaseComponent, IdComponent, IdCustomTCPServer,
  IdCustomHTTPServer, IdHTTPServer, IdContext, FMX.Layouts, FMX.Memo,
  HTTPServerDispatcher;

type
  TServerFrm = class(TForm)
    StartBtn: TButton;
    StopBtn: TButton;
    LogMemo: TMemo;
    Timer1: TTimer;
    ClearBlacklistBtn: TButton;
    procedure FormShow(Sender: TObject);
    procedure StartBtnClick(Sender: TObject);
    procedure StopBtnClick(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure Timer1Timer(Sender: TObject);
    procedure ClearBlacklistBtnClick(Sender: TObject);
  private
    { Private declarations }
    Dispatcher: THTTPServerDispatcher;
    procedure UpdateCtrls;
    procedure SetDispatcherState(Active: boolean);
  public
    { Public declarations }
  end;

var
  ServerFrm: TServerFrm;

implementation

{$R *.fmx}

uses
  IdSchedulerOfThreadpool, HTTPServerPageProducer,
  CheckInPageProducer, CheckInAppLogic, CheckInMailer, CheckInAudit;

procedure TServerFrm.SetDispatcherState(Active: boolean);
var
  PrevState: boolean;
  i: integer;

const
  RetryMs = 100;
  RetryTimes = 50;

begin
  PrevState := Dispatcher.Active;
  Dispatcher.Active := Active;
  if Active <> PrevState then
  begin
    if Active then
      GCheckInMailer.SendAdminMessage(amtServerUp)
    else
    begin
      GCheckInMailer.SendAdminMessage(amtServerDown);
      //Give the mailer thread 5 seconds to send any remaining messages including
      //this one.
      i := RetryTimes;
      while GCheckInMailer.MailsInQueue and (i > 0) do
      begin
        Sleep(RetryMs);
        Dec(i);
      end;
    end;
    UpdateCtrls;
    if Active then
      LogMemo.Lines.Add('Server now active.')
    else
      LogMemo.Lines.Add('Server stopped.');
  end;
end;


procedure TServerFrm.UpdateCtrls;
begin
  StartBtn.Enabled := not Dispatcher.Active;
  StopBtn.Enabled := Dispatcher.Active;
end;

procedure TServerFrm.ClearBlacklistBtnClick(Sender: TObject);
begin
  if GCheckInApp.ClearBlacklist then
    LogMemo.Lines.Add('Blacklist cleared.')
  else
    LogMemo.Lines.Add('Could not clear blacklist.');
end;

procedure TServerFrm.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  SetDispatcherState(False);
end;

procedure TServerFrm.FormCreate(Sender: TObject);
begin
  Dispatcher := THTTPServerDispatcher.Create;
  Dispatcher.SessionClass := TPageProducerSession;
  Dispatcher.LogonInfoClass := TCheckInLogonInfo;
  Dispatcher.PageProducerClass := TCheckInPageProducer;
end;

procedure TServerFrm.FormDestroy(Sender: TObject);
begin
  Dispatcher.Free;
end;

procedure TServerFrm.FormShow(Sender: TObject);
begin
  UpdateCtrls;
end;

procedure TServerFrm.StartBtnClick(Sender: TObject);
begin
  SetDispatcherState(true);
end;

procedure TServerFrm.StopBtnClick(Sender: TObject);
begin
  SetDispatcherState(false);
end;

procedure TServerFrm.Timer1Timer(Sender: TObject);
begin
  GCheckInApp.DoPeriodic;
  GAuditLog.DoPeriodic;
end;

end.
