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
    procedure FormShow(Sender: TObject);
    procedure StartBtnClick(Sender: TObject);
    procedure StopBtnClick(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure FormCreate(Sender: TObject);
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
  CheckInPageProducer, CheckInAppLogic, CheckInMailer;

procedure TServerFrm.SetDispatcherState(Active: boolean);
var
  PrevState: boolean;
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
      Sleep(5000);
    end;
    UpdateCtrls;
  end;
end;


procedure TServerFrm.UpdateCtrls;
begin
  StartBtn.Enabled := not Dispatcher.Active;
  StopBtn.Enabled := Dispatcher.Active;
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

end.
