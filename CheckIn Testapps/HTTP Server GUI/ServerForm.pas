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
  public
    { Public declarations }
  end;

var
  ServerFrm: TServerFrm;

implementation

{$R *.fmx}

uses
  IdSchedulerOfThreadpool, HTTPServerPageProducer,
  CheckInPageProducer, CheckInAppLogic;

procedure TServerFrm.UpdateCtrls;
begin
  StartBtn.Enabled := not Dispatcher.Active;
  StopBtn.Enabled := Dispatcher.Active;
end;

procedure TServerFrm.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  Dispatcher.Active := false;
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
  Dispatcher.Active := true;
  UpdateCtrls;
end;

procedure TServerFrm.StopBtnClick(Sender: TObject);
begin
  Dispatcher.Active := false;
  UpdateCtrls;
end;

end.
