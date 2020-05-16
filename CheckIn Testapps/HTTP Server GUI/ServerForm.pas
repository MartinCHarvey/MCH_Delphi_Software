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
    procedure Timer1Timer(Sender: TObject);
    procedure ClearBlacklistBtnClick(Sender: TObject);
  private
    { Private declarations }
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
  CheckInAppLogic, CheckInServiceCoOrdinator;

procedure TServerFrm.UpdateCtrls;
begin
  StartBtn.Enabled := not ChkInSrvStarted;
  StopBtn.Enabled := ChkInSrvStarted;
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
  CheckInServiceCoOrdinator.DoPeriodic;
end;

procedure TServerFrm.SetDispatcherState(Active: boolean);
begin
  if Active then
    CheckInSrvStart
  else
    CheckInSrvStop;
  UpdateCtrls;
end;


end.
