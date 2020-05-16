unit SrvController;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Classes, Vcl.Graphics, Vcl.Controls, Vcl.SvcMgr, Vcl.Dialogs;

type
  TChkInService = class(TService)
    procedure ServiceStart(Sender: TService; var Started: Boolean);
    procedure ServiceStop(Sender: TService; var Stopped: Boolean);
    procedure ServiceShutdown(Sender: TService);
  private
    { Private declarations }
  public
    function GetServiceController: TServiceController; override;
    { Public declarations }
  end;

var
  ChkInService: TChkInService;

implementation

{$R *.DFM}

uses CheckInServiceCoOrdinator;

procedure ServiceController(CtrlCode: DWord); stdcall;
begin
  ChkInService.Controller(CtrlCode);
end;

function TChkInService.GetServiceController: TServiceController;
begin
  Result := ServiceController;
end;

procedure TChkInService.ServiceShutdown(Sender: TService);
begin
  CheckInSrvStop;
end;

procedure TChkInService.ServiceStart(Sender: TService; var Started: Boolean);
begin
  CheckInSrvStart;
end;

procedure TChkInService.ServiceStop(Sender: TService; var Stopped: Boolean);
begin
  CheckInSrvStop;
end;

end.
