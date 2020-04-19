unit ServiceUnit;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Classes, Vcl.Graphics, Vcl.Controls, Vcl.SvcMgr, Vcl.Dialogs,
  IdBaseComponent, IdComponent, IdCustomTCPServer, IdCustomHTTPServer,
  IdHTTPServer;

type
  TMCH_HTTPService = class(TService)
    HTTPServer: TIdHTTPServer;
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
  MCH_HTTPService: TMCH_HTTPService;

implementation

{$R *.DFM}

procedure ServiceController(CtrlCode: DWord); stdcall;
begin
  MCH_HTTPService.Controller(CtrlCode);
end;

function TMCH_HTTPService.GetServiceController: TServiceController;
begin
  Result := ServiceController;
end;

procedure TMCH_HTTPService.ServiceShutdown(Sender: TService);
begin
  HTTPServer.Active := false;
end;

procedure TMCH_HTTPService.ServiceStart(Sender: TService; var Started: Boolean);
begin
  HTTPServer.AutoStartSession := true;
  HTTPServer.Active := true;
  Started := true;
end;

procedure TMCH_HTTPService.ServiceStop(Sender: TService; var Stopped: Boolean);
begin
  HTTPServer.Active := false;
  Stopped := true; //Don't worry about thread waits at the moment.
end;

end.
