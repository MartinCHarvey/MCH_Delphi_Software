unit FractAboutDialog;

interface

uses Windows, SysUtils, Classes, Graphics, Forms, Controls, StdCtrls,
  Buttons, ExtCtrls;

type
  TFractAboutDlg = class(TForm)
    Bevel1: TBevel;
    Label1: TLabel;
    VerLbl: TLabel;
    Label2: TLabel;
    procedure FormShow(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  FractAboutDlg: TFractAboutDlg;

implementation

uses AppVersionStr;

{$R *.DFM}

procedure TFractAboutDlg.FormShow(Sender: TObject);
begin
  VerLbl.Caption := GetAppVerStr;
end;

end.
