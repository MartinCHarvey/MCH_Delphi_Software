unit ExportOptsDialog;

interface

uses Windows, SysUtils, Classes, Graphics, Forms, Controls, StdCtrls,
  Buttons, ExtCtrls, MonRadioButton;

type
  TExportOptsDlg = class(TForm)
    OKBtn: TButton;
    CancelBtn: TButton;
    Bevel1: TBevel;
    SaveWindowBtn: TRadioButton;
    Render800Btn: TRadioButton;
    Render1024Btn: TRadioButton;
    RenderScreenBtn: TMonRadioButton;
    MakeDeskChk: TCheckBox;
    RenderCustomBtn: TRadioButton;
    Xres: TEdit;
    Label1: TLabel;
    YRes: TEdit;
    procedure FormCloseQuery(Sender: TObject; var CanClose: Boolean);
    procedure FormShow(Sender: TObject);
    procedure RenderScreenBtnCheckedChange(Sender: TObject);
  private
    { Private declarations }
    FInitRenderScreenCaption: string;
  public
    { Public declarations }
    RenderSize: TPoint;
    JustSave: boolean;
    ReRender: boolean;
    MakeDesktop: boolean;
  end;

var
  ExportOptsDlg: TExportOptsDlg;

implementation

{$R *.DFM}

procedure TExportOptsDlg.FormCloseQuery(Sender: TObject;
  var CanClose: Boolean);
begin
  if ModalResult <> mrOk then
  begin
    CanClose := true;
    exit;
  end;
  RenderSize.x := Screen.Width;
  RenderSize.y := Screen.Height;
  if SaveWindowBtn.Checked then
  begin
    JustSave := true;
    ReRender := false;
  end
  else
  begin
    JustSave := false;
    ReRender := true;
    if Render800Btn.Checked then
    begin
      RenderSize.x := 800;
      RenderSize.y := 600;
    end
    else if Render1024Btn.Checked then
    begin
      RenderSize.x := 1024;
      RenderSize.y := 768;
    end
    else if RenderCustomBtn.Checked then
    begin
      RenderSize.x := StrToInt(XRes.Text);
      RenderSize.y := StrToInt(YRes.Text);
    end
    { else RenderScreenBtn.Checked }
  end;
  MakeDesktop := MakeDeskChk.Checked;
end;

procedure TExportOptsDlg.FormShow(Sender: TObject);
begin
  XRes.Text := IntToStr(Screen.Width);
  YRes.Text := IntToStr(Screen.Height);
  if Length(FInitRenderScreenCaption) = 0 then
    FInitRenderScreenCaption := RenderScreenBtn.Caption;
  RenderScreenBtn.Caption := FInitRenderScreenCaption
    + ' (' + IntToStr(Screen.Width) + 'x' + IntToStr(Screen.Height) + ')';
end;

procedure TExportOptsDlg.RenderScreenBtnCheckedChange(Sender: TObject);
begin
  MakeDeskChk.Enabled := RenderScreenBtn.Checked;
  if not MakeDeskChk.Enabled then
    MakeDeskChk.Checked := false;
end;

end.

