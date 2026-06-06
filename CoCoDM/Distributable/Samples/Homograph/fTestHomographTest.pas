unit fTestHomographTest;

interface

uses
  Classes,
  ComCtrls,
  Forms,
  Buttons,
  ExtCtrls,
  StdCtrls,
  Dialogs,
  Controls,
  SysUtils,
  HomographTest;

type
  TfmTestHomographTest = class(TForm)
    pnlButtons: TPanel;
    pnlSource: TPanel;
    pnlOutput: TPanel;
    OpenDialog: TOpenDialog;
    SaveDialog: TSaveDialog;
    memSource: TMemo;
    memOutput: TMemo;
    splSourceOutput: TSplitter;
    btnOpen: TButton;
    btnSave: TButton;
    btnExecute: TButton;
    btnVersion: TButton;
    chkUseHomograph: TCheckBox;
    procedure btnVersionClick(Sender: TObject);
    procedure btnExecuteClick(Sender: TObject);
    procedure btnSaveClick(Sender: TObject);
    procedure btnOpenClick(Sender: TObject);
    procedure memSourceChange(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure chkUseHomographClick(Sender: TObject);
  private
    FHomographTest: THomographTest;
  public
    property HomographTest : THomographTest read FHomographTest write FHomographTest;
  end;

var
  fmTestHomographTest: TfmTestHomographTest;

implementation

{$R *.DFM}

procedure TfmTestHomographTest.btnVersionClick(Sender: TObject);
begin
  MessageDlg('HomographTest - ' + HomographTest.Version
      + FormatDateTime(' (ddddd t)',HomographTest.BuildDate),mtInformation,[mbOk],0);
end;

procedure TfmTestHomographTest.FormCreate(Sender: TObject);
begin
  FHomographTest := THomographTest.Create(nil);
end;

procedure TfmTestHomographTest.FormDestroy(Sender: TObject);
begin
  if Assigned(FHomographTest) then
    FHomographTest.Free;
end;

procedure TfmTestHomographTest.btnOpenClick(Sender: TObject);
begin
  if OpenDialog.Execute then
  begin
    memSource.Clear;
    memSource.Lines.LoadFromFile(OpenDialog.FileName);
  end;
end;

procedure TfmTestHomographTest.btnSaveClick(Sender: TObject);
begin
  if SaveDialog.Execute then
    memSource.Lines.SaveToFile(SaveDialog.FileName);
end;

procedure TfmTestHomographTest.btnExecuteClick(Sender: TObject);
begin
  memSource.Lines.SaveToStream(HomographTest.SourceStream);
  HomographTest.Execute;
  memOutput.Clear;
  memOutput.Lines.LoadFromStream(HomographTest.ListStream);
end;

procedure TfmTestHomographTest.memSourceChange(Sender: TObject);
begin
  btnSave.Enabled := memSource.Text > '';
end;

procedure TfmTestHomographTest.chkUseHomographClick(Sender: TObject);
begin
  HomographTest.UseHomograph := chkUseHomograph.Checked;
end;

end.

