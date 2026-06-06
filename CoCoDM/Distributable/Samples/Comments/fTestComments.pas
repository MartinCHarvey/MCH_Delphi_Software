unit fTestComments;

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
  Comments;

type
  TfmTestComments = class(TForm)
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
    procedure btnVersionClick(Sender: TObject);
    procedure btnExecuteClick(Sender: TObject);
    procedure btnSaveClick(Sender: TObject);
    procedure btnOpenClick(Sender: TObject);
    procedure memSourceChange(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
  private
    FComments: TComments;
  public
    property Comments : TComments read FComments write FComments;
  end;

var
  fmTestComments: TfmTestComments;

implementation

{$R *.DFM}

procedure TfmTestComments.btnVersionClick(Sender: TObject);
begin
  MessageDlg('Comments - ' + Comments.VersionStr + #13#10
      + FormatDateTime(' (dddddd tt)',Comments.BuildDate)
      + #13#10#13#10 + Comments.VersionInfo,mtInformation,[mbOk],0);
end;

procedure TfmTestComments.FormCreate(Sender: TObject);
begin
  FComments := TComments.Create(nil);
end;

procedure TfmTestComments.FormDestroy(Sender: TObject);
begin
  if Assigned(FComments) then
    FComments.Free;
end;

procedure TfmTestComments.btnOpenClick(Sender: TObject);
begin
  if OpenDialog.Execute then
  begin
    memSource.Clear;
    memSource.Lines.LoadFromFile(OpenDialog.FileName);
  end;
end;

procedure TfmTestComments.btnSaveClick(Sender: TObject);
begin
  if SaveDialog.Execute then
    memSource.Lines.SaveToFile(SaveDialog.FileName);
end;

procedure TfmTestComments.btnExecuteClick(Sender: TObject);
begin
  memSource.Lines.SaveToStream(Comments.SourceStream);
  Comments.Execute;
  memOutput.Clear;
  memOutput.Lines.LoadFromStream(Comments.ListStream);
end;

procedure TfmTestComments.memSourceChange(Sender: TObject);
begin
  btnSave.Enabled := memSource.Text > '';
end;

end.    
