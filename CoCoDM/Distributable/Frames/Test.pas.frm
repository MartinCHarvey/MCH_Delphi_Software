unit fTest-->Grammar<--;

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
  -->Grammar<--;

type
  TfmTest-->Grammar<-- = class(TForm)
    pnlButtons: TPanel;
    pnlSource: TPanel;
    pnlOutput: TPanel;
    OpenDialog: TOpenDialog;
    SaveDialog: TSaveDialog;
    memSource: -->MemoType<--;
    memOutput: -->MemoType<--;
    splSourceOutput: TSplitter;
    btnOpen: TButton;
    btnSave: TButton;
    btnExecute: TButton;
    -->VersionBtnDec<--
    procedure btnExecuteClick(Sender: TObject);
    procedure btnSaveClick(Sender: TObject);
    procedure btnOpenClick(Sender: TObject);
    procedure memSourceChange(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
  private
    F-->Grammar<--: T-->Grammar<--;
  public
    property -->Grammar<-- : T-->Grammar<-- read F-->Grammar<-- write F-->Grammar<--;
  end;

var
  fmTest-->Grammar<--: TfmTest-->Grammar<--;

implementation

{$R *.DFM}

-->VersionBtn<--

procedure TfmTest-->Grammar<--.FormCreate(Sender: TObject);
begin
  F-->Grammar<-- := T-->Grammar<--.Create(nil);
end;

procedure TfmTest-->Grammar<--.FormDestroy(Sender: TObject);
begin
  if Assigned(F-->Grammar<--) then
    F-->Grammar<--.Free;
end;

procedure TfmTest-->Grammar<--.btnOpenClick(Sender: TObject);
begin
  if OpenDialog.Execute then
  begin
    memSource.Clear;
    memSource.Lines.LoadFromFile(OpenDialog.FileName);
  end;
end;

procedure TfmTest-->Grammar<--.btnSaveClick(Sender: TObject);
begin
  if SaveDialog.Execute then
    memSource.Lines.SaveToFile(SaveDialog.FileName);
end;

procedure TfmTest-->Grammar<--.btnExecuteClick(Sender: TObject);
begin
  memSource.Lines.SaveToStream(-->Grammar<--.SourceStream);
  -->Grammar<--.Execute;
  memOutput.Clear;
  memOutput.Lines.LoadFromStream(-->Grammar<--.ListStream);
end;

procedure TfmTest-->Grammar<--.memSourceChange(Sender: TObject);
begin
  btnSave.Enabled := memSource.Text > '';
end;

end.

