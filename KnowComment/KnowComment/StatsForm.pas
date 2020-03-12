unit StatsForm;
{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, FMX.Layouts, FMX.Memo, BatchLoader;

type
  TStatsFrm = class(TForm)
    Label1: TLabel;
    Label2: TLabel;
    Label3: TLabel;
    Label4: TLabel;
    Label5: TLabel;
    Label6: TLabel;
    QueueMemo: TMemo;
    StopBtn: TButton;
    CloseBtn: TButton;
    ProgressCountLbl: TLabel;
    CurrentTaskLbl: TLabel;
    UProfCountLbl: TLabel;
    MiCountLbl: TLabel;
    CiCountLbl: TLabel;
    RefreshTimer: TTimer;
    Label7: TLabel;
    NewProfCountLbl: TLabel;
    ErrLstMemo: TMemo;
    Label8: TLabel;
    ErrClearBtn: TButton;
    DumpDbgBtn: TButton;
    procedure RefreshTimerTimer(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure StopBtnClick(Sender: TObject);
    procedure CloseBtnClick(Sender: TObject);
    procedure ErrClearBtnClick(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure FormHide(Sender: TObject);
    procedure DumpDbgBtnClick(Sender: TObject);
  private
    { Private declarations }
    function StringListsSame(A,B: TStrings): boolean;
  public
    { Public declarations }
    procedure HandleBatchLoaderCompletion(Sender: TObject; MultiOp: TMultiOp);
  end;

var
  StatsFrm: TStatsFrm;

implementation

{$R *.fmx}

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  DBContainer;

function TStatsFrm.StringListsSame(A,B: TStrings): boolean;
var
  Idx: integer;
begin
  result := A.Count = B.Count;
  if result then
  begin
    for Idx := 0 to Pred(A.Count) do
    begin
      if A.Strings[Idx] <> B.Strings[Idx] then
      begin
        result := false;
        break;
      end;
    end;
  end;
end;

procedure TStatsFrm.CloseBtnClick(Sender: TObject);
begin
  Close;
end;

procedure TStatsFrm.DumpDbgBtnClick(Sender: TObject);
{$IFDEF USE_TRACKABLES}
var
  MemUsage: TStrings;
{$ENDIF}
begin
{$IFDEF USE_TRACKABLES}
  MemUsage := AppGlobalTracker.GetTrackedMemUsage;
  try
    ErrLstMemo.Lines.AddStrings(MemUsage);
  finally
    MemUsage.Free;
  end;
{$ELSE}
  ErrLstMemo.Lines.Add('Mem usage info available if USE_TRACKABLES defined.')
{$ENDIF}
end;

procedure TStatsFrm.ErrClearBtnClick(Sender: TObject);
begin
  ErrLstMemo.Lines.Clear;
end;

procedure TStatsFrm.FormCreate(Sender: TObject);
begin
{$IFOPT C+}
  DumpDbgBtn.Enabled := true;
  DumpDbgBtn.Visible := true;
{$ELSE}
  DumpDbgBtn.Enabled := false;
  DumpDbgBtn.Visible := false;
{$ENDIF}
  RefreshTimerTimer(self);
end;

procedure TStatsFrm.FormHide(Sender: TObject);
begin
  RefreshTimer.Enabled := false;
end;

procedure TStatsFrm.FormShow(Sender: TObject);
begin
  RefreshTimer.Enabled := true;
  RefreshTimerTimer(Sender);
end;

procedure TStatsFrm.HandleBatchLoaderCompletion(Sender: TObject; MultiOp: TMultiOp);
begin
  if not MultiOp.OK then
    ErrLstMemo.Lines.Add(DateTimeToStr(Now) + ' '
      + MultiOp.GetOpDetails + ' ' + MultiOp.Msg);
end;

procedure TStatsFrm.RefreshTimerTimer(Sender: TObject);
var
  idx: integer;
  Item: TFMXObject;
  Stats: TBatchLoaderStats;
  QueueStrings: TStringList;
begin
  if Assigned(DBCont) and Assigned(DBCont.BatchLoader) then
  begin
    DBCont.BatchLoader.GetStats(Stats);
    ProgressCountLbl.Text := IntToStr(Stats.MultiOps);
    StopBtn.Enabled := Stats.MultiOps > 0;
    if Stats.MultiOps > 0 then
    begin
      QueueStrings := DBCont.BatchLoader.GetQueuedOpDetails;
      try
        Assert(QueueStrings.Count = Stats.MultiOps);
        CurrentTaskLbl.Text := QueueStrings[0];
        QueueStrings.Delete(0);
        UProfCountLbl.Text := IntToStr(Stats.UserProfilesChanged);
        MiCountLbl.Text := IntToStr(Stats.MediaItemsChanged);
        CiCountLbl.Text := IntToStr(Stats.CommentItemsChanged);
        NewProfCountLbl.Text := IntToStr(Stats.NewProfilesDiscovered);
        if not StringListsSame(QueueMemo.Lines, QueueStrings) then
          QueueMemo.Lines.Assign(QueueStrings);
      finally
        QueueStrings.Free;
      end;
    end
    else
    begin
      CurrentTaskLbl.Text := '';
      UProfCountLbl.Text := '';
      MiCountLbl.Text := '';
      CiCountLbl.Text := '';
      NewProfCountLbl.Text := '';
      QueueMemo.Lines.Clear;
    end;
  end
  else
  begin
    for idx := 0 to Pred(Children.Count) do
    begin
      Item := Children.Items[idx];
      if (Item is TLabel) or (Item is TMemo)
        or (Item = StopBtn) then
        (Item as TControl).Enabled := false;
    end;
  end;
end;

procedure TStatsFrm.StopBtnClick(Sender: TObject);
begin
  if Assigned(DBCont) and Assigned(DBCont.BatchLoader) then
    DBCont.BatchLoader.Stop;
end;

end.
