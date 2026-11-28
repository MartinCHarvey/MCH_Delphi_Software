unit TestForm;
{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, Forms, Dialogs,
  StdCtrls, WorkItems, LockAbstractions;

const
  WM_COMPLETION = WM_APP + 1;

type
  TWorkItemTestForm = class(TForm)
    CreateButton: TButton;
    NFlowBtn: TButton;
    CFlowBtn: TButton;
    procedure CreateButtonClick(Sender: TObject);
    procedure NFlowBtnClick(Sender: TObject);
    procedure CFlowBtnClick(Sender: TObject);
  private
    { Private declarations }
    Added, Completed, Cancelled, InFlight: integer;
    WorkFarm: TWorkFarm;
    procedure InitCountsAndFarm;
    procedure CheckFiniFarm;
    procedure SendItems;
    procedure HandlePotentialCompletion(var Message:TMessage);message WM_COMPLETION;
  public
    { Public declarations }
  end;

var
  WorkItemTestForm: TWorkItemTestForm;

implementation

{$R *.DFM}

type
  TTestWorkItem = class(TWorkItem)
  private
    FCompleteInt, FCancelInt, FInFlightInt: PInteger;
    FHandle: HWND;
  protected
   function  DoWork:integer; override;
   procedure DoNormalCompletion; override;
   procedure DoCancelledCompletion; override;
   procedure CompleteCommon;
  public
    constructor Create;
  end;

procedure TWorkItemTestForm.HandlePotentialCompletion(var Message: TMessage);
begin
  CheckFiniFarm;
end;

procedure TWorkItemTestForm.CreateButtonClick(Sender: TObject);
var
  WF: TWorkFarm;
begin
  WF := TWorkFarm.Create;
  WF.ThreadCount := 42;
  WF.Free;
end;

procedure TWorkItemTestForm.InitCountsAndFarm;
begin
  Assert(not Assigned(WorkFarm));
  WorkFarm := TWorkFarm.Create;
  Completed := 0;
  Cancelled := 0;
  Added := 0;
  NFlowBtn.Enabled := false;
  CFlowBtn.Enabled := false;
end;

procedure TWorkItemTestForm.CheckFiniFarm;
begin
  if Completed + Cancelled = Added then
  begin
    WorkFarm.Free;
    WorkFarm := nil;
    ShowMessage('Added: '
                +IntToStr(Added)
                +' Completed: '
                +IntToStr(Completed)
                +' Cancelled: '
                +IntToStr(Cancelled));
    Assert(InFlight = 0);
    Completed := 0;
    Cancelled := 0;
    Added := 0;
    NFlowBtn.Enabled := true;
    CFlowBtn.Enabled := true;
  end;
end;

procedure TWorkItemTestForm.SendItems;
const
  WiCount = 65535;
var
  WIs: array of TTestWorkItem;
  idx: integer;
begin
  SetLength(WIs, WiCount);
  Added := WiCount;
  InFlight := WiCount;
  for idx := 0 to Pred(WiCount) do
  begin
    WIs[idx] := TTestWorkItem.Create;
    WIs[idx].FHandle := Handle;
    WIs[idx].FCompleteInt := @Completed;
    WIs[idx].FCancelInt := @Cancelled;
    WIs[idx].FInFlightInt := @InFlight;
  end;
  if not WorkFarm.AddWorkItemBatch(@WIs[0], WiCount) then
  begin
    Added := 0;
    InFlight := 0;
    for idx := 0 to Pred(WiCount) do
      WIs[idx].Free;
    CheckFiniFarm;
  end;
end;

procedure TWorkItemTestForm.NFlowBtnClick(Sender: TObject);
begin
  InitCountsAndFarm;
  SendItems;
end;

procedure TWorkItemTestForm.CFlowBtnClick(Sender: TObject);
begin
  InitCountsAndFarm;
  SendItems;
  WorkFarm.StartFlush(true);
end;


function  TTestWorkItem.DoWork:integer;
begin
  Sleep(0);
  result := 0;
end;

procedure TTestWorkItem.DoNormalCompletion;
begin
  LockAbstractions.InterlockedIncrement(FCompleteInt^);
  CompleteCommon;
end;

procedure TTestWorkItem.DoCancelledCompletion;
begin
  LockAbstractions.InterlockedIncrement(FCancelInt^);
  CompleteCommon;
end;

procedure TTestWorkItem.CompleteCommon;
begin
  if LockAbstractions.InterlockedDecrement(FInFlightInt^) = 0 then
    PostMessage(FHandle, WM_COMPLETION, 0, 0);
end;

constructor TTestWorkItem.Create;
begin
  inherited;
  CanAutoFree := true;
end;

end.
