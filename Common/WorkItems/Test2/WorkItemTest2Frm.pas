unit WorkItemTest2Frm;
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
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls, RecycleItems, Vcl.ComCtrls;

const
  MSG_CALC_FINISHED = WM_APP+1;

type
  TWorkItemTest2Form = class(TForm)
    LowEdit: TEdit;
    HighEdit: TEdit;
    CalcBtn: TButton;
    PrimeEdit: TEdit;
    ResultEdit: TEdit;
    CheckBtn: TButton;
    StatusBar: TStatusBar;
    ResetBtn: TButton;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure CalcBtnClick(Sender: TObject);
    procedure ResetBtnClick(Sender: TObject);
    procedure CheckBtnClick(Sender: TObject);
  private
    { Private declarations }
    FPrimeStore: TReusableObjStore;
    procedure HandleCalcFinished(Sender: TObject);
    procedure CalcFinished(var Message: TMessage); message MSG_CALC_FINISHED;
    procedure UpdateStatus;
  public
    { Public declarations }
  end;

var
  WorkItemTest2Form: TWorkItemTest2Form;

implementation

{$R *.dfm}

procedure TWorkItemTest2Form.FormCreate(Sender: TObject);
begin
  FPrimeStore := TReusableObjStore.Create;
  FPrimeStore.OnCalcFinished := HandleCalcFinished;
end;

procedure TWorkItemTest2Form.HandleCalcFinished(Sender: TObject);
begin
  PostMessage(self.Handle, MSG_CALC_FINISHED, 0, 0);
end;

procedure TWorkItemTest2Form.ResetBtnClick(Sender: TObject);
begin
  FPrimeStore.Reset;
  UpdateStatus;
end;

procedure TWorkItemTest2Form.CalcBtnClick(Sender: TObject);
var
  Low, High: cardinal;
begin
  Low := StrToInt(LowEdit.Text);
  High := StrToInt(HighEdit.Text);
  FPrimeStore.Reset;
  if FPrimeStore.CalcPrimes(Low, High) <> 0 then
    ShowMessage('Error calculating primes');
  UpdateStatus;
end;

procedure TWorkItemTest2Form.CalcFinished(var Message: TMessage);
begin
  UpdateStatus;
end;

procedure TWorkItemTest2Form.CheckBtnClick(Sender: TObject);
var
  CheckRet: integer;
  Yes: boolean;
  ResStr: string;
begin
  CheckRet := FPrimeStore.CheckPrime(StrToInt(PrimeEdit.Text), Yes);
  if CheckRet = 0 then
  begin
    if Yes then
      ResStr := PrimeEdit.Text + ' is prime.'
    else
      ResStr := PrimeEdit.Text + ' is not prime.';
  end
  else if CheckRet = E_BAD_PARAMS then
    ResStr := 'Prime out of range.'
  else if CheckRet = E_NO_DATA then
    ResStr := 'No data.'
  else
    ResStr := 'Unspecified error';
  ResultEdit.Text := ResStr;
end;

procedure TWorkItemTest2Form.FormDestroy(Sender: TObject);
begin
  FPrimeStore.Free;
end;

procedure TWorkItemTest2Form.UpdateStatus;
var
  State: TReusableObjStoreState;
  StatusBlock: TStatusStruct;
begin
  FPrimeStore.GetStatus(State, StatusBlock);
  case State of
    rosIdle: StatusBar.SimpleText := 'Idle';
    rosCalcing: StatusBar.SimpleText := 'Calculating...';
    rosResults: StatusBar.SimpleText := 'Found ' + IntToStr(StatusBlock.NumPrimes) + ' primes between '
     + IntToStr(StatusBlock.LowBound) + ' and '
     + IntToStr(StatusBlock.HighBound) + ' (spawn: '
     + IntToStr(StatusBlock.Spawned) + ' queue: '
     + IntToStr(StatusBlock.Queued) + ' merge: '
     + IntToStr(StatusBlock.Merges) + ')'
  end;
end;

end.
