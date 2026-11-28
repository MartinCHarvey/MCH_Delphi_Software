unit WorkItemTest3;
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
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls;

type
  TForm1 = class(TForm)
    TestNumaBtn: TButton;
    TestAllocBtn: TButton;
    procedure TestNumaBtnClick(Sender: TObject);
    procedure TestAllocBtnClick(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  Form1: TForm1;

implementation

{$R *.dfm}

uses WorkItemsNUMA;

procedure TForm1.TestAllocBtnClick(Sender: TObject);
var
  P:Pointer;
  A: TObject;
begin
  P := AllocMem(4096);
  FreeMem(P);
  A := AllocMem(TObject.InstanceSize);
  TObject.InitInstance(A);
  A.Create;
  A.Free;
end;

procedure TForm1.TestNumaBtnClick(Sender: TObject);
var
  WorkFarm: TNumaWorkFarm;
  WorkFarm2: TGroupWorkFarm;
begin
  WorkFarm := TNumaWorkFarm.Create;
  try
    WorkFarm.NumaNode := 0;
    WorkFarm.NumaNode := -1;
  finally
    WorkFarm.Free;
  end;
  WorkFarm2 := TGroupWorkFarm.Create;
  try
    WorkFarm2.GroupNumber := 1;
  finally
    WorkFarm2.Free;
  end;
end;

end.
