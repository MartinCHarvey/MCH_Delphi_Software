unit WorkItemTest3;

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
