unit ExactCoverTestFrm;

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, FMX.Layouts, FMX.Memo;

type
  TForm1 = class(TForm)
    TestMatrix: TButton;
    TestSolver: TButton;
    LogMemo: TMemo;
    procedure TestMatrixClick(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  Form1: TForm1;

implementation

{$R *.fmx}

uses
  ExactCover;

const
  MATRIX_SIZE = 100;

procedure TForm1.TestMatrixClick(Sender: TObject);
var
  M: TExactCoverMatrix;

  procedure InitMatrixHeaders;
  var
    i: integer;
  begin
    for i := 0 to Pred(MATRIX_SIZE) do
    begin
      M.AddRow;
      M.AddColumn;
    end;
  end;

  procedure InitMatrixCells;
  var
    i,j,count: integer;
    C: TExactCoverCell;
  begin
    count := 0;
    while count < ((MATRIX_SIZE * MATRIX_SIZE) div 2) do
    begin
      i := Random(MATRIX_SIZE);
      j := Random(MATRIX_SIZE);
      C := M.GetCell(i, j) as TExactCoverCell;
      if not Assigned(C) then
      begin
        M.InsertCell(i, j);
        Inc(count);
      end;
    end;
  end;

  procedure InitMatrix;
  begin
    InitMatrixHeaders;
    InitMatrixCells;
  end;

  procedure TestRandomPush(AndPop: boolean);
  var
    RndSel, RndIdx, Lim, PushCount: integer;
    Header: TExactCoverHeader;
  begin
    PushCount := 0;
    Header := nil;
    Lim := 0;

    while (M.IncludedRowCount > 0) or (M.IncludedColCount > 0) do
    begin
      if (M.IncludedRowCount > 0) and (M.IncludedColCount > 0) then
        RndSel := Random(2) //0, 1
      else if (M.IncludedRowCount > 0) then
        RndSel := 0
      else
        RndSel := 1;

      case RndSel of
        0: begin
            Header := M.FirstIncludedRow;
            Lim := M.IncludedRowCount;
        end;
        1: begin
             Header := M.FirstIncludedColumn;
             Lim := M.IncludedColCount;
        end;
      else
        Assert(false);
      end;
      Assert(Assigned(Header));
      Assert(Lim > 0);
      RndIdx := Random(Lim);
      while RndIdx > 0 do
      begin
        Header := Header.NextIncludedHeader;
        Dec(RndIdx);
      end;
      M.ExcludeAndPush(Header);
      Inc(PushCount);
    end;
    Assert(PushCount = 2 * MATRIX_SIZE);
    if AndPop then
    begin
      while M.PopAndInclude <> nil do
        Dec(PushCount);
      Assert(PushCount = 0);
      Assert(M.AllIncluded);
    end;
  end;

begin
{$IFOPT C-}
  LogMemo.Lines.Add('INFO: Turn assertions on for more complete checking');
{$ENDIF}
  //Test create and free
  M:= TExactCoverMatrix.Create;
  M.Free;

  M := TExactCoverMatrix.Create;
  InitMatrixHeaders;
  M.Free;

  M := TExactCoverMatrix.Create;
  InitMatrix;
  M.Free;

  LogMemo.Lines.Add('PASS: Basic matrix create, populate and free');

  M := TExactCoverMatrix.Create;
  try
    InitMatrix;
    Assert(not M.AllIncluded);
  finally
    M.Free;
  end;
  LogMemo.Lines.Add('PASS: All included when not fully init');

  M := TExactCoverMatrix.Create;
  try
    InitMatrix;
    Assert(not M.AllIncluded);
    M.InitAllIncluded;
  finally
    M.Free;
  end;
  LogMemo.Lines.Add('PASS: Init all included');

  M := TExactCoverMatrix.Create;
  try
    InitMatrix;
    M.InitAllIncluded;
    TestRandomPush(false);
    //Free whilst all pushed, check counts OK.
  finally
    M.Free;
  end;
  LogMemo.Lines.Add('PASS: Test random push all and free');

  M := TExactCoverMatrix.Create;
  try
    InitMatrix;
    M.InitAllIncluded;
    TestRandomPush(true);
    //Free whilst all pushed, check counts OK.
    if M.AllIncluded then
      LogMemo.Lines.Add('PASS: Test random push all, pop all and free')
    else
      LogMemo.Lines.Add('FAIL: Test random push all, pop all and free');
  finally
    M.Free;
  end;
end;

initialization
  Randomize;
end.
