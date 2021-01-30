unit ExactCoverTestFrm;

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, FMX.Layouts, FMX.Memo, ExactCover;

type
  TForm1 = class(TForm)
    TestMatrix: TButton;
    TestSolver: TButton;
    LogMemo: TMemo;
    procedure TestMatrixClick(Sender: TObject);
    procedure TestSolverClick(Sender: TObject);
  private
    { Private declarations }
    FTerminationCounts: array[TCoverTerminationType] of integer;
    procedure CoverNotify1(Sender: TObject;
                                TerminationType: TCoverTerminationType;
                                var Continue: boolean);
    function CheckSatisfies1(Possibility: TPossibility; Constraint: TConstraint): boolean;
  public
    { Public declarations }
  end;

var
  Form1: TForm1;

implementation

{$R *.fmx}

const
  MATRIX_SIZE = 100;

//1. Test the matrix.

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

//2. Test example problem.

(*
As in the an example, let S = {A, B, C, D, E, F} be a collection of subsets of a set X = {1, 2, 3, 4, 5, 6, 7} such that:

A = {1, 4, 7};
B = {1, 4};
C = {4, 5, 7};
D = {3, 5, 6};
E = {2, 3, 6, 7}; and
F = {2, 7}.

This is represented in a (sparse) matrix as follows:

	A	B	C	D	E	F
1	1	1	0	0	0	0
2	0	0	0	0	1	1
3	0	0	0	1	1	0
4	1	1	1	0	0	0
5	0	0	1	1	0	0
6	0	0	0	1	1	0
7	1	0	1	0	1	1

For this, we need to select rows 1, 2 and 5, which then contains exactly one
element from A,B,C,D,E,F.
*)

procedure TForm1.CoverNotify1(Sender: TObject;
                       TerminationType: TCoverTerminationType;
                       var Continue: boolean);
begin
  Inc(FTerminationCounts[TerminationType]);
  //And continue;
end;

function TForm1.CheckSatisfies1(Possibility: TPossibility; Constraint: TConstraint): boolean;
begin
  Assert((Possibility.Idx >= 0) and (Possibility.Idx < 7));
  Assert((Constraint.Idx >= 0) and (COnstraint.Idx < 6));
  case Possibility.Idx of
    0:
      case Constraint.Idx of
        0,1: result := true;
      end;
    1:
      case Constraint.Idx of
        4,5: result := true;
      end;
    2:
      case Constraint.Idx of
        3,4: result := true;
      end;
    3:
      case Constraint.Idx of
        0,1,2: result := true;
      end;
    4:
      case Constraint.Idx of
        2,3: result := true;
      end;
    5:
      case Constraint.Idx of
        4,5: result := true;
      end;
    6:
      case Constraint.Idx of
        0,2,4,5: result := true;
      end;
  end;
end;

procedure TForm1.TestSolverClick(Sender: TObject);
var
  Problem: TExactCoverProblem;
  idx: integer;
begin
{$IFOPT C-}
  LogMemo.Lines.Add('INFO: Turn assertions on for more complete checking');
{$ENDIF}

  //1. Check basic solve.
  Problem := TExactCoverProblem.Create;
  try

    Problem.OnPossibilitySatisfiesConstraint := CheckSatisfies1;
    Problem.CoverNotifySet :=
      [Low(TCoverTerminationType) .. High(TCoverTerminationType)];
    Problem.OnCoverNotify := CoverNotify1;
    for idx := 0 to 6 do
      Problem.AddPossibility;
    for idx := 0 to 5 do
      Problem.AddConstraint;
    Problem.SetupConnectivity;
    FillChar(FTerminationCounts, sizeof(FTerminationCounts), 0);
    Problem.AlgorithmX;
    Assert(Problem.Solved);
    Assert(FTerminationCounts[cttInvalid] =0);
    Assert(FTerminationCounts[cttOKExactCover] = 2);
    Assert(FTerminationCounts[cttOKUnderconstrained] = 0);
    //Check that we can re-run the algo without re-setting up.
    FillChar(FTerminationCounts, sizeof(FTerminationCounts), 0);
    Problem.AlgorithmX;
    Assert(Problem.Solved);
    Assert(FTerminationCounts[cttInvalid] =0);
    Assert(FTerminationCounts[cttOKExactCover] = 2);
    Assert(FTerminationCounts[cttOKUnderconstrained] = 0);
  finally
    Problem.Free;
  end;
  LogMemo.Lines.Add('PASS: Basic solve.');

  //2. Check Free path when terminated early.

  //TODO 3. Check initial placement of givens,
  //and free path when some solutions pushed.

  //TODO 4. Check under and overconstrained problems.
end;

initialization
  Randomize;
end.
