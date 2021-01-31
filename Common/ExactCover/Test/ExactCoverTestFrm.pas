unit ExactCoverTestFrm;

{$C+}

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
                                var Stop: boolean);
    procedure CoverNotify2(Sender: TObject;
                                TerminationType: TCoverTerminationType;
                                var Stop: boolean);
    function CheckSatisfies1(Possibility: TPossibility; Constraint: TConstraint): boolean;
    procedure PrintSolution(Sender: TObject);
  public
    { Public declarations }
  end;

var
  Form1: TForm1;

implementation

{$R *.fmx}

const
  MATRIX_SIZE = 100;

procedure CHECK(B: boolean);
begin
  if not B then
  begin
  {$IFOPT C+}
    raise EAssertionFailed.Create('Debug check failed');
  {$ELSE}
    raise Exception.Create('Debug check failed');
  {$ENDIF}
  end;
end;

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
        CHECK(false);
      end;
      CHECK(Assigned(Header));
      CHECK(Lim > 0);
      RndIdx := Random(Lim);
      while RndIdx > 0 do
      begin
        Header := Header.NextIncludedHeader;
        Dec(RndIdx);
      end;
      M.ExcludeAndPush(Header);
      Inc(PushCount);
    end;
    CHECK(PushCount = 2 * MATRIX_SIZE);
    if AndPop then
    begin
      while M.PopAndInclude <> nil do
        Dec(PushCount);
      CHECK(PushCount = 0);
      CHECK(M.AllIncluded);
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
    CHECK(not M.AllIncluded);
  finally
    M.Free;
  end;
  LogMemo.Lines.Add('PASS: All included when not fully init');

  M := TExactCoverMatrix.Create;
  try
    InitMatrix;
    CHECK(not M.AllIncluded);
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

procedure TForm1.PrintSolution(Sender: TObject);
var
  P: TExactCoverProblem;
  Str: string;
  Poss: TPossibility;
begin
  P := Sender as TExactCoverProblem;
  Str := '  (';
  Poss := P.TopPartialSolutionPossibility;
  while Assigned(Poss) do
  begin
    Str := Str + IntToStr(Succ(Poss.Idx));
    Poss := P.NextPartialSolutionPossibility(Poss);
    if Assigned(Poss) then
      Str := Str + ', '
    else
      Str := Str + ')';
  end;
  LogMemo.Lines.Add('Solution: ' + Str);
end;

procedure TForm1.CoverNotify1(Sender: TObject;
                       TerminationType: TCoverTerminationType;
                       var Stop: boolean);
var
  Str: string;
  Poss:TPossibility;
  P: TExactCoverProblem;
begin
  Inc(FTerminationCounts[TerminationType]);
  if TerminationType = cttOKExactCover then
    PrintSolution(Sender);
  //And continue;
end;

procedure TForm1.CoverNotify2(Sender: TObject;
                       TerminationType: TCoverTerminationType;
                       var Stop: boolean);
begin
  Inc(FTerminationCounts[TerminationType]);
  if TerminationType = cttOKExactCover then
    PrintSolution(Sender);
  Stop := TerminationType = cttOKExactCover;
end;


function TForm1.CheckSatisfies1(Possibility: TPossibility; Constraint: TConstraint): boolean;
begin
  CHECK((Possibility.Idx >= 0) and (Possibility.Idx < 7));
  CHECK((Constraint.Idx >= 0) and (COnstraint.Idx < 6));
  result := false;
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
  Possibility: TPossibility;
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
    LogMemo.Lines.Add('------');
    Problem.AlgorithmX;
    LogMemo.Lines.Add('------');
    CHECK(Problem.Solved);
    CHECK(FTerminationCounts[cttInvalid] =0);
    CHECK(FTerminationCounts[cttOKExactCover] = 2);
    CHECK(FTerminationCounts[cttOKUnderconstrained] = 0);
    //Check that we can re-run the algo without re-setting up.
    FillChar(FTerminationCounts, sizeof(FTerminationCounts), 0);
    LogMemo.Lines.Add('------');
    Problem.AlgorithmX;
    LogMemo.Lines.Add('------');
    CHECK(Problem.Solved);
    CHECK(FTerminationCounts[cttInvalid] =0);
    CHECK(FTerminationCounts[cttOKExactCover] = 2); //explanation for this later...
    CHECK(FTerminationCounts[cttOKUnderconstrained] = 0);
  finally
    Problem.Free;
  end;
  LogMemo.Lines.Add('PASS: Basic solve.');

  //2. Check Free path when terminated early.
  Problem := TExactCoverProblem.Create;
  try
    Problem.OnPossibilitySatisfiesConstraint := CheckSatisfies1;
    Problem.CoverNotifySet :=
      [Low(TCoverTerminationType) .. High(TCoverTerminationType)];
    Problem.OnCoverNotify := CoverNotify2;
    ///NB stop after initial solution.
    for idx := 0 to 6 do
      Problem.AddPossibility;
    for idx := 0 to 5 do
      Problem.AddConstraint;
    Problem.SetupConnectivity;
    FillChar(FTerminationCounts, sizeof(FTerminationCounts), 0);
    LogMemo.Lines.Add('------');
    Problem.AlgorithmX;
    LogMemo.Lines.Add('------');
    CHECK(FTerminationCounts[cttInvalid] =0);
    //Just check we found just one solution
    CHECK(FTerminationCounts[cttOKExactCover] = 1);
    CHECK(FTerminationCounts[cttOKUnderconstrained] = 0);
  finally
    Problem.Free;
  end;
  LogMemo.Lines.Add('PASS: Free when partly solved.');

  //TODO 3. Check initial placement of givens, free path when initial solutions pushed.
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

    //Constrain ourselves to solutions that contain the second possibility.
    //By doing this, we remove the choice about possibilities 3 and 6,
    //which means that we only get to the solution once.
    Possibility := Problem.FirstPossibility;
    Possibility := Problem.NextPossibility(Possibility);
    Problem.PushSolutionPossibility(Possibility, true);

    LogMemo.Lines.Add('------');
    Problem.AlgorithmX;
    LogMemo.Lines.Add('------');
    CHECK(Problem.Solved);
    CHECK(FTerminationCounts[cttInvalid] =0);
    CHECK(FTerminationCounts[cttOKExactCover] = 1);
    CHECK(FTerminationCounts[cttOKUnderconstrained] = 0);
    CHECK(Problem.PartialSolutionStackCount > 0);
    CHECK(Problem.RowsColsStacked > 0);
    LogMemo.Lines.Add('PASS: Solve with initial given possibility and free.');

    //Now supply bad initial possibility.
    Problem.PopTopSolutionPossibility(true);
    CHECK(Problem.PartialSolutionStackCount = 0);
    CHECK(Problem.RowsColsStacked = 0);

    FillChar(FTerminationCounts, sizeof(FTerminationCounts), 0);
    Possibility := Problem.FirstPossibility;
    Possibility := Problem.NextPossibility(Possibility);
    Possibility := Problem.NextPossibility(Possibility);
    Problem.PushSolutionPossibility(Possibility, true);

    LogMemo.Lines.Add('------');
    Problem.AlgorithmX;
    LogMemo.Lines.Add('------');
    CHECK(Problem.Solved); //Doesn't mean it found an exact cover...
    CHECK(FTerminationCounts[cttInvalid] = 0);
    CHECK(FTerminationCounts[cttOKExactCover] = 0);
    CHECK(FTerminationCounts[cttOKUnderconstrained] = 0);
    CHECK(Problem.PartialSolutionStackCount > 0);
    CHECK(Problem.RowsColsStacked > 0);
    LogMemo.Lines.Add('PASS: No solution with bad initial possibility.');
  finally
    Problem.Free;
  end;
end;

initialization
  Randomize;
end.
