unit QueensCover;

{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

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

uses ExactCover;

{

Row, column, and diagonal numbering:

    +-------+
n-1 |       |
..  |       |  Rows and columns numbered as expected, first is zero,
2   |       |  last is N - 1.
1   |       |
0   |       |
    +-------+
     0123.n-1

Leading diagonal:

    +-------+
n-1 |n-1.2n-2|  Diagonals where Row + Col = Constant,
..  |       |   Constant is 0 .. 2n-2 Giving us Succ(2n-2) leading diagonals.
2   |       |
1   |123.  n|
0   |012.n-1|
    +-------+
     0123.n-1

Trailing diagonal:

    +-------+
n-1 |1-n    |  Diagonals where Col - Row = Constant,
..  |       |  Constant is 1-n .. n-1 Giving us:
2   |-2     |  Succ((n-1)-(1-n)) = Succ(n-1-1+n), Succ (2n-2) trailing diagonals.
1   |-1     |
0   |012.n-1|
    +-------+
     0123.n-1

}

{
  Possibilities and constraints.

  Possibilities: n^2 = Queen in (RxCy)

  Constraints:

  Exactly constrained:

  Row & Column: There is a Queen in Row X, Column X.

  Not exactly constrained (at most, not exactly).:

  There is a Queen in Leading Diagonal N.
  There is a Queen in Trailing Diagonal N. (For N specified between above limits).

  In order to inexactly constrain the diagonals, for each diagonal, we add
  a possibility "Diagonal Unfullfilled (Lead/Trail N), which then allows for an
  exact solution.
}


type
  TQPossType = (tqpQueenRC, tqpLeadEmpty, tqpTrailEmpty);

  TQueensCoverPossibility = class(TPossibility)
  private
    FPossType: TQPossType;
    FRow, FCol: integer;
    FEmptyDiag: integer;
  public
    property PossType: TQPossType read FPossType;
    property Row: integer read FRow;
    property Col: integer read FCol;
    property EmptyDiag:integer read FEmptyDiag;
  end;

  TQConstraintType = (tqtRow, tqtCol, tqtLeadDiag, tqtTrailDiag);

  TQueensCoverConstraint = class(TConstraint)
  private
    FConsType: TQConstraintType;
    FNumber: integer;
  public
    property ConsType: TQConstraintType read FConsType;
    property Number: integer read FNumber;
  end;

  TQueensCover = class(TExactCoverProblem)
  private
    FN: integer;
    FSolutions: integer;
  protected
    procedure DeletePossCons;
    procedure AddPossCons;

    function FirstLD: integer;
    function LastLD: integer;
    function FirstTD: integer;
    function LastTD: integer;

    function HandleAllocPossibility(Sender:TObject): TPossibility;
    function HandleAllocConstraint(Sender: TObject): TConstraint;
    function HandleCheckSatisfies(Possibility: TPossibility;
                                    Constraint: TConstraint): boolean;
    procedure HandleCover(Sender: TObject;
                          TerminationType: TCoverTerminationType;
                          var Stop: boolean);

{$IFDEF EXACT_COVER_LOGGING}
    procedure HandleLogPossibility(Sender: TObject; Poss: TPossibility; EvtType: TEvtType);
    procedure HandleLogConstraint(Sender: TObject; Cons: TConstraint; EvtType: TEvtType);
    procedure HandleLogOther(Sender: TObject; EvtType: TEvtType);
{$ENDIF}
  public
    procedure Solve(Nparam: integer);
    property N:integer read FN;
    property Solutions: integer read FSolutions;
  end;

implementation

uses
  SysUtils, SparseMatrix, DLList;

const
  S_BOARD_TOO_SMALL = 'Board size must be > 0';

function TQueensCover.HandleAllocPossibility(Sender:TObject): TPossibility;
begin
  result := TQueensCoverPossibility.Create;
end;

function TQueensCover.HandleAllocConstraint(Sender: TObject): TConstraint;
begin
  result := TQueensCoverConstraint.Create;
end;

procedure TQueensCover.HandleCover(Sender: TObject;
                                   TerminationType: TCoverTerminationType;
                                   var Stop: boolean);
begin
  Assert(not Stop);
  case TerminationType of
    cttOKExactCover, cttOKUnderconstrained: begin
      Inc(FSolutions);
{$IFDEF EXACT_COVER_LOGGING}
      WriteLn('Solution!');
{$ENDIF}
    end;
  else
    Assert(false);
  end;
end;

function TQueensCover.HandleCheckSatisfies(Possibility: TPossibility;
                                           Constraint: TConstraint): boolean;
var
  Poss: TQueensCoverPossibility;
  Cons: TQueensCoverConstraint;
begin
  Poss := Possibility as TQueensCoverPossibility;
  Cons := Constraint as TQueensCoverConstraint;
  case Cons.FConsType of
    tqtRow: begin
      case Poss.FPossType of
        tqpQueenRC: result := Poss.FRow = Cons.FNumber;
      else
        result := false;
      end;
    end;
    tqtCol: begin
      case Poss.FPossType of
        tqpQueenRC: result := Poss.FCol = Cons.FNumber;
      else
        result := false;
      end;
    end;
    tqtLeadDiag: begin
      case Poss.FPossType of
        tqpQueenRC: result := (Poss.FRow + Poss.FCol) = Cons.FNumber;
        tqpLeadEmpty: result := Poss.FEmptyDiag = Cons.FNumber;
        tqpTrailEmpty: result := false;
      else
        Assert(false);
        result := false;
      end;
    end;
    tqtTrailDiag: begin
      case Poss.FPossType of
        tqpQueenRC: result := (Poss.FCol - Poss.FRow) = Cons.FNumber;
        tqpLeadEmpty: result := false;
        tqpTrailEmpty: result := Poss.FEmptyDiag = Cons.FNumber;
      else
        Assert(false);
        result := false;
      end;
    end;
  else
    Assert(false);
    result := false;
  end;
end;

procedure TQueensCover.DeletePossCons;
var
  Poss: TPossibility;
  Cons: TConstraint;
begin
  Poss := FirstPossibility;
  while Assigned(Poss) do
  begin
    DeletePossibility(Poss);
    Poss := FirstPossibility;
  end;
  Cons := FirstConstraint;
  while Assigned(Cons) do
  begin
    DeleteConstraint(Cons);
    Cons := FirstConstraint;
  end;
end;

function TQueensCover.FirstLD: integer;
begin
  result := 0;
end;

function TQueensCover.LastLD: integer;
begin
  result := (2 *FN) - 2;
end;

function TQueensCover.FirstTD: integer;
begin
  result := 1 - FN;
end;

function TQueensCover.LastTD: integer;
begin
  result := FN - 1;
end;

procedure TQueensCover.AddPossCons;
var
  Row, Col, Diag: integer;
  Poss: TQueensCoverPossibility;
  Cons: TQueensCoverConstraint;
begin
  //Poss.
  for Row := 0 to Pred(FN) do
  begin
    for Col := 0 to Pred(FN) do
    begin
      Poss := AddPossibility as TQueensCoverPossibility;
      Poss.FPossType := tqpQueenRC;
      Poss.FRow := Row;
      Poss.FCol := Col;
    end;
  end;
  for Diag := FirstLD to LastLD do
  begin
    Poss := AddPossibility as TQueensCoverPossibility;
    Poss.FPossType := tqpLeadEmpty;
    Poss.FEmptyDiag := Diag;
  end;
  for Diag := FirstTD to LastTD do
  begin
    Poss := AddPossibility as TQueensCoverPossibility;
    Poss.FPossType := tqpTrailEmpty;
    Poss.FEmptyDiag := Diag;
  end;
  //Cons.
  for Row := 0 to Pred(FN) do
  begin
    Cons := AddConstraint as TQueensCoverConstraint;
    Cons.FConsType := tqtRow;
    Cons.FNumber := Row;
  end;
  for Col := 0 to Pred(FN) do
  begin
    Cons := AddConstraint as TQueensCoverConstraint;
    Cons.FConsType := tqtCol;
    Cons.FNumber := Col;
  end;
  for Diag := FirstLD to LastLD do
  begin
    Cons := AddConstraint as TQueensCoverConstraint;
    Cons.FConsType := tqtLeadDiag;
    Cons.FNumber := Diag;
  end;
  for Diag := FirstTD to LastTD do
  begin
    Cons := AddConstraint as TQueensCoverConstraint;
    Cons.FConsType := tqtTrailDiag;
    Cons.FNumber := Diag;
  end;
end;

procedure TQueensCover.Solve(Nparam: Integer);
begin
  if Nparam < 1 then
    raise Exception.Create(S_BOARD_TOO_SMALL);
  FN := Nparam;
  FSolutions := 0;
  ClearConnectivity;
  DeletePossCons;
  Assert(not Solved);
  OnAllocPossibility := HandleAllocPossibility;
  OnAllocConstraint := HandleAllocConstraint;
  OnPossibilitySatisfiesConstraint := HandleCheckSatisfies;
{$IFDEF EXACT_COVER_LOGGING}
  OnLogPossibilityEvent := HandleLogPossibility;
  OnLogConstraintEvent := HandleLogConstraint;
  OnLogOtherEvent := HandleLogOther;
{$ENDIF}
  OnCoverNotify := HandleCover;
  AddPossCons;
  SetupConnectivity;
  AlgorithmX;
  Assert(Solved);
end;

{$IFDEF EXACT_COVER_LOGGING}
procedure TQueensCover.HandleLogPossibility(Sender: TObject; Poss: TPossibility; EvtType: TEvtType);
var
  Msg, M: string;
  P: TQueensCoverPossibility;
begin
  Msg := EvtTypeStrs[EvtType] + ': ';
  P := Poss as TQueensCoverPossibility;
  case P.PossType of
    tqpQueenRC: M := 'Queen at Row: ' + IntToStr(P.Row) + ' Col: ' + IntToStr(P.Col);
    tqpLeadEmpty: M := 'Lead diagonal: ' + IntToStr(P.EmptyDiag) + ' empty.';
    tqpTrailEmpty: M := 'Trail diagonal: ' + IntToStr(P.EmptyDiag) + ' empty.';
  else
    Assert(false);
  end;
  WriteLn(Msg + M);
end;

procedure TQueensCover.HandleLogConstraint(Sender: TObject; Cons: TConstraint; EvtType: TEvtType);
var
  Msg, M: string;
  C: TQueensCoverConstraint;
begin
  Msg := EvtTypeStrs[EvtType] + ': ';
  C := Cons as TQueensCoverConstraint;
  case C.ConsType of
    tqtRow: M := 'Row ' + IntToStr(C.Number) + ' filled';
    tqtCol: M := 'Column ' + IntToStr(C.Number) + ' filled';
    tqtLeadDiag: M := 'Lead diagonal ' + IntToStr(C.Number) + ' filled';
    tqtTrailDiag: M := 'Trail diagonal ' + IntToStr(C.Number) + ' filled';
  else
    Assert(false);
  end;
  WriteLn(Msg + M);
end;

procedure TQueensCover.HandleLogOther(Sender: TObject; EvtType: TEvtType);
begin
 WriteLn(EvtTypeStrs[EvtType]);
end;
{$ENDIF}

end.
