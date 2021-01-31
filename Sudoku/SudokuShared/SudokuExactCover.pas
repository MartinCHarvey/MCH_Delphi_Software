unit SudokuExactCover;

{

Copyright © 2021 Martin Harvey <martin_c_harvey@hotmail.com>

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

{

Application of Exact Cover problem to Sudoku.

This is then simply an application of labelling. For possibilities, instead of
1,2,3,4,5 we have a possible placement of a number in a row and column:

Row Labels, "possibilities" in our matrix:

Row X, Col Y, constains # Z.

R1C1N1, R1C1N2 ... RX, CY, NZ ... R9C9N9.

Column labels.

Each column is a constraint.

There are four types contraints, each of which should be satisfied exactly once:

Row-Column: Each row column intersection contains one number.

81 constraints, one for each Cell.
R1C1 = Entry set in RnCn constraint if that row and column contains a number.

Row-Number: Each row contains a particular number.

81 constraints, 9 rows by 9 possible numbers.
R1#1 = Each entry set in Rn#, constraint if that row contains a particular number.

Column-number: Same as for rows, 81 constraints for that row contains a particular number.

C1#1 = Each entry set in Rn#, constraint if that column contains a particular number.

Block-Number: Like rows and columns, label the boxes (groups of 3x3 cells),
Constraint satisfied if each boxcontains a particular number.

}

uses
  ExactCover, SudokuAbstracts, DLList;

type
  TSExactCoverState = class;

  TSSudokuPossibility = class(TPossibility)
  private
    FRow, FCol, FNumber: TSNumber;
    FSimpleRepLink: TDLEntry;
    FParentCoverState: TSExactCoverState;
  public
    constructor Create;
    destructor Destroy; override;
    property Row:TSNumber read FRow;
    property Col: TSNumber read FCol;
    property Number: TSNumber read FNumber;
  end;

  TSConstraintType =
    (sctCellContainsNum,
     sctRowContainsNum,
     sctColContainsNum,
     sctBlockContainsNum);

  //Given only two parameters for each, could save space!
  TSSudokuConstraint = class(TConstraint)
  private
    FConsType: TSConstraintType;
    FRow, FCol, FBlock, FNumber: TSNumber;
    //Anything else required here? Maybe but not now.
  end;

  TSimplePlace = record
    Entries: TDLEntry;
    Count: integer;
  end;

  TSimpleRow = array [TSNumber] of TSimplePlace;
  TSimpleStateRep = array[TSNumber] of TSimpleRow;

  TSExactCoverState = class(TSAbstractBoardState)
  private
    FExactCover: TExactCoverProblem;
    FSimpleRep: TSimpleStateRep;
  protected
    //Routines to be implemented for exact cover.
    procedure InitSimpleRep;
    procedure FiniSimpleRep;
    procedure InitProb;
    function HandleAllocPossibility(Sender:TObject): TPossibility;
    function HandleAllocConstraint(Sender: TObject): TConstraint;
    function HandleCheckSatisfies(Possibility: TPossibility;
                                    Constraint: TConstraint): boolean;

    procedure SetCoverNotify(CoverNotify: TCoverNotifyEvent);
    function GetCoverNotify: TCoverNotifyEvent;

    //Routines to be implemented for sudoku abstract board.
    function GetOptEntry(Row, Col: TSNumber): TSOptNumber; override;
    function SetEntry(Row, Col, Entry: TSNumber): boolean; override;
    procedure ClearEntry(Row, Col: TSNumber); override;

    function GetComplete: boolean; override;
    function GetProceedable: boolean; override; //Proceedable means "obvious next move".

    function FindNextMoveInt: TSSudokuPossibility;
    function FindNextMovePosInt(Row, Col: TSNumber):TSSudokuPossibility;
  public
    constructor Create; override;
    destructor Destroy; override;
    //Routines to be implemented for exact cover.
    //Routines to be implemented for sudoku abstract board.

    //FindNext move differs here in that it only finds moves which
    //are unambigous, but combined with the change in "EvolveIfSingular",
    //that should probably not present a problem
    //For more complicated problems, we use the proper solver.
    function FindNextMove(var Row, Col: TSNumber): TSNumberSet; override;
    function EvolveIfSingular(Row, Col: TSNumber; NSet: TSNumberSet): boolean; override;
    procedure Clear; override;

    //Mirror cover notification handler up to the next level.
    property OnCoverNotify: TCoverNotifyEvent read GetCoverNotify write SetCoverNotify;
  end;

  TSExactCoverSolver = class(TSAbstractSolver)
  private
    FReturnSolved: boolean;
  protected
    procedure HandleCoverNotify(Sender: TObject;
                                TerminationType: TCoverTerminationType;
                                var Stop: boolean);
    function CreateBoardState: TSAbstractBoardState; override;
  public
    function Solve: boolean; override;
  end;


implementation

uses
  SysUtils;

{ TSSudokuPossibility }

constructor TSSudokuPossibility.Create;
begin
  inherited;
  DLItemInitObj(self, @FSimpleRepLink);
end;

destructor TSSudokuPossibility.Destroy;
begin
  DLListRemoveObj(@FSimpleRepLink);
  Dec(FParentCoverState.FSimpleRep[FRow][FCol].Count);
  inherited;
end;

{ TSExactCoverState }

procedure TSExactCoverState.InitSimpleRep;
var
  i, j: TSNumber;
begin
  for i := Low(i) to High(i) do
    for j := Low(j) to High(j) do
    begin
      DLItemInitList(@FSimpleRep[i,j].Entries);
      FSimpleRep[i,j].Count := 0;
    end;
end;

procedure TSExactCoverState.FiniSimpleRep;
var
  i, j: TSNumber;
begin
  for i := Low(i) to High(i) do
    for j := Low(j) to High(j) do
    begin
      Assert(DLItemIsEmpty(@FSimpleRep[i,j].Entries));
      Assert(FSimpleRep[i,j].Count = 0);
    end;
end;

procedure TSExactCoverState.InitProb;
var
  Col, Row, Number, Block: TSNumber;
  Poss: TSSudokuPossibility;
  Cons: TSSudokuConstraint;
  ConsType: TSConstraintType;
begin
  for Row := Low(Row) to High(Row) do
    for Col := Low(Col) to High(Col) do
      for Number := Low(Number) to High(Number) do
      begin
        Poss := FExactCover.AddPossibility as TSSudokuPossibility;
        Assert(Assigned(Poss));
        Poss.FRow := Row;
        Poss.FCol := Col;
        Poss.FNumber := Number;
        Poss.FParentCoverState := self;
        Inc(FSimpleRep[Row][Col].Count);
        DLListInsertTail(@FSimpleRep[Row][Col].Entries, @Poss.FSimpleRepLink);
      end;
  for ConsType := Low(ConsType) to High(ConsType) do
  begin
    //Given only two parameters for each, could save space!
    case ConsType of
      sctCellContainsNum:
      begin
        for Row := Low(Row) to High(Row) do
          for Col := Low(Col) to High(Col) do
          begin
            Cons := FExactCover.AddConstraint as TSSudokuConstraint;
            Cons.FConsType := ConsType;
            Cons.FRow := Row;
            Cons.FCol := Col;
          end;
      end;
      sctRowContainsNum:
      begin
        for Row := Low(Row) to High(Row) do
          for Number := Low(Number) to High(Number) do
          begin
            Cons := FExactCover.AddConstraint as TSSudokuConstraint;
            Cons.FConsType := ConsType;
            Cons.FRow := Row;
            Cons.FNumber := Number;
          end;
      end;
      sctColContainsNum:
      begin
        for Col := Low(Col) to High(Col) do
          for Number := Low(Number) to High(Number) do
          begin
            Cons := FExactCover.AddConstraint as TSSudokuConstraint;
            Cons.FConsType := ConsType;
            Cons.FCol := Col;
            Cons.FNumber := Number;
          end;
      end;
      sctBlockContainsNum:
      begin
        for Block := Low(Block) to High(Block) do
          for Number := Low(Number) to High(Number) do
          begin
            Cons := FExactCover.AddConstraint as TSSudokuConstraint;
            Cons.FConsType := ConsType;
            Cons.FBlock := Block;
            Cons.FNumber := Number;
          end;
      end;
    else
      Assert(false);
    end;
  end;
end;

function TSExactCoverState.HandleAllocPossibility(Sender:TObject): TPossibility;
begin
  result := TSSudokuPossibility.Create;
end;

function TSExactCoverState.HandleAllocConstraint(Sender: TObject): TConstraint;
begin
  result := TSSudokuConstraint.Create;
end;

function TSExactCoverState.HandleCheckSatisfies(Possibility: TPossibility;
                                Constraint: TConstraint): boolean;
var
  Poss: TSSudokuPossibility;
  Cons: TSSudokuConstraint;
begin
  Poss := Possibility as TSSudokuPossibility;
  Cons := Constraint as TSSudokuConstraint;
  case Cons.FConsType of
    sctCellContainsNum: //Cell contains any number.
      result := (Poss.FRow = Cons.FRow) and (Poss.FCol = Cons.FCol);
    sctRowContainsNum: //Row contains specific number
      result := (Poss.FRow = Cons.FRow) and (Poss.FNumber = Cons.FNumber);
    sctColContainsNum: //Col contains specific number
      result := (Poss.FCol = Cons.FCol) and (Poss.FNumber = Cons.FNumber);
    sctBlockContainsNum: //Block contains specific number
      result := (MapToBlockSetIdx(Poss.FRow, Poss.FCol) = Cons.FBlock)
        and (Poss.FNumber = Cons.FNumber);
  else
    Assert(false);
    result := false;
  end;
end;


constructor TSExactCoverState.Create;
begin
  inherited;
  FExactCover := TExactCoverProblem.Create;
  FExactCover.OnPossibilitySatisfiesConstraint := HandleCheckSatisfies;
  FExactCover.OnAllocPossibility := HandleAllocPossibility;
  FExactCover.OnAllocConstraint := HandleAllocConstraint;
  InitSimpleRep;
  InitProb;
  FExactCover.SetupConnectivity(false);
end;

destructor TSExactCoverState.Destroy;
begin
  FExactCover.Free;
  FiniSimpleRep;
  inherited;
end;

function TSExactCoverState.GetOptEntry(Row, Col: TSNumber): TSOptNumber;
var
  Poss: TSSudokuPossibility;
begin
  result := 0;
  Poss := FSimpleRep[Row][Col].Entries.FLink.Owner as TSSudokuPossibility;
  while Assigned(Poss) do
  begin
    //Already picked something for this entry in board?
    if Poss.PartOfSolution then
    begin
      Assert(Poss.Row = Row);
      Assert(Poss.Col = Col);
      result := Poss.Number;
    end;
    Poss := Poss.FSimpleRepLink.FLink.Owner as TSSudokuPossibility;
  end;
end;

function TSExactCoverState.SetEntry(Row, Col, Entry: TSNumber): boolean;
var
  Poss: TSSudokuPossibility;
  OldEntry: TSOptNumber;
  Reset: boolean;
  Found: boolean;
begin
  OldEntry := GetOptEntry(Row, Col);
  if Entry = OldEntry then
  begin
    result := true;
    exit;
  end
  else if OldEntry <> 0 then
    ClearEntry(Row, Col);

  result := false;
  Found := false;

  Poss := FSimpleRep[Row][Col].Entries.FLink.Owner as TSSudokuPossibility;
  while Assigned(Poss) do
  begin
    Assert(Poss.Row = Row);
    Assert(Poss.Col = Col);
    if Poss.Number = Entry then
    begin
      Found := true;
      result := not Poss.StackedEliminated;
      if result then
        FExactCover.PushSolutionPossibility(Poss, true);
      break;
    end;
    Poss := Poss.FSimpleRepLink.FLink.Owner as TSSudokuPossibility;
  end;
  Assert(found);
  if (OldEntry <> 0) and not result then
  begin
    Reset := SetEntry(Row, Col, OldEntry);
    Assert(reset);
  end;
end;

procedure TSExactCoverState.ClearEntry(Row, Col: TSNumber);
var
  SavedStack: array of TSSudokuPossibility;
  SavedStackItems: integer;
  Poss: TSSudokuPossibility;
begin
  if GetOptEntry(Row,Col) = 0 then
    exit; //Nothing to clear.
  //Unfortunately, entries are a stack, so we need to pop a bunch,
  //and then omit the entry we wish to clear when re-pushing.
  SetLength(SavedStack, FExactCover.PartialSolutionStackCount);
  SavedStackItems := 0;
  Poss := FExactCover.PopTopSolutionPossibility(true) as TSSudokuPossibility;
  while Assigned(Poss) do
  begin
    Assert(Poss.StackedEliminated);
    Assert(Poss.PartOfSolution);
    if (Poss.Row = Row) and (Poss.Col = Col) then
      break
    else
    begin
      SavedStack[SavedStackItems] := Poss;
      Inc(SavedStackItems);
    end;
    Poss := FExactCover.TopPartialSolutionPossibility as TSSudokuPossibility;
  end;
  Assert(Assigned(Poss)); //Because GetOptEntry <> 0
  //One item we do not wish to re-push, but the rest saved in the stack, we do.
  while SavedStackItems > 0 do
  begin
    FExactCover.PushSolutionPossibility(SavedStack[Pred(SavedStackItems)], true);
    Dec(SavedStackItems);
  end;
end;


function TSExactCoverState.GetComplete: boolean;
begin
  result := (FExactCover.IncludedPossibilityCount = 0)
    and (FExactCover.IncludedConstraintCount = 0); //Have an exact cover.
end;

function TSExactCoverState.GetProceedable: boolean;
var
  P: TSSudokuPossibility;
begin
  P := FindNextMoveInt;
  result := Assigned(P);
end;

function TSExactCoverState.FindNextMovePosInt(Row, Col: TSNumber):TSSudokuPossibility;
var
  Poss: TSSudokuPossibility;
begin
  result := nil;
  Poss := FSimpleRep[Row][Col].Entries.FLink.Owner as TSSudokuPossibility;
  while Assigned(Poss) do
  begin
    //Already picked something for this entry in board?
    if Poss.PartOfSolution then
    begin
      result := nil;
      break;
    end
    else
    begin
      if not Poss.StackedEliminated then
      begin
        //Want just one possibility not stacked eliminated.
        if not Assigned(result) then
          result := Poss
        else
        begin
          result := nil;
          break; //more than one possibility.
        end;
      end;
    end;
    Poss := Poss.FSimpleRepLink.FLink.Owner as TSSudokuPossibility;
  end;
end;


function TSExactCoverState.FindNextMoveInt: TSSudokuPossibility;
var
  Row, Col: TSNumber;
begin
  for Row := Low(Row) to High(Row) do
    for Col := Low(Col) to High(Col) do
    begin
      result := FindNextMovePosInt(Row, Col);
      if Assigned(result) then
        exit;
    end;
end;

function TSExactCoverState.FindNextMove(var Row, Col: TSNumber): TSNumberSet;
var
  Poss: TSSudokuPossibility;
begin
  result := EmptySet;
  Poss := FindNextMoveInt;
  if Assigned(Poss) then
  begin
    Row := Poss.Row;
    Col := Poss.Col;
    result := TSSetIndividual(Poss.Number);
  end
end;

function TSExactCoverState.EvolveIfSingular(Row, Col: TSNumber; NSet: TSNumberSet): boolean;
var
  Poss: TSSudokuPossibility;
begin
  result := false;
  Poss := FindNextMovePosInt(Row, Col);
  if Assigned(Poss) then
  begin
    Assert(Poss.Row = Row);
    Assert(Poss.Col = Col);
    if TSSetIndividual(Poss.Number) = NSet then
    begin
      result := SetEntry(Row, Col, Poss.Number);
      Assert(result);
    end
    else
      Assert(false);
  end;
end;

procedure TSExactCoverState.Clear;
var
  P: TObject;
begin
  repeat
    P := FExactCover.PopTopSolutionPossibility(true);
  until not Assigned(P);
end;

procedure TSExactCoverState.SetCoverNotify(CoverNotify: TCoverNotifyEvent);
begin
  FExactCover.OnCoverNotify := CoverNotify;
end;

function TSExactCoverState.GetCoverNotify: TCoverNotifyEvent;
begin
  result := FExactCover.OnCoverNotify;
end;

{ TSExactCoverSolver }

procedure TSExactCoverSolver.HandleCoverNotify(Sender: TObject;
                            TerminationType: TCoverTerminationType;
                            var Stop: boolean);
begin
  //TODO - Stats for when the algorithm has to make a decision?
  case TerminationType of
    cttOKExactCover:
    begin
      Inc(FStats.CorrectSolutions);
      FReturnSolved := true;
      case FStats.CorrectSolutions of
        1:
        begin
          if not Assigned(FUniqueSolution) then
          begin
            FUniqueSolution := CreateBoardState;
            (FUniqueSolution as TSExactCoverState).OnCoverNotify := nil;
          end;
          FUniqueSolution.Assign(FBoardState);
          Assert(FUniqueSolution.Complete);
        end;
        2:
        begin
          FUniqueSolution.Free;
          FUniqueSolution := nil;
        end;
      end;
    end;
    cttOKUnderconstrained,
    cttFailGenerallyOverconstrained,
    cttFailSpecificallyOverconstrained:
      Inc(FStats.DeadEnds);
  else
    Assert(false);
  end;
end;

function TSExactCoverSolver.CreateBoardState: TSAbstractBoardState;
begin
  result := TSExactCoverState.Create;
  (result as TSExactCoverState).OnCoverNotify := HandleCoverNotify;
end;

function TSExactCoverSolver.Solve: boolean;
begin
  FStats.StartTime := Now;
  FReturnSolved := false;
  (FBoardState as TSExactCoverState).FExactCover.AlgorithmX;
  FStats.ElapsedTime := Now - FStats.StartTime;
  result := FReturnSolved;
end;


end.
