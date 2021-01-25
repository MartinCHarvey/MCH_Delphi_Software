unit ExactCover;

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

(*

Notes:

The rows reperesent possibilities, columns represent constrainsts (or subsets).
To solve Exact Cover we need to reduce the matrix by removing rows so that
we end up with a subset of the rows such that each column has exactly one
entry in it.

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

Application to Sudoku.

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

TODO - Detecting under and overconstraint.
 This will be not enough 1's or not able to eliminate enough rows.
 Suspect possibly problem specific so will not include it in the general solution here.

TODO - (Sudoku and other problems) - initial placements.
   - Do we just remove possibilities from rows without removing any constraints,
   - Or do we run the algorithm removing columns so that we also remove the constraints
     that have been satisfied?

*)

interface

{
  TODO - Review inlines and optimisations after profiling.
}

{$IFOPT C+}
{$DEFINE PICKER_STRINGENT_CHECKS}
{$ENDIF}

uses SysUtils, SparseMatrix, Trackables, DLList;

type

  TExactCoverCell = class;

  //Just keep an included list.
  //Debug checks can check counts by using parent sparse matrix iterators.
  //Excluded nodes do not need to belong to an excluded list.

  TExactCoverHeader = class(THeader)
  private
    FStackLink,
    FInclHeaderLink,
    FInclCellsHead: TDLEntry;
    FPrevInclSave: PDLEntry;
    FInclCellsCount: integer;
  protected
    function GetIncluded: boolean; inline;
    procedure SetIncluded(NewIncluded: boolean); //Runtime include / exclude by row / col
    procedure InitAllIncluded; //Startup IncludeAll
    function CheckAllIncluded: boolean;
    procedure GenericUnlinkAndFree; override;

    //And some handler functions to allow later dependent code to keep track
    //of included rows/cols and how many cells they contain.
    //TODO - If virtual functions / class traverse too slow,
    //then use event handlers instead.
    procedure ClientHandleExclude; virtual;
    procedure ClientHandleInclude(InclCellCount: integer); virtual;
    procedure ClientHandleInclCountChange(OldCount, NewCount: integer); virtual;
  public
    constructor Create;

    function FirstIncludedItem: TExactCoverCell; inline;
    function LastIncludedItem: TExactCoverCell; inline;

    function NextIncludedHeader: TExactCoverHeader; inline;
    function PrevIncludedHeader: TExactCoverHeader; inline;

    property Included: boolean read GetIncluded write SetIncluded;
    property InclCellsCount: integer read FInclCellsCount; //Included cells when row itself not excluded.
  end;

  TExactCoverCell = class(TMatrixCell)
  private
    FInclRowLink, FInclColLink: TDLEntry;
    FPrevRowSave, FPrevColSave: PDLEntry;
  protected
    function GetIncluded: boolean; inline;//Both row and col linked, both row saves NIL.

    procedure SetIncluded(NewIncluded: boolean; Row: boolean);  //Runtime include / exclude row / col
    procedure InitAllIncluded(Row: boolean); //Startup IncludeAll by Row / Col
    function CheckAllIncluded(Row: boolean): boolean;
    procedure GenericUnlinkAndFree; override;
  public
    constructor Create;

    function NextIncludedAlongRow: TExactCoverCell; inline; //Row included
    function NextIncludedDownColumn: TExactCoverCell; inline; //Col included
    function PrevIncludedAlongRow: TExactCoverCell; inline; //Row included
    function PrevIncludedDownColumn: TExactCoverCell; inline; //Col included

    property Included: boolean read GetIncluded; //Both row and col included.
  end;

  TExactCoverProblem = class;

  TExactCoverMatrix = class(TSparseMatrix)
  private
    FParentProblem: TExactCoverProblem;
    FIncludedRows, FIncludedCols: TDLEntry;
    FIncludedRowCount, FIncludedColCount: integer;
    FStackedExcludes: TDLEntry;
    FStackedCount: integer;
  protected
    function CheckAllIncluded: boolean;
  public
    //NB NB Assumption in this class is that after adding rows, cols, and cells
    //you call "init all included" before including / exluding rows.

    //If you won't want to do that, then you need to rewrite without
    //"previous item save" pointers, traversing rows / cols to find
    //out where to reinsert, which gets tricky, because when excluding a row / col,
    //we are only tearing down "included" pointers to items not already excluded
    //from solution - hence why a stack when including / excluding rows.
    constructor Create;
    destructor Destroy; override;

    procedure InitAllIncluded; //Startup IncludeAll, and make assertions about structure.

    procedure ExcludeAndPush(Header: TExactCoverHeader);
    function PopAndInclude: TExactCoverHeader;
    function PeekTop: TExactCoverHeader; inline;
    //TODO - Do we need to keep track of how many to push / pop at once?
    //Do we need to push / pop in bunches of stuff?

    function AllocCell: TMatrixCell; override;
    procedure FreeCell(Cell: TMatrixCell); override;
    function AllocHeader(Row: boolean): THeader; override;
    procedure FreeHeader(Header: THeader); override;

    function FirstIncludedRow: TExactCoverHeader; inline;
    function LastIncludedRow: TExactCoverHeader; inline;
    function FirstIncludedColumn: TExactCoverHeader; inline;
    function LastIncludedColumn: TExactCoverHeader; inline;

    property AllIncluded: boolean read CheckAllIncluded;
    property IncludedRowCount: integer read FIncludedRowCount;
    property IncludedColCount: integer read FIncludedColCount;
    property StackedHeadersPushed: integer read FStackedCount;
  end;

  //NB. Because of the ways that rows and cols can be included / excluded,
  //headers can have nonzero entry counts despite the fact the entire row / col
  //has been excluded from the matrix as a whole.
  TAbstractPicker = class;

  //TODO - Get rid of these tiny virtuals if a perf problem.
  //Change to events in theExactCoverHeader.
  TPickedHeader = class(TExactCoverHeader)
  private
    FPicker: TAbstractPicker;
  protected
    procedure ClientHandleExclude; override;
    procedure ClientHandleInclude(InclCellCount: integer); override;
    procedure ClientHandleInclCountChange(OldCount, NewCount: integer); override;
  end;

  TConstraint = class (TPickedHeader)
  protected
    function GetStackedSatisfied: boolean; inline;
  public
    property StackedSatisfied: boolean read GetStackedSatisfied;
  end;

  TPossibility = class(TPickedHeader)
  private
    FPartSolutionLink: TDLEntry;
  protected
    function GetStackedEliminated: boolean; inline;
    function GetPartOfSolution:boolean; inline;
    procedure GenericUnlinkAndFree; override;
  public
    constructor Create;
    property StackedEliminated: boolean read GetStackedEliminated;
    property PartOfSolution: boolean read GetPartOfSolution;
  end;

  TPossConstraintMethod = function(Possibility: TPossibility;
                                    Constraint: TConstraint): boolean of object;

  TOnAllocPossibilityEvent = function(Sender:TObject): TPossibility of object;
  TOnAllocConstraintEvent = function(Sender: TObject): TConstraint of object;
  TOnAllocCellEvent = function(Sender: TObject): TExactCoverCell of object;

  TOnFreePossibilityEvent = procedure(Sender: TObject; Possibility: TPossibility) of object;
  TOnFreeConstraintEvent = procedure(Sender: TObject; Constraint: TConstraint) of object;
  TOnFreeCellEvent = procedure(Sender: TObject; Cell: TExactCoverCell) of object;

  EExactCoverException = class(Exception);

{$IFDEF USE_TRACKABLES}
  TExactCoverProblem = class(TTrackable)
{$ELSE}
  TExactCoverProblem = class
{$ENDIF}
  private
    FMatrix: TExactCoverMatrix;
    FConstraintPicker: TAbstractPicker;
    FPossibilityPicker: TAbstractPicker;

    FOnAllocCell: TOnAllocCellEvent;
    FOnAllocPossibility:TOnAllocPossibilityEvent;
    FOnAllocConstraint:TOnAllocConstraintEvent;

    FOnFreeCell: TOnFreeCellEvent;
    FOnFreePossibility: TOnFreePossibilityEvent;
    FOnFreeConstraint: TOnFreeConstraintEvent;

    FConnectivityCurrent: boolean;
    FConnectivityComplete: boolean;
    FPossConsCheck: TPossConstraintMethod;
    FPartSolutionStack: TDLEntry;
    FPartSolutionStackCount: integer;
  protected
    function GetRowsColsStacked: integer; inline;

    function DoAllocCell: TMatrixCell;
    procedure DoFreeCell(Cell: TMatrixCell);
    function DoAllocHeader(Row: boolean): THeader;
    procedure DoFreeHeader(Header: THeader);
  public
    constructor Create;
    destructor Destroy; override;

    //Iteration through all possibilities and constraints at all times.
    function FirstPossibility: TPossibility; inline;
    function LastPossibility: TPossibility; inline;
    function NextPossibility(Poss: TPossibility): TPossibility; inline;
    function PrevPossibility(Poss: TPossibility): TPossibility; inline;

    //Iteration through all possibilities and constraints at all times.
    function FirstConstraint: TConstraint; inline;
    function LastConstraint: TConstraint; inline;
    function NextConstraint(Cons: TConstraint): TConstraint; inline;
    function PrevConstraint(Cons: TConstraint): TConstraint; inline;

    //Change possibilities and constraints when not solving (no rows/cols stacked).
    function AddPossibility: TPossibility;
    procedure DeletePossibility(Poss: TPossibility);

    //Change possibilities and constraints when not solving.
    function AddConstraint: TConstraint;
    procedure DeleteConstraint(Cons: TConstraint);

    //Setup connectivity when not solving.
    procedure ClearConnectivity;
    //If allow partial addition, re-scans poss satisfying no constraints, contraints satisfying
    //no poss, and allows addition, else clears and re-scans everything.
    procedure SetupConnectivity(AllowPartialAddition: boolean = false);


    //Possibilities which have not only been eliminated from matrix, but
    //stacked as part of a partial solution.
    function TopPartialSolutionPossibility: TPossibility; inline;
    function NextPartialSolutionPossibility(Poss: TPossibility): TPossibility; inline;

    //TODO - Iterate remaining constraints not yet satisfied?

    //If RunOneAlgX step, then performs AlgX row-column elimination for
    //that possibility, else just elimininates the possibility (no eliminate constraint).
    //May need to do this for "givens" or part-initial solutions.
    procedure StackSolutionPossibility(Poss: TPossibility; RunOneAlgXStep: boolean = true);
    function PopTopSolutionPossibility: TPossibility;
    procedure AlgorithmX;

    property RowsColsStacked: integer read GetRowsColsStacked;
    property ConnectivityCurrent: boolean read FConnectivityCurrent;
    property ConnectivityComplete: boolean read FConnectivityCurrent;
    property OnPossibilitySatisfiesConstraint: TPossConstraintMethod
      read FPossConsCheck write FPossConsCheck;

    property OnAllocateCell: TOnAllocCellEvent read FOnAllocCell write FOnAllocCell;
    property OnAllocPossibility:TOnAllocPossibilityEvent read FOnAllocPossibility write FOnAllocPossibility;
    property OnAllocConstraint:TOnAllocConstraintEvent read FOnAllocConstraint write FOnAllocConstraint;
    property OnFreeCell: TOnFreeCellEvent read FOnFreeCell write FOnFreeCell;
    property OnFreePossibility:TOnFreePossibilityEvent read FOnFreePossibility write FOnFreePossibility;
    property OnFreeConstraint:TOnFreeConstraintEvent read FOnFreeConstraint write FOnFreeConstraint;
    property PartialSolutionStackCount: integer read FPartSolutionStackCount;
  end;

  //TODO - Opt involving removal of unnecessary virtuals...
  //Conditional compile if necessary.

{$IFDEF USE_TRACKABLES}
  TAbstractPicker = class(TTrackable)
{$ELSE}
  TAbstractPicker = class
{$ENDIF}
  private
    FParentProblem: TExactCoverProblem;
  protected
  public
    procedure ExcludeHeader(Header: TPickedHeader); virtual;
    procedure IncludeHeader(Header: TPickedHeader; InitialCount: integer); virtual;
    procedure ConstraintCountChange(Header: TPickedHeader; OldCount, NewCount: integer); virtual;

    procedure PickerPushNewLevel; virtual;
    procedure PickerPopOldLevel; virtual;

    function PickOne:TPickedHeader; virtual; abstract;
  end;


{$IFDEF USE_TRACKABLES}
  TNaivePickerStackEntry = class(TTrackable)
{$ELSE}
  TNaivePickerStackEntry = class
{$ENDIF}
  private
    FStackLink: TDLEntry;
{$IFDEF PICKER_STRINGENT_CHECKS}
    FHeadersThisLevel: array of TPickedHeader;
{$ENDIF}
    FLastPickedThisLevel: TPickedHeader;
  public
    constructor Create;
    destructor Destroy; override;
  end;

  //Naive picker fairly easily guaranteed to pick all possible
  //constraints at any particular level.
  //Naive picker also contains debug checks.
  TNaivePicker = class (TAbstractPicker)
  private
    FStackHead: TDLEntry;
    FPickingPossibilities: boolean;
  protected
{$IFDEF PICKER_STRINGENT_CHECKS}
    procedure CheckHeaderOrderAgrees;
{$ENDIF}
  public
    procedure ExcludeHeader(Header: TPickedHeader); override;
    procedure IncludeHeader(Header: TPickedHeader; InitialCount: integer); override;
    procedure ConstraintCountChange(Header: TPickedHeader; OldCount, NewCount: integer); override;
    procedure PickerPushNewLevel; override;
    procedure PickerPopOldLevel; override;
    function PickOne:TPickedHeader; override;

    constructor Create;
    destructor Destroy; override;
    property PickingPossibilities: boolean read FPickingPossibilities write FPickingPossibilities;
  end;

  // TODO TLeastChoice picker.
  //
  // The constraint picker needs to systematically pick all possible columns,
  // but starting from those which have the lowest number of
  // possibilities as the algorithm moves forwards and backtracks.
  // (See later TODO's in code).
  // Depending on row / column interdependent include / uninclude order
  // (whether include / uninclude is done strictly in stack order or not),
  // the innermost level of lists might or might not need to be a stack or a queue.
  // - Include / uninclude is a stack, sublists should be a queue,
  // - Include / uninclude is a queue, sublists should be a stack.

  //TODO - Also optimise with lookasides to remove mem alloc overhead.

  //Possibility picker is potentially easier - just pick one at random...
  TLeastChoicePicker = class(TAbstractPicker)
  //TODO - Implement this if required.
  end;

implementation

const
  S_CANNOT_CHANGE_MATRIX_WHILST_SOLVING = 'Cannot change possibilities or constraints whilst partly solved.';
  S_NEED_CONNECTIVITY_HANDLER = 'Need OnPossibilitySatisfiesConstraint handler to proceed.';
  S_NO_POSS_OR_CONS_FOR_SETUP = 'Matrix contains no possibilities or constraints, cannot setup connectivity.';
  S_MATRIX_IS_EMPTY = 'Matrix has possibilities and contstraints, but no connections between them.';
  S_DUPLICATE_POSS_ELIMINATION = 'Cannot stack possibility: already either included in solution or discounted.';
  S_CANNOT_SOLVE_UNTIL_CONNECTIVITY_SETUP = 'Connectivity matrix needs to have been setup/updated before solving.';

{ TExactCoverHeader }

function TExactCoverHeader.FirstIncludedItem: TExactCoverCell;
begin
  result := FInclCellsHead.FLink.Owner as TExactCoverCell;
end;

function TExactCoverHeader.LastIncludedItem: TExactCoverCell;
begin
  result := FInclCellsHead.BLink.Owner as TExactCoverCell;
end;

function TExactCoverHeader.NextIncludedHeader: TExactCoverHeader;
begin
  if not DLItemIsEmpty(@FInclHeaderLink) then
    result := FInclHeaderLink.FLink.Owner as TExactCoverHeader
  else
  begin
    Assert(false);
    result := nil;
  end;
end;

function TExactCoverHeader.PrevIncludedHeader: TExactCoverHeader;
begin
  if not DLItemIsEmpty(@FInclHeaderLink) then
    result := FInclHeaderLink.BLink.Owner as TExactCoverHeader
  else
  begin
    Assert(false);
    result := nil;
  end;
end;

function TExactCoverHeader.GetIncluded: boolean;
begin
 Assert(DLItemIsEmpty(@FInclHeaderLink) <> DLItemIsEmpty(@FStackLink));
 //This is not called by CheckAllIncluded, so cannot be called in
 //initial (inconsistent) state.
 Assert(Assigned(FPrevInclSave) = DlItemIsEmpty(@FInclHeaderLink));
 result := not DLItemIsEmpty(@FInclHeaderLink);
end;

procedure TExactCoverHeader.SetIncluded(NewIncluded: boolean);
var
  M: TExactCoverMatrix;
  C: TExactCoverCell;
begin
  Assert(NewIncluded <> Included);
  M := Matrix as TExactCoverMatrix;
  //Only need to remove row /col links from included cells, since removal
  //re-insertion is a stack.
  if NewIncluded then
  begin
    //Insert back into included header list.
    Assert(Assigned(FPrevInclSave));
    DLItemInsertAfter(FPrevInclSave, @FInclHeaderLink);
    FPrevInclSave := nil;
    if IsRow then
    begin
      Inc(M.FIncludedRowCount);
      Assert(M.FIncludedRowCount <= M.RowCount);
    end
    else
    begin
      Inc(M.FIncludedColCount);
      Assert(M.FIncludedColCount <= M.ColCount);
    end;
    ClientHandleInclude(FInclCellsCount);
  end
  else
  begin
    ClientHandleExclude;
    //Remove from header list.
    Assert(not Assigned(FPrevInclSave));
    FPrevInclSave := FInclHeaderLink.BLink;
    DLListRemoveObj(@FInclHeaderLink);
    if IsRow then
    begin
      Dec(M.FIncludedRowCount);
      Assert(M.FIncludedRowCount >= 0);
    end
    else
    begin
      Dec(M.FIncludedColCount);
      Assert(M.FIncludedColCount >= 0);
    end;
  end;
  //Add/Remove Remove each cell in row / coll from coll / row

  //TODO - Potentially some subtlety here about row/column order when considering
  //the constraint picker: if include / uninclude happy stack-wise, then the picker
  //should form a list of queues, and if it's always in the same order,
  //then the picker should be a list of stacks...
  //TODO - These assumptions need testing (and logging).
  C := FirstIncludedItem;
  while Assigned(C) do
  begin
    C.SetIncluded(NewIncluded, IsRow);
    if IsRow then
      C := C.NextIncludedAlongRow
    else
      C := C.NextIncludedDownColumn;
  end;
end;

procedure TExactCoverHeader.InitAllIncluded; //Startup IncludeAll
var
  Cell: TExactCoverCell;
begin
  if not CheckAllIncluded then
  begin
    Assert(DLItemIsEmpty(@FStackLink));
    Assert(DLItemIsEmpty(@FInclHeaderLink));
    Assert(DlItemIsEmpty(@FInclCellsHead));
    Assert(not Assigned(FPrevInclSave));
    Assert(FInclCellsCount = 0);
    Cell := FirstItem as TExactCoverCell;
    while Assigned(Cell) do
    begin
      Cell.InitAllIncluded(IsRow);
      if IsRow then
        Cell := Cell.NextCellAlongRow as TExactCoverCell
      else
        Cell := Cell.NextCellDownColumn as TExactCoverCell
    end;
    ClientHandleInclude(FInclCellsCount);
  end;
end;

function TExactCoverHeader.CheckAllIncluded: boolean;
var
  Cell, InclCell: TExactCoverCell;
begin
  result := InclCellsCount = EntryCount;
  if result then
  begin
    //Check cells are in the right order.
    Cell := FirstItem as TExactCoverCell;
    InclCell := FirstIncludedItem;
    Assert(Cell = InclCell);
    while Assigned(Cell) do
    begin
      Assert(Cell.CheckAllIncluded(IsRow));
      if IsRow then
      begin
        Cell := Cell.NextCellAlongRow as TExactCoverCell;
        InclCell := InclCell.NextIncludedAlongRow;
      end
      else
      begin
        Cell := Cell.NextCellDownColumn as TExactCoverCell;
        InclCell := InclCell.NextIncludedDownColumn;
      end;
      Assert(Cell = InclCell);
    end;
  end;
end;

procedure TExactCoverHeader.GenericUnlinkAndFree;
begin
  if Included then
    ClientHandleExclude;

  if not DlItemIsEmpty(@FStackLink) then
  begin
    DLListRemoveObj(@FStackLink);
    Assert((Matrix as TExactCoverMatrix).FStackedCount > 0);
    Dec((Matrix as TExactCoverMatrix).FStackedCount);
  end;
  if not DLItemIsEmpty(@FInclHeaderLink) then
  begin
    DLListRemoveObj(@FInclHeaderLink);
    if self.IsRow then
      Dec((Matrix as TExactCoverMatrix).FIncludedRowCount)
    else
      Dec((Matrix as TExactCoverMatrix).FIncludedColCount);
  end;
  inherited;
  Assert(DlItemIsEmpty(@FInclCellsHead));
  FPrevInclSave := nil;
  Assert(FInclCellsCount = 0);
end;

procedure TExactCoverHeader.ClientHandleExclude;
begin
end;

procedure TExactCoverHeader.ClientHandleInclude(InclCellCount: integer);
begin
end;

procedure TExactCoverHeader.ClientHandleInclCountChange(OldCount, NewCount: integer);
begin
end;

constructor TExactCoverHeader.Create;
begin
  inherited;
  DLItemInitObj(self, @FStackLink);
  DLItemInitObj(self, @FInclHeaderLink);
  DLItemInitList(@FInclCellsHead);
end;

{ TExactCoverCell }

function TExactCoverCell.NextIncludedAlongRow: TExactCoverCell;
begin
  if not DLItemIsEmpty(@FInclRowLink) then
    result := FInclRowLink.FLink.Owner as TExactCoverCell
  else
  begin
    Assert(false);
    result := nil;
  end;
end;

function TExactCoverCell.NextIncludedDownColumn: TExactCoverCell;
begin
  if not DLItemIsEmpty(@FInclColLink) then
    result := FInclColLink.FLink.Owner as TExactCoverCell
  else
  begin
    Assert(false);
    result := nil;
  end;
end;

function TExactCoverCell.PrevIncludedAlongRow: TExactCoverCell;
begin
  if not DLItemIsEmpty(@FInclRowLink) then
    result := FInclRowLink.BLink.Owner as TExactCoverCell
  else
  begin
    Assert(false);
    result := nil;
  end;
end;

function TExactCoverCell.PrevIncludedDownColumn: TExactCoverCell;
begin
  if not DLItemIsEmpty(@FInclColLink) then
    result := FInclColLink.BLink.Owner as TExactCoverCell
  else
  begin
    Assert(false);
    result := nil;
  end;
end;

procedure TExactCoverCell.SetIncluded(NewIncluded: boolean; Row: boolean);
var
  Hdr: TExactCoverHeader;
begin
  //NB: When we call this we are moving along the row / col of included
  //cells in that row / col. Hence, we need to insert / remove from the col / row.
  //Traverse long rows = remove / add col links and vice versa.
  if NewIncluded then
  begin
    if Row then
    begin
      Assert(Assigned(FPrevColSave));
      DLItemInsertAfter(FPrevColSave, @FInclColLink);
      FPrevColSave := nil;
      Hdr := (ColHeader as TExactCoverHeader);
      Inc(Hdr.FInclCellsCount);
      Hdr.ClientHandleInclCountChange(Pred(Hdr.FInclCellsCount), Hdr.FInclCellsCount);
    end
    else
    begin
      Assert(Assigned(FPrevRowSave));
      DLItemInsertAfter(FPrevRowSave, @FInclRowLink);
      FPrevRowSave := nil;
      Hdr := (RowHeader as TExactCoverHeader);
      Inc(Hdr.FInclCellsCount);
      Hdr.ClientHandleInclCountChange(Pred(Hdr.FInclCellsCount), Hdr.FInclCellsCount);
    end;
  end
  else
  begin
    if Row then
    begin
      Assert(not Assigned(FPrevColSave));
      FPrevColSave := FInclColLink.BLink;
      DLListRemoveObj(@FInclColLink);
      Hdr := (ColHeader as TExactCoverHeader);
      Dec(Hdr.FInclCellsCount);
      Hdr.ClientHandleInclCountChange(Succ(Hdr.FInclCellsCount), Hdr.FInclCellsCount);
    end
    else
    begin
      Assert(not Assigned(FPrevRowSave));
      FPrevRowSave := FInclRowLink.BLink;
      DLListRemoveObj(@FInclRowLink);
      Hdr := (RowHeader as TExactCoverHeader);
      Dec(Hdr.FInclCellsCount);
      Hdr.ClientHandleInclCountChange(Succ(Hdr.FInclCellsCount), Hdr.FInclCellsCount);
    end;
  end;
end;

function TExactCoverCell.GetIncluded: boolean; //Both row and col linked, both row saves NIL.
begin
  result := not (DLItemIsEmpty(@FInclRowLink) or DLItemIsEmpty(@FInclColLink));
  //These do not hold at very first initialization, but in such cases,
  //CheckAllIncluded should not be called for cells, only for individual rows
  //(where test shd fail).
  Assert(DLItemIsEmpty(@FInclRowLink) = Assigned(FPrevRowSave));
  Assert(DlItemIsEmpty(@FInclColLink) = Assigned(FPRevColSave));
end;

function TExactCoverCell.CheckAllIncluded(Row: boolean): boolean;
begin
  //Do not expect assertions above to hold, only check inclusion or pre-row or per-col basis.
  if Row then
    result := not (DLItemIsEmpty(@FInclRowLink))
  else
    result := not (DLItemIsEmpty(@FInclColLink));
end;

procedure TExactCoverCell.InitAllIncluded(Row: boolean);
var
  Header: TExactCoverHeader;
begin
  if not CheckAllIncluded(Row) then
  begin
    if Row then
    begin
      Assert(DlItemIsEmpty(@FInclRowLink));
      Assert(not Assigned(FPrevRowSave));
      Header := RowHeader as TExactCoverHeader;
      DLListInsertTail(@Header.FInclCellsHead, @FInclRowLink);
      Assert(Header.IsRow = Row);
      Inc(Header.FInclCellsCount);
    end
    else
    begin
      Assert(DLItemIsEmpty(@FInclColLink));
      Assert(not Assigned(FPrevColSave));
      Header := ColHeader as TExactCoverHeader;
      DLListInsertTail(@Header.FInclCellsHead, @FInclColLink);
      Assert(Header.IsRow = Row);
      Inc(Header.FInclCellsCount);
    end;
  end;
end;

procedure TExactCoverCell.GenericUnlinkAndFree;
begin
  //Not sure whether full Incl set up yet when destroying, so can't fully constrain
  //all possibs
  Assert((not Assigned(FPrevRowSave)) or (DlItemIsEmpty(@FInclRowLink)));
  Assert((not Assigned(FPrevColSave)) or (DlItemIsEmpty(@FInclColLink)));
  //OK to bin stacking info.
  FPrevRowSave := nil;
  FPrevColSave := nil;
  //Don't notify client of included cell count changes here,
  //Matrix being destroyed, so we'll just notify
  //of entire row exclusion in header destructors.
  if not DlItemIsEmpty(@FInclRowLink) then
  begin
    DLListRemoveObj(@FInclRowLink);
    Dec((RowHeader as TExactCoverHeader).FInclCellsCount);
  end;
  if not DlItemIsEmpty(@FInclColLink) then
  begin
    DLListRemoveObj(@FInclColLink);
    Dec((ColHeader as TExactCoverHeader).FInclCellsCount);
  end;
  inherited;
end;

constructor TExactCoverCell.Create;
begin
  inherited;
  DlItemInitObj(self, @FInclRowLink);
  DlItemInitObj(self, @FInclColLink);
end;

{ TExactCoverMatrix }

function TExactCoverMatrix.FirstIncludedRow: TExactCoverHeader;
begin
  result := Self.FIncludedRows.FLink.Owner as TExactCoverHeader;
end;

function TExactCoverMatrix.LastIncludedRow: TExactCoverHeader;
begin
  result := Self.FIncludedRows.BLink.Owner as TExactCoverHeader;
end;

function TExactCoverMatrix.FirstIncludedColumn: TExactCoverHeader;
begin
  result := Self.FIncludedCols.FLink.Owner as TExactCoverHeader;
end;

function TExactCoverMatrix.LastIncludedColumn: TExactCoverHeader;
begin
  result := Self.FIncludedCols.BLink.Owner as TExactCoverHeader;
end;

procedure TExactCoverMatrix.InitAllIncluded; //Startup IncludeAll, and make assertions about structure.
var
  Header: TExactCoverHeader;
begin
  while not DLItemIsEmpty(@FStackedExcludes) do
    PopAndInclude;

  if not CheckAllIncluded then
  begin
    //Lets do include all the rows by row link, and then include all the cols
    //by col link.
    Assert(DLItemIsEmpty(@FIncludedRows));
    Assert(DlItemIsEmpty(@FIncludedCols));
    Assert(FIncludedRowCount = 0);
    Assert(FIncludedColCount = 0);
    //And checked nothing is stacked
    Assert(DLItemIsEmpty(@FStackedExcludes));
    Assert(FStackedCount =0);

    Header := FirstRow as TExactCoverHeader;
    while Assigned(Header) do
    begin
      Header.InitAllIncluded;
      DLListInsertTail(@FIncludedRows, @Header.FInclHeaderLink);
      Inc(FIncludedRowCount);
      Header := NextRow(Header) as TExactCoverHeader;
    end;
    Header := FirstColumn as TExactCoverHeader;
    while Assigned(Header) do
    begin
      Header.InitAllIncluded;
      DLListInsertTail(@FIncludedCols, @Header.FInclHeaderLink);
      Inc(FIncludedColCount);
      Header := NextColumn(Header) as TExactCoverHeader;
    end;
  end;
  Assert(CheckAllIncluded);
end;

function TExactCoverMatrix.CheckAllIncluded: boolean;
var
  Header, InclHeader: TExactCoverHeader;
begin
  Assert(RowCount > 0);
  Assert(ColCount > 0);
  result := (IncludedRowCount = RowCount) and (IncludedColCount = ColCount);
  if result then
  begin
    Header := FirstRow as TExactCoverHeader;
    InclHeader := FirstIncludedRow;
    Assert(Header = InclHeader);
    while Assigned(Header) do
    begin
      Assert(Header.CheckAllIncluded);
      Header := NextRow(Header) as TExactCoverHeader;
      InclHeader := InclHeader.NextIncludedHeader;
      Assert(InclHeader = Header);
    end;
    Header := FirstColumn as TExactCoverHeader;
    InclHeader := FirstIncludedColumn;
    Assert(Header = InclHeader);
    while Assigned(Header) do
    begin
      Assert(Header.CheckAllIncluded);
      Header := NextColumn(Header) as TExactCoverHeader;
      InclHeader := InclHeader.NextIncludedHeader;
      Assert(InclHeader = Header);
    end;
  end;
end;

procedure TExactCoverMatrix.ExcludeAndPush(Header: TExactCoverHeader);
begin
  Assert(Assigned(Header));
  Assert(Header.Included);
  Assert(DLItemIsEmpty(@Header.FStackLink));
  Header.Included := false;
  DLListInsertHead(@FStackedExcludes, @Header.FStackLink);
  Inc(FStackedCount);
end;

function TExactCoverMatrix.PopAndInclude: TExactCoverHeader;
var
  Entry: PDLEntry;
{$IFOPT C+}
  TestEntry: PDLEntry;
{$ENDIF}
begin
  if not DLItemIsEmpty(@FStackedExcludes) then
  begin
    Entry := FStackedExcludes.FLink;
    result := Entry.Owner as TExactCoverHeader;
    Assert(not result.Included); //Move this up because expects consistent state.
{$IFOPT C+}
    TestEntry := Entry;
{$ENDIF}
    result.Included := true;
    Entry := DLListRemoveHead(@FStackedExcludes);
    Assert(FStackedCount > 0);
    Dec(FStackedCount);
{$IFOPT C+}
    Assert(Entry = TestEntry);
{$ENDIF}
    Assert(Assigned(result));
  end
  else
    result := nil;
end;

function TExactCoverMatrix.PeekTop: TExactCoverHeader;
begin
  result := FStackedExcludes.FLink.Owner as TExactCoverHeader;
end;

constructor TExactCoverMatrix.Create;
begin
  inherited;
  DLItemInitList(@FIncludedRows);
  DLItemInitList(@FIncludedCols);
  DLItemInitList(@FStackedExcludes);
end;

destructor TExactCoverMatrix.Destroy;
begin
  inherited; //Innards should clean themselves up.
  //Potentially include post-destroy virtual here. To investigate.
  Assert(DLItemIsEmpty(@FIncludedRows));
  Assert(DlItemIsEmpty(@FIncludedCols));
  Assert(FIncludedRowCount = 0);
  Assert(FIncludedColCount = 0);
  Assert(DlItemIsEmpty(@FStackedExcludes));
  Assert(FStackedCount = 0);
end;

function TExactCoverMatrix.AllocCell: TMatrixCell;
begin
  if Assigned(FParentProblem) then
    result := FParentProblem.DoAllocCell
  else
    result := TExactCoverCell.Create;
end;

function TExactCoverMatrix.AllocHeader(Row: boolean): THeader;
begin
  if Assigned(FParentProblem) then
    result := FParentProblem.DoAllocHeader(Row)
  else
  begin
    if Row then
      result := TPossibility.Create
    else
      result := TConstraint.Create;
  end;
end;

procedure TExactCoverMatrix.FreeCell(Cell: TMatrixCell);
begin
  if Assigned(FParentProblem) then
    FParentProblem.DoFreeCell(Cell)
  else
    Cell.Free;
end;

procedure TExactCoverMatrix.FreeHeader(Header: THeader);
begin
  if Assigned(FParentProblem) then
    FParentProblem.DoFreeHeader(Header)
  else
    Header.Free;
end;

{ TPickedHeader }

procedure TPickedHeader.ClientHandleExclude;
begin
  FPicker.ExcludeHeader(self);
end;

procedure TPickedHeader.ClientHandleInclude(InclCellCount: integer);
begin
  FPicker.IncludeHeader(self, InclCellCount);
end;

procedure TPickedHeader.ClientHandleInclCountChange(OldCount, NewCount: integer);
begin
  FPicker.ConstraintCountChange(self, OldCount, NewCount);
end;

{ TConstraint }

function TConstraint.GetStackedSatisfied;
begin
  result := not Included;
end;

{ TPossibility }

function TPossibility.GetStackedEliminated: boolean;
begin
  result := not Included;
end;

function TPossibility.GetPartOfSolution: boolean;
begin
  result := not DlItemIsEmpty(@FPartSolutionLink);
end;

constructor TPossibility.Create;
begin
  inherited;
  DLItemInitObj(self, @FPartSolutionLink);
end;

procedure TPossibility.GenericUnlinkAndFree;
begin
  if not DLItemIsEmpty(@FPartSolutionLink) then
  begin
   DLListRemoveObj(@FPartSolutionLink);
   Assert((self.Matrix as TExactCoverMatrix).FParentProblem.PartialSolutionStackCount > 0);
   Dec((Matrix as TExactCoverMatrix).FParentProblem.FPartSolutionStackCount);
  end;
  inherited;
end;

{ TExactCoverProblem }

{
    //Possibilities which have not only been eliminated from matrix, but
    //stacked as part of a partial solution.
    function FirstPartialSolutionPossibility: TPossibility;
    function NextPartialSolutionPossibility: TPossibility;

    //TODO - Iterate remaining constraints not yet satisfied?

    //If RunOneAlgX step, then performs AlgX row-column elimination for
    //that possibility, else just elimininates the possibility (no eliminate constraint).
    //May need to do this for "givens" or part-initial solutions.
    procedure StackSolutionPossibility(Poss: TPossibility; RunOneAlgXStep: boolean = true);
    function PeekTopSolutionPossibity: TPossibility; //Determine if a "given"
    function PopTopSolutionPossibility: TPossibility;

    property RowsColsStacked: integer read GetRowsColsStacked;
    property ConnectivityCurrent: boolean read FConnectivityCurrent;
    property ConnectivityComplete: boolean read FConnectivityCurrent;
    property OnPossibilitySatisfiesConstraint: TPossConstraintMethod
      read FPossConsCheck write FPossConsCheck;
    property OnAllocateCell: TOnAllocCellEvent read FOnAllocCell write FOnAllocCell;
    property OnAllocPossibility:TOnAllocPossibilityEvent read FOnAllocPossibility write FOnAllocPossibility;
    property OnAllocConstraint:TOnAllocConstraintEvent read FOnAllocConstraint write FOnAllocConstraint;
    property OnFreeCell: TOnFreeCellEvent read FOnFreeCell write FOnFreeCell;
    property OnFreePossibility:TOnFreePossibilityEvent read FOnFreePossibility write FOnFreePossibility;
    property OnFreeConstraint:TOnFreeConstraintEvent read FOnFreeConstraint write FOnFreeConstraint;
    property PartialSolutionStackCount: integer read FPartSolutionStackCount;
  end;
}

{
    FMatrix: TExactCoverMatrix;

    FConnectivityCurrent: boolean;
    FConnectivityComplete: boolean;

    FPartSolutionStack: TDLEntry;
    FPartSolutionStackCount: integer;
}

function TExactCoverProblem.DoAllocCell: TMatrixCell;
begin
  if Assigned(FOnAllocCell) then
  begin
    result := FOnAllocCell(self);
    Assert(Assigned(result));
  end
  else
    result := TExactCoverCell.Create;
end;

function TExactCoverProblem.DoAllocHeader(Row: boolean): THeader;
begin
  if Row then
  begin
    if Assigned(FOnAllocPossibility) then
    begin
      result := FOnAllocPossibility(self);
      Assert(Assigned(result));
    end
    else
      result := TPossibility.Create;
    (result as TPickedHeader).FPicker := FPossibilityPicker;
  end
  else
  begin
    if Assigned(FOnAllocConstraint) then
    begin
        result := FOnAllocConstraint(self);
        Assert(Assigned(result));
    end
    else
      result := TConstraint.Create;
    (result as TPickedHeader).FPicker := FConstraintPicker;
  end;
end;

procedure TExactCoverProblem.DoFreeCell(Cell: TMatrixCell);
begin
  if Assigned(FOnFreeCell) then
    FOnFreeCell(self, Cell as TExactCoverCell)
  else
    Cell.Free;
end;

procedure TExactCoverProblem.DoFreeHeader(Header: THeader);
begin
  if Header.IsRow then
  begin
    if Assigned(FOnFreePossibility) then
      FOnFreePossibility(self, Header as TPossibility)
    else
      Header.Free;
  end
  else
  begin
    if Assigned(FOnFreeConstraint) then
      FOnFreeConstraint(self, Header as TConstraint)
    else
      Header.Free;
  end;
end;


constructor TExactCoverProblem.Create;
begin
  inherited;
  FMatrix := TExactCoverMatrix.Create;
  DLItemInitList(@FPartSolutionStack);
end;

destructor TExactCoverProblem.Destroy;
begin
  FMatrix.Free;
  Assert(DlItemIsEmpty(@FPartSolutionStack));
  Assert(FPartSolutionStackCount = 0);
  inherited;
end;

function TExactCoverProblem.GetRowsColsStacked;
begin
  result := FMatrix.StackedHeadersPushed;
end;

function TExactCoverProblem.FirstPossibility: TPossibility;
begin
  result := FMatrix.FirstRow as TPossibility;
end;

function TExactCoverProblem.LastPossibility: TPossibility;
begin
  result := FMatrix.LastRow as TPossibility;
end;

function TExactCoverProblem.NextPossibility(Poss: TPossibility): TPossibility;
begin
  result := FMatrix.NextRow(Poss) as TPossibility;
end;

function TExactCoverProblem.PrevPossibility(Poss: TPossibility): TPossibility;
begin
  result := FMatrix.PrevRow(Poss) as TPossibility;
end;

function TExactCoverProblem.FirstConstraint: TConstraint;
begin
  result := FMatrix.FirstColumn as TConstraint;
end;

function TExactCoverProblem.LastConstraint: TConstraint;
begin
  result := FMatrix.LastColumn as TConstraint;
end;

function TExactCoverProblem.NextConstraint(Cons: TConstraint): TConstraint;
begin
  result := FMatrix.NextColumn(Cons) as TConstraint;
end;

function TExactCoverProblem.PrevConstraint(Cons: TConstraint): TConstraint;
begin
  result := FMatrix.PrevColumn(Cons) as TConstraint;
end;

function TExactCoverProblem.AddPossibility: TPossibility;
begin
  Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
  if FPartSolutionStackCount = 0 then
  begin
    result := FMatrix.AddRow as TPossibility;
    FConnectivityCurrent := false;
    FConnectivityComplete := false;
  end
  else
    raise EExactCoverException.Create(S_CANNOT_CHANGE_MATRIX_WHILST_SOLVING);
end;

procedure TExactCoverProblem.DeletePossibility(Poss: TPossibility);
begin
  Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
  if FPartSolutionStackCount = 0 then
  begin
    FMatrix.DeleteRow(Poss, false);
    FConnectivityCurrent := false;
  end
  else
    raise EExactCoverException.Create(S_CANNOT_CHANGE_MATRIX_WHILST_SOLVING);
end;

function TExactCoverProblem.AddConstraint: TConstraint;
begin
  Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
  if FPartSolutionStackCount = 0 then
  begin
    result := FMatrix.AddColumn as TConstraint;
    FConnectivityCurrent := false;
    FConnectivityComplete := false;
  end
  else
    raise EExactCoverException.Create(S_CANNOT_CHANGE_MATRIX_WHILST_SOLVING);
end;

procedure TExactCoverProblem.DeleteConstraint(Cons: TConstraint);
begin
  Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
  if FPartSolutionStackCount = 0 then
  begin
    FMatrix.DeleteColumn(Cons, false);
    FConnectivityCurrent := false;
  end
  else
    raise EExactCoverException.Create(S_CANNOT_CHANGE_MATRIX_WHILST_SOLVING);
end;

procedure TExactCoverProblem.ClearConnectivity;
begin
  Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
  if FPartSolutionStackCount = 0 then
  begin
    FMatrix.Clear;
    FConnectivityCurrent := false;
    FConnectivityComplete := false;
  end
  else
    raise EExactCoverException.Create(S_CANNOT_CHANGE_MATRIX_WHILST_SOLVING);
end;

procedure TExactCoverProblem.SetupConnectivity(AllowPartialAddition: boolean);
var
  Poss: TPossibility;
  Cons: TConstraint;
  Added: boolean;
begin
  Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
  if FPartSolutionStackCount = 0 then
  begin
    if (FMatrix.RowCount = 0) or (FMatrix.ColCount = 0) then
      raise EExactCoverException.Create(S_NO_POSS_OR_CONS_FOR_SETUP);
    Added := false;
    if not AllowPartialAddition then
    begin
      FMatrix.Clear;
      Poss := FirstPossibility;
      while Assigned(Poss) do
      begin
        Cons := FirstConstraint;
        while Assigned(Cons) do
        begin
          if Assigned(FPossConsCheck) then
          begin
            if FPossConsCheck(Poss, Cons) then
            begin
              FMatrix.InsertCell(Poss, Cons);
              Added := true;
            end;
          end
          else
            raise EExactCoverException.Create(S_NEED_CONNECTIVITY_HANDLER);
          Cons := NextConstraint(Cons);
        end;
        Poss := NextPossibility(Poss);
      end;
      if not Added then
        raise EExactCoverException.Create(S_MATRIX_IS_EMPTY);
    end
    else
    begin
      Poss := FirstPossibility;
      while Assigned(Poss) do
      begin
        if Poss.EntryCount = 0 then
        begin
          Cons := FirstConstraint;
          while Assigned(Cons) do
          begin
            if Assigned(FPossConsCheck) then
            begin
              if FPossConsCheck(Poss, Cons) then
              begin
                FMatrix.InsertCell(Poss, Cons);
                Added := true;
              end;
            end
            else
              raise EExactCoverException.Create(S_NEED_CONNECTIVITY_HANDLER);
            Cons := NextConstraint(Cons);
          end;
        end
        else
          Added := true;
        Poss := NextPossibility(Poss);
      end;
      Cons := FirstConstraint;
      while Assigned(Cons) do
      begin
        if Cons.EntryCount = 0 then
        begin
          Poss := FirstPossibility;
          while Assigned(Poss) do
          begin
            if Assigned(FPossConsCheck) then
            begin
              if FPossConsCheck(Poss, Cons) then
              begin
                FMatrix.InsertCell(Poss, Cons);
                Added := true;
              end;
            end
            else
              raise EExactCoverException.Create(S_NEED_CONNECTIVITY_HANDLER);
            Poss := NextPossibility(Poss);
          end;
        end
        else
          Added := true;
        Cons := NextConstraint(Cons);
      end;
      if not Added then
        raise EExactCoverException.Create(S_MATRIX_IS_EMPTY);
    end;

    FConnectivityCurrent := true;
    FConnectivityComplete := true;
  end
  else
    raise EExactCoverException.Create(S_CANNOT_CHANGE_MATRIX_WHILST_SOLVING);
end;

function TExactCoverProblem.TopPartialSolutionPossibility: TPossibility;
begin
  result := FPartSolutionStack.FLink.Owner as TPossibility;
end;

function TExactCoverProblem.NextPartialSolutionPossibility(Poss: TPossibility): TPossibility;
begin
  result := Poss.FPartSolutionLink.FLink.Owner as TPossibility;
end;

//TODO - think about where to put the exceptions in this bit of code. (poss opt?)

procedure TExactCoverProblem.StackSolutionPossibility(Poss: TPossibility; RunOneAlgXStep: boolean = true);
var
  Cons, NextCons: TConstraint; //Constraints satisfied by row inclusion in poss.
  ExclPoss, NextExclPoss: TPossibility; //Other possibilities excluded by satsified constraint.
  RowCell, ColCell: TExactCoverCell;
begin
  if (FConnectivityCurrent and FConnectivityComplete) then
  begin
    Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
    Assert((FMatrix.StackedHeadersPushed = 0) = (FPartSolutionStackCount = 0));
    Assert(TopPartialSolutionPossibility = FMatrix.PeekTop);

    if Poss.StackedEliminated then
      raise EExactCoverException.Create(S_DUPLICATE_POSS_ELIMINATION);

    if RunOneAlgXStep then
    begin
      //(1) Foreach constraint referenced in main stacked possibility.
      RowCell := Poss.FirstIncludedItem;
      if Assigned(RowCell) then
      begin
        Cons := RowCell.ColHeader as TConstraint;
        Assert(Cons.Included);
        RowCell := RowCell.NextIncludedAlongRow;
        if Assigned(RowCell) then
        begin
          NextCons := RowCell.ColHeader as TConstraint;
          Assert(NextCons.Included);
        end
        else
          NextCons := nil;
      end
      else
      begin
        Cons := nil;
        NextCons := nil;
      end;

      while Assigned(Cons) do
      begin
        //(1) Body
          //(2) Foreach other possibilty referenced by dependent constraint
        ColCell := Cons.FirstIncludedItem;
        if Assigned(ColCell) then
        begin
          ExclPoss := ColCell.RowHeader as TPossibility;
          Assert(ExclPoss.Included);
          ColCell := ColCell.NextIncludedDownColumn;
          if Assigned(ColCell) then
          begin
            NextExclPoss := ColCell.RowHeader as TPossibility;
            Assert(NextExclPoss.Included);
          end
          else
            NextExclPoss := nil;
        end
        else
        begin
          ExclPoss := nil;
          NextExclPoss := nil;
        end;

        while Assigned(ExclPoss) do
        begin
          //(2) Body
          if ExclPoss <> Poss then //Stack and remove excluded poss not that selected.
            FMatrix.ExcludeAndPush(ExclPoss);
          //(2) End body.
          //Next possibility referenced by dependent constraint.
          ExclPoss := NextExclPoss;
          if Assigned(ColCell) then
          begin
            Assert(ColCell.RowHeader = NextExclPoss);
            ColCell := ColCell.NextIncludedDownColumn;
            if Assigned(ColCell) then
            begin
              NextExclPoss := ColCell.RowHeader as TPossibility;
              Assert(NextExclPoss.Included);
            end
            else
              NextExclPoss := nil;
          end;
        end;
        //Stack and remove constraint satisfied.
        FMatrix.ExcludeAndPush(Cons);
        //(1) EndBody
        //Next constraint column for main stacked poss.
        Cons := NextCons;
        if Assigned(RowCell) then
        begin
          Assert(RowCell.ColHeader = NextCons);
          RowCell := RowCell.NextIncludedAlongRow;
          if Assigned(RowCell) then
          begin
            NextCons := RowCell.ColHeader as TConstraint;
            Assert(NextCons.Included);
          end
          else
            NextCons := nil;
        end;
      end;
      //(1) EndForeach constraint referenced in main stacked possibility.
    end;
    //Stack and remove possibility selected, adding to the stack of selected possibilities.
    FMatrix.ExcludeAndPush(Poss);
    DLListInsertHead(@FPartSolutionStack, @Poss.FPartSolutionLink);
    Inc(FPartSolutionStackCount);

    Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
    Assert((FMatrix.StackedHeadersPushed = 0) = (FPartSolutionStackCount = 0));
    Assert(TopPartialSolutionPossibility = FMatrix.PeekTop);
  end
  else
    raise EExactCoverException.Create(S_CANNOT_SOLVE_UNTIL_CONNECTIVITY_SETUP);
end;

function TExactCoverProblem.PopTopSolutionPossibility: TPossibility;
var
  OtherPoss: TPossibility;
begin
  if (FConnectivityCurrent and FConnectivityComplete) then
  begin
    Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
    Assert((FMatrix.StackedHeadersPushed = 0) = (FPartSolutionStackCount = 0));
    Assert(TopPartialSolutionPossibility = FMatrix.PeekTop);

    if FPartSolutionStackCount > 0 then
    begin
      //Pop the possibility which was part of the solution.
      result := DLListRemoveHead(@FPartSolutionStack).Owner as TPossibility;
      OtherPoss := FMatrix.PopAndInclude as TPossibility;
      Assert(OtherPoss = result);
      //Now find the next possibility (maybe nil) that we should stop popping at.
      OtherPoss := TopPartialSolutionPossibility;
      //Now just pop headers (both constraints and possibilities)
      //until we get to the next partial soln possibility.
      while FMatrix.PeekTop <> OtherPoss do
        FMatrix.PopAndInclude;

      Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
      Assert((FMatrix.StackedHeadersPushed = 0) = (FPartSolutionStackCount = 0));
      Assert(TopPartialSolutionPossibility = FMatrix.PeekTop);
    end
    else
      result := nil;
  end
  else
    raise EExactCoverException.Create(S_CANNOT_SOLVE_UNTIL_CONNECTIVITY_SETUP);
end;

{
  https://en.wikipedia.org/wiki/Knuth's_Algorithm_X
  https://en.wikipedia.org/wiki/Dancing_Links

  ---- AlgorithmX
  1. If the matrix A has no columns, the current partial solution is a
     valid solution; terminate successfully.
  2. Otherwise choose a column c (deterministically).
  3. Choose a row r such that Ar, c = 1 (nondeterministically).
    ---- StackSolutionPossibility
    4. Include row r in the partial solution.
    5. For each column j such that Ar, j = 1,
    6.   for each row i such that Ai, j = 1,
    7.     delete row i from matrix A.
    8.   delete column j from matrix A.
    ----
  9. Repeat this algorithm recursively on the reduced matrix A.
  ----
}

procedure TExactCoverProblem.AlgorithmX;
begin
  if (FConnectivityCurrent and FConnectivityComplete) then
  begin

  end
  else
    raise EExactCoverException.Create(S_CANNOT_SOLVE_UNTIL_CONNECTIVITY_SETUP);
end;

{ TAbstractPicker }

procedure TAbstractPicker.ExcludeHeader(Header: TPickedHeader);
begin
end;

procedure TAbstractPicker.IncludeHeader(Header: TPickedHeader; InitialCount: integer);
begin
end;

procedure TAbstractPicker.ConstraintCountChange(Header: TPickedHeader; OldCount, NewCount: integer);
begin
end;

procedure TAbstractPicker.PickerPushNewLevel;
begin
end;

procedure TAbstractPicker.PickerPopOldLevel;
begin
end;

{ TNaivePickerStackEntry }

constructor TNaivePickerStackEntry.Create;
begin
  inherited;
  DLItemInitObj(self, @FStackLink);
end;

destructor TNaivePickerStackEntry.Destroy;
begin
  Assert(DlItemIsEmpty(@FStackLink));
  //Headers get destroyed.
  Assert(not Assigned(FLastPickedThisLevel));
  //TODO - do we need to assert we've tried all possibs or constraints?
end;

{ TNaivePicker }

{$IFDEF PICKER_STRINGENT_CHECKS}
procedure TNaivePicker.CheckHeaderOrderAgrees;
begin
  Assert(false); //TODO - write this.
end;
{$ENDIF}

procedure TNaivePicker.ExcludeHeader(Header: TPickedHeader);
begin //Ignore header notifications
end;

procedure TNaivePicker.IncludeHeader(Header: TPickedHeader; InitialCount: integer);
begin //Ignore header notifications
end;

procedure TNaivePicker.ConstraintCountChange(Header: TPickedHeader; OldCount, NewCount: integer);
begin //Ignore header notifications
end;

procedure TNaivePicker.PickerPushNewLevel;
var
  NewEntry: TNaivePickerStackEntry;
  Count, Idx: integer;
  Header: TPickedHeader;
begin
  NewEntry := TNaivePickerStackEntry.Create;
  DLListInsertHead(@FStackHead, @NewEntry.FStackLink);
  if FPickingPossibilities then
  begin
{$IFDEF PICKER_STRINGENT_CHECKS}
    Count := FParentProblem.FMatrix.IncludedRowCount;
{$ENDIF}
    Header := FParentProblem.FMatrix.FirstIncludedRow as TPickedHeader;
  end
  else
  begin
{$IFDEF PICKER_STRINGENT_CHECKS}
    Count := FParentProblem.FMatrix.IncludedColCount;
{$ENDIF}
    Header := FParentProblem.FMatrix.FirstIncludedColumn as TPickedHeader;
  end;
{$IFDEF PICKER_STRINGENT_CHECKS}
  SetLength(NewEntry.FHeadersThisLevel, Count);
  Idx := 0;
  while Idx < Count do
  begin
    NewEntry.FHeadersThisLevel[Idx] := Header;
    Inc(Idx);
    Header := Header.NextIncludedHeader as TPickedHeader;
  end;
{$ENDIF}
end;

procedure TNaivePicker.PickerPopOldLevel;
var
  OldEntry: TNaivePickerStackEntry;
begin
{$IFDEF PICKER_STRINGENT_CHECKS}
  CheckHeaderOrderAgrees;
{$ENDIF}
  OldEntry := DLListRemoveHead(@FStackHead).Owner as TNaivePickerStackEntry;
  Assert(Assigned(OldEntry));
  OldEntry.Free;
end;

function TNaivePicker.PickOne:TPickedHeader;
begin
{$IFDEF PICKER_STRINGENT_CHECKS}
  CheckHeaderOrderAgrees;
{$ENDIF}
end;

constructor TNaivePicker.Create;
begin
  inherited;
  Assert(false);
end;

destructor TNaivePicker.Destroy;
begin
  Assert(false);
  inherited;
end;

end.
