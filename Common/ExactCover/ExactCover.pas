unit ExactCover;

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

(*

Notes:

The rows reperesent possibilities, columns represent constrainsts (or subsets).
To solve Exact Cover we need to reduce the matrix by removing rows so that
we end up with a subset of the rows such that each column has exactly one
entry in it.

As in the an example, let S = {A, B, C, D, E, F} be a collection of subsets of a
set X = {1, 2, 3, 4, 5, 6, 7} such that:

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

interface

//  TODO - Review inlines and optimisations after profiling.

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
    procedure SetIncluded(NewIncluded: boolean); inline;//Runtime include / exclude by row / col
    procedure InitAllIncluded; //Startup IncludeAll
    function CheckAllIncluded: boolean;
    procedure GenericUnlinkAndFree; override;
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

    procedure SetIncluded(NewIncluded: boolean; Row: boolean); //Runtime include / exclude row / col
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

  TConstraint = class (TExactCoverHeader)
  protected
    function GetStackedEliminated: boolean; inline;
  public
    property StackedEliminated: boolean read GetStackedEliminated;
  end;

  TPossibility = class(TExactCoverHeader)
  private
    FPartSolutionLink: TDLEntry;
    FNoBacktrack: boolean;
  protected
    function GetStackedEliminated: boolean; inline;
    function GetPartOfSolution:boolean; inline;
    procedure GenericUnlinkAndFree; override;
  public
    constructor Create;
    property StackedEliminated: boolean read GetStackedEliminated;
    property PartOfSolution: boolean read GetPartOfSolution;
    property NoBackTrack: boolean read FNoBacktrack;
  end;

  TPossConstraintEvent = function(Possibility: TPossibility;
                                    Constraint: TConstraint): boolean of object;

  TOnAllocPossibilityEvent = function(Sender:TObject): TPossibility of object;
  TOnAllocConstraintEvent = function(Sender: TObject): TConstraint of object;
  TOnAllocCellEvent = function(Sender: TObject): TExactCoverCell of object;

  TOnFreePossibilityEvent = procedure(Sender: TObject; Possibility: TPossibility) of object;
  TOnFreeConstraintEvent = procedure(Sender: TObject; Constraint: TConstraint) of object;
  TOnFreeCellEvent = procedure(Sender: TObject; Cell: TExactCoverCell) of object;

{$IFDEF EXACT_COVER_LOGGING}
  TEvtType = (tetConstraintSelected,
              tetConstraintSatisfied,
              tetPossibilitySelected,
              tetPossibilityEliminated,
              tetPossibilityIncluded,
              tetBacktrackStart,
              tetBacktrackEnd,
              tetConstraintRestored,
              tetPossibilityRestored);

  TLogPossibilityEvent = procedure(Sender: TObject; Poss: TPossibility; EvtType: TEvtType) of object;
  TLogConstraintEvent = procedure(Sender: TObject; Cons: TConstraint; EvtType: TEvtType) of object;
  TLogOtherEvent = procedure(Sender: TObject; EvtType: TEvtType) of object;
{$ENDIF}


  EExactCoverException = class(Exception);

  TCoverTerminationType =
   (cttInvalid, //No valid computation performed.
    cttOKExactCover, //Covered everything exactly.
    cttOKUnderconstrained, //Covered all constraints, possibs outstanding.
    cttFailGenerallyOverconstrained, //Not covered all constraints, no possibs left.
    cttFailSpecificallyOverconstrained //Not covered all constraints, selected constraint cannot be satisfied.
    );

  TCoverNotifySet = set of TCoverTerminationType;

  TCoverNotifyEvent = procedure(Sender: TObject;
                                TerminationType: TCoverTerminationType;
                                var Stop: boolean) of object;


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

{$IFDEF EXACT_COVER_LOGGING}
    FOnLogPossibilityEvent: TLogPossibilityEvent;
    FOnLogConstraintEvent: TLogConstraintEvent;
    FOnLogOtherEvent: TLogOtherEvent;
{$ENDIF}
    FOnCoverNotifyEvent: TCoverNotifyEvent;
    FGeneralResult: TCoverTerminationType;
    FCoverNotifySet: TCoverNotifySet;

    FConnectivityCurrent: boolean;
    FConnectivityComplete: boolean;
    FSolved: boolean;
    FPossConsCheck: TPossConstraintEvent;
    FPartSolutionStack: TDLEntry;
    FPartSolutionStackCount: integer;
  protected
{$IFDEF EXACT_COVER_LOGGING}
    procedure DoLogPossibility(Poss: TPossibility; EvtType: TEvtType);
    procedure DoLogConstraint(Cons: TConstraint; EvtType: TEvtType);
    procedure DoLogOther(EvtType: TEvtType);
{$ENDIF}

    function GetRowsColsStacked: integer; inline;

    function DoAllocCell: TMatrixCell;
    procedure DoFreeCell(Cell: TMatrixCell);
    function DoAllocHeader(Row: boolean): TExactCoverHeader;
    procedure DoFreeHeader(Header: TExactCoverHeader);

    function GetTotalPossibilityCount: integer; inline;
    function GetTotalConstraintCount: integer; inline;
    function GetIncludedPossibilityCount: integer;
    function GetIncludedConstraintCount: integer;
    procedure CreatePickers; virtual;
    //Returns whether to quit.
    function DoTerminationHandler(NewTermination: TCoverTerminationType): boolean;
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

    //If RunOneAlgX step, then performs AlgX row-column elimination for
    //that possibility, else just elimininates the possibility (no eliminate constraint).
    //May need to do this for "givens" or part-initial solutions.
    procedure PushSolutionPossibility(Poss: TPossibility;
      NoBacktrack: boolean = false);
    function PopTopSolutionPossibility(OverBacktracks: boolean = false): TPossibility;
    procedure AlgorithmX;

    //Rows/Cols stacked is matrix internal, consider using
    //"PartialSolutionStackCount" of possibilities included in the solution.
    property RowsColsStacked: integer read GetRowsColsStacked;
    property ConnectivityCurrent: boolean read FConnectivityCurrent;
    property ConnectivityComplete: boolean read FConnectivityCurrent;

    property OnPossibilitySatisfiesConstraint: TPossConstraintEvent
      read FPossConsCheck write FPossConsCheck;

    property OnCoverNotify: TCoverNotifyEvent read FOnCoverNotifyEvent
      write FOnCoverNotifyEvent;

    property CoverNotifySet: TCoverNotifySet
      read FCoverNotifySet write FCoverNotifySet;

    property OnAllocateCell: TOnAllocCellEvent read FOnAllocCell write FOnAllocCell;
    property OnAllocPossibility:TOnAllocPossibilityEvent read FOnAllocPossibility write FOnAllocPossibility;
    property OnAllocConstraint:TOnAllocConstraintEvent read FOnAllocConstraint write FOnAllocConstraint;
    property OnFreeCell: TOnFreeCellEvent read FOnFreeCell write FOnFreeCell;
    property OnFreePossibility:TOnFreePossibilityEvent read FOnFreePossibility write FOnFreePossibility;
    property OnFreeConstraint:TOnFreeConstraintEvent read FOnFreeConstraint write FOnFreeConstraint;

{$IFDEF EXACT_COVER_LOGGING}
    property OnLogPossibilityEvent: TLogPossibilityEvent read FOnLogPossibilityEvent write FOnLogPossibilityEvent;
    property OnLogConstraintEvent: TLogConstraintEvent read FOnLogConstraintEvent write FOnLogConstraintEvent;
    property OnLogOtherEvent: TLogOtherEvent read FOnLogOtherEvent write FOnLogOtherEvent;
{$ENDIF}

    //See Top/Next partial solution possibility.
    property PartialSolutionStackCount: integer read FPartSolutionStackCount;

    property TotalPossibilityCount: integer read GetTotalPossibilityCount;
    property TotalConstraintCount: integer read GetTotalConstraintCount;

    property IncludedPossibilityCount: integer read GetIncludedPossibilityCount;
    property IncludedConstraintCount: integer read GetIncludedConstraintCount;
    property Solved: boolean read FSolved;
  end;

{$IFDEF USE_TRACKABLES}
  TAbstractPicker = class(TTrackable)
{$ELSE}
  TAbstractPicker = class
{$ENDIF}
  private
    FPickingPossibilities: boolean;
  protected
    FParentProblem: TExactCoverProblem;
  public
    procedure PickerInitSolve; virtual;
    procedure PickerFiniSolve; virtual;
    procedure PickerPostPush; virtual;
    procedure PickerPostPop; virtual;
    //Init no context for columns, init with context for rows.
    function PickOne(Context: TExactCoverHeader):TExactCoverHeader; virtual; abstract;

    property PickingPossibilities: boolean read FPickingPossibilities write FPickingPossibilities;
  end;

//Pick first or lowest related possibs constraint at each stack level,
//no iteration required, even if want to iter all possible solutions.
  TSimpleConstraintPicker = class(TAbstractPicker)
  private
    FPickLowest: boolean;
  public
    function PickOne(Context: TExactCoverHeader):TExactCoverHeader; override;

    property PickLowest: boolean read FPickLowest write FPickLowest;
  end;

//Iterate through all possibilities at each stack level,
//so solver can find one, or all possible solutions.
  TSimplePossibilityPicker = class(TAbstractPicker)
  private
    FPickedStack: array of TExactCoverCell;
    FStackIndex: integer;
  public
    procedure PickerInitSolve; override;
    procedure PickerFiniSolve; override;
    procedure PickerPostPush; override;
    procedure PickerPostPop; override;
    function PickOne(Context: TExactCoverHeader):TExactCoverHeader; override;
  end;

{
  N.B Solver with default pickers will not give us all possible solutions
  in all possible orders, because constraints are deterministically selected
  in a certain order.

  In order for us to get the same solutions in different orders, we would have
  to pick the constraints in another order for any particular stats.
}

{$IFDEF EXACT_COVER_LOGGING}
  TEvtTypeStrs = array[TEvtType] of string;

const
  EvtTypeStrs:TEvtTypeStrs = ('ConstraintSelected',
                              'ConstraintSatisfied',
                              'PossibilitySelected',
                              'PossibilityEliminated',
                              'PossibilityIncluded',
                              'BacktrackStart',
                              'BacktrackEnd',
                              'ConstraintRestored',
                              'PossibilityRestored');
{$ENDIF}

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
{$IFOPT C+}
  result := FInclCellsHead.FLink.Owner as TExactCoverCell;
{$ELSE}
  result := TExactCoverCell(FInclCellsHead.FLink.Owner);
{$ENDIF}
end;

function TExactCoverHeader.LastIncludedItem: TExactCoverCell;
begin
{$IFOPT C+}
  result := FInclCellsHead.BLink.Owner as TExactCoverCell;
{$ELSE}
  result := TExactCoverCell(FInclCellsHead.BLink.Owner);
{$ENDIF}
end;

function TExactCoverHeader.NextIncludedHeader: TExactCoverHeader;
begin
  if not DLItemIsEmpty(@FInclHeaderLink) then
{$IFOPT C+}
    result := FInclHeaderLink.FLink.Owner as TExactCoverHeader
{$ELSE}
    result := TExactCoverHeader(FInclHeaderLink.FLink.Owner)
{$ENDIF}
  else
  begin
    Assert(false);
    result := nil;
  end;
end;

function TExactCoverHeader.PrevIncludedHeader: TExactCoverHeader;
begin
  if not DLItemIsEmpty(@FInclHeaderLink) then
{$IFOPT C+}
    result := FInclHeaderLink.BLink.Owner as TExactCoverHeader
{$ELSE}
    result := TExactCoverHeader(FInclHeaderLink.BLink.Owner)
{$ENDIF}
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
{$IFOPT C+}
  M := Matrix as TExactCoverMatrix;
{$ELSE}
  M := TExactCoverMatrix(Matrix);
{$ENDIF}
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
  end
  else
  begin
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
{$IFOPT C+}
    result := FInclRowLink.FLink.Owner as TExactCoverCell
{$ELSE}
    result := TExactCoverCell(FInclRowLink.FLink.Owner)
{$ENDIF}
  else
  begin
    Assert(false);
    result := nil;
  end;
end;

function TExactCoverCell.NextIncludedDownColumn: TExactCoverCell;
begin
  if not DLItemIsEmpty(@FInclColLink) then
{$IFOPT C+}
    result := FInclColLink.FLink.Owner as TExactCoverCell
{$ELSE}
    result := TExactCoverCell(FInclColLink.FLink.Owner)
{$ENDIF}
  else
  begin
    Assert(false);
    result := nil;
  end;
end;

function TExactCoverCell.PrevIncludedAlongRow: TExactCoverCell;
begin
  if not DLItemIsEmpty(@FInclRowLink) then
{$IFOPT C+}
    result := FInclRowLink.BLink.Owner as TExactCoverCell
{$ELSE}
    result := TExactCoverCell(FInclRowLink.BLink.Owner)
{$ENDIF}
  else
  begin
    Assert(false);
    result := nil;
  end;
end;

function TExactCoverCell.PrevIncludedDownColumn: TExactCoverCell;
begin
  if not DLItemIsEmpty(@FInclColLink) then
{$IFOPT C+}
    result := FInclColLink.BLink.Owner as TExactCoverCell
{$ELSE}
    result := TExactCoverCell(FInclColLink.BLink.Owner)
{$ENDIF}
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
{$IFOPT C+}
      Hdr := (ColHeader as TExactCoverHeader);
{$ELSE}
      Hdr := TExactCoverHeader(ColHeader);
{$ENDIF}
      Inc(Hdr.FInclCellsCount);
    end
    else
    begin
      Assert(Assigned(FPrevRowSave));
      DLItemInsertAfter(FPrevRowSave, @FInclRowLink);
      FPrevRowSave := nil;
{$IFOPT C+}
      Hdr := (RowHeader as TExactCoverHeader);
{$ELSE}
      Hdr := TExactCoverHeader(RowHeader);
{$ENDIF}
      Inc(Hdr.FInclCellsCount);
    end;
  end
  else
  begin
    if Row then
    begin
      Assert(not Assigned(FPrevColSave));
      FPrevColSave := FInclColLink.BLink;
      DLListRemoveObj(@FInclColLink);
{$IFOPT C+}
      Hdr := (ColHeader as TExactCoverHeader);
{$ELSE}
      Hdr := TExactCoverHeader(ColHeader);
{$ENDIF}
      Dec(Hdr.FInclCellsCount);
    end
    else
    begin
      Assert(not Assigned(FPrevRowSave));
      FPrevRowSave := FInclRowLink.BLink;
      DLListRemoveObj(@FInclRowLink);
{$IFOPT C+}
      Hdr := (RowHeader as TExactCoverHeader);
{$ELSE}
      Hdr := TExactCoverHeader(RowHeader);
{$ENDIF}
      Dec(Hdr.FInclCellsCount);
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
{$IFOPT C+}
  result := Self.FIncludedRows.FLink.Owner as TExactCoverHeader;
{$ELSE}
  result := TExactCoverHeader(Self.FIncludedRows.FLink.Owner);
{$ENDIF}
end;

function TExactCoverMatrix.LastIncludedRow: TExactCoverHeader;
begin
{$IFOPT C+}
  result := Self.FIncludedRows.BLink.Owner as TExactCoverHeader;
{$ELSE}
  result := TExactCoverHeader(Self.FIncludedRows.BLink.Owner);
{$ENDIF}
end;

function TExactCoverMatrix.FirstIncludedColumn: TExactCoverHeader;
begin
{$IFOPT C+}
  result := Self.FIncludedCols.FLink.Owner as TExactCoverHeader;
{$ELSE}
  result := TExactCoverHeader(Self.FIncludedCols.FLink.Owner);
{$ENDIF}
end;

function TExactCoverMatrix.LastIncludedColumn: TExactCoverHeader;
begin
{$IFOPT C+}
  result := Self.FIncludedCols.BLink.Owner as TExactCoverHeader;
{$ELSE}
  result := TExactCoverHeader(Self.FIncludedCols.BLink.Owner);
{$ENDIF}
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
{$IFOPT C+}
    result := Entry.Owner as TExactCoverHeader;
{$ELSE}
    result := TExactCoverHeader(Entry.Owner);
{$ENDIF}
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
{$IFOPT C+}
  result := FStackedExcludes.FLink.Owner as TExactCoverHeader;
{$ELSE}
  result := TExactCoverHeader(FStackedExcludes.FLink.Owner);
{$ENDIF}
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
    FParentProblem.DoFreeHeader(Header as TExactCoverHeader)
  else
    Header.Free;
end;

{ TConstraint }

function TConstraint.GetStackedEliminated;
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
   if Assigned((self.Matrix as TExactCoverMatrix).FParentProblem) then
   begin
     Assert((self.Matrix as TExactCoverMatrix).FParentProblem.PartialSolutionStackCount > 0);
     Dec((Matrix as TExactCoverMatrix).FParentProblem.FPartSolutionStackCount);
   end;
  end;
  inherited;
end;

{ TExactCoverProblem }

{$IFDEF EXACT_COVER_LOGGING}
procedure TExactCoverProblem.DoLogPossibility(Poss: TPossibility; EvtType: TEvtType);
begin
  if Assigned(FOnLogPossibilityEvent) then
    FOnLogPossibilityEvent(self, Poss, EvtType);
end;

procedure TExactCoverProblem.DoLogConstraint(Cons: TConstraint; EvtType: TEvtType);
begin
  if Assigned(FOnLogConstraintEvent) then
    FOnLogConstraintEvent(self, Cons, EvtType);
end;

procedure TExactCoverProblem.DoLogOther(EvtType: TEvtType);
begin
  if Assigned(FOnLogOtherEvent) then
    FOnLogOtherEvent(self, EvtType);
end;
{$ENDIF}

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

function TExactCoverProblem.DoAllocHeader(Row: boolean): TExactCoverHeader;
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
  end;
end;

procedure TExactCoverProblem.DoFreeCell(Cell: TMatrixCell);
begin
  if Assigned(FOnFreeCell) then
    FOnFreeCell(self, Cell as TExactCoverCell)
  else
    Cell.Free;
end;

procedure TExactCoverProblem.DoFreeHeader(Header: TExactCoverHeader);
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

procedure TExactCoverProblem.CreatePickers;
begin
  FConstraintPicker := TSimpleConstraintPicker.Create;
  FConstraintPicker.FPickingPossibilities := false;
  (FConstraintPicker as TSimpleConstraintPicker).PickLowest := true;
  FPossibilityPicker := TSimplePossibilityPicker.Create;
  FPossibilityPicker.FPickingPossibilities := true;
  FConstraintPicker.FParentProblem := self;
  FPossibilityPicker.FParentProblem := self;
end;

constructor TExactCoverProblem.Create;
begin
  inherited;
  FCoverNotifySet := [cttOKExactCover];
  FMatrix := TExactCoverMatrix.Create;
  FMatrix.FParentProblem := self;
  DLItemInitList(@FPartSolutionStack);
  CreatePickers;
end;

destructor TExactCoverProblem.Destroy;
begin
  FMatrix.Free;
  Assert(DlItemIsEmpty(@FPartSolutionStack));
  Assert(FPartSolutionStackCount = 0);
  FConstraintPicker.Free;
  FPossibilityPicker.Free;
  inherited;
end;

function TExactCoverProblem.GetRowsColsStacked;
begin
  result := FMatrix.StackedHeadersPushed;
end;

function TExactCoverProblem.GetTotalPossibilityCount: integer;
begin
  result := FMatrix.RowCount;
end;

function TExactCoverProblem.GetTotalConstraintCount: integer;
begin
  result := FMatrix.ColCount;
end;

function TExactCoverProblem.GetIncludedPossibilityCount: integer;
begin
  result := FMatrix.IncludedRowCount;
end;

function TExactCoverProblem.GetIncludedConstraintCount: integer;
begin
  result := FMatrix.IncludedColCount;
end;


function TExactCoverProblem.FirstPossibility: TPossibility;
begin
{$IFOPT C+}
  result := FMatrix.FirstRow as TPossibility;
{$ELSE}
  result := TPossibility(FMatrix.FirstRow);
{$ENDIF}
end;

function TExactCoverProblem.LastPossibility: TPossibility;
begin
{$IFOPT C+}
  result := FMatrix.LastRow as TPossibility;
{$ELSE}
  result := TPossibility(FMatrix.LastRow);
{$ENDIF}
end;

function TExactCoverProblem.NextPossibility(Poss: TPossibility): TPossibility;
begin
{$IFOPT C+}
  result := FMatrix.NextRow(Poss) as TPossibility;
{$ELSE}
  result := TPossibility(FMatrix.NextRow(Poss));
{$ENDIF}
end;

function TExactCoverProblem.PrevPossibility(Poss: TPossibility): TPossibility;
begin
{$IFOPT C+}
  result := FMatrix.PrevRow(Poss) as TPossibility;
{$ELSE}
  result := TPossibility(FMatrix.PrevRow(Poss));
{$ENDIF}
end;

function TExactCoverProblem.FirstConstraint: TConstraint;
begin
{$IFOPT C+}
  result := FMatrix.FirstColumn as TConstraint;
{$ELSE}
  result := TConstraint(FMatrix.FirstColumn);
{$ENDIF}
end;

function TExactCoverProblem.LastConstraint: TConstraint;
begin
{$IFOPT C+}
  result := FMatrix.LastColumn as TConstraint;
{$ELSE}
  result := TConstraint(FMatrix.LastColumn);
{$ENDIF}
end;

function TExactCoverProblem.NextConstraint(Cons: TConstraint): TConstraint;
begin
{$IFOPT C+}
  result := FMatrix.NextColumn(Cons) as TConstraint;
{$ELSE}
  result := TConstraint(FMatrix.NextColumn(Cons));
{$ENDIF}
end;

function TExactCoverProblem.PrevConstraint(Cons: TConstraint): TConstraint;
begin
{$IFOPT C+}
  result := FMatrix.PrevColumn(Cons) as TConstraint;
{$ELSE}
  result := TConstraint(FMatrix.PrevColumn(Cons));
{$ENDIF}
end;

function TExactCoverProblem.AddPossibility: TPossibility;
begin
  Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
  if FPartSolutionStackCount = 0 then
  begin
    result := FMatrix.AddRow as TPossibility;
    FConnectivityCurrent := false;
    FConnectivityComplete := false;
    FSolved := false;
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
    FSolved := false;
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
    FSolved := false;
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
    FSolved := false;
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
    FSolved := false;
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
    FSolved := false;
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
    FMatrix.InitAllIncluded;
  end
  else
    raise EExactCoverException.Create(S_CANNOT_CHANGE_MATRIX_WHILST_SOLVING);
end;

function TExactCoverProblem.TopPartialSolutionPossibility: TPossibility;
begin
{$IFOPT C+}
  result := FPartSolutionStack.FLink.Owner as TPossibility;
{$ELSE}
  result := TPossibility(FPartSolutionStack.FLink.Owner);
{$ENDIF}
end;

function TExactCoverProblem.NextPartialSolutionPossibility(Poss: TPossibility): TPossibility;
begin
{$IFOPT C+}
  result := Poss.FPartSolutionLink.FLink.Owner as TPossibility;
{$ELSE}
  result := TPossibility(Poss.FPartSolutionLink.FLink.Owner);
{$ENDIF}
end;

procedure TExactCoverProblem.PushSolutionPossibility(Poss: TPossibility; NoBacktrack: boolean);
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
    if NoBackTrack then
      FSolved := false;

    if Poss.StackedEliminated then
      raise EExactCoverException.Create(S_DUPLICATE_POSS_ELIMINATION);

    Poss.FNoBacktrack := NoBacktrack;

    //(1) Foreach constraint referenced in main stacked possibility.
    RowCell := Poss.FirstIncludedItem;
    if Assigned(RowCell) then
    begin
{$IFOPT C+}
      Cons := RowCell.ColHeader as TConstraint;
{$ELSE}
      Cons := TConstraint(RowCell.ColHeader);
{$ENDIF}
      Assert(Cons.Included);
      RowCell := RowCell.NextIncludedAlongRow;
      if Assigned(RowCell) then
      begin
{$IFOPT C+}
        NextCons := RowCell.ColHeader as TConstraint;
{$ELSE}
        NextCons := TConstraint(RowCell.ColHeader);
{$ENDIF}
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
{$IFOPT C+}
        ExclPoss := ColCell.RowHeader as TPossibility;
{$ELSE}
        ExclPoss := TPossibility(ColCell.RowHeader);
{$ENDIF}
        Assert(ExclPoss.Included);
        ColCell := ColCell.NextIncludedDownColumn;
        if Assigned(ColCell) then
        begin
{$IFOPT C+}
          NextExclPoss := ColCell.RowHeader as TPossibility;
{$ELSE}
          NextExclPoss := TPossibility(ColCell.RowHeader);
{$ENDIF}
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
        begin
          FMatrix.ExcludeAndPush(ExclPoss);
{$IFDEF EXACT_COVER_LOGGING}
          DoLogPossibility(ExclPoss, tetPossibilityEliminated);
{$ENDIF}
        end;
        //(2) End body.
        //Next possibility referenced by dependent constraint.
        ExclPoss := NextExclPoss;
        if Assigned(ColCell) then
        begin
          Assert(ColCell.RowHeader = NextExclPoss);
          ColCell := ColCell.NextIncludedDownColumn;
          if Assigned(ColCell) then
          begin
{$IFOPT C+}
            NextExclPoss := ColCell.RowHeader as TPossibility;
{$ELSE}
            NextExclPoss := TPossibility(ColCell.RowHeader);
{$ENDIF}
            Assert(NextExclPoss.Included);
          end
          else
            NextExclPoss := nil;
        end;
      end;
      //Stack and remove constraint satisfied.
      FMatrix.ExcludeAndPush(Cons);
{$IFDEF EXACT_COVER_LOGGING}
      DoLogConstraint(Cons, tetConstraintSatisfied);
{$ENDIF}
      //(1) EndBody
      //Next constraint column for main stacked poss.
      Cons := NextCons;
      if Assigned(RowCell) then
      begin
        Assert(RowCell.ColHeader = NextCons);
        RowCell := RowCell.NextIncludedAlongRow;
        if Assigned(RowCell) then
        begin
{$IFOPT C+}
          NextCons := RowCell.ColHeader as TConstraint;
{$ELSE}
          NextCons := TConstraint(RowCell.ColHeader);
{$ENDIF}
          Assert(NextCons.Included);
        end
        else
          NextCons := nil;
      end;
    end;
    //(1) EndForeach constraint referenced in main stacked possibility.

    //Stack and remove possibility selected, adding to the stack of selected possibilities.
    FMatrix.ExcludeAndPush(Poss);
    DLListInsertHead(@FPartSolutionStack, @Poss.FPartSolutionLink);
    Inc(FPartSolutionStackCount);
{$IFDEF EXACT_COVER_LOGGING}
    DoLogPossibility(Poss, tetPossibilityIncluded);
{$ENDIF}

    Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
    Assert((FMatrix.StackedHeadersPushed = 0) = (FPartSolutionStackCount = 0));
    Assert(TopPartialSolutionPossibility = FMatrix.PeekTop);
  end
  else
    raise EExactCoverException.Create(S_CANNOT_SOLVE_UNTIL_CONNECTIVITY_SETUP);
end;

function TExactCoverProblem.PopTopSolutionPossibility(OverBacktracks: boolean): TPossibility;
var
  OtherPoss: TPossibility;
  PoppedHdr: TExactCoverHeader;
begin
  if (FConnectivityCurrent and FConnectivityComplete) then
  begin
    Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
    Assert((FMatrix.StackedHeadersPushed = 0) = (FPartSolutionStackCount = 0));
    Assert(TopPartialSolutionPossibility = FMatrix.PeekTop);

    if FPartSolutionStackCount > 0 then
    begin
{$IFDEF EXACT_COVER_LOGGING}
      DoLogOther(tetBacktrackStart);
{$ENDIF}
      if OverBacktracks then
        FSolved := false;

      //Pop the possibility which was part of the solution.
{$IFOPT C+}
      result := FPartSolutionStack.FLink.Owner as TPossibility;
{$ELSE}
      result := TPossibility(FPartSolutionStack.FLink.Owner);
{$ENDIF}
      Assert(Assigned(result));
      if result.NoBackTrack and not OverBacktracks then
        result := nil;

      if Assigned(result) then
      begin
        DLListRemoveHead(@FPartSolutionStack);
        Dec(FPartSolutionStackCount);

{$IFOPT C+}
        OtherPoss := FMatrix.PopAndInclude as TPossibility;
{$ELSE}
        OtherPoss := TPossibility(FMatrix.PopAndInclude);
{$ENDIF}
        Assert(OtherPoss = result);
        //Now find the next possibility (maybe nil) that we should stop popping at.
        OtherPoss := TopPartialSolutionPossibility;
        //Now just pop headers (both constraints and possibilities)
        //until we get to the next partial soln possibility.
        while FMatrix.PeekTop <> OtherPoss do
        begin
          PoppedHdr := FMatrix.PopAndInclude;
          if Assigned(PoppedHdr) then
          begin
{$IFDEF EXACT_COVER_LOGGING}
            if PoppedHdr is TPossibility then
              DoLogPossibility(TPossibility(PoppedHdr), tetPossibilityRestored)
            else if PoppedHdr is TConstraint then
              DoLogConstraint(TConstraint(PoppedHdr), tetConstraintRestored)
            else
              Assert(false);
{$ENDIF}
          end;
        end;
      end;
{$IFDEF EXACT_COVER_LOGGING}
      DoLogOther(tetBacktrackEnd);
{$ENDIF}
    end
    else
      result := nil;

    Assert(DlItemIsEmpty(@FPartSolutionStack) = (FPartSolutionStackCount = 0));
    Assert((FMatrix.StackedHeadersPushed = 0) = (FPartSolutionStackCount = 0));
    Assert(TopPartialSolutionPossibility = FMatrix.PeekTop);
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
    ---- PushSolutionPossibility
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
var
  Cons: TConstraint;
  Poss: TPossibility;
  Terminate: boolean;
  Pop: boolean;
begin
  if (FConnectivityCurrent and FConnectivityComplete) then
  begin
    FGeneralResult := cttInvalid;

    Terminate := false;
    Pop := false;
    FPossibilityPicker.PickerInitSolve;
    FConstraintPicker.PickerInitSolve;
    while true do
    begin
      //Remove warnings.
      Cons := nil;
      Poss := nil;

      //First of all, should we bomb out all the way?
      if IncludedConstraintCount = 0 then
      begin
        if IncludedPossibilityCount = 0 then
          Terminate := DoTerminationHandler(cttOKExactCover)
        else
          Terminate := DoTerminationHandler(cttOKUnderconstrained);
        Pop := true; //regardless, we have to back up.
      end
      else if IncludedPossibilityCount = 0 then
      begin
        Terminate := DoTerminationHandler(TCoverTerminationType.cttFailGenerallyOverconstrained);
        Pop := true;
      end;

      //Proceed forward...
      if not Pop then
      begin
        //Constraint picker should deterministically pick same constraint
        //from a set given same initial set of constraints to choose from.
        //If order of constraint picks changes result, then problem not well
        //specified.
{$IFOPT C+}
        Cons := FConstraintPicker.PickOne(nil) as TConstraint;
{$ELSE}
        Cons := TConstraint(FConstraintPicker.PickOne(nil));
{$ENDIF}
        Assert(Assigned(Cons));
{$IFDEF EXACT_COVER_LOGGING}
        DoLogConstraint(Cons, tetConstraintSelected);
{$ENDIF}
        if Cons.InclCellsCount = 0 then
        begin
          //We have possibilities, but none of them seem to remove the constraint we have
          Terminate := DoTerminationHandler(TCoverTerminationType.cttFailSpecificallyOverconstrained);
          Pop := true; //And we need to back out.
        end;
      end;
      //Proceed forward...
      if not Pop then
      begin
        //Possibility picker, there are more options here.
        //Knuths algorithm picks one at random, but arguably, one could
        //iterate through all possibilities at each level, which allows one to
        //find all possible solutions in a subset of problems.
{$IFOPT C+}
        Poss := FPossibilityPicker.PickOne(Cons) as TPossibility;
{$ELSE}
        Poss := TPossibility(FPossibilityPicker.PickOne(Cons));
{$ENDIF}
        Pop := not Assigned(Poss);
      end;
      //Proceed forward...
      if not Pop then
      begin
{$IFDEF EXACT_COVER_LOGGING}
        DoLogPossibility(Poss, tetPossibilitySelected);
{$ENDIF}
        PushSolutionPossibility(Poss);
        FPossibilityPicker.PickerPostPush;
        FConstraintPicker.PickerPostPush;
      end;
      //Ooops no, back up a level.
      if Pop then
      begin
        //Have to backtrack.
        if PopTopSolutionPossibility() <> nil then
        begin
          FConstraintPicker.PickerPostPop;
          FPossibilityPicker.PickerPostPop;
          if not Terminate then
            Pop := false; //Don't pop next time round, try next poss
        end
        else
        begin
          FSolved := true;
          FPossibilityPicker.PickerFiniSolve;
          FConstraintPicker.PickerFiniSolve;
          exit; //No more pops to do, finished.
        end;
      end;
    end;
  end
  else
    raise EExactCoverException.Create(S_CANNOT_SOLVE_UNTIL_CONNECTIVITY_SETUP);
end;

function TExactCoverProblem.DoTerminationHandler(NewTermination: TCoverTerminationType): boolean;
begin
  result := false; //don't quit.
  if NewTermination in FCoverNotifySet then
  begin
    if Assigned(FOnCoverNotifyEvent) then
      FOnCoverNotifyEvent(self, NewTermination, result);
  end;

  //TODO - Accumulation here might not be what you want,
  //and assumptions bear some thinking about.
  if (FGeneralResult = cttInvalid) or (NewTermination = cttOKExactCover) then
    FGeneralResult := NewTermination;
end;


{ TAbstractPicker }

procedure TAbstractPicker.PickerInitSolve;
begin
end;

procedure TAbstractPicker.PickerFiniSolve;
begin
end;

procedure TAbstractPicker.PickerPostPush;
begin
end;

procedure TAbstractPicker.PickerPostPop;
begin
end;

{ TSimpleConstraintPicker }

function TSimpleConstraintPicker.PickOne(Context: TExactCoverHeader):TExactCoverHeader;
var
  Cons, LowestCons: TConstraint;
begin
  Assert(not FPickingPossibilities);
  if not PickLowest then
     result := FParentProblem.FMatrix.FirstIncludedColumn
  else
  begin
{$IFOPT C+}
    Cons := FParentProblem.FMatrix.FirstIncludedColumn as TConstraint;
{$ELSE}
    Cons := TCOnstraint(FParentProblem.FMatrix.FirstIncludedColumn);
{$ENDIF}
    LowestCons := nil;
    while Assigned(Cons) do
    begin
      if (not Assigned(LowestCons))
        or (LowestCons.InclCellsCount > Cons.InclCellsCount) then
        LowestCons := Cons;
{$IFOPT C+}
      Cons := Cons.NextIncludedHeader as TConstraint;
{$ELSE}
      Cons := TConstraint(Cons.NextIncludedHeader);
{$ENDIF}
    end;
    result := LowestCons;
  end;
end;

{ TSimplePossibilityPicker }


procedure TSimplePossibilityPicker.PickerInitSolve;
begin
  Assert(FStackIndex = 0);
  SetLength(FPickedStack, Succ(0));
  Assert(FPickedStack[FStackIndex] = nil);
  inherited;
end;

procedure TSimplePossibilityPicker.PickerFiniSolve;
begin
  Assert(FStackIndex = 0);
  SetLength(FPickedStack, 0);
  inherited;
end;

procedure TSimplePossibilityPicker.PickerPostPush;
begin
  Inc(FStackIndex);
  if Length(FPickedStack) < Succ(FStackIndex) then
    SetLength(FPickedStack, Succ(FStackIndex));
  FPickedStack[FStackIndex] := nil;
end;

procedure TSimplePossibilityPicker.PickerPostPop;
begin
  Dec(FStackIndex);
end;

function TSimplePossibilityPicker.PickOne(Context: TExactCoverHeader):TExactCoverHeader;
var
  Cell: TExactCoverCell;
begin
  Assert(Context is TConstraint);
  Assert(FPickingPossibilities);
  if not Assigned(FPickedStack[FStackIndex]) then
    Cell := Context.FirstIncludedItem
  else
    Cell := FPickedStack[FStackIndex].NextIncludedDownColumn;
  Assert(FStackIndex >= 0);
  FPickedStack[FStackIndex] := Cell;
  Assert((not Assigned(Cell)) or (Cell.ColHeader = Context));
  if Assigned(Cell) then
{$IFOPT C+}
    result := Cell.RowHeader as TExactCoverHeader
{$ELSE}
    result := TExactCoverHeader(Cell.RowHeader)
{$ENDIF}
  else
    result := nil;
end;

end.
