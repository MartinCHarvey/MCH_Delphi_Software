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

*)

interface

{
  TODO - Review inlines and optimisations after profiling.
}

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
    function GetIncluded: boolean;
    procedure SetIncluded(NewIncluded: boolean); //Runtime include / exclude by row / col
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
    function GetIncluded: boolean; //Both row and col linked, both row saves NIL.

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
    //TODO - Do we need to keep track of how many to push / pop at once?
    //Do we need to push / pop in bunches of stuff?

    function AllocCell: TMatrixCell; override;
    function AllocHeader(Row: boolean): THeader; override;

    function FirstIncludedRow: TExactCoverHeader; inline;
    function LastIncludedRow: TExactCoverHeader; inline;
    function FirstIncludedColumn: TExactCoverHeader; inline;
    function LastIncludedColumn: TExactCoverHeader; inline;

    property AllIncluded: boolean read CheckAllIncluded;
    property IncludedRowCount: integer read FIncludedRowCount;
    property IncludedColCount: integer read FIncludedColCount;
  end;

  //NB. Because of the ways that rows and cols can be included / excluded,
  //headers can have nonzero entry counts despite the fact the entire row / col
  //has been excluded from the matrix as a whole.

  TConstraint = class (TExactCoverHeader)
  end;

  TPossibility = class(TExactCoverHeader)
  end;


  TPossConstraintMethod = function(Possibility: TPossibility;
                                    Constraint: TConstraint): boolean of object;


  //TODO - Intrinsic problem setup (rules of game)
  //versus initial stacking / removing possibilities (part-solved-stub).
  TSetupState = (tssSetupMatrix, tssSetupInclLists, tssReady, tssSolved);

//TODO - Benchmark and compare this with some of the fastest solvers out there,
//but look at their code second!

  TOnAllocPossibilityEvent = function(Sender:TObject): TPossibility of object;
  TOnAllocConstraintEvent = function(Sender: TObject): TConstraint of object;
  TOnAllocCellEvent = function(Sender: TObject): TExactCoverCell of object;

{$IFDEF USE_TRACKABLES}
  TExactCoverProblem = class(TTrackable)
{$ELSE}
  TExactCoverProblem = class
{$ENDIF}
  private
    FMatrix: TExactCoverMatrix;
    FOnAllocPossibility:TOnAllocPossibilityEvent;
    FOnAllocConstraint:TOnAllocConstraintEvent;
    FOnAllocCell: TOnAllocCellEvent;
  protected
{    procedure SetupGeneric(Func: TPossConstraintFunc;
                          Method: TPossConstraintMethod);
}
  public
    constructor Create;
    destructor Destroy; override;
  end;

implementation


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
      Inc((ColHeader as TExactCoverHeader).FInclCellsCount);
    end
    else
    begin
      Assert(Assigned(FPrevRowSave));
      DLItemInsertAfter(FPrevRowSave, @FInclRowLink);
      FPrevRowSave := nil;
      Inc((RowHeader as TExactCoverHeader).FInclCellsCount);
    end;
  end
  else
  begin
    if Row then
    begin
      Assert(not Assigned(FPrevColSave));
      FPrevColSave := FInclColLink.BLink;
      DLListRemoveObj(@FInclColLink);
      Dec((ColHeader as TExactCoverHeader).FInclCellsCount);
    end
    else
    begin
      Assert(not Assigned(FPrevRowSave));
      FPrevRowSave := FInclRowLink.BLink;
      DLListRemoveObj(@FInclRowLink);
      Dec((RowHeader as TExactCoverHeader).FInclCellsCount);
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
    Dec(FStackedCount);
{$IFOPT C+}
    Assert(Entry = TestEntry);
{$ENDIF}
    Assert(Assigned(result));
  end
  else
    result := nil;
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
  if Assigned(FParentProblem) and Assigned(FParentProblem.FOnAllocCell) then
  begin
    result := FParentProblem.FOnAllocCell(FParentProblem);
    Assert(Assigned(result));
  end
  else
    result := TExactCoverCell.Create;
end;

function TExactCoverMatrix.AllocHeader(Row: boolean): THeader;
begin
  if Row then
  begin
    if Assigned(FParentProblem) and Assigned(FParentProblem.FOnAllocPossibility) then
    begin
      result := FParentProblem.FOnAllocPossibility(FParentProblem);
      Assert(Assigned(result));
    end
    else
      result := TPossibility.Create;
  end
  else
  begin
    if Assigned(FParentProblem) and Assigned(FParentProblem.FOnAllocConstraint) then
    begin
        result := FParentProblem.FOnAllocConstraint(FParentProblem);
        Assert(Assigned(result));
    end
    else
      result := TConstraint.Create;
  end;
end;

{ TExactCoverProblem }

constructor TExactCoverProblem.Create;
begin
  inherited;
  FMatrix := TExactCoverMatrix.Create;
end;

destructor TExactCoverProblem.Destroy;
begin
  FMatrix.Free;
  inherited;
end;


end.
