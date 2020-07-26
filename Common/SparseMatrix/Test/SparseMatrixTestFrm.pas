unit SparseMatrixTestFrm;

{$C+} //Assertions must be on in this unit.

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, FMX.Layouts, FMX.Memo, SparseMatrix;

const
  MATRIX_SIZE = 100;
  DELETE_COUNT = 5;

type
  TMatrixSet = set of 0..(MATRIX_SIZE -1);
  TMatrixCellSet = array[0..MATRIX_SIZE-1] of TMatrixSet;
  TTest = procedure of object;

type
  TTestForm = class(TForm)
    TestBtn: TButton;
    ResultsMemo: TMemo;
    procedure TestBtnClick(Sender: TObject);
  private
    { Private declarations }
    FTestMatrix: TSparseMatrix;

    procedure TestRowColGetAfterSetup;
    procedure TestCellGetAfterSetup;
    procedure ClearRowColsAfterSetup;

    procedure TestRowColOrderedAdd;

    procedure TestRowColDuplicateAdd;
    procedure TestRowColArbitraryInsertNoReindex;
    procedure TestRowColArbitraryInsertReindex;
    procedure TestRowColAddAfterInsert;

    procedure TestRowColDeleteNoReindex;
    procedure TestRowColDeleteReindex;

    procedure TestCellOrderedAdd;
    procedure TestCellDuplicateAdd;
    procedure TestCellArbitraryAdd;
    procedure TestMatrixPartiallyFilled;

    procedure TestCellOrderedDelete;
    procedure TestCellArbitraryDelete;
    procedure TestFreeWithCells;

    procedure TestPre;
    procedure TestPost;
    procedure DoTest(Test: TTest; TestName: string);
    procedure CheckAssertionsOn;
  public
    { Public declarations }
  end;

var
  TestForm: TTestForm;

implementation

{$R *.fmx}

procedure TTestForm.CheckAssertionsOn;
var
  Raised: boolean;
begin
  Raised := false;
  try
    Assert(false);
  except
    on EAssertionFailed do Raised := true;
  end;
  if not Raised then
    raise Exception.Create('Assertions need to be enabled in this unit for testapp to work.');
end;

procedure TTestForm.TestBtnClick(Sender: TObject);
var
  iter: integer;
begin
  CheckAssertionsOn;
//  for iter := 0 to MATRIX_SIZE do
//  begin
    ResultsMemo.Lines.Clear;
    DoTest(TestRowColOrderedAdd, 'Row and column ordered addition');
    DoTest(TestRowColDuplicateAdd, 'Row and column duplicate addition');
    DoTest(TestRowColArbitraryInsertNoReindex, 'Row and column arbitrary insert, no reindex');
    DoTest(TestRowColArbitraryInsertReindex, 'Row and col arbitrary insert, reindex');
    DoTest(TestRowColAddAfterInsert, 'Test add after arbitrary inserts');
    DoTest(TestRowColDeleteReindex, 'Test row and column delete with reindexing on');
    DoTest(TestRowColDeleteNoReindex, 'Test row and column delete with reindexing off');
    DoTest(TestCellOrderedAdd, 'Test cell ordered addition');
    DoTest(TestCellDuplicateAdd, 'Test cell duplicate addition');
    DoTest(TestCellArbitraryAdd, 'Test cell arbitrary addition');
    DoTest(TestMatrixPartiallyFilled, 'Test matrix partially filled');
    DoTest(TestCellOrderedDelete, 'Test cell ordered delete');
    DoTest(TestCellArbitraryDelete, 'Test cell arbitrary delete');
    DoTest(TestFreeWithCells, 'Test free with cells.');
//  end;
end;

procedure TTestForm.TestPre;
begin
  Assert(not Assigned(FTestMatrix));
  FTestMatrix := TSparseMatrix.Create;
end;

procedure TTestForm.TestPost;
begin
  Assert(Assigned(FTestMatrix));
  FTestMatrix.Free;
  FTestMatrix := nil;
end;

procedure TTestForm.DoTest(Test: TTest; TestName: string);
var
  OK: boolean;
begin
  OK := false;
  TestPre;
  try
    try
      Test();
      OK := true;
    except
      on E: EAssertionFailed do
        ResultsMemo.Lines.Add('Test FAILED ' + TestName + ': (Assertion) : ' +E.Message);
      on E2: Exception do
        ResultsMemo.Lines.Add('Test FAILED ' + TestName + ': (Other exception' + E2.ClassName + ') : ' +E2.Message);
    end;
  finally
    TestPost;
  end;
  if OK then
    ResultsMemo.Lines.Add('Test PASSED ' + TestName);
end;

procedure TTestForm.TestRowColGetAfterSetup;
var
  i: integer;
  FHeader, LHeader: THeader;
  UHeader, DHeader: THeader;
begin
  //Just require all or none rows cols present after setup
  FHeader := FTestMatrix.FirstRow;
  LHeader := FTestMatrix.LastRow;
  Assert(Assigned(FHeader) = Assigned(LHeader));
  if Assigned(FHeader) then
  begin
    Assert(FHeader.Idx = 0);
    Assert(FHeader.IsRow);
  end;
  if Assigned(LHeader) then
  begin
    Assert(LHeader.Idx = Pred(MATRIX_SIZE));
    Assert(LHeader.IsRow);
  end;

  FHeader := FTestMatrix.FirstColumn;
  LHeader := FTestMatrix.LastColumn;
  Assert(Assigned(FHeader) = Assigned(LHeader));
  if Assigned(FHeader) then
  begin
    Assert(FHeader.Idx = 0);
    Assert(not FHeader.IsRow);
  end;
  if Assigned(LHeader) then
  begin
    Assert(LHeader.Idx = Pred(MATRIX_SIZE));
    Assert(not LHeader.IsRow);
  end;

  //Test rows.
  FHeader := FTestMatrix.FirstRow;
  LHeader := FTestMatrix.LastRow;
  if Assigned(FHeader) then
  begin
    for i := 0 to Pred(MATRIX_SIZE) do
    begin
      UHeader := FTestMatrix.GetRow(i);
      DHeader := FTestMatrix.GetRow(Pred(MATRIX_SIZE - i));
      Assert(UHeader = FHeader);
      Assert(DHeader = LHeader);
      Assert(FTestMatrix.NextRow(UHeader) = UHeader.NextHeader);
      Assert(FTestMatrix.PrevRow(DHeader) = DHeader.PrevHeader);

      FHeader := FTestMatrix.NextRow(FHeader);
      LHeader := FTestMatrix.PrevRow(LHeader);
    end;
  end;

  //Test columns.
  FHeader := FTestMatrix.FirstColumn;
  LHeader := FTestMatrix.LastColumn;
  if Assigned(FHeader) then
  begin
    for i := 0 to Pred(MATRIX_SIZE) do
    begin
      UHeader := FTestMatrix.GetColumn(i);
      DHeader := FTestMatrix.GetColumn(Pred(MATRIX_SIZE - i));
      Assert(UHeader = FHeader);
      Assert(DHeader = LHeader);
      Assert(FTestMatrix.NextColumn(UHeader) = UHeader.NextHeader);
      Assert(FTestMatrix.PrevColumn(DHeader) = DHeader.PrevHeader);

      FHeader := FTestMatrix.NextColumn(FHeader);
      LHeader := FTestMatrix.PrevColumn(LHeader);
    end;
  end;
end;

procedure TTestForm.TestCellGetAfterSetup;
var
  RHeader, CHeader: THeader;
  Row, Col: integer;
  Cell: TMatrixCell;
begin
  TestRowColGetAfterSetup;
  if (FTestMatrix.FirstRow <> nil) and (FTestMatrix.FirstColumn <> nil) then
  begin
    //We expect a matrix full of cells.
    Row := 0;
    RHeader := FTestMatrix.FirstRow;
    while Assigned(RHeader) do
    begin
      Col := 0;
      CHeader := FTestMatrix.FirstColumn;
      while Assigned(CHeader) do
      begin
        //Check getting cell points to correct row and col.
        Cell := FTestMatrix.GetCell(Row, Col);
        Assert(Assigned(Cell));
        Assert(Cell.Row = Row);
        Assert(Cell.Col = Col);
        //Check next/previous cell in each direction points to correct row
        //and col - and assignment of cells is correct.
        Assert((Cell.Row > 0) = (Cell.PrevCellDownColumn <> nil));
        Assert((Cell.Col > 0) = (Cell.PrevCellAlongRow <> nil));
        Assert((Cell.Row < Pred(MATRIX_SIZE)) = (Cell.NextCellDownColumn <> nil));
        Assert((Cell.Col < Pred(MATRIX_SIZE)) = (Cell.NextCellAlongRow <> nil));

        if Cell.PrevCellDownColumn <> nil then
          Assert(Cell.PrevCellDownColumn.Row = Pred(Row));

        if Cell.PrevCellAlongRow <> nil then
          Assert(Cell.PrevCellAlongRow.Col = Pred(Col));

        if Cell.NextCellDownColumn <> nil then
          Assert(Cell.NextCellDownColumn.Row = Succ(Row));

        if Cell.NextCellAlongRow <> nil then
          Assert(Cell.NextCellAlongRow.Col = Succ(Col));

        CHeader := FTestMatrix.NextColumn(CHeader);
        Inc(Col);
      end;
      RHeader := FTestMatrix.NextRow(RHeader);
      Inc(Row)
    end;

  end;
end;

procedure TTestForm.ClearRowColsAfterSetup;
var
  Header: THeader;
begin
  //Simplest row and col clear possible at the moment;
  Header := FTestMatrix.FirstRow;
  while Assigned(Header) do
  begin
    FTestMatrix.DeleteRow(Header, False);
    Header := FTestMatrix.FirstRow;
  end;
  Header := FTestMatrix.FirstColumn;
  while Assigned(Header) do
  begin
    FTestMatrix.DeleteColumn(Header, False);
    Header := FTestMatrix.FirstColumn;
  end;
end;


procedure TTestForm.TestRowColOrderedAdd;
var
  i: integer;
begin
  TestRowColGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  TestRowColGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;
  TestRowColGetAfterSetup;
  ClearRowColsAfterSetup;
  TestRowColGetAfterSetup;
end;

procedure TTestForm.TestRowColDuplicateAdd;
var
  i: integer;
  Header: THeader;
begin
  TestRowColGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  TestRowColGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;
  TestRowColGetAfterSetup;

  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    Header := FTestMatrix.GetRow(i);
    Assert(Header = FTestMatrix.InsertRowAt(i, false));
  end;
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    Header := FTestMatrix.GetColumn(i);
    Assert(Header = FTestMatrix.InsertColumnAt(i, false));
  end;

  TestRowColGetAfterSetup;
  ClearRowColsAfterSetup;
  TestRowColGetAfterSetup;
end;

procedure TTestForm.TestRowColArbitraryInsertNoReindex;
var
  S: TMatrixSet;
  i: integer;
begin
  TestRowColGetAfterSetup;
  S := [0..Pred(MATRIX_SIZE)];
  while S <> [] do
  begin
    i := Random(MATRIX_SIZE);
    if i in S then
    begin
      FTestMatrix.InsertRowAt(i, false);
      S := S - [i];
    end;
  end;
  TestRowColGetAfterSetup;
  S := [0..Pred(MATRIX_SIZE)];
  while S <> [] do
  begin
    i := Random(MATRIX_SIZE);
    if i in S then
    begin
      FTestMatrix.InsertColumnAt(i, false);
      S := S - [i];
    end;
  end;
  TestRowColGetAfterSetup;
  ClearRowColsAfterSetup;
  TestRowColGetAfterSetup;
end;


procedure TTestForm.TestRowColArbitraryInsertReindex;
var
  S: TMatrixSet;
  i, i2, InsIdx: integer;
begin
  TestRowColGetAfterSetup;
  S := [0..Pred(MATRIX_SIZE)];
  while S <> [] do
  begin
    i := Random(MATRIX_SIZE);
    if i in S then
    begin
      //Hahaha! Now we need to work out what the index value needs to be.
      //it's i, minus all the one's before we haven't inserted.
      InsIdx := i;
      for i2 := 0 to Pred(i) do
        if i2 in S then
          Dec(InsIdx);
      FTestMatrix.InsertRowAt(InsIdx, true);
      S := S - [i];
    end;
  end;
  TestRowColGetAfterSetup;
  S := [0..Pred(MATRIX_SIZE)];
  while S <> [] do
  begin
    i := Random(MATRIX_SIZE);
    if i in S then
    begin
      //Hahaha! Now we need to work out what the index value needs to be.
      //it's i, minus all the one's before we haven't inserted.
      InsIdx := i;
      for i2 := 0 to Pred(i) do
        if i2 in S then
          Dec(InsIdx);
      FTestMatrix.InsertColumnAt(InsIdx, true);
      S := S - [i];
    end;
  end;
  TestRowColGetAfterSetup;
  ClearRowColsAfterSetup;
  TestRowColGetAfterSetup;
end;

procedure TTestForm.TestRowColAddAfterInsert;
var
  S: TMatrixSet;
  i, i2, InsIdx: integer;
begin
  TestRowColGetAfterSetup;
  S := [0..Pred(Pred(MATRIX_SIZE))];
  while S <> [] do
  begin
    i := Random(MATRIX_SIZE);
    if i in S then
    begin
      //Hahaha! Now we need to work out what the index value needs to be.
      //it's i, minus all the one's before we haven't inserted.
      InsIdx := i;
      for i2 := 0 to Pred(i) do
        if i2 in S then
          Dec(InsIdx);
      FTestMatrix.InsertRowAt(InsIdx, true);
      S := S - [i];
    end;
  end;
  FTestMatrix.AddRow;
  TestRowColGetAfterSetup;
  S := [0..Pred(Pred(MATRIX_SIZE))];
  while S <> [] do
  begin
    i := Random(MATRIX_SIZE);
    if i in S then
    begin
      //Hahaha! Now we need to work out what the index value needs to be.
      //it's i, minus all the one's before we haven't inserted.
      InsIdx := i;
      for i2 := 0 to Pred(i) do
        if i2 in S then
          Dec(InsIdx);
      FTestMatrix.InsertColumnAt(InsIdx, true);
      S := S - [i];
    end;
  end;
  FTestMatrix.AddColumn;
  TestRowColGetAfterSetup;
  ClearRowColsAfterSetup;
  TestRowColGetAfterSetup;
end;

procedure TTestForm.TestRowColDeleteNoReindex;
var
  i,j: integer;
  S: TMatrixSet;
  Header: THeader;
begin
  TestRowColGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  TestRowColGetAfterSetup;
  //Delete up to 5 at random.
  S := [0..Pred(MATRIX_SIZE)];
  for i := 0 to Pred(DELETE_COUNT) do
  begin
    j := Random(MATRIX_SIZE);
    if j in S then
    begin
      Header := FTestMatrix.GetRow(j);
      Assert(Assigned(Header));
      FTestMatrix.DeleteRow(Header, false);
      S := S - [j];
    end;
  end;
  //And now reinsert exactly those we deleted.
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    if not (i in S) then
    begin
      FTestMatrix.InsertRowAt(i, false);
      S := S + [i];
    end;
  end;
  //And now recheck.
  TestRowColGetAfterSetup;


  //And now columns.
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;
  TestRowColGetAfterSetup;
  //Delete up to 5 at random.
  S := [0..Pred(MATRIX_SIZE)];
  for i := 0 to Pred(DELETE_COUNT) do
  begin
    j := Random(MATRIX_SIZE);
    if j in S then
    begin
      Header := FTestMatrix.GetColumn(j);
      Assert(Assigned(Header));
      FTestMatrix.DeleteColumn(Header, false);
      S := S - [j];
    end;
  end;
  //And now reinsert exactly those we deleted.
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    if not (i in S) then
    begin
      FTestMatrix.InsertColumnAt(i, false);
      S := S + [i];
    end;
  end;
  TestRowColGetAfterSetup;
end;

procedure TTestForm.TestRowColDeleteReindex;
var
  i, j: integer;
  Header: THeader;
begin
  //First try rows.
  TestRowColGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  TestRowColGetAfterSetup;
  //Delete up to 5 at random.
  for i := 0 to Pred(DELETE_COUNT) do
  begin
    j := Random(MATRIX_SIZE - i);
    Header := FTestMatrix.GetRow(j);
    Assert(Assigned(Header));
    FTestMatrix.DeleteRow(Header, true);
  end;
  for i := 0 to Pred(DELETE_COUNT) do
    FTestMatrix.AddRow;
  TestRowColGetAfterSetup;
  //Then try columns.
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;
  TestRowColGetAfterSetup;
  //Delete up to 5 at random.
  for i := 0 to Pred(DELETE_COUNT) do
  begin
    j := Random(MATRIX_SIZE - i);
    Header := FTestMatrix.GetColumn(j);
    Assert(Assigned(Header));
    FTestMatrix.DeleteColumn(Header, true);
  end;
  for i := 0 to Pred(DELETE_COUNT) do
    FTestMatrix.AddColumn;
  TestRowColGetAfterSetup;
  ClearRowColsAfterSetup;
  TestRowColGetAfterSetup;
end;

procedure TTestForm.TestCellOrderedAdd;
var
  i,j: integer;
begin
  TestCellGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    for j := 0 to Pred(MATRIX_SIZE) do
      FTestMatrix.InsertCell(i, j);
  end;
  TestCellGetAfterSetup;
  ClearRowColsAfterSetup;
  TestCellGetAfterSetup;
end;

procedure TTestForm.TestCellDuplicateAdd;
var
  i, j: integer;
  NCell, GCell: TMatrixCell;
begin
  TestCellGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    for j := 0 to Pred(MATRIX_SIZE) do
      FTestMatrix.InsertCell(i, j);
  end;
  TestCellGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    for j := 0 to Pred(MATRIX_SIZE) do
    begin
      GCell := FTestMatrix.GetCell(i, j);
      NCell := FTestMatrix.InsertCell(i, j);
      Assert(NCell = GCell);
    end;
  end;
  TestCellGetAfterSetup;
  ClearRowColsAfterSetup;
  TestCellGetAfterSetup;
end;

procedure TTestForm.TestCellArbitraryAdd;
var
  SetCells: TMatrixCellSet;
  i,j: integer;
  Done, NDone: boolean;
begin
  TestCellGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;

  for i := 0 to Pred(MATRIX_SIZE) do
    SetCells[i] := [0..Pred(MATRIX_SIZE)];
  Done := false;
  while not Done do
  begin
    i := Random(MATRIX_SIZE);
    j := Random(MATRIX_SIZE);
    if (j in SetCells[i]) then
    begin
      FTestMatrix.InsertCell(i, j);
      SetCells[i] := SetCells[i] - [j];
      NDone := true;
      for i := 0 to Pred(MATRIX_SIZE) do
      begin
        if SetCells[i] <> [] then
          NDone := false;
      end;
      Done := NDone;
    end;
  end;
  TestCellGetAfterSetup;
  ClearRowColsAfterSetup;
  TestCellGetAfterSetup;
end;

procedure TTestForm.TestMatrixPartiallyFilled;
var
  i,j: integer;
begin
  TestCellGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    for j := 0 to Pred(MATRIX_SIZE) do
    begin
      if (i mod 2 = 0) and (j mod 2 = 0) then
        FTestMatrix.InsertCell(i, j);
    end;
  end;
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    for j := 0 to Pred(MATRIX_SIZE) do
    begin
      Assert(Assigned(FTestMatrix.GetCell(i,j))
       = ((i mod 2 = 0) and (j mod 2 = 0)));
    end;
  end;
  ClearRowColsAfterSetup;
  TestCellGetAfterSetup;
end;

procedure TTestForm.TestCellOrderedDelete;
var
  i,j:integer;

begin
  TestCellGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    for j := 0 to Pred(MATRIX_SIZE) do
      FTestMatrix.InsertCell(i, j);
  end;
  TestCellGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    for j := 0 to Pred(MATRIX_SIZE) do
    begin
      if (i mod 2 = 0) and (j mod 2 = 0) then
        FTestMatrix.DeleteCell(i, j);
    end;
  end;
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    for j := 0 to Pred(MATRIX_SIZE) do
    begin
      Assert(Assigned(FTestMatrix.GetCell(i,j))
       = not ((i mod 2 = 0) and (j mod 2 = 0)));
    end;
  end;
  ClearRowColsAfterSetup;
  TestCellGetAfterSetup;
end;

procedure TTestForm.TestCellArbitraryDelete;
var
  SetCells: TMatrixCellSet;
  i,j: integer;
  Done, NDone: boolean;
begin
  TestRowColGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  TestRowColGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;

  for i := 0 to Pred(MATRIX_SIZE) do
    SetCells[i] := [0..Pred(MATRIX_SIZE)];
  Done := false;
  while not Done do
  begin
    i := Random(MATRIX_SIZE);
    j := Random(MATRIX_SIZE);
    if (j in SetCells[i]) then
    begin
      FTestMatrix.DeleteCell(i, j);
      SetCells[i] := SetCells[i] - [j];
      NDone := true;
      for i := 0 to Pred(MATRIX_SIZE) do
      begin
        if SetCells[i] <> [] then
          NDone := false;
      end;
      Done := NDone;
    end;
  end;

  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    for j := 0 to Pred(MATRIX_SIZE) do
      Assert(not Assigned(FTestMatrix.GetCell(i,j)));
  end;
  ClearRowColsAfterSetup;
  TestRowColGetAfterSetup;
end;

procedure TTestForm.TestFreeWithCells;
var
  i,j: integer;
begin
  TestCellGetAfterSetup;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddRow;
  for i := 0 to Pred(MATRIX_SIZE) do
    FTestMatrix.AddColumn;
  for i := 0 to Pred(MATRIX_SIZE) do
  begin
    for j := 0 to Pred(MATRIX_SIZE) do
      FTestMatrix.InsertCell(i, j);
  end;
  TestCellGetAfterSetup;
end;

end.

