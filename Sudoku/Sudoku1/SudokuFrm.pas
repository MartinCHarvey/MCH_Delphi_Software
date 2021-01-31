unit SudokuFrm;
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

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, FMX.Layouts, FMX.Grid, StreamSysXML, SSAbstracts,
  SSStreamables, SudokuAbstracts, SudokuExactCover;

type
  TSudokuSimpleForm = class(TForm)
    Grid: TStringGrid;
    SolveBtn: TButton;
    TopLayout: TLayout;
    NextMoveBtn: TButton;
    LoadBtn: TButton;
    SaveBtn: TButton;
    OpenDialog: TOpenDialog;
    SaveDialog: TSaveDialog;
    ClearBtn: TButton;
    BottomLayout: TLayout;
    SimpleSolver: TRadioButton;
    DLXSolver: TRadioButton;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure GridEditingDone(Sender: TObject; const Col, Row: Integer);
    procedure SolveBtnClick(Sender: TObject);
    procedure NextMoveBtnClick(Sender: TObject);
    procedure LoadBtnClick(Sender: TObject);
    procedure SaveBtnClick(Sender: TObject);
    procedure ClearBtnClick(Sender: TObject);
  private
    { Private declarations }
    FBoard: TSAbstractBoardState;
    procedure GridFromBoard;
    procedure ShowStats(Stats: TSSolverStats);
  public
    { Public declarations }
  end;

var
  SudokuSimpleForm: TSudokuSimpleForm;

implementation

{$R *.fmx}

uses
  SudokuStreaming, SudokuBoard;

const
  S_MULTIPLE = 'Multiple next moves from this point, click solve instead.';
  S_NONE = 'No next move from this point, click solve instead.';
  S_NOT_A_VALID_BOARD = 'Not a valid board.';

procedure TSudokuSimpleForm.GridEditingDone(Sender: TObject; const Col, Row: Integer);
var
  NRow, NCol: TSNumber;
  C: integer;
  NC: TSNumber;
  CTxt: string;
  Clear: boolean;
begin
  NRow := Succ(Row);
  NCol := Succ(Col);
  try
    CTxt := Grid.Cells[Col, Row];
    Clear := CTxt.Length = 0;
    if Clear then
      FBoard.Entries[NRow, NCol] := 0
    else
    begin
      C := StrToInt(CTxt);
      if (C >= Low(NC)) and (C <= High(NC)) then
        NC := C
      else
        raise EConvertError.Create('');
      FBoard.Entries[NRow, NCol] := NC; //Nonzero entry.
    end
  except
    on EConvertError do ;
  end;
  GridFromBoard;
end;

procedure TSudokuSimpleForm.GridFromBoard;
var
  Row, Col: TSNumber;
  GRow, GCol: integer;
  Value: TSOptNumber;
  Txt: string;
begin
  for Row := Low(TSNumber) to High(TSNumber) do
    for Col := Low(TSNumber) to High(TSNumber) do
    begin
      Value := FBoard.Entries[Row, Col];
      if Value = 0 then
        Txt := ''
      else
        Txt := IntToStr(Value);
      GRow := Pred(Row);
      GCol := Pred(Col);
      if Txt <> Grid.Cells[GCol, GRow] then
        Grid.Cells[GCol, GRow] := Txt;
    end;
end;

procedure TSudokuSimpleForm.LoadBtnClick(Sender: TObject);
var
  FS: TFileStream;
  SS: TStreamSysXml;
  Obj: TObjStreamable;
begin
  if OpenDialog.Execute then
  begin
    FS := TFileStream.Create(OpenDialog.FileName, fmOpenRead, fmShareDenyNone);
    try
      SS := TStreamSysXML.Create;
      SS.RegisterHeirarchy(SBoardHeirarchy);
      try
        Obj := SS.ReadStructureFromStream(FS) as TObjStreamable;
        try
          if Assigned(Obj) and (Obj is TSAbstractBoardState) then
            FBoard.Assign(Obj)
          else
            raise EStreamSystemError.Create(S_NOT_A_VALID_BOARD);
        finally
          Obj.Free;
        end;
      finally
        SS.Free;
      end;
    finally
      FS.Free;
    end;
    GridFromBoard;
  end;
end;

procedure TSudokuSimpleForm.NextMoveBtnClick(Sender: TObject);
var
  NRow, NCol: TSNumber;
  NSet: TSNumberSet;
  DoneMove: boolean;
begin
  NSet := FBoard.FindNextMove(NRow, NCol);
  DoneMove := FBoard.EvolveIfSingular(NRow, NCol, NSet);
  if not DoneMove then
  begin
    if not FBoard.Complete then
    begin
      if FBoard.Proceedable then
        ShowMessage(S_MULTIPLE)
      else
        ShowMessage(S_NONE);
    end;
  end;
  GridFromBoard;
end;

procedure TSudokuSimpleForm.SaveBtnClick(Sender: TObject);
var
  FS: TFileStream;
  SS: TStreamSysXml;
begin
  if SaveDialog.Execute then
  begin
    FS := TFileStream.Create(SaveDialog.FileName, fmCreate, fmShareExclusive);
    try
      SS := TStreamSysXML.Create;
      SS.RegisterHeirarchy(SBoardHeirarchy);
      try
        SS.WriteStructureToStream(FBoard, FS);
      finally
        SS.Free;
      end;
    finally
      FS.Free;
    end;
  end;
end;

procedure TSudokuSimpleForm.ShowStats(Stats: TSSolverStats);
var
  S: string;
begin
  S :=
    'DecisionPoints: ' + IntToStr(Stats.DecisionPoints) + #13 + #10 +
    'DecisionPointsWithSolution: ' + IntToStr(Stats.DecisionPointsWithSolution) + #13 + #10 +
    'DecisionPointsAllDeadEnd: ' + IntToStr(Stats.DecisionPointsAllDeadEnd) + #13 + #10 +
    'CorrectSolutions: ' + IntToStr(Stats.CorrectSolutions) + #13 + #10 +
    'DeadEnds: ' + IntToStr(Stats.DeadEnds) + #13 + #10 +
    'Elapsed Time: ' + FloatToStrF(Stats.ElapsedTime * (3600 * 24),
    TFloatFormat.ffGeneral, 5, 10);
  ShowMessage(S);
end;

procedure TSudokuSimpleForm.SolveBtnClick(Sender: TObject);
var
  SSolver: TSAbstractSolver;
begin
  if DLXSolver.IsChecked then
    SSolver := TSExactCoverSolver.Create
  else
    SSolver := TSSimpleSolver.Create;
  try
    SSolver.BoardState := FBoard;
    SSolver.Solve;
    if Assigned(SSolver.UniqueSolution) then
      FBoard.Assign(SSolver.UniqueSolution)
    else
      FBoard.Assign(SSolver.BoardState);
    GridFromBoard;
    ShowStats(SSolver.Stats);
  finally
    SSolver.Free;
  end;
  GridFromBoard;
end;

procedure TSudokuSimpleForm.ClearBtnClick(Sender: TObject);
begin
  FBoard.Clear;
  GridFromBoard;
end;

procedure TSudokuSimpleForm.FormCreate(Sender: TObject);
var
  i: TSNumber;
  Col: TStringColumn;
begin
  Grid.RowCount := Succ(High(TSNumber) - Low(TSNumber));
  for I := Low(TSNumber) to High(TSNumber) do
  begin
    //TODO - Slightly darker lines every third row.
    Col := TStringColumn.Create(Grid);
    Col.Width := Grid.RowHeight;
    Grid.AddObject(Col);
  end;
  Assert(Grid.ColumnCount = Grid.RowCount);
  FBoard := TSBoardState.Create;
  GridFromBoard;
end;

procedure TSudokuSimpleForm.FormDestroy(Sender: TObject);
begin
  FBoard.Free;
end;

end.
