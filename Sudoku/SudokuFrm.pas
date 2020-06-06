unit SudokuFrm;

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, FMX.Layouts, FMX.Grid, SudokuBoard;

type
  TSudokuSimpleForm = class(TForm)
    Grid: TStringGrid;
    SolveBtn: TButton;
    TopLayout: TLayout;
    NextMoveBtn: TButton;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure GridEditingDone(Sender: TObject; const Col, Row: Integer);
    procedure SolveBtnClick(Sender: TObject);
    procedure NextMoveBtnClick(Sender: TObject);
  private
    { Private declarations }
    FBoard: TSBoardState;
    procedure GridFromBoard;
  public
    { Public declarations }
  end;

var
  SudokuSimpleForm: TSudokuSimpleForm;

implementation

{$R *.fmx}


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

const
  S_MULTIPLE = 'Not fully solved: Multiple next moves from this point.';
  S_NONE = 'Not fully solved: No next move from this point.';

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

procedure TSudokuSimpleForm.SolveBtnClick(Sender: TObject);
var
  NRow, NCol: TSNumber;
  NSet: TSNumberSet;
  DoneMove: boolean;
begin
  DoneMove := true;
  while DoneMove do
  begin
    NSet := FBoard.FindNextMove(NRow, NCol);
    DoneMove := FBoard.EvolveIfSingular(NRow, NCol, NSet);
  end;
  if not FBoard.Complete then
  begin
    if FBoard.Proceedable then
      ShowMessage(S_MULTIPLE)
    else
      ShowMessage(S_NONE);
  end;
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
