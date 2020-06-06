program Sudoku;

uses
  FMX.Forms,
  SudokuFrm in 'SudokuFrm.pas' {SudokuSimpleForm},
  SudokuBoard in 'SudokuBoard.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TSudokuSimpleForm, SudokuSimpleForm);
  Application.Run;
end.
