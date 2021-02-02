program SudokuBatch;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  SBatchUtils in 'SBatchUtils.pas',
  WorkItems in '..\..\Common\WorkItems\WorkItems.pas',
  SudokuExactCover in '..\SudokuShared\SudokuExactCover.pas',
  ExactCover in '..\..\Common\ExactCover\ExactCover.pas',
  SparseMatrix in '..\..\Common\SparseMatrix\SparseMatrix.pas',
  DLList in '..\..\Common\DLList\DLList.pas',
  Trackables in '..\..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\..\Common\Balanced Tree\BinaryTree.pas',
  SudokuAbstracts in '..\SudokuShared\SudokuAbstracts.pas',
  SudokuBoard in '..\Sudoku1\SudokuBoard.pas',
  BufferedFileStream in '..\..\Common\CachedStream\BufferedFileStream.pas';

var
  Start, Finish: TDateTime;
  Elapsed: double;
  S: string;

begin
  try
    Start := Now;
    SBatchUtils.SpecList.ReadFromFile(ParamStr(1));
    SBatchUtils.RunCalculation;
    Finish := Now;
    Elapsed := (Finish-Start) * (3600 * 24);
    S := 'Solved: ' + InttoStr(Solved) +
        ' Unsolved: ' +IntToStr(Unsolved) +
        ' Error: ' + IntToStr(Error) + #13 + #10 +
        'Elapsed: ' + IntToStr(Trunc(Elapsed)) + ' secs, ' +
        IntToStr(Trunc(Solved / Elapsed)) + ' puzzles/second.';
    WriteLn(S);
    WriteLn('');
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
