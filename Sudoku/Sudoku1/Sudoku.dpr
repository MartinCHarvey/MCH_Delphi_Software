program Sudoku;

uses
  FMX.Forms,
  SudokuFrm in 'SudokuFrm.pas' {SudokuSimpleForm},
  SudokuBoard in 'SudokuBoard.pas',
  SSAbstracts in '..\..\Common\StreamingSystem\SSAbstracts.pas',
  SSIntermediates in '..\..\Common\StreamingSystem\SSIntermediates.pas',
  SSStreamables in '..\..\Common\StreamingSystem\SSStreamables.pas',
  StreamingSystem in '..\..\Common\StreamingSystem\StreamingSystem.pas',
  StreamSysBinary in '..\..\Common\StreamingSystem\StreamSysBinary.pas',
  StreamSysXML in '..\..\Common\StreamingSystem\StreamSysXML.pas',
  TrivXML in '..\..\Common\StreamingSystem\TrivXML.PAS',
  TrivXMLDefs in '..\..\Common\StreamingSystem\TrivXMLDefs.pas',
  Trackables in '..\..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\..\Common\Balanced Tree\BinaryTree.pas',
  IndexedStore in '..\..\Common\Indexed Store\IndexedStore.pas',
  DLList in '..\..\Common\DLList\DLList.pas',
  CocoBase in '..\..\CoCoDM\Distributable\Frames\CocoBase.pas',
  SudokuAbstracts in '..\SudokuShared\SudokuAbstracts.pas',
  SudokuStreaming in '..\SudokuShared\SudokuStreaming.pas',
  SudokuExactCover in '..\SudokuShared\SudokuExactCover.pas',
  ExactCover in '..\..\Common\ExactCover\ExactCover.pas',
  SparseMatrix in '..\..\Common\SparseMatrix\SparseMatrix.pas',
  Parallelizer in '..\..\Common\Parallelizer\Parallelizer.pas',
  LockAbstractions in '..\..\Common\LockAbstractions\LockAbstractions.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TSudokuSimpleForm, SudokuSimpleForm);
  Application.Run;
end.
