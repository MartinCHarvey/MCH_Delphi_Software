program SparseMatrixTest;

uses
  FMX.Forms,
  SparseMatrixTestFrm in 'SparseMatrixTestFrm.pas' {TestForm},
  SparseMatrix in '..\SparseMatrix.pas',
  DLList in '..\..\DLList\DLList.pas',
  Trackables in '..\..\Tracking\Trackables.pas',
  BinaryTree in '..\..\Balanced Tree\BinaryTree.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TTestForm, TestForm);
  Application.Run;
end.
