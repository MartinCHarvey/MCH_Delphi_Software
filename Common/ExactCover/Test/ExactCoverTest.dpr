program ExactCoverTest;

uses
  FMX.Forms,
  ExactCoverTestFrm in 'ExactCoverTestFrm.pas' {Form1},
  ExactCover in '..\ExactCover.pas',
  SparseMatrix in '..\..\SparseMatrix\SparseMatrix.pas',
  Trackables in '..\..\Tracking\Trackables.pas',
  DLList in '..\..\DLList\DLList.pas',
  BinaryTree in '..\..\Balanced Tree\BinaryTree.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
