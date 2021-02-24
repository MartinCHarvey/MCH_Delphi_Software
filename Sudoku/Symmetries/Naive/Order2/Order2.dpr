program Order2;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  SharedSymmetries in '..\..\Shared\SharedSymmetries.pas',
  NaiveSymmetries in '..\NaiveSymmetries.pas',
  SymmetryIterations in '..\..\Shared\SymmetryIterations.pas',
  Trackables in '..\..\..\..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\..\..\..\Common\Balanced Tree\BinaryTree.pas',
  DLList in '..\..\..\..\Common\DLList\DLList.pas',
  ExactCover in '..\..\..\..\Common\ExactCover\ExactCover.pas',
  SparseMatrix in '..\..\..\..\Common\SparseMatrix\SparseMatrix.pas',
  CellCountSymmetries in '..\..\CellCount\CellCountSymmetries.pas';

begin
  try
    TestNaive;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
