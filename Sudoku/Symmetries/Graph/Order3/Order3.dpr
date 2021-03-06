program Order3;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  SharedSymmetries in '..\..\Shared\SharedSymmetries.pas',
  SymmetryIterations in '..\..\Shared\SymmetryIterations.pas',
  NaiveSymmetries in '..\..\Naive\NaiveSymmetries.pas',
  GraphSymmetries in '..\GraphSymmetries.pas',
  DLList in '..\..\..\..\Common\DLList\DLList.pas',
  ExactCover in '..\..\..\..\Common\ExactCover\ExactCover.pas',
  SparseMatrix in '..\..\..\..\Common\SparseMatrix\SparseMatrix.pas',
  Trackables in '..\..\..\..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\..\..\..\Common\Balanced Tree\BinaryTree.pas',
  CellCountSymmetries in '..\..\CellCount\CellCountSymmetries.pas',
  CellCountShared in '..\..\Shared\CellCountShared.pas';

begin
  try
    TestGraph;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
