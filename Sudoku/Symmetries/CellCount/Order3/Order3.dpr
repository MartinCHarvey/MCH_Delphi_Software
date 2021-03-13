program Order3;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  SymmetryIterations in '..\..\Shared\SymmetryIterations.pas',
  SharedSymmetries in '..\..\Shared\SharedSymmetries.pas',
  NaiveSymmetries in '..\..\Naive\NaiveSymmetries.pas',
  CellCountSymmetries in '..\CellCountSymmetries.pas',
  GraphSymmetries in '..\..\Graph\GraphSymmetries.pas',
  BinaryTree in '..\..\..\..\Common\Balanced Tree\BinaryTree.pas',
  Trackables in '..\..\..\..\Common\Tracking\Trackables.pas',
  DLList in '..\..\..\..\Common\DLList\DLList.pas',
  SparseMatrix in '..\..\..\..\Common\SparseMatrix\SparseMatrix.pas',
  ExactCover in '..\..\..\..\Common\ExactCover\ExactCover.pas',
  IndexedStore in '..\..\..\..\Common\Indexed Store\IndexedStore.pas',
  CellCountShared in '..\..\Shared\CellCountShared.pas';

begin
  try
    TestCellCount;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
