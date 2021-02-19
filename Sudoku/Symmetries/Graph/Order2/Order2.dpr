program Order2;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  SharedSymmetries in '..\..\Shared\SharedSymmetries.pas',
  SymmetryIterations in '..\..\Shared\SymmetryIterations.pas',
  Trackables in '..\..\..\..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\..\..\..\Common\Balanced Tree\BinaryTree.pas',
  NaiveSymmetries in '..\..\Naive\NaiveSymmetries.pas',
  GraphSymmetries in '..\GraphSymmetries.pas',
  DLList in '..\..\..\..\Common\DLList\DLList.pas',
  ExactCover in '..\..\..\..\Common\ExactCover\ExactCover.pas',
  SparseMatrix in '..\..\..\..\Common\SparseMatrix\SparseMatrix.pas';

begin
  try
    Test2;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
