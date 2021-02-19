program Order3;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  SharedSymmetries in '..\..\Shared\SharedSymmetries.pas',
  SymmetryIterations in '..\..\Shared\SymmetryIterations.pas',
  NaiveSymmetries in '..\NaiveSymmetries.pas',
  Trackables in '..\..\..\..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\..\..\..\Common\Balanced Tree\BinaryTree.pas',
  GraphSymmetries in '..\..\Graph\GraphSymmetries.pas',
  DLList in '..\..\..\..\Common\DLList\DLList.pas',
  ExactCover in '..\..\..\..\Common\ExactCover\ExactCover.pas',
  SparseMatrix in '..\..\..\..\Common\SparseMatrix\SparseMatrix.pas';

begin
  try
    SymmetryIterations.Test1;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
