program Order2;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  SymmetryIterationsMT in '..\..\Shared\SymmetryIterationsMT.pas',
  SharedSymmetries in '..\..\Shared\SharedSymmetries.pas',
  CellCountSymmetriesMT in '..\CellCountSymmetriesMT.pas',
  IndexedStore in '..\..\..\..\Common\Indexed Store\IndexedStore.pas',
  BinaryTree in '..\..\..\..\Common\Balanced Tree\BinaryTree.pas',
  DLList in '..\..\..\..\Common\DLList\DLList.pas',
  Trackables in '..\..\..\..\Common\Tracking\Trackables.pas',
  WorkItems in '..\..\..\..\Common\WorkItems\WorkItems.pas',
  CellCountShared in '..\..\Shared\CellCountShared.pas',
  Parallelizer in '..\..\..\..\Common\Parallelizer\Parallelizer.pas',
  LockAbstractions in '..\..\..\..\Common\LockAbstractions\LockAbstractions.pas';

begin
  try
    TestCellCountMT;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
