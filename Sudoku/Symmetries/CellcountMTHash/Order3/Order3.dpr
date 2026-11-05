program Order3;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  CellCountSymmetriesMT in '..\CellCountSymmetriesMT.pas',
  SharedSymmetries in '..\..\Shared\SharedSymmetries.pas',
  CellCountShared in '..\..\Shared\CellCountShared.pas',
  SymmetryIterationsMT in '..\..\Shared\SymmetryIterationsMT.pas',
  WorkItems in '..\..\..\..\Common\WorkItems\WorkItems.pas',
  DLList in '..\..\..\..\Common\DLList\DLList.pas',
  LockAbstractions in '..\..\..\..\Common\LockAbstractions\LockAbstractions.pas',
  SRWLockWrapper in '..\..\..\..\Common\SRWLockWrapper\SRWLockWrapper.pas';

begin
  try
    TestCellCountMT;
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
