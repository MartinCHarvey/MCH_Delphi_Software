program KnownSymmetries;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  KnownSymUtils in 'KnownSymUtils.pas',
  BufferedFileStream in '..\..\..\Common\CachedStream\BufferedFileStream.pas',
  KnownSymmetriesMT in 'KnownSymmetriesMT.pas',
  WorkItems in '..\..\..\Common\WorkItems\WorkItems.pas',
  CellCountSymmetriesMT in '..\CellCountMT\CellCountSymmetriesMT.pas',
  IndexedStore in '..\..\..\Common\Indexed Store\IndexedStore.pas',
  BinaryTree in '..\..\..\Common\Balanced Tree\BinaryTree.pas',
  DLList in '..\..\..\Common\DLList\DLList.pas',
  Trackables in '..\..\..\Common\Tracking\Trackables.pas',
  SharedSymmetries in '..\Shared\SharedSymmetries.pas',
  CellCountShared in '..\Shared\CellCountShared.pas',
  Parallelizer in '..\..\..\Common\Parallelizer\Parallelizer.pas',
  LockAbstractions in '..\..\..\Common\LockAbstractions\LockAbstractions.pas';

begin
  try
    KnownSymmetriesMT.KnownSymmetries(TCellCountMTBoard, ParamStr(1));
  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
