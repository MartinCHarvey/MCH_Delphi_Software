program CoWTreeTest;

uses
  Vcl.Forms,
  CoWTreeTestFrm in 'CoWTreeTestFrm.pas' {CoWTreeTestForm},
  CoWTree in '..\CoWTree.pas',
  Reffed in '..\..\Reffed\Reffed.pas',
  IndexedStore in '..\..\Indexed Store\IndexedStore.pas',
  BinaryTree in '..\..\Balanced Tree\BinaryTree.pas',
  DLList in '..\..\DLList\DLList.pas',
  Trackables in '..\..\Tracking\Trackables.pas',
  Parallelizer in '..\..\Parallelizer\Parallelizer.pas',
  TinyLock in '..\..\LockAbstractions\TinyLock.pas',
  LockAbstractions in '..\..\LockAbstractions\LockAbstractions.pas',
  CoWABTree in '..\CoWABTree.pas',
  CoWSimpleTree in '..\CoWSimpleTree.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TCoWTreeTestForm, CoWTreeTestForm);
  Application.Run;
end.
