program MemDB2Test;

uses
  FMX.Forms,
  MemDB2TestForm in 'MemDB2TestForm.pas' {Form1},
  Trackables in '..\..\Tracking\Trackables.pas',
  MemDB2 in '..\MemDB2.pas',
  MemDB2Buffered in '..\MemDB2Buffered.pas',
  MemDB2Misc in '..\MemDB2Misc.pas',
  IndexedStore in '..\..\Indexed Store\IndexedStore.pas',
  BinaryTree in '..\..\Balanced Tree\BinaryTree.pas',
  DLList in '..\..\DLList\DLList.pas',
  MemDB2Indexing in '..\MemDB2Indexing.pas',
  MemDB2Streamable in '..\MemDB2Streamable.pas',
  MemDB2Journal in '..\MemDB2Journal.pas',
  DLThreadQueue in '..\..\WorkItems\DLThreadQueue.pas',
  MemDB2Api in '..\MemDB2Api.pas',
  BufferedFileStream in '..\..\CachedStream\BufferedFileStream.pas',
  GlobalLog in '..\..\Logging\GlobalLog.pas',
  Parallelizer in '..\..\Parallelizer\Parallelizer.pas',
  NullStream in '..\..\NullStream\NullStream.pas',
  LockAbstractions in '..\..\LockAbstractions\LockAbstractions.pas',
  SRWLockWrapper in '..\..\SRWLockWrapper\SRWLockWrapper.pas',
  TinyLock in '..\..\LockAbstractions\TinyLock.pas',
  RWWLock in '..\..\LockAbstractions\RWWLock.pas',
  MemDB2BufBase in '..\MemDB2BufBase.pas',
  Reffed in '..\..\Reffed\Reffed.pas',
  CoWTree in '..\..\CoWTree\CoWTree.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
