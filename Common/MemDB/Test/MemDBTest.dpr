program MemDBTest;

uses
  FMX.Forms,
  MemDBTestForm in 'MemDBTestForm.pas' {Form1},
  Trackables in '..\..\Tracking\Trackables.pas',
  MemDB in '..\MemDB.pas',
  MemDBBuffered in '..\MemDBBuffered.pas',
  MemDBMisc in '..\MemDBMisc.pas',
  IndexedStore in '..\..\Indexed Store\IndexedStore.pas',
  BinaryTree in '..\..\Balanced Tree\BinaryTree.pas',
  DLList in '..\..\DLList\DLList.pas',
  MemDBIndexing in '..\MemDBIndexing.pas',
  MemDbStreamable in '..\MemDbStreamable.pas',
  MemDBJournal in '..\MemDBJournal.pas',
  DLThreadQueue in '..\..\WorkItems\DLThreadQueue.pas',
  MemDBApi in '..\MemDBApi.pas',
  BufferedFileStream in '..\..\CachedStream\BufferedFileStream.pas',
  CommonPool in '..\..\WorkItems\CommonPool.pas',
  WorkItems in '..\..\WorkItems\WorkItems.pas',
  GlobalLog in '..\..\Logging\GlobalLog.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
