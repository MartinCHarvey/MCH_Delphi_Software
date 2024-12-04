program SSTest;

uses
  Forms,
  SSTestForm in 'SSTestForm.pas' {SSTestFrm},
  SSAbstracts in '..\SSAbstracts.pas',
  SSIntermediates in '..\SSIntermediates.pas',
  StreamingSystem in '..\StreamingSystem.pas',
  Trackables in '..\..\Tracking\Trackables.pas',
  BinaryTree in '..\..\Balanced Tree\BinaryTree.pas',
  TrivXML in '..\TrivXML.PAS',
  TrivXMLDefs in '..\TrivXMLDefs.pas',
  StreamSysXML in '..\StreamSysXML.pas',
  SSStreamables in '..\SSStreamables.pas',
  TestStreamables in 'TestStreamables.pas',
  CocoBase in '..\..\..\CoCoDM\Distributable\Frames\CocoBase.pas',
  IndexedStore in '..\..\Indexed Store\IndexedStore.pas',
  DLList in '..\..\DLList\DLList.pas',
  StreamSysBinary in '..\StreamSysBinary.pas',
  BufferedFileStream in '..\..\CachedStream\BufferedFileStream.pas',
  Parallelizer in '..\..\Parallelizer\Parallelizer.pas';

{$R *.RES}

begin
  Application.Initialize;
  Application.CreateForm(TSSTestFrm, SSTestFrm);
  Application.Run;
end.

