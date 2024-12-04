program ServerApp;

uses
  FMX.Forms,
  ServerForm in 'ServerForm.pas' {ServerFrm},
  HTTPServerDispatcher in '..\..\Common\HTTPServerDispatcher\HTTPServerDispatcher.pas',
  GlobalLog in '..\..\Common\Logging\GlobalLog.pas',
  CommonPool in '..\..\Common\WorkItems\CommonPool.pas',
  Trackables in '..\..\Common\Tracking\Trackables.pas',
  IndexedStore in '..\..\Common\Indexed Store\IndexedStore.pas',
  BinaryTree in '..\..\Common\Balanced Tree\BinaryTree.pas',
  WorkItems in '..\..\Common\WorkItems\WorkItems.pas',
  DLList in '..\..\Common\DLList\DLList.pas',
  DLThreadQueue in '..\..\Common\WorkItems\DLThreadQueue.pas',
  HTTPServerPageProducer in '..\..\Common\HTTPServerDispatcher\HTTPServerPageProducer.pas',
  HTMLEscapeHelper in '..\..\Common\HTMLParser\HTMLEscapeHelper.pas',
  CheckInPageProducer in '..\..\CheckIn\CheckInPageProducer.pas',
  CheckInMailer in '..\..\CheckIn\CheckInMailer.pas',
  CheckInAppLogic in '..\..\CheckIn\CheckInAppLogic.pas',
  MemDB in '..\..\Common\MemDB\MemDB.pas',
  MemDBApi in '..\..\Common\MemDB\MemDBApi.pas',
  MemDBBuffered in '..\..\Common\MemDB\MemDBBuffered.pas',
  MemDBIndexing in '..\..\Common\MemDB\MemDBIndexing.pas',
  MemDBJournal in '..\..\Common\MemDB\MemDBJournal.pas',
  MemDBMisc in '..\..\Common\MemDB\MemDBMisc.pas',
  MemDbStreamable in '..\..\Common\MemDB\MemDbStreamable.pas',
  MemDBUtils in '..\..\Common\MemDB\MemDBUtils.pas',
  SSAbstracts in '..\..\Common\StreamingSystem\SSAbstracts.pas',
  SSIntermediates in '..\..\Common\StreamingSystem\SSIntermediates.pas',
  SSStreamables in '..\..\Common\StreamingSystem\SSStreamables.pas',
  StreamingSystem in '..\..\Common\StreamingSystem\StreamingSystem.pas',
  StreamSysBinary in '..\..\Common\StreamingSystem\StreamSysBinary.pas',
  StreamSysXML in '..\..\Common\StreamingSystem\StreamSysXML.pas',
  TrivXML in '..\..\Common\StreamingSystem\TrivXML.PAS',
  TrivXMLDefs in '..\..\Common\StreamingSystem\TrivXMLDefs.pas',
  CocoBase in '..\..\CoCoDM\Distributable\Frames\CocoBase.pas',
  QueuedMailer in '..\..\Common\QueuedMailer\QueuedMailer.pas',
  CheckInAppConfig in '..\..\CheckIn\CheckInAppConfig.pas',
  CheckInAudit in '..\..\CheckIn\CheckInAudit.pas',
  BufferedFileStream in '..\..\Common\CachedStream\BufferedFileStream.pas',
  HTTPForwarder in '..\..\Common\HTTPServerDispatcher\HTTPForwarder.pas',
  HTTPMisc in '..\..\Common\HTTPServerDispatcher\HTTPMisc.pas',
  CheckInServiceCoOrdinator in '..\..\CheckIn\CheckInServiceCoOrdinator.pas',
  Parallelizer in '..\..\Common\Parallelizer\Parallelizer.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TServerFrm, ServerFrm);
  Application.Run;
end.
