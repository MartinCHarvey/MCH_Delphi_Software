program CheckInService;

uses
  Vcl.SvcMgr,
  SrvController in 'SrvController.pas' {ChkInService: TService},
  CheckInAppConfig in '..\CheckIn\CheckInAppConfig.pas',
  CheckInAppLogic in '..\CheckIn\CheckInAppLogic.pas',
  CheckInAudit in '..\CheckIn\CheckInAudit.pas',
  CheckInMailer in '..\CheckIn\CheckInMailer.pas',
  CheckInPageProducer in '..\CheckIn\CheckInPageProducer.pas',
  CheckInServiceCoOrdinator in '..\CheckIn\CheckInServiceCoOrdinator.pas',
  HTTPForwarder in '..\Common\HTTPServerDispatcher\HTTPForwarder.pas',
  HTTPMisc in '..\Common\HTTPServerDispatcher\HTTPMisc.pas',
  HTTPServerDispatcher in '..\Common\HTTPServerDispatcher\HTTPServerDispatcher.pas',
  HTTPServerPageProducer in '..\Common\HTTPServerDispatcher\HTTPServerPageProducer.pas',
  SSAbstracts in '..\Common\StreamingSystem\SSAbstracts.pas',
  SSIntermediates in '..\Common\StreamingSystem\SSIntermediates.pas',
  SSStreamables in '..\Common\StreamingSystem\SSStreamables.pas',
  StreamingSystem in '..\Common\StreamingSystem\StreamingSystem.pas',
  StreamSysBinary in '..\Common\StreamingSystem\StreamSysBinary.pas',
  StreamSysXML in '..\Common\StreamingSystem\StreamSysXML.pas',
  TrivXML in '..\Common\StreamingSystem\TrivXML.PAS',
  TrivXMLDefs in '..\Common\StreamingSystem\TrivXMLDefs.pas',
  Trackables in '..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\Common\Balanced Tree\BinaryTree.pas',
  IndexedStore in '..\Common\Indexed Store\IndexedStore.pas',
  DLList in '..\Common\DLList\DLList.pas',
  CocoBase in '..\CoCoDM\Distributable\Frames\CocoBase.pas',
  MemDB in '..\Common\MemDB\MemDB.pas',
  MemDBApi in '..\Common\MemDB\MemDBApi.pas',
  MemDBBuffered in '..\Common\MemDB\MemDBBuffered.pas',
  MemDBIndexing in '..\Common\MemDB\MemDBIndexing.pas',
  MemDBJournal in '..\Common\MemDB\MemDBJournal.pas',
  MemDBMisc in '..\Common\MemDB\MemDBMisc.pas',
  MemDbStreamable in '..\Common\MemDB\MemDbStreamable.pas',
  MemDBUtils in '..\Common\MemDB\MemDBUtils.pas',
  CommonPool in '..\Common\WorkItems\CommonPool.pas',
  DLThreadQueue in '..\Common\WorkItems\DLThreadQueue.pas',
  WorkItems in '..\Common\WorkItems\WorkItems.pas',
  BufferedFileStream in '..\Common\CachedStream\BufferedFileStream.pas',
  GlobalLog in '..\Common\Logging\GlobalLog.pas',
  HTMLEscapeHelper in '..\Common\HTMLParser\HTMLEscapeHelper.pas',
  QueuedMailer in '..\Common\QueuedMailer\QueuedMailer.pas',
  Parallelizer in '..\Common\Parallelizer\Parallelizer.pas';

{$R *.RES}

begin
  // Windows 2003 Server requires StartServiceCtrlDispatcher to be
  // called before CoRegisterClassObject, which can be called indirectly
  // by Application.Initialize. TServiceApplication.DelayInitialize allows
  // Application.Initialize to be called from TService.Main (after
  // StartServiceCtrlDispatcher has been called).
  //
  // Delayed initialization of the Application object may affect
  // events which then occur prior to initialization, such as
  // TService.OnCreate. It is only recommended if the ServiceApplication
  // registers a class object with OLE and is intended for use with
  // Windows 2003 Server.
  //
  // Application.DelayInitialize := True;
  //
  if not Application.DelayInitialize or Application.Installing then
    Application.Initialize;
  Application.CreateForm(TChkInService, ChkInService);
  Application.Run;
end.
