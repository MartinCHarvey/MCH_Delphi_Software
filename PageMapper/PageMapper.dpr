program PageMapper;

uses
  Vcl.Forms,
  PageMapperFrm in 'PageMapperFrm.pas' {PageMapperForm},
  FetcherParser in '..\Common\HTTPFetcherParser\FetcherParser.pas',
  HTTPDocFetcher in '..\Common\HTTPFetcher\HTTPDocFetcher.pas',
  DLThreadQueue in '..\Common\WorkItems\DLThreadQueue.pas',
  DLList in '..\Common\DLList\DLList.pas',
  WorkItems in '..\Common\WorkItems\WorkItems.pas',
  CommonNodes in '..\Common\HTMLParser\CommonNodes.pas',
  CSSGrammar in '..\Common\HTMLParser\CSSGrammar.PAS',
  CSSNodes in '..\Common\HTMLParser\CSSNodes.pas',
  HTMLEscapeHelper in '..\Common\HTMLParser\HTMLEscapeHelper.pas',
  HTMLGrammar in '..\Common\HTMLParser\HTMLGrammar.PAS',
  HTMLNodes in '..\Common\HTMLParser\HTMLNodes.pas',
  HTMLParseEvents in '..\Common\HTMLParser\HTMLParseEvents.pas',
  HTMLParser in '..\Common\HTMLParser\HTMLParser.pas',
  JScriptGrammar in '..\Common\HTMLParser\JScriptGrammar.PAS',
  JSNodes in '..\Common\HTMLParser\JSNodes.pas',
  JSONGrammar in '..\Common\HTMLParser\JSONGrammar.PAS',
  JSONNodes in '..\Common\HTMLParser\JSONNodes.pas',
  Trackables in '..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\Common\Balanced Tree\BinaryTree.pas',
  CommonPool in '..\Common\WorkItems\CommonPool.pas',
  GlobalLog in '..\Common\Logging\GlobalLog.pas',
  IndexedStore in '..\Common\Indexed Store\IndexedStore.pas',
  CocoBase in '..\CoCoDM\Distributable\Frames\CocoBase.pas',
  ErrorRecovery in '..\Common\HTMLParser\ErrorRecovery.pas',
  SoftLexer in '..\Common\SoftLexer\SoftLexer.pas',
  OrdinalSets in '..\Common\OrdinalSets\OrdinalSets.pas',
  Parallelizer in '..\Common\Parallelizer\Parallelizer.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TPageMapperForm, PageMapperForm);
  Application.Run;
end.
