program MMapper;

uses
  FMX.Forms,
  MMapperFrm in 'MMapperFrm.pas' {MapperForm},
  WorkItems in '..\Common\WorkItems\WorkItems.pas',
  DLThreadQueue in '..\Common\WorkItems\DLThreadQueue.pas',
  DLList in '..\Common\DLList\DLList.pas',
  HTTPDocFetcher in '..\Common\HTTPFetcher\HTTPDocFetcher.pas',
  HTMLGrammar in '..\Common\HTMLParser\HTMLGrammar.PAS',
  Trackables in '..\Common\Tracking\Trackables.pas',
  BinaryTree in '..\Common\Balanced Tree\BinaryTree.pas',
  HTMLParser in '..\Common\HTMLParser\HTMLParser.pas',
  HTMLNodes in '..\Common\HTMLParser\HTMLNodes.pas',
  HTMLEscapeHelper in '..\Common\HTMLParser\HTMLEscapeHelper.pas',
  HTMLParseEvents in '..\Common\HTMLParser\HTMLParseEvents.pas',
  CommonNodes in '..\Common\HTMLParser\CommonNodes.pas',
  JSONNodes in '..\Common\HTMLParser\JSONNodes.pas',
  JSONGrammar in '..\Common\HTMLParser\JSONGrammar.PAS',
  JScriptGrammar in '..\Common\HTMLParser\JScriptGrammar.PAS',
  JSNodes in '..\Common\HTMLParser\JSNodes.pas',
  CSSGrammar in '..\Common\HTMLParser\CSSGrammar.PAS',
  CSSNodes in '..\Common\HTMLParser\CSSNodes.pas',
  FetcherParser in '..\Common\HTTPFetcherParser\FetcherParser.pas',
  CommonPool in '..\Common\WorkItems\CommonPool.pas',
  GlobalLog in '..\Common\Logging\GlobalLog.pas',
  OAuth2 in '..\Common\HTTPFetcher\OAuth2.pas',
  KKUtils in '..\KnowComment\KnowCommentShared\KKUtils.pas',
  IndexedStore in '..\Common\Indexed Store\IndexedStore.pas',
  CocoBase in '..\CoCoDM\Distributable\Frames\CocoBase.pas',
  ErrorRecovery in '..\Common\HTMLParser\ErrorRecovery.pas',
  SoftLexer in '..\Common\SoftLexer\SoftLexer.pas',
  OrdinalSets in '..\Common\OrdinalSets\OrdinalSets.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TMapperForm, MapperForm);
  Application.Run;
end.
