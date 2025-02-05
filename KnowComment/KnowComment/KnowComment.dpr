program KnowComment;

{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

uses
  FMX.Forms,
  MainForm in 'MainForm.pas' {MainFrm},
  ClientForm in 'ClientForm.pas' {ClientFrm},
  MemDB in '..\..\Common\MemDB\MemDB.pas',
  MemDBApi in '..\..\Common\MemDB\MemDBApi.pas',
  MemDBBuffered in '..\..\Common\MemDB\MemDBBuffered.pas',
  MemDBIndexing in '..\..\Common\MemDB\MemDBIndexing.pas',
  MemDBJournal in '..\..\Common\MemDB\MemDBJournal.pas',
  MemDBMisc in '..\..\Common\MemDB\MemDBMisc.pas',
  MemDbStreamable in '..\..\Common\MemDB\MemDbStreamable.pas',
  Trackables in '..\..\Common\Tracking\Trackables.pas',
  IndexedStore in '..\..\Common\Indexed Store\IndexedStore.pas',
  GlobalLog in '..\..\Common\Logging\GlobalLog.pas',
  BinaryTree in '..\..\Common\Balanced Tree\BinaryTree.pas',
  DLList in '..\..\Common\DLList\DLList.pas',
  CocoBase in '..\..\CoCoDM\Distributable\Frames\CocoBase.pas',
  BufferedFileStream in '..\..\Common\CachedStream\BufferedFileStream.pas',
  CommonNodes in '..\..\Common\HTMLParser\CommonNodes.pas',
  CSSGrammar in '..\..\Common\HTMLParser\CSSGrammar.PAS',
  CSSNodes in '..\..\Common\HTMLParser\CSSNodes.pas',
  ErrorRecovery in '..\..\Common\HTMLParser\ErrorRecovery.pas',
  HTMLEscapeHelper in '..\..\Common\HTMLParser\HTMLEscapeHelper.pas',
  HTMLGrammar in '..\..\Common\HTMLParser\HTMLGrammar.PAS',
  HTMLNodes in '..\..\Common\HTMLParser\HTMLNodes.pas',
  HTMLParseEvents in '..\..\Common\HTMLParser\HTMLParseEvents.pas',
  HTMLParser in '..\..\Common\HTMLParser\HTMLParser.pas',
  JScriptGrammar in '..\..\Common\HTMLParser\JScriptGrammar.PAS',
  JSNodes in '..\..\Common\HTMLParser\JSNodes.pas',
  JSONGrammar in '..\..\Common\HTMLParser\JSONGrammar.PAS',
  JSONNodes in '..\..\Common\HTMLParser\JSONNodes.pas',
  CommonPool in '..\..\Common\WorkItems\CommonPool.pas',
  DLThreadQueue in '..\..\Common\WorkItems\DLThreadQueue.pas',
  WorkItems in '..\..\Common\WorkItems\WorkItems.pas',
  SoftLexer in '..\..\Common\SoftLexer\SoftLexer.pas',
  OrdinalSets in '..\..\Common\OrdinalSets\OrdinalSets.pas',
  BatchLoader in '..\KnowCommentShared\BatchLoader.pas',
  DataObjects in '..\KnowCommentShared\DataObjects.pas',
  DataStore in '..\KnowCommentShared\DataStore.pas',
  DBGeneric in '..\KnowCommentShared\DBGeneric.pas',
  DBPersist in '..\KnowCommentShared\DBPersist.pas',
  DBSchemaStrs in '..\KnowCommentShared\DBSchemaStrs.pas',
  Importers in '..\KnowCommentShared\Importers.pas',
  InstagramImporters in '..\KnowCommentShared\InstagramImporters.pas',
  KKUtils in '..\KnowCommentShared\KKUtils.pas',
  MemDBInit in '..\KnowCommentShared\MemDBInit.pas',
  MemDBPersist in '..\KnowCommentShared\MemDBPersist.pas',
  TwitterImporters in '..\KnowCommentShared\TwitterImporters.pas',
  FetcherParser in '..\..\Common\HTTPFetcherParser\FetcherParser.pas',
  HTTPDocFetcher in '..\..\Common\HTTPFetcher\HTTPDocFetcher.pas',
  AuthenticateForm in '..\KnowCommentLoader\AuthenticateForm.pas' {AuthenticateFrm},
  ImageCache in 'ImageCache.pas',
  DBContainer in 'DBContainer.pas',
  StatsForm in 'StatsForm.pas' {StatsFrm},
  QueryForm in 'QueryForm.pas' {QueryFrm},
  GUIMisc in 'GUIMisc.pas',
  UserPrefs in 'UserPrefs.pas',
  AddNewUserForm in 'AddNewUserForm.pas' {AddNewUserFrm},
  SSAbstracts in '..\..\Common\StreamingSystem\SSAbstracts.pas',
  SSIntermediates in '..\..\Common\StreamingSystem\SSIntermediates.pas',
  SSStreamables in '..\..\Common\StreamingSystem\SSStreamables.pas',
  StreamingSystem in '..\..\Common\StreamingSystem\StreamingSystem.pas',
  StreamSysBinary in '..\..\Common\StreamingSystem\StreamSysBinary.pas',
  StreamSysXML in '..\..\Common\StreamingSystem\StreamSysXML.pas',
  TrivXML in '..\..\Common\StreamingSystem\TrivXML.PAS',
  TrivXMLDefs in '..\..\Common\StreamingSystem\TrivXMLDefs.pas',
  PrefsEditForm in 'PrefsEditForm.pas' {PrefsEditFrm},
  Parallelizer in '..\..\Common\Parallelizer\Parallelizer.pas',
  LockAbstractions in '..\..\Common\LockAbstractions\LockAbstractions.pas',
  NullStream in '..\..\Common\NullStream\NullStream.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TMainFrm, MainFrm);
  Application.CreateForm(TQueryFrm, QueryFrm);
  Application.CreateForm(TAddNewUserFrm, AddNewUserFrm);
  Application.CreateForm(TPrefsEditFrm, PrefsEditFrm);
  //  Application.CreateForm(TClientFrm, ClientFrm);
//  Application.CreateForm(TAuthenticateFrm, AuthenticateFrm);
  Application.CreateForm(TStatsFrm, StatsFrm);
  Application.Run;
end.
