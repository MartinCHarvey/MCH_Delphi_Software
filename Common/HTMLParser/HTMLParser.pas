unit HTMLParser;
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

interface

uses Classes, DLThreadQueue, SyncObjs, HTMLGrammar, HTMLNodes,
  Trackables, CommonNodes, JSONGrammar, JScriptGrammar, CSSGrammar,
  CoCoBase, HTMLEscapeHelper, CommonPool;

type
  THTMLParser = class;

  THTMLParseItem = class(TCommonPoolWorkItem)
  private
    //For external queueing ops via TDLThreadQueue, separate from TWorkItemQueue.
    FDLProxy: TDLThreadQueueable;

    FInputData: TStream;
    FParentParser: THTMLParser;
    FPageParser: THTMLGrammar;
    FJSONParser: TJSONGrammar;
    FJScriptParser: TJScriptGrammar;
    FCSSParser: TCSSGrammar;
    FParseResult: TCommonNode;
    FEventList: TList;
    FUnresolvedRefList: TList; //TODO - recalc unresolved ref list
    FCanonicalURL: string;
    FDocType: THTMLDocType;
    FResultRef: TObject;
  protected
    function GetParser(ParserType: THTMLDocType): TCoCoRGrammar;

    function InitialParse: integer;
    function ParseScripts: integer;

    function DoWork: integer; override;

    function GetListStream: TStream;
    function GetEventList: TList;
    function GetUnresolvedRefList: TList;
    function GetSuccessful:boolean;
  public
    constructor Create;
    destructor Destroy; override;

    //Helper function for delayed load scripts.
    procedure ParseScriptForBlock(Node: THTMLNode);

    //Only invoked directly elsewhere for inline scripts:
    //need to determine how much of input has been consumed.
    function ParseCommon(DocType: THTMLDocType;
                         InputStream: TStream;
                         var ParseResult: TCommonNode
{$IFDEF DEBUG_PARSERS}
                         ; ParentLine, ParentCol: integer
{$ENDIF}
                         ; var BytesConsumed: int64): boolean;

    property InputData: TStream read FInputData write FInputData;
    property ParseResult: TCommonNode read FParseResult write FParseResult;
    property ListStream: TStream read GetListStream;
    property EventList: TList read GetEventList;
    property UnresolvedRefList: TList read GetUnresolvedRefList;
    property Successful:boolean read GetSuccessful;
    property CanonicalURL: string read FCanonicalURL write FCanonicalURL;
    property DLProxy: TDLThreadQueueable read FDLProxy write FDLProxy;
    property ParentParser:THTMLParser read FParentParser write FParentParser;
    property CanAutoReset;
    property ResultRef: TObject read FResultRef write FResultRef;
  end;

{$IFDEF USE_TRACKABLES}
  THTMLParser = class(TTrackable)
{$ELSE}
  THTMLParser = class
{$ENDIF}
  private
    FCommonPoolClientRec: TClientRec;
    FParsedItems: TDLProxyThreadQueuePublicLock;
    FAbandonedItems: TDLProxyThreadQueuePublicLock;
    FDeleting: boolean;
    FOnItemsParsed: TNotifyEvent;
    FOnItemsAbandoned: TNotifyEvent;
    FLock: TCriticalSection;
  protected
    procedure CommonPoolNormalCompletion(Sender: TObject);
    procedure CommonPoolCancelledCompletion(Sender: TObject);

    procedure AcquireLock;
    procedure ReleaseLock;

    function AddCompletedParseInternal(Item: THTMLParseItem): boolean; //returns whether to notify main thread.
    function AddAbandonedParseInternal(Item: THTMLParseItem): boolean; //returns whether to notify main thread.
    procedure DoItemsParsed;
    procedure DoItemsAbandoned;
  public
    constructor Create;
    destructor Destroy; override;

    //Initial HTML stream that needs parsing.
    function AddItemToParse(Data: TStream; CanonicalURL: string; DocType: THTMLDocType; ResultRef: TObject): boolean;

    //HTML parsed - embedded scripts etc need parsing.
    function ReparseItem(Item: THTMLParseItem): boolean;

    function GetParsedItem(): THTMLParseItem;
    function GetAbandonedItem(): THTMLParseItem;

    //NB. These procedures called when completed / abandoned queue length
    //was originally zero, and is now nonzero.
    property OnItemsParsed: TNotifyEvent
      read FOnItemsParsed write FOnItemsParsed;
    property OnItemsAbandoned: TNotifyEvent
      read FOnItemsAbandoned write FOnItemsAbandoned;
  end;

implementation

//TODO - Allow for exceptions during the parse.
//tracking system should allow for a good clean-up

{ THTMLParseItem }

//IOUtils use temporary.
uses HTMLParseEvents, SysUtils, DLList, IOUtils, GlobalLog;

function THTMLParseItem.GetParser(ParserType: THTMLDocType): TCoCoRGrammar;
begin
  case ParserType of
    tdtHTML:
    begin
      if not Assigned(FPageParser) then
      begin
        FPageParser := THTMLGrammar.Create(nil);
        FPageParser.ClearSourceStream := false;
        FPageParser.GenListWhen := glNever;
        FPageParser.ParentObject := self;
      end;
      result := FPageParser;
    end;
    tdtJScript:
    begin
      if not Assigned(FJScriptParser) then
      begin
        FJScriptParser := TJScriptGrammar.Create(nil);
        FJScriptParser.ClearSourceStream := false;
        FJScriptParser.GenListWhen := glNever;
        FJScriptParser.ParentObject := self;
      end;
      result := FJScriptParser;
    end;
    tdtJSON:
    begin
      if not Assigned(FJSONParser) then
      begin
        FJSONParser := TJSONGrammar.Create(nil);
        FJSONParser.ClearSourceStream := false;
        FJSONParser.GenListWhen := glNever;
        FJSONParser.ParentObject := self;
      end;
      result := FJSONParser;
    end;
    tdtCSS:
    begin
      if not Assigned(FCSSParser) then
      begin
        FCSSParser := TCSSGrammar.Create(nil);
        FCSSParser.ClearSourceStream := false;
        FCSSParser.GenListWhen := glNever;
        FCSSParser.ParentObject := self;
      end;
      result := FCSSParser;
    end;
  else
    result := nil;
    Assert(false);
  end;
end;

procedure THTMLParseItem.ParseScriptForBlock(Node: THTMLNode);
var
  Block: THTMLBlock;
  Tag: THTMLScriptTag;
  ParseResult: TCommonNode;
  ParseSuccessful: boolean;
  BytesConsumed: int64;

begin
  ParseResult := nil;
  Block := Node as THTMLBlock;
  //Tag value pairs are chained off the script tag as child nodes,
  //So parse results of script must be chained off the underlying block.
  Assert(Assigned(Block.Tag));
  Tag := Block.Tag as THTMLScriptTag;
  if (Assigned(Tag.ScriptData) and not Tag.FailedParse)
    and DlItemIsEmpty(@Block.ContainedListHead)
    and (Tag.ScriptData.Size > 0)
    and (Tag.DocType <> tdtHTML) then
  begin
    ParseSuccessful := ParseCommon(Tag.DocType, Tag.ScriptData, ParseResult
{$IFDEF DEBUG_PARSERS}
                                  , Tag.Line, Tag.Col
{$ENDIF}
                                   , BytesConsumed);

    //Attach results even inf partially bad parse, creation rules
    //should be such that all classes are accounted for.
    if Assigned(ParseResult) then
    begin
        Assert(DLItemIsEmpty(@Block.ContainedListHead));
        DLListInsertHead(@Block.ContainedListHead, @ParseResult.SiblingListEntry);
    end;
    Tag.FailedParse := not (ParseSuccessful and Assigned(ParseResult));
    Block.FixupChildContainedPtrsRec; //Because can be done after initial parse.
    if Assigned(ParseResult) then
      ParseResult.IterateNodes(ExecuteAlways, FixNodeStrings, RecurseAlways, ParseResult, nil, nil);
  end;
end;

function THTMLParseItem.ParseScripts: integer;
begin
  result := 0;
  if FParseResult is THTMLDocument then
  begin
    with FParseResult as THTMLDocument do
    begin
      Assert(UnparsedScriptData);
      IterateNodes(ScriptNodeCheck, ScriptNodeParse, ScriptNodeRecurse, self, nil, nil);
      UnparsedScriptData := false;
    end;
  end;
end;

{$IFDEF DEBUG_PARSERS}
var
  Seq: integer = 0;
{$ENDIF}

function THTMLParseItem.ParseCommon(DocType: THTMLDocType;
                                    InputStream: TStream;
                                    var ParseResult: TCommonNode
{$IFDEF DEBUG_PARSERS}
                         ; ParentLine, ParentCol: integer
{$ENDIF}
                                    ; var BytesConsumed: int64): boolean;
var
  InputStreamCopy: TMemoryStream;
  InputStreamOldSize: int64;
  Parser: TCoCoRGrammar;
  i: integer;
{$IFDEF DEBUG_PARSERS}
  SProto, SSite, SFullFile, SName, SExt: string;
  EV: TCocoError;
  LSeq: integer;
  MarkerName: string;
{$ENDIF}
begin
  InputStreamCopy := nil;
  Parser := GetParser(DocType);
  BytesConsumed := 0;
  try
    InputStreamOldSize := InputStream.Size;
    if InputStream is TMemoryStream then
    begin
        Parser.SourceStream := InputStream as TMemoryStream;
    end
    else
    begin
      InputStreamCopy := TTrackedMemoryStream.Create;
      InputStreamCopy.CopyFrom(InputStream, InputStream.Size);
      Parser.SourceStream := InputStreamCopy
    end;
    try
      Parser.SourceStream.Seek(0, soFromBeginning);
      Parser.Execute;
      //BytesConsumed includes either a #0 (EOF) tacked on by the parser,
      //or alternatively, the "<" of the next "</script>" or "</style>",
      //and hence is one more char than the actual script data.
      if Parser.StreamPartRead >= 0 then
        BytesConsumed := Parser.StreamPartRead
      else
      begin
        if Parser.Successful then
          BytesConsumed := InputStream.Size
        else
        begin
          //Amount we relibably consumed is up to the first error.
          Assert(Parser.ErrorList.Count > 0);
          BytesConsumed := TCoCoError(Parser.ErrorList[0]).Pos;
        end;
      end;
      ParseResult := Parser.ParseResult as TCommonNode;
      if InputStream.Size > InputStreamOldSize then
      begin
        Assert(InputStream.Size = InputStreamOldSize + 1);
        InputStream.Size := InputStreamOldSize; //Parser has added a #0 on the end.
      end;
    except
      on E: Exception do
      begin
        (Parser.ParseTracker as TTracker).FreeTrackedClassesAndReset;
        ParseResult := nil;
        FEventList.Add(TParseEvent.CreateFromException(E, Parser));
        //Can we come up with any realistic estimate as to where it all went
        //wrong?
        if Parser.ErrorList.Count > 0 then
          BytesConsumed := TCoCoError(Parser.ErrorList[0]).Pos;
      end;
    end;
  finally
    Parser.ParseResult := nil;
    Parser.SourceStream := nil;
    InputStreamCopy.Free;
  end;
  (Parser.ParseTracker as TTracker).ResetWithoutFree;

  for i := 0 to Pred(Parser.ErrorList.Count) do
    FEventList.Add(TParseEvent.CreateFromSecondaryParseError(
      TCoCoError(Parser.ErrorList.Items[i]),
      Parser));

  Result := Parser.Successful;
{$IFDEF DEBUG_PARSERS}
  if DocType = tdtHTML then
  begin
    ParseUrl(FCanonicalURL, SProto, SSite, SFullFile, SName, SExt);

    //Debug code.
    if Length(SExt) = 0 then
      SExt := '.html';

    for i := 1 to Length(SName) do
      if CharInSet(SName[i], ['/', '\', '?']) then
        SName[i] := '_';

    for i := 1 to Length(SExt) do
      if CharInSet(SExt[i], ['/', '\', '?']) then
        SExt[i] := '_';
  end;
  //Debug code from here on down.
  LSeq := TInterlocked.Increment(Seq);
  if Result and Assigned(ParseResult) then
  begin
    if DocType = tdtHTML then
    begin
      MarkerName := '_good_' + IntToStr(LSeq) + '_' + SName + SExt + '_.txt';
    end
    else
    begin
      MarkerName := '_good_script_' + IntToStr(LSeq) + '_' + Parser.ClassName + '_' + IntToStr(ParentLine) + '_' +IntToStr(ParentCol) + '.txt';
    end;
    GLogLog(SV_INFO, MarkerName);
    GLogLogStream(SV_INFO, InputStream);
  end
  else
  begin
    if DocType = tdtHTML then
    begin
      MarkerName := '_bad_' + IntToStr(LSeq) + '_' + SName + SExt + '_.txt';
    end
    else
    begin
      MarkerName := '_bad_script_' + IntToStr(LSeq) + '_' + Parser.ClassName +
      '_' + IntToStr(ParentLine) + '_' +IntToStr(ParentCol) + '.txt';
    end;
    GLogLog(SV_INFO, MarkerName);
    GLogLogStream(SV_INFO, InputStream);

    //And dump out the errors too.
    for i := 0 to Pred(Parser.ErrorList.Count) do
    begin
      EV := TCoCoError(Parser.ErrorList.Items[i]);
      GLogLog(SV_INFO, ' Line: ' + IntToStr(EV.Line) +
                       ' Col: ' + IntToStr(EV.Col) +
                       ' Code: ' + IntToStr(EV.ErrorCode) + ' ' +
                       Parser.ErrorStr(EV.ErrorCode, ''));
    end;
  end;
{$ENDIF}
end;


function THTMLParseItem.InitialParse: integer;
var
  i: integer;
  Stats: TPostParseStats;
  UR: TUnresolvedReference;
  ParseSuccessful: boolean;
  BytesConsumed: int64;

begin
  result := 0;
  Assert(Assigned(FInputData));
  FInputData.Seek(0, soFromBeginning);

  ParseSuccessful := ParseCommon(FDocType, FInputData, FParseResult
{$IFDEF DEBUG_PARSERS}
                                  , 0,0
{$ENDIF}
  , BytesConsumed);
  try
    if Assigned(FParseResult) then
    begin
      if FParseResult is THTMLDocument then
      begin
        with FParseResult as THTMLDocument do
        begin
          HTMLPostParse(Stats, FEventList);
          HTMLGenRefList(FUnresolvedRefList, FEventList);
          for i := 0 to Pred(FUnresolvedRefList.Count) do
          begin
            UR := TUnresolvedReference(FUnresolvedRefList.Items[i]);
            UR.ParseItem := self;
            UR.CanonicalURL :=  MakeCanonicalURL(FCanonicalURL, UR.URL);
          end;
        end;
  {$IFDEF DEBUG_PARSERS}
    //Debug & stats code.
    FEventList.Add(TParseEvent.CreateFromTagMatchString(
      'Scripts fetched: ' + IntToStr(FUnresolvedRefList.Count)));

    FEventList.Add(TParseEvent.CreateFromTagMatchString(
      'Blocks coalesced: ' + IntToStr(Stats.BlocksCoalesced)));
    FEventList.Add(TParseEvent.CreateFromTagMatchString(
      'Tags matched: ' + IntToStr(Stats.TagsMatched)));
    FEventList.Add(TParseEvent.CreateFromTagMatchString(
      'Open Tags unmatched: ' + IntToStr(Stats.OpenTagsUnmatched)));
    FEventList.Add(TParseEvent.CreateFromTagMatchString(
      'Close Tags unmatched: ' + IntToStr(Stats.CloseTagsUnmatched)));
  {$ENDIF}
      end
      else
      begin
        if Assigned(ParseResult) then
        begin
          ParseResult.FixupChildContainedPtrsRec;
          ParseResult.IterateNodes(ExecuteAlways, FixNodeStrings, RecurseAlways, ParseResult, nil, nil);
        end;
        Assert(FUnresolvedRefList.Count = 0);
        FillChar(Stats, sizeof(Stats), 0);
      end;
    end;
  Except
    on E:Exception do
    begin
      FParseResult.Free;
      FParseResult := nil;
      FEventList.Add(TParseEvent.CreateFromTagMatchString(
        'Exception in HTML post parse stage: ' + E.Message));
    end;
  end;
end;

function THTMLParseItem.DoWork: integer;
begin
  if not Assigned(FParseResult) then
    result := InitialParse
  else
    result := ParseScripts;
end;

constructor THTMLParseItem.Create;
begin
  inherited;
  FEventList := TList.Create;
  FUnresolvedRefList := TList.Create;
end;

destructor THTMLParseItem.Destroy;
var
  i: integer;
begin
  FInputData.Free;
  FJSONParser.Free;
  FJScriptParser.Free;
  FPageParser.Free;
  FParseResult.Free;
  for i := 0 to Pred(EventList.Count) do
    TObject(EventList.Items[i]).Free;
  FEventList.Free;
  for i := 0 to Pred(UnresolvedRefList.Count) do
    TObject(UnresolvedRefList.Items[i]).Free;
  FUnresolvedRefList.Free;
  inherited;
end;

function THTMLParseItem.GetListStream: TStream;
begin
  if Assigned(FPageParser) then
    result := FPageParser.ListStream
  else
    result := nil;
end;

function THTMLParseItem.GetEventList:TList;
begin
  result := FEventList;
end;

function THTMLParseItem.GetUnresolvedRefList:TList;
begin
  result := FUnresolvedRefList;
end;

function THTMLParseItem.GetSuccessful:boolean;
begin
  if Assigned(FPageParser) then
    result := FPageParser.Successful
  else
    result := false;
end;


{ THTMLParser }

procedure THTMLParser.CommonPoolNormalCompletion(Sender: TObject);
var
  Item: THTMLParseItem;
begin
  Item := Sender as THTMLParseItem;
  Assert(Item.ThreadRec.CurItem = Item);
  if AddCompletedParseInternal(item) then
    Item.ThreadRec.Thread.Queue(DoItemsParsed);
end;

procedure THTMLParser.CommonPoolCancelledCompletion(Sender: TObject);
var
  Item: THTMLParseItem;
begin
  Item := Sender as THTMLParseItem;
  Assert(Item.ThreadRec.CurItem = Item);
  if AddCompletedParseInternal(item) then
    Item.ThreadRec.Thread.Queue(DoItemsAbandoned);
end;

constructor THTMLParser.Create;
begin
  inherited;
  FLock := TCriticalSection.Create;
  FCommonPoolClientRec := GCommonPool.RegisterClient(self, CommonPoolNormalCompletion, CommonPoolCancelledCompletion);
  FParsedItems := TDLProxyThreadQueuePublicLock.Create;
  FParsedItems.Lock := Self.FLock;
  FAbandonedItems := TDLProxyThreadQueuePublicLock.Create;
  FAbandonedItems.Lock := Self.FLock;
end;

destructor THTMLParser.Destroy;
var
  Obj: TObject;
begin
  FDeleting := true;
  GCommonPool.DeRegisterClient(FCommonPoolClientRec);
  AcquireLock;
  repeat
    Obj := FParsedItems.RemoveTailObj;
    if Assigned(Obj) then Obj.Free;
  until not Assigned(Obj);

  repeat
    Obj := FAbandonedItems.RemoveTailObj;
    if Assigned(Obj) then Obj.Free;
  until not Assigned(Obj);
  ReleaseLock;

  FLock := nil;
  FParsedItems.Free;
  FAbandonedItems.Free;
  inherited;
end;

procedure THTMLParser.AcquireLock;
begin
  FLock.Acquire;
  FParsedItems.AcquireLock;
  FAbandonedItems.AcquireLock;
end;

procedure THTMLParser.ReleaseLock;
begin
  FAbandonedItems.ReleaseLock;
  FParsedItems.ReleaseLock;
  FLock.Release;
end;

function THTMLParser.AddItemToParse(Data: TStream; CanonicalURL: string; DocType: THTMLDocType; ResultRef: TObject): boolean;
var
  Item: THTMLParseItem;
begin
  Item := THTMLParseItem.Create;
  Item.CanAutoReset := true;
  Item.FParentParser := self;
  Item.CanonicalURL := CanonicalURL;
  Item.FDocType := DocType;
  Item.ResultRef := ResultRef;
  Item.InputData := Data;

  AcquireLock;
  if not FDeleting then
    result := GCommonPool.AddWorkItem(FCommonPoolClientRec, Item)
  else
    result := false;
  ReleaseLock;
  if not Result then
  begin
    Item.InputData := nil;
    Item.Free;
  end;
end;

function THTMLParser.ReparseItem(Item: THTMLParseItem): boolean;
begin
  AcquireLock;
  if not FDeleting then
    result := GCommonPool.AddWorkItem(FCommonPoolClientRec, item)
  else
    result := false;
  ReleaseLock;
end;

function THTMLParser.GetParsedItem(): THTMLParseItem;
begin
  if not FDeleting then
  begin
    AcquireLock;
    result := FParsedItems.RemoveTailObj as THTMLParseItem;
    ReleaseLock;
  end
  else
    result := nil;
end;

function THTMLParser.GetAbandonedItem(): THTMLParseItem;
begin
  if not FDeleting then
  begin
    AcquireLock;
    Result := FAbandonedItems.RemoveTailObj as THTMLParseItem;
    ReleaseLock;
  end
  else
    result := nil;
end;

function THTMLParser.AddCompletedParseInternal(Item: THTMLParseItem): boolean;
begin
  AcquireLock;
  result := FParsedItems.Count = 0;
  FParsedItems.AddHeadObj(Item);
  ReleaseLock;
end;

function THTMLParser.AddAbandonedParseInternal(Item: THTMLParseItem): boolean;
begin
  AcquireLock;
  result := FAbandonedItems.Count = 0;
  FAbandonedItems.AddHeadObj(Item);
  ReleaseLock;
end;

procedure THTMLParser.DoItemsParsed;
begin
  if Assigned(FOnItemsParsed) then
    FOnItemsParsed(Self);
end;

procedure THTMLParser.DoItemsAbandoned;
begin
  if Assigned(FOnItemsAbandoned) then
    FOnItemsAbandoned(Self);
end;

end.
