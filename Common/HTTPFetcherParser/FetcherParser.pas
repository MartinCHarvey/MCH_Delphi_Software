unit FetcherParser;
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

uses HTTPDocFetcher, HTMLParser, Trackables, Classes, DLThreadQueue,
     CommonNodes, HTMLEscapeHelper;

type
{$IFDEF USE_TRACKABLES}
  TFetchParseResult = class(TTrackable)
{$ELSE}
  TFetchParseResult = class
{$ENDIF}
  private
    FUrl, FOriginalUrl: string;
    FFetchResult: TDocItemFailReason;
    FHTTPCode: integer;
    FFetchString: string;
    FParseResult: TCommonNode;
    FParseEventList: TList;
    FMethod: TFetchParseMethod;
    FPassedInStream: TStream;
    FFPRef1, FFPRef2: TObject;
    FFetcherOpts: TFetcherOpts;
  public
    constructor Create;
    destructor Destroy; override;
    property FetchResult: TDocItemFailReason read FFetchResult;
    property HTTPCode: integer read FHTTPCode;
    property FetchString: string read FFetchString;
    property Method: TFetchParseMethod read FMethod;
    property ParseResult: TCommonNode read FParseResult write FParseResult;
    property ParseEventList: TList read FParseEventList;
    property Url: string read FUrl;
    property OriginalURL: string read FOriginalUrl;
    property PassedInStream: TStream read FPassedInStream write FPassedInStream;
    //Two arbitrary references for fetcher-parser.
    property FPRef1: TObject read FFPRef1 write FFPRef1;
    property FPRef2: TObject read FFPRef2 write FFPRef2;
    property FetcherOpts: TFetcherOpts read FFetcherOpts write FFetcherOpts;
  end;


{$IFDEF USE_TRACKABLES}
  TFetcherParser = class(TTrackable)
{$ELSE}
  TFetcherParser = class
{$ENDIF}
  private
    FStopping: boolean;
    FOnFetchParsesCompleted: TNotifyEvent;

    FCompletedQueue: TDLProxyThreadQueuePublicLock;
    FRefsQueue: TDLProxyThreadQueuePublicLock;

    FFetcher: THTTPDocFetcher;
    FParser: THTMLParser;
  protected
    procedure AddToCompletedQueueInternal(Res: TFetchParseResult);
    procedure HandleDependentFetch(Item: THTTPDocItem);
    procedure CandidateReparse(Item: THTMLParseItem);

    procedure HandleFetchesCompleted(Sender: TObject);
    procedure HandleFetchesAbandoned(Sender: TObject);
    procedure HandleParsesCompleted(Sender: TObject);
    procedure HandleParsesAbandoned(Sender: TObject);

    procedure DoFetchParsesCompleted;
    function GetParallelFetches: integer;
  public
    constructor Create;
    destructor Destroy; override;
    procedure Stop;
    procedure AgeCache;
    function AddDocToFetchParse(Url: string; PostData: TStream; Method:TFetchParseMethod;
                                Opts:TFetcherOpts; FPRef1, FPRef2: TObject): boolean;
    //Small "hack" to allow us to feed chunks of stuff directly into the parser if we need to.
    function AddStreamToParse(Stream: TStream; DocType: THTMLDocType;
                              FPRef1, FPRef2: TObject): boolean;

    procedure CancelOutstandingFetches;

    function GetCompletedFetchParse:TFetchParseResult;
    property OnFetchParsesCompleted: TNotifyEvent
      read FOnFetchParsesCompleted write FOnFetchParsesCompleted;
    property ParallelFetches: integer read GetParallelFetches;
  end;

var
  GFetcherParser:TFetcherParser;

implementation

uses HTMLParseEvents, System.Types, HTMLNodes, SysUtils
{$IFDEF INCLUDE_AUTH}
, OAuth2
{$ENDIF}
;

{ TFetchParseResult }

constructor TFetchParseResult.Create;
begin
 inherited;
 FParseEventList := TList.Create;
end;

destructor TFetchParseResult.Destroy;
var
  i: integer;
begin
  FParseResult.Free;
  for i := 0 to Pred(FParseEventList.Count) do
    TObject(FParseEventList.Items[i]).Free;
  FParseEventList.Free;
  FFetcherOpts.Free;
  FPassedInStream.Free;
  inherited;
end;

{ TFetcherParser }

procedure TFetcherParser.DoFetchParsesCompleted;
begin
  if Assigned(FOnFetchParsesCompleted) then
    FOnFetchParsesCompleted(self);
end;

procedure TFetcherParser.AddToCompletedQueueInternal(Res: TFetchParseResult);
var
  NonEmpty: boolean;
begin
  NonEmpty := FCompletedQueue.Count = 0;
  FCompletedQueue.AddTailObj(Res);
  if NonEmpty then
    DoFetchParsesCompleted;
end;

procedure TFetcherParser.CandidateReparse(Item: THTMLParseItem);
var
  FPResult: TFetchParseResult;
begin
  FPResult := nil;
  if Item.UnresolvedRefList.Count = 0 then
  begin
    if Assigned(Item.DLProxy) then
    begin
        FRefsQueue.RemoveObj(Item, Item.DLProxy);
        Item.DLProxy := nil;
    end;
    if Assigned(Item.ParseResult) and (Item.ParseResult is THTMLDocument) and
       ((Item.ParseResult as THTMLDocument).UnparsedScriptData) then
    begin
      if not FParser.ReparseItem(Item) then
      begin
        FPResult := Item.ResultRef as TFetchParseResult;
        FPResult.FFetchResult := difOther;
        FPResult.FFetchString := 'Error, failed to start reparse.';
        if Item.InputData = FPResult.FPassedInStream then
          Item.InputData := nil;
        Item.Free;
      end;
    end
    else
    begin
      FPResult := Item.ResultRef as TFetchParseResult;
      FPResult.FFetchResult := difNone;
      FPResult.FParseResult := Item.ParseResult;
      FPResult.FParseEventList.Assign(Item.EventList, laCopy);
      if Item.InputData = FPResult.FPassedInStream then
        Item.InputData := nil;
      Item.EventList.Clear;
      Item.ParseResult := nil;
      Item.Free;
    end;
  end
  else
  begin
    if not Assigned(Item.DLProxy) then
      Item.DLProxy := FRefsQueue.AddHeadObj(Item);
  end;
  if Assigned(FPResult) then
    AddToCompletedQueueInternal(FPResult);
end;


procedure TFetcherParser.HandleDependentFetch(Item: THTTPDocItem);
var
  ParseItem: THTMLParseItem;
  UR: TUnresolvedReference;
begin
  ParseItem := Item.ParserRef as THTMLParseItem;
  UR := Item.UnresolvedRef as TUnresolvedReference;
  UR.ScriptTag.ReferencedDataFetched(ParseItem.ParseResult as THTMLDocument, Item.Data);
  Item.Data := nil;
  ParseItem.UnresolvedRefList.Remove(UR);
  UR.Free;
  CandidateReparse(ParseItem);
end;

function TFetcherParser.AddStreamToParse(Stream: TStream; DocType: THTMLDocType;
                                         FPRef1, FPRef2: TObject): boolean;
var
  FPResult: TFetchParseResult;
begin
  if FStopping then
    result := false
  else
  begin
    FPResult := TFetchParseResult.Create;
    FPResult.FPassedInStream := Stream;
    FPResult.FUrl := '';
    FPResult.FPRef1 := FPRef1;
    FPResult.FPRef2 := FPRef2;
    result := FParser.AddItemToParse(Stream, FPResult.FUrl, DocType, FPResult);
    if not result then
      FPResult.Free;
  end;
end;

procedure TFetcherParser.HandleFetchesCompleted(Sender: TObject);
var
  Item: THTTPDocItem;
  FPResult: TFetchParseResult;
begin
  Assert(Sender = FFetcher);
  Item := FFetcher.GetRetrievedDoc;
  FPResult := nil;
  while Assigned(Item) do
  begin
    if Assigned(Item.ParserRef) or Assigned(Item.UnresolvedRef) then
      HandleDependentFetch(Item)
    else
    begin
      if Item.FailReason <> difRedirect then //Redirects just go in the bin.
      begin
        FPResult := TFetchParseResult.Create;
        FPResult.FUrl := Item.Url;
        FPResult.FOriginalUrl := Item.OriginalUrl;
        FPResult.FFetchResult := Item.FailReason;
        FPResult.FHTTPCode := Item.HTTPCode;
        FPResult.FFetchString := Item.FailString;
        FPResult.FMethod := Item.Method;
        FPResult.FPRef1 := Item.FPRef1;
        FPResult.FPRef2 := Item.FPRef2;
        FPResult.FFetcherOpts := Item.FetcherOpts;
        Item.FetcherOpts := nil;
        if (FPResult.FFetchResult = difNone)
           and Assigned(Item.Data)
           and not FStopping then
        begin
          if FParser.AddItemToParse(Item.Data, Item.Url, Item.DocType, FPResult) then
          begin
            Item.Data := nil;
            FPResult := nil;
          end
          else
          begin
            FPResult.FFetchResult := difOther;
            FPResult.FFetchString := 'Error, failed to start parse.';
            //FPResult to go in completed queue.
          end;
        end; //else FPResult to go in completed queue.
      end;
    end;
    if Assigned(FPResult) then //Error cases add to the completed queue immediately.
    begin
      AddToCompletedQueueInternal(FPResult);
      FPResult := nil;
    end;

    Item.Free;
    Item := FFetcher.GetRetrievedDoc;
  end;
end;

procedure TFetcherParser.HandleFetchesAbandoned(Sender: TObject);
var
  Item: THTTPDocItem;
  FPResult: TFetchParseResult;
begin
  Item := FFetcher.GetAbandonedDoc;
  FPResult := nil;
  while Assigned(Item) do
  begin
    if Item.FailReason <> difRedirect then
    begin
      //Dependent fetch for something else?
      if Assigned(Item.ParserRef) or Assigned(Item.UnresolvedRef) then
      begin
        if Assigned(Item.Data) then
        begin
          (Item.ParserRef as THTMLParseItem).EventList.Add(
            TParseEvent.CreateFromScriptReferencingErr(
              (Item.UnresolvedRef as TUnresolvedReference).ScriptTag.Col,
              (Item.UnresolvedRef as TUnresolvedReference).ScriptTag.Line,
              'Fetched script data may be bad, HTTP error:' + IntToStr(Item.HTTPCode)));
        end;
        //Assert(not Assigned(Item.Data)); //Don't want random "not found" HTML....
        HandleDependentFetch(Item);
      end
      else
      begin
        FPResult := TFetchParseResult.Create;
        FPResult.FUrl := Item.Url;
        FPResult.FOriginalUrl := Item.OriginalUrl;
        FPResult.FFetchResult := Item.FailReason;
        FPResult.FHTTPCode := Item.HTTPCode;
        FPResult.FFetchString := Item.FailString;
        FPResult.FMethod := Item.Method;
        FPResult.FPRef1 := Item.FPRef1;
        FPResult.FPRef2 := Item.FPRef2;
        FPResult.FFetcherOpts := Item.FetcherOpts;
        Item.FetcherOpts := nil;
      end;
    end;
    if Assigned(FPResult) then //Error cases add to the completed queue immediately.
    begin
      AddToCompletedQueueInternal(FPResult);
      FPResult := nil;
    end;

    Item.Free;
    Item := FFetcher.GetAbandonedDoc;
  end;
end;

procedure TFetcherParser.HandleParsesCompleted(Sender: TObject);
var
  Item: THTMLParseItem;
  UR: TUnresolvedReference;
  i: integer;
begin
  Item := FParser.GetParsedItem;
  while Assigned(Item) do
  begin
    i := 0;
    if (Item.ResultRef as TFetchParseResult).Method = fpmGetWithScripts then
    begin
      while i < Item.UnresolvedRefList.Count do
      begin
        UR := TUnresolvedReference(Item.UnresolvedRefList.Items[i]);
        //Method fpmGet, as I have yet to test multiple recursive script references.....
        if not FFetcher.AddUrlToRetrieve(UR.CanonicalURL, nil, UR.ParseItem, UR, fpmGet, nil, nil, nil) then
        begin
          Item.EventList.Add(
              TParseEvent.CreateFromScriptReferencingErr(
                UR.ScriptTag.Col,
                UR.ScriptTag.Line,
                'Internal, unable to fetch script data.'));
          Item.UnresolvedRefList.Remove(UR);
          UR.Free;
        end
        else
          Inc(i);
      end;
    end
    else
    begin
      while i < Item.UnresolvedRefList.Count do
      begin
        UR := TUnresolvedReference(Item.UnresolvedRefList.Items[i]);
        UR.Free;
        Inc(i);
      end;
      Item.UnresolvedRefList.Clear;
    end;
    CandidateReparse(Item);
    Item := FParser.GetParsedItem;
  end;
end;

procedure TFetcherParser.HandleParsesAbandoned(Sender: TObject);
var
  Item: THTMLParseItem;
  FPResult: TFetchParseResult;
begin
  Item := FParser.GetAbandonedItem;
  while Assigned(Item) do
  begin
    FPResult := Item.ResultRef as TFetchParseResult;
    FPResult.FFetchResult := difOther;
    FPResult.FFetchString := 'Parse abandoned.';
    FPResult.FParseResult := Item.ParseResult;
    FPResult.FParseEventList.Assign(Item.EventList, laCopy);
    if Item.InputData = FPResult.FPassedInStream then
      Item.InputData := nil;
    Item.EventList.Clear;
    Item.ParseResult := nil;
    Item.Free;
    AddToCompletedQueueInternal(FPResult);
    Item := FParser.GetAbandonedItem;
  end;
end;

procedure TFetcherParser.Stop;
begin
  FStopping := true;
end;

constructor TFetcherParser.Create;
begin
  inherited;
  FCompletedQueue := TDLProxyThreadQueuePublicLock.Create;
  FRefsQueue := TDLProxyThreadQueuePublicLock.Create;
  FCompletedQueue.Lock := nil;
  FRefsQueue.Lock := nil;
  FFetcher := THTTPDocFetcher.Create;
  FFetcher.OnDocsCompleted := HandleFetchesCompleted;
  FFetcher.OnDocsAbandoned := HandleFetchesAbandoned;
{$IFDEF INCLUDE_AUTH}
  FFetcher.AuthProvider := GAuthProvider;
{$ENDIF}
  FParser := THTMLParser.Create;
  FParser.OnItemsParsed := HandleParsesCompleted;
  FParser.OnItemsAbandoned := HandleParsesAbandoned;
end;

procedure TFetcherParser.AgeCache;
begin
  if Assigned(FFetcher) then
    FFetcher.AgeCache;
end;

destructor TFetcherParser.Destroy;
var
  O: TObject;
begin
  //Free calls flush workitems through.
  FFetcher.Free;
  FParser.Free;

  O := FRefsQueue.RemoveHeadObj;
  while Assigned(O) do
  begin
    O.Free;
    O := FRefsQueue.RemoveHeadObj;
  end;

  O := FCompletedQueue.RemoveHeadObj;
  while Assigned(O) do
  begin
    O.Free;
    O := FCompletedQueue.RemoveHeadObj;
  end;

  inherited;
end;

function TFetcherParser.AddDocToFetchParse(Url: string; PostData: TStream; Method:TFetchParseMethod;
                                           Opts:TFetcherOpts; FPRef1, FPRef2: TObject): boolean;
begin
  if FStopping then
    result := false
  else
    Result := FFetcher.AddUrlToRetrieve(Url, PostData, nil, nil, Method, Opts, FPRef1, FPRef2);
end;

function TFetcherParser.GetCompletedFetchParse:TFetchParseResult;
begin
  result := FCompletedQueue.RemoveHeadObj as TFetchParseResult;
end;

procedure TFetcherParser.CancelOutstandingFetches;
begin
  FFetcher.CancelOutstandingFetches;
end;

function TFetcherParser.GetParallelFetches: integer;
begin
  result := FFetcher.ParallelFetches;
end;


procedure CreateGlobals;
begin
  Assert(not Assigned(GFetcherParser));
  GFetcherParser := TFetcherParser.Create;
end;

procedure DestroyGlobals;
begin
  Assert(Assigned(GFetcherParser));
  GFetcherParser.Free;
  GFetcherParser := nil;
end;

initialization
  CreateGlobals;
finalization
  DestroyGlobals;
end.
