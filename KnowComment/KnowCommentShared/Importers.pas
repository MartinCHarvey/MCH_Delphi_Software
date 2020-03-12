unit Importers;
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

uses
  CommonPool, FetcherParser, Trackables, SyncObjs, Classes, DataObjects,
  SysUtils;

// TODO - eventually, have separate insta / twitter / etc importers.

type
  TImporter = class;

  TImporterWorkItem = class(TCommonPoolWorkItem)
  private
    FImporter: TImporter;
    FFPResult: TFetchParseResult;
  protected
    function DoWork: integer; override;
  public
    constructor Create;
    destructor Destroy; override;
    property Importer: TImporter read FImporter;
    property FPResult: TFetchParseResult read FFPResult;
  end;

  TImporterState = (stIdle, stRunning, stDeleting);

{$IFDEF USE_TRACKABLES}

  TImportOptions = class(TTrackable)
{$ELSE}
  TImportOptions = class
{$ENDIF}
  private
    FBailoutTime: TDateTime;
    FInitialFetchOnly: boolean;
  public
    property BailoutTime: TDateTime read FBailoutTime write FBailoutTime;
    property InitialFetchOnly: boolean read FInitialFetchOnly
      write FInitialFetchOnly;
  end;

  // OK, all fine.
  // Warn: Received data decoded OK, but not all data retrieved
  // (because one or more fetches failed).
  // Fail: Either no data at all, or decoded data bad.
  TImportErrLevel = (ielOK, ielWarn, ielFail);

{$IFDEF USE_TRACKABLES}

  TImportErrInfo = class(TTrackable)
{$ELSE}
  TImportErrInfo = class
{$ENDIF}
  private
    FFetchOK: boolean;
    FParseOK: boolean;
    FHTTPCode: integer;
    FErrMsg: string;
  protected
    function GetImportErrLevel: TImportErrLevel;
  public
    procedure Reset;
    property ImportErrLevel: TImportErrLevel read GetImportErrLevel;
    property FetchOK: boolean read FFetchOK write FFetchOK;
    property ParseOK: boolean read FParseOK write FParseOK;
    property HTTPCode: integer read FHTTPCode;
    procedure Assign(Src: TImportErrInfo);
    property ErrMsg: string read FErrMsg write FErrMsg;
  end;

{$IFDEF USE_TRACKABLES}

  TImporter = class(TTrackable)
{$ELSE}
  TImporter = class
{$ENDIF}
  private
    FCommonPoolClientRec: TClientRec;
    FPreDeleteDone: boolean;
    FLock: TCriticalSection;
    FState: TImporterState;
    FFetchParseRefCount: integer;
    FFetchParsesDone: TEvent;
    FOnRequestCompleted: TNotifyEvent;
    FThrottleDelay: integer;
    FRef: TObject;

    procedure DoRequestCompleted;

    function StartDeleteProcess: boolean; // Destructor has started,
    procedure WaitDeleteReady;

    procedure HandleFetchParseInThread(WorkItem: TImporterWorkItem);
    procedure IncThrottleDelay();
    procedure DecThrottleDelay();
  protected
    FErrInfo: TImportErrInfo;
    // Lock and unlock *only* to be used to protect retrival of final result.
    function LockCheckIdle: boolean;
    procedure UnlockFromIdle;

    function AttemptNextFetchParseRequest: boolean;
    // Working, can astart Fetch-parse?
    function FinishedFetchParseRequest: boolean;

    function AttemptStartProcessing: boolean;
    // Idle, can start working on behalf of client.
    procedure FinishedProcessing(WorkItem: TImporterWorkItem);

    function ProcessParseTree(WorkItem: TImporterWorkItem; var ErrMsg: string)
      : boolean; virtual; abstract;
    function CheckAssigned(Obj: TObject; Msg: string = ''): TObject; virtual;
  public
    constructor Create;
    destructor Destroy; override;
    function Stop: boolean; // returns whether idle
    function HandleFetchParse(FPResult: TFetchParseResult): boolean;
    property OnRequestCompleted: TNotifyEvent read FOnRequestCompleted
      write FOnRequestCompleted;
    property ThrottleDelay: integer read FThrottleDelay;
    property Ref: TObject read FRef write FRef;
  end;

  TUserProfileImporter = class(TImporter)
  public
    // Does not take ownership of blocks.
    // Unfortunately, request functions require different param lists.
    // function RequestUserProfile(Block: TKSiteUserBlock; Options: TImportOptions): boolean; virtual; abstract;
    function RetrieveResult(var UserProfile: TKUserProfile;
      var ErrInfo: TImportErrInfo): boolean; virtual; abstract;
  end;

  TMediaImporter = class(TImporter)
  public
    // Does not take ownership of blocks.
    // Unfortunately, request functions require different param lists.
    // function RequestMedia(Block: TKSiteMediaBlock; Options: TImportOptions): boolean; virtual; abstract;
    function RetrieveResult(var Media: TKMediaItem; var ErrInfo: TImportErrInfo)
      : boolean; virtual; abstract;
  end;

  EImportError = class(Exception);

{$IFDEF USE_TRACKABLES}

  TFPCompHandler = class(TTrackable)
{$ELSE}
  TFPCompHandler = class
{$ENDIF}
  private
  protected
    procedure HandleFetchParsesCompleted(Sender: TObject);
  public
  end;

implementation

uses
  InstagramImporters, JSNodes, HTTPDocFetcher, TwitterImporters,
  HTMLNodes;

const
  S_NULL_PARSE_TREE = 'Importer given NULL parse tree, (exception in parser)';

var
  // Common
  FPCompHandler: TFPCompHandler;

procedure CreateGlobals;
begin
  FPCompHandler := TFPCompHandler.Create;
  Assert(not Assigned(GFetcherParser.OnFetchParsesCompleted));
  GFetcherParser.OnFetchParsesCompleted :=
    FPCompHandler.HandleFetchParsesCompleted;
end;

procedure DestroyGlobals;
begin
  FPCompHandler.Free;
end;

{ TImportErrInfo }

procedure TImportErrInfo.Reset;
begin
  FFetchOK := false;
  FParseOK := false;
  FHTTPCode := 0;
end;

function TImportErrInfo.GetImportErrLevel: TImportErrLevel;
begin
  if FParseOK then
  begin
    if FFetchOK then
      result := ielOK
    else
      result := ielWarn;
  end
  else
    result := ielFail;
end;

procedure TImportErrInfo.Assign(Src: TImportErrInfo);
begin
  FFetchOK := Src.FFetchOK;
  FParseOK := Src.FParseOK;
  FHTTPCode := Src.FHTTPCode;
  FErrMsg := Src.FErrMsg;
end;

{ TImporterWorkItem }

function TImporterWorkItem.DoWork;
begin
  Assert(Assigned(FImporter));
  Assert(Assigned(FFPResult));
  FImporter.HandleFetchParseInThread(self);
  result := 0;
end;

constructor TImporterWorkItem.Create;
begin
  inherited;
  CanAutoFree := true;
end;

destructor TImporterWorkItem.Destroy;
begin
  FFPResult.Free;
  inherited;
end;

{ TImporter }

function TImporter.CheckAssigned(Obj: TObject; Msg: string): TObject;
begin
  if not Assigned(Obj) then
    raise EImportError.Create(Msg);
  result := Obj;
end;

constructor TImporter.Create;
begin
  inherited;
  FFetchParsesDone := TEvent.Create(nil, true, false, '');
  FLock := TCriticalSection.Create;
  FCommonPoolClientRec := GCommonPool.RegisterClient(self, nil, nil);
  FErrInfo := TImportErrInfo.Create;
end;

function TImporter.Stop: boolean;
begin
  result := not StartDeleteProcess;
  FPreDeleteDone := true;
end;

destructor TImporter.Destroy;
begin
  if not FPreDeleteDone then
    Stop;
  WaitDeleteReady;
  GCommonPool.DeRegisterClient(FCommonPoolClientRec);
  FLock.Free;
  FErrInfo.Free;
  inherited;
end;

function TImporter.AttemptStartProcessing: boolean;
// Idle, can start working on behalf of client.
begin
  FLock.Acquire;
  try
    result := (FState = stIdle) and (FFetchParseRefCount = 0);
    if result then
    begin
      FState := stRunning;
    end;
  finally
    FLock.Release;
  end;
end;

function TImporter.AttemptNextFetchParseRequest: boolean;
begin
  FLock.Acquire;
  try
    result := FState = stRunning;
    if result then
      Inc(FFetchParseRefCount);
  finally
    FLock.Release;
  end;
end;

function TImporter.FinishedFetchParseRequest: boolean;
begin
  FLock.Acquire;
  try
    // Can be in any state, because finished processing, and
    // finished fetch parse request may be called out of order.
    Dec(FFetchParseRefCount);
    Assert(FFetchParseRefCount >= 0);
    if (FState = stDeleting) and (FFetchParseRefCount = 0) then
      FFetchParsesDone.SetEvent;
    result := true;
  finally
    FLock.Release;
  end;
end;

procedure TImporter.FinishedProcessing(WorkItem: TImporterWorkItem);
begin
  FLock.Acquire;
  try
    Assert(FState <> stIdle);
    // Shouldn't call finish twice, only call if nothing in progress.
    if FState = stRunning then
      FState := stIdle;
  finally
    FLock.Release;
  end;
  if Assigned(WorkItem) then
    WorkItem.ThreadRec.Thread.Queue(DoRequestCompleted);
end;

procedure TImporter.DoRequestCompleted;
begin
  if Assigned(FOnRequestCompleted) then
    FOnRequestCompleted(self);
end;

function TImporter.StartDeleteProcess: boolean;
begin
  FLock.Acquire;
  try
    result := FFetchParseRefCount > 0;
    if result then
    begin
      FFetchParsesDone.ResetEvent;
    end
    else
      FFetchParsesDone.SetEvent;
    FState := stDeleting;
  finally
    FLock.Release;
  end;
end;

procedure TImporter.WaitDeleteReady;
var
  Wait: boolean;
begin
  Wait := false;
  FLock.Acquire;
  try
    Assert(FState = stDeleting);
    if FFetchParseRefCount > 0 then
      Wait := true;
  finally
    FLock.Release;
  end;
  if Wait then
    FFetchParsesDone.WaitFor(INFINITE);
end;

function TImporter.HandleFetchParse(FPResult: TFetchParseResult): boolean;
var
  WorkItem: TImporterWorkItem;
begin
  WorkItem := TImporterWorkItem.Create;
  WorkItem.FImporter := self;
  WorkItem.FFPResult := FPResult;
  result := GCommonPool.AddWorkItem(FCommonPoolClientRec, WorkItem);
  if not result then
  begin
    FinishedFetchParseRequest;
    WorkItem.Free;
  end;
end;

function RandConst: integer;
begin
  result := Random(2);
  Assert(result >= 0);
  Assert(result <= 1);
  Inc(result);
end;

procedure TImporter.IncThrottleDelay();
begin
  Inc(FThrottleDelay, 1000 * RandConst);
end;

procedure TImporter.DecThrottleDelay();
begin
  if FThrottleDelay > 2000 then
    Dec(FThrottleDelay, 1000 * RandConst)
  else if FThrottleDelay > 1000 then
    Dec(FThrottleDelay, 100 * RandConst)
  else if FThrottleDelay > 100 then
    Dec(FThrottleDelay, 10 * RandConst)
  else if FThrottleDelay > 0 then
    Dec(FThrottleDelay, RandConst);
end;

procedure TImporter.HandleFetchParseInThread(WorkItem: TImporterWorkItem);
var
  QueryOpts: TFetcherOpts;
begin
  FLock.Acquire;
  try
    Assert(Assigned(WorkItem));
    Assert(WorkItem.Importer = self);
    Assert(Assigned(WorkItem.FPResult));
    // No, parses can go awry.
    // Assert(Assigned(WorkItem.FPResult.ParseResult)
    // = (WorkItem.FPResult.FetchResult = difNone));
    FErrInfo.FFetchOK := WorkItem.FPResult.FetchResult = difNone;
    if FErrInfo.FFetchOK then
    begin
      // Clear possible previous errors from HTTP 429.
      FErrInfo.FHTTPCode := 0;
      FErrInfo.FErrMsg := '';
      DecThrottleDelay();
      if Assigned(WorkItem.FPResult.ParseResult) then
        FErrInfo.FParseOK := ProcessParseTree(WorkItem, FErrInfo.FErrMsg)
      else
      begin
        FErrInfo.FParseOK := false;
        FErrInfo.FErrMsg := S_NULL_PARSE_TREE;
      end;
      if not FErrInfo.FParseOK then
        FinishedProcessing(WorkItem);
    end
    else
    begin
      FErrInfo.FHTTPCode := WorkItem.FPResult.HTTPCode;
      if FErrInfo.FHTTPCode = 429 then
      // HTTP codes indicating rate limiting required
      begin
        // Retry with a higher throttling value, exponential backoff.
        IncThrottleDelay();

        if not Assigned(WorkItem.FPResult.FetcherOpts) then
        begin
          QueryOpts := WorkItem.FPResult.FetcherOpts;
          WorkItem.FPResult.FetcherOpts := nil;
        end
        else
          QueryOpts := TFetcherOpts.Create;

        QueryOpts.ThrottleDelay := FThrottleDelay;
        // And retry the request, adding new fetch/parse in same way as supplementary requests.
        if AttemptNextFetchParseRequest then
        begin
          if not GFetcherParser.AddDocToFetchParse(WorkItem.FPResult.Url, nil,
            WorkItem.FPResult.Method, QueryOpts, self, nil) then
          begin
            QueryOpts.Free;
            FinishedFetchParseRequest;
            FinishedProcessing(WorkItem)
          end;
        end
        else
          FinishedProcessing(WorkItem);
      end
      else
      begin
        FErrInfo.FErrMsg := WorkItem.FPResult.FetchString;
        FinishedProcessing(WorkItem);
      end;
    end;
    FinishedFetchParseRequest;
  finally
    FLock.Release;
  end;
end;

function TImporter.LockCheckIdle: boolean;
begin
  FLock.Acquire;
  result := FState = stIdle;
end;

procedure TImporter.UnlockFromIdle;
begin
  FLock.Release;
end;

{ TFPCompHandler }

procedure TFPCompHandler.HandleFetchParsesCompleted(Sender: TObject);

var
  FPResult: TFetchParseResult;

begin
  FPResult := GFetcherParser.GetCompletedFetchParse;
  while Assigned(FPResult) do
  begin
    (FPResult.FPRef1 as TImporter).HandleFetchParse(FPResult);
    // Importer has ownership or freed FPResult.
    FPResult := GFetcherParser.GetCompletedFetchParse;
  end;
end;

initialization

CreateGlobals;

finalization

DestroyGlobals;

end.
