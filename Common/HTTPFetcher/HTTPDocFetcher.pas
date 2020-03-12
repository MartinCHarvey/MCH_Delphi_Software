unit HTTPDocFetcher;
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

{$DEFINE USE_SHARED_WORK_FARM}

uses DLThreadQueue, WorkItems, SyncObjs, Classes,
  IdHTTP, IdSSLOpenSSL, IdIOHandler, IdIOHandlerStack, SysUtils, IdCookieManager,
  IdCookie, HTMLEscapeHelper, IdHeaderList, Trackables;

type
  TFetchParseMethod = (fpmGet, fpmGetWithScripts, fpmHead, fpmPost, fpmPut, fpmDelete, fpmTrace);

  THTTPDocFetcher = class;
  TIdHttpCached = class;

  TDocItemFailReason = (difNone, difParams, difRedirect, difAuthenticate, difException, difOther);

  TRequestSigPolicy = (signNone, signAddToken, signAddAndSign);

  { Need a class to hold some advanced fetch options for instagram,
    in order to do ajax-like stuff. }
{$IFDEF USE_TRACKABLES}

  TFetcherOpts = class(TTrackable)
{$ELSE}
  TFetcherOpts = class
{$ENDIF}
  private
    FSendCSRFToken: boolean;
    FRequestContentType: string;
    FRequestCustomOrigin: string;
    FReferer: string;
    FThrottleDelay: integer;
    FRequireAuthBeforeFetch: boolean;
    FDesireAuth: boolean;
    FOAuth2SignRequest: TRequestSigPolicy;
    FCustomUserAgent: string;
    FSupplementaryHdrNames: TStringList;
    FSupplementaryHdrVals: TStringList;
  public
    constructor Create;
    destructor Destroy; override;
    property SendCSRFToken: boolean read FSendCSRFToken write FSendCSRFToken;
    property RequestContentType: string read FRequestContentType write FRequestContentType;
    property RequestCustomOrigin: string read FRequestCustomOrigin write FRequestCustomOrigin;
    property Referer: string read FReferer write FReferer;
    property ThrottleDelay: integer read FThrottleDelay write FThrottleDelay;
    property RequireAuthBeforeFetch: boolean read FRequireAuthBeforeFetch
      write FRequireAuthBeforeFetch;
    property DesireAuth: boolean read FDesireAuth write FDesireAuth;
    property OAuth2SignRequest: TRequestSigPolicy read FOAuth2SignRequest write FOAuth2SignRequest;
    property CustomUserAgent: string read FCustomUserAgent write FCustomUserAgent;
    property SupplementaryHeaderNames: TStringList read FSupplementaryHdrNames;
    property SupplementaryHeaderVals: TStringList read FSupplementaryHdrVals;
  end;

  THTTPDocItem = class(TWorkItem)
  private
    FUrl: string;
    FOriginalUrl: string;
    FRedirectCount: integer;
    FData: TStream;
    FResultDocType: THTMLDocType;
    FParentFetcher: THTTPDocFetcher;
    FCachedConn: TIdHttpCached;
    FFailReason: TDocItemFailReason;
    FHTTPCode: integer;
    FFailString: string;
    FParserRef, FUnresolvedRef: TObject;
    FMethod: TFetchParseMethod;
    FFPRef1, FFPRef2: TObject;
    FPostData: TStream;
    FFetcherOpts: TFetcherOpts;
  protected
    procedure DoCancel; override;
    function DoWork: integer; override;
    procedure DoNormalCompletion; override;
    procedure DoCancelledCompletion; override;
    function GetHTTPErrCode(E: EIdHTTPProtocolException): integer;
    procedure HandleIndyRedirect(Sender: TObject; var dest: string; var NumRedirect: integer;
      var Handled: boolean; var VMethod: TIdHTTPMethod);
    procedure HandleHeadersAvailable(Sender: TObject; AHeaders: TIdHeaderList;
      var VContinue: boolean);
  public
    property ThreadRec;

    constructor Create;
    destructor Destroy; override;
    procedure Assign(Source: THTTPDocItem); virtual;
    property Url: string read FUrl write FUrl;
    property OriginalUrl: string read FOriginalUrl write FOriginalUrl;
    property RedirectCount: integer read FRedirectCount write FRedirectCount;
    property Data: TStream read FData write FData;
    property ParentFetcher: THTTPDocFetcher read FParentFetcher write FParentFetcher;
    property FailReason: TDocItemFailReason read FFailReason write FFailReason;
    property FailString: string read FFailString write FFailString;
    property HTTPCode: integer read FHTTPCode;
    property ParserRef: TObject read FParserRef write FParserRef;
    property UnresolvedRef: TObject read FUnresolvedRef write FUnresolvedRef;
    property Method: TFetchParseMethod read FMethod write FMethod;
    property FPRef1: TObject read FFPRef1 write FFPRef1;
    property FPRef2: TObject read FFPRef2 write FFPRef2;
    property PostData: TStream read FPostData write FPostData;
    property DocType: THTMLDocType read FResultDocType write FResultDocType;
    property FetcherOpts: TFetcherOpts read FFetcherOpts write FFetcherOpts;
  end;

{$IFDEF USE_TRACKABLES}

  TCSRFToken = class(TTrackable)
{$ELSE}
  TCSRFToken = class
{$ENDIF}
  private
    FTokenString: string;
    FExpiry: TDateTime;
    FMaxAge: Int64;
    FPath: string;
  public
    procedure UpdateToken(CookieManager: TIdCookieManager);
    function OutOfDate(CurrentTime: TDateTime): boolean;

    property TokenString: string read FTokenString write FTokenString;
    property Expiry: TDateTime read FExpiry write FExpiry;
    property MaxAge: Int64 read FMaxAge write FMaxAge;
    property Path: string read FPath write FPath;
  end;

  // Abstract classes here, concrete definitions elsewhere.
{$IFDEF USE_TRACKABLES}

  TAuthProvider = class(TTrackable)
{$ELSE}
  TAuthProvider = class
{$ENDIF}
  private
  protected
  public
    procedure ConnectionAuthRequest(Conn: TIdHttpCached); virtual; abstract;
  end;

  // TODO - We'll worry about expiry and renewal in due course.
  // TODO - OAuth1 and 2 tokens not the same, need to go through
  // fields here, and see which we can review and move into
  // child classes from base.
{$IFDEF USE_TRACKABLES}

  TAuthToken = class(TTrackable)
{$ELSE}
  TAuthToken = class
{$ENDIF}
  private
    FSite: string;
    FClientSecret: string;
    FTokenServerResponse: string;
    FTokenHexlifiedToken: string;
    FTokenParseTree: TObject;
  public
    function Clone: TAuthToken; virtual;
    constructor Create; virtual;
    destructor Destroy; override;
    function SignRequest(ToSign: string): string; virtual; abstract;
    function AppendSigToURL(Url: string; var SigURL: string; SigPolicy: TRequestSigPolicy): boolean;
      virtual; abstract;

    property TokenServerResponse: string read FTokenServerResponse write FTokenServerResponse;
    property TokenHexlifiedToken: string read FTokenHexlifiedToken write FTokenHexlifiedToken;
    property ClientSecret: string read FClientSecret write FClientSecret;
    property Site: string read FSite write FSite;
    property TokenParseTree: TObject read FTokenParseTree write FTokenParseTree;
  end;

  TAuthTokenClass = class of TAuthToken;

{$IFDEF USE_TRACKABLES}

  THTTPConnectionCache = class(TTrackable)
{$ELSE}
  THTTPConnectionCache = class
{$ENDIF}
  private
    FParentFetcher: THTTPDocFetcher;
    FList: TList;
  protected
    procedure Prune;
  public
    constructor Create;
    destructor Destroy; override;
    function Get(WorkItemCtx: THTTPDocItem; Proto, Site: string; DoAuth: boolean): TIdHttpCached;
    procedure Put(Conn: TIdHttpCached; DoAuth: boolean; PruneThis: boolean);
    procedure AgeCache;
    property ParentFetcher: THTTPDocFetcher read FParentFetcher write FParentFetcher;
  end;

  TIDHttpCacheable = class(TIdHTTP)
  private
    FCachedParent: TIdHttpCached;
  public
    property CachedParent: TIdHttpCached read FCachedParent;
  end;

{$IFDEF USE_TRACKABLES}

  TIdHttpCached = class(TTrackable)
{$ELSE}
  TIdHttpCached = class
{$ENDIF}
  private
    FParentCache: THTTPConnectionCache;
    FIdHttp: TIDHttpCacheable;
    FWorkItemCtx: THTTPDocItem;
    FCSRFToken: TCSRFToken;
    FAuthToken: TAuthToken;
    FAuthSyncContext: TObject; // For Auth module use only - todo, remove this?
    // TODO - Refresh token?
    FLastUsed: TDateTime;
    FPruneImmediately: boolean;
    FProto: string;
    FSite: string;
    FInUse: boolean;
    FDoAuth: boolean;
    FAuthFailed: boolean;
    FAuthFailString: string;
    FLatestAuthRedirect: string;
  protected
    procedure AuthoriseConnection;
  public
    destructor Destroy; override;
    procedure StartUsing(WorkItemCtx: THTTPDocItem; NProto: string; NSite: string);
    procedure StopUsing;
    function UpdateCSRFTokenFromId: boolean;

    property ParentCache: THTTPConnectionCache read FParentCache;
    property CSRFToken: TCSRFToken read FCSRFToken;
    property AuthToken: TAuthToken read FAuthToken write FAuthToken;
    property IdHTTP: TIDHttpCacheable read FIdHttp;
    property LastUsed: TDateTime read FLastUsed;
    property Proto: string read FProto;
    property Site: string read FSite;
    property InUse: boolean read FInUse;
    property DoAuth: boolean read FDoAuth;
    property AuthFailed: boolean read FAuthFailed write FAuthFailed;
    property AuthFailString: string read FAuthFailString write FAuthFailString;
    property LatestAuthRedirect: string read FLatestAuthRedirect write FLatestAuthRedirect;
    property AuthSyncContext: TObject read FAuthSyncContext write FAuthSyncContext;
    // For auth module use only.
    property WorkItemCtx: THTTPDocItem read FWorkItemCtx write FWorkItemCtx;
    property PruneImmediately: boolean read FPruneImmediately write FPruneImmediately;
  end;

{$IFDEF USE_TRACKABLES}

  THTTPDocFetcher = class(TTrackable)
{$ELSE}
  THTTPDocFetcher = class
{$ENDIF}
  private
    FRetrievedDocs: TDLProxyThreadQueuePublicLock;
    FAbandonedDocs: TDLProxyThreadQueuePublicLock;
    FDeleting: boolean;
    FWorkFarm: TWorkFarm;
    FOnDocsCompleted: TNotifyEvent;
    FOnDocsAbandoned: TNotifyEvent;
    FLock: TCriticalSection;
    FConnCache: THTTPConnectionCache;
    FAuthProvider: TAuthProvider;
  protected
    procedure AcquireLock;
    procedure ReleaseLock;

    function AddItemInternal(Item: THTTPDocItem): boolean;
    function AddCompletedDocInternal(Item: THTTPDocItem): boolean;
    // returns whether to notify main thread.
    function AddAbandonedDocInternal(Item: THTTPDocItem): boolean;
    // returns whether to notify main thread.
    procedure DoDocsCompleted;
    procedure DoDocsAbandoned;
    function GetParallelFetches: integer;

    property ConnCache: THTTPConnectionCache read FConnCache;
  public
    constructor Create;
    destructor Destroy; override;
    procedure AgeCache;

    function AddUrlToRetrieve(Url: string; PostData: TStream; ParserRef, UnresolvedRef: TObject;
      Method: TFetchParseMethod; Opts: TFetcherOpts; FPRef1, FPRef2: TObject): boolean;
    function GetRetrievedDoc(): THTTPDocItem;
    function GetAbandonedDoc(): THTTPDocItem;
    procedure CancelOutstandingFetches;

    property OnDocsCompleted: TNotifyEvent read FOnDocsCompleted write FOnDocsCompleted;
    property OnDocsAbandoned: TNotifyEvent read FOnDocsAbandoned write FOnDocsAbandoned;

    property AuthProvider: TAuthProvider read FAuthProvider write FAuthProvider;
    property ParallelFetches: integer read GetParallelFetches;
  end;

const
  FetchParseMethodStrings: array [TFetchParseMethod] of string = ('fpmGet', 'fpmGetWithScripts',
    'fpmHead', 'fpmPost', 'fpmPut', 'fpmDelete', 'fpmTrace');

  S_SSL_PROTOCOL = 'https';
  S_HTTP_PROTOCOL = 'http';
  S_USER_AGENT = 'MCH Fetcher-Parser 1.0 martin_c_harvey@hotmail.com ' +
    '(Delphi XE-4, Indy 10, Open SSL 1.0.2t)';

implementation

uses AnsiStrings
{$IFOPT C+}
    , System.IoUtils
{$ENDIF}
{$IFDEF INCLUDE_AUTH}
    , OAuth2
{$ENDIF}
    , GlobalLog, IDHMACMD5, IdGlobal;

const
  S_FAIL_REDIRECT = 'Redirect from ';
  S_FAIL_BAD_URL_OR_PARAMS = 'Bad URL or parameters: need valid site and protocol.';
  S_FAIL_AUTHORIZATION_FAILED = 'Authorization failed';
  S_FAIL_UNHANDLED_EXCEPTION = 'Unhandled exception: ';
  S_FAIL_CANCELLED_BEFORE_FETCH = 'Failed, workitem cancelled while throttling';
  S_CSRF_TOKEN_STR = 'csrftoken';
  S_COOKIE_STR = 'cookie';
  S_CSRF_EXPIRY_STR = 'expires';
  S_CSRF_MAX_AGE_STR = 'Max-Age';
  S_CSRF_PATH_STR = 'Path';
  S_HDR_CONTENT_TYPE = 'content-type';
  S_REDIRECTED_TO = 'Redirected to:';
  S_RESPONSE_HDR_NAME = 'Rsp Hdr:';
  S_RESPONSE_HDR_VAL = ' Val :';
  S_AUTH_NOT_SUPPORTED_THIS_BUILD = 'Auth not supported in this build.';

{$IFDEF USE_SHARED_WORK_FARM}
var
  SharedWorkFarm: TWorkFarm;
{$ENDIF}

  { Misc }

function MakeTokenString: string;
var
  Plain: string;
  PlainBytes, EncBytes: TIdBytes;
  MD5Enc: TIdHMACMD5;
begin
  Plain := DateTimeToStr(Now);
  MD5Enc := TIdHMACMD5.Create;
  try
    UTF8StringToIdBytes(Plain, PlainBytes);
    EncBytes := MD5Enc.HashValue(PlainBytes);
    result := IdBytesToHexString(EncBytes);
  finally
    MD5Enc.Free;
  end;
end;

{ TFetcherOpts }

constructor TFetcherOpts.Create;
begin
  inherited;
  FSupplementaryHdrNames := TStringList.Create;
  FSupplementaryHdrVals := TStringList.Create;
end;

destructor TFetcherOpts.Destroy;
begin
  FSupplementaryHdrNames.Free;
  FSupplementaryHdrVals.Free;
  inherited;
end;

{ THTTPDocItem }

procedure THTTPDocItem.DoCancel;
begin
  inherited;
  FParentFetcher.AcquireLock;
  try
    if Assigned(FCachedConn) then
      FCachedConn.IdHTTP.Disconnect; // Terminate TCP connection, later put will release
  except
    on E: Exception do ; //TODO - Some logging when Indy throws its arms up?
  end;
  FParentFetcher.ReleaseLock;
  FailReason := difOther;
end;

procedure THTTPDocItem.HandleHeadersAvailable(Sender: TObject; AHeaders: TIdHeaderList;
  var VContinue: boolean);
var
  idx: integer;
  TypeStr: string;
  SepPos: integer;
begin
  // Dup header is debug.
{$IFDEF DEBUG_FETCHES}
  for idx := 0 to Pred(AHeaders.Count) do
  begin
    GLogLog(SV_INFO, S_RESPONSE_HDR_NAME + ' ' + AHeaders.Names[idx] + ' ' + S_RESPONSE_HDR_VAL +
      ' ' + AHeaders.Values[AHeaders.Names[idx]]);
  end;
{$ENDIF}
  // Discover doc type from headers.
  idx := AHeaders.IndexOfName(S_HDR_CONTENT_TYPE);
  if idx >= 0 then
  begin
    TypeStr := AHeaders.Values[S_HDR_CONTENT_TYPE];
    SepPos := Pos(';', TypeStr);
    if SepPos > 0 then
      TypeStr := TypeStr.Substring(0, Pred(SepPos));
    if Length(TypeStr) > 0 then
      FResultDocType := GetDocTypeFromContentHeader(TypeStr);
  end;
end;

procedure THTTPDocItem.Assign(Source: THTTPDocItem);
begin
  Assert(Assigned(Source));
  FUrl := Source.FUrl;
  FOriginalUrl := Source.FOriginalUrl;
  FRedirectCount := Source.FRedirectCount;
  FParentFetcher := Source.FParentFetcher;
  FParserRef := Source.FParserRef;
  FUnresolvedRef := Source.FUnresolvedRef;
  FMethod := Source.FMethod;
  FFPRef1 := Source.FPRef1;
  FFPRef2 := Source.FPRef2;
  FPostData := Source.FPostData; // To be nilled in source.
  FFetcherOpts := Source.FFetcherOpts; // To be nilled in source.
  // These fields to be handled later at execution time.
  // FData
  // FResultDocType
  // FCachedConn
  // FFailReason
  // FFailString
end;

function THTTPDocItem.GetHTTPErrCode(E: EIdHTTPProtocolException): integer;
var
  Str, Str2: string;
begin
  result := -1;
  Str := E.Message;
  Str := Str.Trim;
  Str2 := '';
  if not(Pos('HTTP', Str) = 1) then
    exit;
  Str := Str.Substring(Length('HTTP'), Length(Str) - Length('HTTP'));
  if (Length(Str) = 0) or not(Str[1] = '/') then
    exit;
  Str := Str.Substring(1, Pred(Length(Str)));
  while (Length(Str) > 0) and (((Str[1] >= '0') and (Str[1] <= '9')) or (Str[1] = '.')) do
    Str := Str.Substring(1, Pred(Length(Str)));
  Str := Str.Trim;
  while (Length(Str) > 0) and ((Str[1] >= '0') and (Str[1] <= '9')) do
  begin
    Str2 := Str2 + Str[1];
    Str := Str.Substring(1, Pred(Length(Str)));
  end;
  try
    result := StrToInt(Str2);
  except
    on EConvertError do
      result := -1;
  end;
end;

procedure THTTPDocItem.HandleIndyRedirect(Sender: TObject; var dest: string;
  var NumRedirect: integer; var Handled: boolean; var VMethod: TIdHTTPMethod);
var
  RedirItem: THTTPDocItem;
begin
  // Suspct NumRedirect has the number of redirections already done.
  if NumRedirect + RedirectCount <= FCachedConn.IdHTTP.RedirectMaximum then
  begin
{$IFDEF DEBUG_FETCHES}
    GLogLog(SV_INFO, S_REDIRECTED_TO + ' ' + dest);
{$ENDIF}
    RedirItem := THTTPDocItem.Create;
    try
      RedirItem.Assign(self);
      RedirItem.Url := MakeCanonicalURL(Url, dest);
      RedirItem.RedirectCount := NumRedirect + RedirectCount;
      FFetcherOpts := nil;
      FPostData := nil;
      ParentFetcher.AddItemInternal(RedirItem);
    except
      RedirItem.Free;
      raise;
    end;
  end; // else too many redirects, throw in bin.
  Handled := true;
  Cancel; // No useful document data in this workitem.
  FailReason := difRedirect;
  FailString := S_FAIL_REDIRECT + Url;
end;

function THTTPDocItem.DoWork: integer;
const
  DelayChunk: integer = 10; // Do delays in 10 ms chunks.
var
  Proto, Site, FullFile, Name, Ext: string;
  TmpStream: TMemoryStream;
  DelayRemaining: integer;
  SignedURL: string;
  SupplementaryHeaders, idx: integer;
  TokenString: string;
  DestroyConn: boolean;
begin
  result := 0;
  DestroyConn := false;
  ParseURL(FUrl, Proto, Site, FullFile, Name, Ext);
  if (Length(Proto) = 0) or (Length(Site) = 0) then
  begin
    Cancel;
    FailReason := difParams;
    FailString := S_FAIL_BAD_URL_OR_PARAMS;
    exit;
  end;

  // Sleep before acquiring the cached connection so that we don't
  // waste time holding TCP connections open.

  // Holding multiple connections open does not convince
  // rate limiting websites to give us any more bandwidth.
  if Assigned(FFetcherOpts) then
  begin
    if FFetcherOpts.ThrottleDelay <> 0 then
    begin
      DelayRemaining := FFetcherOpts.ThrottleDelay;
      while (DelayRemaining > 0) and not Cancelled do
      begin
        if DelayRemaining > DelayChunk then
        begin
          Sleep(DelayChunk);
          Dec(DelayRemaining, DelayChunk);
        end
        else
        begin
          Sleep(DelayRemaining);
          DelayRemaining := 0;
        end;
      end;
    end;
  end;

  // Acquires and releases lock as necessary.
  FCachedConn := FParentFetcher.ConnCache.Get(self, Proto, Site, Assigned(FFetcherOpts) and
    (FFetcherOpts.RequireAuthBeforeFetch or FFetcherOpts.DesireAuth));
  try
    try
      if Assigned(FFetcherOpts) and FFetcherOpts.RequireAuthBeforeFetch then
      begin
{$IFDEF INCLUDE_AUTH}
        if FCachedConn.AuthFailed then
        begin
          Assert(FCachedConn.AuthFailed = not Assigned(FCachedConn.AuthToken));
          raise EAuthException.Create(FCachedConn.AuthFailString);
        end;
{$ELSE}
        raise Exception.Create(S_AUTH_NOT_SUPPORTED_THIS_BUILD);
{$ENDIF}
      end;
      FCachedConn.UpdateCSRFTokenFromId;
      // TODO - Bit of a grey area on what happens here if we've
      // already asked for auth on a certain site, and got our
      // endpoint / io handler etc set up, and we want to connect
      // to something that's not HTTP.
      FCachedConn.IdHTTP.HandleRedirects := false;
      FCachedConn.IdHTTP.OnRedirect := self.HandleIndyRedirect;
      FCachedConn.IdHTTP.OnHeadersAvailable := self.HandleHeadersAvailable;
      if Assigned(FFetcherOpts) and (Length(FFetcherOpts.CustomUserAgent) > 0) then
        FCachedConn.IdHTTP.Request.UserAgent := FFetcherOpts.CustomUserAgent
      else
        FCachedConn.IdHTTP.Request.UserAgent := S_USER_AGENT;

      if (CompareText(Proto, S_SSL_PROTOCOL) = 0) and not FCachedConn.IdHTTP.ManagedIOHandler then
      begin
        FCachedConn.IdHTTP.CreateIoHandler(TIdIoHandlerClass(TIdSSLIOHandlerSocketOpenSSL));
        with FCachedConn.IdHTTP.IOHandler as TIdSSLIOHandlerSocketOpenSSL do
        begin
          SSLOptions.Method := sslvTLSv1_2;
          SSLOptions.SSLVersions := [sslvTLSv1, sslvTLSv1_1, sslvTLSv1_2];
        end;
      end;

      if Assigned(FFetcherOpts) then
      begin
        // Tokens sent by default over HTTP, via collab with cookie collection
        // but perhaps not x-csrf.
        if Assigned(FCachedConn.FCSRFToken) and FFetcherOpts.SendCSRFToken then
        begin
          FCachedConn.IdHTTP.Request.CustomHeaders.Values['x-csrftoken'] :=
            FCachedConn.FCSRFToken.FTokenString;
        end;
        if FFetcherOpts.SupplementaryHeaderNames.Count > FFetcherOpts.SupplementaryHeaderVals.Count
        then
          SupplementaryHeaders := FFetcherOpts.SupplementaryHeaderVals.Count
        else
          SupplementaryHeaders := FFetcherOpts.SupplementaryHeaderNames.Count;
        for idx := 0 to Pred(SupplementaryHeaders) do
          FCachedConn.IdHTTP.Request.CustomHeaders.Values[FFetcherOpts.SupplementaryHeaderNames[idx]
            ] := FFetcherOpts.SupplementaryHeaderVals[idx];
        if Length(FFetcherOpts.RequestContentType) > 0 then
          FCachedConn.IdHTTP.Request.ContentType := FFetcherOpts.RequestContentType;
        if Length(FFetcherOpts.RequestCustomOrigin) > 0 then
          FCachedConn.IdHTTP.Request.CustomHeaders.Values['origin'] :=
            FFetcherOpts.RequestCustomOrigin;
        if Length(FFetcherOpts.Referer) > 0 then
          FCachedConn.IdHTTP.Request.Referer := FFetcherOpts.Referer;
        if FFetcherOpts.OAuth2SignRequest <> signNone then
        begin
          if Assigned(FCachedConn.FAuthToken) and FCachedConn.FAuthToken.AppendSigToURL(FUrl,
            SignedURL, FFetcherOpts.OAuth2SignRequest) then
            FUrl := SignedURL;
        end;
      end;

{$IFDEF DEBUG_FETCHES}
      GLogLog(SV_INFO, '');
      GLogLog(SV_INFO, FetchParseMethodStrings[FMethod] + ': ' + FUrl);
      GLogLog(SV_INFO, '');
      GLogLogStream(SV_INFO, FPostData);
{$ENDIF}
      if not Cancelled then
      begin
        if (FMethod = fpmGet) or (FMethod = fpmGetWithScripts) then
          FCachedConn.IdHTTP.Get(FUrl, FData)
        else if (FMethod = fpmPost) then
          FCachedConn.IdHTTP.Post(FUrl, FPostData, FData)
        else if (FMethod = fpmPut) then
          FCachedConn.IdHTTP.Put(FUrl, FPostData, FData)
        else if (FMethod = fpmHead) then
          FCachedConn.IdHTTP.Head(FUrl)
        else if (FMethod = fpmDelete) then
          FCachedConn.IdHTTP.Delete(FUrl)
        else if (FMethod = fpmTrace) then
          FCachedConn.IdHTTP.Trace(FUrl, FData);
      end
      else
      begin
{$IFDEF DEBUG_FETCHES}
        GLogLog(SV_INFO, 'CANCELLED');
{$ENDIF}
        FailReason := difOther;
        FailString := S_FAIL_CANCELLED_BEFORE_FETCH;
      end;

      // If previous DocType detection has not worked.
      if FResultDocType = tdtUnknown then
        FResultDocType := GetDocTypeFromURL(FUrl);

      FCachedConn.UpdateCSRFTokenFromId;

{$IFDEF DEBUG_FETCHES}
      GLogLog(SV_INFO, 'OK');
      GLogLog(SV_INFO, '');
{$ENDIF}
    except
      on E: Exception do
      begin
        DestroyConn := true;
        Cancel;
        FailReason := difException;
        if E is EIdHTTPProtocolException then
        begin
          FHTTPCode := GetHTTPErrCode(E as EIdHTTPProtocolException);
          FailString := E.Message;
          case FHTTPCode of
            429, 404: DestroyConn := false;
          end;
        end
{$IFDEF INCLUDE_AUTH}
        else if E is EAuthException then
        begin
          FailReason := difAuthenticate;
          FailString := S_FAIL_AUTHORIZATION_FAILED + E.Message;
        end
{$ENDIF}
        else
          FailString := S_FAIL_UNHANDLED_EXCEPTION + E.Message;
{$IFDEF DEBUG_FETCHES}
        GLogLog(SV_INFO, 'FAILED');
        GLogLog(SV_INFO, FailString);
{$ENDIF}
      end;
    end;
  finally
    FParentFetcher.ConnCache.Put(FCachedConn, Assigned(FFetcherOpts) and (FFetcherOpts.DesireAuth),
      DestroyConn);
    FCachedConn := nil;
  end;
end;

procedure THTTPDocItem.DoNormalCompletion;
begin
  Assert(ThreadRec.CurItem = self);
  if FParentFetcher.AddCompletedDocInternal(self) then
    ThreadRec.Thread.Queue(FParentFetcher.DoDocsCompleted);
end;

procedure THTTPDocItem.DoCancelledCompletion;
begin
  Assert(ThreadRec.CurItem = self);
  if FParentFetcher.AddAbandonedDocInternal(self) then
    ThreadRec.Thread.Queue(FParentFetcher.DoDocsAbandoned);
end;

constructor THTTPDocItem.Create;
begin
  inherited;
  FData := TTrackedMemoryStream.Create;
end;

destructor THTTPDocItem.Destroy;
begin
  FData.Free;
  FPostData.Free;
  FFetcherOpts.Free;
  inherited;
end;

{ TCSRFToken }

function TCSRFToken.OutOfDate(CurrentTime: TDateTime): boolean;
begin
  result := FExpiry < CurrentTime;
end;

procedure TCSRFToken.UpdateToken(CookieManager: TIdCookieManager);
var
  Coll: TIdCookies;
  i: integer;
begin
  Coll := CookieManager.CookieCollection;
  Coll.LockCookieList(TIdCookieAccess.caRead);
  try
    i := Coll.GetCookieIndex(S_CSRF_TOKEN_STR);
    if i >= 0 then
    begin
      FTokenString := Coll.Cookies[i].Value;
      FExpiry := Coll.Cookies[i].Expires;
      FMaxAge := Coll.Cookies[i].MaxAge;
      FPath := Coll.Cookies[i].Path;
    end;
  finally
    Coll.UnlockCookieList(TIdCookieAccess.caRead);
  end;
end;

{ TAuthToken }

function TAuthToken.Clone: TAuthToken;
begin
  result := TAuthTokenClass(ClassType).Create;
  result.FSite := FSite;
  result.FClientSecret := FClientSecret;
  result.FTokenServerResponse := FTokenServerResponse;
  // Overrides should tie up hexlified token and parse tree.
end;

constructor TAuthToken.Create;
begin
  inherited;
end;

destructor TAuthToken.Destroy;
begin
  FTokenParseTree.Free;
  inherited;
end;

{ TIdHttpCached }

destructor TIdHttpCached.Destroy;
begin
  FIdHttp.Free;
  FCSRFToken.Free;
  FAuthToken.Free;
  inherited;
end;

procedure TIdHttpCached.AuthoriseConnection;
var
  FPF: THTTPDocFetcher;
begin
  if (not Assigned(FAuthToken)) and (not FAuthFailed) and FDoAuth then
  begin
    FPF := FParentCache.FParentFetcher;
    if Assigned(FPF.AuthProvider) then
      FPF.AuthProvider.ConnectionAuthRequest(self)
    else
      FAuthFailed := true;
  end;
end;

procedure TIdHttpCached.StartUsing(WorkItemCtx: THTTPDocItem; NProto: string; NSite: string);
begin
  Assert(not FInUse);
  if Length(FProto) = 0 then
    FProto := NProto
  else
    Assert(FProto = NProto);
  if Length(FSite) = 0 then
    FSite := NSite
  else
    Assert(FSite = NSite);
  FInUse := true;
  FLastUsed := Now;
  FWorkItemCtx := WorkItemCtx;
  if not Assigned(FIdHttp) then
  begin
    FIdHttp := TIDHttpCacheable.Create;
    FIdHttp.FCachedParent := self;
  end;
end;

procedure TIdHttpCached.StopUsing;
begin
  Assert(FInUse);
  WorkItemCtx := nil;
  FInUse := false;
  FLastUsed := Now;
end;

function TIdHttpCached.UpdateCSRFTokenFromId: boolean;
begin
  result := false;
  if Assigned(FIdHttp.CookieManager) then
  begin
    FIdHttp.CookieManager.CookieCollection.LockCookieList(TIdCookieAccess.caRead);
    try
      if (FIdHttp.CookieManager.CookieCollection.GetCookieIndex(S_CSRF_TOKEN_STR) >= 0) then
      begin
        if not Assigned(FCSRFToken) then
          FCSRFToken := TCSRFToken.Create;
        FCSRFToken.UpdateToken(FIdHttp.CookieManager);
        result := true;
      end;
    finally
      FIdHttp.CookieManager.CookieCollection.UnlockCookieList(TIdCookieAccess.caRead);
    end;
  end;
end;

{ THTTPConnectionCache }

constructor THTTPConnectionCache.Create;
begin
  inherited;
  FList := TList.Create;
end;

destructor THTTPConnectionCache.Destroy;
var
  i: integer;
begin
  for i := 0 to Pred(FList.Count) do
    TIdHttpCached(FList.Items[i]).Free;
  FList.Free;
  inherited;
end;

function THTTPConnectionCache.Get(WorkItemCtx: THTTPDocItem; Proto, Site: string; DoAuth: boolean)
  : TIdHttpCached;
var
  i: integer;
  CachedItem: TIdHttpCached;
begin
  result := nil;
  ParentFetcher.AcquireLock;
  try
    if not((Length(Proto) > 0) and (Length(Site) > 0)) then
    begin
      Assert(false);
      exit;
    end;
    // Slow search here, but I don't expect that many connections.
    for i := 0 to Pred(FList.Count) do
    begin
      CachedItem := TIdHttpCached(FList.Items[i]);
      Assert(Assigned(CachedItem));
      if (not CachedItem.InUse) and (CachedItem.Proto = Proto) and (CachedItem.Site = Site) then
      begin
        result := CachedItem;
        break;
      end;
    end;
    if not Assigned(result) then
    begin
      // Couldn't find existing.
      result := TIdHttpCached.Create;
      FList.Add(result);
      result.FParentCache := self;
    end;
    result.StartUsing(WorkItemCtx, Proto, Site);
    result.FDoAuth := DoAuth;
    Prune;
  finally
    ParentFetcher.ReleaseLock;
  end;
  result.AuthoriseConnection;
end;

procedure THTTPConnectionCache.Put(Conn: TIdHttpCached; DoAuth: boolean; PruneThis: boolean);
var
  i: integer;
begin
  Conn.FDoAuth := DoAuth;
  Conn.AuthoriseConnection;
  ParentFetcher.AcquireLock;
  try
    i := FList.IndexOf(Conn);
    if not((i >= 0) and (i <= FList.Count)) then
    begin
      Assert(false);
      exit;
    end;
    Conn.PruneImmediately := PruneThis;
    Conn.StopUsing;
    Prune;
  finally
    ParentFetcher.ReleaseLock;
  end;
end;

procedure THTTPConnectionCache.AgeCache;
begin
  ParentFetcher.AcquireLock;
  try
    Prune;
  finally
    ParentFetcher.ReleaseLock;
  end;
end;

const
  ThirtySecsOld = (30.0 * MSecsPerSec) / MSecsPerDay;

procedure THTTPConnectionCache.Prune;
var
  i: integer;
  CachedItem: TIdHttpCached;
  CT: TDateTime;
  Age: TDateTime;
  DoPrune: boolean;
begin
  i := 0;
  CT := Now;
  while (i < FList.Count) do
  begin
    CachedItem := TIdHttpCached(FList.Items[i]);
    Age := CT - CachedItem.LastUsed;
    if (not CachedItem.InUse) or CachedItem.PruneImmediately then
    begin
      DoPrune := (Age > ThirtySecsOld) or CachedItem.PruneImmediately;
      if Assigned(CachedItem.CSRFToken) then
        DoPrune := DoPrune or CachedItem.CSRFToken.OutOfDate(CT);
      if DoPrune then
      begin
        FList.Delete(i);
        CachedItem.Free;
      end
      else
        Inc(i);
    end
    else
      Inc(i);
  end;
end;

{ THTTPDocFetcher }

procedure THTTPDocFetcher.AcquireLock;
begin
  FLock.Acquire;
  // Queue locks should point to the same lock,
  // but need to lock them anyway to set debug flags correctly.
  FRetrievedDocs.AcquireLock;
  FAbandonedDocs.AcquireLock;
end;

procedure THTTPDocFetcher.ReleaseLock;
begin
  // Queue locks should point to the same lock,
  // but need to lock them anyway to set debug flags correctly.
  FAbandonedDocs.ReleaseLock;
  FRetrievedDocs.ReleaseLock;

  FLock.Release;
end;

function THTTPDocFetcher.AddItemInternal(Item: THTTPDocItem): boolean;
begin
  AcquireLock;
  try
    result := FWorkFarm.AddWorkItem(Item);
  finally
    ReleaseLock;
  end;
end;

function THTTPDocFetcher.AddUrlToRetrieve(Url: string; PostData: TStream;
  ParserRef, UnresolvedRef: TObject; Method: TFetchParseMethod; Opts: TFetcherOpts;
  FPRef1, FPRef2: TObject): boolean;
var
  Item: THTTPDocItem;
begin
  if ((Method = fpmPost) or (Method = fpmPut)) and not Assigned(PostData) then
  begin
    result := false;
    exit;
  end;

  Item := THTTPDocItem.Create;
  Item.Url := Url;
  Item.OriginalUrl := Url;
  Item.ParentFetcher := self;
  Item.RedirectCount := 0;
  Item.ParserRef := ParserRef;
  Item.UnresolvedRef := UnresolvedRef;
  Item.Method := Method;
  Item.FPRef1 := FPRef1;
  Item.FPRef2 := FPRef2;
  Item.FPostData := PostData;
  Item.FetcherOpts := Opts;
  result := AddItemInternal(Item);
end;

function THTTPDocFetcher.GetRetrievedDoc(): THTTPDocItem;
begin
  if not FDeleting then
  begin
    AcquireLock;
    try
      result := FRetrievedDocs.RemoveTailObj as THTTPDocItem;
    finally
      ReleaseLock;
    end;
  end
  else
    result := nil;
end;

function THTTPDocFetcher.GetAbandonedDoc(): THTTPDocItem;
begin
  if not FDeleting then
  begin
    AcquireLock;
    result := FAbandonedDocs.RemoveTailObj as THTTPDocItem;
    ReleaseLock;
  end
  else
    result := nil;
end;

procedure THTTPDocFetcher.CancelOutstandingFetches;
begin
  if not FDeleting then
  begin
    // Can't do this under any locks, as required for integrity of queue
    // datastructures.

{$IFDEF USE_SHARED_WORK_FARM}
    //TODO - If work farm shared, destruction of one fetcher,
    //will cancel others fetches.
{$ENDIF}
    FWorkFarm.FlushAndWait(true);
    FWorkFarm.ResetFinishedFlush;
  end;
end;

constructor THTTPDocFetcher.Create;
begin
  inherited;
{$IFDEF USE_SHARED_WORK_FARM}
  FWorkFarm := SharedWorkFarm;
{$ELSE}
  FWorkFarm := TWorkFarm.Create;
{$ENDIF}
  FLock := FWorkFarm.IntLock;
  FRetrievedDocs := TDLProxyThreadQueuePublicLock.Create;
  FRetrievedDocs.Lock := self.FLock;
  FAbandonedDocs := TDLProxyThreadQueuePublicLock.Create;
  FAbandonedDocs.Lock := self.FLock;
  FConnCache := THTTPConnectionCache.Create;
  FConnCache.ParentFetcher := self;
end;

procedure THTTPDocFetcher.AgeCache;
begin
  if Assigned(FConnCache) then
    FConnCache.AgeCache;
end;

destructor THTTPDocFetcher.Destroy;
var
  Obj: THTTPDocItem;
begin
  FDeleting := true;
  FWorkFarm.FlushAndWait(true);
  AcquireLock;
  try
    repeat
      Obj := FRetrievedDocs.RemoveTailObj as THTTPDocItem;
      if Assigned(Obj) then
        Obj.DecFreeRefCount;
    until not Assigned(Obj);

    repeat
      Obj := FAbandonedDocs.RemoveTailObj as THTTPDocItem;
      if Assigned(Obj) then
        Obj.DecFreeRefCount;
    until not Assigned(Obj);
  finally
    ReleaseLock;
  end;

  FLock := nil;
{$IFDEF USE_SHARED_WORK_FARM}
  FWorkFarm := nil;
{$ELSE}
  FWorkFarm.Free;
{$ENDIF}
  FRetrievedDocs.Free;
  FAbandonedDocs.Free;
  FConnCache.Free;
  inherited;
end;

function THTTPDocFetcher.AddCompletedDocInternal(Item: THTTPDocItem): boolean;
begin
  AcquireLock;
  try
    result := FRetrievedDocs.Count = 0;
    FRetrievedDocs.AddHeadObj(Item);
  finally
    ReleaseLock;
  end;
end;

function THTTPDocFetcher.AddAbandonedDocInternal(Item: THTTPDocItem): boolean;
begin
  AcquireLock;
  try
    result := FAbandonedDocs.Count = 0;
    FAbandonedDocs.AddHeadObj(Item);
  finally
    ReleaseLock;
  end;
end;

procedure THTTPDocFetcher.DoDocsCompleted;
begin
  if Assigned(FOnDocsCompleted) then
    FOnDocsCompleted(self);
end;

procedure THTTPDocFetcher.DoDocsAbandoned;
begin
  if Assigned(FOnDocsAbandoned) then
    FOnDocsAbandoned(self);
end;

function THTTPDocFetcher.GetParallelFetches: integer;
begin
  result := FWorkFarm.ThreadCount;
end;

initialization
TIdSSLIOHandlerSocketOpenSSL.SetDefaultClass;
TIdIOHandlerStack.SetDefaultClass;
{$IFDEF USE_SHARED_WORK_FARM}
SharedWorkFarm := TWorkFarm.Create;
{$ENDIF}
finalization
{$IFDEF USE_SHARED_WORK_FARM}
SharedWorkFarm.Free;
{$ENDIF}
end.
