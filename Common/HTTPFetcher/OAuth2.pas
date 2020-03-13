unit OAuth2;
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

uses HTTPDocFetcher, SysUtils, IdHTTP, IdHeaderList, Classes, Trackables,
     DLList, SyncObjs,
     IdHMACSHA1, IdGlobal, WorkItems, CommonPool;

//TODO - May need to support three legged oauth1 request as well.
//TODO - Deal with multiple simultaneous accesses for the same site
//when user authenticating: need to block subsequent requests.

type
  TOAuthVersion = (oavVersion1, oavVersion2);

  //Generally this unit does not throw these, but allows auth-specific abort mechanism.
  EAuthException = class(Exception);

  //Auth base query URL should not include initial protocol, just site and
  //location up to and including the ?

  TAuthRequestExistingAccessToken =
    function (Sender: TObject;
              Site: string): TAuthToken of object;

  TAuthCacheNewAccessToken =  procedure (Sender: TObject;
                            Token: TAuthToken) of object;

  TAuthRequestClientIdAuthEndpoint =
    function (Sender: TObject;
              Site: string;
              out AuthBaseQueryURL: string;
              out AuthBaseTokenURL: string;
              out ClientId: string;
              out ClientSecret: string;
              out Scope: string;
              out RedirectURI: string;
              out ProtocolVersion: TOAuthVersion;
              out UserAgentString:string): boolean of object;

    TAuthRequestBrowserSignIn =
      function(Sender: TObject;
                Site:string;
                InitialBrowserURL: string;
                out RedirectedBrowserURL: string): boolean of object;

  TChainedThunk = class(TTrackable)
  public
    ThunkDoneEvent: TEvent;
    ThunkDone: boolean;
    ThunkLink: TDLEntry;
    constructor Create;
    destructor Destroy; override;
  end;

  TAuthThunkClientIdAndEndpoint = class(TChainedThunk)
  public
    //In.
    Site: string;
    //Out.
    AuthBaseQueryURL: string;
    AuthBaseTokenURL: string;
    ClientId: string;
    ClientSecret: string;
    Scope: string;
    RedirectURI: string;
    ProtocolVersion: TOAuthVersion;
    UserAgentString:string;
    Result: boolean;
  end;

  TAuthThunkBrowserSignIn = class(TChainedThunk)
  public
    //In
    Site: string;
    InitialBrowserURL: string;
    //Out
    RedirectedBrowserURL: string;
    Result: boolean;
  end;

  TAuthThunkRequestAccessToken = class(TChainedThunk)
  public
    //In
    Site: string;
    //Out
    Result: TAuthToken;
  end;

  TAuthThunkCacheAccessToken = class(TChainedThunk)
  public
    //In
    Token: TAuthToken;
  end;

  TOAuth1Token = class(TAuthToken)
  end;

(* Sample token data from instagram
'{
   "access_token": "570502641.e8dac73.6d94c51033fb4b779125c2b38d8a7e30",
   "user":
   {
     "id":
     "570502641",
     "username": "martincharvey",
     "profile_picture":
     "https://instagram.flhr3-2.fna.fbcdn.net/t51.2885-19/s150x150/14533588_163185660799513_8750595054956969984_a.jpg",
     "full_name": "Martin Harvey",
     "bio": "",
     "website": "http://www.martincharvey.net/",
     "is_business": false
   }
}'
*)

  TOAuth2Token = class(TAuthToken)
  private
    FHMac: TIdHMACSHA256;
  protected
    function SetIdHMACFromClientSecret(): boolean;
    function ParseServerResponse: boolean;
  public
    function AppendSigToURL(URL: string; var SigURL:string; SigPolicy: TRequestSigPolicy):boolean; override;

    function Clone:TAuthToken; override;
    constructor Create; override;
    destructor Destroy; override;
    function SignRequest(ToSign: string): string; override;
  end;

  TOAuth2Provider = class(TAuthProvider)
  private
    FAuthRequestClientIdAndEndpoint: TAuthRequestClientIdAuthEndpoint;
    FAuthRequestBrowserSignIn: TAuthRequestBrowserSignIn;
    FAuthRequestExistingAccessToken: TAuthRequestExistingAccessToken;
    FAuthCacheNewAccessToken: TAuthCacheNewAccessToken;

    FChainedThunksOutstanding: TDLEntry;
    FChainedThunkLock: TCriticalSection;
  protected
    procedure ProcessThunks;

    procedure DoMainThreadRequestClientIdAndEndpoint(Thunk: TAuthThunkClientIdAndEndpoint);
    procedure DoMainThreadReuqestBrowserSignIn(Thunk: TAuthThunkBrowserSignIn);
    procedure DoMainThreadRequestExistingAcessToken(Thunk: TAuthThunkRequestAccessToken);
    procedure DoMainThreadCacheNewAccessToken(Thunk: TAuthThunkCacheAccessToken);

    function DoRequestClientIdAndEndpoint(Conn: TIDHTTPCached;
              Site: string;
              out AuthBaseQueryURL: string;
              out AuthBaseTokenURL: string;
              out ClientId: string;
              out ClientSecret: string;
              out Scope: string;
              out RedirectURI: string;
              out ProtocolVersion: TOAuthVersion;
              out UserAgentString:string): boolean;

    function DoRequestBrowserSignIn(Conn: TIDHTTPCached;
                                    Site:string;
                                    InitialBrowserURL: string;
                                    out RedirectedBrowserURL: string): boolean;

    //Produces token.
    function DoRequestExistingAccessToken(Conn: TIDHTTPCached;
                                          Site: string): TAuthToken;

    //Consumes token.
    procedure DoCacheNewAccessToken(Conn: TIDHTTPCached;
                                    Token: TAuthToken);

    procedure HandleAuthHeaders(Sender: TObject; AHeaders: TIdHeaderList; var VContinue: Boolean);
    procedure HandleAuthRedirect(Sender: TObject; var dest: string; var NumRedirect: Integer; var Handled: boolean; var VMethod: TIdHTTPMethod);

    procedure OAuth1ProtocolFlow(Conn: TIDHTTPCached;
                                 AuthBaseQueryUrl:string;
                                 AuthBaseTokenURL: string;
                                 ClientId:string;
                                 ClientSecret: string;
                                 Scope:string;
                                 RedirectURI:string;
                                 UserAgentString:string);

    //Pass in the query URL that you've been redirected to,
    //get out the URL with the code - hopefully.
    function OAuth2GetCodeFromURL(URL: string; out Leg1AuthCode:string): boolean;
    procedure OAuth2ProtocolFlow(Conn: TIdHttpCached;
                                 AuthBaseQueryUrl:string;
                                 AuthBaseTokenUrl:string;
                                 ClientId:string;
                                 ClientSecret:string;
                                 Scope:string;
                                 RedirectURI:string;
                                 UserAgentString:string);
  public
    constructor Create;
    destructor Destroy; override;

    procedure ConnectionAuthRequest(Conn: TIdHttpCached); override;

    property OnRequestClientIdAndEndpoint: TAuthRequestClientIdAuthEndpoint
      read FAuthRequestClientIdAndEndpoint write FAuthRequestClientIdAndEndpoint;
    property OnRequestBrowserSignIn: TAuthRequestBrowserSignIn
      read FAuthRequestBrowserSignIn write FAuthRequestBrowserSignIn;
    property OnAuthRequestExistingAccessToken: TAuthRequestExistingAccessToken
      read FAuthRequestExistingAccessToken write FAuthRequestExistingAccessToken;
    property OnAuthCacheNewAccessToken: TAuthCacheNewAccessToken
      read FAuthCacheNewAccessToken write FAuthCacheNewAccessToken;
  end;

var
  GAuthProvider: TOAuth2Provider;
  OAuth2PassedSelfTest: boolean;
  OAuth1PassedSelfTest: boolean;
  DbgGoodMask: integer;
  DbgFound: boolean;

implementation

uses
  HTMLEscapeHelper, IdIoHandler, IdSSLOpenSSL, HTMLGrammar, CocoBase,
  HTMLNodes, CommonNodes, JSONGrammar, JSONNodes, KKUtils, IdHMAC,
  GlobalLog;

const
  S_FAIL_BAD_PROTO = 'Not going to auth this connection, not https';
  S_FAIL_SITE_NOT_RECOGNISED = 'Auth failed: Could not find auth provider, client ID, or redirect URI.';
  S_FAIL_BAD_PROTOCOL_VERSION = 'Auth failed: Unknown protocol version.';
  S_FAIL_NOT_YET_IMPLEMENTED = 'Auth failed: Protocol version not yet implemented';
  S_FAIL_TOO_MANY_REDIRECTS = 'Auth failed: too many redirects.';
  S_FAIL_NO_LEG1_CODE = 'Auth failed: no code found at end of first leg.';
  S_FAIL_AUTH2_LOGIN_SCREEN = 'Auth failed: failed to parse, or execute login screen.';
  S_FAIL_SECRET_KEY_BAD_DATA = 'Client secret not a valid byte string.';

  //OAuth 2 constants.
  //Leg 1 parameter constants.
  S_RESPONSE_TYPE = 'response_type';
  S_CODE= 'code';
  S_CLIENT_ID = 'client_id';
  S_CLIENT_SECRET = 'client_secret';
  S_SCOPE = 'scope';
  S_REDIRECT_URI = 'redirect_uri';
  S_STATE = 'state';
  S_AUTH_REDIRECTED_TO = 'Redirected to: ';
  S_AUTH_RESPONSE_HDR_NAME = 'Auth Res Hdr:';
  S_AUTH_RESPONSE_HDR_VAL = 'Val:';
  S_LEG1_CODE = 'Auth Leg1 code: ';
  S_ACTION = 'action';
  S_INPUT = 'input';
  S_NAME = 'name';
  S_VALUE = 'value';
  S_GRANT_TYPE = 'grant_type';
  S_AUTHORIZATION_CODE= 'authorization_code';
  S_SIG = 'sig';
  S_ACCESS_TOKEN = 'access_token';

  //OAuth1 constants.
  S_OAUTH_REALM = 'realm';
  S_OAUTH_CONSUMER_KEY = 'oauth_consumer_key';
  S_OAUTH_SIGNATURE_METHOD = 'oauth_signature_method';
  S_OAUTH_HMAC_SHA1 = 'HMAC-SHA1';
  S_OAUTH_TIMESTAMP = 'oauth_timestamp';
  S_OAUTH_NONCE = 'oauth_nonce';
  S_OAUTH_CALLBACK = 'oauth_callback';
  S_OAUTH_VERSION = 'oauth_version';
  S_OAUTH_VERSION_1_0 = '1.0';
  S_OAUTH_SIGNATURE = 'oauth_signature';
  S_OAUTH_TOKEN = 'oauth_token';
  S_OAUTH1_HEADER = 'Authorization';
  S_OAUTH1_HEADER_START = 'OAuth ';

function SetOAuth1MACFromClientAndTokenSecrets(Mac: TIdHMACSha1;ClientSecret: string; TokenSecret:string): boolean;
var
  KeyString: string;
  Key: TIdBytes;

begin
  KeyString := ClientSecret + '&' + TokenSecret;
  result := UTF8StringToIdBytes(KeyString, Key);
  if result then
    Mac.Key := Key;
end;

{ OAuth1 Signature building methods - need these because
  we need to do a whole bunch of signing before we have
  a token object. }


function OAuth1MacSignRequest(Mac: TIdHMACSha1; ToSign: string): string;
var
  TokenBytes, HashBytes: TIdBytes;
  res: boolean;
begin
  res := UTF8StringToIdBytes(ToSign, TokenBytes);
  Assert(res);
  HashBytes := Mac.HashValue(TokenBytes);
  result := IdBytesToBase64String(HashBytes);
end;

{ TOAuth1Token }

{ TOAuth2Token }

function TOAuth2Token.SetIdHMACFromClientSecret(): boolean;
var
  Key: TIdBytes;
begin
  result := UTF8StringToIdBytes(ClientSecret, Key);
  if not result then
    exit;
  FHMac.Key := Key;
  result := true;
end;

function TOAuth2Token.ParseServerResponse: boolean;
var
  Grammar: TJSONGrammar;
  AnsiSrvResponse: AnsiString;
  ResponseStream: TMemoryStream;
  ParseResult: TCommonNode;
  JN, JV: TObject;
begin
  //At the moment, just expect a JSON response.
  Grammar := TJSONGrammar.Create(nil);
  ResponseStream := TMemoryStream.Create;
  try
    AnsiSrvResponse := TokenServerResponse;
    ResponseStream.WriteBuffer(AnsiSrvResponse[1], Length(AnsiSrvResponse));
    Grammar.SourceStream := ResponseStream;
    Grammar.Execute;
    ParseResult := Grammar.ParseResult as TCommonNode;
  finally
    (Grammar.ParseTracker as TTracker).ResetWithoutFree;
    Grammar.SourceStream := nil;
    Grammar.Free;
    ResponseStream.Free;
  end;
  result := false;

  if not Assigned(ParseResult) then
    exit;
  try
    ParseResult.IterateNodes(ExecuteAlways, FixNodeStrings, RecurseAlways, ParseResult, nil, nil);
    if not (ParseResult is TJSONDocument) then
      exit;
    JN := ParseResult.ContainedListHead.FLink.Owner;
    if not (Assigned(JN) and (JN is TJSONContainer) and ((JN as TJSONContainer).ContainerType = jctObject)) then
      exit;
    JN := (JN as TCommonNode).ContainedListHead.FLink.Owner;
    while Assigned(JN) and (JN is TJSONMember) do
    begin
      if (JN as TJSONMember).Name = S_ACCESS_TOKEN then
      begin
        JV := (JN as TCommonNode).ContainedListHead.FLink.Owner;
        if Assigned(JV) and (JV is TJSONSimpleValue) and ((JV as TJSONSimpleValue).ValType = svtString) then
        begin
          TokenHexlifiedToken := (JV as TJSONSimpleValue).StrData;
          TokenParseTree := ParseResult;
          ParseResult := nil;
          JN := nil;
          result := true;
        end;
      end;
      if Assigned(JN) then
        JN := (JN as TCommonNode).SiblingListEntry.Owner as TJSONNode;
    end;
  finally
    ParseResult.Free;
  end;
end;

function TOAuth2Token.Clone:TAuthToken;
var
  CloneDataGood: boolean;
begin
  result := inherited;
  CloneDataGood := (result as TOAuth2Token).SetIdHMACFromClientSecret;
  Assert(CloneDataGood);
  CloneDataGood := (result as TOAuth2Token).ParseServerResponse;
  Assert(CloneDataGood);
end;

constructor TOAuth2Token.Create;
begin
  inherited;
  FHMac := TIdHMACSHA256.Create;
end;

destructor TOAuth2Token.Destroy;
begin
  FHMac.Free;
  inherited;
end;

function TOAuth2Token.AppendSigToURL(URL: string; var SigURL:string;
                                     SigPolicy: TRequestSigPolicy):boolean;
var
  Proto, Site, FullFile, Name, Ext: string;
  HasParams: boolean;
  Params, Values: TStringList;
  Sig: string;
  Idx: integer;
begin
  if SigPolicy = signNone then
  begin
    SigURL := URL;
    result := true;
    exit;
  end;

  Params := TStringList.Create;
  Values := TStringList.Create;
  try
    ParseURLEx(URL, Proto, Site, FullFile, Name, Ext, HasParams, Params, Values);
    //First things first - if access token not already there, then add it.
    if not (HasParams and Params.Find(S_ACCESS_TOKEN, Idx)) then
    begin
      HasParams := true;
      Params.Add(S_ACCESS_TOKEN);
      Values.Add(TokenHexlifiedToken);
      URL := BuildURL(Proto, Site, FullFile, HasParams, Params, Values);
      if SigPolicy = signAddToken then
      begin
        SigURL := URL;
        result := true;
        exit;
      end;
    end;

    if not (OAuth2PassedSelfTest and (Length(TokenHexlifiedToken) > 0)) then
    begin
      result := false;
      exit;
    end;
    Assert(SigPolicy = signAddAndSign);
    Sig := SignRequest(URL);
    HasParams := true;
    Params.Add(S_SIG);
    Values.Add(Sig);
    SigURL := BuildURL(Proto, Site, FullFile, HasParams, Params, Values);
    result := true;
  finally
    Params.Free;
    Values.Free;
  end;
end;

//Note that this does not add the token on to the request
//or any such like thing.
function TOAuth2Token.SignRequest(ToSign: string): string;
var
  Proto, Site, FullFile, Name, Ext: string;
  HasParams: boolean;
  Params, Values: TStringList;
  TokenString: string;
  TokenBytes: TIdBytes;
  HashBytes: TIdBytes;
  Res: boolean;
begin
  if not OAuth2PassedSelfTest then
  begin
    result := '';
    exit;
  end;

  Params := TStringList.Create;
  Values := TStringList.Create;
  try
    ParseURLEx(ToSign,
               Proto,
               Site,
               FullFile,
               Name,
               Ext,
               HasParams,
               Params,
               Values);
    TokenString := OAuth2HMACTokenEncode(FullFile, Params, Values);
    Res := UTF8StringToIdBytes(TokenString, TokenBytes);
    Assert(Res);
    HashBytes := FHMac.HashValue(TokenBytes);
    result := IdBytesToHexString(HashBytes);
  finally
    Params.Free;
    Values.Free;
  end;
end;

{ TChainedThunk }

constructor TChainedThunk.Create;
begin
  inherited;
  ThunkDoneEvent := TEvent.Create(nil, true, false, '');
  DLItemInitObj(self, @ThunkLink);
end;

destructor TChainedThunk.Destroy;
begin
  ThunkDoneEvent.Free;
  inherited;
end;

{ TOAuth2Provider }

procedure TOAuth2Provider.DoMainThreadRequestClientIdAndEndpoint(Thunk: TAuthThunkClientIdAndEndpoint);
begin
  Thunk.Result := false;
  if Assigned(FAuthRequestClientIdAndEndpoint) then
    Thunk.Result := FAuthRequestClientIdAndEndpoint (self,
                                    THunk.Site,
                                    THunk.AuthBaseQueryURL,
                                    Thunk.AuthBaseTokenURL,
                                    THunk.ClientId,
                                    Thunk.ClientSecret,
                                    Thunk.Scope,
                                    THunk.RedirectURI,
                                    Thunk.ProtocolVersion,
                                    Thunk.UserAgentString);
end;

procedure TOAuth2Provider.DoMainThreadReuqestBrowserSignIn(Thunk: TAuthThunkBrowserSignIn);
begin
  Thunk.Result := false;
  if Assigned(FAuthRequestBrowserSignIn) then
    Thunk.Result := FAuthRequestBrowserSignIn(self,
                                              Thunk.Site,
                                              Thunk.InitialBrowserURL, THunk.RedirectedBrowserURL);
end;

procedure TOAuth2Provider.DoMainThreadRequestExistingAcessToken(Thunk: TAuthThunkRequestAccessToken);
begin
  Thunk.Result := nil;
  if Assigned(FAuthRequestExistingAccessToken) then
    Thunk.Result := FAuthRequestExistingAccessToken(self, THunk.Site);
end;

procedure TOAuth2Provider.DoMainThreadCacheNewAccessToken(Thunk: TAuthThunkCacheAccessToken);
begin
  if Assigned(FAuthCacheNewAccessToken) then
    FAuthCacheNewAccessToken(self, Thunk.Token)
  else
    Thunk.Token.Free;
end;

procedure TOAuth2Provider.ProcessThunks;
var
  Thunk: TChainedThunk;
  ProcessedOne: boolean;
begin
  ProcessedOne := true;
  while ProcessedOne do
  begin
    ProcessedOne := false;
    FChainedThunkLock.Acquire;
    try
      Thunk := FChainedThunksOutstanding.FLink.Owner as TChainedThunk;
      while Assigned(Thunk) do
      begin
        if not Thunk.ThunkDone then
          break;
      end;
    finally
      FChainedThunkLock.Release;
    end;
    if Assigned(Thunk) then
    begin
      if Thunk is TAuthThunkClientIdAndEndpoint then
        DoMainThreadRequestClientIdAndEndpoint(Thunk as TAuthThunkClientIdAndEndpoint)
      else if Thunk is TAuthThunkBrowserSignIn then
        DoMainThreadReuqestBrowserSignIn(Thunk as TAuthThunkBrowserSignIn)
      else if Thunk is TAuthThunkRequestAccessToken then
        DoMainThreadRequestExistingAcessToken(Thunk as TAuthThunkRequestAccessToken)
      else if Thunk is TAuthThunkCacheAccessToken then
        DoMainThreadCacheNewAccessToken(Thunk as TAuthThunkCacheAccessToken)
      else
        Assert(false);

      FChainedThunkLock.Acquire;
      Thunk.ThunkDone := true;
      Thunk.ThunkDoneEvent.SetEvent;
      DLListRemoveObj(@Thunk.ThunkLink);
      FChainedThunkLock.Release;
      ProcessedOne := true;
    end;
  end;
end;

function TOAuth2Provider.DoRequestClientIdAndEndpoint(Conn: TIDHTTPCached;
          Site: string;
          out AuthBaseQueryURL: string;
          out AuthBaseTokenURL: string;
          out ClientId: string;
          out ClientSecret: string;
          out Scope: string;
          out RedirectURI: string;
          out ProtocolVersion: TOAuthVersion;
          out UserAgentString:string): boolean;
var
  Thunk: TAuthThunkClientIdAndEndpoint;
begin
  Thunk := TAuthThunkClientIdAndEndpoint.Create;
  try
    Thunk.Site := Site;

    FChainedThunkLock.Acquire;
    try
      DLListInsertTail(@FChainedThunksOutstanding, @Thunk.ThunkLink);
    finally
      FChainedThunkLock.Release;
    end;
    Conn.WorkItemCtx.ThreadRec.Thread.Queue(ProcessThunks);

    Thunk.ThunkDoneEvent.WaitFor(INFINITE);

    AuthBaseQueryURL := Thunk.AuthBaseQueryURL;
    AuthBaseTokenURL := THunk.AuthBaseTokenURL;
    ClientId := Thunk.ClientId;
    ClientSecret := Thunk.ClientSecret;
    Scope := Thunk.Scope;
    RedirectURI := Thunk.RedirectURI;
    ProtocolVersion := Thunk.ProtocolVersion;
    UserAgentString := THunk.UserAgentString;
    result := Thunk.Result;
  finally
    Thunk.Free;
  end;
end;

function TOAuth2Provider.DoRequestBrowserSignIn(Conn: TIDHTTPCached;
                                Site: string;
                                InitialBrowserURL: string;
                                out RedirectedBrowserURL: string): boolean;
var
  Thunk: TAuthThunkBrowserSignIn;
begin
  Thunk := TAuthThunkBrowserSignIn.Create;
  try
    Thunk.Site := Site;
    Thunk.InitialBrowserURL := InitialBrowserURL;

    FChainedThunkLock.Acquire;
    try
      DLListInsertTail(@FChainedThunksOutstanding, @Thunk.ThunkLink);
    finally
      FChainedThunkLock.Release;
    end;
    Conn.WorkItemCtx.ThreadRec.Thread.Queue(ProcessThunks);

    Thunk.ThunkDoneEvent.WaitFor(INFINITE);

    RedirectedBrowserURL := Thunk.RedirectedBrowserURL;
    result := Thunk.Result;
  finally
    Thunk.Free;
  end;
end;

//Produces token.
function TOAuth2Provider.DoRequestExistingAccessToken(Conn: TIDHTTPCached;
                                      Site: string): TAuthToken;
var
  Thunk: TAuthThunkRequestAccessToken;
begin
  Thunk := TAuthThunkRequestAccessToken.Create;
  try
    Thunk.Site := Site;

    FChainedThunkLock.Acquire;
    try
      DLListInsertTail(@FChainedThunksOutstanding, @Thunk.ThunkLink);
    finally
      FChainedThunkLock.Release;
    end;
    Conn.WorkItemCtx.ThreadRec.Thread.Queue(ProcessThunks);

    Thunk.ThunkDoneEvent.WaitFor(INFINITE);

    result := Thunk.Result;
  finally
    Thunk.Free;
  end;
end;

//Consumes token.
procedure TOAuth2Provider.DoCacheNewAccessToken(Conn: TIDHTTPCached;
                                Token: TAuthToken);
var
  Thunk: TAuthThunkCacheAccessToken;
begin
  Thunk := TAuthThunkCacheAccessToken.Create;
  try
    Thunk.Token := Token;

    FChainedThunkLock.Acquire;
    try
      DLListInsertTail(@FChainedThunksOutstanding, @Thunk.ThunkLink);
    finally
      FChainedThunkLock.Release;
    end;
    Conn.WorkItemCtx.ThreadRec.Thread.Queue(ProcessThunks);

    Thunk.ThunkDoneEvent.WaitFor(INFINITE);
  finally
    Thunk.Free;
  end;
end;


procedure TOAuth2Provider.HandleAuthHeaders(Sender: TObject; AHeaders: TIdHeaderList; var VContinue: Boolean);
var
 idx: integer;
begin
  //Dup header is debug.
{$IFOPT C+}
  for idx := 0 to Pred(AHeaders.Count) do
  begin
    GLogLog(SV_INFO, S_AUTH_RESPONSE_HDR_NAME + ' ' + AHeaders.Names[idx]
                 + ' ' + S_AUTH_RESPONSE_HDR_VAL + ' '
                 + AHeaders.Values[AHeaders.Names[idx]]);
  end;
{$ENDIF}
end;

procedure TOAuth2Provider.HandleAuthRedirect(Sender: TObject; var dest: string; var NumRedirect: Integer; var Handled: boolean; var VMethod: TIdHTTPMethod);
var
  Conn: TIdHTTPCached;
begin
  Assert(Sender is TIDHttpCacheable);
  Conn := (Sender as TIDHttpCacheable).CachedParent;
  if (NumRedirect < Conn.IdHttp.RedirectMaximum) then
  begin
    //dest now contains The URL with assorted parameters etc.
    Conn.LatestAuthRedirect := dest;
    GLogLog(SV_INFO, S_AUTH_REDIRECTED_TO +  Conn.LatestAuthRedirect);
    Inc(NumRedirect);
  end
  else
  begin
    Conn.AuthFailed := true;
    Conn.AuthFailString := S_FAIL_TOO_MANY_REDIRECTS;
    GLogLog(SV_INFO, 'FAILED');
    GLogLog(SV_INFO, Conn.AuthFailString);
  end;
end;

function TOAuth2Provider.OAuth2GetCodeFromURL(URL: string; out Leg1AuthCode:string): boolean;
var
  Params: TStringList;
  Values: TStringList;
  i: integer;
begin
  result := false;
  Leg1AuthCode := '';
  Params := TStringList.Create;
  Values := TStringList.Create;
  try
    URLPostDataDecode(URL, Params, Values);
    i := Params.IndexOf(S_CODE);
    if (i >= 0) then
    begin
      Leg1AuthCode := Values[i];
      result := true;
      exit;
    end;
  finally
    Params.Free;
    Values.Free;
  end;
end;

procedure TOAuth2Provider.OAuth1ProtocolFlow(Conn: TIDHTTPCached;
                                             AuthBaseQueryUrl:string;
                                             AuthBaseTokenURL: string;
                                             ClientId:string;
                                             ClientSecret: string;
                                             Scope:string;
                                             RedirectURI:string;
                                             UserAgentString:string);
var
  QueryURL: string;
  Params, Values: TStringList;
  NonceGuid: TGUID;
  SignString: string;
  ProtoStr, SiteStr, FullFileStr, NameStr, ExtStr: string;
  HasParams: boolean;
  BaseParams, BaseValues: TStringList;
  AuthHeader: string;
  PostData, PostResponse: TMemoryStream;
  Mac: TIdHMACSha1;
begin
  if CompareText(Conn.Proto, S_SSL_PROTOCOL) <> 0 then
  begin
    Conn.AuthFailString := S_FAIL_BAD_PROTO;
    Conn.AuthFailed := true;
    exit;
  end;

  Conn.IdHttp.HandleRedirects := false;
  Conn.IdHttp.OnRedirect := HandleAuthRedirect;
  Conn.IdHttp.OnHeadersAvailable := HandleAuthHeaders;
  if Length(UserAgentString) > 0 then
    Conn.IdHttp.Request.UserAgent := UserAgentString;
  if not Conn.IdHttp.ManagedIOHandler then
    Conn.IdHttp.CreateIoHandler(TIdIoHandlerClass(TIdSSLIOHandlerSocketOpenSSL));

  Params := TStringList.Create;
  Values := TStringList.Create;
  BaseParams := TStringList.Create;
  BaseValues := TStringList.Create;
  PostData := TTrackedMemoryStream.Create;
  PostResponse := TTrackedMemoryStream.Create;
  try
    QueryURL := S_SSL_PROTOCOL + '://' + AuthBaseQueryURL;
    ParseURL(QueryUrl, ProtoStr, SiteStr, FullFileStr, NameStr, ExtStr);
    Params.Add(S_OAUTH_REALM);
    Values.Add(ProtoStr + '://' + SiteStr);
    Params.Add(S_OAUTH_CONSUMER_KEY);
    Values.Add(ClientId);
    Params.Add(S_OAUTH_SIGNATURE_METHOD);
    Values.Add(S_OAUTH_HMAC_SHA1);
    Params.Add(S_OAUTH_TIMESTAMP);
    Values.Add(IntToStr(DelphiDateTimeToUnixDateTime(Now))); //Need C time.
    //TODO - Probably need something more secure than just a GUID,
    //but this will do for now.
    Params.Add(S_OAUTH_NONCE);
    CreateGUID(NonceGuid);
    Values.Add(GUIDToString(NonceGuid));
    Params.Add(S_OAUTH_CALLBACK);
    Values.Add(RedirectURI);
    Params.Add(S_OAUTH_VERSION);
    Values.Add(S_OAUTH_VERSION_1_0);
    //Uggg, have to calculate the signature over all this!!
    SignString := OAuth1HMACTokenEncode('POST',
                                          S_SSL_PROTOCOL + '://' + AuthBaseQueryUrl,
                                          Params,
                                          Values);
    Mac := TIdHMACSha1.Create;
    try
      SetOAuth1MACFromClientAndTokenSecrets(Mac, ClientSecret, '');
      SignString := OAuth1MacSignRequest(Mac, SignString);
    finally
      Mac.Free;
    end;

    //If signature is last param / value,
    //then we can remove, re-sign and re-add easily.
    Params.Add(S_OAUTH_SIGNATURE);
    Values.Add(SignString);

    AuthHeader := S_OAUTH1_HEADER_START + OAuth1HeaderValEncode(Params, Values);

    GLogLog(SV_INFO, '');
    GLogLog(SV_INFO, FetchParseMethodStrings[fpmPut] + ': ' + QueryUrl);
    GLogLog(SV_INFO, '');
    GLogLog(SV_INFO, S_OAUTH1_HEADER + ': ' + AuthHeader);
    try
      Conn.IdHttp.Request.CustomHeaders.AddValue(S_OAUTH1_HEADER, AuthHeader);
      Conn.IdHttp.Post(QueryUrl, PostData, PostResponse);
    except
      on E:Exception do
      begin
        Conn.AuthFailed := true;
        Conn.AuthFailString := E.Message;
        GLogLog(SV_INFO, 'FAILED');
        GLogLog(SV_INFO, Conn.AuthFailString);
        if E is EIdHTTPProtocolException then
          GLogLog(SV_INFO, (E as EIdHTTPProtocolException).ErrorMessage);
        exit;
      end;
    end;

    GLogLog(SV_INFO, '');
    GLogLog(SV_INFO, 'OK so far,');
    GLogLog(SV_INFO, '');
    GLogLogStream(SV_INFO, PostResponse);

  finally
    Params.Free;
    Values.Free;
    BaseParams.Free;
    BaseValues.Free;
    PostData.Free;
    PostResponse.Free;
  end;
end;

procedure TOAuth2Provider.OAuth2ProtocolFlow(Conn: TIDHTTPCached;
                                             AuthBaseQueryUrl:string;
                                             AuthBaseTokenURL: string;
                                             ClientId:string;
                                             ClientSecret: string;
                                             Scope:string;
                                             RedirectURI:string;
                                             UserAgentString:string);

var
  Params: TStringList;
  Values: TStringList;
  StateGuid: TGUID;
  PostStream, ResponseStream: TTrackedMemoryStream;
  QueryURL: string;
  QueryParams: string;
  QueryParamsAnsi: AnsiString;
  Retry: boolean;
  Leg1AuthCode: string;
  CodeURI: string;
  TokenString: AnsiString;

  //Parsing, building strings.
  ProtoStr, SiteStr, FullFileStr, NameStr, ExtStr: string;
  HasParams: boolean;

  //Setting up OAuth2 token for signing.
  OAuth2Token: TOAuth2Token;

begin
  if CompareText(Conn.Proto, S_SSL_PROTOCOL) <> 0 then
  begin
    Conn.AuthFailString := S_FAIL_BAD_PROTO;
    Conn.AuthFailed := true;
    exit;
  end;

  Conn.IdHttp.HandleRedirects := false;
  Conn.IdHttp.OnRedirect := HandleAuthRedirect;
  Conn.IdHttp.OnHeadersAvailable := HandleAuthHeaders;
  if Length(UserAgentString) > 0 then
    Conn.IdHttp.Request.UserAgent := UserAgentString;
  if not Conn.IdHttp.ManagedIOHandler then
    Conn.IdHttp.CreateIoHandler(TIdIoHandlerClass(TIdSSLIOHandlerSocketOpenSSL));

  //Protocol stuff below here.
  Params := TStringList.Create;
  Values := TStringList.Create;
  ResponseStream := TTrackedMemoryStream.Create;
  PostStream := TTrackedMemoryStream.Create;
  try
    //Leg 1 of Auth.
    //Build URL for initial authorization request.
    Leg1AuthCode := '';

    ParseURLEx(S_SSL_PROTOCOL + '://' + AuthBaseQueryURL,
               ProtoStr, SiteStr, FullFileStr, NameStr, ExtStr,
               HasParams, Params, Values);

    Params.Add(S_CLIENT_ID);
    Values.Add(ClientId);
    if Length(Scope) > 0 then
    begin
      Params.Add(S_SCOPE);
      Values.Add(Scope);
    end;
    if Length(RedirectURI) > 0 then
    begin
      Params.Add(S_REDIRECT_URI);
      Values.Add(RedirectURI);
    end;
    //TODO - State should probably be something more secure than just a GUID
    //need to put a hash in here at some point - however, we are in SSL.
    Params.Add(S_RESPONSE_TYPE);
    Values.Add(S_CODE);

    CreateGuid(StateGuid);
    Params.Add(S_STATE);
    Values.Add(GUIDToString(StateGuid));

    QueryUrl := BuildURL(ProtoStr, SiteStr, FullFileStr, true, Params, Values);

    Retry := true;
    while Retry do
    begin
      try
        GLogLog(SV_INFO, '');
        GLogLog(SV_INFO, FetchParseMethodStrings[fpmGet] + ': ' + QueryUrl);
        GLogLog(SV_INFO, '');
        Conn.IdHttp.Get(QueryURL, ResponseStream);
        Retry := false;
      except
        on E:Exception do
        begin
          if (E is EIdHTTPProtocolException)
            and ((E as EIdHTTPProtocolException).ErrorCode = 302)
          then
          begin
            if not Conn.AuthFailed then
            begin
              if OAuth2GetCodeFromURL(Conn.LatestAuthRedirect, Leg1AuthCode) then
                Retry := false
              else
                QueryUrl := Conn.LatestAuthRedirect;
            end
            else
              Retry := false;
          end
          else
          begin
            Conn.AuthFailString := E.Message;
            Conn.AuthFailed := true;
            GLogLog(SV_INFO, 'FAILED');
            GLogLog(SV_INFO, Conn.AuthFailString);
            if E is EIdHTTPProtocolException then
              GLogLog(SV_INFO, (E as EIdHTTPProtocolException).ErrorMessage);
            exit;
          end;
        end;
      end;
    end;

    //And now we get back a response.
    if ResponseStream.Size > 0 then
    begin
      GLogLogStream(SV_INFO, ResponseStream);
    end;

    if Length(Leg1AuthCode) > 0 then
    begin
      //Code was in redirect.
      GLogLog(SV_INFO, S_LEG1_CODE + Leg1AuthCode);
    end
    else
    begin
      //Oh dear, website has required that we do some kind of a login screen.
      DoRequestBrowserSignIn(Conn, Conn.Site, QueryURL, CodeURI);
      OAuth2GetCodeFromURL(CodeURI, Leg1AuthCode);
    end;
    //Still not auth code - fail.
    if not (Length(Leg1AuthCode) > 0) then
    begin
      Conn.AuthFailString := S_FAIL_NO_LEG1_CODE;
      Conn.AuthFailed := true;
      exit;
    end
    else
    begin
      //Code obtained from browser.
      GLogLog(SV_INFO, S_LEG1_CODE + Leg1AuthCode);
    end;

    //Params and values here encoded into data stream, not URL itself.
    Params.Clear;
    Values.Clear;
    Params.Add(S_CLIENT_ID);
    Values.Add(ClientId);
    Params.Add(S_CLIENT_SECRET);
    Values.Add(ClientSecret);
    Params.Add(S_GRANT_TYPE);
    Values.Add(S_AUTHORIZATION_CODE);
    Params.Add(S_REDIRECT_URI);
    Values.Add(RedirectURI);
    Params.Add(S_CODE);
    Values.Add(Leg1AuthCode);

    QueryParams := URLPostDataEncode(Params, Values);
    QueryParamsAnsi := QueryParams;

    QueryURL := S_SSL_PROTOCOL + '://' + AuthBaseTokenUrl;

    //Leg 2 of Auth - Exchange code for a proper access token.
    PostStream.Clear;
    PostStream.WriteBuffer(QueryParamsAnsi[1], Length(QueryParamsAnsi));
    GLogLogStream(SV_INFO, PostStream);
    ResponseStream.Clear;
    try
      GLogLog(SV_INFO, '');
      GLogLog(SV_INFO, FetchParseMethodStrings[fpmPost] + ': ' + QueryUrl);
      GLogLog(SV_INFO, '');
      Conn.IdHttp.Request.CustomHeaders.AddValue('Content-Type', 'application/x-www-form-urlencoded');
      Conn.IdHttp.Post(QueryURL, PostStream, ResponseStream);
    except
      on E:Exception do
      begin
        Conn.AuthFailString := E.Message;
        Conn.AuthFailed := true;
        GLogLog(SV_INFO, 'FAILED');
        GLogLog(SV_INFO, Conn.AuthFailString);
        if E is EIdHTTPProtocolException then
          GLogLog(SV_INFO, (E as EIdHTTPProtocolException).ErrorMessage);
        exit;
      end;
    end;

    GLogLogStream(SV_INFO, ResponseStream);

    Conn.AuthFailed := false;
    Conn.AuthToken := TOAuth2Token.Create;

    SetLength(TokenString, ResponseStream.Size);
    ResponseStream.Seek(0, soFromBeginning);
    ResponseStream.ReadBuffer(TokenString[1], ResponseStream.Size);

    Conn.AuthFailed := false;

    OAuth2Token := Conn.AuthToken as TOAuth2Token;
    OAuth2Token.TokenServerResponse := TokenString;
    OAuth2Token.ClientSecret := ClientSecret;

    if not (OAuth2Token.SetIdHMACFromClientSecret
         and OAuth2Token.ParseServerResponse) then
    begin
      Conn.AuthFailed := true;
      Conn.AuthFailString := S_FAIL_SECRET_KEY_BAD_DATA;
      Conn.AuthToken.Free;
      Conn.AuthToken := nil;
      exit;
    end;

    GLogLog(SV_INFO, 'OK');
  finally
    Params.Free;
    Values.Free;
    ResponseStream.Free;
    PostStream.Free;
  end;
end;

procedure TOAuth2Provider.ConnectionAuthRequest(Conn: TIdHttpCached);
var
  AuthBaseQueryURL, AuthBaseTokenURL, ClientId, ClientSecret, Scope, RedirectURI, UserAgentString: string;
  ProtocolVersion: TOAuthVersion;
  Token: TAuthToken;
begin
  //Connection has suitable proto.
  AuthBaseQueryURL := '';
  AuthBaseTokenURL := '';
  ClientId := '';
  CLientSecret := '';
  RedirectURI := '';
  UserAgentString := '';
  Conn.AuthFailed := false;

  Token := DoRequestExistingAccessToken(Conn, Conn.Site);
  if Assigned(Token) then
  begin
    Conn.AuthToken := Token;
    Conn.AuthFailed := false;
    exit;
  end;

  //Need to get sufficient basic details for authorization request.
  if not DoRequestClientIdAndEndpoint(Conn, Conn.Site,
      AuthBaseQueryUrl, AuthBaseTokenURL, ClientId, ClientSecret, Scope, RedirectURI, ProtocolVersion, UserAgentString) then
  begin
    Conn.AuthFailString := S_FAIL_SITE_NOT_RECOGNISED + ' ' + Conn.Site;
    Conn.AuthFailed := true;
    exit;
  end;

  if (ProtocolVersion = oavVersion1) then
  begin
    OAuth1ProtocolFlow(Conn,
                       AuthBaseQueryUrl,
                       AuthBaseTokenUrl,
                       ClientId,
                       ClientSecret,
                       Scope,
                       RedirectURI,
                       UserAgentString);
  end
  else if (ProtocolVersion = oavVersion2) then
  begin
    OAuth2ProtocolFlow(Conn,
                       AuthBaseQueryUrl,
                       AuthBaseTokenUrl,
                       ClientId,
                       ClientSecret,
                       Scope,
                       RedirectURI,
                       UserAgentString);
  end
  else
  begin
    Conn.AuthFailString := S_FAIL_BAD_PROTOCOL_VERSION;
    Conn.AuthFailed := true;
  end;

  if Assigned(Conn.AuthToken) then
    DoCacheNewAccessToken(Conn, Conn.AuthToken.Clone);
end;

constructor TOAuth2Provider.Create;
begin
  inherited;
  DLItemInitList(@FChainedThunksOutstanding);
  FChainedThunkLock := TCriticalSection.Create;
end;

destructor TOAuth2Provider.Destroy;
begin
  //TODO - Suspect previous checking of "all idle" will mean we don't
  //destroy this until nothing else outstanding, but
  //might still need to consider a "busy / idle / stop" mechanism like
  //other objs.
  Assert(DlItemIsEmpty(@FChainedThunksOutstanding));
  FChainedThunkLock.Free;
  inherited;
end;

{ Test code }
{
POST /request?b5=%3D%253D&a3=a&c%40=&a2=r%20b HTTP/1.1
     Host: example.com
     Content-Type: application/x-www-form-urlencoded
     Authorization: OAuth realm="Example",
                    oauth_consumer_key="9djdj82h48djs9d2",
                    oauth_token="kkk9d7dh3k39sjv7",
                    oauth_signature_method="HMAC-SHA1",
                    oauth_timestamp="137131201",
                    oauth_nonce="7d8f3e4a",
                    oauth_signature="bYT5CMsGcbgUdFHObYMEfcx6bsw%3D"

c2&a3=2+q

     POST&http%3A%2F%2Fexample.com%2Frequest&a2%3Dr%2520b%26a3%3D2%2520q
     %26a3%3Da%26b5%3D%253D%25253D%26c%2540%3D%26c2%3D%26oauth_consumer_
     key%3D9djdj82h48djs9d2%26oauth_nonce%3D7d8f3e4a%26oauth_signature_m
     ethod%3DHMAC-SHA1%26oauth_timestamp%3D137131201%26oauth_token%3Dkkk
     9d7dh3k39sjv7
}

function OAuth1TestHMACTokenEncode: boolean;
const
  ConcatResult: string =
     'POST&http%3A%2F%2Fexample.com%2Frequest&a2%3Dr%2520b%26a3%3D2%2520q' +
     '%26a3%3Da%26b5%3D%253D%25253D%26c%2540%3D%26c2%3D%26oauth_consumer_' +
     'key%3D9djdj82h48djs9d2%26oauth_nonce%3D7d8f3e4a%26oauth_signature_m' +
     'ethod%3DHMAC-SHA1%26oauth_timestamp%3D137131201%26oauth_token%3Dkkk' +
     '9d7dh3k39sjv7';

  ConcatResult2: string =
     'GET&http%3A%2F%2Fphotos.example.net%2Fphotos&file%3Dvacation.jpg%26' +
     'oauth_consumer_key%3Ddpf43f3p2l4k3l03%26oauth_nonce%3Dkllo9940pd933' +
     '3jh%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D119124' +
     '2096%26oauth_token%3Dnnch734d00sl2jdk%26oauth_version%3D1.0%26size%' +
     '3Doriginal';
var
  TokenRes: string;
  Params, Values: TStringList;
begin
  Params := TStringList.Create;
  Values := TStringList.Create;
  try
    Params.Add(S_OAUTH_CONSUMER_KEY);
    Values.Add('9djdj82h48djs9d2');
    Params.Add(S_OAUTH_TOKEN);
    Values.Add('kkk9d7dh3k39sjv7');
    Params.Add(S_OAUTH_SIGNATURE_METHOD);
    Values.Add(S_OAUTH_HMAC_SHA1);
    Params.Add(S_OAUTH_TIMESTAMP);
    Values.Add('137131201');
    Params.Add(S_OAUTH_NONCE);
    Values.Add('7d8f3e4a');
    //Add body params and values here.
    Params.Add('c2');
    Values.Add('');
    Params.Add('a3');
    Values.Add('2 q');

    TokenRes := OAuth1HMACTokenEncode(
      'POST',
      'http://example.com/request?b5=%3D%253D&a3=a&c%40=&a2=r%20b',
      Params, Values);
    result := TokenRes = ConcatResult;

    Params.Clear;
    Values.Clear;

    Params.Add('file');
    Values.Add('vacation.jpg');
    Params.Add('oauth_consumer_key');
    Values.Add('dpf43f3p2l4k3l03');
    Params.Add('oauth_nonce');
    Values.Add('kllo9940pd9333jh');
    Params.Add('oauth_signature_method');
    Values.Add('HMAC-SHA1');
    Params.Add('oauth_timestamp');
    Values.Add('1191242096');
    Params.Add('oauth_token');
    Values.Add('nnch734d00sl2jdk');
    Params.Add('oauth_version');
    Values.Add('1.0');
    Params.Add('size');
    Values.Add('original');

    TokenRes := OAuth1HMACTokenEncode(
      'GET',
      'http://photos.example.net/photos',
      Params, Values);
    result := result and (TokenRes = ConcatResult2);
  finally
    Params.Free;
    Values.Free;
  end;
end;

function OAuth1TestHMACSHA1: boolean;
var
  Mac: TIdHMACSha1;
  KeyString, MsgString: string;
  KeyBytes, MsgBytes,ResBytes: TIdBytes;
  ResHex: string;
begin
  result := true;
  Mac := TIdHMACSha1.Create;
  try
    KeyString := '';
    MsgString := '';
    result := result and UTF8StringToIdBytes(KeyString, KeyBytes);
    result := result and UTF8StringToIdBytes(MsgString, MsgBytes);
    Mac.Key := KeyBytes;
    ResBytes := Mac.HashValue(MsgBytes);
    ResHex := IdBytesToHexString(ResBytes);
    result := result and
      (CompareText(ResHex, 'fbdb1d1b18aa6c08324b7d64b71fb76370690e1d') = 0);

    KeyString := 'key';
    MsgString := 'The quick brown fox jumps over the lazy dog';
    result := result and UTF8StringToIdBytes(KeyString, KeyBytes);
    result := result and UTF8StringToIdBytes(MsgString, MsgBytes);
    Mac.Key := KeyBytes;
    ResBytes := Mac.HashValue(MsgBytes);
    ResHex := IdBytesToHexString(ResBytes);
    result := result and
      (CompareText(ResHex, 'de7c9b85b8b78aa6bc8a7a36f70a90701c9db4d9') = 0);
  finally
    Mac.Free;
  end;
end;

function OAuth1TestHMACSHA1Digest: boolean;
var
  Mac: TIdHMACSha1;
  KeyString, MsgString: string;
  KeyBytes, MsgBytes,ResBytes: TIdBytes;
  ResDigest: string;
begin
  result := true;

  Mac := TIdHMACSha1.Create;
  try
    KeyString := 'kd94hf93k423kf44&pfkkdhi9sl3r4s00';
    MsgString := 'text';
    result := result and UTF8StringToIdBytes(KeyString, KeyBytes);
    Mac.Key := KeyBytes;
    ResDigest := OAuth1MacSignRequest(Mac, MsgString);
    result := result and
      (CompareText(ResDigest, 'tR3+Ty81lMeYAr/Fid0kMTYa/WM=') = 0);
  finally
    Mac.Free;
  end;

end;
{

POST /oauth/request_token HTTP/1.1
Host: api.twitter.com
Accept: */*
Authorization:
        OAuth oauth_callback="http%3A%2F%2Flocalhost%2Fsign-in-with-twitter%2F",
              oauth_consumer_key="cChZNFj6T5R0TigYB9yd1w",
              oauth_nonce="ea9ec8429b68d6b77cd5600adbbb0456",
              oauth_signature="F1Li3tvehgcraF8DMJ7OyxO4w9Y%3D",
              oauth_signature_method="HMAC-SHA1",
              oauth_timestamp="1318467427",
              oauth_version="1.0"

Consumer secret used: L8qq9PZyRg6ieKGEKhZolGC0vJWLw8iEJ88DRdyOg

}

function OAuth1TestSecureRequestSigning(): boolean;
var
  Params, Values: TStringList;
  QueryURL, StringToSign: string;
  MAC: TIdHMACSha1;
  HashSig: string;

begin
  Params := TStringList.Create;
  Values := TStringList.Create;
  MAC := TIdHMACSha1.Create;
  try
    QueryURL := 'http://photos.example.net/initiate';
    Params.Add(S_OAUTH_REALM);
    Values.Add('Photos');
    Params.Add(S_OAUTH_CONSUMER_KEY);
    Values.Add('dpf43f3p2l4k3l03');
    Params.Add(S_OAUTH_SIGNATURE_METHOD);
    Values.Add(S_OAUTH_HMAC_SHA1);
    Params.Add(S_OAUTH_TIMESTAMP);
    Values.Add('137131200');
    Params.Add(S_OAUTH_NONCE);
    Values.Add('wIjqoS');
    Params.Add(S_OAUTH_CALLBACK);
    Values.Add('http://printer.example.com/ready');

    StringToSign := OAuth1HMACTokenEncode('POST',
                                          QueryURL,
                                          Params,
                                          Values);

    SetOAuth1MACFromClientAndTokenSecrets(Mac, 'kd94hf93k423kf44', '');
    HashSig := OAuth1MacSignRequest(Mac, StringToSign);
    HashSig := URLEncode(HashSig);
    if (CompareText(HashSig, '74KNZJeDHnMBp0EMJ9ZHt%2FXKycU%3D') <> 0) then
    begin
      result := false;
      exit;
    end;
    result := true;

    QueryURL := 'http://api.twitter.com/oauth/request_token';
    Params.Add(S_OAUTH_CALLBACK);
    Values.Add('http://localhost/sign-in-with-twitter/');
    Params.Add(S_OAUTH_CONSUMER_KEY);
    Values.Add('cChZNFj6T5R0TigYB9yd1w');
    Params.Add(S_OAUTH_NONCE);
    Values.Add('ea9ec8429b68d6b77cd5600adbbb0456');
    Params.Add(S_OAUTH_SIGNATURE_METHOD);
    Values.Add(S_OAUTH_HMAC_SHA1);
    Params.Add(S_OAUTH_TIMESTAMP);
    Values.Add('1318467427');
    Params.Add(S_OAUTH_VERSION);
    Values.Add(S_OAUTH_VERSION_1_0);

    StringToSign := OAuth1HMACTokenEncode('POST',
                                          QueryURL,
                                          Params,
                                          Values);

    SetOAuth1MACFromClientAndTokenSecrets(Mac, 'L8qq9PZyRg6ieKGEKhZolGC0vJWLw8iEJ88DRdyOg', '');
    HashSig := OAuth1MacSignRequest(Mac, StringToSign);
    HashSig := URLEncode(HashSig);
    if (CompareText(HashSig, 'F1Li3tvehgcraF8DMJ7OyxO4w9Y%3D') <> 0) then
    begin
      result := false;
      exit;
    end;
    result := true;
  finally
    Params.Free;
    Values.Free;
    MAC.Free;
  end;
end;

function OAuth2TestSecureRequestSigning(): boolean;
var
  URL: string;
  HashSig: string;
  Params: TStringList;
  Values: TStringList;
  Token: TOAuth2Token;
begin
  if not LoadOpenSSLLibrary then
  begin
    result := false;
    exit;
  end;
  Params := TStringList.Create;
  Values := TStringList.Create;
  Token := TOAuth2Token.Create;
  try
    Token.ClientSecret := '6dc1787668c64c939929c17683d7cb74';
    Params.Add(S_ACCESS_TOKEN);
    Values.Add('fb2e77d.47a0479900504cb3ab4a1f626d174d2d');
    Params.Add('count');
    Values.Add('10');
    if not Token.SetIdHMACFromClientSecret then
    begin
      result := false;
      exit;
    end;
    URL := BuildURL(S_SSL_PROTOCOL, 'www.instagram.com', '/media/657988443280050001_25025320',
                    true, Params, Values);
    OAuth2PassedSelfTest := true; //Need to turn functionality on to test it....
    HashSig := Token.SignRequest(URL);
    if CompareText(HashSig, '260634b241a6cfef5e4644c205fb30246ff637591142781b86e2075faf1b163a') <> 0 then
    begin
      result := false;
      exit;
    end;
    result := true;
  finally
    Params.Free;
    Values.Free;
    Token.Free;
  end;
end;

initialization
  GAuthProvider := TOAuth2Provider.Create;
  OAuth2PassedSelfTest := OAuth2TestSecureRequestSigning;
  OAuth1PassedSelfTest := OAuth1TestHMACTokenEncode
                  and OAuth1TestHMACSHA1
                  and OAuth1TestHMACSHA1Digest
                  and OAuth1TestSecureRequestSigning;
{
  Assert(OAuth2PassedSelfTest);
  Assert(OAuth1PassedSelfTest);
}
finalization
  GAuthProvider.Free;
end.
