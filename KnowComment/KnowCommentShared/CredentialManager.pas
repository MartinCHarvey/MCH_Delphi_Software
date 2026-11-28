unit CredentialManager;

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
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SysUtils,
  Classes,
  OAuth2,
  HTTPDocFetcher;

{
  Maintains
    - User credentials : In ram only.
    - Access tokens: In ram only, eventually persist to disk.
    - Client ID's: Hard coded.
    - Authorization URL's: Hard coded.

    TODO: Thread safety.
    TODO: For the moment, all hard coded whilst I debug Auth.
}

type
{$IFDEF USE_TRACKABLES}
  TCredentialManager = class(TTrackable)
{$ELSE}
  TCredentialManager = class
{$ENDIF}
  private
    FCachedTokenList: TList;
  protected
    function AuthRequestExistingTokenIdx(Site: string): integer;
    //Produces clone.
    function AuthRequestExistingAccessToken(Sender: TObject; Site: string): TAuthToken;
    //Takes ownership.
    procedure AuthCacheNewAccessToken(Sender: TObject; NewToken: TAuthToken);

    function AuthRequestClientIdAndEndpoint(Sender: TObject;
              Site: string;
              out AuthBaseQueryURL: string;
              out AuthBaseTokenURL: string;
              out ClientId: string;
              out ClientSecret: string;
              out Scope: string;
              out RedirectURI: string;
              out ProtocolVersion: TOAuthVersion;
              out UserAgentString:string):boolean;

    function AuthRequestBrowserSignIn(Sender: TObject;
              Site: string;
              InitialBrowserURL: string;
              out RedirectedBrowserURL: string): boolean;
  public
    constructor Create;
    destructor Destroy; override;
  end;

var
  GCredentialManager: TCredentialManager;

implementation

uses
{$IFDEF MSWINDOWS}
  ShellApi, Windows, UITypes
{$ENDIF MSWINDOWS}
{$IFDEF POSIX}
  //TODO - Posix imports for ShellExecute functionality.
{$ENDIF POSIX}
  , AuthenticateForm,
  KKUtils;
  //Want trackables to be uninit later.

{ TCredentialManager }

const
  //These site names only site names we will allow AUTH requests for.
  //Not the same as a site URL.
  //Should this be www.instagram.com if not using JSON api requests?
  S_INSTAGRAM_AUTH_URL = 'www.instagram.com/oauth/authorize/';
  S_INSTAGRAM_TOKEN_URL = 'www.instagram.com/oauth/access_token';
  S_INSTAGRAM_CLIENT_ID = 'e8dac7375613424ca3f10eadfed01e69';
  S_INSTAGRAM_CLIENT_SECRET = 'fbb208cfec414cc194adc37d313a3967';
  S_INSTAGRAM_SCOPE = 'basic';
  S_TWITTER_AUTH_URL = 'twitter.com/oauth/request_token';
  S_TWITTER_TOKEN_URL = 'twitter.com/oauth/access_token';
  S_TWITTER_CLIENT_ID = 'nCTX8nIOZdGYb37FvUaydH5u6';
  S_TWITTER_CLIENT_SECRET = 'p9UNaDvQ2u2mFlpvL61beREwpjeCQEmjAOl14G2Epkc4ROkRTq';
  S_TWITTER_SCOPE = 'basic';
  S_REDIRECT_URI = 'http://www.martincharvey.com/?page_id=143';

function TCredentialManager.AuthRequestClientIdAndEndpoint(Sender: TObject;
          Site: string;
          out AuthBaseQueryURL: string;
          out AuthBaseTokenURL: string;
          out ClientId: string;
          out ClientSecret: string;
          out Scope: string;
          out RedirectURI: string;
          out ProtocolVersion: TOAuthVersion;
          out UserAgentString:string):boolean;
begin
  UserAgentString := S_USER_AGENT;
  if Site = S_INSTAGRAM_SITE_NAME then
  begin
    AuthBaseQueryUrl := S_INSTAGRAM_AUTH_URL;
    AuthBaseTokenUrl := S_INSTAGRAM_TOKEN_URL;
    ClientId := S_INSTAGRAM_CLIENT_ID;
    ClientSecret := S_INSTAGRAM_CLIENT_SECRET;
    Scope := S_INSTAGRAM_SCOPE;
    RedirectURI := S_REDIRECT_URI;
    ProtocolVersion := oavVersion2;
    result := true;
  end
  else if Site = S_TWITTER_SITE_NAME then
  begin
    AuthBaseQueryUrl := S_TWITTER_AUTH_URL;
    AuthBaseTokenUrl := S_TWITTER_TOKEN_URL;
    ClientId := S_TWITTER_CLIENT_ID;
    ClientSecret := S_TWITTER_CLIENT_SECRET;
    Scope := S_TWITTER_SCOPE;
    RedirectURI := S_REDIRECT_URI;
    ProtocolVersion := oavVersion1; //Probably not but worth a shot.
    result := true;
  end
  else
    result := false;
end;

function TCredentialManager.AuthRequestBrowserSignIn(Sender: TObject;
        Site: string;
        InitialBrowserURL: string;
        out RedirectedBrowserURL: string): boolean;
var
  AuthFrm: TAuthenticateFrm;

begin
{$IFDEF MSWINDOWS}
  ShellExecute(0, 'OPEN', PChar(InitialBrowserURL), '', '', SW_SHOWNORMAL);
{$ENDIF MSWINDOWS}
{$IFDEF POSIX}
  _system(PAnsiChar('open ' + AnsiString(InitialBrowserURL)));
{$ENDIF POSIX}
  AuthFrm := TAuthenticateFrm.Create(nil);
  try
    AuthFrm.SiteLabel.Text := Site;
    result := AuthFrm.ShowModal = mrOK;
    if result then
      RedirectedBrowserURL := AuthFrm.URLEdit.Text;
  finally
    AuthFrm.Release;
  end;
end;

constructor TCredentialManager.Create;
begin
  inherited;
  FCachedTokenList := TList.Create;
end;

destructor TCredentialManager.Destroy;
var
  i: integer;
begin
  for i := 0 to Pred(FCachedTokenList.Count) do
    TObject(FCachedTokenList[i]).Free;
  FCachedTokenList.Free;
  inherited;
end;

function TCredentialManager.AuthRequestExistingTokenIdx(Site: string): integer;
var
  i: integer;
begin
  result := -1;
  for i := 0 to Pred(FCachedTokenList.Count) do
  begin
    if CompareText(Site, TAuthToken(FCachedTokenList[i]).Site) = 0 then
      result := i;
  end;
end;

function TCredentialManager.AuthRequestExistingAccessToken(Sender: TObject; Site: string): TAuthToken;
var
  i: integer;
begin
  result := nil;
  i := AuthRequestExistingTokenIdx(Site);
  if i >= 0 then
    result := TAuthToken(FCachedTokenList[i]).Clone;
end;

procedure TCredentialManager.AuthCacheNewAccessToken(Sender: TObject; NewToken: TAuthToken);
var
  i: integer;
begin
  i := AuthRequestExistingTokenIdx(NewToken.Site);
  if i >= 0 then
  begin
    TAuthToken(FCachedTokenList[i]).Free;
    FCachedTokenList[i] := NewToken;
  end
  else
    FCachedTokenList.Add(NewToken);
end;

initialization
  GCredentialManager := TCredentialManager.Create;
  //Connect to authorization component.
  GAuthProvider.OnRequestClientIdAndEndpoint := GCredentialManager.AuthRequestClientIdAndEndpoint;
  GAuthProvider.OnRequestBrowserSignIn := GCredentialManager.AuthRequestBrowserSignIn;
  GAuthProvider.OnAuthRequestExistingAccessToken := GCredentialManager.AuthRequestExistingAccessToken;
  GAuthProvider.OnAuthCacheNewAccessToken := GCredentialManager.AuthCacheNewAccessToken;
finalization
  GCredentialManager.Free;
end.
