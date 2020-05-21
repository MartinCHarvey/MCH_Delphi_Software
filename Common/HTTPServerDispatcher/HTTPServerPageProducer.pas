unit HTTPServerPageProducer;

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

//TODO - This all uses table tags, when it should prob use div's and CSS.

//TODO - "Reset my password" link.

uses
  HTTPServerDispatcher, IdCustomHTTPServer, IdContext,
  Classes;


type
  //TODO - Could clean this up with slightly fewer classes
  //in a bit.

  TPageProducerSession = class(THTTPDispatcherSession)
  protected
    FPageProducer: TCustomPageProducer;
    procedure AfterSessionStart;override;
    procedure BeforeSessionEnd;override;
  public
    procedure ProcessIdCommandGet(AContext: TIdContext;
                                  ARequestInfo: TIdHTTPRequestInfo;
                                  AResponseInfo: TIdHTTPResponseInfo);override;
  end;

  //This does a bit of pre-processing for parameters, URI, etc...

  type
    //TODO - All a bit up in the air as to how much of this will be needed.
    TIndyParams = record
      AContext: TIdContext;
      ARequestInfo: TIdHTTPRequestInfo;
      AResponseInfo: TIdHTTPResponseInfo
    end;

    TPageParams = record
      Params: TStringList;
      Values: TStringList;
    end;

    TParsedURI = record
      Method: THTTPCommandType;
      URI: string;
      RawURI: string;
      Components: TStringList;
      Extension: string;
      ParamsOrBookmark: string;
    end;

    TParsedParameters = record
      IndyParams: TIndyParams;
      PageQueryParams: TPageParams;
      ParsedURI: TParsedURI;
    end;

  TPageProducer = class(TCustomPageProducer)
  public
    //Indicates whether handled - hence can gen ancestor classes to handle
    //common situations.
    function ProcessIdCommand(const ParsedParams: TParsedParameters): boolean; virtual; abstract;
  end;

  TPageProducerLogonInfo = class(THTTPLogonInfo)
  private
    FCreated: TDateTime;
    FMostRecentRequest: TDateTime;
  public
    constructor Create; override;
    procedure ReqTimestamp;

    property Created: TDateTime read FCreated;
    property MostRecentRequest: TDateTime read FMostRecentRequest;
    //TODO - Specialisation with respect to logon ephemeral state.
  end;

  //Some default handling for a simple site, including default pages (logon / register),
  //sidebars, and default handling.

  TValidateAction = (vaAllow, vaDenied, vaNotFound, vaRedirect);
  TValidate = record
    Action: TValidateAction;
    RedirectAddress: string;
  end;

  TSimpleSitePageProducer = class(TPageProducer)
  private
  protected
    function DoValidateRequest(const ParsedParams: TParsedParameters;
                               var Validate: TValidate):boolean; virtual;
  public
    //Boolean functions all indicate whether request completely handled.
    function ProcessIdCommand(const ParsedParams: TParsedParameters): boolean; override;
    function GetRequestParam(const PageParams: TPageParams;
                             ParamName: string; var Value: string): boolean;
  end;

  TWellKnownPageType = (wkpLogin, wkpRegister, wkpLogout);

  TRegisterEvent = function(Sender: TObject; Username, CryptKey, CryptPassword:string):boolean of object;
  TLoginEvent = procedure(Sender: TObject; Username: string; ValidateOk: boolean) of object;
  TRequestCryptKeyEvent = function (Sender: TObject; Username: string; var CryptKey, CryptPassword: string): boolean of object;

  TLoginPageProducer = class(TSimpleSitePageProducer)
  private
    FOnLoginRequest: TLoginEvent;
    FOnRegisterRequest: TRegisterEvent;
    FOnCryptKeyRequest: TRequestCryptKeyEvent;
    FLastErrMsg: string;
  protected
    function VerifyPasswordInternal(Username, Password: string): boolean;

    function DoLogin(Username, Password: string): boolean;
    function DoRegister(Username, Password: string): boolean;
    function DoRequestCryptKey(Username: string; var CryptKey, CryptPassword: string):boolean;

    procedure WritePagePreamble(ParsedParams: TParsedParameters);virtual;
    procedure WritePagePostamble(ParsedParams: TParsedParameters);virtual;
    procedure WritePageHeader(ParsedParams: TParsedParameters);virtual;
    procedure WritePageFooter(ParsedParams: TParsedParameters);virtual;

    procedure WriteLoginPage(ParsedParams: TParsedParameters);
    procedure WriteRegisterPage(ParsedParams: TParsedParameters);

    function IsWellKnownPageType(const ParsedParams: TParsedParameters;
                                 var WellKnownType: TWellKnownPageType): boolean;

    function DoValidateRequest(const ParsedParams: TParsedParameters;
                               var Validate: TValidate):boolean; override;

    function DoValidateRequestLoggedOn(const ParsedParams: TParsedParameters;
                                      Session: THTTPDispatcherSession;
                                      var Validate: TValidate): boolean; virtual;

    procedure ChildSetLastErr(LastErr: string);
  public
    function ProcessIdCommand(const ParsedParams: TParsedParameters): boolean; override;

    function VerifyPasswordForApp(Password: string): boolean;

    property OnLoginRequest: TLoginEvent read FOnLoginRequest write FOnLoginRequest;
    property OnCryptKeyRequest: TRequestCryptKeyEvent read FOnCryptKeyRequest write FOnCryptKeyRequest;
    property OnRegisterRequest: TRegisterEvent read FOnRegisterRequest write FOnRegisterRequest;
  end;

  //TODO - TTemplatedPageProducer - for moe complicated site handling,
  //creation of pages from generic templates.

  function HTMLSafeString(Str: string): string;
  function URLSafeString(Str: string): string;

const
  S_MAIN_PAGE = 'main';
  S_LOGIN_PAGE = 'login';

implementation

uses
  SysUtils, HTMLEscapeHelper, IdHMAC, IdCoderMIME, IdHMACSHA1,
  IdSSLOpenSSl, IdGlobal;

{ TPageProducerLogonInfo }

constructor TPageProducerLogonInfo.Create;
begin
  inherited;
  FCreated := Now;
end;

procedure TPageProducerLogonInfo.ReqTimestamp;
begin
  FMostRecentRequest := Now;
end;


{ TPageProducerSession }

procedure TPageProducerSession.AfterSessionStart;
begin
  inherited;
  if not Assigned(FPageProducer) then
  begin
    FPageProducer := Container.PageProducerClass.Create;
    FPageProducer.Session := self;
  end;
  //For the moment, synchronize requests on both a per-session
  //and per-logon basis, so don't need to write any explicit sync code
  //(hopefully!)
  SessionSynchronize := true;
  LogonSynchronize := true;
end;

procedure TPageProducerSession.BeforeSessionEnd;
begin
  FPageProducer.Free;
  FPageProducer := nil;
  inherited;
end;

procedure TPageProducerSession.ProcessIdCommandGet(AContext: TIdContext;
                              ARequestInfo: TIdHTTPRequestInfo;
                              AResponseInfo: TIdHTTPResponseInfo);

  procedure QueryParamValueHandling(const Params: TStringList;
                                    const Values: TStringList);
  var
    Idx: integer;
    EqPos: integer;
  begin
    for Idx := 0 to Pred(ARequestInfo.Params.Count) do
    begin
      EqPos := Pos('=', ARequestInfo.Params[Idx]);
      if EqPos > 0 then
      begin
        Params.Add(ARequestInfo.Params[Idx].Substring(0, Pred(EQPos)));
        Values.Add(ARequestInfo.Params[Idx].Substring(EQPos));
      end
      else
      begin
        Params.Add(ARequestInfo.Params[Idx]);
        Values.Add('');
      end;
    end;
  end;

  procedure QueryParamURIHandling(OriginalURI: string;
                                  URIComponents: TStringList;
                                  var URIExtension: string;
                                  var ParamsOrBookmark: string);
  var
    SepPos, tmp: integer;
    done: boolean;
  begin
    //TODO - expect an uri, but will give up when we get ? or #
    //explicitly should not get // strings (empty URI components).
    if Pos('/', OriginalURI) = 1 then
      OriginalURI := OriginalURI.Substring(1);

    while Length(OriginalURI) > 0 do
    begin
      Done := false;

      SepPos := Succ(Length(OriginalURI));
      tmp := Pos('/', OriginalURI);
      if (tmp > 0) and (tmp <= SepPos) then
        SepPos := tmp;

      tmp := Pos('#', OriginalURI);
      if (tmp > 0) and (tmp <= SepPos) then
      begin
        SepPos := tmp;
        Done := true;
      end;

      tmp := Pos('?', OriginalURI);
      if (tmp > 0) and (tmp <= SepPos) then
      begin
        SepPos := tmp;
        Done := true;
      end;

      URIComponents.Add(OriginalURI.Substring(0, Pred(SepPos)));
      if Done then
      begin
        ParamsOrBookmark := OriginalURI.Substring(Pred(SepPos));
        OriginalURI := '';
      end
      else
        OriginalURI := OriginalURI.Substring(SepPos);
    end;
    if URIComponents.Count > 0 then
    begin
      URIExtension := URIComponents.Strings[Pred(URIComponents.Count)];
      SepPos := Pos('.', URIExtension);
      if SepPos > 0 then
        URIExtension := URIExtension.Substring(Pred(SepPos))
      else
        URIExtension := '';
    end;
  end;

  procedure QueryParamRawURI(var RawURI: string);
  var
    Tmp: string;
    SpacePos: integer;
  begin
    Tmp := ARequestInfo.RawHTTPCommand.Trim;
    SpacePos := Pos(' ', Tmp);
    if SpacePos > 0 then
    begin
      Tmp := Tmp.Substring(SpacePos).Trim;
      SpacePos := Pos(' ', Tmp);
      if SpacePos > 0 then
        Tmp := Tmp.Substring(0, Pred(SpacePos));
    end;
    RawURI := Tmp;
  end;


{$IFOPT C+}
  //Validate local parsing here with other parsing code in other codebases....
  procedure QueryParseMethodsCrossCheck(const ParsedParams: TParsedParameters);
  var
    ParamNames, ParamValues: TStringList;
    BuiltFullFileStr:string;
    Idx: integer;
    ProtoStr, SiteStr, FullFileStr, NameStr, ExtStr: string;
    HasParams: boolean;
  begin
    ParamNames := TStringList.Create;
    ParamValues := TStringList.Create;
    try
      ParseURLEx(ParsedParams.ParsedURI.RawURI,
                 ProtoStr,
                 SiteStr,
                 FullFileStr,
                 NameStr,
                 ExtStr,
                 HasParams,
                 ParamNames,
                 ParamValues);
      Assert(Length(ProtoStr) = 0);
      Assert(Length(SiteStr) = 0);

      //First the params.
      if ParsedParams.IndyParams.ARequestInfo.CommandType = hcGet then
      begin
        Assert(ParsedParams.PageQueryParams.Params.Count
         = ParsedParams.PageQueryParams.Values.Count);
        Assert(ParamNames.Count = ParamValues.Count);
        Assert(ParamNames.Count = ParsedParams.PageQueryParams.Params.Count);
        for Idx := 0 to Pred(ParsedParams.PageQueryParams.Params.Count) do
        begin
          Assert(ParamNames[idx] = ParsedParams.PageQueryParams.Params[idx]);
          Assert(ParamValues[idx] = ParsedParams.PageQueryParams.Values[idx]);
        end;
      end;

      //Now the file name and extension etc.
      if ParsedParams.ParsedURI.Components.Count > 0 then
      begin
        for Idx := 0 to Pred(ParsedParams.ParsedURI.Components.Count) do
          BuiltFullFileStr := BuiltFullFileStr + '/'
            + ParsedParams.ParsedURI.Components[idx];
        Assert(FullFileStr = BuiltFullFileStr);
        Assert(NameStr = ParsedParams.ParsedURI.Components
                [Pred(ParsedParams.ParsedURI.Components.Count)]);
        if Length(ExtStr) > 0 then
          Assert(('.' + ExtStr) = ParsedParams.ParsedURI.Extension);
      end
      else
      begin
        Assert(FullFileStr = '/');
        Assert(NameStr = '');
        Assert(ExtStr = '');
      end;
    finally
      ParamNames.Free;
      ParamValues.Free;
    end;
  end;
{$ENDIF}

var
  ParsedParams: TParsedParameters;
begin
  if Assigned(LogonInfo) and (LogonInfo is TPageProducerLogonInfo) then
    (LogonInfo as TPageProducerLogonInfo).ReqTimestamp;

  ParsedParams.PageQueryParams.Params := TStringList.Create;
  ParsedParams.PageQueryParams.Values := TStringList.Create;
  ParsedParams.ParsedURI.Components := TStringList.Create;
  try
    ParsedParams.IndyParams.AContext := AContext;
    ParsedParams.IndyParams.ARequestInfo := ARequestInfo;
    ParsedParams.IndyParams.AResponseInfo := AResponseInfo;
    QueryParamValueHandling(ParsedParams.PageQueryParams.Params,
                            ParsedParams.PageQueryParams.Values);
    ParsedParams.ParsedURI.Method := ARequestInfo.CommandType;
    ParsedParams.ParsedURI.URI := ARequestInfo.URI;
    QueryParamRawURI(ParsedParams.ParsedURI.RawURI);
    QueryParamURIHandling(ParsedParams.ParsedURI.RawURI,
                          ParsedParams.ParsedURI.Components,
                          ParsedParams.ParsedURI.Extension,
                          ParsedParams.ParsedURI.ParamsOrBookmark);
    //And cross check with other URI / URL parsing code.
  {$IFOPT C+}
    QueryParseMethodsCrossCheck(ParsedParams);
  {$ENDIF}

    if not (FPageProducer as TPageProducer).ProcessIdCommand(ParsedParams) then
      raise EPageProducer.Create('Command not fully handled or rejected: ' + ARequestInfo.URI)
    else
    begin
      //We think we handled it OK, and wrote some content. If no other
      //number then replace with 200.
      if AResponseInfo.ResponseNo = 501 then
        AResponseInfo.ResponseNo := 200;
    end;
  finally
    ParsedParams.PageQueryParams.Params.Free;
    ParsedParams.PageQueryParams.Values.Free;
    ParsedParams.ParsedURI.Components.Free;
  end;
end;

{ TSimpleSitePageProducer }

function TSimpleSitePageProducer.ProcessIdCommand(const ParsedParams: TParsedParameters): boolean;
var
  Validate: TValidate;
begin
  if DoValidateRequest(ParsedParams, Validate) then
  begin
    case Validate.Action of
      vaAllow: result := false; //Not completely handled.
      vaDenied:
      begin
        ParsedParams.IndyParams.AResponseInfo.ResponseNo := 403;
        ParsedParams.IndyParams.AResponseInfo.ResponseText := 'Forbidden.';
        result := true;
      end;
      vaNotFound:
      begin
        ParsedParams.IndyParams.AResponseInfo.ResponseNo := 404;
        ParsedParams.IndyParams.AResponseInfo.ResponseText := 'Not found.';
        result := true;
      end;
      vaRedirect:
      begin
        ParsedParams.IndyParams.AResponseInfo.Redirect(Validate.RedirectAddress);
        ParsedParams.IndyParams.AResponseInfo.ContentText := #13 + #10;
        result := true;
      end
    else
      raise EPageProducer.Create('Bad validation action');
    end;
  end
  else
    raise EPageProducer.Create('Page access not validated.');
end;

function TSimpleSitePageProducer.GetRequestParam(const PageParams: TPageParams;
    ParamName: string; var Value: string): boolean;
var
  Idx: integer;
begin
  //TODO - Should this be case sensitive?
  for Idx := 0 to Pred(PageParams.Params.Count) do
  begin
    if PageParams.Params[idx] = ParamName then
    begin
      Value := PageParams.Values[idx];
      result := true;
      exit;
    end;
  end;
  result := false;
end;


function TSimpleSitePageProducer.DoValidateRequest(const ParsedParams: TParsedParameters;
                           var Validate: TValidate):boolean;
begin
  result := false;
end;

{ TLoginPageProducer }

const
  S_USERNAME = 'USERNAME';
  S_PASSWORD = 'PASSWORD';
  S_PASSWORD_CONFIRM = 'PASSWORD_CONFIRM';
  S_REGISTER_PAGE = 'register';
  S_LOGOUT_PAGE = 'logout';
  FAILED_LOGIN_DELAY = 1000;
  S_LOGIN_FAILED = 'Login failed.';
  S_REGISTRATION_OK = 'Registration OK.';
  S_REGISTRATION_FAILED = 'Registration failed.';

function TLoginPageProducer.IsWellKnownPageType(const ParsedParams: TParsedParameters;
                             var WellKnownType: TWellKnownPageType): boolean;
var
  S: string;
begin
  if ParsedParams.ParsedURI.Components.Count = 1 then
  begin
    S := ParsedParams.ParsedURI.Components.Strings[0];
    if S = S_LOGIN_PAGE then
    begin
      WellKnownType := wkpLogin;
      result := true;
    end
    else if S = S_REGISTER_PAGE then
    begin
      WellKnownType := wkpRegister;
      result := true;
    end
    else if S = S_LOGOUT_PAGE then
    begin
      WellKnownType := wkpLogout;
      result := true;
    end
    else
      result := false;
  end
  else
    result := false;
end;

function TLoginPageProducer.DoValidateRequestLoggedOn(const ParsedParams: TParsedParameters;
                                  Session: THTTPDispatcherSession;
                                  var Validate: TValidate): boolean;
begin
  result := false;
  //Not vouching for anythign past login and register pages once we're logged in.
end;

procedure TLoginPageProducer.ChildSetLastErr(LastErr: string);
begin
  if (Length(FLastErrMsg) = 0) and (Length(LastErr) > 0) then
    FLastErrMsg := LastErr;
end;

function TLoginPageProducer.DoValidateRequest(const ParsedParams: TParsedParameters;
                           var Validate: TValidate):boolean;
var
  WellKnownType: TWellKnownPageType;
  Tmp: string;
begin
  result := inherited;
  if result then
    exit;

  if IsWellKnownPageType(ParsedParams, WellKnownType) then
  begin
    case WellKnownType of
      wkpLogin:
      begin
        //We will allow a GET with 0 params,
        //or a POST with USERNAME, and PASSWORD params only.
        case ParsedParams.ParsedURI.Method of
          hcGET:
          begin
            Validate.Action := vaDenied;
            if ParsedParams.PageQueryParams.Params.Count = 0 then
              Validate.Action := vaAllow;
            result := true;
          end;
          hcPOST:
          begin
            Validate.Action := vaDenied;
            if (ParsedParams.PageQueryParams.Params.Count = 2)
            and GetRequestParam(ParsedParams.PageQueryParams, S_USERNAME, Tmp)
            and GetRequestParam(ParsedParams.PageQueryParams, S_PASSWORD, Tmp) then
                Validate.Action := vaAllow;
            result := true;
          end;
        else
          Validate.Action := vaDenied;
          result := true;
        end;
      end;
      wkpRegister:
      begin
        case ParsedParams.ParsedURI.Method of
          hcGET:
          begin
            Validate.Action := vaDenied;
            result := true;
            if ParsedParams.PageQueryParams.Params.Count = 0 then
              Validate.Action := vaAllow;
          end;
          hcPOST:
          begin
            Validate.Action := vaDenied;
            result := true;
            if (ParsedParams.PageQueryParams.Params.Count = 3)
              and GetRequestParam(ParsedParams.PageQueryParams, S_USERNAME, Tmp)
              and GetRequestParam(ParsedParams.PageQueryParams, S_PASSWORD, Tmp)
              and GetRequestParam(ParsedParams.PageQueryParams, S_PASSWORD_CONFIRM, Tmp) then
                Validate.Action := vaAllow
          end;
        else
          Validate.Action := vaDenied;
          result := true;
        end;
      end;
      wkpLogout:
      begin
        Validate.Action := vaAllow;
        result := true;
      end
    end;
  end
  else
  begin
    if Assigned(Session)
      and Assigned(Session.LogonInfo) then
    begin
      result := DoValidateRequestLoggedOn(ParsedParams,
                                         Session,
                                         Validate);
    end
    else
    begin
      Validate.Action := vaRedirect;
      Validate.RedirectAddress := '/' + S_LOGIN_PAGE;
      result := true;
    end;
  end;
end;

function TLoginPageProducer.ProcessIdCommand(const ParsedParams: TParsedParameters): boolean;
var
  WellKnownType: TWellKnownPageType;
  User, Pass, PassConfirm: string;
begin
  result := inherited;
  if result then
    exit;

  if IsWellKnownPageType(ParsedParams, WellKnownType) then
  begin
    case WellKnownType of
      wkpLogin:
      begin
        case ParsedParams.ParsedURI.Method of
          hcGET:
          begin
            WriteLoginPage(ParsedParams);
            result := true;
          end;
          hcPOST:
          begin
            if GetRequestParam(ParsedParams.PageQueryParams, S_USERNAME, User)
            and GetRequestParam(ParsedParams.PageQueryParams, S_PASSWORD, Pass)
            and DoLogin(User, Pass) then
            begin
              ParsedParams.IndyParams.AResponseInfo.Redirect('/' + S_MAIN_PAGE);
              ParsedParams.IndyParams.AResponseInfo.ContentText := #13 + #10;
            end
            else
            begin
              Sleep(FAILED_LOGIN_DELAY);
              FLastErrMsg := S_LOGIN_FAILED;
              WriteLoginPage(ParsedParams);
            end;
            result := true;
          end;
        end;
      end;
      wkpRegister:
      begin
        case ParsedParams.ParsedURI.Method of
          hcGET:
          begin
            WriteRegisterPage(ParsedParams);
            result := true;
          end;
          hcPOST:
          begin
            if GetRequestParam(ParsedParams.PageQueryParams, S_USERNAME, User)
            and GetRequestParam(ParsedParams.PageQueryParams, S_PASSWORD, Pass)
            and GetRequestParam(ParsedParams.PageQueryParams, S_PASSWORD_CONFIRM, PassConfirm)
            and (Pass = PassConfirm)
            and DoRegister(User, Pass) then
            begin
              ParsedParams.IndyParams.AResponseInfo.Redirect('/' + S_LOGIN_PAGE);
              ParsedParams.IndyParams.AResponseInfo.ContentText := #13 + #10;
              FLastErrMsg := S_REGISTRATION_OK;
            end
            else
            begin
              Sleep(FAILED_LOGIN_DELAY);
              FLastErrMsg := S_REGISTRATION_FAILED;
              WriteRegisterPage(ParsedParams);
            end;
            result := true;
          end;
        end;
      end;
      wkpLogout:
      begin
        Session.LogonId := '';
        ParsedParams.IndyParams.AResponseInfo.Redirect('/'+S_LOGIN_PAGE);
        ParsedParams.IndyParams.AResponseInfo.ContentText := #13 + #10;
        result := true;
      end;
    end;
  end
end;

procedure TLoginPageProducer.WritePagePreamble(ParsedParams: TParsedParameters);
begin
  with ParsedParams.IndyParams.AResponseInfo do
  begin
    ContentText := ContentText + #13 + #10 + #13 + #10 +
      //Need to start with blank line (some buggy browsers)
      '<html><head></head><body>' + #13 + #10;
  end;
end;

procedure TLoginPageProducer.WritePagePostamble(ParsedParams: TParsedParameters);
begin
  with ParsedParams.IndyParams.AResponseInfo do
  begin
    ContentText := ContentText +
      '</body></html>' + #13 + #10;
  end;
end;

procedure TLoginPageProducer.WritePageHeader(ParsedParams: TParsedParameters);
begin
  with ParsedParams.IndyParams.AResponseInfo do
  begin
    ContentText := ContentText + '<table><tr><td><align=left>';
    if Length(FLastErrMsg) > 0 then
      ContentText := ContentText + HTMLSafeString(FLastErrMsg)
    else
    begin
      if Assigned((Session as THTTPDispatcherSession).LogonInfo) then
        ContentText := ContentText + 'Logged in as: ' +
          HTMLSafeString((Session as THTTPDispatcherSession).LogonInfo.LogonId);
    end;
    ContentText := ContentText + '</align></td>'
                               + '<td><align=right>';
    if Assigned((Session as THTTPDispatcherSession).LogonInfo) then
      ContentText := ContentText + '<a href="/'+ S_LOGOUT_PAGE +'">Logout</a>';
    ContentText := ContentText + '</td></tr></table>'
                               + '<hr>' + #13 + #10;
  end;
  FLastErrMsg := '';
end;


procedure TLoginPageProducer.WritePageFooter(ParsedParams: TParsedParameters);
begin
  with ParsedParams.IndyParams.AResponseInfo do
  begin
    ContentText := ContentText +
      '<hr>' + #13 + #10;
  end;
end;


procedure TLoginPageProducer.WriteLoginPage(ParsedParams: TParsedParameters);
begin
  WritePagePreamble(ParsedParams);
  with ParsedParams.IndyParams.AResponseInfo do
  begin
    WritePageHeader(ParsedParams);
    ContentText := ContentText +
      '<center>' +
      '<form action="/'+ S_LOGIN_PAGE +'" method="post">' +
      '<label for="'+S_USERNAME+'">User Name:  </label>' +
      '<input type="text" id="'+S_USERNAME+'" name="'+S_USERNAME+'"><br>' +
      '<br>' +
      '<label for="'+S_PASSWORD+'">Password:  </label>' +
      '<input type="password" id="'+S_PASSWORD+'" name="'+S_PASSWORD+'"><br>' +
      '<br>' +
      '<input type="submit" value="Login">' +
      '</form>' +
      '<br>' +
      '<br>' +
      '<a href="/' + S_REGISTER_PAGE+ '">Register...</a>' +
      '</center>';
    WritePageFooter(ParsedParams);
  end;
  WritePagePostamble(ParsedParams);
end;

procedure TLoginPageProducer.WriteRegisterPage(ParsedParams: TParsedParameters);
begin
  WritePagePreamble(ParsedParams);
  with ParsedParams.IndyParams.AResponseInfo do
  begin
    WritePageHeader(ParsedParams);
    ContentText := ContentText +
      '<center>' +
      '<form action="/'+ S_REGISTER_PAGE +'" method="post">' +
      '<label for="'+S_USERNAME+'">User Name:  </label>' +
      '<input type="text" id="'+S_USERNAME+'" name="'+S_USERNAME+'"><br>' +
      '<br>' +
      '<label for="'+S_PASSWORD+'">Password:  </label>' +
      '<input type="password" id="'+S_PASSWORD+'" name="'+S_PASSWORD+'"><br>' +
      '<br>' +
      '<label for="'+S_PASSWORD_CONFIRM+'">Confirm Password:  </label>' +
      '<input type="password" id="'+S_PASSWORD_CONFIRM+'" name="'+S_PASSWORD_CONFIRM+'"><br>' +
      '<br>' +
      '<input type="submit" value="Register">' +
      '</form>' +
      '<br>' +
      '<br>' +
      '<a href="/' + S_LOGIN_PAGE+ '">Login...</a>' +
      '</center>';
    WritePageFooter(ParsedParams);
  end;
  WritePagePostamble(ParsedParams);
end;

function TLoginPageProducer.DoRequestCryptKey(Username: string; var CryptKey, CryptPassword: string):boolean;
begin
  if Assigned(FOnCryptKeyRequest) then
    result := FOnCryptKeyRequest(self, USerName, CryptKey, CryptPassword)
  else
    result := false;
end;

function TLoginPageProducer.VerifyPasswordInternal(Username, Password: string): boolean;
var
  IdHMAC: TIdHMACSHA256;
  IdCoderMime: TIdDecoderMime;
  CryptKey, CorrectCryptPassword: string;
  KeyBytes, TrialPassBytes, TrialCryptBytes, CorrectCryptBytes: TIdBytes;
begin
  if (Length(Username) = 0) or (Length(Password) = 0) then
    result := false
  else
  begin
    IdHMAC := TIdHMACSHA256.Create;
    IdCoderMime := TIdDecoderMime.Create(nil);
    try
      result := DoRequestCryptKey(Username, CryptKey, CorrectCryptPassword);
      if result then
      begin
        KeyBytes := IdCoderMime.DecodeBytes(CryptKey);
        IdHMAC.Key := KeyBytes;
        //Convert password to TIdBytes
        SetLength(TrialPassBytes, Length(Password) * sizeof(Password[1]));
        Move(Password[1], TrialPassBytes[0], Length(TrialPassBytes));
        //Crypt the password.
        TrialCryptBytes := IdHMac.HashValue(TrialPassBytes);
        CorrectCryptBytes := IdCoderMime.DecodeBytes(CorrectCryptPassword);
        result := (Length(TrialCryptBytes) = Length(CorrectCryptBytes))
          and (CompareMem(@TrialCryptBytes[0], @CorrectCryptBytes[0], Length(TrialCryptBytes)));
      end;
    finally
      IdHMAC.Free;
      IdCoderMime.Free;
    end;
  end;
end;

function TLoginPageProducer.VerifyPasswordForApp(Password: string): boolean;
var
  Username: string;
begin
  //Assumes synchronized with login handling,
  //might require further work depending how you call it.
  if Assigned(Session.LogonInfo) then
    Username := Session.LogonInfo.LogonId;
  result := VerifyPasswordInternal(Username, Password);
end;

function TLoginPageProducer.DoLogin(Username, Password: string): boolean;
begin
  result := VerifyPasswordInternal(Username, Password);
  if result then
    Session.LogonId := Username
  else
    Session.LogonId := '';

  if Assigned(FOnLoginRequest) then
    FOnLoginRequest(self, Username, result);
end;


function TLoginPageProducer.DoRegister(Username, Password: string): boolean;
var
  KeySize, i: integer;
  KeyBytes, PassBytes, CryptBytes: TIdBytes;
  IdHMAC: TIdHMACSHA256;
  CryptKey, CryptPass: string;
  IdCoderMime: TIdEncoderMime;
begin
  if Assigned(FOnRegisterRequest) then
  begin
    IdHMAC := TIdHMACSHA256.Create;
    IdCoderMime := TIdEncoderMIME.Create(nil);
    try
      //Generate random key.
      KeySize := IdHMac.HashSize;
      SetLength(KeyBytes, KeySize);
      for i := 0 to Pred(KeySize) do
          KeyBytes[i] := Random(High(Byte));
      IdHMac.Key := KeyBytes;
      CryptKey := IdCoderMime.EncodeBytes(KeyBytes);
      //Now convert password to TIdBytes.
      SetLength(PassBytes, Length(Password) * sizeof(Password[1]));
      Move(Password[1], PassBytes[0], Length(PassBytes));
      //Crypt the password.
      CryptBytes := IdHMac.HashValue(PassBytes);
      //Store the crypt.
      CryptPass := IdCoderMime.EncodeBytes(CryptBytes);
      result := FOnRegisterRequest(self, Username, CryptKey, CryptPass);
    finally
      IdHMAC.Free;
      IdCoderMime.Free;
    end;
  end
  else
    result := false;
end;

function HTMLSafeString(Str: string): string;
begin
  result := EscapeToHTML(Str);
end;

function URLSafeString(Str: string): string;
begin
  result := URLEncode(Str);
end;

initialization
  IdSSLOpenSSL.LoadOpenSSLLibrary;
end.
