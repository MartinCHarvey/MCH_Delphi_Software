unit CheckInPageProducer;

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

uses
  HTTPServerPageProducer, HTTPServerDispatcher, HTTPMisc;

type
  TCheckInPageProducer = class(TLoginPageProducer)
  private
    FOnEndpointRequest: TEndpointRequestEvent;
  protected
    function DoValidateRequest(const ParsedParams: TParsedParameters;
                               var Validate: TValidate):boolean; override;

    function DoValidateRequestLoggedOn(const ParsedParams: TParsedParameters;
                                      Session: THTTPDispatcherSession;
                                      var Validate: TValidate): boolean; override;

    procedure WriteMailActionResult(ParsedParams: TParsedParameters;
                                    OK: boolean; ResponseHint: string);

    procedure WriteEmailChangeForm(const ParsedParams: TParsedParameters;
                                   var R: string; UpdateTag: string);
    procedure WriteQuickCheckInToggle(const ParsedParams: TParsedParameters;
                                      var R: string;
                                      CurrentlyEnabled: boolean);

    procedure WriteRegistrationInfo(const ParsedParams: TParsedParameters);
    procedure WriteUpdateLinks(const ParsedParams: TParsedParameters);
    procedure WriteRefreshLink(const ParsedParams: TParsedParameters);
    procedure WriteHistory(const ParsedParams: TParsedParameters);
    procedure WriteMainPage(const ParsedParams: TParsedParameters);
    procedure WriteCheckInResult(const ParsedParams: TParsedParameters;
                                 DecodeOK: boolean; ResponseHint: string);
    function DoEndpointRequest: string;
  public
    constructor Create; override;
    function ProcessIdCommand(const ParsedParams: TParsedParameters): boolean; override;
    property OnEndpointRequest: TEndpointRequestEvent read FOnEndpointRequest write FOnEndpointRequest;
  end;

const
  S_LOGLESS_CHECKIN = 'CheckIn';
  S_MAIL_ACTION = 'MailAction';
  S_PAD_PARAM = '?Pad=';
  S_ACTION_PARAM = '&Action=';
  S_PAD = 'Pad';
  S_ACTION = 'Action';

implementation

uses
  IdCustomHTTPServer, SysUtils, HTMLEscapeHelper, CheckInAppLogic, Classes,
  CheckInAudit;

const
  S_UPDATE_OWN_EMAIL = 'UpdateOwnEmail';
  S_UPDATE_CONTACT_EMAIL = 'UpdateContactEmail';
  S_DELETE_ACCOUNT = 'DeleteAccount';
  S_NEW_EMAIL = 'NewEmail';
  S_DISABLE_QUICK_CHECKIN = 'DisableQuickCheckin';
  S_DISABLE_BTN = 'Disable';
  S_ENABLE_QUICK_CHECKIN = 'EnableQuickCheckin';
  S_ENABLE_BTN = 'Enable';

  S_EMAIL_CHANGE_FAILED = 'Email address change failed.';
  S_QUICK_CHECKIN_FAILED = 'Set quick check in failed.';
  S_DELETE_ACCOUNT_FAILED = 'Delete account failed.';

constructor TCheckInPageProducer.Create;
begin
  inherited;
  OnLoginRequest := GCheckInApp.HandleLoginRequest;
  OnRegisterRequest := GCheckInApp.HandleRegisterRequest;
  OnCryptKeyRequest := GCheckInApp.HandleCryptKeyRequest;
  OnEndpointRequest := GCheckInApp.HandleEndpointRequest;
end;

function TCheckInPageProducer.DoValidateRequest(const ParsedParams: TParsedParameters;
                           var Validate: TValidate):boolean;
var
  S, Tmp: string;
begin
  //Pages that can be accessed bypassing log-on (at any time).
  result := false;
  case ParsedParams.ParsedURI.Method of
    hcGET:
    begin
      if ParsedParams.ParsedURI.Components.Count = 1 then
      begin
        S := ParsedParams.ParsedURI.Components.Strings[0];
        if S = S_LOGLESS_CHECKIN then
        begin
          if (ParsedParams.PageQueryParams.Params.Count = 1)
          and GetRequestParam(ParsedParams.PageQueryParams, S_PAD, Tmp) then
          begin
            Validate.Action := vaAllow;
            result := true;
          end;
        end
        else if S = S_MAIL_ACTION then
        begin
          if (ParsedParams.PageQueryParams.Params.Count = 2)
          and GetRequestParam(ParsedParams.PageQueryParams, S_PAD, Tmp)
          and GetRequestParam(ParsedParams.PageQueryParams, S_ACTION, Tmp) then
          begin
            Validate.Action := vaAllow;
            result := true;
          end;
        end;
      end
    end;
  end;
  //Standard handling for the rest.
  if not result then
    result := inherited;
end;

function TCheckInPageProducer.DoValidateRequestLoggedOn(const ParsedParams: TParsedParameters;
                                  Session: THTTPDispatcherSession;
                                  var Validate: TValidate): boolean;
var
  S, Tmp: string;
begin
  result := inherited;
  if result then
    exit; //All done.

  case ParsedParams.ParsedURI.Method of
    hcGET:
    begin
      if ParsedParams.ParsedURI.Components.Count = 1 then
      begin
        S := ParsedParams.ParsedURI.Components.Strings[0];
        if S = S_MAIN_PAGE then
        begin
          Validate.Action := vaAllow;
          result := true;
        end
        else
        begin
          Validate.Action := vaNotFound;
          result := true;
        end;
      end
      else
      begin
        Validate.Action := vaRedirect;
        Validate.RedirectAddress := '/' + S_MAIN_PAGE;
        result := true;
      end;
    end;
    hcPOST:
    begin
      Validate.Action := vaDenied;
      result := true;
      if ParsedParams.ParsedURI.Components.Count = 1 then
      begin
        S := ParsedParams.ParsedURI.Components.Strings[0];
        if (S = S_UPDATE_OWN_EMAIL) or (S = S_UPDATE_CONTACT_EMAIL) then
        begin
          if (ParsedParams.PageQueryParams.Params.Count = 1)
          and GetRequestParam(ParsedParams.PageQueryParams, S_NEW_EMAIL, Tmp) then
            Validate.Action := vaAllow;
        end
        else if (S = S_DISABLE_QUICK_CHECKIN)
             or (S = S_ENABLE_QUICK_CHECKIN)
             or (S = S_DELETE_ACCOUNT) then
        begin
          if ParsedParams.PageQueryParams.Params.Count = 0 then
            Validate.Action := vaAllow;
        end
      end;
    end;
  else
    Validate.Action := vaDenied;
    result := true;
  end;
end;

function TCheckInPageProducer.ProcessIdCommand(const ParsedParams: TParsedParameters): boolean;
var
  S, Param1, Param2, ResponseHint: string;
  LogonInfo: TCheckInLogonInfo;
  EmailType: TEmailType;
  OK: boolean;
begin
  result := inherited;
  if result then
    exit; //All done.

  case ParsedParams.ParsedURI.Method of
    hcGET:
    begin
      if ParsedParams.ParsedURI.Components.Count = 1 then
      begin
        S := ParsedParams.ParsedURI.Components.Strings[0];
        if S = S_MAIN_PAGE then
        begin
          WriteMainPage(ParsedParams);
          result := true;
        end
        else if S = S_LOGLESS_CHECKIN then
        begin
          if (ParsedParams.PageQueryParams.Params.Count = 1)
          and GetRequestParam(ParsedParams.PageQueryParams, S_PAD, Param1) then
          begin
            try
              Param1 := URLDecode(Param1);
              OK := true;
            except
              OK := false;
            end;
            if OK then
              OK := GCheckInApp.HandleQuickCheckInRequest(Self, Param1, ResponseHint);
            WriteCheckInResult(ParsedParams, OK, ResponseHint);
            result := true;
          end;
        end
        else if S = S_MAIL_ACTION then
        begin
          if (ParsedParams.PageQueryParams.Params.Count = 2)
          and GetRequestParam(ParsedParams.PageQueryParams, S_PAD, Param1)
          and GetRequestParam(ParsedParams.PageQueryParams, S_ACTION, Param2) then
          begin
            try
              Param1 := URLDecode(Param1);
              OK := true;
            except
              OK := false;
            end;
            if OK then
            begin
              try
                Param2 := URLDecode(Param2);
                OK := true;
              except
                OK := false;
              end;
            end;
            if OK then
              OK := GCheckInApp.HandleMailAction(Self, Param1, Param2, ResponseHint);
            WriteMailActionResult(ParsedParams, OK, ResponseHint);
            result := true;
          end;
        end;
      end
    end;
    hcPOST:
    begin
      if ParsedParams.ParsedURI.Components.Count = 1 then
      begin
        S := ParsedParams.ParsedURI.Components.Strings[0];
        LogonInfo := Session.LogonInfo as TCheckInLogonInfo;
        if (S = S_UPDATE_OWN_EMAIL) or (S = S_UPDATE_CONTACT_EMAIL) then
        begin
          if (ParsedParams.PageQueryParams.Params.Count = 1)
          and GetRequestParam(ParsedParams.PageQueryParams, S_NEW_EMAIL, Param1) then
          begin
            if S = S_UPDATE_OWN_EMAIL then
              EmailType := tetOwn
            else
              EMailType := tetContact;

            if not GCheckInApp.SetEmail(LogonInfo.LogonId, Param1, EmailType) then
              ChildSetLastErr(S_EMAIL_CHANGE_FAILED);

            WriteMainPage(ParsedParams);
            result := true;
          end;
        end
        else if (S = S_DISABLE_QUICK_CHECKIN) or (S = S_ENABLE_QUICK_CHECKIN) then
        begin
          if ParsedParams.PageQueryParams.Params.Count = 0 then
          begin
            if not GCheckInApp.SetQuickCheckin(LogonInfo.LogonId, S = S_ENABLE_QUICK_CHECKIN) then
              ChildSetLastErr(S_QUICK_CHECKIN_FAILED);

            WriteMainPage(ParsedParams);
            result := true;
          end;
        end
        else if (S = S_DELETE_ACCOUNT) then
        begin
          if ParsedParams.PageQueryParams.Params.Count = 0 then
          begin
            if GCheckInApp.DeleteAccount(LogonInfo.LogonId) then
            begin
              //Implicit logout.
              Session.LogonId := '';
              ParsedParams.IndyParams.AResponseInfo.Redirect('/'+ S_LOGIN_PAGE);
              ParsedParams.IndyParams.AResponseInfo.ContentText := #13 + #10;
            end
            else
            begin
              ChildSetLastErr(S_DELETE_ACCOUNT_FAILED);
              WriteMainPage(ParsedParams);
            end;
            result := true;
          end;
        end;
      end;
    end;
  end;
end;


procedure TCheckInPageProducer.WriteEmailChangeForm(const ParsedParams: TParsedParameters;
                               var R: string; UpdateTag: string);
begin
  R := R + '<tr><td></td><td>' +
      '<form action="/'+ UpdateTag +'" method="post">' +
//      '<label for="'+S_NEW_EMAIL+'">Change to:  </label>' +
      '<input type="text" id="'+S_NEW_EMAIL+'" name="'+S_NEW_EMAIL+'">' +
      '<input type="submit" value="Change">' +
      '</form>'
  + '</td></tr>';
end;

procedure TCheckInPageProducer.WriteQuickCheckInToggle(const ParsedParams: TParsedParameters;
                                  var R: string;
                                  CurrentlyEnabled: boolean);
var
  ToggleTag: string;
  ButtonText: string;
begin
  if CurrentlyEnabled then
  begin
    ToggleTag := S_DISABLE_QUICK_CHECKIN;
    ButtonText := S_DISABLE_BTN;
  end
  else
  begin
    ToggleTag := S_ENABLE_QUICK_CHECKIN;
    Buttontext := S_ENABLE_BTN;
  end;
  R := R + '<tr><td></td><td>' +
      '<form action="/'+ ToggleTag +'" method="post">' +
      '<input type="submit" value="' + ButtonText +'">' +
      '</form>'
  + '</td></tr>';
end;


procedure TCheckInPageProducer.WriteRegistrationInfo(const ParsedParams: TParsedParameters);

  function EmailStr(S: string):string;
  begin
    if Length(S) > 0 then
      result := HTMLSafeString(S)
    else
      result := '&lt;None&gt;';
  end;

var
  R: string;
  UserRec: TUserRecord;
  LogonInfo: TCheckInLogonInfo;
  CxLink: string;
begin
  LogonInfo := Session.LogonInfo as TCheckInLogonInfo;
  UserRec := GCheckInApp.ReadUserRecord(LogonInfo.LogonId);
  if Assigned(UserRec) then
  begin
    try
      R := R + '<table>';
      R := R + '<tr><td>' + 'Username: '+ '</td><td>' + HTMLSafeString(UserRec.UserId) + '</td></tr>';
      R := R + '<tr><td><br></td></tr>';

      R := R + '<tr><td>' + 'Last login: '+ '</td><td>' +
        HTMLSafeString(DateTimeToStr(LogonInfo.LastLoginBeforeNow))
        + '</td></tr>';
      R := R + '<tr><td>' + 'Last checkin: '+ '</td><td>' +
        HTMLSafeString(DateTimeToStr(LogonInfo.LastCheckinBeforeNow))
        + '</td></tr>';
      R := R + '<tr><td><br></td></tr>';

      R := R + '<tr><td>' + 'Own E-mail: '+ '</td><td>' + EmailStr(UserRec.OwnEmail) + '</td></tr>';
      WriteEmailChangeForm(ParsedParams, R, S_UPDATE_OWN_EMAIL);
      R := R + '<tr><td><br></td></tr>';

      R := R + '<tr><td>' + 'Contact E-mail: '+ '</td><td>' + EmailStr(UserRec.ContactEmail) + '</td></tr>';
      WriteEmailChangeForm(ParsedParams, R, S_UPDATE_CONTACT_EMAIL);
      R := R + '<tr><td><br></td></tr>';
      R := R + '<tr><td>' + 'Quick check-in link: '+ '</td><td>';
      if Length(UserRec.LoglessCheckInPad) > 0 then
      begin
        R := R + 'Enabled: ';
        CxLink := S_TRANSPORT_PREFIX
          + DoEndpointRequest + '/'
          + S_LOGLESS_CHECKIN + S_PAD_PARAM
          + URLSafeString(UserRec.LoglessCheckInPad);
        R := R + '<a href="'
          + CxLink + '">'
          + CxLink + '</a>';
      end
      else
        R := R + 'Disabled';
      R := R + '</td></tr>';
      WriteQuickCheckInToggle(ParsedParams, R, Length(UserRec.LoglessCheckInPad) > 0);

      R := R + '<tr><td><br></td></tr>';

      if (UserRec.OwnVerifyState = vpsUnverifiedPadForVerify)
        or (UserRec.ContactVerifyState = vpsUnverifiedPadForVerify) then
      begin
        R := R + '<tr><td>';
        if (UserRec.OwnVerifyState = vpsUnverifiedPadForVerify)
          and (UserRec.ContactVerifySTate = vpsUnverifiedPadForVerify) then
          R := R + 'Own and contact emails '
        else if (UserRec.OwnVerifyState = vpsUnverifiedPadForVerify) then
          R := R + 'Own email '
        else if (UserRec.ContactVerifyState = vpsUnverifiedPadForVerify) then
          R := R + 'Contact email ';
        R := R + 'unset or unverified. </td>';
        R := R + '<td>User will be deleted after: ' +
          HTMLSafeString(DateTimeToStr(UserRec.ExpireAfter));
        R := R + '</td></tr>';
      end;

      R := R + '</table>';
    finally
      UserRec.Free;
    end;
  end
  else
    R := 'No user information. Sorry.';
  with ParsedParams.IndyParams.AResponseInfo do
    ContentText := ContentText + R;
end;


procedure TCheckInPageProducer.WriteUpdateLinks(const ParsedParams: TParsedParameters);
var
  R: string;
begin
  R := R + '<table><tr><td>Account actions:</td><td>' +
      '<form action="/'+ S_DELETE_ACCOUNT +'" method="post">' +
      '<input type="submit" value="Delete account">' +
      '</form>'
  + '</td></tr></table>';
  with ParsedParams.IndyParams.AResponseInfo do
    ContentText := ContentText + R;
end;

function CompareLogs(Item1, Item2: Pointer): integer;
var
  L1, L2: TAuditLogEntry;
  X: double;
begin
  L1 := TObject(Item1) as TAuditLogEntry;
  L2 := TObject(Item2) as TAuditLogEntry;
  X := L2.Timestamp - L1.Timestamp;
  if X > 0 then
    result := 1
  else if X < 0 then
    result := -1
  else
    result := 0;
end;

procedure TCheckInPageProducer.WriteHistory(const ParsedParams: TParsedParameters);

var
  R: string;
  LogList: TList;
  LogonInfo: TCheckInLogonInfo;
  AuditLog: TAuditLogEntry;
  i: integer;

begin
  LogonInfo := Session.LogonInfo as TCheckInLogonInfo;
  R := '<table><tr><td>Account History:</td><td></td></tr>';
  R := R + '<tr><td></td><td>';
  R := R + '<table>';
  R := R + '<tr><td>Time</td><td>Username</td><td>Info</td></tr>';
  LogList := TList.Create;
  try
    GCheckInApp.ReadAuditLog(LogonInfo.LogonId, LogList);
    LogList.Sort(CompareLogs);
    for i := 0 to Pred(LogList.Count) do
    begin
      AuditLog := TObject(LogList[i]) as TAuditLogEntry;
      R := R + '<tr><td>'
        + DateTimeToStr(AuditLog.Timestamp) +'</td><td>'
        + AuditLog.UserName + '</td><td>'
        + AuditLog.Details + '</td></tr>';
    end;
  finally
    for i := 0 to Pred(LogList.Count) do
      TObject(LogList[i]).Free;
    LogList.Free;
  end;
  R := R + '</table>';
  R := R + '</td></tr></table>';
  with ParsedParams.IndyParams.AResponseInfo do
    ContentText := ContentText + R;
end;

procedure TCheckInPageProducer.WriteMailActionResult(ParsedParams: TParsedParameters;
                                                     OK: boolean; ResponseHint: string);
var
  S: string;
begin
  with ParsedParams.IndyParams.AResponseInfo do
  begin
    WritePagePreamble(ParsedParams);
    WritePageHeader(ParsedParams);
    S := '<center><h3>';
    if OK then
      S := S + 'Email action successful. ' + HTMLSafeString(ResponseHint)
    else
      S := S + 'EMail action failed. ' + HTMLSafeString(ResponseHint);
    S := S + '<br> <a href="' + S_TRANSPORT_PREFIX
          + DoEndpointRequest + '/'
          + '">Home</a></h3></center>';
    ContentText := ContentText + S;
    WritePageFooter(ParsedParams);
    WritePagePostamble(ParsedParams);
  end;
end;

procedure TCheckInPageProducer.WriteRefreshLink(const ParsedParams: TParsedParameters);
var
  S: string;
begin
  S := '<a href="' + S_TRANSPORT_PREFIX + DoEndpointRequest + '/' + S_MAIN_PAGE+ '">Refresh</a>';
  with ParsedParams.IndyParams.AResponseInfo do
    ContentText := ContentText + S;
end;

procedure TCheckInPageProducer.WriteMainPage(const ParsedParams: TParsedParameters);
begin
  with ParsedParams.IndyParams.AResponseInfo do
  begin
    WritePagePreamble(ParsedParams);
    WritePageHeader(ParsedParams);
    ContentText := ContentText + '<table><tr><td>';
    WriteRefreshLink(ParsedParams);
    ContentText := ContentText + '</td></tr><tr><td>';
    WriteRegistrationInfo(ParsedParams);
    ContentText := ContentText + '</td></tr><tr><td>';
    WriteUpdateLinks(ParsedParams);
    ContentText := ContentText + '</td></tr><tr><td>';
    WriteHistory(ParsedParams);
    ContentText := ContentText + '</td></tr><table>';
    WritePageFooter(ParsedParams);
    WritePagePostamble(ParsedParams);
  end;
end;

procedure TCheckInPageProducer.WriteCheckInResult(const ParsedParams: TParsedParameters;
                                                  DecodeOK: boolean; ResponseHint: string);
var
  S: string;
begin
  with ParsedParams.IndyParams.AResponseInfo do
  begin
    WritePagePreamble(ParsedParams);
    WritePageHeader(ParsedParams);
    S := '<center><h3>';
    if DecodeOK then
      S := S + 'Check in successful. ' + HTMLSafeString(ResponseHint)
    else
      S := S + 'Check in failed. ' + HTMLSafeString(ResponseHint);
    S := S + '<br> <a href="' + S_TRANSPORT_PREFIX
          + DoEndpointRequest + '/'
          + '">Home</a></h3></center>';
    ContentText := ContentText + S;
    WritePageFooter(ParsedParams);
    WritePagePostamble(ParsedParams);
  end;
end;


function TCheckInPageProducer.DoEndpointRequest: string;
begin
  if Assigned(FOnEndpointRequest) then
    result := FOnEndpointRequest(self)
  else
    result := S_LOCALHOST;
end;

end.

