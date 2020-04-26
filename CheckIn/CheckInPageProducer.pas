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

//TODO TODO TODO - Make data strings HTML safe when displayed on page.
//TODO - Make generated links properly URL-encoded.

uses
  HTTPServerPageProducer, HTTPServerDispatcher;

type
  TEndpointRequestEvent = function(Sender: TObject):string of object;

  TCheckInPageProducer = class(TLoginPageProducer)
  private
    FOnEndpointRequest: TEndpointRequestEvent;
  protected
    function DoValidateRequest(const ParsedParams: TParsedParameters;
                               var Validate: TValidate):boolean; override;

    function DoValidateRequestLoggedOn(const ParsedParams: TParsedParameters;
                                      Session: THTTPDispatcherSession;
                                      var Validate: TValidate): boolean; override;

    procedure WriteEmailChangeForm(const ParsedParams: TParsedParameters;
                                   var R: string; UpdateTag: string);
    procedure WriteQuickCheckInToggle(const ParsedParams: TParsedParameters;
                                      var R: string;
                                      CurrentlyEnabled: boolean);

    procedure WriteRegistrationInfo(const ParsedParams: TParsedParameters);
    procedure WriteUpdateLinks(const ParsedParams: TParsedParameters);
    procedure WriteHistory(const ParsedParams: TParsedParameters);
    procedure WriteMainPage(const ParsedParams: TParsedParameters);
    procedure WriteCheckInResult(const ParsedParams: TParsedParameters;
                                 DecodeOK: boolean; ResponseHint: string);
    function DoEndpointRequest: string;
  public
    constructor Create; override;
    function ProcessIdCommand(const ParsedParams: TParsedParameters): boolean; override;
  end;


implementation

uses
  IdCustomHTTPServer, CheckInAppLogic, SysUtils, HTMLEscapeHelper;

const
  S_UPDATE_OWN_EMAIL = 'UpdateOwnEmail';
  S_UPDATE_CONTACT_EMAIL = 'UpdateContactEmail';
  S_NEW_EMAIL = 'NewEmail';
  S_DISABLE_QUICK_CHECKIN = 'DisableQuickCheckin';
  S_DISABLE_BTN = 'Disable';
  S_ENABLE_QUICK_CHECKIN = 'EnableQuickCheckin';
  S_ENABLE_BTN = 'Enable';

  S_EMAIL_CHANGE_FAILED = 'Email address change failed.';
  S_QUICK_CHECKIN_FAILED = 'Set quick check in failed.';

  S_LOCALHOST = 'localhost';
  S_CX_LINK_PREFIX = 'http://';

  S_LOGLESS_CHECKIN = 'CheckIn';
  S_PAD_PARAM = '?Pad=';
  S_PAD = 'Pad';

constructor TCheckInPageProducer.Create;
begin
  inherited;
  OnLoginRequest := GCheckInApp.HandleLoginRequest;
  OnRegisterRequest := GCheckInApp.HandleRegisterRequest;
  OnCryptKeyRequest := GCheckInApp.HandleCryptKeyRequest;
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
        end;
      end;
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

  //TODO - This handles all requests, maybe allow subclasses by not handling
  //all requests in future.

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
        else if (S = S_DISABLE_QUICK_CHECKIN) or (S = S_ENABLE_QUICK_CHECKIN) then
        begin
          if ParsedParams.PageQueryParams.Params.Count = 0 then
            Validate.Action := vaAllow;
        end;
        //TODO - S_DELETE_ACCOUNT etc in future.
      end;
    end;
  else
    Validate.Action := vaDenied;
    result := true;
  end;
end;

function TCheckInPageProducer.ProcessIdCommand(const ParsedParams: TParsedParameters): boolean;
var
  S, Tmp, ResponseHint: string;
  LogonInfo: TCheckInLogonInfo;
  EmailType: TEmailType;
  DecodeOK: boolean;
begin
  result := inherited;
  if result then
    exit; //All done.

  //TODO - This handles all requests, maybe allow subclasses by not handling
  //all requests in future.
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
          and GetRequestParam(ParsedParams.PageQueryParams, S_PAD, Tmp) then
          begin
            try
              Tmp := URLDecode(Tmp);
              DecodeOK := true;
            except
              DecodeOK := false;
            end;
            if DecodeOK then
              DecodeOK := GCheckInApp.HandleQuickCheckInRequest(Self, Tmp, ResponseHint);
            WriteCheckInResult(ParsedParams, DecodeOK, ResponseHint);
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
          and GetRequestParam(ParsedParams.PageQueryParams, S_NEW_EMAIL, Tmp) then
          begin
            if S = S_UPDATE_OWN_EMAIL then
              EmailType := tetOwn
            else
              EMailType := tetContact;

            if not GCheckInApp.SetEmail(LogonInfo.LogonId, Tmp, EmailType) then
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
        end;
        //TODO - S_DELETE_ACCOUNT etc in future.
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
        //TODO - HTTP versus HTTPS on logless check-in.
        CxLink := S_CX_LINK_PREFIX
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
begin

end;

procedure TCheckInPageProducer.WriteHistory(const ParsedParams: TParsedParameters);
begin

end;

procedure TCheckInPageProducer.WriteMainPage(const ParsedParams: TParsedParameters);
begin
  with ParsedParams.IndyParams.AResponseInfo do
  begin
    WritePagePreamble(ParsedParams);
    WritePageHeader(ParsedParams);
    ContentText := ContentText + '<table><tr><td>';
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
    S := S + '<br> <a href="' + S_CX_LINK_PREFIX
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
