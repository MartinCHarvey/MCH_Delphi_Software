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
  HTTPServerPageProducer, HTTPServerDispatcher;

type
  TCheckInPageProducer = class(TLoginPageProducer)
  protected
    //TODO - Better page header / status handling.
    function DoValidateRequestLoggedOn(const ParsedParams: TParsedParameters;
                                      Session: THTTPDispatcherSession;
                                      var Validate: TValidate): boolean; override;

    procedure WriteEmailChangeForm(ParsedParams: TParsedParameters;
                                   var R: string; UpdateTag: string);
    procedure WriteQuickCheckInToggle(ParsedParams: TParsedParameters;
                                      var R: string;
                                      CurrentlyEnabled: boolean);

    procedure WriteRegistrationInfo(ParsedParams: TParsedParameters);
    procedure WriteUpdateLinks(ParsedParams: TParsedParameters);
    procedure WriteHistory(ParsedParams: TParsedParameters);
    procedure WriteMainPage(const ParsedParams: TParsedParameters);
  public
    constructor Create; override;
    function ProcessIdCommand(const ParsedParams: TParsedParameters): boolean; override;
  end;


implementation

uses
  IdCustomHTTPServer, CheckInAppLogic, SysUtils;

const
  S_UPDATE_OWN_EMAIL = 'UpdateOwnEmail';
  S_UPDATE_CONTACT_EMAIL = 'UpdateContactEmail';
  S_NEW_EMAIL = 'NewEmail';
  S_DISABLE_QUICK_CHECKIN = 'DisableQuickCheckin';
  S_DISABLE_BTN = 'Disable';
  S_ENABLE_QUICK_CHECKIN = 'EnableQuickCheckin';
  S_ENABLE_BTN = 'Enable';

constructor TCheckInPageProducer.Create;
begin
  inherited;
  OnLoginRequest := GCheckInApp.HandleLoginRequest;
  OnRegisterRequest := GCheckInApp.HandleRegisterRequest;
  OnCryptKeyRequest := GCheckInApp.HandleCryptKeyRequest;
end;

function TCheckInPageProducer.DoValidateRequestLoggedOn(const ParsedParams: TParsedParameters;
                                  Session: THTTPDispatcherSession;
                                  var Validate: TValidate): boolean;
var
  S: string;
begin
  result := inherited;
  if result then
    exit; //All done.

  //First of all, allow get requests for specific pages, no params.
  if ParsedParams.ParsedURI.Components.Count = 1 then
  begin
    if ParsedParams.ParsedURI.Method = hcGET then
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
      Validate.Action := vaDenied;
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

function TCheckInPageProducer.ProcessIdCommand(const ParsedParams: TParsedParameters): boolean;
begin
  result := inherited;
  if result then
    exit; //All done.

  if ParsedParams.ParsedURI.Components.Strings[0] = S_MAIN_PAGE then
  begin
    WriteMainPage(ParsedParams);
    result := true;
  end;
end;

{
    property UserId: string read FUserId;
    property OwnEmail: string read FOwnEmail;
    property OwnVerifyState:TVerifyPadState read FOwnVerifyState;
    property OwnEmailPad: string read FOwnEmailPad;
    property ContactEmail: string read FContactEmail;
    property ContactVerifySTate: TVerifyPadState read FContactVerifyState;
    property ContactPad: string read FContactPad;
    property LoglessCheckInPad: string read FLoglessCheckInPad;

    property NextPeriodic: TDateTime read FNextPeriodic;
    property ExpireAfter: TDateTime read FExpireAfter;
    property LastLogin: TDateTime read FLastLogin;
    property LastCheckin: TDateTime read FLastCheckin;
}
procedure TCheckInPageProducer.WriteEmailChangeForm(ParsedParams: TParsedParameters;
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

procedure TCheckInPageProducer.WriteQuickCheckInToggle(ParsedParams: TParsedParameters;
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


procedure TCheckInPageProducer.WriteRegistrationInfo(ParsedParams: TParsedParameters);

  function EmailStr(S: string):string;
  begin
    if Length(S) > 0 then
      result := S
    else
      result := '&lt;None&gt;';
  end;

var
  R: string;
  UserRec: TUserRecord;
  LogonInfo: TCheckInLogonInfo;
begin
  LogonInfo := Session.LogonInfo as TCheckInLogonInfo;
  UserRec := GCheckInApp.ReadUserRecord(LogonInfo.LogonId);
  if Assigned(UserRec) then
  begin
    try
      R := R + '<table>';
  //    R := R + '<tr><td>' + + '</td><td>' + + '</td></tr>';
      R := R + '<tr><td>' + 'Username: '+ '</td><td>' + UserRec.UserId+ '</td></tr>';
      R := R + '<tr><td><br></td></tr>';

      R := R + '<tr><td>' + 'Last login: '+ '</td><td>' + DateTimeToStr(LogonInfo.LastLoginBeforeNow) + '</td></tr>';
      R := R + '<tr><td>' + 'Last checkin: '+ '</td><td>' + DateTimeToStr(LogonInfo.LastCheckinBeforeNow) + '</td></tr>';
      R := R + '<tr><td><br></td></tr>';

//      R := R + '<tr><td>' + 'Expires after: '+ '</td><td>' + DateTimeToStr(UserRec.ExpireAfter) + '</td></tr>';
      R := R + '<tr><td>' + 'Own E-mail: '+ '</td><td>' + EmailStr(UserRec.OwnEmail) + '</td></tr>';
      WriteEmailChangeForm(ParsedParams, R, S_UPDATE_OWN_EMAIL);
      R := R + '<tr><td><br></td></tr>';

      R := R + '<tr><td>' + 'Contact E-mail: '+ '</td><td>' + EmailStr(UserRec.ContactEmail) + '</td></tr>';
      WriteEmailChangeForm(ParsedParams, R, S_UPDATE_CONTACT_EMAIL);
      R := R + '<tr><td><br></td></tr>';
      R := R + '<tr><td>' + 'Quick check-in link: '+ '</td><td>';
      if Length(UserRec.LoglessCheckInPad) > 0 then
        R := R + 'Enabled.'
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
        R := R + '<td>User will be deleted after: ' + DateTimeToStr(UserRec.ExpireAfter);
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

procedure TCheckInPageProducer.WriteUpdateLinks(ParsedParams: TParsedParameters);
begin

end;

procedure TCheckInPageProducer.WriteHistory(ParsedParams: TParsedParameters);
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


end.
