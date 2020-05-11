unit CheckInMailer;
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
  QueuedMailer;

type
  TAdminMsgType = (amtServerUp, amtServerDown);
  TRegReminderType = (rrtOwnerRegister,
                      rrtContactRegister,
                      rrtCheckIn,
                      rrtContactCheckIn);

  TCheckInMailer = class(TQueuedMailer)
  private
    FEmailName:string;
    FEmailAddress: string;
  public
    function SendAdminMessage(MsgType: TAdminMsgType): boolean;
    //Positive action is generally "proceed", and negative action is
    //generally "unsubscribe"
    function SendReminder(MsgType: TRegReminderType;
                          UsrName, UsrEmail, ContEmail,
                          PositiveAction, NegativeAction: string): boolean;

    property SenderEmailName: string read FEmailName write FEmailName;
    property SenderEmailAddress: string read FEmailAddress write FEmailAddress;
  end;

var
  GCheckInMailer: TCheckInMailer;

implementation

uses
  SysUtils, IdMessage, IdEmailAddress, CheckInAppLogic, CheckInAppConfig;

const
  S_ADMIN_MSG = 'CHECKIN: Admin message: ';
  S_SERVER_UP = 'Server up';
  S_SERVER_DOWN = 'Server down';

  S_OWN_REGISTER_SUBJECT = 'CHECKIN: Register your own e-mail address.';
  S_CONTACT_REGISTER_SUBJECT = 'CHECKIN: Register as a contact.';
  S_OWN_CHECKIN_SUBJECT = 'CHECKIN: Remember to checkin!';
  S_CONTACT_CHECKIN_SUBJECT1 = 'CHECKIN: ';
  S_CONTACT_CHECKIN_SUBJECT2 = ' has not checked in!!';

{ TCheckInMailer }

function TCheckInMailer.SendReminder(MsgType: TRegReminderType;
                                      UsrName, UsrEmail, ContEmail,
                                      PositiveAction, NegativeAction: string): boolean;
var
  Msg: TIdMessage;
  Recipient: TIdEmailAddressItem;
begin
  Msg := TIdMessage.Create(nil);
  Msg.From.Name := FEmailName;
  Msg.From.Address := FEmailAddress;
  Recipient := Msg.Recipients.Add;
  case MsgType of
    rrtOwnerRegister, rrtCheckIn:
    begin
      Recipient.Name := UsrEmail;
      Recipient.Address := UsrEmail;
    end;
    rrtContactRegister, rrtContactCheckIn:
    begin
      Recipient.Name := ContEmail;
      Recipient.Address := ContEmail;
    end;
  else
    Assert(false);
  end;
  case MsgType of
    rrtOwnerRegister: Msg.Subject := S_OWN_REGISTER_SUBJECT;
    rrtContactRegister: Msg.Subject := S_CONTACT_REGISTER_SUBJECT;
    rrtCheckIn: Msg.Subject := S_OWN_CHECKIN_SUBJECT;
    rrtContactCheckIn: Msg.Subject :=
      S_CONTACT_CHECKIN_SUBJECT1 + UsrName + ' ' + UsrEmail + S_CONTACT_CHECKIN_SUBJECT2;
  else
    Assert(false);
  end;
  case MsgType of
    rrtOwnerRegister:
    begin
      Msg.Body.Add('Please register your email address and account by following the link below.');
      Msg.Body.Add('');
      Msg.Body.Add(PositiveAction);
      Msg.Body.Add('');
      Msg.Body.Add('');
      Msg.Body.Add('If you''ve forgotten your password, you can delete your account');
      Msg.Body.Add('and start over by following this link.');
      Msg.Body.Add('');
      Msg.Body.Add(NegativeAction);
      Msg.Body.Add('');
      Msg.Body.Add('');
    end;
    rrtContactRegister:
    begin
      Msg.Body.Add(UsrName + ' ' + UsrEmail + ' has asked you to register as a point of contact');
      Msg.Body.Add('if they become unwell and are unable to check-in on this system periodically.');
      Msg.Body.Add('If you agree with this, please follow the link below.');
      Msg.Body.Add('You will be notified by e-mail if they fail to respond to check-in requests.');
      Msg.Body.Add('');
      Msg.Body.Add(PositiveAction);
      Msg.Body.Add('');
      Msg.Body.Add('');
      Msg.Body.Add('If you do not agree, and do not wish to receive these e-mails,');
      Msg.Body.Add('follow the link below.');
      Msg.Body.Add('');
      Msg.Body.Add(NegativeAction);
      Msg.Body.Add('');
      Msg.Body.Add('');
    end;
    rrtCheckIn:
    begin
      Msg.Body.Add('You haven''t checked in recently. Please check-in by following the link below.');
      Msg.Body.Add('');
      Msg.Body.Add(PositiveAction);
      Msg.Body.Add('');
      Msg.Body.Add('');
      Msg.Body.Add('If you wish to delete your account, please follow this link:');
      Msg.Body.Add('');
      Msg.Body.Add(NegativeAction);
      Msg.Body.Add('');
      Msg.Body.Add('');
    end;
    rrtContactCheckIn:
    begin
      Msg.Body.Add(UsrName + ' ' + UsrEmail + ' has not checked in recently,');
      Msg.Body.Add('it might be a good idea to contact them, see if they are OK.');
      Msg.Body.Add('');
      Msg.Body.Add('');
      Msg.Body.Add('If you do not wish to receive these e-mails,');
      Msg.Body.Add('follow the link below.');
      Msg.Body.Add('');
      Msg.Body.Add(NegativeAction);
      Msg.Body.Add('');
      Msg.Body.Add('');
    end;
  else
    Assert(false);
  end;
  result := Self.QueueEmail(Msg);
  if not result then
    Msg.Free;
end;

function TCheckInMailer.SendAdminMessage(MsgType: TAdminMsgType): boolean;
var
  Msg: TIdMessage;
  Recipient: TIdEmailAddressItem;
begin
  Msg := TIdMessage.Create(nil);
  Msg.From.Name := FEmailName;
  Msg.From.Address := FEmailAddress;
  Recipient := Msg.Recipients.Add;
  Recipient.Assign(Msg.From);
  Msg.Encoding := TIdMessageEncoding.mePlainText;
  case MsgType of
    amtServerUp: Msg.Subject := S_ADMIN_MSG + S_SERVER_UP;
    amtServerDown: Msg.Subject := S_ADMIN_MSG + S_SERVER_DOWN;
  else
    Assert(false);
  end;
  Msg.Body.Add(Msg.Subject);
  Msg.Body.Add(DateTimeToStr(Now));
  result := Self.QueueEmail(Msg);
  if not result then
    Msg.Free;
end;

initialization
  GCheckInMailer := TCheckinMailer.Create;
  with GCheckInMailer do
  begin
    Server := GAppConfig.MailerServer;
    Port := GAppConfig.MailerPort;
    Username := GAppConfig.MailerUName;
    Password := GAppConfig.MailerPasswd;
    UseTLS := GAppConfig.MailerUseTLS;
    SenderEmailName := GAppConfig.MailerSenderEmailName;
    SenderEmailAddress := GAppConfig.MailerSenderEmailAddress;
  end;
finalization
  GCheckInMailer.Free;
end.
