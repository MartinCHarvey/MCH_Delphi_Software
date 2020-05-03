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

  TCheckInMailer = class(TQueuedMailer)
  private
    FEmailName:string;
    FEmailAddress: string;
  public
    function SendAdminMessage(MsgType: TAdminMsgType): boolean;

    property SenderEmailName: string read FEmailName write FEmailName;
    property SenderEmailAddress: string read FEmailAddress write FEmailAddress;
  end;

var
  GCheckInMailer: TCheckInMailer;

implementation

uses
  SysUtils, CheckInAppLogic, IdMessage, IdEmailAddress;

const
  S_ADMIN_MSG = 'CHECKIN: Admin message: ';
  S_SERVER_UP = 'Server up';
  S_SERVER_DOWN = 'Server down';

{ TCheckInMailer }

function TCheckInMailer.SendAdminMessage(MsgType: TAdminMsgType): boolean;
var
  Msg: TIdMessage;
  Recipient: TIdEmailAddressItem;
begin
  Msg := TIdMessage.Create(nil);
  //TODO - properties for these.
  Msg.From.Name := FEmailName;
  Msg.From.Address := FEmailAddress;
  Recipient := Msg.Recipients.Add;
  Recipient.Assign(Msg.From);
  //TODO - Fancy HTML messages for one-click / register / deregister.
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
