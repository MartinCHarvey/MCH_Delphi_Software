unit CheckInAppLogic;

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

{
  DB handling, app logic / anything else that isn't HTML'y.
  Might need splitting up later.
}

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SysUtils, MemDBAPI, HTTPServerPageProducer, Classes, MemDB;

type
  TUserRecord = class;

  TEmailType = (tetOwn, tetContact);

  //Should not be any state in this class - this just does app logic.
{$IFDEF USE_TRACKABLES}
  TCheckInApp = class(TTrackable)
{$ELSE}
  TCheckInApp = class
{$ENDIF}
  private
    FLastPeriodicAllTasks: TDateTime;
  protected
    procedure DBStart;
    procedure DBStop;
    function GenPad: string;

    procedure UpdateRegistrationTimers(UTable: TMemAPITableData; Initial: boolean);
    procedure UpdateCheckInTimers(UTable: TMemAPITableData);

    function UserPeriodicActions(T: TMemDBTransaction; UTable: TMemAPITableData): boolean;
    function UserVerificationActions(T: TMemDBTransaction; UTable: TMemAPITableData; StartUserRecord: TUserRecord): boolean;
    function UserCheckInActions(T: TMemDBTransaction; UTable: TMemAPITableData;StartUserRecord: TUserRecord): boolean;

    procedure AuditPersistRecentToDB(Sender: TObject; const LogEntries: TList);
    procedure AuditGetItemsForPrune(Sender: TObject; const LogEntries: TList; Before: TDateTime);
    procedure AuditGetLastPruneTime(Sender: TObject; var LastPrune: TDateTime);

    function AddToBlackListInTrans(T: TMemDBTransaction; Address: string): boolean;
    function EmailBlackListedInTrans(T: TMemDBTransaction; Address: string): boolean;
  public
    constructor Create;
    destructor Destroy; override;
    function HandleEndpointRequest(Sender: TObject):string;
    function HandleRegisterRequest(Sender: TObject; Username, CryptKey, CryptPassword:string):boolean;
    function HandleCryptKeyRequest(Sender: TObject; Username: string; var CryptKey, CryptPassword: string): boolean;
    procedure HandleLoginRequest(Sender: TObject; Username: string; ValidateOk: boolean);
    function SetEmail(Username: string; NewEmail: string; EmailType: TEmailType): boolean;
    function SetQuickCheckin(Username: string; Enable: boolean): boolean;
    function ReadUserRecord(Username: string): TUserRecord;
    function DeleteAccount(USername: string): boolean;
    function HandleQuickCheckInRequest(PageProducer:TPageProducer; Pad: string; var ResInfo: string): boolean;
    function HandleMailAction(PageProducer: TPageProducer; Pad: string; Action: string; var ResInfo: string): boolean;
    procedure ReadAuditLog(Username: string; LogList: TList);

    function ClearBlacklist: boolean;
    procedure DoPeriodic;
  end;


  TVerifyPadState = (vpsUnverifiedPadForVerify, vpsVerifiedPadForUnsub);

  //Can check in either with mail pad, or alternatively,
  //logless check-in pad. One requires action field, other does not.
  TLinkActionType = (latMailConfirm,
                     latAccountDelete,
                     latMailBlacklist,
                     latCheckIn);

  //Eveything but the password crypts.
{$IFDEF USE_TRACKABLES}
  TUserRecord = class(TTrackable)
{$ELSE}
  TUserRecord = class
{$ENDIF}
  private
    FUserId: string;

    FOwnEmail: string;
    FOwnVerifyState: TVerifyPadState; //Re-gen a pad whenevr u change this.
    FOwnEmailPad: string;


    FContactEmail: string;
    FContactVerifyState: TVerifyPadState;
    FContactPad: string;

    FLoglessCheckInPad: string;

    FNextPeriodic: TDateTime;
    FLastLogin, FLastCheckin: TDateTime;
    FExpireAfter, FNextRegisterRemind, FNextCheckinRemind, FStopCheckinRemind,
    FNextContactCheckinRemind: TDateTime;
  public
    function GenLink(EmailPadSelect: TEmailType;
                     LinkAction: TLinkActionType): string;

    procedure FromUTable(UTable: TMemAPITableData);
    property UserId: string read FUserId;
    property OwnEmail: string read FOwnEmail;
    property OwnVerifyState:TVerifyPadState read FOwnVerifyState;
    property OwnEmailPad: string read FOwnEmailPad;
    property ContactEmail: string read FContactEmail;
    property ContactVerifySTate: TVerifyPadState read FContactVerifyState;
    property ContactPad: string read FContactPad;
    property LoglessCheckInPad: string read FLoglessCheckInPad;

    property NextPeriodic: TDateTime read FNextPeriodic;

    property LastLogin: TDateTime read FLastLogin;
    property LastCheckin: TDateTime read FLastCheckin;

    property ExpireAfter: TDateTime read FExpireAfter;
    property NextRegisterRemind: TDateTime read FNextRegisterRemind write FNextRegisterRemind;
    property NextCheckinRemind: TDateTime read FNextCheckinRemind write FNextCheckinRemind;
    property NextContactCheckinRemind: TDateTime read FNextContactCheckinRemind write FNextContactCheckinRemind;
    property StopCheckinRemind: TDateTime read FStopCheckinRemind write FStopCheckinRemind;
  end;

  TCheckInLogonInfo = class(TPageProducerLogonInfo)
  private
    FLastLoginBeforeNow: TDateTime;
    FLastCheckinBeforeNow: TDateTime;
  public
    property LastLoginBeforeNow: TDateTime read FLastLoginBeforeNow;
    property LastCheckinBeforeNow: TDateTime read FLastCheckinBeforeNow;
  end;

var
  GCheckInApp: TCheckInApp;

implementation

uses
  IOUtils, MemDBMisc, GlobalLog, CheckInPageProducer,
  HTTPServerDispatcher, IdHMACSha1, IdGlobal, IdCoderMIME, CheckInMailer,
  CheckInAppConfig, CheckInAudit;

{ ---------------- Config / DB init and finalization ------------- }

const
  S_DB_INIT_FATAL = 'DB init failed. Throw in unit init: fatal.';

  { ------------------ User table }
  S_USERTABLE = 'UserTable';
  //Crucial login and next-check state.
  S_USERID = 'UserId';
  S_IDX_USERID = 'IdxUserId';
  S_PASS_HMAC_KEY = 'PassHMacKey';
  S_PASS_CRYPT = 'PassCrypt';
  S_NEXT_PERIODIC = 'NextPeriodic';
  S_IDX_NEXT_PERIODIC = 'IdxNextPeriodic';
  //Further validation etc information.

  S_OWN_EMAIL = 'OwnEmail';
  //S_IDX_OWN_EMAIL = 'IdxOwnEmail';
  S_OWN_VERIFY_STATE = 'OwnVerifyState';
  S_OWN_EMAIL_PAD = 'OwnEmailPad';
  S_IDX_OWN_EMAIL_PAD = 'IdxOwnEmailPad';
  //S_IDX_OWN_EMAIL_PAD = 'IdxOwnEmailPad';

  S_CONTACT_EMAIL = 'ContactEmail';
  //S_IDX_CONTACT_EMAIL = 'IdxContactEmail';
  S_CONTACT_VERIFY_STATE = 'ContactVerifyState';
  S_CONTACT_EMAIL_PAD = 'ContactEmailPad';
  S_IDX_CONTACT_EMAIL_PAD = 'IdxContactEmailPad';
  //S_IDX_CONTACT_EMAIL_PAD = 'IdxContactEmailPad';

  S_LOGLESS_CHECKIN_PAD = 'LoglessCheckInPad';
  S_IDX_LOGLESS_CHECKIN_PAD = 'LoglessCheckInPad';

  S_LAST_LOGIN = 'LastLogin';
  S_LAST_CHECKIN = 'LastCheckin';

  S_EXPIRE_AFTER = 'ExpireAfter';
  S_NEXT_REGISTER_REMIND = 'NextRegisterRemind';
  S_NEXT_CHECKIN_REMIND = 'NextCheckinRemind';
  S_NEXT_CONTACT_CHECKIN_REMIND = 'NextContactCheckinRemind';
  S_STOP_CHECKIN_REMIND = 'StopCheckinRemind';

  { ------------------ Audit table }
  S_AUDIT_TABLE = 'AuditTable';

  S_AUDIT_USERID = 'AuditUserid';
  S_IDX_AUDIT_USERID = 'IdxAuditUserId';
  S_AUDIT_DATETIME = 'AuditDateTime';
  S_IDX_AUDIT_DATETIME = 'IdxAuditDateTime';
  S_AUDIT_DETAILS = 'AuditDetails';

  { ------------------ Blacklist table }
  S_BLACKLIST_TABLE = 'BlacklistTable';
  S_BLACKLIST_DATETIME = 'BlacklistDateTime';
  S_BLACKLIST_EMAIL = 'BlacklistEmail';
  S_IDX_BLACKLIST_EMAIL = 'IdxBlacklistEmail';

  { ------------------ Assorted user messages. }

  S_USERPASS_TOO_SIMPLE = 'Username or password too simple';
  S_USER_ALREADY_REG = 'User already registered.';
  S_USER_NOT_FOUND = 'User not found.';
  S_INTERNAL_CRYPT = 'Internal error reading crypto from database.';
  S_LOGIN = 'Login attempt for username: ';
  S_REGISTER = 'Register request for username: ';
  S_CRYPT_KEY_REQUEST = 'Crypt key request for username: ';
  S_NO_CHECK_IN_KEY = 'No check-in key.';
  S_CHECK_IN_KEY_NOT_FOUND = 'Check-in key not found.';
  S_CHECKIN_REQUEST = 'Checkin request: ';
  S_INTERNAL_ERROR = 'Internal error.';
  S_SET_EMAIL = 'Set e-mail:';
  S_SET_QUICK_CHECKIN = 'Set quick checkin: ';
  S_EXCEPTION_IN_PERIODIC_HANDLING = 'Exception in periodic handling: ';
  S_OWNER_REGISTER_REMIND_FAILED = 'Owner registration reminder failed: ';
  S_CONTACT_REGISTER_REMIND_FAILED = 'Contact registration reminder failed: ';
  S_OWN_CHECKIN_REMIND_FAILED = 'Checkin reminder failed: ';
  S_CONTACT_CHECKIN_REMIND_FAILED = 'Contact checkin reminder failed: ';

  S_ACTION_MAIL_CONFIRM = 'MailConfirm';
  S_ACTION_ACCOUNT_DELETE = 'AccountDelete';
  S_ACTION_BLACKLIST = 'Blacklist';
  S_ACTION_CHECK_IN = 'CheckIn';

  S_ACTION_NOT_SUPPORTED = 'Action not supported';
  S_NOT_YET_IMPLEMENTED = 'Not yet implemented';

  S_AUDIT_LOG_WRITE_FAILED = 'Audit log write failed: ';
  S_AUDIT_LOG_PRUNE_FAILED = 'Audit log prune failed: ';
  S_AUDIT_LOG_QUERY_PRUNE_TIME_FAILED = 'Audit log query prune time failed: ';

  S_AUDIT_INACTIVE_USER_EXPIRED = 'Inactive user expired.';
  S_AUDIT_CHECKIN_REMINDER_SEND_FAILED = 'Checkin reminder failed.';
  S_AUDIT_CHECKIN_REMINDER_SENT = 'Checkin reminder sent.';
  S_AUDIT_CONTACT_REMINDER_SEND_FAILED = 'Reminder to contact failed.';
  S_AUDIT_CONTACT_REMINDER_SENT = 'Reminder to contact sent.';
  S_AUDIT_UNVERIFIED_USER_EXPIRED = 'User with unverified addresses expired.';
  S_AUDIT_VERIFY_ACCOUNT_SEND_FAILED = 'Account verification reminder failed.';
  S_AUDIT_VERIFY_ACCOUNT_SENT = 'Account verification reminder sent.';
  S_AUDIT_VERIFY_CONTACT_SEND_FAILED = 'Contact verification reminder failed.';
  S_AUDIT_VERIFY_CONTACT_SENT = 'Contact verification reminder sent.';
  S_AUDIT_CRYPT_KEY_REQUEST_EXCEPTION = 'Login crypto key request exception.';
  S_AUDIT_REGISTRATION_REQUEST = 'Registration request.';
  S_AUDIT_REGISTRATION_REQUEST_OK = 'Account registered.';
  S_AUDIT_REGISTRATION_FAILED = 'Registration request failed.';
  S_AUDIT_REGISTRATION_EXCEPTION = 'Registration request exception.';
  S_AUDIT_LOGIN_REQUEST_OK = 'Logged in.';
  S_AUDIT_LOGIN_REQUEST_DENIED = 'Login failed.';
  S_AUDIT_LOGIN_REQUEST_EXCEPTION = 'Login request exception.';
  S_AUDIT_ACCOUNT_EMAIL_VERIFIED = 'Account email verified';
  S_AUDIT_ACCOUNT_DELETED_VIA_EMAIL = 'Account deleted via email link';
  S_AUDIT_ACCOUNT_DELETED_WEBUI = 'Account deleted via webUI';
  S_AUDIT_ACCOUNT_CHECKED_IN = 'Account checked in';
  S_AUDIT_CONTACT_EMAIL_VERIFIED = 'Contact email verified';
  S_AUDIT_ACCOUNT_QUICK_CHECKIN = 'Account checked in with quick checkin link.';
  S_AUDIT_CHANGED_OWN_EMAIL = 'Account email changed';
  S_AUDIT_CHANGED_CONTACT_EMAIL = 'Contact email changed.';
  S_AUDIT_SET_EMAIL_EXCEPTION = 'Exception setting e-mail';
  S_AUDIT_SET_QUICK_CHECKIN_EXCEPTION = 'Exception in quick checkin';
  S_AUDIT_ACCOUNT_DELETE_EXCEPTION = 'Account delete exception.';

  S_MAIL_AUDIT = '__Email: ';
  S_AUDIT_EMAIL_BLACKLISTED = 'Email added to blacklist';
  S_AUDIT_EMAIL_BLACKLIST_FAILED = 'Unable to add email to blacklist.';
  S_AUDIT_SET_MAIL_FAILED_BLACKLISTED = 'Cannot set email; it has been blacklisted.';
  S_AUDIT_SEND_MAIL_FAILED_BLACKLISTED = 'Cannot send to email; it has been blacklisted.';
  S_AUDIT_READ_AUDIT_LOG_FAILED = 'Read audit log failed.';

  S_BLACKLIST_ADD_FAILED = 'Failed to add e-mail to blacklist table.';
  S_BLACKLIST_CHECK_FAILED = 'Failed to check e-mail in blacklist table.';
  S_CLEARING_BLACKLIST = 'Clearing blacklist';
  S_BLACKLIST_CLEAR_FAILED = 'Failed to clear blacklist: ';

  S_LOCALHOST = 'localhost';

const
  ONE_MINUTE = (1 / (24 * 60));
{$IFDEF DEBUG}
  SYSTEM_PERIODIC_CHECK_INTERVAL = ONE_MINUTE;
  USER_PERIODIC_CHECK_INTERVAL = 3 * ONE_MINUTE;
  USER_REGISTER_NOTIFY_INTERVAL = 15 * ONE_MINUTE;
  USER_REGISTER_EXPIRE_INTERVAL =  60 * ONE_MINUTE;
  USER_CHECKIN_NOTIFY_INTERVAL = 15 * ONE_MINUTE;
  USER_CHECKIN_STOP_INTERVAL = 1;
  USER_INACTIVE_EXPIRE_INTERVAL = 7;
{$ELSE}
  SYSTEM_PERIODIC_CHECK_INTERVAL = 15 * ONE_MINUTE;
  USER_PERIODIC_CHECK_INTERVAL = 0.5;
  USER_REGISTER_NOTIFY_INTERVAL = 2;
  USER_REGISTER_EXPIRE_INTERVAL =  7;
  USER_CHECKIN_NOTIFY_INTERVAL = 1;
  USER_CHECKIN_STOP_INTERVAL = 7;
  USER_INACTIVE_EXPIRE_INTERVAL = 31;
{$ENDIF}
  CONTACT_CHECKIN_NOTIFY_INTERVAL = USER_CHECKIN_NOTIFY_INTERVAL * 2;
  USER_REGISTER_INITIAL_NOTIFY_INTERVAL = ONE_MINUTE;


var
  MemDB: TMemDB;

{ ---------------- Proper app logic  ------------- }

{ TUserRecord }

function TUserRecord.GenLink(EmailPadSelect: TEmailType;
                             LinkAction: TLinkActionType):string;
var
  Host: string;
  Pad: string;
  Action: string;
begin
  Host := GCheckInApp.HandleEndpointRequest(self);
  case EmailPadSelect of
    tetOwn: Pad := OwnEmailPad;
    tetContact: Pad := ContactPad;
  else
    Assert(false);
  end;
  case LinkAction of
    latMailConfirm: Action := S_ACTION_MAIL_CONFIRM;
    latAccountDelete: Action := S_ACTION_ACCOUNT_DELETE;
    latMailBlacklist: Action := S_ACTION_BLACKLIST;
    latCheckIn: Action := S_ACTION_CHECK_IN;
  else
    Assert(false);
  end;
  Pad := URLSafeString(Pad);
  Action := URLSafeString(Action);
  //TODO - Deal with HTTPS.
  result := S_PROTO_LINK_PREFIX + Host + '/' + S_MAIL_ACTION + S_PAD_PARAM
    + Pad + S_ACTION_PARAM + Action;
end;

procedure TUserRecord.FromUTable(UTable: TMemAPITableData);
var
  DataRec: TMemDBFieldDataRec;
begin
  UTable.ReadField(S_USERID, DataRec);
  Assert(DataRec.FieldType = ftUnicodeString);
  FUserId := DataRec.sVal;

  UTable.ReadField(S_OWN_EMAIL, DataRec);
  Assert(DataRec.FieldType = ftUnicodeString);
  FOwnEmail := DataRec.sVal;

  UTable.ReadField(S_OWN_EMAIL_PAD, DataRec);
  Assert(DataRec.FieldType = ftUnicodeString);
  FOwnEmailPad := DataRec.sVal;

  UTable.ReadField(S_CONTACT_EMAIL, DataRec);
  Assert(DataRec.FieldType = ftUnicodeString);
  FContactEmail := DataRec.sVal;

  UTable.ReadField(S_CONTACT_EMAIL_PAD, DataRec);
  Assert(DataRec.FieldType = ftUnicodeString);
  FContactPad := DataRec.sVal;

  UTable.ReadField(S_OWN_VERIFY_STATE, DataRec);
  Assert(DataRec.FieldType = ftInteger);
  FOwnVerifyState := TVerifyPadState(DataRec.i32Val);

  UTable.ReadField(S_CONTACT_VERIFY_STATE, DataRec);
  Assert(DataRec.FieldType = ftInteger);
  FContactVerifyState := TVerifyPadState(DataRec.i32Val);

  UTable.ReadField(S_LOGLESS_CHECKIN_PAD, DataRec);
  Assert(DataRec.FieldType = ftUnicodeString);
  FLoglessCheckInPad := DataRec.sVal;

  UTable.ReadField(S_NEXT_PERIODIC, DataRec);
  Assert(DataRec.FieldType = ftDouble);
  FNextPeriodic := DataRec.dVal;

  UTable.ReadField(S_EXPIRE_AFTER, DataRec);
  Assert(DataRec.FieldType = ftDouble);
  FExpireAfter := DataRec.dVal;

  UTable.ReadField(S_LAST_LOGIN, DataRec);
  Assert(DataRec.FieldType = ftDouble);
  FLastLogin := DataRec.dVal;

  UTable.ReadField(S_LAST_CHECKIN, DataRec);
  Assert(DataRec.FieldType = ftDouble);
  FLastCheckin := DataRec.dVal;

  UTable.ReadField(S_NEXT_REGISTER_REMIND, DataRec);
  Assert(DataRec.FieldType = ftDouble);
  FNextRegisterRemind := DataRec.dVal;

  UTable.ReadField(S_NEXT_CHECKIN_REMIND, DataRec);
  Assert(DataRec.FieldType = ftDouble);
  FNextCheckinRemind := DataRec.dVal;

  UTable.ReadField(S_NEXT_CONTACT_CHECKIN_REMIND, DataRec);
  Assert(DataRec.FieldType = ftDouble);
  FNextContactCheckinRemind := DataRec.dVal;

  UTable.ReadField(S_STOP_CHECKIN_REMIND, DataRec);
  Assert(DataRec.FieldType = ftDouble);
  FStopCheckinRemind := DataRec.dVal;
end;

{ TCheckInApp }

function TCheckInApp.UserCheckInActions(T: TMemDBTransaction; UTable: TMemAPITableData; StartUserRecord: TUserRecord): boolean;
var
  RightNow: TDateTime;
  DataRec: TMemDbFieldDataRec;
  Blacklisted: boolean;
begin
  RightNow := Now;
  result := (RightNow > StartUserRecord.LastLogin + USER_INACTIVE_EXPIRE_INTERVAL)
   and (RightNow > StartUserRecord.LastCheckin + USER_INACTIVE_EXPIRE_INTERVAL);
  if result then
  begin
    AudLog(StartUserRecord.UserId, S_AUDIT_INACTIVE_USER_EXPIRED);
    UTable.Discard;
    UTable.Delete;
  end
  else
  begin
    if not (RightNow > StartUserRecord.StopCheckinRemind) then
    begin
      if RightNow > StartUserRecord.NextCheckInRemind then
      begin
        BlackListed := EmailBlackListedInTrans(T, StartUserRecord.OwnEmail);
        if Blacklisted then
          AudLog(StartUserRecord.UserId, S_AUDIT_SEND_MAIL_FAILED_BLACKLISTED);
        if Blacklisted or not GCheckInMailer.SendReminder(rrtCheckIn,
                                           StartUserRecord.UserId,
                                           StartUserRecord.OwnEmail,
                                           StartUserRecord.ContactEmail,
                                           StartUserRecord.GenLink(tetOwn, latCheckIn),
                                           StartUserRecord.GenLink(tetOwn, latAccountDelete)) then
        begin
          AudLog(StartUserRecord.UserId, S_AUDIT_CHECKIN_REMINDER_SEND_FAILED);
          GLogLog(SV_WARN, S_OWN_CHECKIN_REMIND_FAILED + StartUserRecord.UserId);
        end
        else
          AudLog(StartUserRecord.UserId, S_AUDIT_CHECKIN_REMINDER_SENT);

        DataRec.FieldType := ftDouble;
        DataRec.dVal := RightNow + USER_CHECKIN_NOTIFY_INTERVAL;
        UTable.WriteField(S_NEXT_CHECKIN_REMIND, DataRec);
      end;
      if RightNow > StartUserRecord.NextContactCheckinRemind then
      begin
        BlackListed := EmailBlackListedInTrans(T, StartUserRecord.ContactEmail);
        if Blacklisted then
          AudLog(StartUserRecord.UserId, S_AUDIT_SEND_MAIL_FAILED_BLACKLISTED);
        if Blacklisted or not GCheckInMailer.SendReminder(rrtContactCheckIn,
                                           StartUserRecord.UserId,
                                           StartUserRecord.OwnEmail,
                                           StartUserRecord.ContactEmail,
                                           '',
                                           StartUserRecord.GenLink(tetContact, latMailBlacklist)) then
        begin
          AudLog(StartUserRecord.UserId, S_AUDIT_CONTACT_REMINDER_SEND_FAILED);
          GLogLog(SV_WARN, S_CONTACT_CHECKIN_REMIND_FAILED + StartUserRecord.UserId);
        end
        else
          AudLog(StartUserRecord.UserId, S_AUDIT_CONTACT_REMINDER_SENT);

        DataRec.FieldType := ftDouble;
        //This is not a bug: initial wait is CONTACT_CHECKIN_NOTIFY, subsequent
        //reminders delayed by USER_CHECKIN_NOTIFY
        DataRec.dVal := RightNow + USER_CHECKIN_NOTIFY_INTERVAL;
        UTable.WriteField(S_NEXT_CONTACT_CHECKIN_REMIND, DataRec);
      end;
    end;
  end;
end;


function TCheckInApp.UserVerificationActions(T: TMemDBTransaction; UTable: TMemAPITableData; StartUserRecord: TUserRecord): boolean;
var
  RightNow: TDateTime;
  DataRec: TMemDbFieldDataRec;
  Blacklisted: boolean;
begin
  RightNow := Now;
  result := StartUserRecord.ExpireAfter < RightNow;
  if result then
  begin
    AudLog(StartUserRecord.UserId, S_AUDIT_UNVERIFIED_USER_EXPIRED);
    UTable.Discard;
    UTable.Delete;
  end
  else
  begin
    if StartUserRecord.NextRegisterRemind < RightNow then
    begin
      if StartUserRecord.OwnVerifyState = vpsUnverifiedPadForVerify then
      begin
        BlackListed := EmailBlackListedInTrans(T, StartUserRecord.OwnEmail);
        if Blacklisted then
          AudLog(StartUserRecord.UserId, S_AUDIT_SEND_MAIL_FAILED_BLACKLISTED);
        //Sends to own email.
        if Blacklisted or not GCheckInMailer.SendReminder(rrtOwnerRegister,
                                           StartUserRecord.UserId,
                                           StartUserRecord.OwnEmail,
                                           StartUserRecord.ContactEmail,
                                           StartUserRecord.GenLink(tetOwn, latMailConfirm),
                                           StartUserRecord.GenLink(tetOwn, latAccountDelete)) then
        begin
          AudLog(StartUserRecord.UserId, S_AUDIT_VERIFY_ACCOUNT_SEND_FAILED);
          GLogLog(SV_WARN, S_OWNER_REGISTER_REMIND_FAILED + StartUserRecord.UserId);
        end
        else
          AudLog(StartUserRecord.UserId, S_AUDIT_VERIFY_ACCOUNT_SENT);
      end;
      if StartUserRecord.ContactVerifySTate = vpsUnverifiedPadForVerify then
      begin
        BlackListed := EmailBlackListedInTrans(T, StartUserRecord.ContactEmail);
        if Blacklisted then
          AudLog(StartUserRecord.UserId, S_AUDIT_SEND_MAIL_FAILED_BLACKLISTED);
        //Sends to contact email
        if Blacklisted or not GCheckInMailer.SendReminder(rrtContactRegister,
                                           StartUserRecord.UserId,
                                           StartUserRecord.OwnEmail,
                                           StartUserRecord.ContactEmail,
                                           StartUserRecord.GenLink(tetContact, latMailConfirm),
                                           StartUserRecord.GenLink(tetContact, latMailBlacklist)) then
        begin
          AudLog(StartUserRecord.UserId, S_AUDIT_VERIFY_CONTACT_SEND_FAILED);
          GLogLog(SV_WARN, S_CONTACT_REGISTER_REMIND_FAILED + StartUserRecord.UserId);
        end
        else
          AudLog(StartUserRecord.UserId, S_AUDIT_VERIFY_CONTACT_SENT);
      end;
      //Now just increment the registration, don't change the expiry.
      DataRec.FieldType := ftDouble;
      DataRec.dVal := RightNow + USER_REGISTER_NOTIFY_INTERVAL;
      UTable.WriteField(S_NEXT_REGISTER_REMIND, DataRec);
    end;
  end;
end;


function TCheckInApp.UserPeriodicActions(T: TMemDBTransaction; UTable: TMemAPITableData): boolean;
var
  StartUserRecord: TUserRecord;
begin
  StartUserRecord := TUSerRecord.Create;
  try
    StartUserRecord.FromUTable(UTable);
    if (StartUserRecord.OwnVerifyState = vpsUnverifiedPadForVerify)
      or (StartUserRecord.ContactVerifySTate = vpsUnverifiedPadForVerify) then
      result := UserVerificationActions(T, UTable, StartUserRecord)
    else
      result := UserCheckInActions(T, UTable, StartUserRecord);
  finally
    StartUserRecord.Free;
  end;
end;

procedure TCheckInApp.DoPeriodic;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  ApiDB: TMemAPIDatabase;
  ApiData: TMemAPITableData;
  DataRec: TMemDBFieldDataRec;
  UsrTblHandle: TMemDBHandle;
  Located, Deleted: boolean;
begin
  try
    if FLastPeriodicAllTasks + SYSTEM_PERIODIC_CHECK_INTERVAL < Now then
    begin
      FLastPeriodicAllTasks := Now;
      S := MemDB.StartSession;
      try
        T := S.StartTransaction(amReadWrite, amLazyWrite, ilCommittedRead);
        try
          ApiDB := T.GetAPI;
          try
            UsrTblHandle := ApiDB.OpenTableOrKey(S_USERTABLE);
            ApiData := APIDB.GetApiObjectFromHandle(UsrTblHandle, APITableData) as TMemAPITableData;
            try
              Located := ApiData.Locate(ptFirst, S_IDX_NEXT_PERIODIC);
              if Located then
              begin
                ApiData.ReadField(S_NEXT_PERIODIC, DataRec);
                Assert(DataRec.FieldType = ftDouble);
              end;
              while Located and (DataRec.dVal < Now)  do
              begin
                DataRec.dVal := Now + USER_PERIODIC_CHECK_INTERVAL;
                ApiData.WriteField(S_NEXT_PERIODIC, DataRec);
                Deleted := UserPeriodicActions(T, ApiData);
                if not Deleted then
                  ApiData.Post;
                Located := ApiData.Locate(ptNext, S_IDX_NEXT_PERIODIC);
                if Located then
                begin
                  ApiData.ReadField(S_NEXT_PERIODIC, DataRec);
                  Assert(DataRec.FieldType = ftDouble);
                end;
              end;
            finally
              ApiData.Free;
            end;
          finally
            ApiDB.Free;
          end;
          T.CommitAndFree;
        except
          T.RollbackAndFree;
          raise;
        end;
      finally
        S.Free;
      end;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_EXCEPTION_IN_PERIODIC_HANDLING + E.ClassName + ' ' + E.Message);
    end;
  end;
end;

constructor TCheckInApp.Create;
begin
  inherited;
  DBStart;
end;

destructor TCheckInApp.Destroy;
begin
  DBStop;
  inherited;
end;

function TCheckInApp.HandleEndpointRequest(Sender: TObject):string;
begin
  if Length(GAppConfig.EndpointName) > 0 then
    result := GAppConfig.EndpointName
  else
  begin
    //TODO - Dynamically look-up if possible.
    result := S_LOCALHOST;
  end;
end;

function TCheckInApp.HandleCryptKeyRequest(Sender: TObject; Username: string; var CryptKey, CryptPassword: string): boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  ApiDB: TMemAPIDatabase;
  ApiData: TMemAPITableData;
  DataRec: TMemDBFieldDataRec;
  UsrTblHandle: TMemDBHandle;
begin
  result := false;
  try
    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amRead);
      try
        ApiDB := T.GetAPI;
        try
          UsrTblHandle := ApiDB.OpenTableOrKey(S_USERTABLE);
          ApiData := APIDB.GetApiObjectFromHandle(UsrTblHandle, APITableData) as TMemAPITableData;
          try
            DataRec.FieldType := ftUnicodeString;
            DataRec.sVal := Username;
            if not ApiData.FindByIndex(S_IDX_USERID, DataRec) then
              raise Exception.Create(S_USER_NOT_FOUND);

            //Read key from DB and convert to bytes.
            APIData.ReadField(S_PASS_HMAC_KEY, DataRec);
            if DataRec.FieldType <> ftUnicodeString then
              raise Exception.Create(S_INTERNAL_CRYPT);
            CryptKey := DataRec.sVal;

            APIData.ReadField(S_PASS_CRYPT, DataRec);
            if DataRec.FieldType <> ftUnicodeString then
              raise Exception.Create(S_INTERNAL_CRYPT);
            CryptPassword := DataRec.sVal;
            result := true;
          finally
            ApiData.Free;
          end;
        finally
          ApiDB.Free;
        end;
      finally
        T.RollbackAndFree;
      end;
    finally
      S.Free;
    end;
  except
    on E: Exception do
    begin
      result := false;
      GLogLog(SV_FAIL, S_CRYPT_KEY_REQUEST + E.ClassName + ' ' + E.Message);
      AudLog(Username, S_AUDIT_CRYPT_KEY_REQUEST_EXCEPTION)
    end;
  end;
end;

procedure TCheckInApp.UpdateCheckInTimers(UTable: TMemAPITableData);
var
  DataRec: TMemDbFieldDataRec;
  RightNow: TDateTime;
begin
  DataRec.FieldType := ftDouble;
  RightNow := Now;
  DataRec.dVal := RightNow + USER_CHECKIN_NOTIFY_INTERVAL;
  UTable.WriteField(S_NEXT_CHECKIN_REMIND, DataRec);
  DataRec.dVal := RightNow + CONTACT_CHECKIN_NOTIFY_INTERVAL;
  UTable.WriteField(S_NEXT_CONTACT_CHECKIN_REMIND, DataRec);
  DataRec.dVal := RightNow + USER_CHECKIN_STOP_INTERVAL;
  UTable.WriteField(S_STOP_CHECKIN_REMIND, DataRec);
end;

procedure TCheckInApp.UpdateRegistrationTimers(UTable: TMemAPITableData; Initial: boolean);
var
  DataRec: TMemDbFieldDataRec;
  RightNow: TDateTime;
begin
  DataRec.FieldType := ftDouble;
  RightNow := Now;
  if Initial then
    DataRec.dVal := RightNow + USER_REGISTER_INITIAL_NOTIFY_INTERVAL
  else
    DataRec.dVal := RightNow + USER_REGISTER_NOTIFY_INTERVAL;
  UTable.WriteField(S_NEXT_REGISTER_REMIND, DataRec);
  DataRec.dVal := RightNow + USER_REGISTER_EXPIRE_INTERVAL;
  UTable.WriteField(S_EXPIRE_AFTER, DataRec);
end;

function TCheckInApp.HandleRegisterRequest(Sender: TObject; Username, CryptKey, CryptPassword:string):boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  UsrTblHandle: TMemDBHandle;
  ApiDB: TMemAPIDatabase;
  ApiData: TMemAPITableData;
  DataRec: TMemDBFieldDataRec;
begin
  AudLog(Username, S_AUDIT_REGISTRATION_REQUEST);
  try
    result := false;
    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amReadWrite);
      try
        ApiDB := T.GetAPI;
        try
          UsrTblHandle := ApiDB.OpenTableOrKey(S_USERTABLE);
          ApiData := APIDB.GetApiObjectFromHandle(UsrTblHandle, APITableData) as TMemAPITableData;
          try
            DataRec.FieldType := ftUnicodeString;
            DataRec.sVal := Username;
            result := not ApiData.FindByIndex(S_IDX_USERID, DataRec);
            if result then
            begin
              ApiData.Append;
              ApiData.WriteField(S_USERID, DataRec);
              DataRec.sVal := CryptKey;
              ApiData.WriteField(S_PASS_HMAC_KEY, DataRec);
              DataRec.sVal := CryptPassword;
              ApiData.WriteField(S_PASS_CRYPT, DataRec);

              UpdateRegistrationTimers(ApiData, false);
              //S_NEXT_CHECKIN_REMIND, S_STOP_CHECKIN_REMIND not needed yet.
              //S_LAST_LOGIN, S_LAST_CHECKIN both zero.
              ApiData.Post;
            end;
          finally
            APIData.Free;
          end;
        finally
          APIDb.Free;
        end;
        if result then
          T.CommitAndFree
        else
          T.RollbackAndFree;
      except
        T.RollbackAndFree;
        raise;
      end;
    finally
      S.Free;
    end;
    if result then
      AudLog(Username, S_AUDIT_REGISTRATION_REQUEST_OK)
    else
      AudLog(Username, S_AUDIT_REGISTRATION_FAILED);
  except
    on E: Exception do
    begin
      result := false;
      GLogLog(SV_FAIL, S_REGISTER + Username + E.ClassName + ' ' + E.Message);
      AudLog(Username, S_AUDIT_REGISTRATION_EXCEPTION);
    end;
  end;
end;

procedure TCheckInApp.HandleLoginRequest(Sender: TObject; Username: string; ValidateOk: boolean);
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  UsrTblHandle: TMemDBHandle;
  ApiDB: TMemAPIDatabase;
  ApiData: TMemAPITableData;
  DataRec: TMemDBFieldDataRec;

  PageProducer: TCustomPageProducer;
  Session: THTTPDispatcherSession;
  LogonInfo: TCheckInLogonInfo;
begin
  try
    if ValidateOk then
      AudLog(Username, S_AUDIT_LOGIN_REQUEST_OK)
    else
      AudLog(Username, S_AUDIT_LOGIN_REQUEST_DENIED);
    GLogLog(SV_INFO, S_LOGIN + Username + ': ' + BoolToStr(ValidateOK, true));

    if ValidateOK then
    begin
      S := MemDB.StartSession;
      try
        T := S.StartTransaction(amReadWrite);
        try
          ApiDB := T.GetAPI;
          try
            UsrTblHandle := ApiDB.OpenTableOrKey(S_USERTABLE);
            ApiData := APIDB.GetApiObjectFromHandle(UsrTblHandle, APITableData) as TMemAPITableData;
            try
              DataRec.FieldType := ftUnicodeString;
              DataRec.sVal := Username;
              if ApiData.FindByIndex(S_IDX_USERID, DataRec) then
              begin
                PageProducer := Sender as TCustomPageProducer;
                Assert(Assigned(PageProducer));
                Session := PageProducer.Session;
                Assert(Assigned(Session));
                LogonInfo := Session.LogonInfo as TCheckInLogonInfo;
                Assert(Assigned(LogonInfo));

                DataRec.FieldType := ftDouble;
                //Get previous vals out of DB and put into temp logoninfo state.
                ApiData.ReadField(S_LAST_LOGIN, DataRec);
                LogonInfo.FLastLoginBeforeNow := DataRec.dVal;
                ApiData.ReadField(S_LAST_CHECKIN, DataRec);
                LogonInfo.FLastCheckinBeforeNow := DataRec.dVal;
                //And write current time back to DB.
                DataRec.dVal := Now;
                ApiData.WriteField(S_LAST_LOGIN, DataRec);
                ApiData.WriteField(S_LAST_CHECKIN, DataRec);
                ApiData.Post;
              end;
            finally
              APIData.Free;
            end;
          finally
            APIDb.Free;
          end;
          T.CommitAndFree;
        except
          T.RollbackAndFree;
          raise;
        end;
      finally
        S.Free;
      end;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_LOGIN + Username + ' ' + E.ClassName + ' ' + E.Message);
      AudLog(Username, S_AUDIT_LOGIN_REQUEST_EXCEPTION);
    end;
  end;
end;

function TCheckInApp.ReadUserRecord(Username: string): TUserRecord;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  DB: TMemAPIDatabase;
  TD: TMemAPITableData;
  H: TMemDBHandle;
  Data: TMemDbFieldDataRec;
begin
  result := nil;
  S := MemDB.StartSession;
  try
    T := S.StartTransaction(amRead);
    try
      DB := T.GetAPI;
      try
        H := DB.OpenTableOrKey(S_USERTABLE);
        TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
        try
          Data.FieldType := ftUnicodeString;
          Data.sVal := Username;
          if TD.FindByIndex(S_IDX_USERID, Data) then
          begin
            result := TUserRecord.Create;
            result.FromUTable(TD);
          end;
        finally
          TD.Free;
        end;
      finally
        DB.Free;
      end;
    finally
      T.RollbackAndFree;
    end;
  finally
    S.Free;
  end;
end;

function TCheckInApp.HandleMailAction(PageProducer: TPageProducer; Pad: string; Action: string; var ResInfo: string): boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  DB: TMemAPIDatabase;
  H: TMemDBHandle;
  TD: TMemAPITableData;
  Data: TMemDBFieldDataRec;
  CheckInUserId: string;
  CheckInTime: TDateTime;
  Session: THTTPDispatcherSession;
  LogonInfo: TCheckInLogonInfo;
begin
  SetLength(CheckInUserId, 0);
  CheckInTime := Now;
  try
    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amReadWrite);
      try
        DB := T.GetAPI;
        try
          H := DB.OpenTableOrKey(S_USERTABLE);
          TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
          try
            Data.FieldType := ftUnicodeString;
            Data.sVal := Pad;
            result := TD.FindByIndex(S_IDX_OWN_EMAIL_PAD, Data);
            if result then
            begin
              if Action = S_ACTION_MAIL_CONFIRM then
              begin
                Data.FieldType := ftInteger;
                Data.i32Val := Ord(vpsVerifiedPadForUnsub);
                TD.WriteField(S_OWN_VERIFY_STATE, Data);

                Data.FieldType := ftUnicodeString;
                Data.sVal := GenPad;
                TD.WriteField(S_OWN_EMAIL_PAD, Data);

                UpdateRegistrationTimers(TD, false);
                UpdateCheckInTimers(TD);

                TD.ReadField(S_USERID, Data);
                AudLog(Data.sVal, S_AUDIT_ACCOUNT_EMAIL_VERIFIED);
                TD.Post;
              end
              else if Action = S_ACTION_ACCOUNT_DELETE then
              begin
                TD.ReadField(S_USERID, Data);
                AudLog(Data.sVal, S_AUDIT_ACCOUNT_DELETED_VIA_EMAIL);
                TD.Delete;
              end
              else if Action = S_ACTION_CHECK_IN then
              begin
                TD.ReadField(S_USERID, Data);
                Assert(Data.FieldType = ftUnicodeString);
                CheckInUserId := Data.sVal;
                Data.FieldType := ftDouble;
                Data.dVal := CheckInTime;
                TD.WriteField(S_LAST_CHECKIN, Data);
                UpdateCheckInTimers(TD);
                AudLog(CheckInUserId, S_AUDIT_ACCOUNT_CHECKED_IN);
                TD.Post;
              end
              else
              begin
                ResInfo := S_ACTION_NOT_SUPPORTED;
              end;
            end
            else
            begin
              result := TD.FindByIndex(S_IDX_CONTACT_EMAIL_PAD, Data);
              if result then
              begin
                if Action = S_ACTION_MAIL_CONFIRM then
                begin
                  Data.FieldType := ftInteger;

                  Data.i32Val := Ord(vpsVerifiedPadForUnsub);
                  TD.WriteField(S_CONTACT_VERIFY_STATE, Data);

                  Data.FieldType := ftUnicodeString;
                  Data.sVal := GenPad;
                  TD.WriteField(S_CONTACT_EMAIL_PAD, Data);

                  UpdateRegistrationTimers(TD, false);

                  TD.ReadField(S_USERID, Data);
                  AudLog(Data.sVal, S_AUDIT_CONTACT_EMAIL_VERIFIED);

                  TD.Post;
                end
                else if Action = S_ACTION_BLACKLIST then
                begin
                  TD.ReadField(S_CONTACT_EMAIL, Data);
                  result := AddToBlackListInTrans(T, Data.sVal);
                  if result then
                    AudLog(S_MAIL_AUDIT + Data.sVal, S_AUDIT_EMAIL_BLACKLISTED)
                  else
                    AudLog(S_MAIL_AUDIT + Data.sVal, S_AUDIT_EMAIL_BLACKLIST_FAILED);
                end
                else
                begin
                  result := false;
                  ResInfo := S_ACTION_NOT_SUPPORTED;
                end;
              end
              else
              begin
                result := false;
                ResInfo := S_CHECK_IN_KEY_NOT_FOUND;
              end;
            end;
          finally
            TD.Free;
          end;
        finally
          DB.Free;
        end;
        T.CommitAndFree;
      except
        T.RollbackAndFree;
        result := false;
        raise;
      end;
    finally
      S.Free;
    end;
    if Result and (Length(CheckInUserId) > 0) then
    begin
      Session := PageProducer.Session;
      if Assigned(Session) and Assigned(Session.LogonInfo) then
      begin
        LogonInfo := Session.LogonInfo as TCheckInLogonInfo;
        if LogonInfo.LogonId = CheckInUserId then
          LogonInfo.FLastCheckinBeforeNow := CheckInTime;
      end;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_CHECKIN_REQUEST + ' ' + E.ClassName + ' ' + E.Message);
      ResInfo := S_INTERNAL_ERROR;
      result := false;
    end;
  end;
end;

function TCheckInApp.HandleQuickCheckInRequest(PageProducer:TPageProducer; Pad: string; var ResInfo: string): boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  DB: TMemAPIDatabase;
  H: TMemDBHandle;
  TD: TMemAPITableData;
  Data: TMemDbFieldDataRec;
  CheckInUserId: string;
  Session: THTTPDispatcherSession;
  LogonInfo: TCheckInLogonInfo;
  CheckInTime: TDateTime;
begin
  SetLength(ResInfo, 0);
  result := false;
  if Length(Pad) = 0 then
  begin
    ResInfo := S_NO_CHECK_IN_KEY;
  end
  else
  begin
    CheckInTime := Now;
    try
      S := MemDB.StartSession;
      try
        T := S.StartTransaction(amReadWrite);
        try
          DB := T.GetAPI;
          try
            H := DB.OpenTableOrKey(S_USERTABLE);
            TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
            try
              Data.FieldType := ftUnicodeString;
              Data.sVal := Pad;
              result := TD.FindByIndex(S_IDX_LOGLESS_CHECKIN_PAD, Data);
              if result then
              begin
                TD.ReadField(S_USERID, Data);
                Assert(Data.FieldType = ftUnicodeString);
                CheckInUserId := Data.sVal;
                Data.FieldType := ftDouble;
                Data.dVal := CheckInTime;
                TD.WriteField(S_LAST_CHECKIN, Data);
                UpdateCheckInTimers(TD);

                AudLog(CheckInUserId, S_AUDIT_ACCOUNT_QUICK_CHECKIN);
                TD.Post;
              end
              else
                ResInfo := S_CHECK_IN_KEY_NOT_FOUND;
            finally
              TD.Free;
            end;
          finally
            DB.Free;
          end;
          if result then
            T.CommitAndFree
          else
            T.RollbackAndFree;
        except
          T.RollbackAndFree;
          raise;
        end;
      finally
        S.Free;
      end;
      if Result and (Length(CheckInUserId) > 0)then
      begin
        Session := PageProducer.Session;
        if Assigned(Session) and Assigned(Session.LogonInfo) then
        begin
          LogonInfo := Session.LogonInfo as TCheckInLogonInfo;
          if LogonInfo.LogonId = CheckInUserId then
            LogonInfo.FLastCheckinBeforeNow := CheckInTime;
        end;
      end;
    except
      on E: Exception do
      begin
        GLogLog(SV_FAIL, S_CHECKIN_REQUEST + ' ' + E.ClassName + ' ' + E.Message);
        ResInfo := S_INTERNAL_ERROR;
        result := false;
      end;
    end;
  end;
end;


function TCheckInApp.SetEmail(Username: string; NewEmail: string; EmailType: TEmailType): boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  DB: TMemAPIDatabase;
  H: TMemDBHandle;
  TD: TMemAPITableData;
  Data: TMemDbFieldDataRec;
  FieldName: string;
begin
  result := false;
  try
    NewEmail := NewEmail.Trim;
    if (Length(NewEmail) = 0) or (Pos('@', NewEmail) <= 0) then
      exit;

    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amReadWrite);
      try
        DB := T.GetAPI;
        try
          H := DB.OpenTableOrKey(S_USERTABLE);
          TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
          try
            Data.FieldType := ftUnicodeString;
            Data.sVal := Username;
            result := TD.FindByIndex(S_IDX_USERID, Data);
            if result then
            begin
              if not (EMailType in [tetOwn, tetContact]) then
              begin
                Assert(false);
                result := false;
                exit;
              end;
              if EmailBlacklistedInTrans(T, NewEmail) then
              begin
                result := false;
                AudLog(Username, S_AUDIT_SET_MAIL_FAILED_BLACKLISTED);
              end
              else
              begin
                case EmailType of
                  tetOwn: FieldName := S_OWN_EMAIL;
                  tetContact: FieldName := S_CONTACT_EMAIL;
                end;
                Data.sVal := NewEmail;
                TD.WriteField(FieldName, Data);

                case EmailType of
                  tetOwn: FieldName := S_OWN_EMAIL_PAD;
                  tetContact: FieldName := S_CONTACT_EMAIL_PAD;
                end;
                Data.sVal := GenPad;
                TD.WriteField(FieldName, Data);

                case EmailType of
                  tetOwn: FieldName := S_OWN_VERIFY_STATE;
                  tetContact: FieldName := S_CONTACT_VERIFY_STATE;
                end;
                Data.FieldType := ftInteger;
                Data.i32Val := Ord(vpsUnverifiedPadForVerify);
                TD.WriteField(FieldName, Data);
                //Reset expiry timers.
                UpdateRegistrationTimers(TD, true);
                case EmailType of
                  tetOwn: AudLog(Username, S_AUDIT_CHANGED_OWN_EMAIL);
                  tetContact: AudLog(Username, S_AUDIT_CHANGED_CONTACT_EMAIL);
                end;
                TD.Post;
              end;
            end;
          finally
            TD.Free;
          end;
        finally
          DB.Free;
        end;
        if result then
          T.CommitAndFree
        else
          T.RollbackAndFree;
      except
        T.RollbackAndFree;
        raise;
      end;
    finally
      S.Free;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_SET_EMAIL + Username + ' ' + E.ClassName + ' ' + E.Message);
      AudLog(Username, S_AUDIT_SET_EMAIL_EXCEPTION);
      result := false;
    end;
  end;
end;

function TCheckInApp.SetQuickCheckin(Username: string; Enable: boolean): boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  DB: TMemAPIDatabase;
  H: TMemDBHandle;
  TD: TMemAPITableData;
  Data: TMemDbFieldDataRec;
begin
  result := false;
  try
    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amReadWrite);
      try
        DB := T.GetAPI;
        try
          H := DB.OpenTableOrKey(S_USERTABLE);
          TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
          try
            Data.FieldType := ftUnicodeString;
            Data.sVal := Username;
            result := TD.FindByIndex(S_IDX_USERID, Data);
            if result then
            begin
              if Enable then
                Data.sVal := GenPad
              else
                SetLength(Data.sVal, 0);
              TD.WriteField(S_LOGLESS_CHECKIN_PAD, Data);

              TD.Post;
            end;
          finally
            TD.Free;
          end;
        finally
          DB.Free;
        end;
        if result then
          T.CommitAndFree
        else
          T.RollbackAndFree;
      except
        T.RollbackAndFree;
        raise;
      end;
    finally
      S.Free;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_SET_QUICK_CHECKIN + Username + ' ' + E.ClassName + ' ' + E.Message);
      AudLog(Username, S_AUDIT_SET_QUICK_CHECKIN_EXCEPTION);
      result := false;
    end;
  end;
end;

function TCheckInApp.DeleteAccount(Username: string): boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  DB: TMemAPIDatabase;
  H: TMemDBHandle;
  TD: TMemAPITableData;
  Data: TMemDbFieldDataRec;
begin
  result := false;
  try
    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amReadWrite);
      try
        DB := T.GetAPI;
        try
          H := DB.OpenTableOrKey(S_USERTABLE);
          TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
          try
            Data.FieldType := ftUnicodeString;
            Data.sVal := Username;
            result := TD.FindByIndex(S_IDX_USERID, Data);
            if result then
            begin
              AudLog(Username, S_AUDIT_ACCOUNT_DELETED_WEBUI);
              TD.Delete;
            end;
          finally
            TD.Free;
          end;
        finally
          DB.Free;
        end;
        if result then
          T.CommitAndFree
        else
          T.RollbackAndFree;
      except
        T.RollbackAndFree;
        raise;
      end;
    finally
      S.Free;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_SET_QUICK_CHECKIN + Username + ' ' + E.ClassName + ' ' + E.Message);
      AudLog(Username, S_AUDIT_ACCOUNT_DELETE_EXCEPTION);
      result := false;
    end;
  end;
end;


function TCheckInApp.GenPad: string;
var
  KeyBytes, DataBytes, CryptBytes: TIdBytes;
  HMAC: TIdHMACSha256;
  IdMime: TIdEncoderMime;
  i: integer;
begin
  HMAC := TIdHMACSHA256.Create;
  IdMime := TIdEncoderMime.Create;
  try
    SetLength(KeyBytes, HMAC.HashSize);
    for i := 0 to Pred(Length(KeyBytes)) do
      KeyBytes[i] := Random(High(byte));
    SetLength(DataBytes, HMAC.BlockSize);
    for i := 0 to Pred(Length(DataBytes)) do
      DataBytes[i] := Random(High(byte));
    HMAC.Key := KeyBytes;
    CryptBytes := HMAC.HashValue(DataBytes);
    result := IdMime.EncodeBytes(CryptBytes);
  finally
    HMAC.Free;
    IdMime.Free;
  end;
end;

procedure TCheckInApp.AuditPersistRecentToDB(Sender: TObject; const LogEntries: TList);
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  DB: TMemAPIDatabase;
  H: TMemDBHandle;
  TD: TMemAPITableData;
  Data: TMemDbFieldDataRec;
  i: integer;
  Log: TAuditLogEntry;
begin
  try
    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amReadWrite);
      try
        DB := T.GetAPI;
        try
          H := DB.OpenTableOrKey(S_AUDIT_TABLE);
          TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
          try
            for i := 0 to Pred(LogEntries.Count) do
            begin
              Log := TObject(LogEntries.Items[i]) as TAuditLogEntry;
              TD.Append;
              Data.FieldType := ftUnicodeString;
              Data.sVal := Log.Username;
              TD.WriteField(S_AUDIT_USERID, Data);
              Data.sVal := Log.Details;
              TD.WriteField(S_AUDIT_DETAILS, Data);
              Data.FieldType := ftDouble;
              Data.dVal := Log.Timestamp;
              TD.WriteField(S_AUDIT_DATETIME, Data);
              TD.Post;
            end;
          finally
            TD.Free;
          end;
        finally
          DB.Free;
        end;
        T.CommitAndFree;
      except
        T.RollbackAndFree;
        raise;
      end;
    finally
      S.Free;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_AUDIT_LOG_WRITE_FAILED + E.ClassName + ' ' + E.Message);
    end;
  end;
end;

procedure TCheckInApp.AuditGetItemsForPrune(Sender: TObject; const LogEntries: TList; Before: TDateTime);
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  DB: TMemAPIDatabase;
  H: TMemDBHandle;
  TD: TMemAPITableData;
  Data: TMemDbFieldDataRec;
  Located: boolean;
  Log: TAuditLogEntry;
begin
  try
    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amReadWrite, amLazyWrite, ilCommittedRead);
      try
        DB := T.GetAPI;
        try
          H := DB.OpenTableOrKey(S_AUDIT_TABLE);
          TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
          try
            Located := TD.Locate(ptFirst, S_IDX_AUDIT_DATETIME);
            if Located then
            begin
              TD.ReadField(S_AUDIT_DATETIME, Data);
              Assert(Data.FieldType = ftDouble);
            end;
            while Located and (Data.dVal < Before) do
            begin
              Log := TAuditLogEntry.Create;
              Log.Timestamp := Data.dVal;
              TD.ReadField(S_AUDIT_USERID, Data);
              Assert(Data.FieldType = ftUnicodeString);
              Log.Username := Data.sVal;
              TD.ReadField(S_AUDIT_DETAILS, Data);
              Assert(Data.FieldType = ftUnicodeString);
              Log.Details := Data.sVal;
              LogEntries.Add(Log);
              TD.Delete;

              Located := TD.Locate(ptNext, S_IDX_AUDIT_DATETIME);
              if Located then
                TD.ReadField(S_AUDIT_DATETIME, Data);
            end;
          finally
            TD.Free;
          end;
        finally
          DB.Free;
        end;
        T.CommitAndFree;
      except
        T.RollbackAndFree;
        raise;
      end;
    finally
      S.Free;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_AUDIT_LOG_PRUNE_FAILED + E.ClassName + ' ' + E.Message);
    end;
  end;
end;

procedure TCheckInApp.AuditGetLastPruneTime(Sender: TObject; var LastPrune: TDateTime);
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  DB: TMemAPIDatabase;
  H: TMemDBHandle;
  TD: TMemAPITableData;
  Data: TMemDbFieldDataRec;
  Located: boolean;
begin
  LastPrune := Now;
  try
    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amRead);
      try
        DB := T.GetAPI;
        try
          H := DB.OpenTableOrKey(S_AUDIT_TABLE);
          TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
          try
            Located := TD.Locate(ptFirst, S_IDX_AUDIT_DATETIME);
            if Located then
            begin
              TD.ReadField(S_AUDIT_DATETIME, Data);
              Assert(Data.FieldType = ftDouble);
              LastPrune := Data.dVal;
            end;
          finally
            TD.Free;
          end;
        finally
          DB.Free;
        end;
      finally
        T.RollbackAndFree;
      end;
    finally
      S.Free;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_AUDIT_LOG_QUERY_PRUNE_TIME_FAILED + E.ClassName + ' ' + E.Message);
    end;
  end;
end;

procedure TCheckInApp.ReadAuditLog(Username: string; LogList: TList);
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  DB: TMemAPIDatabase;
  H: TMemDBHandle;
  TD: TMemAPITableData;
  Data: TMemDbFieldDataRec;
  Located: boolean;
  AuditLog: TAuditLogEntry;
begin
  try
    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amRead);
      try
        DB := T.GetAPI;
        try
          H := DB.OpenTableOrKey(S_AUDIT_TABLE);
          TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
          try
            Data.FieldType := ftUnicodeString;
            Data.sVal := Username;
            Located := TD.FindEdgeByIndex(ptFirst, S_IDX_AUDIT_USERID, Data);
            while Located do
            begin
              AuditLog := TAuditLogEntry.Create;
              try
                TD.ReadField(S_AUDIT_USERID, Data);
                Assert(Data.FieldType = ftUnicodeString);
                AuditLog.Username := Data.sVal;
                TD.ReadField(S_AUDIT_DATETIME, Data);
                Assert(Data.FieldType = ftDouble);
                AuditLog.Timestamp := Data.dVal;
                TD.ReadField(S_AUDIT_DETAILS, Data);
                Assert(Data.FieldType = ftUnicodeString);
                AuditLog.Details := Data.sVal;
                LogList.Add(AuditLog);
              except
                AuditLog.Free;
                raise;
              end;
              Located := TD.Locate(ptNext, S_IDX_AUDIT_USERID);
              if Located then
              begin
                TD.ReadField(S_AUDIT_USERID, Data);
                Assert(Data.FieldType = ftUnicodeString);
                Located := Data.sVal = Username;
              end;
            end;
          finally
            TD.Free;
          end;
        finally
          DB.Free;
        end;
      finally
        T.RollbackAndFree;
      end;
    finally
      S.Free;
    end;
  except
    on E: Exception do
      GLogLog(SV_FAIL, S_AUDIT_READ_AUDIT_LOG_FAILED + E.ClassName + ' ' + E.Message);
  end;
end;

function TCheckInApp.AddToBlackListInTrans(T: TMemDBTransaction; Address: string): boolean;
var
  DB: TMemAPIDatabase;
  TD: TMemAPITableData;
  H: TMemDBHandle;
  Data: TMemDbFieldDataRec;
  Located: boolean;
begin
  Address := Address.Trim.ToLower;
  try
    DB := T.GetAPI;
    try
      H := DB.OpenTableOrKey(S_BLACKLIST_TABLE);
      TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
      try
        Data.FieldType := ftUnicodeString;
        Data.sVal := Address;
        Located := TD.FindByIndex(S_IDX_BLACKLIST_EMAIL, Data);
        if not Located then
        begin
          TD.Append;
          TD.WriteField(S_BLACKLIST_EMAIL, Data);
          Data.FieldType := ftDouble;
          Data.dVal := Now;
          TD.WriteField(S_BLACKLIST_DATETIME, Data);
          TD.Post;
        end;
        result := true;
      finally
        TD.Free;
      end;
    finally
      DB.Free;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_BLACKLIST_ADD_FAILED + E.ClassName + ' ' + E.Message);
      result := false;
    end;
  end;
end;

function TCheckInApp.EmailBlackListedInTrans(T: TMemDBTransaction; Address: string): boolean;
var
  DB: TMemAPIDatabase;
  TD: TMemAPITableData;
  H: TMemDBHandle;
  Data: TMemDbFieldDataRec;
begin
  Address := Address.Trim.ToLower;
  try
    DB := T.GetAPI;
    try
      H := DB.OpenTableOrKey(S_BLACKLIST_TABLE);
      TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
      try
        Data.FieldType := ftUnicodeString;
        Data.sVal := Address;
        result := TD.FindByIndex(S_IDX_BLACKLIST_EMAIL, Data);
      finally
        TD.Free;
      end;
    finally
      DB.Free;
    end;
  except
    on E: Exception do
    begin
      GLogLog(SV_FAIL, S_BLACKLIST_CHECK_FAILED + E.ClassName + ' ' + E.Message);
      result := false;
    end;
  end;
end;

function TCheckInApp.ClearBlacklist;
var
  S: TMemDbSession;
  T: TMemDBTransaction;
  H: TMemDBHandle;
  DB: TMemAPIDatabase;
  TD: TMemAPITableData;
  Changed: boolean;
begin
  Changed := false;
  GLogLog(SV_INFO, S_CLEARING_BLACKLIST);
  try
    S := MemDB.StartSession;
    try
      T := S.StartTransaction(amReadWrite);
      try
        DB:= T.GetAPI;
        try
          H := DB.OpenTableOrKey(S_BLACKLIST_TABLE);
          TD := DB.GetApiObjectFromHandle(H, APITableData) as TMemAPITableData;
          try
            TD.Locate(ptFirst);
            while TD.RowSelected do
            begin
              Changed := true;
              TD.Delete;
            end;
          finally
            TD.Free;
          end;
        finally
          DB.Free;
        end;
        if Changed then
          T.CommitAndFree
        else
          T.RollbackAndFree;
        result := true;
      except
        result := false;
        T.RollbackAndFree;
        raise;
      end;
    finally
      S.Free;
    end;
  except
    on E: Exception do
      GLogLog(SV_FAIL, S_BLACKLIST_CLEAR_FAILED + E.ClassName + ' ' + E.Message);
  end;
end;

{ ---------------- Config / DB init and finalization ------------- }

procedure TCheckInApp.DBStart;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  APIDB: TMemAPIDatabase;
  APITblMeta: TMemAPITableMetadata;
  EntList: TStringList;
  MadeChanges: boolean;
  TblHandle: TMemDBHandle;
  FieldList: TStringList;
  IndexList: TStringList;
begin
  GLogLog(SV_INFO, 'DB startup done, initialize tables.');
  MadeChanges := false;
  S := MemDB.StartSession;
  try
    T := S.StartTransaction(amReadWrite);
    try
      APIDB := T.GetAPI;
      try
        EntList := APIDB.GetEntityNames;
        try
          //User table creation.
          if EntList.IndexOf(S_USERTABLE) < 0 then
          begin
            TblHandle := APIDB.CreateTable(S_USERTABLE);
            MadeChanges := true;
          end
          else
            TblHandle := APIDB.OpenTableOrKey(S_USERTABLE);
          //User table metadata.
          APITblMeta := APIDB.GetApiObjectFromHandle(TblHandle, APITableMetadata) as TMemAPITableMetadata;
          try
            FieldList := ApiTblMeta.GetFieldNames;
            IndexList := APiTblMeta.GetIndexNames;
            try
              if FieldList.IndexOf(S_USERID) < 0 then
              begin
                ApiTblMeta.CreateField(S_USERID, ftUnicodeString);
                MadeChanges := true;
              end;
              if IndexList.IndexOf(S_IDX_USERID) < 0 then
              begin
                ApiTblMeta.CreateIndex(S_IDX_USERID, S_USERID, [iaUnique, iaNotEmpty]);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_PASS_HMAC_KEY) < 0 then
              begin
                ApiTblMeta.CreateField(S_PASS_HMAC_KEY, ftUnicodeString);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_PASS_CRYPT) < 0 then
              begin
                ApiTblMeta.CreateField(S_PASS_CRYPT, ftUnicodeString);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_NEXT_PERIODIC) < 0 then
              begin
                ApiTblMeta.CreateField(S_NEXT_PERIODIC, ftDouble);
                MadeChanges := true;
              end;
              if IndexList.IndexOf(S_IDX_NEXT_PERIODIC) < 0 then
              begin
                ApiTblMeta.CreateIndex(S_IDX_NEXT_PERIODIC, S_NEXT_PERIODIC, []);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_OWN_EMAIL) < 0 then
              begin
                ApiTblMeta.CreateField(S_OWN_EMAIL, ftUnicodeString);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_OWN_VERIFY_STATE) < 0 then
              begin
                ApiTblMeta.CreateField(S_OWN_VERIFY_STATE, ftInteger);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_OWN_EMAIL_PAD) < 0 then
              begin
                ApiTblMeta.CreateField(S_OWN_EMAIL_PAD, ftUnicodeString);
                MadeChanges := true;
              end;
              if IndexList.IndexOf(S_IDX_OWN_EMAIL_PAD) < 0 then
              begin
                ApiTblMeta.CreateIndex(S_IDX_OWN_EMAIL_PAD, S_OWN_EMAIL_PAD, []);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_CONTACT_EMAIL) < 0 then
              begin
                ApiTblMeta.CreateField(S_CONTACT_EMAIL,ftUnicodeString);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_CONTACT_VERIFY_STATE) < 0 then
              begin
                ApiTblMeta.CreateField(S_CONTACT_VERIFY_STATE, ftInteger);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_CONTACT_EMAIL_PAD) < 0 then
              begin
                ApiTblMeta.CreateField(S_CONTACT_EMAIL_PAD,ftUnicodeString);
                MadeChanges := true;
              end;
              if IndexList.IndexOf(S_IDX_CONTACT_EMAIL_PAD) < 0 then
              begin
                ApiTblMeta.CreateIndex(S_IDX_CONTACT_EMAIL_PAD, S_CONTACT_EMAIL_PAD, []);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_LOGLESS_CHECKIN_PAD) < 0 then
              begin
                ApiTblMeta.CreateField(S_LOGLESS_CHECKIN_PAD,ftUnicodeString);
                MadeChanges := true;
              end;
              if IndexList.IndexOf(S_IDX_LOGLESS_CHECKIN_PAD) < 0 then
              begin
                ApiTblMeta.CreateIndex(S_IDX_LOGLESS_CHECKIN_PAD, S_LOGLESS_CHECKIN_PAD, []);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_EXPIRE_AFTER) < 0 then
              begin
                ApiTblMeta.CreateField(S_EXPIRE_AFTER, ftDouble);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_NEXT_REGISTER_REMIND) < 0 then
              begin
                ApiTblMeta.CreateField(S_NEXT_REGISTER_REMIND, ftDouble);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_NEXT_CHECKIN_REMIND) < 0 then
              begin
                ApiTblMeta.CreateField(S_NEXT_CHECKIN_REMIND, ftDouble);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_NEXT_CONTACT_CHECKIN_REMIND) < 0 then
              begin
                ApiTblMeta.CreateField(S_NEXT_CONTACT_CHECKIN_REMIND, ftDouble);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_STOP_CHECKIN_REMIND) < 0 then
              begin
                ApiTblMeta.CreateField(S_STOP_CHECKIN_REMIND, ftDouble);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_LAST_LOGIN) < 0 then
              begin
                ApiTblMeta.CreateField(S_LAST_LOGIN, ftDouble);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_LAST_CHECKIN) < 0 then
              begin
                ApiTblMeta.CreateField(S_LAST_CHECKIN, ftDouble);
                MadeChanges := true;
              end;
            finally
              FieldList.Free;
              IndexList.Free;
            end;
          finally
             ApiTblMeta.Free;
          end;
          //Audit table creation.
          if EntList.IndexOf(S_AUDIT_TABLE) < 0 then
          begin
            TblHandle := APIDB.CreateTable(S_AUDIT_TABLE);
            MadeChanges := true;
          end
          else
            TblHandle := APIDB.OpenTableOrKey(S_AUDIT_TABLE);
          //Audit table metadata.
          APITblMeta := APIDB.GetApiObjectFromHandle(TblHandle, APITableMetadata) as TMemAPITableMetadata;
          try
            FieldList := ApiTblMeta.GetFieldNames;
            IndexList := APiTblMeta.GetIndexNames;
            try
              if FieldList.IndexOf(S_AUDIT_USERID) < 0 then
              begin
                ApiTblMeta.CreateField(S_AUDIT_USERID, ftUnicodeString);
                MadeChanges := true;
              end;
              if IndexList.IndexOf(S_IDX_AUDIT_USERID) < 0 then
              begin
                ApiTblMeta.CreateIndex(S_IDX_AUDIT_USERID, S_AUDIT_USERID, []);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_AUDIT_DATETIME) < 0 then
              begin
                ApiTblMeta.CreateField(S_AUDIT_DATETIME, ftDouble);
                MadeChanges := true;
              end;
              if IndexList.IndexOf(S_IDX_AUDIT_DATETIME) < 0 then
              begin
                ApiTblMeta.CreateIndex(S_IDX_AUDIT_DATETIME, S_AUDIT_DATETIME, []);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_AUDIT_DETAILS) < 0 then
              begin
                ApiTblMeta.CreateField(S_AUDIT_DETAILS, ftUnicodeString);
                MadeChanges := true;
              end;
            finally
              FieldList.Free;
              IndexList.Free;
            end;
          finally
             ApiTblMeta.Free;
          end;
          //Blacklist table creation.
          if EntList.IndexOf(S_BLACKLIST_TABLE) < 0 then
          begin
            TblHandle := APIDB.CreateTable(S_BLACKLIST_TABLE);
            MadeChanges := true;
          end
          else
            TblHandle := APIDB.OpenTableOrKey(S_BLACKLIST_TABLE);
          //Blacklist table metadata.
          APITblMeta := APIDB.GetApiObjectFromHandle(TblHandle, APITableMetadata) as TMemAPITableMetadata;
          try
            FieldList := ApiTblMeta.GetFieldNames;
            IndexList := APiTblMeta.GetIndexNames;
            try
              if FieldList.IndexOf(S_BLACKLIST_EMAIL) < 0 then
              begin
                ApiTblMeta.CreateField(S_BLACKLIST_EMAIL, ftUnicodeString);
                MadeChanges := true;
              end;
              if IndexList.IndexOf(S_IDX_BLACKLIST_EMAIL) < 0 then
              begin
                ApiTblMeta.CreateIndex(S_IDX_BLACKLIST_EMAIL, S_BLACKLIST_EMAIL, [iaUnique, iaNotEmpty]);
                MadeChanges := true;
              end;
              if FieldList.IndexOf(S_BLACKLIST_DATETIME) < 0 then
              begin
                ApiTblMeta.CreateField(S_BLACKLIST_DATETIME, ftDouble);
                MadeChanges := true;
              end;
            finally
              FieldList.Free;
              IndexList.Free;
            end;
          finally
             ApiTblMeta.Free;
          end;
        finally
          EntList.Free;
        end;
      finally
        APIDB.Free;
      end;
      if MadeChanges then
        T.CommitAndFree
      else
        T.RollbackAndFree;
    except
      T.RollbackAndFree;
      raise;
    end;
  finally
    S.Free;
  end;
end;

procedure TCheckInApp.DBStop;
begin
  GLogLog(SV_INFO, 'DB teardown.');
end;

procedure SetupDB;
begin
  MemDB := TMemDB.Create;
  if not MemDB.InitDB(GAppConfig.DBRootDir) then
    raise Exception.Create(S_DB_INIT_FATAL);
end;

procedure FiniDB;
begin
  MemDB.Free;
  MemDB := nil;
end;

initialization
  Randomize;
  SetupDB;
  GCheckInApp := TCheckInApp.Create;
  Assert(Assigned(GAuditLog));
  GAuditLog.OnPersistRecentToDb := GCheckInApp.AuditPersistRecentToDb;
  GAuditLog.OnPruneDBToFile := GCheckInApp.AuditGetItemsForPrune;
  GAuditLog.OnGetLastPrune := GCheckInApp.AuditGetLastPruneTime;
finalization
  GAuditLog.Flush;
  GAuditLog.OnPersistRecentToDb := nil;
  GAuditLog.OnPruneDBToFile := nil;
  GAuditLog.OnGetLastPrune := nil;
  GCheckInApp.Free;
  FiniDB;
end.

