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
  SysUtils, SSStreamables, MemDBAPI, HTTPServerPageProducer;

type
  TUserRecord = class;

  //TODO - Some basic DB transaction context stuff to take the drudgery away.

  TEmailType = (tetOwn, tetContact);

  //Should not be any state in this class - this just does app logic.
{$IFDEF USE_TRACKABLES}
  TCheckInApp = class(TTrackable)
{$ELSE}
  TCheckInApp = class
{$ENDIF}
  private
  protected
    procedure DBStart;
    procedure DBStop;
    function GenPad: string;
  public
    constructor Create;
    destructor Destroy; override;
    function HandleRegisterRequest(Sender: TObject; Username, CryptKey, CryptPassword:string):boolean;
    function HandleCryptKeyRequest(Sender: TObject; Username: string; var CryptKey, CryptPassword: string): boolean;
    procedure HandleLoginRequest(Sender: TObject; Username: string; ValidateOk: boolean);
    function SetEmail(Username: string; NewEmail: string; EmailType: TEmailType): boolean;
    function SetQuickCheckin(Username: string; Enable: boolean): boolean;
    function ReadUserRecord(Username: string): TUserRecord;
    function HandleQuickCheckInRequest(PageProducer:TPageProducer; Pad: string; var ResInfo: string): boolean;
  end;

  TCheckInAppConfig = class(TObjStreamable)
  private
    FDBRootDir: string;
    FEndpointName: string;
  public
    constructor Create; override;
    property DBRootDir:string read FDBRootDir write FDBRootDir;
    property EndpointName: string read FEndpointName write FEndpointName;
  end;

  TVerifyPadState = (vpsUnverifiedPadForVerify, vpsVerifiedPadForUnsub);

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

    //TODO - Think a bit about expiry times.
    FNextPeriodic, FExpireAfter, FLastLogin, FLastCheckin: TDateTime;
  public
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
    property ExpireAfter: TDateTime read FExpireAfter;
    property LastLogin: TDateTime read FLastLogin;
    property LastCheckin: TDateTime read FLastCheckin;
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
  GAppConfig: TCheckInAppConfig;
  GCheckInApp: TCheckInApp;

//TODO - case handling for email addresses and user ID's.

implementation

uses
  MemDB, StreamingSystem, StreamSysXML, Classes, IOUtils,
  MemDBMisc, GlobalLog, CheckInPageProducer, HTTPServerDispatcher,
  IdHMACSha1, IdGlobal, IdCoderMIME;

const
  DEFAULT_DATA_ROOT_LOCATION = 'MemDB';
  DEFAULT_APP_DATA_DIR = 'CheckIn';
  DEFAULT_PREFS_FILENAME = 'CheckIn.prefs';
  PERIODIC_CHECK_INTERVAL = 1; //Day
  EXPIRE_INTERVAL = 7; //Days.

{ ---------------- Config / DB init and finalization ------------- }

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

  //TODO - Let's just do the subscription side of things first,
  //and worry about pads for unsubscribe in a sec...

  S_OWN_EMAIL = 'OwnEmail';
  //S_IDX_OWN_EMAIL = 'IdxOwnEmail';
  S_OWN_VERIFY_STATE = 'OwnVerifyState';
  S_OWN_EMAIL_PAD = 'OwnEmailPad';
  //S_IDX_OWN_EMAIL_PAD = 'IdxOwnEmailPad';

  S_CONTACT_EMAIL = 'ContactEmail';
  //S_IDX_CONTACT_EMAIL = 'IdxContactEmail';
  S_CONTACT_VERIFY_STATE = 'ContactVerifyState';
  S_CONTACT_EMAIL_PAD = 'ContactEmailPad';
  //S_IDX_CONTACT_EMAIL_PAD = 'IdxContactEmailPad';

  S_LOGLESS_CHECKIN_PAD = 'LoglessCheckInPad';
  S_IDX_LOGLESS_CHECKIN_PAD = 'LoglessCheckInPad';

  //Hopefully only need to index NextPeriodic - that shd be all.
  S_EXPIRE_AFTER = 'ExpireAfter';
  S_LAST_LOGIN = 'LastLogin';
  S_LAST_CHECKIN = 'LastCheckin';

  //TODO on the audit tables.

  { ------------------ Audit table }
  S_AUDIT_TABLE = 'AuditTable';

  S_AUDIT_USERID = 'AuditUserid';
  //S_IDX_AUDIT_USERID = 'IdxAuditUserid';
  S_AUDIT_DATETIME = 'AuditDateTime';
  //S_IDX_AUDIT_DATETIME = 'IdxAuditDateTime';
  S_AUDIT_DETAILS = 'AuditDetails';

  { ------------------ Blacklist table }
  S_BLACKLIST_TABLE = 'BlacklistTable';
  S_BLACKLIST_ENTRY = 'BlacklistEntry';
  //S_IDX_BLACKLIST_ENTRY = 'IdxBlacklistEntry';


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

  S_LOCALHOST = 'localhost';

var
  MemDB: TMemDB;
  ConfFilename: string;
  Heirarchy: THeirarchyInfo;
  RootDir, DBDir: string;

{ ---------------- Proper app logic  ------------- }

{ TUserRecord }

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
end;

{ TCheckInApp }

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
    end;
  end;
end;

function TCheckInApp.HandleRegisterRequest(Sender: TObject; Username, CryptKey, CryptPassword:string):boolean;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  UsrTblHandle: TMemDBHandle;
  ApiDB: TMemAPIDatabase;
  ApiData: TMemAPITableData;
  DataRec: TMemDBFieldDataRec;
  RightNow: TDateTime;
begin
  result := false;
  try
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
              raise Exception.Create(S_USER_ALREADY_REG);
            ApiData.Append;
            ApiData.WriteField(S_USERID, DataRec);
            DataRec.sVal := CryptKey;
            ApiData.WriteField(S_PASS_HMAC_KEY, DataRec);
            DataRec.sVal := CryptPassword;
            ApiData.WriteField(S_PASS_CRYPT, DataRec);
            DataRec.FieldType := ftDouble;
            RightNow := Now;
            DataRec.dVal := RightNow + PERIODIC_CHECK_INTERVAL;
            ApiData.WriteField(S_NEXT_PERIODIC, DataRec);
            DataRec.dVal := RightNow + EXPIRE_INTERVAL;
            ApiData.WriteField(S_EXPIRE_AFTER, DataRec);
            //Last login, last checkin both 0.
            ApiData.Post;
          finally
            APIData.Free;
          end;
        finally
          APIDb.Free;
        end;
        T.CommitAndFree;
        result := true;
      except
        T.RollbackAndFree;
        raise;
      end;
    finally
      S.Free;
    end;
    GLogLog(SV_INFO, S_REGISTER + Username + ' OK');
  except
    on E: Exception do
    begin
      result := false;
      GLogLog(SV_FAIL, S_REGISTER + Username + E.ClassName + ' ' + E.Message);
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
      GLogLog(SV_FAIL, S_LOGIN + Username + ' ' + E.ClassName + ' ' + E.Message);
  end;
end;

//TODO - Basic user-centric ops.

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
  if Length(Pad) = 0 then
  begin
    ResInfo := S_NO_CHECK_IN_KEY;
    result := false;
  end
  else
  begin
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
                CheckInTime := Now;
                Data.dVal := CheckInTime;
                TD.WriteField(S_LAST_CHECKIN, Data);
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
      if Result then
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
      GLogLog(SV_FAIL, S_SET_EMAIL + Username + ' ' + E.ClassName + ' ' + E.Message);
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
  FieldName: string;
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


//TODO - Any specific blacklist table handling.

//TODO - Remove recent history to older history and file.

{ ---------------- Config / DB init and finalization ------------- }

procedure TCheckInApp.DBStart;
var
  S: TMemDBSession;
  T: TMemDBTransaction;
  APIDB: TMemAPIDatabase;
  APITblMeta: TMemAPITableMetadata;
  EntList: TStringList;
  MadeChanges: boolean;
  UsrTblHandle: TMemDBHandle;
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
            UsrTblHandle := APIDB.CreateTable(S_USERTABLE);
            MadeChanges := true;
          end
          else
            UsrTblHandle := APIDB.OpenTableOrKey(S_USERTABLE);
          //User table metadata.
          APITblMeta := APIDB.GetApiObjectFromHandle(UsrTblHandle, APITableMetadata) as TMemAPITableMetadata;
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
          //TODO - Blacklist table (unsubscribe...)
          //TODO - Recent history table.
        finally
          EntList.Free;
        end;
      finally
        APIDB.Free;
      end;
      //TODO - The DB could keep track of this.
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

{$IFDEF MSWINDOWS}
const
  PathSep = '\';
{$ELSE}
const
  PathSep = '/';
{$ENDIF}

procedure AppendTrailingDirSlash(var Path: string);
begin
  if Length(Path) > 0 then
  begin
    if (Path[Length(Path)] <> PathSep) then
      Path := Path + PathSep;
  end;
end;

constructor TCheckInAppConfig.Create;
begin
  inherited;
  FDBRootDir:= DBDir;
  FEndpointName :=  S_LOCALHOST;
end;

procedure SetupHeirarchy(var H: THeirarchyInfo);
begin
  H := TObjDefaultHeirarchy;
  with H do
  begin
    SetLength(MemberClasses, 1);
    MemberClasses[0] :=  TCheckInAppConfig;
  end;
end;

procedure SetupPrefs;
var
  XMLStream: TStreamSysXML;
  FileStream: TFileStream;
begin
  RootDir := TPath.GetHomePath;
  AppendTrailingDirSlash(RootDir);
  if not DirectoryExists(RootDir) then
    TDirectory.CreateDirectory(RootDir);
  RootDir := RootDir + DEFAULT_APP_DATA_DIR;
  AppendTrailingDirSlash(RootDir);
  if not DirectoryExists(RootDir) then
    TDirectory.CreateDirectory(RootDir);
  DBDir := RootDir + DEFAULT_DATA_ROOT_LOCATION;
  AppendTrailingDirSlash(DBDir);
  if not DirectoryExists(DBDir) then
    TDirectory.CreateDirectory(DBDir);
  ConfFileName := RootDir + DEFAULT_PREFS_FILENAME;
  SetupHeirarchy(Heirarchy);

  FileStream := nil;
  XMLStream := nil;
  try
    try
      XMLStream := TStreamSysXML.Create;
      //Don't fail on no class / no property.
      XMLStream.RegisterHeirarchy(Heirarchy);
      FileStream := TFileStream.Create(ConfFileName, fmOpenRead);
      GAppConfig := XMLStream.ReadStructureFromStream(FileStream) as TCheckInAppConfig;
    except
      on Exception do ;
    end;
  finally
    FreeAndNil(FileStream);
    FreeAndNil(XMLStream);
  end;
  if not Assigned(GAppConfig) then
    GAppConfig := TCheckInAppConfig.Create;
end;

procedure FinishPrefs;
var
  XMLStream: TStreamSysXML;
  FileStream: TFileStream;
begin
  FileStream := nil;
  XMLStream := nil;
  try
    try
      XMLStream := TStreamSysXML.Create;
      //Don't fail on no class / no property.
      XMLStream.RegisterHeirarchy(Heirarchy);
      FileStream := TFileStream.Create(ConfFileName, fmCreate);
      XMLStream.WriteStructureToStream(GAppConfig, FileStream);
    except
      on Exception do ;
    end;
  finally
    FreeAndNil(FileStream);
    FreeAndNil(XMLStream);
  end;
  GAppConfig.Free;
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
  SetupPrefs;
  SetupDB;
  GCheckInApp := TCheckInApp.Create;
finalization
  GCheckInApp.Free;
  FiniDB;
  FinishPrefs;
end.

