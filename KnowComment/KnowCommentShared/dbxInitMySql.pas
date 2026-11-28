unit dbxInitMySql;

{ Martin Harvey 2017. }

interface

uses dbXpressInit, SqlExpr, SimpleDS;

type
  TDBXMySQL = class(TDBXDB)
  private
    //TODO - At some point add indexes to table create, not now.
    procedure PatchInIndex(TableName, IndexName, FieldName: string);
    procedure PatchInUTableIndexes;
    procedure PatchInMTableIndexes;
    procedure PatchInCTableIndexes;
  protected
    procedure DBOpenAndSetUser(out Msg:string; AllowCreate: boolean); override;
    function OpenOrCreateUserTable(out Msg:string): boolean; override;
    function OpenOrCreateMediaTable(out Msg:string): boolean; override;
    function OpenOrCreateCommentTable(out Msg:string): boolean; override;
    function DestroyTablesWorker: boolean; override;

    function GetFieldQuoteChar: string; override;
  end;

implementation

uses
  SysUtils, DB, DBXCommon, DBGeneric, DBXMySQL,
  DBSchemaStrs, DataObjects;

const
  SysDatabaseName: string = 'sys';

function TDBXMySQL.GetFieldQuoteChar: string;
begin
  result := '`';
end;

procedure TDBXMySQL.DBOpenAndSetUser(out Msg:string; AllowCreate: boolean);

procedure SetParams(var C: TSQLConnection; Uname, Passwd, DBName: string);
begin
  C.Connected := false;
  C.DriverName := 'MySQL';
  C.Params.Clear;
  C.Params.Add('hostname=localhost');
  C.Params.Add('database=' + DBName);
  C.Params.Add('user_name=' + Uname);
  C.Params.Add('password=' + Passwd);
  C.Params.Add('timeout=3600');
  C.Params.Add('readtimeout=3600');
  C.Params.Add('writetimeout=3600');
  C.LoginPrompt := false;
end;

const
  Username = 'SYSDBA';
  Password = 'masterkey';

var
  SQLQuery: TSQLQuery;
  NewConn: TSQLConnection;
  Fail: boolean;
begin
  Fail := false;
  try
    NewConn := SQLConnection;
    if not Assigned(NewConn) then
      NewConn := TSQLConnection.Create(nil);
    //Create or open database.
    SetParams(NewConn, Username, Password, DBName);
    try
      NewConn.Connected := true;
    except
      on E: Exception do
      begin
        if AllowCreate then
        begin
          SetParams(NewConn, Username, Password, SysDatabaseName);
          try
            NewConn.Connected := true;
            SQLQuery := TSQLQuery.Create(nil);
            try
              SQLQuery.SQLConnection := NewConn;
              SQLQuery.SQL.Add('CREATE DATABASE ' + DBName);
              SQLQuery.ExecSQL();
            finally
              SQLQuery.Free;
            end;
          except
            on E: EDatabaseError do
            begin
              Msg := E.Message;
              Fail := true;
            end;
          end;
          SetParams(NewConn, Username, Password, DBName);
          try
            NewConn.Connected := true;
          except
            on E: EDatabaseError do
            begin
              Msg := E.Message;
              Fail := true;
            end;
          end;
        end
        else
          Fail := true;
      end;
    end;
  finally
    if Fail then
    begin
      SQLConnection := nil;
      NewConn.Free;
    end
    else
    begin
      if NewConn <> SQLConnection  then
        SQLConnection := NewConn;
    end;
  end;
end;


procedure TDBXMySQL.PatchInIndex(TableName, IndexName, FieldName: string);
var
  SQL: TSQLQuery;
  Transaction: TDBXTransaction;
  X: TDBXError;
begin
  SQL := TSQLQuery.Create(nil);
  SQL.SQLConnection := SQLConnection;
  try
    Transaction := SqlConnection.BeginTransaction;
    try
      SQL.SQL.Add(
        'ALTER TABLE ' + TableName +
        ' ADD INDEX ' + IndexName +
        '('+ FieldName +');'
      );
      SQL.ExecSQL;
      SqlConnection.CommitFreeAndNil(Transaction);
    except
      on Exception do
      begin
        SqlConnection.RollbackFreeAndNil(Transaction);
      end;
    end;
  finally
    SQL.Free;
  end;
end;

procedure TDBXMySQL.PatchInUTableIndexes;
var
  ST: TKSiteType;
  UBX: TUserBlockIndexes;
begin
  for ST := Low(ST) to High(ST) do
  begin
    for UBX := Low(UBX) to High(UBX) do
    begin
      PatchInIndex(
        S_USER_TABLE,
        UserBlockIndexNames[ST][UBX],
        UserBlockIndexFieldNames[ST][UBX]);
    end;
  end;
end;

procedure TDBXMySQL.PatchInMTableIndexes;
var
  ST: TKSiteType;
  MBX: TMediaBlockIndexes;
begin
  PatchInIndex(S_MEDIA_TABLE, S_INDEX_MEDIA_OWNER_KEY, S_MEDIA_OWNER_KEY);
  for ST := Low(ST) to High(ST) do
  begin
    for MBX := Low(MBX) to High(MBX) do
    begin
      PatchInIndex(
        S_MEDIA_TABLE,
        MediaBlockIndexNames[ST][MBX],
        MediaBlockIndexFieldNames[ST][MBX]);
    end;
  end;
end;

procedure TDBXMySQL.PatchInCTableIndexes;
var
  ST: TKSiteType;
  CBX: TCommentBlockIndexes;
begin
  PatchInIndex(S_COMMENT_TABLE, S_INDEX_COMMENT_MEDIA_KEY, S_COMMENT_MEDIA_KEY);
  PatchInIndex(S_COMMENT_TABLE, S_INDEX_COMMENT_OWNER_KEY, S_COMMENT_OWNER_KEY);
  for ST := Low(ST) to High(ST) do
  begin
    for CBX := Low(CBX) to High(CBX) do
    begin
      PatchInIndex(
        S_COMMENT_TABLE,
        CommentBlockIndexNames[ST][CBX],
        CommentBlockIndexFieldNames[ST][CBX]);
    end;
  end;
end;

function TDBXMySQL.OpenOrCreateUserTable(out Msg:string):boolean;
var
  SQL: TSQLQuery;
  Transaction: TDBXTransaction;
begin
  try
    if not TableExists(SqlConnection, S_USER_TABLE) then
    begin
      SQL := TSQLQuery.Create(nil);
      SQL.SQLConnection := SqlConnection;
      try
        Transaction := SqlConnection.BeginTransaction;
        try

          SQL.SQL.Add('CREATE TABLE `USER_TABLE`'
                    + '('
                    + '  `'+S_USER_KEY+'`	CHAR(38) NOT NULL,'
                    + '  `'+S_USER_INSTA_UNAME+'`	VARCHAR(64),'
                    + '  `'+S_USER_INSTA_UID+'`	VARCHAR(64),'
                    + '  `'+S_USER_INSTA_VERIFIED+'`	SMALLINT,'
                    + '  `'+S_USER_INSTA_PROF_URL+'`	TEXT,'
                    + '  `'+S_USER_INSTA_FULL_NAME+'`	TEXT,'
                    + '  `'+S_USER_INSTA_BIO+'`	TEXT,'
                    + '  `'+S_USER_INSTA_FOLLOWS_COUNT+'`	BIGINT,'
                    + '  `'+S_USER_INSTA_FOLLOWER_COUNT+'`	BIGINT,'
                    + '  `'+S_USER_TWITTER_UNAME+'`	VARCHAR(64),'
                    + '  `'+S_USER_TWITTER_UID+'`	VARCHAR(64),'
                    + '  `'+S_USER_TWITTER_VERIFIED+'`	SMALLINT,'
                    + '  `'+S_USER_TWITTER_PROF_URL+'`	TEXT,'
                    + '  `'+S_USER_TWITTER_FULL_NAME+'`	TEXT,'
                    + '  `'+S_USER_TWITTER_BIO+'`	TEXT,'
                    + '  `'+S_USER_TWITTER_FOLLOWS_COUNT+'`	BIGINT,'
                    + '  `'+S_USER_TWITTER_FOLLOWER_COUNT+'`	BIGINT,'
                    + '  `'+S_LAST_UPDATED+'`	DOUBLE,'
                    + '  `'+S_USER_INTEREST+'` SMALLINT'
                    + ');');
          SQL.ExecSQL;

          SQL.SQL.Clear;
          SQL.SQL.Add('ALTER TABLE ' + S_USER_TABLE +
                      ' ADD CONSTRAINT ' + S_INDEX_USER_TABLE_PRIMARY +
                      ' PRIMARY KEY (' + S_USER_KEY  + ')');
          SQL.ExecSQL;

          SqlConnection.CommitFreeAndNil(Transaction);
        except
          on EDatabaseError do
          begin
            SqlConnection.RollbackFreeAndNil(Transaction);
            raise;
          end;
        end;
      finally
        SQL.Free;
      end;
    end;
  except
    on E: EDatabaseError do
    begin
      Msg := E.Message;
      result := false;
      exit;
    end;
  end;
  PatchInUTableIndexes;
  result := true;
end;

function TDBXMySQL.OpenOrCreateMediaTable(out Msg:string): boolean;
var
  SQL: TSQLQuery;
  Transaction: TDBXTransaction;
begin
  try
    SQL := TSQLQuery.Create(nil);
    try
      SQL.SQLConnection  := SqlConnection;
      if not TableExists(SqlConnection, S_MEDIA_TABLE) then
      begin
        Transaction := SqlConnection.BeginTransaction;
        try
          SQL.SQL.Add('CREATE TABLE `MEDIA_TABLE`' +
                    '(' +
                    '  `'+S_MEDIA_KEY+'`	CHAR(38) NOT NULL,' +
                    '  `'+S_MEDIA_OWNER_KEY+'`	CHAR(38) NOT NULL,' +
                    '  `'+S_MEDIA_DATE+'`	DOUBLE,' +
                    '  `'+S_MEDIA_DATA_STR+'`	TEXT,' +
                    '  `'+S_MEDIA_TYPE+'` INTEGER,' +
                    '  `'+S_MEDIA_RESOURCE_URL+'`	TEXT,' +
                    '  `'+S_MEDIA_INSTA_MEDIA_ID+'`	VARCHAR(64),' +
                    '  `'+S_MEDIA_INSTA_OWNER_ID+'`	VARCHAR(64),' +
                    '  `'+S_MEDIA_INSTA_MEDIA_CODE+'`	VARCHAR(32),' +
                    '  `'+S_MEDIA_TWITTER_MEDIA_ID+'`	VARCHAR(64),' +
                    '  `'+S_MEDIA_TWITTER_OWNER_ID+'`	VARCHAR(64),' +
                    '  `'+S_MEDIA_TWITTER_MEDIA_CODE+'`	VARCHAR(32),' +
                    '  `'+S_LAST_UPDATED+'`	DOUBLE' +
                    ');');
          SQL.ExecSql;

          SQL.SQL.Clear;
          SQL.SQL.Add('ALTER TABLE ' + S_MEDIA_TABLE +
                      ' ADD CONSTRAINT ' + S_INDEX_MEDIA_TABLE_PRIMARY +
                      ' PRIMARY KEY (' + S_MEDIA_KEY  + ')');
          SQL.ExecSQL;

          SQL.SQL.Clear;
          SQL.SQL.Add('ALTER TABLE ' + S_MEDIA_TABLE +
                      ' ADD CONSTRAINT ' + S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID +
                      ' FOREIGN KEY (' + S_MEDIA_OWNER_KEY  +
                      ') REFERENCES ' + S_USER_TABLE +
                      '(' + S_USER_KEY + ')' +
                      ' ON UPDATE CASCADE' +
                      ' ON DELETE RESTRICT');
          SQL.ExecSQL;
          SqlConnection.CommitFreeAndNil(Transaction);
        except
          on EDatabaseError do
          begin
            SqlConnection.RollbackFreeAndNil(Transaction);
            raise;
          end;
        end;
      end;
    finally
      SQL.Free;
    end;
  except
    on E: EDatabaseError do
    begin
      Msg := E.Message;
      result := false;
      exit;
    end;
  end;
  PatchInMTableIndexes;
  result := true;
end;

function TDBXMySQL.OpenOrCreateCommentTable(out Msg:string): boolean;
var
  SQL: TSQLQuery;
  Transaction: TDBXTransaction;
begin
  try
    SQL := TSQLQuery.Create(nil);
    try
      SQL.SQLConnection  := SqlConnection;
      if not TableExists(SqlConnection, S_COMMENT_TABLE) then
      begin
        Transaction := SqlConnection.BeginTransaction;
        try
          SQL.SQL.Add('CREATE TABLE `COMMENT_TABLE`' +
                      '(' +
                      '  `'+S_COMMENT_KEY+'`	CHAR(38) NOT NULL,' +
                      '  `'+S_COMMENT_OWNER_KEY+'`	CHAR(38) NOT NULL,' +
                      '  `'+S_COMMENT_MEDIA_KEY+'`	CHAR(38) NOT NULL,' +
                      '  `'+S_COMMENT_INSTA_COMMENT_ID+'`	VARCHAR(64),' +
                      '  `'+S_COMMENT_INSTA_OWNER_ID+'`	VARCHAR(64),' +
                      '  `'+S_COMMENT_TWITTER_COMMENT_ID+'`	VARCHAR(64),' +
                      '  `'+S_COMMENT_TWITTER_OWNER_ID+'`	VARCHAR(64),' +
                      '  `'+S_COMMENT_DATE+'`	DOUBLE,' +
                      '  `'+S_COMMENT_DATA_STR+'` TEXT,' +
                      '  `'+S_COMMENT_TYPE+'` INTEGER,' +
                      '  `'+S_LAST_UPDATED+'`	DOUBLE' +
                      ');');
          SQL.ExecSQL;

          SQL.SQL.Clear;
          SQL.SQL.Add('ALTER TABLE ' + S_COMMENT_TABLE +
                      ' ADD CONSTRAINT ' + S_INDEX_COMMENT_TABLE_PRIMARY +
                      ' PRIMARY KEY (' + S_COMMENT_KEY  + ')');
          SQL.ExecSQL;

          SQL.SQL.Clear;
          SQL.SQL.Add('ALTER TABLE ' + S_COMMENT_TABLE +
                      ' ADD CONSTRAINT ' + S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID +
                      ' FOREIGN KEY (' + S_COMMENT_OWNER_KEY  +
                      ') REFERENCES ' + S_USER_TABLE +
                      '(' + S_USER_KEY + ')' +
                      ' ON UPDATE CASCADE' +
                      ' ON DELETE RESTRICT');
          SQL.ExecSQL;
          SQL.SQL.Clear;
          SQL.SQL.Add('ALTER TABLE ' + S_COMMENT_TABLE +
                      ' ADD CONSTRAINT ' + S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID +
                      ' FOREIGN KEY (' + S_COMMENT_MEDIA_KEY  +
                      ') REFERENCES ' + S_MEDIA_TABLE +
                      '(' + S_MEDIA_KEY + ')' +
                      ' ON UPDATE CASCADE' +
                      ' ON DELETE RESTRICT');
          SQL.ExecSQL;

          SqlConnection.CommitFreeAndNil(Transaction);
        except
          on EDatabaseError do
          begin
            SqlConnection.RollbackFreeAndNil(Transaction);
            raise;
          end;
        end;
      end;
    finally
      SQL.Free;
    end;
  except
    on E: EDatabaseError do
    begin
      Msg := E.Message;
      result := false;
      exit;
    end;
  end;
  PatchInCTableIndexes;
  result := true;
end;

function TDBXMySQL.DestroyTablesWorker: boolean;
var
  SQL: TSQLQuery;
  Transaction: TDBXTransaction;
  Msg: string;
begin
  DBOpenAndSetUser(Msg, false);
  if not Assigned(SqlConnection) then
  begin
    result := false;
    exit;
  end;
  result := true;
  SQL := TSQLQuery.Create(nil);
  try
    Transaction := SqlConnection.BeginTransaction;
    try
      //Do things individually to cater for damaged table / index structure.
      SQL.SQLConnection := SqlConnection;

      SQL.SQL.Clear;
      SQL.SQL.Add('ALTER TABLE ' + S_COMMENT_TABLE);
      SQL.SQL.Add('DROP FOREIGN KEY ' + S_COMMENT_CONSTRAINT_FK_USER_TABLE_UID);
      SQL.ExecSQL;

      SQL.SQL.Clear;
      SQL.SQL.Add('ALTER TABLE ' + S_COMMENT_TABLE);
      SQL.SQL.Add('DROP FOREIGN KEY ' + S_COMMENT_CONSTRAINT_FK_MEDIA_TABLE_MID);
      SQL.ExecSQL;

      SQL.SQL.Clear;
      SQL.SQL.Add('ALTER TABLE ' + S_MEDIA_TABLE);
      SQL.SQL.Add('DROP FOREIGN KEY ' + S_MEDIA_CONSTRAINT_FK_USER_TABLE_UID);
      SQL.ExecSQL;

      SQL.SQL.Clear;
      SQL.SQL.Add('DROP TABLE ' + S_COMMENT_TABLE + ';');
      SQL.ExecSQL;

      SQL.SQL.Clear;
      SQL.SQL.Add('DROP TABLE ' + S_MEDIA_TABLE+ ';');
      SQL.ExecSQL;

      SQL.SQL.Clear;
      SQL.SQL.Add('DROP TABLE ' + S_USER_TABLE+ ';');
      SQL.ExecSQL;

      SqlConnection.CommitFreeAndNil(Transaction);
    except
      on E: EDatabaseError do
      begin
        SqlConnection.RollbackFreeAndNil(Transaction);
        result := false;
      end;
    end;
  finally
    SQL.Free;
  end;
end;

end.
