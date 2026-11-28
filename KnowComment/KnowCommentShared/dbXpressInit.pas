unit dbXpressInit;

{ Copyright Martin Harvey December 2016. }

interface

uses Trackables, DataObjects, DBGeneric, Data.SQLExpr, Data.DBXCommon,
  DBXInterbase, SimpleDS, DLList, Classes, DB, SyncObjs, SysUtils;

type
  TDatasetAccessor = record
    Table: TSimpleDataset;
    Query: TSQLQuery;
    TableUsed, QueryUsed: boolean;
  end;

  TDatasetAccessors = array [TKListLevel] of TDatasetAccessor;

  TDBXDB = class(TGenericDB)
  private
    FDBName: string;
    FSQLConnection: TSQLConnection;
    FLock: TCriticalSection;
    FOpenDatasetsCount: integer;
    FTrans: TDBXTransaction;
    FErrorInTrans: string; // Accumulated whether error in transaction.
    FAccessors: TDatasetAccessors;
    FTablesSetup: boolean;
  protected
    function GetIsInit: boolean; override;
    function HasOpenDatasets: boolean;
    function GetFieldQuoteChar: string; virtual; abstract;

    procedure CreateHelpers;
    procedure DestroyHelpers;
    procedure ReEvalHelpers(NewTablesSetup, NewConnected: boolean);
    procedure SetSQLConnection(NewConnection: TSQLConnection);
    procedure SetTablesSetUp(NewTablesSetup: boolean);

    procedure DBOpenAndSetUser(out Msg: string; AllowCreate: boolean); virtual; abstract;
    function OpenOrCreateUserTable(out Msg: string): boolean; virtual; abstract;
    function OpenOrCreateMediaTable(out Msg: string): boolean; virtual; abstract;
    function OpenOrCreateCommentTable(out Msg: string): boolean; virtual; abstract;
    function TableExists(C: TSQLConnection; TableName: string): boolean;

    // Do not need to acquire locks in these fn's.
    function DestroyTablesWorker: boolean; virtual; abstract;

    property SQLConnection: TSQLConnection read FSQLConnection write SetSQLConnection;
    property TablesSetUp: boolean read FTablesSetup write SetTablesSetUp;
    property Lock: TCriticalSection read FLock;

    procedure IncOpenDatasets;
    procedure DecOpenDatasets(Errcase: string = '');
  public
    constructor Create;
    destructor Destroy; override;
    function InitDBAndTables(out Msg: string): boolean; override;
    function FiniDBAndTables: boolean; override;
    function DestroyTables: boolean; override;
    function ClearTablesData: boolean; override;

    function PersisterClass(Level: TKListLevel): TDBPersistClass; override;

    function GetDataset(Level: TKListLevel): TDataset;
    procedure PutDataset(Dataset: TDataset);
    function GetSQLQuery(SQL: string; Level: TKListLevel): TDataset;
    procedure PutSQLQuery(Query: TDataset);

    property DBName: string read FDBName write FDBName;
    property FieldQuoteChar: string read GetFieldQuoteChar;
  end;

implementation

uses
  IOUtils, dbXpressPersist, GlobalLog, DBSchemaStrs;

const
  S_APPLY_UPDATES_FAILED = 'Table ApplyUpdates failed (concurrency conflict?), op aborted.';
  S_DATASET_COMMAND_TEXT = 'Dataset is table, cmd text: ';
  S_SQL_QUERY_COMMAND_TEXT = 'Dataset is SQL query, SQL: ';

  { TDBXDB }

constructor TDBXDB.Create;
begin
  inherited;
  FLock := TCriticalSection.Create;
end;

destructor TDBXDB.Destroy;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Destructor, acquiring lock');
{$ENDIF}
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Destructor, got lock');
{$ENDIF}
  try
    TablesSetUp := false;
    FSQLConnection.Free;
    FSQLConnection := nil;
  finally
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'Destructor, releasing lock');
{$ENDIF}
    FLock.Release;
  end;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Destructor, freeing lock');
{$ENDIF}
  FLock.Free;
  inherited;
end;

procedure TDBXDB.CreateHelpers;
var
  LL: TKListLevel;
  ST: TKSiteType;
  UBX: TUserBlockIndexes;
  MBX: TMediaBlockIndexes;
  CBX: TCommentBlockIndexes;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Creating helpers');
{$ENDIF}
  for LL := Low(LL) to High(LL) do
  begin
    FAccessors[LL].Table := TSimpleDataset.Create(nil);
    FAccessors[LL].Table.Connection := FSQLConnection;
    FAccessors[LL].Table.Dataset.CommandType := ctTable;
    FAccessors[LL].Query := TSQLQuery.Create(nil);
    FAccessors[LL].Query.SQLConnection := FSQLConnection;
    FAccessors[LL].TableUsed := false;
    FAccessors[LL].QueryUsed := false;
    case LL of
      klUserList:
        begin
          with FAccessors[LL].Table as TSimpleDataset do
          begin
            Dataset.CommandText := S_USER_TABLE;
            Open;
            AddIndex(S_INDEX_USER_TABLE_PRIMARY, S_USER_KEY, [ixPrimary, ixUnique]);
            for ST := Low(ST) to High(ST) do
            begin
              for UBX := Low(UBX) to High(UBX) do
              begin
                AddIndex(
                  UserBlockIndexNames[ST][UBX],
                  UserBlockIndexFieldNames[ST][UBX], []);
              end;
            end;
          end;
        end;
      klMediaList:
        begin
          with FAccessors[LL].Table as TSimpleDataset do
          begin
            Dataset.CommandText := S_MEDIA_TABLE;
            Open;
            AddIndex(S_INDEX_MEDIA_TABLE_PRIMARY, S_MEDIA_KEY, [ixPrimary, ixUnique]);
            AddIndex(S_INDEX_MEDIA_OWNER_KEY, S_MEDIA_OWNER_KEY, []);
            for ST := Low(ST) to High(ST) do
            begin
              for MBX := Low(MBX) to High(MBX) do
              begin
                AddIndex(
                  MediaBlockIndexNames[ST][MBX],
                  MediaBlockIndexFieldNames[ST][MBX], []);
              end;
            end;
          end;
        end;
      klCommentList:
        begin
          with FAccessors[LL].Table as TSimpleDataset do
          begin
            Dataset.CommandText := S_COMMENT_TABLE;
            Open;
            AddIndex(S_INDEX_COMMENT_TABLE_PRIMARY, S_COMMENT_KEY, [ixPrimary, ixUnique]);
            AddIndex(S_INDEX_COMMENT_MEDIA_KEY, S_COMMENT_MEDIA_KEY, []);
            AddIndex(S_INDEX_COMMENT_OWNER_KEY, S_COMMENT_OWNER_KEY, []);
            for ST := Low(ST) to High(ST) do
            begin
              for CBX := Low(CBX) to High(CBX) do
              begin
                AddIndex(
                  CommentBlockIndexNames[ST][CBX],
                  CommentBlockIndexFieldNames[ST][CBX], []);
              end;
            end;
          end;
        end
    else
      Assert(false);
    end;
  end;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Created helpers');
{$ENDIF}
end;

procedure TDBXDB.DestroyHelpers;
var
  LL: TKListLevel;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Destroying helpers');
{$ENDIF}
  for LL := Low(LL) to High(LL) do
  begin
    with FAccessors[LL] do
    begin
      Assert(not(TableUsed or QueryUsed));
      Table.Free;
      Query.Free;
    end;
  end;
  FillChar(FAccessors, sizeof(FAccessors), 0);
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Destroyed helpers');
{$ENDIF}
end;

procedure TDBXDB.ReEvalHelpers(NewTablesSetup, NewConnected: boolean);
var
  OldHelpersUp, NewHelpersUp: boolean;
begin
  OldHelpersUp := FTablesSetup and Assigned(FSQLConnection);
  NewHelpersUp := NewTablesSetup and NewConnected;
  if OldHelpersUp and not NewHelpersUp then
    DestroyHelpers
  else if (not OldHelpersUp) and NewHelpersUp then
    CreateHelpers
end;

procedure TDBXDB.SetTablesSetUp(NewTablesSetup: boolean);
begin
  ReEvalHelpers(NewTablesSetup, Assigned(FSQLConnection));
  FTablesSetup := NewTablesSetup;
end;

procedure TDBXDB.SetSQLConnection(NewConnection: TSQLConnection);
begin
  if Assigned(NewConnection) and not Assigned(FSQLConnection) then
  begin
    FSQLConnection := NewConnection;
    ReEvalHelpers(FTablesSetup, Assigned(FSQLConnection));
  end
  else if (not Assigned(NewConnection)) and Assigned(FSQLConnection) then
  begin
    ReEvalHelpers(FTablesSetup, Assigned(NewConnection));
    FSQLConnection.Free;
    FSQLConnection := NewConnection;
  end
  else
    Assert(false);
end;

function TDBXDB.TableExists(C: TSQLConnection; TableName: string): boolean;
var
  Names: TStrings;
  i: integer;
begin
  result := false;
  Names := TStringList.Create;
  try
    C.GetTableNames(Names);
    for i := 0 to Pred(Names.Count) do
    begin
      if CompareText(Names[i].Trim, TableName.Trim) = 0 then
      begin
        result := true;
        break;
      end;
    end;
  finally
    Names.Free;
  end;
end;

function TDBXDB.PersisterClass(Level: TKListLevel): TDBPersistClass;
begin
  case Level of
    klUserList:
      result := TDBXUserProfilePersister;
    klMediaList:
      result := TDBXMediaItemPersister;
    klCommentList:
      result := TDBXCommentItemPersister;
  else
    Assert(false);
    result := nil;
  end;
end;

function TDBXDB.InitDBAndTables(out Msg: string): boolean;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'InitDB, acquiring lock');
{$ENDIF}
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'InitDB, got lock');
{$ENDIF}
  try
    if HasOpenDatasets then
    begin
      result := false;
      exit;
    end;
    DBOpenAndSetUser(Msg, true);
    if not Assigned(SQLConnection) then
    begin
      result := false;
      exit;
    end;
    result := OpenOrCreateUserTable(Msg);
    result := result and OpenOrCreateMediaTable(Msg);
    result := result and OpenOrCreateCommentTable(Msg);
    TablesSetUp := result;
    if not result then
      SQLConnection := nil;
  finally
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'InitDB, releasing lock');
{$ENDIF}
    FLock.Release;
  end;
end;

function TDBXDB.FiniDBAndTables: boolean;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'FiniDB, acquiring lock');
{$ENDIF}
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'FiniDB, got lock');
{$ENDIF}
  try
    if HasOpenDatasets then
    begin
      result := false;
      exit;
    end;
    SQLConnection := nil;
    result := true;
  finally
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'FiniDB, releasing lock');
{$ENDIF}
    FLock.Release;
  end;
end;

function TDBXDB.DestroyTables: boolean;
var
  Msg: string;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Destroy tables, acquiring lock');
{$ENDIF}
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Destroy tables, got lock');
{$ENDIF}
  try
    if HasOpenDatasets then
    begin
      result := false;
      exit;
    end;
    DBOpenAndSetUser(Msg, false);
    if not Assigned(SQLConnection) then
    begin
      result := false;
      exit;
    end;
    result := DestroyTablesWorker;
  finally
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'Destroy tables, releasing lock');
{$ENDIF}
    FLock.Release;
  end;
end;

function TDBXDB.ClearTablesData: boolean;
var
  Table: TSimpleDataset;
  Transaction: TDBXTransaction;
  Msg: string;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Clear tables, acquiring lock');
{$ENDIF}
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Clear tables, got lock');
{$ENDIF}
  try
    if HasOpenDatasets then
    begin
      result := false;
      exit;
    end;
    DBOpenAndSetUser(Msg, false);
    if not Assigned(SQLConnection) then
    begin
      result := false;
      exit;
    end;
    result := true;
    Table := TSimpleDataset.Create(nil);
    try
      Transaction := SQLConnection.BeginTransaction;
      try
        Table.Connection := SQLConnection;
        Table.Dataset.CommandType := ctTable;
        Table.Dataset.CommandText := S_COMMENT_TABLE;
        Table.Open;
        Table.EmptyDataSet;
        Table.Close;

        Table.Dataset.CommandText := S_MEDIA_TABLE;
        Table.Open;
        Table.EmptyDataSet;
        Table.Close;

        Table.Dataset.CommandText := S_USER_TABLE;
        Table.Open;
        Table.EmptyDataSet;
        Table.Close;
        SQLConnection.CommitFreeAndNil(Transaction);
      except
        on EDatabaseError do
        begin
          SQLConnection.RollbackFreeAndNil(Transaction);
          result := false;
        end;
      end;
    finally
      Table.Free;
    end;
  finally
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'Clear tables, releasing lock');
{$ENDIF}
    FLock.Release;
  end;
end;

procedure TDBXDB.IncOpenDatasets;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Open dataset...');
{$ENDIF}
  Assert(FOpenDatasetsCount >= 0);
  Assert((FOpenDatasetsCount > 0) = Assigned(FTrans));
  if FOpenDatasetsCount = 0 then
  begin
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'First dataset, start transaction...');
{$ENDIF}
    FErrorInTrans := '';
    FTrans := FSQLConnection.BeginTransaction;
  end;
  Inc(FOpenDatasetsCount);
  Assert((FOpenDatasetsCount > 0) = Assigned(FTrans));
  Assert(FOpenDatasetsCount >= 0);
end;

procedure TDBXDB.DecOpenDatasets(Errcase: string);
begin
  Assert(FOpenDatasetsCount >= 0);
  Assert((FOpenDatasetsCount > 0) = Assigned(FTrans));
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Close dataset....');
{$ENDIF}
  if Length(FErrorInTrans) = 0 then
    if Length(Errcase) > 0 then
      FErrorInTrans := Errcase;

{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Close dataset, accumulated errors:' + FErrorInTrans);
{$ENDIF}
  Dec(FOpenDatasetsCount);
  if FOpenDatasetsCount = 0 then
  begin
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'Close dataset, last open dataset.');
{$ENDIF}
    if Length(FErrorInTrans) = 0 then
    begin
      try
{$IFDEF DEBUG_DATABASE_LOCKING}
        GLogLog(SV_INFO, 'Put dataset, commit transaction.');
{$ENDIF}
        SQLConnection.CommitFreeAndNil(FTrans);
      except
        on E: EDatabaseError do
        begin
{$IFDEF DEBUG_DATABASE_LOCKING}
          GLogLog(SV_INFO, 'Put dataset, rollback after failed commit.');
{$ENDIF}
          FErrorInTrans := E.ClassName + ' ' + E.Message;
        end;
      end;
    end;
    if Length(FErrorInTrans) > 0 then
    begin
{$IFDEF DEBUG_DATABASE_LOCKING}
      GLogLog(SV_INFO, 'Close dataset, rollback transaction after failed open or failed commit.');
{$ENDIF}
      FSQLConnection.RollbackFreeAndNil(FTrans);

      raise EDatabaseError.Create(FErrorInTrans);
    end;
  end;
  Assert(FOpenDatasetsCount >= 0);
  Assert((FOpenDatasetsCount > 0) = Assigned(FTrans));
end;

function TDBXDB.GetDataset(Level: TKListLevel): TDataset;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Get dataset, acquiring lock');
{$ENDIF}
  result := nil;
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Get dataset, got lock');
{$ENDIF}
  try
    if Assigned(FSQLConnection) then
    begin
      if not FAccessors[Level].TableUsed then
      begin
        IncOpenDatasets;
        result := FAccessors[Level].Table;
        FAccessors[Level].TableUsed := true;
      end;
    end;
  finally
    if not Assigned(result) then
    begin
{$IFDEF DEBUG_DATABASE_LOCKING}
      GLogLog(SV_WARN, 'Get dataset, releasing lock');
{$ENDIF}
      FLock.Release;
    end
    else
    begin
{$IFDEF DEBUG_DATABASE_LOCKING}
      GLogLog(SV_INFO, 'Get dataset, retaining lock');
{$ENDIF}
    end;
  end;
end;

procedure TDBXDB.PutDataset(Dataset: TDataset);
var
  Level: TKListLevel;
  Freed: boolean;
begin
  if not Assigned(Dataset) then
  begin
    Assert(false);
    exit;
  end;

  Freed := false;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Put dataset, acquiring lock');
{$ENDIF}
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Put dataset, got lock');
{$ENDIF}
  try
    for Level := Low(Level) to High(Level) do
    begin
      if FAccessors[Level].Table = Dataset then
      begin
        Assert(FAccessors[Level].TableUsed);
        FAccessors[Level].TableUsed := false;
        Freed := true;

        if FAccessors[Level].Table.ApplyUpdates(0) > 0 then
        begin
{$IFDEF DEBUG_DATABASE_LOCKING}
          GLogLog(SV_INFO, 'Put dataset, apply updates failed.');
{$ENDIF}
          FAccessors[Level].Table.CancelUpdates;
          DecOpenDatasets(S_APPLY_UPDATES_FAILED)
        end
        else
          DecOpenDatasets;

        break;
      end;
    end;
    Assert(Freed);
  finally
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'Put dataset, got lock');
{$ENDIF}
    if Freed then
    begin
{$IFDEF DEBUG_DATABASE_LOCKING}
      GLogLog(SV_INFO, 'Put dataset, releasing lock from previous acquire');
{$ENDIF}
      FLock.Release;
    end
    else
    begin
{$IFDEF DEBUG_DATABASE_LOCKING}
      GLogLog(SV_WARN, 'Put dataset, no previous lock acquisition');
{$ENDIF}
    end;
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'Put dataset, releasing lock from local acquisition');
{$ENDIF}
    FLock.Release;
  end;
end;

function TDBXDB.GetSQLQuery(SQL: string; Level: TKListLevel): TDataset;
var
  SQuery: TSQLQuery;
begin
  result := nil;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Get SQL query, acquiring lock');
{$ENDIF}
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Get SQL query, got lock');
{$ENDIF}
  try
    if Assigned(FSQLConnection) then
    begin
      if not FAccessors[Level].QueryUsed then
      begin
        IncOpenDatasets;
        result := FAccessors[Level].Query;
        FAccessors[Level].QueryUsed := true;
        SQuery := result as TSQLQuery;
        try
{$IFDEF DEBUG_DATABASE_LOCKING}
          GLogLog(SV_INFO, 'Get query, exec SQL and open.');
{$ENDIF}
          SQuery.SQL.Clear;
          SQuery.SQL.Add(SQL);
          SQuery.Open;
        except
          on E: EDatabaseError do
          begin
{$IFDEF DEBUG_DATABASE_LOCKING}
            GLogLog(SV_INFO, 'Get query, SQL open failed.');
{$ENDIF}
            result := nil;
            FAccessors[Level].QueryUsed := false;
            // One of these two will raise.
            DecOpenDatasets(E.Message);
            raise;
          end;
        end;
      end;
    end;
  finally
    if not Assigned(result) then
    begin
{$IFDEF DEBUG_DATABASE_LOCKING}
      GLogLog(SV_WARN, 'Get SQL query, releasing lock');
{$ENDIF}
      FLock.Release;
    end
    else
    begin
{$IFDEF DEBUG_DATABASE_LOCKING}
      GLogLog(SV_INFO, 'Get SQL query, retaining lock');
{$ENDIF}
    end;
  end;
end;

procedure TDBXDB.PutSQLQuery(Query: TDataset);
var
  Freed: boolean;
  Level: TKListLevel;
begin
  if not Assigned(Query) then
  begin
    Assert(false);
    exit;
  end;

  Freed := false;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Put SQL query, acquiring lock');
{$ENDIF}
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Put SQL query, got lock');
{$ENDIF}
  try
    for Level := Low(Level) to High(Level) do
    begin
      if FAccessors[Level].Query = Query then
      begin
        Assert(FAccessors[Level].QueryUsed);
        FAccessors[Level].QueryUsed := false;
        Freed := true;
        FAccessors[Level].Query.Close;

        DecOpenDatasets;
        break;
      end;
    end;
    Assert(Freed);
  finally
    if Freed then
    begin
{$IFDEF DEBUG_DATABASE_LOCKING}
      GLogLog(SV_INFO, 'Put SQL query, releasing lock from previous acquire');
{$ENDIF}
      FLock.Release;
    end
    else
    begin
{$IFDEF DEBUG_DATABASE_LOCKING}
      GLogLog(SV_WARN, 'Put SQL query, no previous lock acquisition');
{$ENDIF}
    end;
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'Put SQL query, releasing lock from local acquisition');
{$ENDIF}
    FLock.Release;
  end;
end;

function TDBXDB.HasOpenDatasets: boolean;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Open dataset check, acquiring lock');
{$ENDIF}
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Open dataset check, got lock');
{$ENDIF}
  try
    Assert(FOpenDatasetsCount >= 0);
    result := FOpenDatasetsCount > 0;
  finally
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'Open dataset check, releasing lock');
{$ENDIF}
    FLock.Release;
  end;
end;

function TDBXDB.GetIsInit: boolean;
begin
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Init check, acquiring lock');
{$ENDIF}
  FLock.Acquire;
{$IFDEF DEBUG_DATABASE_LOCKING}
  GLogLog(SV_INFO, 'Init check, got lock');
{$ENDIF}
  try
    result := Assigned(FSQLConnection);
  finally
{$IFDEF DEBUG_DATABASE_LOCKING}
    GLogLog(SV_INFO, 'Init check, releasing lock');
{$ENDIF}
    FLock.Release;
  end;
end;

end.
