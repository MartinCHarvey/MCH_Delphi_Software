unit LoaderForm;

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, InstagramImporters, FMX.Layouts, FMX.Memo, DataObjects,
  TwitterImporters, BatchLoader, DataStore, DBGeneric, FMX.ListBox, FMX.Edit,
  MemDBInit, dbxInitMySQL, MemDB2Misc;

type
  TSqlImportData = record
    SQLDB: TDBXMySQL;
    MemDB: TMemKDB;
    SQLDatastore, MemDBDatastore: TLoaderDatastore;
    SQLBatchLoader: TBatchLoader;
    ImportTree: TKUserList;
    UserCursor: TKUserProfile;
    MediaCursor: TKMediaItem;
  end;

  TKCLoaderFrm = class(TForm)
    Memo1: TMemo;
    UTreeLoad: TButton;
    URefresh: TButton;
    ForceClose: TButton;
    AddUser: TButton;
    NewUserEdit: TEdit;
    SiteTypeCombo: TComboBox;
    ListBoxInstagramItem: TListBoxItem;
    ListBoxTwitterItem: TListBoxItem;
    IncreaseInterestVerified: TButton;
    DeleteUser: TButton;
    MemSnapshot: TButton;
    StatsMemo: TMemo;
    StatsTimer: TTimer;
    MySQLLoad: TButton;
    Label1: TLabel;
    DBLabel: TLabel;
    CheckpointBtn: TButton;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure FormCloseQuery(Sender: TObject; var CanClose: Boolean);
    procedure ClearTblsBtnClick(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure UTreeLoadClick(Sender: TObject);
    procedure URefreshClick(Sender: TObject);
    procedure ForceCloseClick(Sender: TObject);
    procedure AddUserClick(Sender: TObject);
    procedure IncreaseInterestVerifiedClick(Sender: TObject);
    procedure DecreaseInterestAllClick(Sender: TObject);
    procedure DeleteUserClick(Sender: TObject);
    procedure MemSnapshotClick(Sender: TObject);
    procedure StatsTimerTimer(Sender: TObject);
    procedure MySQLLoadClick(Sender: TObject);
    procedure CheckpointBtnClick(Sender: TObject);
  private
    { Private declarations }
    FForceClose: boolean;
    function RequestCompleteCommon: boolean;
    procedure CloseCommon(var CanClose: boolean);
    procedure LogToMemo(const Str: string);
    procedure HandleMultiOpCompleted(Sender: TObject; MultiOp: TMultiOp);
    procedure HandleImportBatchLoaderMultiOpCompleted(Sender: TObject; MultiOp: TMultiOp);
    procedure HandleMemDatastoreCompletion(Sender: TObject; Seq: TOpSequence);
    procedure ClearImportData;
  public
    { Public declarations }
    FAttemptingClose: boolean;
    FInProgress: integer;
    FDB: TGenericDB;
    FDataStore: TLoaderDataStore;
    FBatchLoader: TBatchLoader;
    FSQLImportData: TSqlImportData;
    FFirstTime: boolean;
  end;

var
  KCLoaderFrm: TKCLoaderFrm;
  DefaultSince: TDateTime;

function MakeDBRootDir(Dir: string): boolean;

implementation

{$R *.fmx}

uses FetcherParser, Importers, CommonPool, Trackables, IOUtils,
     IndexedStore, FMX.Messages, OAuth2, GlobalLog, dbXpressInit,
     Windows, MemDB2;

const
{$IFOPT C+}
  DATA_ROOT_LOCATION = 'D:\Knowcomment_Debug\';
{$ELSE}
  DATA_ROOT_LOCATION = 'D:\Knowcomment\';
{$ENDIF}
  DATA_ROOT_LOCATION_2 = 'D:\Knowcomment_V2Stream\';
  SQL_DB_NAME = 'KNOWCOMMENT';

procedure TKCLoaderFrm.LogToMemo(const Str: string);
begin
  GLogLog(SV_INFO, 'MEMO: ' + Str);
  Memo1.Lines.Add(Str);
  Memo1.ScrollTo(0, -100);
end;

procedure TKCLoaderFrm.MemSnapshotClick(Sender: TObject);
{$IFOPT C+}
var
  Snapshot: TStrings;
  Dbg: TStrings;
{$ENDIF}
begin
{$IFOPT C+}
  Snapshot := AppGlobalTracker.GetTrackedMemUsage;
  Snapshot.Add('');
{$IFDEF DEBUG_MEM_MIRROR}
  Dbg := FBatchLoader.DataStore.GetDBGInfo;
  try
    Snapshot.AddStrings(Dbg);
  finally
    DBg.Free;
  end;
{$ENDIF}
  try
    Memo1.Lines.AddStrings(Snapshot);
    Memo1.ScrollTo(0, -100);
  finally
    Snapshot.Free;
  end;
{$ENDIF}
end;

procedure TKCLoaderFrm.CheckpointBtnClick(Sender: TObject);
begin
  (FDB as TMemKDB).MemDB.Checkpoint;
end;

procedure TKCLoaderFrm.ClearImportData;
begin
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Loader form clear import data');
{$ENDIF}
  with FSQLImportData do
  begin
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Import delete SQL batch loader: ' + IntToHex(Int64(SQLBatchLoader), 16));
  GLogLog(SV_INFO, 'Import delete SQL datastore: ' + IntToHex(Int64(SQLDatastore), 16));
  GLogLog(SV_INFO, 'Import delete MemDB datastore: ' + IntToHex(Int64(MemDBDatastore), 16));
{$ENDIF}
    SQLBatchLoader.Free;
    MemDBDatastore.Free;
    SQLDatastore.Free;
    MemDB.Free;
    SQLDB.Free;
    ImportTree.Free;
  end;
  FillChar(FSQLImportData, sizeof(FSQLImportData), 0);
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Loader form clear import data done');
{$ENDIF}
end;

procedure TKCLoaderFrm.HandleMemDatastoreCompletion(Sender: TObject; Seq: TOpSequence);
var
  UP: TKUSerProfile;
  TmpUL: TKUSerList;
begin
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Main form Mem datastore completion');
{$ENDIF}
  //This handler here just for the one command.
  if not Seq.OK then
  begin
    Memo1.Lines.Add('Oh dear, all gone wrong in datastore completion: ' +Seq.Msg);
    ClearImportData;
    FormCreate(Sender);
    exit;
  end
  else
  begin
    if Seq is TWriteUserListSequence then
    begin
      //Create a user list with all the trees we need to persist.
      //And a couple of cursor values, and kick off the first one.
      FSQLImportData.ImportTree := TKUserList.Create;
      TmpUL := FSqlImportData.SQLDatastore.GetTemporaryTopLevelList;
      try
        with TmpUL do
        begin
          UP := AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
          while Assigned(UP) do
          begin
            if UP.InterestLevel > kpiFetchUserForRefs then
              FSQLImportData.ImportTree.Add(TKKeyedObject.DeepClone(UP) as TKUserProfile);
            UP := AdjacentBySortVal(katNext, ksvPointer, UP) as TKUserProfile;
          end;
        end;
      finally
        FSQLImportData.SQLDatastore.PutTopLevelList(TmpUL);
      end;
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Create in-mem tree to persist to memdb.');
{$ENDIF}
       FSQLImportData.UserCursor := FSQLImportData.ImportTree.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
      //Now have a copy of all the data we need to import.
      //Set initial cursor.
      //And if necessary, start the operation.
      if not Assigned(FSQLImportData.UserCursor) then
      begin
        Memo1.Lines.Add('No profiles to import');
        ClearImportData;
        FormCreate(Sender);
      end
      else
      begin
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'MemDB update imported user.');
{$ENDIF}
        if not FSQLImportData.MemDBDatastore.UpdateImportedUser(FSQLImportData.UserCursor, nil, nil) then
        begin
          Memo1.Lines.Add('Op start failed');
          ClearImportData;
          FormCreate(Sender);
        end
        else
        begin
          Memo1.Lines.Add('Started copy to memdb');
        end;
      end;
    end
    else if Seq is TWriteUserOpSequence then
    begin
      Assert(not Assigned(FSQLImportData.MediaCursor));
      //Done user, move to media if poss,
      FSQLImportData.MediaCursor :=
        FSQLImportData.UserCursor.Media.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKMediaItem;
      if Assigned(FSQLImportData.MediaCursor) then
      begin
        //Start media import.
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'MemDB update imported media.');
{$ENDIF}
        if not FSQLImportData.MemDBDatastore.UpdateImportedMedia(FSQLImportData.MediaCursor, nil, nil) then
        begin
          Memo1.Lines.Add('Op start failed (2)');
          ClearImportData;
          FormCreate(Sender);
        end;
      end
      else
      begin
        //else next user.
        FSQLImportData.UserCursor :=
          FSQLImportData.ImportTree.AdjacentBySortVal(katNext, ksvPointer, FSQLImportData.UserCursor) as TKUserProfile;
        if Assigned(FSQLImportData.UserCursor) then
        begin
          //Start next user import
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'MemDB update imported user.');
{$ENDIF}
          if not FSQLImportData.MemDBDatastore.UpdateImportedUser(FSQLImportData.UserCursor, nil, nil) then
          begin
            Memo1.Lines.Add('Op start failed (3)');
            ClearImportData;
            FormCreate(Sender);
          end;
        end
        else
        begin
          //All done.
          Memo1.Lines.Add('All finished!');
          ClearImportData;
          FormCreate(Sender);
        end;
      end;
    end
    else if Seq is TWriteMediaOpSequence then
    begin
      //Done media, try next.
      FSQLImportData.MediaCursor :=
        FSQLImportData.UserCursor.Media.AdjacentBySortVal(katNext, ksvPointer, FSQLImportData.MediaCursor)as TKMediaItem;
      if Assigned(FSQLImportData.MediaCursor) then
      begin
        //Start next media
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'MemDB update imported media.');
{$ENDIF}
        if not FSQLImportData.MemDBDatastore.UpdateImportedMedia(FSQLImportData.MediaCursor, nil, nil) then
        begin
          Memo1.Lines.Add('Op start failed (4)');
          ClearImportData;
          FormCreate(Sender);
        end;
      end
      else
      begin
        //else next user.
        FSQLImportData.UserCursor :=
          FSQLImportData.ImportTree.AdjacentBySortVal(katNext, ksvPointer, FSQLImportData.UserCursor) as TKUserProfile;
        if Assigned(FSQLImportData.UserCursor) then
        begin
          //Start next user import
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'MemDB update imported user.');
{$ENDIF}
          if not FSQLImportData.MemDBDatastore.UpdateImportedUser(FSQLImportData.UserCursor, nil, nil) then
          begin
            Memo1.Lines.Add('Op start failed (5)');
            ClearImportData;
            FormCreate(Sender);
          end;
        end
        else
        begin
          //All done.
          Memo1.Lines.Add('All finished!');
          ClearImportData;
          FormCreate(Sender);
        end;
      end;
    end
    else
    begin
      Memo1.Lines.Add('Oh dear, all gone wrong in write user list command: ' +Seq.Msg);
      ClearImportData;
      FormCreate(Sender);
      exit;
    end;
  end;
end;

procedure TKCLoaderFrm.HandleImportBatchLoaderMultiOpCompleted(Sender: TObject; MultiOp: TMultiOp);
var
  TmpUL: TKUSerList;
begin
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Main form batch loader completion');
{$ENDIF}
  if not MultiOp.OK then
  begin
    Memo1.Lines.Add('Oh dear, all gone wrong: ' +MultiOp.Msg);
    ClearImportData;
    FormCreate(Sender);
    exit;
  end
  else
  begin
    if Sender = FSQLImportData.SQLBatchLoader then
    begin
      if MultiOp.OpType = motDbLoadUserList then
      begin
        Memo1.Lines.Add('SQL Load user list OK, now load user trees.');
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'SQLBatchLoader load user trees.');
{$ENDIF}
        FSQLImportData.SQLBatchLoader.LoadUserTree(TGuid.Empty);
      end
      else if MultiOp.OpType = motDbLoadUserTrees then
      begin
        Memo1.Lines.Add('SQL Load user trees OK, now write MemDB user list.');
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Load user trees OK. NO SQL batch loader from here on.');
{$ENDIF}
        //TODO - Kick off the write user list command.
        TmpUL := FSqlImportData.SQLDatastore.GetTemporaryTopLevelList;
        try
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Mem datastore read/write user list.');
{$ENDIF}
          if not FSQLImportData.MemDBDatastore.WriteUserList(TmpUL, nil, nil) then
          begin
            Memo1.Lines.Add('Oh dear, write user list start failed: ' + MultiOp.Msg);
            ClearImportData;
            FormCreate(Sender);
            exit;
          end;
        finally
          FSQLImportData.SQLDatastore.PutTopLevelList(TmpUL);
        end;
      end;
    end
    else
      Assert(false);
  end;
end;


procedure TKCLoaderFrm.MySQLLoadClick(Sender: TObject);
var
  Msg: string;
  OK: boolean;
begin
  //Rip down any existing component.
  FormDestroy(Sender);
  FBatchLoader := nil;
  FDataStore := nil;
  FDB := nil;
  with FSQLImportData do
  begin
    SQLDB := TDBXMySql.Create;
    SQLDB.DBName := SQL_DB_NAME;
    OK := SQLDB.InitDBAndTables(Msg);
    Assert(OK);
    MemDB := TMemKDB.Create;
    OK := MakeDBRootDir(DATA_ROOT_LOCATION);
    (MemDB as TMemKDB).DBPath := DATA_ROOT_LOCATION;
    OK := MemDB.InitDBAndTables(Msg);
    Assert(OK);
    MemDB.DestroyTables;
    MemDB.FiniDBAndTables;
    OK := MemDB.InitDBAndTables(Msg);
    Assert(OK);
    MemDBDatastore:= TLoaderDatastore.Create;
    MemDBDatastore.DB := MemDB;
    MemDBDatastore.OnOpCompletion := self.HandleMemDatastoreCompletion;
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Import create MemDB datastore: ' + IntToHex(Int64(MemDBDatastore), 16));
{$ENDIF}
    SQLDatastore := TLoaderDatastore.Create;
    SQLDatastore.DB := SQLDB;
    SQLBatchLoader := TBatchLoader.Create;
    SQLBatchLoader.DataStore := SQLDatastore;
    SQLDatastore.OnOpCompletion := SQLBatchLoader.HandleDataStoreCompletion;
    SQLBatchLoader.OnMultiOpCompleted := HandleImportBatchLoaderMultiOpCompleted;
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Import create SQL batch loader: ' + IntToHex(Int64(SQLBatchLoader), 16));
  GLogLog(SV_INFO, 'Import create SQL datastore: ' + IntToHex(Int64(SQLDatastore), 16));
{$ENDIF}
  end;
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'SQLBatchLoader load user list.');
{$ENDIF}
  FSQLImportData.SQLBatchLoader.LoadUserList;
end;

function TKCLoaderFrm.RequestCompleteCommon: boolean;
var
  CanClose: boolean;
begin
  result := false;
  if FAttemptingClose then
  begin
    CloseCommon(CanClose);
    if CanClose then
    begin
      result := true;
      Close;
    end;
  end;
end;

procedure TKCLoaderFrm.StatsTimerTimer(Sender: TObject);
var
  BatchStats: TBatchLoaderStats;
  Loader: TBatchLoader;
begin
  StatsMemo.Lines.Clear;
  if Assigned(FBatchLoader) then
    Loader := FBatchLoader
  else
    Loader := nil;

  if Assigned(Loader) then
  begin
    Loader.GetStats(BatchStats);
    with StatsMemo.Lines do
    begin
      Add('Multi ops in progress: ' + IntToStr(BatchStats.MultiOps));
      Add('User profiles changed: ' + IntToStr(BatchStats.UserProfilesChanged));
      Add('Media items changed: ' + IntToStr(BatchStats.MediaItemsChanged));
      Add('Comment items changed: ' + IntToStr(BatchStats.CommentItemsChanged));
      Add('New profiles found: ' + IntToStr(BatchStats.NewProfilesDiscovered));
    end;
  end;
end;

procedure TKCLoaderFrm.URefreshClick(Sender: TObject);
begin
  //For the moment, import everything in the last week.
  if FBatchLoader.RefreshUsers(DefaultSince, TGuid.Empty) then
    LogToMemo('Refresh all users started.');
end;

procedure TKCLoaderFrm.UTreeLoadClick(Sender: TObject);
begin
  if FBatchLoader.LoadUserTree(TGuid.Empty) then
    LogToMemo('Load all user trees started.');
end;

procedure TKCLoaderFrm.AddUserClick(Sender: TObject);
var
  ST: TKSiteType;
begin
  if Length(NewUserEdit.Text) > 0 then
  begin
    if SiteTypeCombo.Selected = ListBoxInstagramItem then
      ST := tstInstagram
    else if SiteTypeCombo.Selected = ListBoxTwitterItem then
      ST := tstTwitter
    else
      exit;
    if FBatchLoader.ScanForNewUser(NewUserEdit.Text.Trim, ST, DefaultSince) then
      LogToMemo('Importing new user: ' + NewUserEdit.Text);
  end;
end;

procedure TKCLoaderFrm.DeleteUserClick(Sender: TObject);
var
  ST: TKSiteType;
begin
  if Length(NewUserEdit.Text) > 0 then
  begin
    if SiteTypeCombo.Selected = ListBoxInstagramItem then
      ST := tstInstagram
    else if SiteTypeCombo.Selected = ListBoxTwitterItem then
      ST := tstTwitter
    else
      exit;
    if FBatchLoader.PurgeUserByUsername(NewUserEdit.Text.Trim,
      klUserList, ST, dtMemAndDb) then
      LogToMemo('Deleting user: ' + NewUserEdit.Text);
  end;
end;

procedure TKCLoaderFrm.ClearTblsBtnClick(Sender: TObject);
var
  Msg: string;
begin
  FDB.ClearTablesData;
  FDB.DestroyTables;
  if not FDB.InitDBAndTables(Msg) then
    LogToMemo('DB Re-init error: ' + Msg);
end;

procedure TKCLoaderFrm.CloseCommon(var CanClose: boolean);
begin
  CanClose := (not Assigned(FBatchLoader)) or FBatchLoader.Stop;
  //Try terminating connections etc if can't close, and we should end up
  //back here soon, fetch/parses having failed.
  if Assigned(GFetcherParser) then GFetcherParser.Stop;
  if not CanClose then
    if Assigned(GFetcherParser) then GFetcherParser.CancelOutstandingFetches;
  //FetcherParser does not strictly keep count of items in progress,
  //so rely on BatchLoader / DataStore to provide info about whether
  //everything finished.
end;

procedure TKCLoaderFrm.DecreaseInterestAllClick(Sender: TObject);
begin
  if FBatchLoader.ChangeUserInterestLevel(TGuid.Empty, kpiFetchUserForRefs, DefaultSince) then
    LogToMemo('Demote all users started.');
end;

procedure TKCLoaderFrm.HandleMultiOpCompleted(Sender: TObject; MultiOp: TMultiOp);
var
  LogStr: string;
  TypeStr: string;
begin
  if RequestCompleteCommon then exit;
  LogStr := 'Batch loader op ';
  if MultiOp.OK then
    LogStr := LogStr + 'completed: '
  else
    LogStr := LogStr + 'failed: ';
  case MultiOp.OpType of
    motDbLoadUserList: TypeStr := 'Load user list';
    motDbLoadUserTrees: TypeStr := 'Load user trees';
    motPurgeOrDecrease: TypeStr := 'Purge Data';
    motImportOrIncrease: TypeStr := 'Import new data';
  else
    Assert(false);
  end;
  LogStr := LogStr + TypeStr;
  if Length(MultiOp.Msg) > 0 then
    LogStr := LogStr + ': ' + MultiOp.Msg;
  LogToMemo(LogStr);
end;

procedure TKCLoaderFrm.IncreaseInterestVerifiedClick(Sender: TObject);
var
  Criteria: TUserSelectionCriteria;
begin
  Criteria.Count := 0;
  Criteria.CriteriaType := sctVerified;
  if FBatchLoader.ChangeUserInterestLevelByCriteria(Criteria, kpiTreeForInfo, DefaultSince) then
    LogToMemo('Promoting verified users to full scan.');
end;

procedure TKCLoaderFrm.ForceCloseClick(Sender: TObject);
begin
  //This code only to debug cases where stop operations / stop refs
  //queues etc have not been properly cleared down - just do
  //the close path anyway - to find out what's stuck / leaked.
  FForceClose := true;
  Close;
end;

procedure TKCLoaderFrm.FormCloseQuery(Sender: TObject; var CanClose: Boolean);
begin
  CloseCommon(CanClose);
  FAttemptingClose := true;
  CanClose := CanClose or FForceClose;
end;

function MakeDBRootDir(Dir: string): boolean;
begin
  //TODO - Platform agnostic ver?
  result := true;
  //First of all, create (or open the) initial directory.
  if not ((Dir[Length(Dir)] = '/')
     or (Dir[Length(Dir)] = '\')) then
    Dir := Dir + '\';
  if not CreateDirectoryW(@Dir[1], nil) then
  begin
    if not GetLastError = ERROR_ALREADY_EXISTS then
      result := false;
  end;
end;

procedure TKCLoaderFrm.FormCreate(Sender: TObject);
var
  Msg: string;
  OK: boolean;
begin
  if not FFirstTime then
  begin
    AppGlobalLog.Severities := AllSeverities;
    AppGlobalLog.OpenFileLog(TPath.GetTempPath + 'KnowCommentLoader.log');
    AppGlobalLog.Log(TLogSeverity.SV_INFO, 'Log file opened: ' + DateTimeToStr(Now));
    FFirstTime := true;
    DBLabel.Text := DATA_ROOT_LOCATION;
  end;

{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Loader form FormCreate');
{$ENDIF}

  FDB := TMemKDB.Create;
  OK := MakeDBRootDir(DATA_ROOT_LOCATION);
  (FDB as TMemKDB).DBPath := DATA_ROOT_LOCATION;
  OK := OK and FDB.InitDBAndTables(Msg);
  FDataStore := TLoaderDataStore.Create;
  FDataStore.DB := FDB;
  FBatchLoader := TBatchLoader.Create;
  FBatchLoader.DataStore := FDataStore;
  FDataStore.OnOpCompletion := FBatchLoader.HandleDataStoreCompletion;
  FBatchLoader.OnMultiOpCompleted := self.HandleMultiOpCompleted;
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Main form create batch loader: ' + IntToHex(Int64(FBatchLoader), 16));
  GLogLog(SV_INFO, 'Main form create datastore: ' + IntToHex(Int64(FBatchLoader), 16));
{$ENDIF}
  if OK then
  begin
    if FBatchLoader.LoadUserList then
      LogToMemo('Load user list started.');
    //Set up controls.
    ListBoxInstagramItem.Text := 'Instagram';
    ListBoxTwitterItem.Text := 'Twitter';
  end
  else
    LogToMemo('Database load failed: ' + Msg);
end;

//TODO - Debug
procedure TKCLoaderFrm.FormShow(Sender: TObject);
begin
  if OAuth2.DbgFound then
  begin
    LogToMemo('Debug crypt found.');
  end;
  if OAuth2.DbgGoodMask  <> 0 then
  begin
    LogToMemo('Debug good mask: '+ IntToHex(Oauth2.DbgGoodMask, 10));
  end;
end;

procedure TKCLoaderFrm.FormDestroy(Sender: TObject);
begin
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Loader form FormDestroy');
{$ENDIF}
  if Assigned(FBatchLoader) then FBatchLoader.OnMultiOpCompleted := nil;
  if Assigned(FDataStore) then FDataStore.OnOpCompletion := nil;
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'Main form destroy batch loader: ' + IntToHex(Int64(FBatchLoader), 16));
  GLogLog(SV_INFO, 'Main form destroy datastore: ' + IntToHex(Int64(FBatchLoader), 16));
{$ENDIF}
  FBatchLoader.Free;
  FDataStore.Free;
  FDB.Free;
  ClearImportData;
// No need, done in GlobalLog finalization.
//  AppGlobalLog.CloseFileLog;
end;

initialization
  DefaultSince := Now - 7;
end.
