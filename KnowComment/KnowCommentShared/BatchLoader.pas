unit BatchLoader;

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
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  Classes, DLList, DataObjects, DataStore, IndexedStore, SysUtils,
  Importers, System.Generics.Collections;

type
  //Stats not guaranteed to be completely accurate, but give a good
  //idea of what's going on.
  TBatchLoaderStats = record
    MultiOps: cardinal;
    UserProfilesChanged: cardinal;
    MediaItemsChanged: cardinal;
    CommentItemsChanged: cardinal;
    NewProfilesDiscovered: cardinal;
  end;

  TBatchLoader = class;

  {
    Batch loader only works on one multi-op at a time,
    but those multi-ops can do multiple DB / import / whatever
    operations, provided they don't break any rules concerning what you
    can do concurrently with importers / datastore.
  }

  TMultiOpType = (
    // Datastore only ops - relatively easy.
    motDbLoadUserList, motDbLoadUserTrees,
    motPurgeOrDecrease, motImportOrIncrease,
    motCustomQuery, motExpire);

{$IFDEF USE_TRACKABLES}

  TMultiOp = class(TTrackable)
{$ELSE}
  TMultiOp = class
{$ENDIF}
  private
    FType: TMultiOpType;
    FParentLoader: TBatchLoader;
    FQueueLink: TDLEntry;
    FOK: boolean;
    FMsg: string;
  protected
    FStopping: boolean;
    procedure HandleDataStoreCompletion(Sender: TObject); virtual; abstract;
    function Start: boolean; virtual;
    procedure AccumErr(OK: boolean; Msg: string);
    procedure SignalCompletion; virtual;
  public
    procedure GetStats(var ResultStats: TBatchLoaderStats); virtual; abstract;
    function GetOpDetails: string; virtual; abstract;
    constructor Create;
    function Stop: boolean; virtual; // returns whether idle.
    property OK: boolean read FOK;
    property Msg: string read FMsg;
    property OpType: TMultiOpType read FType;
  end;

  TCustomQueryMultiOp = class(TMultiOp)
  private
    FTempKeyList: TKUserList;
    FTempFilter: TAdditionalFilter;
    FResult: TKUserList;
    FLevel: TKListLevel;
  protected
    procedure HandleDataStoreCompletion(Sender: TObject); override;
    function Start: boolean; override;
  public
    destructor Destroy; override;
    procedure GetStats(var ResultStats: TBatchLoaderStats); override;
    function GetOpDetails: string; override;
    property Result: TKUSerList read FResult write FResult;
  end;

  TPurgeMultiOpPhase = (pmoWritebackInterest, pmoDoPurge);

  TUserKeyedMultiOp = class(TMultiOp)
  protected
    FStats: TBatchLoaderStats;

    FUserKeysRemaining: TKUserList;
    FPresentationUserKeysRemaining: TKUserList;
    function MakeInterestedUserKeyListFromDB: boolean;
  public
    procedure GetStats(var ResultStats: TBatchLoaderStats); override;
    function GetOpDetails: string; override;
    constructor Create;
    destructor Destroy; override;
    property PresentationUserKeysRemaining: TKUserList read FPresentationUserKeysRemaining;
  end;

  TDBInitMultiOp = class(TUserKeyedMultiOp)
  private
  protected
    procedure IncStatsForLoadUserList;
    procedure IncStatsForLoadUserTree(const UserKey: TGuid);
  public
    procedure HandleDataStoreCompletion(Sender: TObject); override;
    function Start: boolean; override;
    function Stop: boolean; override; // returns whether idle.
    function InitForLoadUserList: boolean;
    function InitForLoadUserTree(UP: TKUserProfile): boolean;
    function InitForLoadAllUserTrees: boolean;
    function GetOpDetails: string; override;
  end;

  TSelCriteraType = (sctAll, sctNone, sctVerified, sctNotVerified, sctMoreFollowersThan,
    sctFewerEqFollowersThan, sctFollowsMoreThan, sctFollowsFewerEqThan);

  TUserSelectionCriteria = record
    Count: integer;
    CriteriaType: TSelCriteraType;
  end;

  //TODO - Both purge and expire multi-ops to add a phase
  //at the end which is "remove unreferenced"
  //(user profiles at the moment, but might be many other stuff).

  TDBPurgeMultiOp = class(TUserKeyedMultiOp)
  private
    FOpsThisPhase: integer; // For parallel operation.
    FPurgeLevel: TKListLevel;
    FPurgeType: TDeleteType;
    FPurgePhase: TPurgeMultiOpPhase;
    FDecreasedInterest: boolean;
  protected
    procedure IncStatsForDatastoreCompletion;
    function StartWriteBackInterest: boolean;
    function StartPurge: boolean;
    procedure IncrementUserKeyCursor(First: boolean; var Cursor: TKUserProfile);
  public
    procedure HandleDataStoreCompletion(Sender: TObject); override;
    function Start: boolean; override;
    function Stop: boolean; override; // returns whether idle.

    function InitForPurgeUser(UP: TKUserProfile; Level: TKListLevel; PurgeType: TDeleteType): boolean;
    function InitForPurgeAll(Level: TKListLevel; PurgeType: TDeleteType): boolean;
    function InitForDecreasedInterest(UP: TKUserProfile): boolean;
    function InitDecreaseByCriteria(Crit: TUserSelectionCriteria): boolean;
    function GetOpDetails: string; override;
    property PurgeLevel: TKListLevel read FPurgeLevel;
    property PurgeType: TDeleteType read FPurgeType;
    property DecreasedInterest: boolean read FDecreasedInterest;
  end;

  TExpireMultiOpPhase = (emoExpire, emoPrune);

  //No multi-phases here, just the one.
  TDBExpireMultiOp = class(TUserKeyedMultiOp)
  private
    FPhase: TExpireMultiOpPhase;
    FOpsThisPhase: integer; // For parallel operation.
    FExpireBefore: TDateTime;
    FExpiryType: TDBExpiryType;
    FLevelSet: TKListLevelSet;
  protected
    function StartExpire: boolean;
    function StartPrune: boolean;
  public
    function InitForExpireUser(UP: TKUserProfile;
                               ExpireBefore: TDateTime; ExpiryType: TDBExpiryType;
                               LevelSet: TKListLevelSet): boolean;
    function InitForExpireAll(ExpireBefore: TDateTime; ExpiryType: TDBExpiryType;
                               LevelSet: TKListLevelSet): boolean;
    procedure HandleDataStoreCompletion(Sender: TObject); override;
    function Start: boolean; override;
    function Stop: boolean; override; // returns whether idle.
    function GetOpDetails: string; override;
    //TODO - More stats?
    //TODO - Remove unreffed UP's at the end of this CMD, no extra multi-op?
  end;

  TImportMultiOp = class;

  TSiteTypeBools = array [Low(TKSiteType) .. High(TKSiteType)] of boolean;

{$IFDEF USE_TRACKABLES}
  TBailoutHint = class(TTrackable)
{$ELSE}
  TBailoutHint = class
{$ENDIF}
  public
    BailTimes: array [Low(TKSiteType) .. High(TKSiteType)] of TDateTime;
  end;

  TMediaLocationPhase = (mlpInitialFetch, mlpFinalWriteback);

{$IFDEF USE_TRACKABLES}
  TUserImportLocation = class(TTrackable)
{$ELSE}
  TUserImportLocation = class
{$ENDIF}
  private
    FUser: TKUserProfile;
    FMedia: TKMediaItem;
    FComment: TKCommentItem;
    FSiteType: TKSiteType;
    FMediaPhase: TMediaLocationPhase;
  protected
    function GetLocationObj: TKKeyedObject;
  public
    procedure Increment(UserTree: TKUSerProfile; var DeleteTreeFragment: boolean);
    property LocationObj: TKKeyedObject read GetLocationObj;
    property LocationSiteType: TKSiteType read FSiteType;
    property MediaPhase: TMediaLocationPhase read FMediaPhase;
  end;

{$IFDEF USE_TRACKABLES}
  TUserImportRec = class(TTrackable)
{$ELSE}
  TUserImportRec = class
{$ENDIF}
  private
    FStats:TBatchLoaderStats;

    FStopping: boolean;
    FFailStarts: boolean;
    FFailMsg: string;
    FParentMultiOp: TImportMultiOp;

    FAccumulatedTree: TKUSerProfile;
    FTreeBailoutData: TKUSerProfile;
    FLocation: TUserImportLocation;
    FTreeFragment: TKKeyedObject;

    FCommentOwnerKeysPersisted: TKUSerList;

    FImporterRetries: integer;
    FProfileImporter: TUserProfileImporter;
    FMediaImporter: TMediaImporter;
    //FSince?? User specified not bailout data?
    FSince: TDateTime;
  protected
    procedure IncStatsForUProfPersist(MediaCount: cardinal);
    procedure IncStatsForMProfPersist(CommentCount: cardinal);
    procedure IncStatsForNewDependentProfile;

    procedure SetUserBailout;
    procedure SetMediaBailout(MediaItem: TKMediaItem);
    procedure RefreshUserSetBailoutsCommon(StoreUP: TKUserProfile);
    procedure ClearBailouts;
    function CommentItemProfileResolvable(CommentItem: TKCommentItem): boolean;
    function MediaImportFetch: boolean;
    function UserImportFetch: boolean;
    procedure SetOptsForUserImport(Opts: TImportOptions);
    procedure SetOptsForMediaImport(Opts: TImportOptions);
    procedure UpdateImportersForSiteType;

    function IncrementLocation(var OutstandingOps: integer): boolean;
    function NextAction(var OutstandingOps: integer; var ItemSkipped: boolean): boolean;
  public
    function Start(var OutstandingOps: integer): boolean;
    function Stop: boolean; // Returns whether idle
    function StartBailCalcs(var OutstandingOps: integer): boolean;
    function StartMainImport(var OutstandingOps: integer): boolean;

    procedure HandleDataStoreCompletion(var OutstandingOps: integer; Sender: TObject);
    procedure HandleImporterCompletion(var OutstandingOps: integer; Sender: TObject);

    procedure GetStats(var ResultStats: TBatchLoaderStats);

    constructor Create;
    destructor Destroy; override;
  end;

  TImportMultiOpPhase = (mopPreLoadList, mopPreLoadTrees, mopBailCalcs, mopMainImport);

  TImportMultiOp = class(TMultiOp)
  private
    FSince: TDateTime;
    FPhase: TImportMultiOpPhase;
    FImportsThisPhase: integer;
    FImportRecs: TIndexedStore;
    FIncreasedInterestOrNewScan: boolean;
    FMinimalRefresh: boolean;
  protected
    function StartPreLoadTrees: boolean;
    function StartBailCalcs: boolean;
    function StartMainImport: boolean;
    procedure HandleDataStoreCompletion(Sender: TObject); override;
    procedure HandleImporterCompletion(Sender: TObject);
    procedure SignalCompletion; override;
  public
    constructor Create;
    destructor Destroy; override;
    function Start: boolean; override;
    function Stop: boolean; override; // returns whether idle.
    function InitForRefreshUsers(Since: TDateTime; UP: TKUserProfile): boolean;
    function InitForRefreshAll(Since: TDateTime): boolean;
    function InitForIncreasedInterest(UP: TKUserProfile; Since: TDateTime): boolean;
    function InitIncreaseByCritera(Crit: TUserSelectionCriteria; Since: TDateTime): boolean;
    function InitForScanForNewUser(Username: string; SType: TKSiteType; Since: TDateTime): boolean;
    function InitForMinimalRefresh(UP: TKUserProfile): boolean;
    property Phase: TImportMultiOpPhase read FPhase;
    procedure GetStats(var ResultStats: TBatchLoaderStats);override;
    function GetOpDetails: string; override;
    function GetSortedKeyList: TList<TGuid>;
    property IncreasedInterestOrNewScan: boolean read FIncreasedInterestOrNewScan;
    property MinimalRefresh: boolean read FMinimalRefresh;
  end;

  TMultiOpCompletedEvent = procedure(Sender: TObject; MultiOp: TMultiOp) of object;

{$IFDEF USE_TRACKABLES}
  TBatchLoader = class(TTrackable)
{$ELSE}
  TBatchLoader = class
{$ENDIF}
  private
    FDataStore: TLoaderDataStore;
    FAsyncQueryQueue: TDLEntry;
    FMultiOpQueue: TDLEntry;
    FMultiOpQueueHeadStarted: boolean;
    FStopping: boolean;
    FOnMultiOpCompleted: TMultiOpCompletedEvent;
  protected
    procedure HandleMultiOpCompletion(MultiOp: TMultiOp);
    function AddMultiOp(MultiOp: TMultiOp): boolean;
    function AddAndStartQuery(MultiOp: TMultiOp): boolean;
    procedure StartNextMultiOp;
    function GetUserProfileByKey(const Key: TGuid): TKUserProfile;
    function GetActive:boolean;
  public
    procedure GetStats(var ResultStats: TBatchLoaderStats);
    function GetQueuedOpDetails: TStringList;

    procedure HandleDataStoreCompletion(Sender: TObject; Seq: TOpSequence);
    constructor Create;
    function Stop: boolean; // returns whether idle (stopped).
    function ConditionalUnstop: boolean; //returns whether unstopped.

    destructor Destroy; override;
    // DB Load functions.
    function LoadUserList: boolean;

    //Load one or many.
    function LoadUserTree(const Key: TGuid): boolean;
    function CanLoadUserTree(const Key: TGuid): boolean;

    //Purge one or many.
    function PurgeUser(Level: TKListLevel; PurgeType: TDeleteType; const Key: TGuid): boolean;
    function CanPurgeUser(Level: TKListLevel; const Key: TGuid): boolean;

    //Expire one or many.
    function ExpireUser(ExpireBefore: TDateTime; ExpiryType: TDBExpiryType;
      LevelSet: TKListLevelSet; const Key: TGuid): boolean;
    function CanExpireUser(ExpireBefore: TDateTime; ExpiryType: TDBExpiryType;
      LevelSet: TKListLevelSet; const Key: TGuid): boolean;

    // Import and refresh one or many.
    function RefreshUsers(Since: TDateTime; const Key: TGuid): boolean;
    function CanRefreshUser(const Key: TGuid): boolean;

    // Refresh one or more users already in the DB.
    function ChangeUserInterestLevel(const Key: TGuid; NewLevel: TKProfileInterestLevel;
      Since: TDateTime): boolean;
    function CanChangeInterestLevel(const Key: TGuid; NewLevel: TKProfileInterestLevel): boolean;
    function MinimalUserRefesh(const Key: TGuid): boolean;

    function ChangeUserInterestLevelByCriteria(Crit: TUserSelectionCriteria;
      NewLevel: TKProfileInterestLevel; Since: TDateTime): boolean;

    function ScanForNewUser(Username: string; SType: TKSiteType; Since: TDateTime): boolean;

    //TODO - Remove these helper functions when no longer needed.
    // Helper functions which will do things by username.
    function LoadUserTreeByUsername(Username: string; SType: TKSiteType): boolean;
    // Refresh can be done by username by doing scan for new user.
    function PurgeUserByUsername(Username: string; Level: TKListLevel; SType: TKSiteType;
      PurgeType: TDeleteType): boolean;

    //Queries execute asynchronously from main multi-op path.
    function CustomQuery(Level: TKListLevel; FilterByUsers: TKUserList; AdditionalFilter: TAdditionalFilter): boolean;

    property OnMultiOpCompleted: TMultiOpCompletedEvent read FOnMultiOpCompleted
      write FOnMultiOpCompleted;
    property DataStore: TLoaderDataStore read FDataStore write FDataStore;
    property Active: boolean read GetActive;
  end;

  procedure SumStats(var Result: TBatchLoaderStats; const Increment: TBatchLoaderStats);

implementation

uses
  InstagramImporters, TwitterImporters, GlobalLog, KKUtils, FetcherParser,
  DBGeneric;

const
  S_CLOSING_OR_INTERNAL_ERROR = 'Unable to schedule multi-op, app closing or internal error.';
  S_MULTI_START_FAILED = 'Multi-op start failed, app closing?';
  S_DB_WRITEBACK_MAIN_UPROF = 'User writeback for main user profile key ';
  S_DB_WRITEBACK_DEP_UPROF = 'User writeback for comment dependent key (race cond? keep going) ';
  S_DB_WRITEBACK_2 = ' failed (';
  S_DB_WRITEBACK_3 = ') no more DB ops for user ';
  S_DB_WRITEBACK_4 = ') struggle on for user ';
  S_DB_BAILOUT_READ_BAD = 'Expected to get bailout vals for item in DB. Stopping: ';
  S_COULDNT_CALC_BAILOUTS = 'Internal error, couldn''t calculate bailouts';
  S_MEDIA_WB_1 = 'Media writeback for key ';
  S_MEDIA_WB_2 = ' failed (';
  S_MEDIA_WB_3 = ') no more DB ops for user ';
  S_NO_DATA_USERNAMES = 'No data for:';
  S_COMPLETE_USERLIST_FAILED = 'Initial userlist read failed: ';
  S_FOR = ' for ';
  S_USERS = ' users.';
  S_USER = 'user: ';
  S_LOAD_USER_LIST = 'Load user list.';
  S_LOAD_USER_TREES = 'Load user tree(s) to memory';
  S_PURGE_FROM = 'Delete ';
  S_DEL_USERL = 'user, media and comments ';
  S_DEL_MEDIAL = 'media and comments ';
  S_DEL_COMMENTL = 'comments ';
  S_MEM_DISK = 'from memory and disk, ';
  S_MEM_ONLY = 'from memory only, ';
  S_CUSTOM_QUERY = 'Custom query.';
  S_EXPIRE_FROM = 'Expire old data from disk and memory.';

  DefaultRetryCount = 3;

  { Misc functions }

  // TODO - Functions in here: can use "CloneListAndItemsOnly" of TKUserList,
  // with selection function for all the User list cloning at Init time.
  // Will speed up both change interest level, and more basic user list
  // cloning. (Currently we clone and prune where we don't need to).
function UserSelectionCriteriaSatisfied(UP: TKUSerProfile; Crit: TUserSelectionCriteria): boolean;
var
  ST: TKSiteType;
  FollowsCount, FollowerCount: integer;
begin
  FollowsCount := 0;
  FollowerCount := 0;
  for ST := Low(ST) to High(ST) do
  begin
    if Assigned(UP.SiteUserBlock[ST]) and UP.SiteUserBlock[ST].Valid then
    begin
      Inc(FollowsCount, UP.SiteUserBlock[ST].FollowsCount);
      Inc(FollowerCount, UP.SiteUserBlock[ST].FollowerCount);
    end;
  end;
  case Crit.CriteriaType of
    sctAll:
      result := true;
    sctNone:
      result := false;
    sctVerified:
      begin
        result := false;
        for ST := Low(ST) to High(ST) do
          result := result or (Assigned(UP.SiteUserBlock[ST]) and UP.SiteUserBlock[ST].Valid and
            UP.SiteUserBlock[ST].Verified);
      end;
    sctNotVerified:
      begin
        result := true;
        for ST := Low(ST) to High(ST) do
          result := result and not(Assigned(UP.SiteUserBlock[ST]) and UP.SiteUserBlock[ST].Valid and
            UP.SiteUserBlock[ST].Verified);
      end;
    sctMoreFollowersThan:
      result := FollowerCount > Crit.Count;
    sctFewerEqFollowersThan:
      result := FollowerCount <= Crit.Count;
    sctFollowsMoreThan:
      result := FollowsCount > Crit.Count;
    sctFollowsFewerEqThan:
      result := FollowsCount < Crit.Count;
  else
    Assert(false);
    result := false;
  end;
end;

procedure SumStats(var Result: TBatchLoaderStats; const Increment: TBatchLoaderStats);
begin
  Inc(Result.MultiOps, Increment.MultiOps);
  Inc(Result.UserProfilesChanged, Increment.UserProfilesChanged);
  Inc(Result.MediaItemsChanged, Increment.MediaItemsChanged);
  Inc(Result.CommentItemsChanged, Increment.CommentItemsChanged);
  Inc(Result.NewProfilesDiscovered, Increment.NewProfilesDiscovered);
end;


function UserSummaryFromUserlist(UL: TKUSerList): string;
var
  UP: TKUserProfile;
  ST: TKSiteType;
begin
  if UL.Count > 1 then
  begin
    result := S_FOR + IntToStr(UL.Count) + S_USERS;
  end
  else
  begin
    result := S_FOR;
    Assert(UL.Count > 0);
    UP := UL.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
    for ST := Low(ST) to High(ST) do
    begin
      if UP.SiteUserBlocks[ST].Valid and (Length(UP.SiteUserBlocks[ST].Username) > 0) then
      begin
        result := result + UP.SiteUserBlocks[ST].Username;
        break;
      end;
    end;
  end;
end;

{ TUserKeyedMultiOp }

procedure TUserKeyedMultiOp.GetStats(var ResultStats: TBatchLoaderStats);
begin
  ResultStats := FStats;
end;

function TUserKeyedMultiOp.GetOpDetails: string;
begin
   result := UserSummaryFromUserlist(FPresentationUserKeysRemaining);
end;

constructor TUserKeyedMultiOp.Create;
begin
  inherited;
  FUserKeysRemaining := TKUserList.Create;
end;

destructor TUserKeyedMultiOp.Destroy;
begin
  FUserKeysRemaining.Free;
  FPresentationUserKeysRemaining.Free;
  inherited;
end;

function TUserKeyedMultiOp.MakeInterestedUserKeyListFromDB: boolean;
var
  DBList: TKUserList;
  UP: TKUSerProfile;
begin
  DBList := FParentLoader.FDataStore.GetTemporaryTopLevelList;
  try
    FUserKeysRemaining.Free;
    FUserKeysRemaining := TKUserList.Create;
    UP := DBList.AdjacentBySortVal(katFirst, ksvPresentationOrder, nil) as TKUserProfile;
    while Assigned(UP) and (UP.InterestLevel > kpiFetchUserForRefs) do
    begin
      FUserKeysRemaining.Add(TKDataObject.Clone(UP) as TKKeyedObject);
      UP := DBList.AdjacentBySortVal(katNext, ksvPresentationOrder, UP) as TKUSerProfile;
    end;
  finally
    FParentLoader.FDataStore.PutTopLevelList(DBList);
  end;
  result := (FUserKeysRemaining.Count > 0);
end;

{ TCustomQueryMultiOp }

procedure TCustomQueryMultiOp.HandleDataStoreCompletion(Sender: TObject);
var
  CustomQuerySeq: TCustomQueryOpSequence;
begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Custom query handle data store completion.');
{$ENDIF}
  CustomQuerySeq := Sender as TCustomQueryOpSequence;
  AccumErr(CustomQuerySeq.OK, CustomQuerySeq.Msg);
  Assert(not Assigned(FResult));
  FResult := CustomQuerySeq.Result;
  CustomQuerySeq.Result := nil;
  SignalCompletion;
end;


function TCustomQueryMultiOp.Start: boolean;
begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Custom query start.');
{$ENDIF}
  if not inherited then
    result := false
  else
  begin
    result := FParentLoader.FDataStore.CustomQuery(
      FLevel, FTempKeyList, FTempFilter, Self, nil);
    if result then
    begin
      //Let the op sequence own this intermediate state.
      FTempKeyList := nil; //Owned by caller.
      FTempFilter := nil; //Owned by op sequence.
    end;
    FOK := result;
  end;
end;

destructor TCustomQueryMultiOp.Destroy;
begin
  //FTempKeyList.Free; The key list is not ours.
  FTempFilter.Free; //But the filter nominally is.
  FResult.Free;
  inherited;
end;

procedure TCustomQueryMultiOp.GetStats(var ResultStats: TBatchLoaderStats);
begin
  FillChar(ResultStats, sizeof(ResultStats), 0);
end;

function TCustomQueryMultiOp.GetOpDetails: string;
begin
  result := S_CUSTOM_QUERY;
end;


{ TDBInitMultiOp }

procedure TDBInitMultiOp.IncStatsForLoadUserList;
var
  UL: TKUserList;
begin
  UL := FParentLoader.FDataStore.GetTemporaryTopLevelList;
  try
    FStats.UserProfilesChanged := UL.Count;
  finally
    FParentLoader.FDataStore.PutTopLevelList(UL);
  end;
end;

procedure TDBInitMultiOp.IncStatsForLoadUserTree(const UserKey: TGuid);
var
  UP: TKUserProfile;
  MI: TKMediaItem;
begin
  if UserKey <> TGuid.Empty then
  begin
    UP := FParentLoader.FDataStore.GetDurableReadOnlyTreeByKey(UserKey);
    try
      if not Assigned(UP) then exit;
      Inc(FStats.UserProfilesChanged);
      MI := UP.Media.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKMediaItem;
      while Assigned(MI) do
      begin
        Inc(FStats.MediaItemsChanged);
        Inc(FStats.CommentItemsChanged, MI.Comments.Count);
        MI := UP.Media.AdjacentBySortVal(katNext, ksvPointer, MI) as TKMediaItem;
      end;
    finally
      FParentLoader.FDataStore.PutDurable(UP);
    end;
  end;
end;

function TDBInitMultiOp.InitForLoadUserList: boolean;
begin
  FType := motDbLoadUserList;
  result := true;
end;

function TDBInitMultiOp.InitForLoadUserTree(UP: TKUserProfile): boolean;
var
  DBProfile: TKUserProfile;
begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Init for load user tree.');
{$ENDIF}
  result := Assigned(UP) and (UP.InterestLevel > kpiFetchUserForRefs);
  if result then
  begin
    FUserKeysRemaining.Free;
    FUserKeysRemaining := TKUserList.Create;
    FType := motDbLoadUserTrees;
    DBProfile := TKUSerProfile.Clone(UP) as TKUSerProfile;
    // Must clone, since we are adding to separate list.
{$IFDEF DEBUG_BATCH_LOADER}
    GLogLog(SV_INFO, 'Init load user tree add user profile: ' + GUIDToString(DBProfile.Key));
{$ENDIF}
    result := FUserKeysRemaining.Add(DBProfile);
    if not result then
      DBProfile.Free;
  end;
  if result then
    FPresentationUserKeysRemaining := TKList.CloneListAndItemsOnly(FUserKeysRemaining) as TKUserList;
end;

function TDBInitMultiOp.InitForLoadAllUserTrees: boolean;
begin
  FType := motDbLoadUserTrees;
  result := MakeInterestedUserKeyListFromDB;
  if result then
    FPresentationUserKeysRemaining := TKList.CloneListAndItemsOnly(FUserKeysRemaining) as TKUserList;
end;

function TDBInitMultiOp.GetOpDetails: string;
begin
  if FType = motDbLoadUserList then
    result := S_LOAD_USER_LIST
  else
    result := S_LOAD_USER_TREES + inherited;
end;

procedure TDBInitMultiOp.HandleDataStoreCompletion(Sender: TObject);
var
  ReadUserListSeq: TReadUserListSequence;
  ReadUserTreeSeq: TReadUserTreeSequence;
  SearchKey: TGuid;
  UserProfile: TKUSerProfile;
begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Load user trees handle data store completion.');
{$ENDIF}
  SearchKey := TGuid.Empty;
  case FType of
    motDbLoadUserList:
      begin
{$IFDEF DEBUG_BATCH_LOADER}
        GLogLog(SV_INFO, 'Load user list op completion.');
{$ENDIF}
        ReadUserListSeq := Sender as TReadUserListSequence;
        AccumErr(ReadUserListSeq.OK, ReadUserListSeq.Msg);
        SignalCompletion;
        exit;
      end;
    motDbLoadUserTrees:
      begin
{$IFDEF DEBUG_BATCH_LOADER}
        GLogLog(SV_INFO, 'Load user tree op completion.');
{$ENDIF}
        ReadUserTreeSeq := Sender as TReadUserTreeSequence;
        AccumErr(ReadUserTreeSeq.OK, ReadUserTreeSeq.Msg);
        SearchKey := ReadUserTreeSeq.ReadKey;
        Assert((SearchKey <> TGuid.Empty) or not OK);
      end;
  else
    Assert(false);
  end;

  if SearchKey <> TGuid.Empty then
  begin
{$IFDEF DEBUG_BATCH_LOADER}
    GLogLog(SV_INFO, 'Load user tree op completion for key: ' + GUIDToString(SearchKey));
{$ENDIF}
    UserProfile := FUserKeysRemaining.SearchByInternalKeyOnly(SearchKey) as TKUSerProfile;
    if Assigned(UserProfile) then
    begin
{$IFDEF DEBUG_BATCH_LOADER}
      GLogLog(SV_INFO, 'Load user tree op completion found key: ' + GUIDToString(SearchKey));
{$ENDIF}
      FUserKeysRemaining.Remove(UserProfile);
      UserProfile.Free;
    end
    else
    begin
{$IFDEF DEBUG_BATCH_LOADER}
      GLogLog(SV_INFO, 'Load user tree op completion key NOT found!: ' + GUIDToString(SearchKey));
{$ENDIF}
    end;
  end;
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Load user tree keys left TODO: ' + IntToStr(FUserKeysRemaining.Count));
{$ENDIF}
  if FUserKeysRemaining.Count = 0 then
    SignalCompletion;
end;

function TDBInitMultiOp.Stop: boolean;
begin
  result := inherited;
  case FType of
    motDbLoadUserList: result := false;
    motDbLoadUserTrees: result := result and (FUserKeysRemaining.Count = 0);
  else
    Assert(false);
  end;
end;

function TDBInitMultiOp.Start: boolean;
var
  UserProf, OldUserProf: TKUSerProfile;
begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'TDBInitMultiOp.Start');
{$ENDIF}
  if not inherited then
  begin
    result := false;
    exit;
  end;
  FOK := true;
  case FType of
    motDbLoadUserList:
      begin
        // Start a single datastore op
        // Ignore hint bits at this point (interferes with start logic).
        result := FParentLoader.FDataStore.ReadUserListToMem(Self, nil);
      end;
    motDbLoadUserTrees:
      begin
        // Start a datastore op per item, if first succeeds, all should succeed,
        // However, for the sake of datastrcuture integrity, if any start,
        // then we should just say we started OK.
        result := true;
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Batch loader: UserKeysRemaining count: ' + IntToStr(FUserKeysRemaining.Count));
{$ENDIF}
        UserProf := FUserKeysRemaining.AdjacentBySortVal(katFirst, ksvPointer, nil)
          as TKUSerProfile;
        while Assigned(UserProf) and result do
        begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Batch loader: Issue ReadUserTree to datastore: ' + GUIDToString(UserProf.Key));
{$ENDIF}
          Assert(FType = motDbLoadUserTrees);
          result := result and FParentLoader.FDataStore.ReadUserTreeToMem(UserProf, Self, nil);
          // Start all the ops in parallel.
          if result then
            UserProf := FUserKeysRemaining.AdjacentBySortVal(katNext, ksvPointer, UserProf)
              as TKUSerProfile;
        end;
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Batch loader: UserKeysRemaining count after ops started: ' + IntToStr(FUserKeysRemaining.Count));
{$ENDIF}
        if Assigned(UserProf) then
        begin
          // Only delete items we haven't started.
          while Assigned(UserProf) do
          begin
            OldUserProf := UserProf;
            UserProf := FUserKeysRemaining.AdjacentBySortVal(katNext, ksvPointer, UserProf)
              as TKUSerProfile;
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Batch loader: Removing profile not started ' + GUIDToString(OldUserProf.Key));
{$ENDIF}
            FUserKeysRemaining.Remove(OldUserProf);
            OldUserProf.Free;
          end;
          FOK := false;
          FMsg := S_CLOSING_OR_INTERNAL_ERROR;
        end;
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Batch loader: UserKeysRemaining count after unstarted trimmed: ' + IntToStr(FUserKeysRemaining.Count));
{$ENDIF}
      end;
  else
    result := false;
    Assert(false);
  end;
end;

{ TDBPurgeMultiOp }

procedure TDBPurgeMultiOp.IncStatsForDatastoreCompletion;
begin
  Inc(FStats.UserProfilesChanged);
end;

procedure TDBPurgeMultiOp.HandleDataStoreCompletion(Sender: TObject);
var
  Seq: TOpSequence;
  FinishedPhase: boolean;
begin
  Dec(FOpsThisPhase);
  Assert(FOpsThisPhase >= 0);
  Seq := Sender as TOpSequence;
  AccumErr(Seq.OK, Seq.Msg);
  FinishedPhase := FOpsThisPhase = 0;
  IncStatsForDatastoreCompletion;
  if FinishedPhase then
  begin
    case FPurgePhase of
      pmoWritebackInterest:
        begin
          if not StartPurge then
            SignalCompletion;
        end;
      pmoDoPurge:
        SignalCompletion;
    else
      Assert(false);
    end;
  end;
end;

procedure TDBPurgeMultiOp.IncrementUserKeyCursor(First: boolean; var Cursor: TKUserProfile);
begin
  if FStopping then
  begin
    Cursor := nil;
    exit;
  end;
  if First then
    Cursor := FUserKeysRemaining.AdjacentBySortVal(katFirst, ksvPointer, nil)
      as TKUSerProfile
  else
    Cursor := FUserKeysRemaining.AdjacentBySortVal(katNext, ksvPointer, Cursor)
      as TKUSerProfile;
end;

// NB. Phase handling here not exactly same as import multi-ops.
function TDBPurgeMultiOp.StartWriteBackInterest: boolean;
var
  User: TKUSerProfile;
  Cursor: TKUserProfile;
begin
  result := false;
  if FStopping or (FPurgePhase <> TPurgeMultiOpPhase.pmoWritebackInterest) then
    exit;
  // Do all the ops in parallel, use count.
  User := TKUSerProfile.Create;
  try
    IncrementUserKeyCursor(true, Cursor);
    while Assigned(Cursor) do
    begin
      User.Assign(Cursor);
      User.InterestLevel := kpiFetchUserForRefs; // Lower interest.
      if FParentLoader.FDataStore.UpdateImportedUser(User, Self, nil, [patUpdate]) then
      begin
        result := true; // started some ops.
        Inc(FOpsThisPhase);
      end;
      IncrementUserKeyCursor(false, Cursor);
    end;
  finally
    User.Free;
  end;
end;

function TDBPurgeMultiOp.StartPurge: boolean;
var
  Cursor: TKUserProfile;
begin
  result := false;
  if FStopping then
    exit;
  FPurgePhase := TPurgeMultiOpPhase.pmoDoPurge;
  // Do all the ops in parallel, use count.
  IncrementUserKeyCursor(true, Cursor);
  while Assigned(Cursor) do
  begin
    if FParentLoader.FDataStore.DeleteUserData(Cursor, FPurgeLevel, FPurgeType, Self, nil)
    then
    begin
      result := true; // started some ops.
      Inc(FOpsThisPhase);
    end;
    IncrementUserKeyCursor(false, Cursor);
  end;
end;

function TDBPurgeMultiOp.Start: boolean;
begin
  if not inherited then
  begin
    result := false;
    exit;
  end;
  FOK := true;
  result := StartWriteBackInterest;
  if not result then
    result := StartPurge;
end;

function TDBPurgeMultiOp.Stop: boolean;
begin
  result := inherited and (FOpsThisPhase = 0);
end;

function TDBPurgeMultiOp.InitForDecreasedInterest(UP: TKUSerProfile): boolean;
var
  DBProfile: TKUSerProfile;
begin
  // Check profile we want to load is already in data-store...
  // (aka, where did you get that GUID key from?)
  result := Assigned(UP) and (UP.InterestLevel > kpiFetchUserForRefs);
  if result then
  begin
    FUserKeysRemaining.Free;
    FUserKeysRemaining := TKUserList.Create;
    FDecreasedInterest := true;
    FType := motPurgeOrDecrease;
    FPurgeLevel := klMediaList;
    FPurgeType := dtMemAndDb;
    FPurgePhase := pmoWritebackInterest;
    DBProfile := TKUSerProfile.Clone(UP) as TKUSerProfile;
    result := FUserKeysRemaining.Add(DBProfile);
    if not result then
      DBProfile.Free;
  end;
  if result then
    FPresentationUserKeysRemaining := TKList.CloneListAndItemsOnly(FUserKeysRemaining)  as TKUserList;
end;

function TDBPurgeMultiOp.InitDecreaseByCriteria(Crit: TUserSelectionCriteria): boolean;
var
  DBList: TKUserList;
  DBUser: TKUSerProfile;
  DBProfile: TKUSerProfile;
  res2: boolean;
begin
  DBList := FParentLoader.FDataStore.GetTemporaryTopLevelList;
  try
    FUserKeysRemaining.Free;
    FUserKeysRemaining := TKUserList.Create;
    FDecreasedInterest := true;
    FType := motPurgeOrDecrease;
    FPurgeLevel := klMediaList;
    FPurgeType := dtMemAndDb;
    FPurgePhase := pmoWritebackInterest;
    DBUser := DBList.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUSerProfile;
    res2 := true;
    result := false;
    while Assigned(DBUser) and res2 do
    begin
      if (DBUser.InterestLevel > kpiFetchUserForRefs) and UserSelectionCriteriaSatisfied(DBUser,
        Crit) then
      begin
        DBProfile := TKUSerProfile.Clone(DBUser) as TKUSerProfile;
        res2 := FUserKeysRemaining.Add(DBProfile);
        if not res2 then
          DBProfile.Free;
        result := result or res2;
      end;
      DBUser := DBList.AdjacentBySortVal(katNext, ksvPointer, DBUser) as TKUSerProfile;
    end;
  finally
    FParentLoader.FDataStore.PutTopLevelList(DBList);
  end;
  if result then
    FPresentationUserKeysRemaining := TKList.CloneListAndItemsOnly(FUserKeysRemaining)  as TKUserList;
end;

function TDBPurgeMultiOp.InitForPurgeUser(UP: TKUSerProfile; Level: TKListLevel;
  PurgeType: TDeleteType): boolean;
var
  DBProfile: TKUSerProfile;
begin
  // Check profile we want to load is already in data-store...
  // (aka, where did you get that GUID key from?)
  FUserKeysRemaining.Free;
  FUserKeysRemaining := TKUserList.Create;
  FType := motPurgeOrDecrease;
  FPurgeLevel := Level;
  FPurgeType := PurgeType;
  FPurgePhase := pmoDoPurge;
  result := Assigned(UP) and ((Level = klUserList) or (UP.InterestLevel > kpiFetchUserForRefs));
  if result then
  begin
    DBProfile := TKUSerProfile.Clone(UP) as TKUSerProfile;
    result := FUserKeysRemaining.Add(DBProfile);
    if not result then
      DBProfile.Free;
  end;
  if result then
    FPresentationUserKeysRemaining := TKList.CloneListAndItemsOnly(FUserKeysRemaining)  as TKUserList;
end;

function TDBPurgeMultiOp.InitForPurgeAll(Level: TKListLevel; PurgeType: TDeleteType): boolean;
begin
  FType := motPurgeOrDecrease;
  FPurgeLevel := Level;
  FPurgeType := PurgeType;
  FPurgePhase := pmoDoPurge;
  result := MakeInterestedUserKeyListFromDB;
  if result then
    FPresentationUserKeysRemaining := TKList.CloneListAndItemsOnly(FUserKeysRemaining)  as TKUserList;
end;

function TDBPurgeMultiOp.GetOpDetails: string;
begin
  result := S_PURGE_FROM;
  case FPurgeLevel of
    klUserList: result := result + S_DEL_USERL;
    klMediaList: result := result + S_DEL_MEDIAL;
    klCommentList: result := result + S_DEL_COMMENTL;
  else
    Assert(false);
  end;
  if FPurgeType = dtMemOnly then
    result := result + S_MEM_ONLY
  else
    result := result + S_MEM_DISK;
  result := result + inherited;
end;

{  TDBExpireMultiOp }

function TDBExpireMultiOp.InitForExpireUser(UP: TKUserProfile;
                           ExpireBefore: TDateTime; ExpiryType: TDBExpiryType;
                           LevelSet: TKListLevelSet): boolean;
var
  DBProfile: TKUserProfile;
begin
  FUserKeysRemaining.Free;
  FUserKeysRemaining := TKUserList.Create;
  FType := motExpire;
  FExpireBefore := ExpireBefore;
  FExpiryType := ExpiryType;
  FLevelSet := LevelSet;
  result := Assigned(UP);
  if result then
  begin
    DBProfile := TKDataObject.Clone(UP) as TKUserProfile;
    result := FUserKeysRemaining.Add(DBProfile);
    if not result then
      DBProfile.Free;
  end;
  if result then
    FPresentationUserKeysRemaining := TKList.CloneListAndItemsOnly(FUserKeysRemaining)  as TKUserList;
end;

function TDBExpireMultiOp.InitForExpireAll(ExpireBefore: TDateTime; ExpiryType: TDBExpiryType;
                           LevelSet: TKListLevelSet): boolean;
begin
  FType := motExpire;
  FExpireBefore := ExpireBefore;
  FExpiryType := ExpiryType;
  FLevelSet := LevelSet;
  //With ExpireAll, we need to run the expiration phase even if no
  //user trees directly affected, or no users being mornitored.
  MakeInterestedUserKeyListFromDB;
  FPresentationUserKeysRemaining := TKList.CloneListAndItemsOnly(FUserKeysRemaining)  as TKUserList;
  result := true;
end;

procedure TDBExpireMultiOp.HandleDataStoreCompletion(Sender: TObject);
var
  OpSeq: TOpSequence;
begin
  Dec(FOpsThisPhase);
  Assert(FOpsThisPhase >= 0);
  OpSeq := Sender as TOpSequence;
  AccumErr(OpSeq.OK, OpSeq.Msg);
  //TODO - Stats?
  if FOpsThisPhase = 0 then
  begin
    case FPhase of
      emoExpire:
      begin
        if not StartPrune then
          SignalCompletion;
      end;
      emoPrune: SignalCompletion;
    else
      Assert(false);
    end;
  end;
end;

function TDBExpireMultiOp.StartExpire: boolean;
var
  UP: TKUserProfile;
begin
  FPhase := emoExpire;
  result := false;
  UP := FUserKeysRemaining.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
  while Assigned(UP) do
  begin
    if FParentLoader.FDataStore.ExpireUserData(UP,
                                               FExpireBefore,
                                               FExpiryType,
                                               FLevelSet,
                                               self, nil)  then
    begin
      result := true;
      Inc(FOpsThisPhase);
    end;
    UP := FUserKeysRemaining.AdjacentBySortVal(katNext, ksvPointer, UP) as TKUserProfile;
  end;
end;

function TDBExpireMultiOp.StartPrune: boolean;
begin
  FPhase := emoPrune;
  result := FParentLoader.FDataStore.PruneUnusedData(Self, nil);
  if result then
    Inc(FOpsThisPhase);
end;

function TDBExpireMultiOp.Start: boolean;
begin
  result := false;
  if FStopping then
      exit;
  result := StartExpire;
  if not result then
    result := StartPrune;
  FOK := result;
end;

function TDBExpireMultiOp.Stop: boolean;
begin
  result := inherited and (FOpsThisPhase = 0);
end;

function TDBExpireMultiOp.GetOpDetails: string;
begin
  result := S_EXPIRE_FROM;
  //TODO - More detail on this in due course?
end;

{ TMultiOp }

procedure TMultiOp.AccumErr(OK: boolean; Msg: string);
begin
  if (not OK) and FOK then
  begin
{$IFDEF DEBUG_BATCH_LOADER}
    GLogLog(SV_INFO, 'Multi op accumulate error, ERROR: ' + Msg);
{$ENDIF}
    FOK := OK;
    FMsg := Msg;
  end;
end;

procedure TMultiOp.SignalCompletion;
begin
  FParentLoader.HandleMultiOpCompletion(Self);
end;

function TMultiOp.Start: boolean;
begin
  result := not FStopping;
end;

function TMultiOp.Stop: boolean;
begin
  FStopping := true;
  result := true;
end;

constructor TMultiOp.Create;
begin
  inherited;
  DLItemInitObj(Self, @FQueueLink);
end;

function TUserImportLocation.GetLocationObj: TKKeyedObject;
begin
  if Assigned(FComment) then
    result := FComment
  else
  if Assigned(FMedia) then
    result := FMedia
  else
    result := FUser;
  Assert((not Assigned(result)) or (result is TKKeyedObject));
end;

procedure TUserImportLocation.Increment(UserTree: TKUSerProfile; var DeleteTreeFragment: boolean);
var
  ValidItemBlock: boolean;
begin
{$IFDEF DEBUG_BATCH_LOADER_VERBOSE}
  if Assigned(LocationObj) then
    GLogLog(SV_INFO, 'User import, increment start: ' +
      IntToHex(Uint64(LocationObj), 16) + ' class type: ' + LocationObj.ClassName)
  else
    GLogLog(SV_INFO, 'User import, increment start: <null location>');
  try
{$ENDIF}
    Assert(Assigned(UserTree));
    DeleteTreeFragment := false;
    ValidItemBlock := false;
    repeat
      if not Assigned(FUser) then
      begin
        Assert(not Assigned(FMedia));
        Assert(not Assigned(FComment));
        FUser := UserTree;
        DeleteTreeFragment := true;
        Assert((not Assigned(FUser)) or (FUser is TKUserProfile));
      end
      else
      begin
        if not Assigned(FMedia) then
        begin
          Assert(not Assigned(FComment));
          FMedia := FUser.Media.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKMediaItem;
          FMediaPhase := mlpInitialFetch;
          DeleteTreeFragment := true;
          Assert((not Assigned(FMedia)) or (FMedia is TKMediaItem));
        end
        else
        begin
          //Don't delete tree fragment moving from media to comments.
          if not Assigned(FComment) then
            FComment := FMedia.Comments.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKCommentItem
          else
            FComment := FMedia.Comments.AdjacentBySortVal(katNext, ksvPointer, FComment) as TKCommentItem;
          Assert((not Assigned(FComment)) or (FComment is TKCommentItem));
          if not Assigned(FComment) then
          begin
            //Don't delete tree fragment moving back to media and incrementing phase.
            if FMediaPhase = mlpInitialFetch then
              Inc(FMediaPhase)
            else
            begin
              //Do delete tree fragment moving to next (or last) media.
              FMedia := FUser.Media.AdjacentBySortVal(katNext, ksvPointer, FMedia) as TKMediaItem;
              FMediaPhase := mlpInitialFetch;
              DeleteTreeFragment := true;
              Assert((not Assigned(FMedia)) or (FMedia is TKMediaItem));
            end;
          end;
        end;
        if not Assigned(FMedia) then
          FUser := nil;
      end;
      if not Assigned(FUser) then
      begin
        if FSiteType < High(FSiteType) then
        begin
          Inc(FSiteType);
          FUser := UserTree;
          Assert((not Assigned(FUser)) or (FUser is TKUserProfile));
        end
        else
        begin
          FSiteType := Low(FSiteType);
          exit;
        end;
      end;
      Assert(Assigned(LocationObj));
      if LocationObj is TKUserProfile then
        ValidItemBlock := (LocationObj as TKUserProfile)
          .SiteUserBlocks[FSiteType].Valid
      else if LocationObj is TKMediaItem then
        ValidItemBlock := (LocationObj as TKMediaItem)
          .SiteMediaBlocks[FSiteType].Valid
      else if LocationObj is TKCommentItem then
        ValidItemBlock := (LocationObj as TKCommentItem)
          .SiteCommentBlocks[FSiteType].Valid
      else
      begin
        Assert(false);
        exit;
      end;
    until ValidItemBlock;
{$IFDEF DEBUG_BATCH_LOADER_VERBOSE}
  finally
    if Assigned(LocationObj) then
      GLogLog(SV_INFO, 'User import, increment end: ' +
        IntToHex(Uint64(LocationObj), 16) + ' class type: ' + LocationObj.ClassName)
    else
      GLogLog(SV_INFO, 'User import, increment end: <null location>');
    GLogLog(SV_INFO, 'User import, increment end, delete frag: '
      + BoolToStr(DeleteTreeFragment, true));
  end;
{$ENDIF}
end;

{ TUserImportRec }

procedure TUserImportRec.GetStats(var ResultStats: TBatchLoaderStats);
begin
  ResultStats := FStats;
end;

procedure TUserImportRec.IncStatsForUProfPersist(MediaCount: cardinal);
begin
  Inc(FStats.UserProfilesChanged);
  Inc(FStats.MediaItemsChanged, MediaCount);
end;

procedure TUSerImportRec.IncStatsForMProfPersist(CommentCount: cardinal);
begin
  Inc(FStats.MediaItemsChanged);
  Inc(FSTats.CommentItemsChanged, CommentCount);
end;

procedure TUserImportRec.IncStatsForNewDependentProfile;
begin
  Inc(FStats.NewProfilesDiscovered);
end;

function TUserImportRec.Stop: boolean;
begin
  FStopping := true;
  // By default, we automatically have things in progress.
  // Could calculate according to phase exactly what's going on, but better
  // to just say "we're busy", and wait for things to go down the "SignalCompletion" path.
  result := false;
  // Assume external code has already made sure that no new DataStore ops will start.
  // So that just leaves the importers.
  if Assigned(FProfileImporter) then
    FProfileImporter.Stop;
  if Assigned(FMediaImporter) then
    FMediaImporter.Stop;
end;

function AllBailSet(const BailSet: TSiteTypeBools): boolean;
var
  ST: TKSiteType;
begin
  result := true;
  for ST := Low(ST) to High(ST) do
  begin
    if not BailSet[ST] then
      result := false;
  end;
end;


//TODO.
//1. Initial dev: See if we can put the bailout hints in the accumulated tree
//   and don't need separate bailout tree.
//2. Later dev: Don't calc bailouts but use last updated timestamps (dealing with cancel?).
//3. Much later dev: don't refresh all, find another way to allow the user
//   to browse up to date datastore, and/or find a way of doing load
//   user trees in parallel.
procedure TUserImportRec.RefreshUserSetBailoutsCommon(StoreUP: TKUSerProfile);
var
  DBProfileClone: TKUSerProfile;
  StoreMI, AccumulatedMI: TKMediaItem;
  CommentFound: TSiteTypeBools;
  ST: TKSiteType;
  StoreCI: TKCommentItem;
  MediaCursor: TKMediaItem;
  AccumulatedCI: TKCommentItem;
begin
  //Pre-load UP, All media items, and most recent comment for each site type.
//  StoreUP := FParentMultiOp.FParentLoader.FDataStore.GetDurableReadOnlyTreeByKey(FAccumulatedTree.Key);
//  result := Assigned(StoreUP);
//  if result then
//  begin
//    try
      //User profile, and all media items.
      DBProfileClone := StoreUP.CloneSelfAndChildKeyed as TKUserProfile;
      FAccumulatedTree.MergeWithNewer(DBProfileClone, [tmhDontChangeInterest]);
      DBProfileClone.Free;
      //FAccumulatedTree does not contain any comments.
      StoreMI := StoreUP.Media.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKMediaItem;
      while Assigned(StoreMI) do
      begin
        AccumulatedMI := FAccumulatedTree.Media.SearchByInternalKeyOnly(StoreMI.Key) as TKMediaItem;
        Assert(Assigned(AccumulatedMI));
        //For each valid site type in MI, find the most recent comment.
        for ST := Low(ST) to High(ST) do
          CommentFound[ST] := not StoreMI.SiteMediaBlock[ST].Valid;

        for ST := Low(ST) to High(ST) do
        begin
          if not CommentFound[ST] then
          begin
            StoreCI := StoreMI.Comments.AdjacentByTimeIndex(katLast, tsvDate, nil) as TKCommentItem;
            while Assigned(StoreCI) and not CommentFound[ST] do
            begin
              if StoreCI.SiteCommentBlock[ST].Valid then
              begin
                //Found the most recent comment, need to see if we can merge in
                //or add.
                AccumulatedCI := AccumulatedMI.Comments.SearchByInternalKeyOnly(StoreCI.Key) as TKCommentItem;
                if not Assigned(AccumulatedCI) then
                begin
                  AccumulatedCI := TKDataObject.DeepClone(StoreCI) as TKCommentItem;
                  AccumulatedMI.Comments.Add(AccumulatedCI);
                end;
                CommentFound[ST] := true;
              end;
              StoreCI := StoreMI.Comments.AdjacentByTimeIndex(katPrevious, tsvDate, StoreCI) as TKCommentItem;
            end;
          end;
        end;
        StoreMI := StoreUP.Media.AdjacentBySortVal(katNext, ksvPointer, StoreMI) as TKMediaItem;
      end;
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
        FAccumulatedTree.SetRefsRecursive(self);
{$ENDIF}
      FTreeBailoutData := TKDataObject.DeepClone(FAccumulatedTree) as TKUSerProfile;
      MediaCursor := FTreeBailoutData.Media.AdjacentBySortVal(katFirst, ksvPointer, nil)
        as TKMediaItem;
      while Assigned(MediaCursor) do
      begin
        SetMediaBailout(MediaCursor);
        MediaCursor := FTreeBailoutData.Media.AdjacentBySortVal(katNext, ksvPointer, MediaCursor)
          as TKMediaItem;
      end;
      SetUserBailout;
//    finally
//      FParentMultiOp.FParentLoader.FDataStore.PutDurable(StoreUP);
//    end;
//  end;
end;

procedure TUserImportRec.SetUserBailout;
var
  MItem, MItem2: TKMediaItem;
  Bail: TBailoutHint;
  BailSet: TSiteTypeBools;
  ST: TKSiteType;

begin
  for ST := Low(ST) to High(ST) do
  begin
    BailSet[ST] := false;
  end;
  // Set bailout for user if approp,
  // Prune media items that don't have bailouts.
  MItem := FTreeBailoutData.Media.AdjacentByTimeIndex(katLast, tsvDate, nil) as TKMediaItem;
  while Assigned(MItem) and not AllBailSet(BailSet) do
  begin
    for ST := Low(ST) to High(ST) do
    begin
      if (not BailSet[ST]) and MItem.SiteMediaBlock[ST].Valid then
      begin
        if not Assigned(FTreeBailoutData.Ref) then
        begin
          Bail := TBailoutHint.Create;
          FTreeBailoutData.Ref := Bail;
        end
        else
          Bail := FTreeBailoutData.Ref as TBailoutHint;
        Bail.BailTimes[ST] := MItem.Date;
        BailSet[ST] := true;
      end;
    end;
    MItem := FTreeBailoutData.Media.AdjacentByTimeIndex(katPrevious, tsvDate, MItem) as TKMediaItem;
  end;
  // Now remove media items without bailout hints.
  MItem := FTreeBailoutData.Media.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKMediaItem;
  while Assigned(MItem) do
  begin
    if Assigned(MItem.Ref) then
      MItem := FTreeBailoutData.Media.AdjacentBySortVal(katNext, ksvPointer, MItem) as TKMediaItem
    else
    begin
      MItem2 := MItem;
      MItem := FTreeBailoutData.Media.AdjacentBySortVal(katNext, ksvPointer, MItem) as TKMediaItem;
      FTreeBailoutData.Media.Remove(MItem2);
      MItem2.Free;
    end;
  end;
end;

procedure TUserImportRec.SetMediaBailout(MediaItem: TKMediaItem);
var
  CItem: TKCommentItem;
  Bail: TBailoutHint;
  BailSet: TSiteTypeBools;
  ST: TKSiteType;

begin
  for ST := Low(ST) to High(ST) do
  begin
    BailSet[ST] := false;
  end;

  CItem := MediaItem.Comments.AdjacentByTimeIndex(katLast, tsvDate, nil) as TKCommentItem;
  while Assigned(CItem) and not AllBailSet(BailSet) do
  begin
    for ST := Low(ST) to High(ST) do
    begin
      if (not BailSet[ST]) and CItem.SiteCommentBlock[ST].Valid then
      begin
        if not Assigned(MediaItem.Ref) then
        begin
          Bail := TBailoutHint.Create;
          MediaItem.Ref := Bail;
        end
        else
          Bail := MediaItem.Ref as TBailoutHint;
        Bail.BailTimes[ST] := CItem.Date;
        BailSet[ST] := true;
      end;
    end;
    CItem := MediaItem.Comments.AdjacentByTimeIndex(katPrevious, tsvDate, CItem) as TKCommentItem;
  end;
  MediaItem.Comments.DeleteChildren;
end;

procedure TUserImportRec.ClearBailouts;
var
  Media: TKMediaItem;
begin
  if Assigned(FTreeBailoutData) then
  begin
    if Assigned(FTreeBailoutData.Ref) then
    begin
      FTreeBailoutData.Ref.Free;
      FTreeBailoutData.Ref := nil;
    end;
    Media := FTreeBailoutData.Media.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKMediaItem;
    while Assigned(Media) do
    begin
      if Assigned(Media.Ref) then
      begin
        Media.Ref.Free;
        Media.Ref := nil;
      end;
      Media := FTreeBailoutData.Media.AdjacentBySortVal(katNext, ksvPointer, Media) as TKMediaItem;
    end;
  end;
end;

function TUserImportRec.CommentItemProfileResolvable(CommentItem: TKCommentItem): boolean;
var
  ST: TKSiteType;
  UP: TKUSerProfile;
  Res: TKKeyedObject;
begin
  UP := TKUSerProfile.Create;
  try
    //First look in main datastore.
    for ST := Low(ST) to High(ST) do
    begin
      if CommentItem.SiteUserBlock[ST].Valid then
        UP.SiteUserBlock[ST].DeepAssign(CommentItem.SiteUserBlock[ST]);
    end;
    Res := FParentMultiOp.FParentLoader.FDataStore.GetDurableReadOnlyTreeByBlockArray(UP.SiteUserBlocks);
    result := Assigned(Res);
    if Assigned(Res) then
      FParentMultiOp.FParentLoader.FDataStore.PutDurable(Res as TKUserProfile);
    //If not there, check own look-aside list.
    if not result then
    begin
      for ST := Low(ST) to High(ST) do
      begin
        Res := FCommentOwnerKeysPersisted.SearchByDataBlock(ST, UP);
        if Assigned(Res) then
        begin
          result := true;
          break;
        end;
      end;
    end;
  finally
    UP.Free;
  end;
end;

function TUserImportRec.UserImportFetch: boolean;
var
  Opts: TImportOptions;
  Profile: TKUSerProfile;
begin
  result := false;
  Opts := TImportOptions.Create;
  try
    Profile := FLocation.LocationObj as TKUserProfile;
    SetOptsForUserImport(Opts);
    case FLocation.LocationSiteType of
      tstInstagram:
        result := (FProfileImporter as TInstaUserProfileImporter)
          .RequestUserProfile(Profile.SiteUserBlocks[FLocation.LocationSiteType], Opts);
      tstTwitter:
        result := (FProfileImporter as TTwitterUserProfileImporter)
          .RequestUserProfile(Profile.SiteUserBlocks[FLocation.LocationSiteType], Opts);
    else
      Assert(false);
    end;
  finally
    Opts.Free;
  end;
end;

procedure TUserImportRec.SetOptsForUserImport(Opts: TImportOptions);
var
  Profile: TKUserProfile;
begin
  Profile := FLocation.LocationObj as TKUserProfile;
  if (Profile.InterestLevel = TKProfileInterestLevel.kpiFetchUserForRefs) then
    Opts.InitialFetchOnly := true
  else
  begin
    Assert(Profile = FAccumulatedTree);
    Opts.BailoutTime := FSince;
    // If we have no media to go back to, then use default since value.
    if Assigned(FTreeBailoutData) and Assigned(FTreeBailoutData.Ref) then
    begin
      Opts.BailoutTime :=
        (FTreeBailoutData.Ref as TBailoutHint).BailTimes[FLocation.LocationSiteType];
    end;
  end;
end;

procedure TUserImportRec.SetOptsForMediaImport(Opts: TImportOptions);
var
  BailMediaItem: TKMediaItem;
begin
  Opts.BailoutTime := FSince;
  // If we have no media to go back to, then use default since value.
  if Assigned(FTreeBailoutData) then
  begin
    BailMediaItem := FTreeBailoutData.Media.SearchByDataBlock
      (FLocation.LocationSiteType,
       FLocation.LocationObj as TKMediaItem) as TKMediaItem;
    if Assigned(BailMediaItem) then
    begin
      Assert(Assigned(BailMediaItem.Ref));
      Opts.BailoutTime :=
        (BailMediaItem.Ref as TBailoutHint).BailTimes[FLocation.LocationSiteType];
    end;
  end;
end;

function TUserImportRec.MediaImportFetch: boolean;
var
  Opts: TImportOptions;
begin
  result := false;
  Opts := TImportOptions.Create;
  try
    SetOptsForMediaImport(Opts);
    case FLocation.LocationSiteType of
      tstInstagram:
        result := (FMediaImporter as TInstaMediaImporter)
          .RequestMedia(
            (FLocation.LocationObj as TKMediaItem).SiteMediaBlocks
              [FLocation.LocationSiteType], Opts);
      tstTwitter:
        result := (FMediaImporter as TTwitterMediaImporter)
          .RequestMedia(FAccumulatedTree.SiteUserBlock
            [FLocation.LocationSiteType],
            (FLocation.LocationObj as TKMediaItem).SiteMediaBlocks
              [FLocation.LocationSiteType], Opts);
    else
      Assert(false);
    end;
  finally
    Opts.Free;
  end;
end;


procedure TUserImportRec.UpdateImportersForSiteType;
begin
  FreeAndNil(FProfileImporter);
  FreeAndNil(FMediaImporter);
  case FLocation.LocationSiteType of
    tstInstagram:
      begin
        FProfileImporter := TInstaUserProfileImporter.Create;
        FProfileImporter.Ref := Self;
        FMediaImporter := TInstaMediaImporter.Create;
        FMediaImporter.Ref := Self;
      end;
    tstTwitter:
      begin
        FProfileImporter := TTwitterUserProfileImporter.Create;
        FProfileImporter.Ref := Self;
        FMediaImporter := TTwitterMediaImporter.Create;
        FMediaImporter.Ref := Self;
      end;
  else
    Assert(false);
  end;
  FProfileImporter.OnRequestCompleted := FParentMultiOp.HandleImporterCompletion;
  FMediaImporter.OnRequestCompleted := FParentMultiOp.HandleImporterCompletion;
end;

constructor TUserImportRec.Create;
begin
  inherited;
  FAccumulatedTree := TKUSerProfile.Create;
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
  FAccumulatedTree.SetRefsRecursive(self);
{$ENDIF}
  FLocation := TUserImportLocation.Create;
  FCommentOwnerKeysPersisted := TKUserList.Create;
end;

destructor TUserImportRec.Destroy;
begin
  FCommentOwnerKeysPersisted.Free;
  FLocation.Free;
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
  FAccumulatedTree.SetRefsRecursive(nil);
{$ENDIF}
  FAccumulatedTree.Free;
  FTreeFragment.Free;
  ClearBailouts;
  FTreeBailoutData.Free;
  FProfileImporter.Free;
  FMediaImporter.Free;
  inherited;
end;

function TUserImportRec.Start(var OutstandingOps: integer): boolean;
var
  DBProfile: TKUserProfile;
  DBS: TLoaderDataStore;
  PreLoad: boolean;
  RefCounts: TDataObRefCounts;
begin
  result := false;
  if FFailStarts or FStopping then
    exit;
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'UserImportRec: Start.');
{$ENDIF}
  DBS := FParentMultiOp.FParentLoader.FDataStore;
  PreLoad := (FAccumulatedTree.Key <> TGuid.Empty)
    and not (FParentMultiOp).MinimalRefresh;
  if PreLoad then
  begin
    DBProfile := DBS.GetDurableReadOnlyTreeByKey(FAccumulatedTree.Key);
    if Assigned(DBProfile) then
    begin
      try
        if Assigned(DBProfile.Ref) then
        begin
          RefCounts := DBProfile.Ref as TDataObRefCounts;
          PreLoad := not (TDataObHintBit.obhTreeUpToDate in RefCounts.HintBits);
        end;
      finally
        DBS.PutDurable(DBProfile);
      end;
    end;
  end;
  if PreLoad then
  begin
{$IFDEF DEBUG_BATCH_LOADER}
    GLogLog(SV_INFO, 'Batch loader: UserImportRec: Preload ReadUserTreeToMem: ' + GUIDToString(FAccumulatedTree.Key));
{$ENDIF}
    if FParentMultiOp.FParentLoader.FDataStore.ReadUserTreeToMem(FAccumulatedTree, FParentMultiOp,
      Self) then
    begin
      Inc(OutstandingOps);
      result := true;
    end;
  end
  else
  begin
{$IFDEF DEBUG_BATCH_LOADER}
    GLogLog(SV_INFO, 'Batch loader: UserImportRec: Preload not required or not possible. ' + GUIDToString(FAccumulatedTree.Key));
{$ENDIF}
  end;
end;

function TUserImportRec.StartBailCalcs(var OutstandingOps: integer): boolean;
var
  BailSetOK: boolean;
  StoreUP: TKUserProfile;
begin
  result := false;
  // No updates to result, because synchronous.
  if FFailStarts or FStopping then
    exit;
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'UserImportRec: Start bail calcs.');
{$ENDIF}
  //If importing a brand new user, we can get here without having any key
  //information (nothing in DB).

  //TODO - Multi-thread this particular bit of code.
  //Handling workitem completion should be relatively easy...
  if (FAccumulatedTree.Key <> TGuid.Empty)
    and not FParentMultiOp.MinimalRefresh then
  begin
    //Result always false, because we don't start any Async ops.
    StoreUP := FParentMultiOp.FParentLoader.FDataStore.GetDurableReadOnlyTreeByKey(FAccumulatedTree.Key);
    BailSetOK := Assigned(StoreUP);
    if BailSetOK then
    begin
      RefreshUserSetBailoutsCommon(StoreUP);
      FParentMultiOp.FParentLoader.FDataStore.PutDurable(StoreUP);
    end
    else
    begin
      FFailMsg := S_COULDNT_CALC_BAILOUTS;
      GLogLog(SV_WARN, FFailMsg);
      FFailStarts := true;
      exit;
    end;
  end;
end;

function TUserImportRec.IncrementLocation(var OutstandingOps: integer): boolean;
var
  OldSiteType: TKSiteType;
  OldLocationObj: TKKeyedObject;
  ItemSkipped: boolean;
  DeleteTreeFragment: boolean;
begin
  result := false;
  repeat
    if FFailStarts or FStopping then
      exit;
    ItemSkipped := false;
    OldSiteType := FLocation.LocationSiteType;
    OldLocationObj := FLocation.LocationObj;
    FImporterRetries := DefaultRetryCount;
    FLocation.Increment(FAccumulatedTree, DeleteTreeFragment);
    if DeleteTreeFragment then
    begin
      FTreeFragment.Free;
      FTreeFragment := nil;
    end;
    if (FLocation.LocationSiteType <> OldSiteType)
      or (Assigned(FLocation.LocationObj) <> Assigned(OldLocationObj)) then
      UpdateImportersForSiteType;

    result := Assigned(FLocation.LocationObj)
      and NextAction(OutstandingOps, ItemSkipped);
  until not ItemSkipped;
end;

function TUserImportRec.NextAction(var OutstandingOps: integer;  var ItemSkipped: boolean): boolean;
var
  DoFetch: boolean;
  DoDBWrite: boolean;
  ErrMsg: string;

  function DBAddDependentProfile(CommentItem: TKCommentItem): boolean;
  var
    ST: TKSiteType;
    UP: TKUserProfile;
  begin
    UP := TKUSerProfile.Create;
    try
      for ST := Low(ST) to High(ST) do
      begin
        if CommentItem.SiteUserBlock[ST].Valid then
          UP.SiteUserBlock[ST].DeepAssign(CommentItem.SiteUserBlock[ST]);
      end;
      result := FParentMultiOp.FParentLoader.FDataStore
        .UpdateImportedUser(UP, FParentMultiOp, Self, [patAdd]);
    finally
      UP.Free;
    end;
  end;

begin
  result := false;
  if FFailStarts or FStopping then
    exit;

  ItemSkipped := false;
  if Assigned(FLocation.LocationObj) then
  begin
    DoFetch :=
      ((FLocation.LocationObj is TKUserProfile) and not Assigned(FTreeFragment))
      or ((FLocation.LocationObj is TKMediaItem)
           and (FLocation.MediaPhase = mlpInitialFetch)
           and (not ((FLocation.LocationObj as TKMediaItem).MediaType
            in [mitMetaLink])) //TODO - HTMLWithQuote? - Skip retweets.
           and (not Assigned(FTreeFragment)));
    DoDBWrite :=
      ((FLocation.LocationObj is TKUSerProfile) and Assigned(FTreeFragment))
      or ((FLocation.LocationObj is TKMediaItem)
          and (FLocation.MediaPhase = mlpFinalWriteback)
          and Assigned(FTreeFragment))
      or ((FLocation.LocationObj is TKCommentItem) and
            not CommentItemProfileResolvable(FLocation.LocationObj as TKCommentItem));
    if DoFetch then
    begin
      Assert(not Assigned(FTreeFragment));
      if FImporterRetries > 0 then
      begin
        Dec(FImporterRetries);
        if FLocation.LocationObj is TKUSerProfile then
          result := UserImportFetch
        else
          result := MediaImportFetch;
        if result then
          Inc(OutstandingOps);
      end
      else
      begin
        if (FLocation.LocationObj is TKUserProfile) then
        begin
          ErrMsg := 'Too many retries, given up for User ID: ' +
            (FLocation.LocationObj as TKUSerProfile).SiteUserBlock
              [FLocation.LocationSiteType].UserId +
            ' username: '+
            (FLocation.LocationObj as TKUSerProfile).SiteUserBlock
              [FLocation.LocationSiteType].Username + ' ErrMsg: ' + FFailMsg;
          GLogLog(SV_FAIL, ErrMsg);
          FFailMsg := ErrMsg;
          FFailStarts := true;
        end
        else
        begin
          GLogLog(SV_FAIL, 'Too many retries, given up for Media ID: ' +
            (FLocation.LocationObj as TKMediaItem).SiteMediaBlock
              [FLocation.LocationSiteType].MediaID +
            ' code: ' +
            (FLocation.LocationObj as TKMediaItem).SiteMediaBlock
              [FLocation.LocationSiteType].MediaCode);
        end;
        //Try next user / media / whatever.
        ItemSkipped := true;
      end;
    end
    else if DoDBWrite then
    begin
      Assert(Assigned(FTreeFragment) or (FLocation.LocationObj is TKCommentItem));
      //Hummm. The tree fragment does not in fact always have newer information.
      //We really should write the amalgamated result back so as not to obliterate
      //info for other site types, user interest levels, etc.
      if FLocation.LocationObj is TKUserProfile then
      begin
        //If minimal refresh then remove all children before writing back,
        //this should not delete any child objects in the DB.
        FTreeFragment.FixupFromDBPersisted(FLocation.LocationObj);
        //TODO - Minimal refresh persist back to DB.
        IncStatsForUProfPersist((FTreeFragment as TKUserProfile).Media.Count);
        result :=
          FParentMultiOp.FParentLoader.FDataStore.UpdateImportedUser(
          FTreeFragment as TKUserProfile, FParentMultiOp, Self)
      end
      else if FLocation.LocationObj is TKMediaItem then
      begin
        FTreeFragment.FixupFromDBPersisted(FLocation.LocationObj);
        IncStatsForMProfPersist((FTreeFragment as TKMediaItem).Comments.Count);
        result :=
          FParentMultiOp.FParentLoader.FDataStore.UpdateImportedMedia(
          FTreeFragment as TKMediaItem, FParentMultiOp, Self)
      end
      else if FLocation.LocationObj is TKCommentItem then
      begin
        IncStatsForNewDependentProfile;
        result := DBAddDependentProfile(FLocation.LocationObj as TKCommentItem)
      end
      else
        Assert(false);
      if result then
        Inc(OutstandingOps);
    end
    else
    begin
      Assert(Assigned(FLocation.LocationObj));
      //We can end up skipping because of failed user fetch,
      //failed media fetch, media is metalink,
      //or comment owner does not need updating.
      ItemSkipped := true;
    end;
  end;
  if ItemSkipped then Assert(not result);
  if result then Assert(not ItemSkipped);
end;


function TUserImportRec.StartMainImport(var OutstandingOps: integer): boolean;
begin
  result := false;
  if FFailStarts or FStopping then
    exit;
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'UserImportRec: Start main import');
{$ENDIF}
  result := IncrementLocation(OutstandingOps);
end;

procedure TUserImportRec.HandleDataStoreCompletion(var OutstandingOps: integer; Sender: TObject);
var
  RdTree: TReadUserTreeSequence;
  WrUser: TWriteUserOpSequence;
  WrMedia: TWriteMediaOpSequence;
  OK: boolean;

  procedure DBAddDependentToLookaside(CommentItem: TKCommentItem);
  var
    ST: TKSiteType;
    UP: TKUserProfile;
  begin
    UP := TKUSerProfile.Create;
    try
      for ST := Low(ST) to High(ST) do
      begin
        if CommentItem.SiteUserBlock[ST].Valid then
          UP.SiteUserBlock[ST].DeepAssign(CommentItem.SiteUserBlock[ST]);
      end;
      if FCommentOwnerKeysPersisted.Add(UP) then
        UP := nil;
    finally
      UP.Free;
    end;
  end;


begin
  if FParentMultiOp.Phase = mopPreLoadTrees then
  begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'UserImportRec: Pre load tree completion.');
{$ENDIF}
    RdTree := Sender as TReadUserTreeSequence;
    Assert(FAccumulatedTree.Key <> TGuid.Empty);
    if not RdTree.OK then
    begin
      FFailMsg := S_DB_BAILOUT_READ_BAD + GuidToString((Sender as TReadUserTreeSequence).ReadKey);
      GLogLog(SV_WARN, FFailMsg);
      FFailStarts := true;
    end;
  end
  else if FParentMultiOp.Phase = mopMainImport then
  begin
    if Sender is TWriteUserOpSequence then
    begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'UserImportRec: Writer user op completion.');
{$ENDIF}
      WrUser := Sender as TWriteUserOpSequence;
      OK := WrUser.OK;
      Assert((FLocation.LocationObj is TKUserProfile)
        or (FLocation.LocationObj is TKCommentItem));
      if not OK then
      begin
        if FLocation.LocationObj is TKUserProfile then
        begin
          FFailMsg := S_DB_WRITEBACK_MAIN_UPROF + GuidToString(WrUser.PersistedKey) + S_DB_WRITEBACK_2 + WrUser.Msg +
            S_DB_WRITEBACK_3 + GuidToString(FAccumulatedTree.Key);
          GLogLog(SV_FAIL, FFailMsg);
        end
        else
        begin
          FFailMsg := S_DB_WRITEBACK_DEP_UPROF + GuidToString(WrUser.PersistedKey) + S_DB_WRITEBACK_2 + WrUser.Msg +
            S_DB_WRITEBACK_4 + GuidToString(FAccumulatedTree.Key);
          GLogLog(SV_WARN, FFailMsg);
          OK := true;
        end;
      end
      else
      begin
        if FLocation.LocationObj is TKCommentItem then
          DBAddDependentToLookaside(FLocation.LocationObj as TKCommentItem);
      end;
    end
    else if Sender is TWriteMediaOpSequence then
    begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'UserImportRec: Writer media op completion.');
{$ENDIF}
      WrMedia := Sender as TWriteMediaOpSequence;
      OK := WrMedia.OK;
      Assert(FLocation.LocationObj is TKMediaItem);
      if not OK then
      begin
        FFailMsg := S_MEDIA_WB_1 + GuidToString(WrMedia.PersistedKey)
          + S_MEDIA_WB_2 + WrMedia.Msg + S_MEDIA_WB_3
          + GuidToString(FAccumulatedTree.Key);
        GLogLog(SV_FAIL, FFailMsg);
      end;
    end
    else
    begin
      Assert(false);
      OK := false;
    end;
    if OK then
      IncrementLocation(OutstandingOps)
    else
      FFailStarts := true;
  end
  else
    Assert(false);
end;

procedure TUserImportRec.HandleImporterCompletion(var OutstandingOps: integer; Sender: TObject);
var
  ImpOK: boolean;
  ImportedObj, ImportCopy: TKKeyedObject;
  ErrInfo: TImportErrInfo;
  HintSet: TKMergeHintSet;
  ItemSkipped: boolean;
  ErrMsg: string;

  function GetUserImport(var ImportedObj: TKKeyedObject; var ErrInfo: TImportErrInfo): boolean;
  var
    UP: TKUSerProfile;
  begin
    case FLocation.LocationSiteType of
      tstInstagram:
        result := (FProfileImporter as TInstaUserProfileImporter).RetrieveResult(UP, ErrInfo)
          and ((ErrInfo.ImportErrLevel = ielOK) or (ErrInfo.ImportErrLevel = ielWarn));
      tstTwitter:
        result := (FProfileImporter as TTwitterUserProfileImporter)
          .RetrieveResult(UP, ErrInfo) and
          ((ErrInfo.ImportErrLevel = ielOK) or (ErrInfo.ImportErrLevel = ielWarn));
    else
      Assert(false);
      result := false;
    end;
    ImportedObj := UP;
  end;

  function GetMediaImport(var ImportedObj: TKKeyedObject; var ErrInfo: TImportErrInfo): boolean;
  var
    MI: TKMediaItem;
  begin
    case FLocation.LocationSiteType of
      tstInstagram:
        result := (FMediaImporter as TInstaMediaImporter).RetrieveResult(MI, ErrInfo) and
          ((ErrInfo.ImportErrLevel = ielOK) or (ErrInfo.ImportErrLevel = ielWarn));
      tstTwitter:
        result := (FMediaImporter as TTwitterMediaImporter).RetrieveResult(MI, ErrInfo) and
          ((ErrInfo.ImportErrLevel = ielOK) or (ErrInfo.ImportErrLevel = ielWarn));
    else
      Assert(false);
      result := false;
    end;
    ImportedObj := MI;
  end;

begin
  ImpOK := false;
  Assert(Assigned(FLocation.LocationObj));

  ErrInfo := nil;
  try
    if Sender is TUserProfileImporter then
    begin
      ImpOK := GetUserImport(ImportedObj, ErrInfo);
      HintSet := [tmhDontChangeInterest];
    end
    else if Sender is TMediaImporter then
    begin
      ImpOK := GetMediaImport(ImportedObj, ErrInfo);
      HintSet := [];
    end
    else
      Assert(false);

    Assert(not Assigned(FTreeFragment));
    if ImpOK then
    begin
      if ErrInfo.ImportErrLevel <> ielOK then
      begin
        if FLocation.LocationObj is TKUserProfile then
        begin
          ErrMsg := 'Profile incomplete data, HTTP: ' + IntToStr(ErrInfo.HTTPCode) +
            ' for username: ' + FAccumulatedTree.SiteUserBlock[FLocation.LocationSiteType].Username +
            ' ErrMsg : ' + ErrInfo.ErrMsg;
          GLogLog(SV_WARN, ErrMsg);
        end
        else if FLocation.LocationObj is TKMediaItem then
        begin
          ErrMsg := 'Media import incomplete data: ' + IntToStr(ErrInfo.HTTPCode) +
            ' for username: ' + FAccumulatedTree.SiteUserBlock[FLocation.LocationSiteType].Username +
            ' ErrMsg : ' + ErrInfo.ErrMsg;
          GLogLog(SV_WARN, ErrMsg);
        end
        else
          Assert(false);
      end;
      //Need to preserve entire imported tree for DB write.
      if FParentMultiOp.MinimalRefresh then
      begin
        if FLocation.LocationObj is TKUserProfile then
        begin
          Assert(ImportedObj is TKUserProfile);
          (ImportedObj as TkUserProfile).Media.DeleteChildren;
        end;
      end;

      ImportCopy := TKDataObject.DeepClone(ImportedObj) as TKKeyedObject;
      try
        Assert(ImportCopy is FLocation.LocationObj.ClassType);
        Assert(FLocation.LocationObj is ImportCopy.ClassType);
        FLocation.LocationObj.MergeWithNewer(ImportCopy, HintSet);
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
        FAccumulatedTree.SetRefsRecursive(self);
{$ENDIF}
      finally
        ImportCopy.Free;
      end;
      FTreeFragment := ImportedObj;
    end
    else
    begin
      if FLocation.LocationObj is TKUserProfile then
      begin
        if Assigned(ErrInfo) then
        begin
          ErrMsg := 'Profile import failed: ' + IntToStr(ErrInfo.HTTPCode) +
            ' for username: ' + FAccumulatedTree.SiteUserBlock
              [FLocation.LocationSiteType].Username +
            ' ErrMsg : ' + ErrInfo.ErrMsg;
          GLogLog(SV_FAIL, ErrMsg);
          if Length(FFailMsg) = 0 then
            FFailMsg := Errmsg;
        end;
      end
      else if FLocation.LocationObj is TKMediaItem then
      begin
        if Assigned(ErrInfo) then
        begin
          ErrMsg := 'Media import failed: ' + IntToStr(ErrInfo.HTTPCode) +
          ' for username: ' + FAccumulatedTree.SiteUserBlock
              [FLocation.LocationSiteType].Username + ' ErrMsg : ' +
            ErrInfo.ErrMsg;
          GLogLog(SV_FAIL, ErrMsg);
          if Length(FFailMsg) = 0 then
            FFailMsg := Errmsg;
        end;
      end
      else
        Assert(false);
    end;
    //Next action will either retry importer fetch, or start DB op.
    NextAction(OutstandingOps, ItemSkipped);
    //If retry fails, on to the next thing.
    if ItemSkipped then
      IncrementLocation(OutstandingOps);
  finally
    ErrInfo.Free;
  end;
end;

{ TImportMultiOp }

procedure TImportMultiOp.GetStats(var ResultStats: TBatchLoaderStats);
var
  IRec: TItemRec;
  UIRec: TUserImportRec;
  RecStats: TBatchLoaderStats;
begin
  FillChar(ResultStats, sizeof(ResultStats), 0);
  IRec := FImportRecs.GetAnItem;
  while Assigned(IRec) do
  begin
    UIRec := IRec.Item as TUserImportRec;
    UIRec.GetStats(RecStats);
    SumStats(ResultStats, RecStats);
    FImportRecs.GetAnotherItem(IRec);
  end;
end;

function TImportMultiOp.Stop: boolean;
var
  IRec: TItemRec;
  UIRec: TUserImportRec;
begin
  result := inherited;
  IRec := FImportRecs.GetAnItem;
  while Assigned(IRec) do
  begin
    UIRec := IRec.Item as TUserImportRec;
    result := UIRec.Stop and result;
    FImportRecs.GetAnotherItem(IRec);
  end;
end;

constructor TImportMultiOp.Create;
begin
  inherited;
  FImportRecs := TIndexedStore.Create;
end;

destructor TImportMultiOp.Destroy;
begin
  FImportRecs.DeleteChildren;
  FImportRecs.Free;
  inherited;
end;

procedure TImportMultiOp.HandleImporterCompletion(Sender: TObject);
var
  UserImportRec: TUserImportRec;
begin
  Dec(FImportsThisPhase);
  Assert(FImportsThisPhase >= 0);
  UserImportRec := (Sender as TImporter).Ref as TUserImportRec;
  UserImportRec.HandleImporterCompletion(FImportsThisPhase, Sender);
  Assert(FPhase = mopMainImport);
  if FImportsThisPhase = 0 then
    SignalCompletion;
end;

procedure TImportMultiOp.HandleDataStoreCompletion(Sender: TObject);
var
  UserImportRec: TUserImportRec;
  RdList: TReadUserListSequence;
begin
  Dec(FImportsThisPhase);
  Assert(FImportsThisPhase >= 0);
  case FPhase of
    mopPreLoadList:
    begin
      RdList := Sender as TReadUserListSequence;
      if not RdList.OK then
      begin
        FMsg := S_COMPLETE_USERLIST_FAILED + RdList.Msg;
        FOK := false;
        GLogLog(SV_WARN, FMsg);

        SignalCompletion;
        exit; //No point even trying to start UserImportRecs / Other phases.
      end;
      //No further action required.
      //As this point, the datastore should have no other ops in progress,
      //so entire userlist is durable in store (very useful!)
    end;
    mopPreLoadTrees, mopBailCalcs, mopMainImport:
    begin
      UserImportRec := (Sender as TOpSequence).Ref2 as TUserImportRec;
      UserImportRec.HandleDataStoreCompletion(FImportsThisPhase, Sender);
    end;
  else
    Assert(false);
  end;
  if FImportsThisPhase = 0 then
  begin
    case FPhase of
      mopPreLoadList: if not StartPreLoadTrees then SignalCompletion;
      mopPreLoadTrees: if not StartBailCalcs then SignalCompletion;
      mopBailCalcs: if not StartMainImport then SignalCompletion; //TODO - Shd this be here?
      mopMainImport: SignalCompletion;
    else
      Assert(false);
    end;
  end;
end;

procedure TImportMultiOp.SignalCompletion;
var
  IRec: TItemRec;
  UserImport: TUserImportRec;
  FailCount: integer;
  FailStrings: string;
begin
  // All done, but need to accumulate error messages before we quit.
  FailCount := 0;
  //Need to get error message from initial list load.
  if not FOK then
  begin
    FailCount := FImportRecs.Count;
    FailStrings := FMsg;
  end;
  IRec := FImportRecs.GetAnItem;
  while Assigned(IRec) do
  begin
    UserImport := IRec.Item as TUserImportRec;
    if UserImport.FFailStarts then
    begin
      if FOK then
        FailStrings := UserImport.FFailMsg
      else
        FailStrings := FailStrings + ', ' + UserImport.FFailMsg;
      FOK := false;
      Inc(FailCount);
    end;
    FImportRecs.GetAnotherItem(IRec);
  end;
  if FailCount > 0 then
    FMsg := IntToStr(FailCount) + ' imports failed: ' + FailStrings;
  inherited;
end;

function TImportMultiOp.StartMainImport: boolean;
var
  IRecImport: TItemRec;
  UserImportRec: TUserImportRec;
begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Import: Start main import.');
{$ENDIF}
  result := false;
  FPhase := mopMainImport;
  FImportsThisPhase := 0;
  IRecImport := FImportRecs.GetAnItem;
  while Assigned(IRecImport) do
  begin
    UserImportRec := IRecImport.Item as TUserImportRec;
    result := UserImportRec.StartMainImport(FImportsThisPhase) or result;
    FImportRecs.GetAnotherItem(IRecImport);
  end;
end;

function TImportMultiOp.StartBailCalcs: boolean;
var
  IRecImport: TItemRec;
  UserImportRec: TUserImportRec;
begin
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Import: Start bail calcs.');
{$ENDIF}
  result := false;
  FPhase := mopBailCalcs;
  FImportsThisPhase := 0;
  IRecImport := FImportRecs.GetAnItem;
  while Assigned(IRecImport) do
  begin
    UserImportRec := IRecImport.Item as TUserImportRec;
    result := UserImportRec.StartBailCalcs(FImportsThisPhase) or result;
    FImportRecs.GetAnotherItem(IRecImport);
  end;
  if not result then
    result := StartMainImport;
end;

function TImportMultiOp.StartPreLoadTrees: boolean;
var
  IRecImport: TItemRec;
  UserImportRec: TUserImportRec;
begin
  // Load user trees for all items to minimise bandwidth during import.
{$IFDEF DEBUG_BATCH_LOADER}
  GLogLog(SV_INFO, 'Import: Start pre-load trees.');
{$ENDIF}
  result := false;
  FPhase := mopPreLoadTrees;
  FImportsThisPhase := 0;
  IRecImport := FImportRecs.GetAnItem;
  while Assigned(IRecImport) do
  begin
    UserImportRec := IRecImport.Item as TUserImportRec;
    result := UserImportRec.Start(FImportsThisPhase) or result;
    FImportRecs.GetAnotherItem(IRecImport);
  end;
  if not result then
    result := StartBailCalcs;
end;

function TImportMultiOp.Start: boolean;
var
  LoadingUserList: boolean;
begin
  result := false;
  if not inherited then
    exit;
  // Need entire user list because we will need to check dependent profiles.
  FOK := true;
  FPhase := mopPreLoadList;
  FImportsThisPhase := 0;
  with FParentLoader.FDataStore do
  begin
    LoadingUserList := (UserListHintState < TUserListHintState.ulhLoaded)
      and ReadUserListToMem(self, nil);
  end;
  if LoadingUserList then
  begin
    result := true;
    Inc(FImportsThisPhase);
  end
  else
    result := StartPreLoadTrees;
end;

function TImportMultiOp.InitForRefreshUsers(Since: TDateTime; UP: TKUserProfile): boolean;
var
  IRecImport: TItemRec;
  UserImportRec: TUserImportRec;
begin
  FSince := Since;
  FType := motImportOrIncrease;
  result := Assigned(UP) and (UP.InterestLevel > kpiFetchUserForRefs);
  if result then
  begin
    UserImportRec := TUserImportRec.Create;
    UserImportRec.FParentMultiOp := Self;
    UserImportRec.FSince := Since;
    UserImportRec.FAccumulatedTree.Assign(UP);
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
    UserImportRec.FAccumulatedTree.SetRefsRecursive(UserImportRec);
{$ENDIF}
    result := FImportRecs.AddItem(UserImportRec, IRecImport) = rvOK;
    if not result then
      UserImportRec.Free;
  end;
end;

function TImportMultiOp.InitForRefreshAll(Since: TDateTime): boolean;
var
  IRecImport: TItemRec;
  UserImportRec: TUserImportRec;
  UL: TKUserList;
  UP: TKUSerProfile;
  tmpres: boolean;
begin
  FSince := Since;
  FType := motImportOrIncrease;
  UL := FParentLoader.FDataStore.GetTemporaryTopLevelList;
  try
    result := false;
    UP := UL.AdjacentBySortVal(katFirst, ksvPresentationOrder, nil) as TKUSerProfile;
    while Assigned(UP) and (UP.InterestLevel > kpiFetchUserForRefs) do
    begin
      UserImportRec := TUserImportRec.Create;
      UserImportRec.FParentMultiOp := Self;
      UserImportRec.FSince := Since;
      UserImportRec.FAccumulatedTree.Assign(UP);
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
      UserImportRec.FAccumulatedTree.SetRefsRecursive(UserImportRec);
{$ENDIF}
      tmpres := FImportRecs.AddItem(UserImportRec, IRecImport) = rvOK;
      if not tmpres then
        UserImportRec.Free;
      result := result or tmpres;
      UP := UL.AdjacentBySortVal(katNext, ksvPresentationOrder, UP) as TKUserProfile;
    end;
  finally
    FParentLoader.FDataStore.PutTopLevelList(UL);
  end;
end;

function TImportMultiOp.InitForMinimalRefresh(UP: TKUserProfile): boolean;
var
  UserImportRec: TUserImportRec;
  IRecImport: TItemRec;
begin
  result := Assigned(UP);
  if result then
  begin
    //TODO Get GUI to reload entire userlist with new item,
    //or wait till later?
    //FIncreasedInterestOrNewScan := true;
    FType := motImportOrIncrease;
    FMinimalRefresh := true;
    UserImportRec := TUserImportRec.Create;
    UserImportRec.FParentMultiOp := Self;
    UserImportRec.FAccumulatedTree.Assign(UP);
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
    UserImportRec.FAccumulatedTree.SetRefsRecursive(UserImportRec);
{$ENDIF}
    result := FImportRecs.AddItem(UserImportRec, IRecImport) = rvOK;
    if not result then
      UserImportRec.Free;
  end;
end;


function TImportMultiOp.InitForIncreasedInterest(UP: TKUserProfile; Since: TDateTime): boolean;
var
  UserImportRec: TUserImportRec;
  IRecImport: TItemRec;
begin
  result := Assigned(UP) and (UP.InterestLevel < kpiTreeForInfo);
  if result then
  begin
    FType := motImportOrIncrease;
    FIncreasedInterestOrNewScan := true;
    UserImportRec := TUserImportRec.Create;
    UserImportRec.FParentMultiOp := Self;
    UserImportRec.FAccumulatedTree.Assign(UP);
    UserImportRec.FAccumulatedTree.InterestLevel := kpiTreeForInfo;
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
    UserImportRec.FAccumulatedTree.SetRefsRecursive(UserImportRec);
{$ENDIF}
    UserImportRec.FSince := Since;
    result := FImportRecs.AddItem(UserImportRec, IRecImport) = rvOK;
    if not result then
      UserImportRec.Free;
  end
end;

function TImportMultiOp.InitIncreaseByCritera(Crit: TUserSelectionCriteria;
  Since: TDateTime): boolean;
var
  UL: TKUserList;
  UP: TKUSerProfile;
  UserImportRec: TUserImportRec;
  IRecImport: TItemRec;
begin
  result := false;
  FType := motImportOrIncrease;
  FIncreasedInterestOrNewScan := true;
  UL := FParentLoader.FDataStore.GetTemporaryTopLevelList;
  try
    UP := UL.AdjacentBySortVal(katFirst, ksvKey, nil) as TKUSerProfile;
    while Assigned(UP) do
    begin
      if (UP.InterestLevel < kpiTreeForInfo) and UserSelectionCriteriaSatisfied(UP, Crit) then
      begin
        UserImportRec := TUserImportRec.Create;
        UserImportRec.FParentMultiOp := Self;
        UserImportRec.FAccumulatedTree.Assign(UP);
        UserImportRec.FAccumulatedTree.InterestLevel := kpiTreeForInfo;
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
        UserImportRec.FAccumulatedTree.SetRefsRecursive(UserImportRec);
{$ENDIF}
        UserImportRec.FSince := Since;
        if not(FImportRecs.AddItem(UserImportRec, IRecImport) = rvOK) then
          UserImportRec.Free
        else
          result := true;
      end;
      UP := UL.AdjacentBySortVal(katNext, ksvKey, UP) as TKUSerProfile;
    end;
  finally
    FParentLoader.FDataStore.PutTopLevelList(UL);
  end;
end;

function TImportMultiOp.InitForScanForNewUser(Username: string; SType: TKSiteType;
  Since: TDateTime): boolean;
var
  IRecImport: TItemRec;
  UL: TKUserList;
  DBProfile: TKUSerProfile;
  SearchItem: TKUSerProfile;
  UserImportRec: TUserImportRec;
begin
  FType := motImportOrIncrease;
  FIncreasedInterestOrNewScan := true;
  UL :=   FParentLoader.FDataStore.GetTemporaryTopLevelList;
  try
    // Check user is not already in database.
    SearchItem := TKUSerProfile.Create;
    try
      SearchItem.SiteUserBlock[SType].Username := Username;
      SearchItem.SiteUserBlock[SType].Valid := true;
      DBProfile := UL.SearchByUserName(SType, SearchItem);
      if Assigned(DBProfile) then
      begin
        if DBProfile.InterestLevel = kpiFetchUserForRefs then
          result := InitForIncreasedInterest(DBProfile, Since)
        else
          result := InitForRefreshUsers(Since, DBProfile);
      end
      else
      begin
        // Not seen this user profile before.
        UserImportRec := TUserImportRec.Create;
        UserImportRec.FParentMultiOp := Self;
        UserImportRec.FAccumulatedTree.SiteUserBlock[SType].Valid := true;
        UserImportRec.FAccumulatedTree.SiteUserBlock[SType].Username := Username;
        UserImportRec.FAccumulatedTree.InterestLevel := kpiTreeForInfo;
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
        UserImportRec.FAccumulatedTree.SetRefsRecursive(UserImportRec);
{$ENDIF}
        UserImportRec.FSince := Since;
        result := FImportRecs.AddItem(UserImportRec, IRecImport) = rvOK;
        if not result then
          UserImportRec.Free;
      end;
    finally
      SearchItem.Free;
    end;
  finally
    FParentLoader.FDataStore.PutTopLevelList(UL);
  end;
end;


const
  S_IMPORT_NEW_DATA = 'Import new data ';

function TImportMultiOp.GetOpDetails: string;
var
  UL: TKUserList;
  UP: TKUSerProfile;
  IRec: TItemRec;
  UIRec: TUserImportRec;
begin
  result := S_IMPORT_NEW_DATA;
  if FImportRecs.Count > 1 then
    result := result + S_FOR + IntToStr(FImportRecs.Count) + S_USERS
  else
  begin
    Assert(FImportRecs.Count > 0);
    UL := TKUserList.Create;
    try
      IRec := FImportRecs.GetAnItem;
      UIRec := IRec.Item as TUserImportRec;
      UP := TKDataObject.Clone(UIRec.FAccumulatedTree) as TKUserProfile;
      UL.Add(UP);
      result := result + UserSummaryFromUserlist(UL);
    finally
      UL.Free;
    end;
  end;
end;

//TODO - We need a GUIDList helper class to enable UI logic to also use GUIDs.
function TImportMultiOp.GetSortedKeyList: TList<TGuid>;
var
  UP: TKUSerProfile;
  IRec: TItemRec;
  UIRec: TUserImportRec;
begin
  result := TList<TGuid>.Create;
  IRec := FImportRecs.GetAnItem;
  while Assigned(IRec) do
  begin
    UIRec := IRec.Item as TUserImportRec;
    UP := UIRec.FAccumulatedTree;
    if UP.Key <> TGuid.Empty then
      result.Add(UP.Key);
    FImportRecs.GetAnotherItem(IRec);
  end;
  result.Sort;
end;

{ TBatchLoader }

//Playing a bit fast and loose with the top level list,
//but OK if all used in same call stack.
function TBatchLoader.GetUserProfileByKey(const Key: TGuid): TKUserProfile;
var
  DBList: TKUSerList;
begin
  if Key = TGuid.Empty then
    result := nil
  else
  begin
    DBList := FDataStore.GetTemporaryTopLevelList;
    try
      result := DBList.SearchByInternalKeyOnly(Key) as TKUSerProfile;
    finally
      FDataStore.PutTopLevelList(DBList);
    end;
  end;
end;

procedure TBatchLoader.GetStats(var ResultStats: TBatchLoaderStats);
var
  HeadOp, Op: TMultiOp;
  OpStats: TBatchLoaderStats;
begin
  FillChar(ResultStats, sizeof(ResultStats), 0);
  HeadOp := FMultiOpQueue.FLink.Owner as TMultiOp;
  Op := HeadOp;
  while Assigned(Op) do
  begin
    if Op = HeadOp then
    begin
      Op.GetStats(OpStats);
      SumStats(ResultStats, OpStats);
    end;
    Inc(ResultStats.MultiOps);
    Op := Op.FQueueLink.FLink.Owner as TMultiOp;
  end;
  Op := FAsyncQueryQueue.FLink.Owner as TMultiOp;
  while Assigned(Op) do
  begin
    Inc(ResultStats.MultiOps);
    Op := Op.FQueueLink.FLink.Owner as TMultiOp;
  end;
end;

function TBatchLoader.GetQueuedOpDetails: TStringList;
var
  Op: TMultiOp;
begin
  result := TStringList.Create;
  Op := FMultiOpQueue.FLink.Owner as TMultiOp;
  while Assigned(Op) do
  begin
    result.Add(Op.GetOpDetails);
    Op := Op.FQueueLink.FLink.Owner as TMultiOp;
  end;
  Op := FAsyncQueryQueue.FLink.Owner as TMultiOp;
  while Assigned(Op) do
  begin
    result.Add(Op.GetOpDetails);
    Op := Op.FQueueLink.FLink.Owner as TMultiOp;
  end;
end;

//Queries execute asynchronously from main multi-op path.
function TBatchLoader.CustomQuery(Level: TKListLevel; FilterByUsers: TKUserList; AdditionalFilter: TAdditionalFilter): boolean;
var
  QueryOp: TCustomQueryMultiOp;
begin
  QueryOp := TCustomQueryMultiOp.Create;
  QueryOp.FParentLoader := Self;
  //No need for fancy init function.
  QueryOp.FTempKeyList := FilterByUsers;
  QueryOp.FTempFilter := AdditionalFilter;
  QueryOp.FLevel := Level;
  QueryOp.FType := motCustomQuery;
  result := AddAndStartQuery(QueryOp);
  if not result then
    QueryOp.Free;
end;

function TBatchLoader.LoadUserList: boolean;
var
  LUL: TDBInitMultiOp;
begin
  LUL := TDBInitMultiOp.Create;
  LUL.FParentLoader := Self;
  result := LUL.InitForLoadUserList and AddMultiOp(LUL);
  if not result then
    LUL.Free;
end;

function TBatchLoader.LoadUserTree(const Key: TGuid): boolean;
var
  LUL: TDBInitMultiOp;
  UP: TKUserProfile;
begin
  LUL := TDBInitMultiOp.Create;
  LUL.FParentLoader := Self;
  if Key <> TGuid.Empty then
  begin
    UP := GetUserProfileByKey(Key);
    result := LUL.InitForLoadUserTree(UP)
  end
  else
    result := LUL.InitForLoadAllUserTrees;
  result := result and AddMultiOp(LUL);
  if not result then
    LUL.Free;
end;

function TBatchLoader.CanLoadUserTree(const Key: TGuid): boolean;
var
  UP: TKUSerProfile;
begin
  UP := GetUserProfileByKey(Key);
  result := Assigned(UP) and (UP.InterestLevel > kpiFetchUserForRefs);
end;

function TBatchLoader.PurgeUser(Level: TKListLevel; PurgeType: TDeleteType;
  const Key: TGuid): boolean;
var
  PUL: TDBPurgeMultiOp;
  UP: TKUserProfile;
begin
  PUL := TDBPurgeMultiOp.Create;
  PUL.FParentLoader := Self;
  if Key <> TGuid.Empty then
  begin
    UP := GetUserProfileByKey(Key);
    result := PUL.InitForPurgeUser(UP, Level, PurgeType)
  end
  else
    result := PUL.InitForPurgeAll(Level, PurgeType);
  result := result and AddMultiOp(PUL);
  if not result then
    PUL.Free;
end;

function TBatchLoader.CanPurgeUser(Level: TKListLevel; const Key: TGuid): boolean;
var
  UP: TKUserProfile;
begin
  UP := GetUserProfileByKey(Key);
  result := Assigned(UP) and ((Level = klUserList)
      or (UP.InterestLevel > kpiFetchUserForRefs));
end;

function TBatchLoader.ExpireUser(ExpireBefore: TDateTime; ExpiryType: TDBExpiryType;
  LevelSet: TKListLevelSet; const Key: TGuid): boolean;
var
  ExpOp: TDBExpireMultiOp;
  UP: TKUserProfile;
begin
  ExpOp := TDBExpireMultiOp.Create;
  ExpOp.FParentLoader := self;
  if Key <> TGuid.Empty then
  begin
    UP := GetUserProfileByKey(Key);
    result := ExpOp.InitForExpireUser(UP, ExpireBefore, ExpiryType, LevelSet);
  end
  else
    result := ExpOp.InitForExpireAll(ExpireBefore, ExpiryType, LevelSet);
  result := result and AddMultiOp(ExpOp);
  if not result then
    ExpOp.Free;
end;

function TBatchLoader.CanExpireUser(ExpireBefore: TDateTime; ExpiryType: TDBExpiryType;
  LevelSet: TKListLevelSet; const Key: TGuid): boolean;
var
  UP: TKUserProfile;
begin
  UP := GetUserProfileByKey(Key);
  result := Assigned(UP) and (UP.InterestLevel > kpiFetchUserForRefs);
end;

procedure TBatchLoader.HandleDataStoreCompletion(Sender: TObject; Seq: TOpSequence);
begin
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, ' Batch loader OpSequenceCompletion, self: '
  + IntToHex(Int64(self), 16) +
  ' Seq: ' +IntToHex(Int64(Seq), 16));
{$ENDIF}
  try
    Assert(Assigned(Seq));
    Assert(Seq is TOpSequence);
    Assert(Assigned(Seq.Ref));
    Assert(Seq.Ref is TMultiOp);
    (Seq.Ref as TMultiOp).HandleDataStoreCompletion(Seq);
  except
{$IFDEF DEBUG_SEQ_MEMORY}
    GLogLog(SV_INFO, ' Batch loader OpSequenceCompletion exception, self: '
    + IntToHex(Int64(self), 16) +
    ' Seq: ' +IntToHex(Int64(Seq), 16));
{$ENDIF}
  end;
end;

constructor TBatchLoader.Create;
begin
  inherited;
  DLItemInitList(@FMultiOpQueue);
  DLItemInitList(@FAsyncQueryQueue);
end;

destructor TBatchLoader.Destroy;
begin
  Assert(DlItemIsEmpty(@FAsyncQueryQueue));
  Assert(DlItemIsEmpty(@FMultiOpQueue));
  inherited;
end;

function TBatchLoader.Stop: boolean;
var
  Op: TMultiOp;
begin
  FStopping := true;
  result := FDataStore.Stop
    and DlItemIsEmpty(@FMultiOpQueue)
    and DlItemIsEMpty(@FAsyncQueryQueue);
  Op := FMultiOpQueue.FLink.Owner as TMultiOp;
  while Assigned(Op) do
  begin
    // Don't enquire as to exact status of ops,
    // wait for them to go down completion path.
    Op.Stop;
    Op := Op.FQueueLink.FLink.Owner as TMultiOp;
  end;
  Op := FAsyncQueryQueue.FLink.Owner as TMultiOp;
  while Assigned(Op) do
  begin
    // Don't enquire as to exact status of ops,
    // wait for them to go down completion path.
    Op.Stop;
    Op := Op.FQueueLink.FLink.Owner as TMultiOp;
  end;
end;

function TBatchLoader.ConditionalUnstop: boolean;
begin
  result := FDataStore.ConditionalUnstop;
  if result then
  begin
    if FStopping
      and DlItemIsEmpty(@FMultiOpQueue)
      and DlItemIsEmpty(@FAsyncQueryQueue) then
      FStopping := false;
    result := result and not FStopping;
  end;
end;

function TBatchLoader.GetActive:boolean;
begin
  result := not
    (DlItemIsEmpty(@FMultiOpQueue) and DlItemIsEmpty(@FAsyncQueryQueue));
end;

procedure TBatchLoader.StartNextMultiOp;
var
  HeadOp: TMultiOp;
begin
  //Async queries start individually.
  HeadOp := FMultiOpQueue.FLink.Owner as TMultiOp;
  Assert(Assigned(HeadOp));
  if not FMultiOpQueueHeadStarted then
  begin
    if not HeadOp.Start then
    begin
      HeadOp.AccumErr(false, S_MULTI_START_FAILED);
      HeadOp.SignalCompletion;
    end
    else
      FMultiOpQueueHeadStarted := true;
  end;
end;

function TBatchLoader.AddMultiOp(MultiOp: TMultiOp): boolean;
var
  ImmediateStart: boolean;
begin
  if FStopping then
  begin
    result := false;
    exit;
  end;
  result := true;
  ImmediateStart := DlItemIsEmpty(@FMultiOpQueue);
  DLListInsertTail(@FMultiOpQueue, @MultiOp.FQueueLink);
  if ImmediateStart then
    StartNextMultiOp;
end;

function TBatchLoader.AddAndStartQuery(MultiOp: TMultiOp): boolean;
begin
  result := not FStopping;
  if result then
  begin
    DLListInsertTail(@FAsyncQueryQueue, @MultiOp.FQueueLink);
    result := MultiOp.Start;
    if not result then
      DLListRemoveObj(@MultiOp.FQueueLink);
  end;
end;

procedure TBatchLoader.HandleMultiOpCompletion(MultiOp: TMultiOp);
var
  HeadOp: TMultiOp;
begin
  HeadOp := FMultiOpQueue.FLink.Owner as TMultiOp;
  if HeadOp = MultiOp then
  begin
    Assert(MultiOp.FType <> motCustomQuery);
    //Main multi-op queue.
    DLListRemoveObj(@MultiOp.FQueueLink);
    FMultiOpQueueHeadStarted := false;
    if Assigned(FOnMultiOpCompleted) then
      FOnMultiOpCompleted(Self, MultiOp);
    MultiOp.Free;
    if not DlItemIsEmpty(@FMultiOpQueue) then
      StartNextMultiOp;
  end
  else
  begin
    Assert(MultiOp.FType = motCustomQuery);
    //Async query queue.
    DLListRemoveObj(@MultiOp.FQueueLink);
    if Assigned(FOnMultiOpCompleted) then
      FOnMultiOpCompleted(Self, MultiOp);
    MultiOp.Free;
  end;
end;

function TBatchLoader.RefreshUsers(Since: TDateTime; const Key: TGuid): boolean;
// Refresh one or more users already in the DB.
var
  IMop: TImportMultiOp;
  UP: TKUserProfile;
begin
  IMop := TImportMultiOp.Create;
  IMop.FParentLoader := Self;
  if Key <> TGuid.Empty then
  begin
    UP := GetUserProfileByKey(Key);
    result := IMop.InitForRefreshUsers(Since, UP);
  end
  else
    result := IMop.InitForRefreshAll(Since);
  result := result and AddMultiOp(IMop);
  if not result then
    IMop.Free;
end;

function TBatchLoader.CanRefreshUser(const Key: TGuid): boolean;
var
  UP: TKUserProfile;
begin
  UP := GetUserProfileByKey(Key);
  if Assigned(UP) then
  begin
    result := UP.InterestLevel > kpiFetchUserForRefs;
  end
  else
    result := false;
end;

function TBatchLoader.ChangeUserInterestLevel(const Key: TGuid; NewLevel: TKProfileInterestLevel;
  Since: TDateTime): boolean;
var
  IMop: TImportMultiOp;
  DBOp: TDBPurgeMultiOp;
  UP: TKUSerProfile;
begin
  UP := GetUserProfileByKey(Key);
  result := Assigned(UP);
  if not result then
    exit;
  IMop := nil;
  DBOp := nil;
  if NewLevel = kpiTreeForInfo then
  begin
    IMop := TImportMultiOp.Create;
    IMop.FParentLoader := Self;
    result := IMop.InitForIncreasedInterest(UP, Since) and AddMultiOp(IMop);
  end
  else
  begin
    DBOp := TDBPurgeMultiOp.Create;
    DBOp.FParentLoader := Self;
    result := DBOp.InitForDecreasedInterest(UP) and AddMultiOp(DBOp);
  end;
  if not result then
  begin
    IMop.Free;
    DBOp.Free;
  end;
end;

function TBatchLoader.MinimalUserRefesh(const Key: TGuid): boolean;
var
 IMOp: TImportMultiOp;
 UP: TKUserProfile;
begin
  UP := GetUserProfileByKey(Key);
  result := Assigned(UP);
  if not result then
    exit;
  IMop := TImportMultiOp.Create;
  IMop.FParentLoader := Self;
  result := IMOp.InitForMinimalRefresh(UP) and AddMultiOp(IMOp);
  if not result then
    IMOp.Free;
end;

function TBatchLoader.CanChangeInterestLevel(const Key: TGuid; NewLevel: TKProfileInterestLevel): boolean;
var
  UP: TKUSerProfile;
begin
  UP := GetUserProfileByKey(Key);
  result := Assigned(UP);
  if NewLevel = kpiTreeForInfo then
    result := result and (UP.InterestLevel < NewLevel)
  else if NewLevel = kpiFetchUserForRefs then
    result := result and (UP.InterestLevel > NewLevel)
  else
  begin
    result := false;
    Assert(false);
  end;
end;

function TBatchLoader.ChangeUserInterestLevelByCriteria(Crit: TUserSelectionCriteria;
  NewLevel: TKProfileInterestLevel; Since: TDateTime): boolean;
var
  IMop: TImportMultiOp;
  DBOp: TDBPurgeMultiOp;
begin
  IMop := nil;
  DBOp := nil;
  if NewLevel = kpiTreeForInfo then
  begin
    IMop := TImportMultiOp.Create;
    IMop.FParentLoader := Self;
    result := IMop.InitIncreaseByCritera(Crit, Since) and AddMultiOp(IMop);
  end
  else
  begin
    DBOp := TDBPurgeMultiOp.Create;
    DBOp.FParentLoader := Self;
    result := DBOp.InitDecreaseByCriteria(Crit) and AddMultiOp(DBOp);
  end;
  if not result then
  begin
    IMop.Free;
    DBOp.Free;
  end;
end;

function TBatchLoader.ScanForNewUser(Username: string; SType: TKSiteType; Since: TDateTime)
  : boolean;
var
  IMop: TImportMultiOp;
begin
  IMop := TImportMultiOp.Create;
  IMop.FParentLoader := Self;
  result := IMop.InitForScanForNewUser(Username, SType, Since);
  result := result and AddMultiOp(IMop);
  if not result then
    IMop.Free;
end;

function TBatchLoader.LoadUserTreeByUsername(Username: string; SType: TKSiteType): boolean;
var
  UL: TKUserList;
  DBProfile: TKUSerProfile;
  SearchItem: TKUSerProfile;
  Key: TGuid;
begin
  Key := TGuid.Empty;
  result := false;
  UL := FDataStore.GetTemporaryTopLevelList;
  try
    // Find user in database.
    SearchItem := TKUSerProfile.Create;
    try
      SearchItem.SiteUserBlock[SType].Username := Username;
      SearchItem.SiteUserBlock[SType].Valid := true;
      DBProfile := UL.SearchByUserName(SType, SearchItem);
      if Assigned(DBProfile) then
      begin
        Key := DBProfile.Key;
      end;
    finally
      SearchItem.Free
    end;
  finally
    FDataStore.PutTopLevelList(UL);
  end;
  if Key <> TGuid.Empty then
    result := LoadUserTree(Key);
end;

function TBatchLoader.PurgeUserByUsername(Username: string; Level: TKListLevel; SType: TKSiteType;
  PurgeType: TDeleteType): boolean;
var
  UL: TKUserList;
  DBProfile: TKUSerProfile;
  Key: TGuid;
  SearchItem: TKUSerProfile;
begin
  Key := TGuid.Empty;
  result := false;
  UL := FDataStore.GetTemporaryTopLevelList;
  try
    // Find user in database.
    SearchItem := TKUSerProfile.Create;
    try
      SearchItem.SiteUserBlock[SType].Username := Username;
      SearchItem.SiteUserBlock[SType].Valid := true;
      DBProfile := UL.SearchByUserName(SType, SearchItem);
      if Assigned(DBProfile) then
        Key := DBProfile.Key;
    finally
      SearchItem.Free;
    end;
  finally
    FDataStore.PutTopLevelList(UL);
  end;
  if Key <> TGuid.Empty then
    result := PurgeUser(Level, PurgeType, Key);
end;

end.
