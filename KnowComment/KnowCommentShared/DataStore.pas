unit DataStore;

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
  DLList, DBGeneric, DataObjects, IndexedStore
{$IFDEF DEBUG_MEM_MIRROR}
  , Classes
{$ENDIF}
  ;

// Only need one op queue, and then change store state.

type
  TLoaderDataStore = class;

{$IFDEF USE_TRACKABLES}

  TOpSequence = class(TTrackable)
{$ELSE}
  TOpSequence = class
{$ENDIF}
  private
  protected
    FRef, FRef2: TObject;
    FSequenceLink: TDLEntry;
    FParentStore: TLoaderDataStore;
    FPersistOp: TDBPersist;
    FClosing: boolean;
    FOK: boolean;
    FMsg: string;
    procedure HandlePersistCompletion(Sender: TObject); virtual; abstract;
    // returns whether can free seq.

    // Syntactic sugar.
    function PersistClass(ListLevel: TKListLevel): TDBPersistClass;
  public
    function Stop: boolean; // returns whether idle.
    function Start(ParentStore: TLoaderDataStore): boolean; virtual; abstract;
    constructor Create;
    destructor Destroy; override;
    property ParentStore: TLoaderDataStore read FParentStore write FParentStore;
    property OK: boolean read FOK;
    property Msg: string read FMsg;
    property Ref: TObject read FRef;
    property Ref2: TObject read FRef2;
  end;

  TOpSequenceCompletionEvent = procedure(Sender: TObject; Seq: TOpSequence) of object;

  TWriteOpSequence = class(TOpSequence)
  protected
    FAllowableActions: TPersistActionSet;
  end;

  TWriteUserOpSequence = class(TWriteOpSequence)
  protected
    FUserProfile: TKUserProfile;
    FPersistedKey: TGuid;
    procedure HandlePersistCompletion(Sender: TObject); override;
  public
    function Start(ParentStore: TLoaderDataStore): boolean; override;
    destructor Destroy; override;
    property PersistedKey: TGuid read FPersistedKey;
  end;

  TWriteUserListSequence = class(TWriteOpSequence)
  protected
    FUserList: TKUserList;
    procedure HandlePersistCompletion(Sender: TObject); override;
  public
    function Start(ParentStore: TLoaderDataStore): boolean; override;
    destructor Destroy; override;
  end;

  TWriteMediaOpSequence = class(TWriteOpSequence)
  protected
    FMediaItem: TKMediaItem;
    FUserKey: TGuid;
    FPersistedKey: TGuid;
    procedure HandlePersistCompletion(Sender: TObject); override;
  public
    function Start(ParentStore: TLoaderDataStore): boolean; override;
    destructor Destroy; override;
    property PersistedKey: TGuid read FPersistedKey;
    property UserKey: TGuid read FUserKey;
  end;

  TReadUserListSequence = class(TOpSequence)
  protected
    procedure HandlePersistCompletion(Sender: TObject); override;
  public
    function Start(ParentStore: TLoaderDataStore): boolean; override;
  end;

  TReadUserTreeSequence = class(TOpSequence)
  protected
    FUserProfile: TKUserProfile;
    FMediaCursor: TKMediaItem;
    FReadKey: TGuid;
    FChangesToMergeToStore: boolean;
    procedure HandlePersistCompletion(Sender: TObject); override;
  public
    function Start(ParentStore: TLoaderDataStore): boolean; override;
    destructor Destroy; override;
    property ReadKey: TGuid read FReadKey;
  end;

  TDeleteType = (dtMemOnly, dtMemAndDb);

  TDeleteDataSequence = class(TOpSequence)
  protected
    FUserProfile: TKUserProfile;
    FLastMediaKey: TGuid;
    FDeleteLevel: TKListLevel;
    FDeleteType: TDeleteType;
    procedure HandlePersistCompletion(Sender: TObject); override;
    procedure DeleteCommentsForUser(User: TKUserProfile);
  public
    function Start(ParentStore: TLoaderDataStore): boolean; override;
    destructor Destroy; override;
  end;

  TExpireDataSequence = class(TOpSequence)
  protected
    FUserProfile: TKUserProfile;
    FLastMediaKey: TGuid; //For deleting comments.
    FCommentList: TKCommentList; //Need to re-read and re-write comments, or each media (potentially).
    FExpireBefore: TDateTime;
    FExpiryType: TDBExpiryType;
    FLevelSet: TKListLevelSet;
    FSecondMediaRead: boolean;
    procedure HandlePersistCompletion(Sender: TObject); override;
    //TODO - Prob need some helper functions.
  public
    function Start(ParentStore: TLoaderDataStore): boolean; override;
    destructor Destroy; override;
  end;

  TPruneUnusedSequence = class(TOpSequence)
  protected
    procedure HandlePersistCompletion(Sender:TObject); override;
  public
    function Start(ParentStore: TLoaderDataStore): boolean; override;
  end;

  TCustomQueryOpSequence = class(TOpSequence)
  protected
    FQueryLevel: TKListLevel;
    FQueryUsersFilter: TKUserList;
    FQueryUserCursor: TKUserProfile;
    FFilter: TAdditionalFilter;
    FResult: TKUserList;
    procedure HandlePersistCompletion(Sender: TObject); override;
  public
    function Start(ParentStore: TLoaderDataStore): boolean; override;
    destructor Destroy; override;
    property QueryUsersFilter: TKUserList read FQueryUsersFilter write FQueryUsersFilter;
    property Result: TKUserList read FResult write FResult;
  end;

  //TODO - Wonder if we can merge the UpdatingImport and ReadingFromDB
  //states - they both involve only adding data, even if initial data
  //flow is in different directions.
  //A/B/C buffering now imposes different constraints.
  TLoaderDataStoreState = (
    ldsIdle,
    ldsUpdatingImport,
    ldsReadingFromDB,
    ldsPruning);

{$IFDEF USE_TRACKABLES}
  TDataStoreAbstract = class(TTrackable)
{$ELSE}
  TDataStoreAbstract = class
{$ENDIF}
  protected
    function GetRWCopyByKey(const Key: TGuid): TKUSerProfile; virtual; abstract;
    function GetRWCopyByBlockArray(Blocks: TKUserBlockArray): TKUSerProfile; virtual; abstract;
    function NewRWCopy: TKUserProfile; virtual; abstract;
    procedure PutRWCopy(Profile: TKUSerProfile); virtual; abstract;
    procedure RWCopyKeyChanging(Profile: TKUserProfile); virtual; abstract;
    procedure RWCopyKeyChanged(Profile: TKUserProfile); virtual; abstract;
    procedure Commit; virtual; abstract;
    //TODO - Will we ever need to do a rollback?
  public
    //Get the top level presentation list.
    //Only to allow you to see far enough to call later methods
    //to geld hold of copies of data objects.
    function GetTemporaryTopLevelList: TKUserList; virtual; abstract;
    procedure PutTopLevelList(List: TKUserList); virtual; abstract;
    function GetDurableReadOnlyTreeByKey(const Key: TGuid): TKUserProfile; virtual; abstract;
    function GetDurableReadOnlyTreeByBlockArray(Blocks: TKUserBlockArray): TKUserProfile; virtual; abstract;
    procedure PutDurable(Profile: TKUserProfile); virtual; abstract;
  end;

  TUserListHintState = (ulhNone, ulhLoading, ulhLoaded);

  TDataObHintBit = (obhTreeUpToDate);
  TDataObHintBits = set of TDataObHintBit;
//This object sits off the "Ref" of TKKeyedObject, help with ref counting etc.
{$IFDEF USE_TRACKABLES}
  TDataObRefCounts = class (TTrackable)
{$ELSE}
  TDataObRefCounts = class
{$ENDIF}
  private
    //Read/write copy information.
    //PresentationCopy / oldcopy information.
    FLinkedPresentationCopy: TKUserProfile;
    FRORefCount: integer;
    FRWRefCount: integer;
    FDeletePending: boolean;
    //Tree state hint bits for use by client code.
    FHintBits: TDataObHintBits;
  public
    property DeletePending: boolean read FDeletePending write FDeletePending;
    property HintBits: TDataObHintBits read FHintBits write FHintBits;
  end;

  TDatastoreMemMirror = class(TDatastoreAbstract)
  private
    FPresentationList, FChangeList, FOldList: TKUserList;
    FPresRefCnt: integer;
    FRWCopiesCheckedOut: integer;
{$IFDEF DEBUG_MEM_MIRROR}
    FRWCopiesCreated: integer;
    FRWCopiesDeleted: integer;
    FRWCopiesMovedToPresentation: integer;
    FPresentationCopiesDeleted: integer;
    FPresentationCopiesMovedToOld: integer;
    FOldCopiesDeleted: integer;
{$ENDIF}
  protected
    //Small note - when other software "gets" the RWCopy, it needs to be removed from
    //the change list, and then re-added when it's not being modified.
    //Calls to Get and Put RWCopies should be done ephemerally (in one call stack).
    function SearchByBlockArray(List: TKUserList; const Blocks: TKUserBlockArray): TKUSerProfile;
    function GetRWCopyCommon(const Key: TGuid; PBlocks: PKUserBlockArray): TKUSerProfile;
    function GetDurableCopyCommon(const Key: TGuid; PBlocks: PKUserBlockArray): TKUSerProfile;

    function GetRWCopyByKey(const Key: TGuid): TKUSerProfile; override;
    function GetRWCopyByBlockArray(Blocks: TKUserBlockArray): TKUSerProfile; override;
    function NewRWCopy: TKUserProfile; override;
    procedure PutRWCopy(Profile: TKUSerProfile); override;
    procedure RWCopyKeyChanging(Profile: TKUserProfile); override;
    procedure RWCopyKeyChanged(Profile: TKUserProfile); override;
    procedure Commit; override;
  public
    constructor Create;
    destructor Destroy; override;
    //Unlike R/W copies, ROCopies can live in their prospective lists both whilst
    //being accessed and when dormant.
    function GetTemporaryTopLevelList: TKUserList; override;
    procedure PutTopLevelList(List: TKUserList); override;
    function GetDurableReadOnlyTreeByKey(const Key: TGuid): TKUserProfile; override;
    function GetDurableReadOnlyTreeByBlockArray(Blocks: TKUserBlockArray): TKUserProfile; override;
    procedure PutDurable(Profile: TKUserProfile); override;
  end;

  TLoaderDataStore = class(TDatastoreAbstract)
  private
    FDB: TGenericDB;
    FOnOpCompletion: TOpSequenceCompletionEvent;
    FDataMirror: TDatastoreMemMirror;

    FInProgressOps: TDLEntry;
    FInProgressOpsCount: integer;
    FClosing: boolean;
    FStoreState: TLoaderDataStoreState;
    FUserListHintState: TUserListHintState;
  protected
    procedure DoSequenceCompletion(Seq: TOpSequence);
    procedure HandlePersistCompletion(Sender: TObject);

    procedure AddSeq(Seq: TOpSequence); // And and remove sequence objects from store.
    procedure RemoveSeq(Seq: TOpSequence);

    procedure SetStoreState(NewState: TLoaderDataStoreState);
    property StoreState: TLoaderDataStoreState read FStoreState write SetStoreState;
    function CheckStartOp(OpResultState: TLoaderDataStoreState): boolean;

    function GetRWCopyByKey(const Key: TGuid): TKUSerProfile; override;
    function GetRWCopyByBlockArray(Blocks: TKUserBlockArray): TKUSerProfile; override;
    function NewRWCopy: TKUserProfile; override;
    procedure PutRWCopy(Profile: TKUSerProfile); override;
    procedure RWCopyKeyChanging(Profile: TKUserProfile); override;
    procedure RWCopyKeyChanged(Profile: TKUserProfile); override;
    procedure Commit; override;
  public
    constructor Create;
    destructor Destroy; override;
    function Stop: boolean; // returns whether idle (stopped).
    function ConditionalUnstop: boolean; //returns whether unstopped.

    //Invoke operations that change both in-memory state and the underlying database.
    //For use by batch loader.
    function ReadUserListToMem(Ref, Ref2: TObject): boolean; // Breath first loaded users only op.
    //Write user list mainly for DB import/export. Expects an empty datastore.
    function WriteUserList(UserList: TKUserList; Ref, Ref2: TObject): boolean;
    function UpdateImportedUser(User: TKUserProfile; Ref, Ref2: TObject; AllowableActions: TPersistActionSet = [patUpdate, patAdd])
      : boolean; // User and media list.
    function UpdateImportedMedia(Media: TKMediaItem; Ref, Ref2: TObject; AllowableActions: TPersistActionSet = [patUpdate, patAdd])
      : boolean; // Media and comment list.
    function ReadUserTreeToMem(User: TKUserProfile; Ref, Ref2: TObject): boolean;
    // Depth first load everything for user op.
    function DeleteUserData(User: TKUserProfile; DeleteLevel: TKListLevel; DeleteType: TDeleteType;
      Ref, Ref2: TObject): boolean;
    // Delete, from specified list level down, either
    // in mem, or in mem and DB.

    //Expire is always both in-mem and in-DB.
    //Removes items older than ...
    function ExpireUserData(User: TKUserProfile; ExpireBefore: TDateTime;
      ExpiryType: TDBExpiryType; LevelSet: TKListLevelSet; Ref, Ref2: TObject): boolean;

    //Prune removes user profiles (& potentially other stuff),
    //that is both unreferenced, and holds no useful state.
    function PruneUnusedData(Ref, Ref2: TObject): boolean;

    //Custom query runs pretty much independently - does not change anything in datastore,
    //and accumulates its own data.
    //We have code here because there's a sequence of ops, and a merge step.
    function CustomQuery(Level: TKListLevel; FilterByUsers: TKUserList; AdditionalFilter: TAdditionalFilter; Ref, Ref2: TObject): boolean;

    property OnOpCompletion: TOpSequenceCompletionEvent read FOnOpCompletion write FOnOpCompletion;
    property DB: TGenericDB read FDB write FDB;

    //Bit of a hack to allow refresh commands not to repeatedly re-read the user list.
    property UserListHintState:TUserListHintState read FUserListHintState;

    //Access functions for use by third party code.
    function GetTemporaryTopLevelList: TKUserList; override;
    procedure PutTopLevelList(List: TKUserList); override;
    function GetDurableReadOnlyTreeByKey(const Key: TGuid): TKUserProfile; override;
    function GetDurableReadOnlyTreeByBlockArray(Blocks: TKUserBlockArray): TKUserProfile; override;
    procedure PutDurable(Profile: TKUserProfile); override;

{$IFDEF DEBUG_MEM_MIRROR}
    function GetDBGInfo: TStringList;
{$ENDIF}    
  end;

implementation

uses
  SysUtils, GlobalLog, DBPersist;

const
  S_INTERNAL_ERROR = 'Internal error.';
  S_ABORTED = 'Operation start failed. Internal error if application not closing.';
  S_COMMENT_OWNERS_NOT_IN_DB = 'Cannot persist a comment list to the database' +
    ' until all owning user profiles for those comments are also in the database.';
  S_DS_MIRROR_CHECKED_OUT_RW_COPIES =
    'Datastore has items being actively modified at destroy time';
  S_DS_MIRROR_EXPECTED_EMPTY_LIST =
    'Datastore has items in change list (ops in progress), or old list (gui ' +
    'references) still open at destroy time';
  S_DS_MIRROR_EPHEM_LIST_REF_NONZERO =
    'Datastore has nonzero references to ephemeral list at destroy time';

  { TOpSequence }

constructor TOpSequence.Create;
begin
  inherited;
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'TOpSequenceCreate: ' +IntToHex(Int64(self), 16));
{$ENDIF}
  DLItemInitObj(self, @FSequenceLink);
end;

destructor TOpSequence.Destroy;
begin
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'TOpSequenceDestroy: ' +IntToHex(Int64(self), 16));
{$ENDIF}
  if not DlItemIsEmpty(@FSequenceLink) then
    FParentStore.RemoveSeq(self);
  FPersistOp.Free;
  inherited;
end;

function TOpSequence.Stop: boolean;
begin
  FClosing := true;
  if Assigned(FPersistOp) then
    result := FPersistOp.Stop
  else
    result := true;
end;

function TOpSequence.PersistClass(ListLevel: TKListLevel): TDBPersistClass;
begin
  result := FParentStore.FDB.PersisterClass(ListLevel);
end;

{ TWriteUserOpSequence }

procedure TWriteUserOpSequence.HandlePersistCompletion(Sender: TObject);
var
  Params: TDBCallParams;
  Res: TDBCallResult;
  LocalOK: boolean;
  SkipListUpdate: boolean;

  procedure MergeToStoreDone;
  var
    RWCopy: TKUserProfile;
  begin
    //And we're always adding data here, not deleting.
    RWCopy := FParentStore.GetRWCopyByKey(FUserProfile.Key);
    if not Assigned(RWCopy) then
      RWCopy := FParentStore.NewRWCopy;
    try
      Assert(Assigned(RWCopy));
      FParentStore.RWCopyKeyChanging(RWCopy);
      try
        RWCopy.MergeWithNewer(FUserProfile);
      finally
        FParentStore.RWCopyKeyChanged(RWCopy);
      end;
    finally
      FreeAndNil(FUserProfile);
      FParentStore.PutRWCopy(RWCopy);
    end;
    FOK := Res.OK;
    FMsg := Res.Msg;
    FPersistOp.Free;
    FPersistOp := nil;
  end;

begin
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Write user op sequence, handle persist completion');
{$ENDIF}
  FOK := (FPersistOp as TDBMPersist).RetrieveResult(Res);
  LocalOK := (FPersistOp as TDBMPersist).RetrieveParams(Params);
  FOK := FOK and LocalOK;
  try
    if not FOK then
    begin
      FMsg := S_INTERNAL_ERROR;
      FPersistOp.Free;
      FPersistOp := nil;
    end
    else
    begin
      if FPersistOp is PersistClass(klUserList) then
      begin
        Assert(Params.CallType = klUserList);
        if Res.OK then
        begin
          if (Params.CallSubType = dbChangeSingle) then
          begin
{$IFDEF DEBUG_SEQUENCING}
            GLogLog(SV_INFO, 'Datastore: Write user op, Change single user OK, get single ' +
              FUserProfile.Key);
{$ENDIF}
            // We got a change single call that worked.
            // Now need to read back the user profile we persisted to get the key.
            FOK := (FPersistOp as TItemPersister)
              .GetSingleItemB(BlocksFromKeyedObject(FUserProfile));
            if not FOK then
            begin
              FMsg := S_ABORTED;
              FPersistOp.Free;
              FPersistOp := nil;
            end;
          end
          else if Params.CallSubType = dbGetSingle then
          begin
{$IFDEF DEBUG_SEQUENCING}
            GLogLog(SV_INFO, 'Datastore: Write user op, Change single user OK.');
            GLogLog(SV_INFO, 'Datastore: Write user op, FUserProfileKey: ' + FUserProfile.Key);
            GLogLog(SV_INFO, 'Datastore: Write user op, MergeWithNewer Key: ' + Res.Item.Key);
{$ENDIF}
            FUserProfile.MergeWithNewer(Res.Item);
            if FPersistedKey = TGuid.Empty then
              FPersistedKey := FUserProfile.Key
            else
              Assert(FPersistedKey = FUserProfile.Key);

            // Small optimisation.
            SkipListUpdate := (FUserProfile.InterestLevel = kpiFetchUserForRefs) and
              ((Res.Item as TKUserProfile).InterestLevel = kpiFetchUserForRefs) and
              (FUserProfile.Media.Count = 0) and ((Res.Item as TKUserProfile).Media.Count = 0);
            if SkipListUpdate then
            begin
{$IFDEF DEBUG_SEQUENCING}
              GLogLog(SV_INFO, 'Datastore: Write user op, optimised, merge to store ' +
                FUserProfile.Key);
{$ENDIF}
              MergeToStoreDone;
            end
            // Merge the new key information in from the Get call.
            // No need to remove and add to tree at this point,
            // because not in tree!
            else
            begin
{$IFDEF DEBUG_SEQUENCING}
              GLogLog(SV_INFO, 'Datastore: WriteUserOp: Writing media list.');
{$ENDIF}
              // Now do the media.
              FPersistOp.Free;
              FPersistOp := FParentStore.FDB.CreateItemPersister(klMediaList);
              FPersistOp.OnRequestCompleted := HandlePersistCompletion;

              FOK := (FPersistOp as TItemPersister).ChangeItemList(FUserProfile.Key,
                FUserProfile.Media, FAllowableActions);
              if not FOK then
              begin
                FMsg := S_ABORTED;
                FPersistOp.Free;
                FPersistOp := nil;
              end;
            end;
          end
          else
            Assert(false);
        end
        else
        begin
          GLogLog(SV_WARN, 'Datastore: Write user op, Change single user FAILED ' +
            GuidToString(FUserProfile.Key));
          FOK := Res.OK;
          FMsg := Res.Msg;
          FPersistOp.Free;
          FPersistOp := nil;
        end;
      end
      else if FPersistOp is PersistClass(klMediaList) then
      begin
        Assert(Params.CallType = klMediaList);
        // if OK, then writethru local state to cached copy.
        if Res.OK then
        begin
          if Params.CallSubType = TDBCallSubType.dbChangeList then
          begin
{$IFDEF DEBUG_SEQUENCING}
            GLogLog(SV_INFO, 'Datastore: Write user op, Change media list OK, try readback.');
{$ENDIF}
            // Okay, now need to read back the entire list to get the GUID key
            // information for list items, and merge the two lists.
            FOK := (FPersistOp as TItemPersister).GetItemListForParent(FUserProfile.Key);
            if not FOK then
            begin
              FMsg := S_ABORTED;
              FPersistOp.Free;
              FPersistOp := nil;
            end;
          end
          else if Params.CallSubType = TDBCallSubType.dbGetList then
          begin
{$IFDEF DEBUG_SEQUENCING}
            GLogLog(SV_INFO, 'Datastore: Write user op, Readback media list OK, merge to store.');
{$ENDIF}
            // Merge the read back list in to our user item.
            FUserProfile.Media.MergeWithNewer(Res.List);
            MergeToStoreDone;
          end
          else
            Assert(false);
        end
        else
        begin
          GLogLog(SV_WARN, 'Datastore: WriteUserOp: Writing media list FAILED, UserKey ' +
            GuidToString(FUserProfile.Key));
          FOK := Res.OK;
          FMsg := Res.Msg;
          FPersistOp.Free;
          FPersistOp := nil;
        end;
      end;
    end;
  finally
    Params.Free;
    FreeAndNil(Res);
  end;
  if not Assigned(FPersistOp) then
    FParentStore.HandlePersistCompletion(self);
end;

function TWriteUserOpSequence.Start(ParentStore: TLoaderDataStore): boolean;
begin
  FParentStore := ParentStore;
  FPersistOp := FParentStore.FDB.CreateItemPersister(klUserList);
  FPersistOp.OnRequestCompleted := HandlePersistCompletion;
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Datastore: Write user op, Started change single ' + FUserProfile.Key);
{$ENDIF}
  if Assigned(FUserProfile) and (FUserProfile.Key <> TGuid.Empty) then
    FPersistedKey := FUserProfile.Key;
  result := (FPersistOp as TItemPersister).ChangeSingleItem(TGuid.Empty, FUserProfile, FAllowableActions);
  if result then
    ParentStore.AddSeq(self);
end;

destructor TWriteUserOpSequence.Destroy;
begin
  FUserProfile.Free;
  inherited;
end;

{ TWriteMediaOpSequence }

procedure TWriteMediaOpSequence.HandlePersistCompletion(Sender: TObject);
var
  LocalOK: boolean;
  Params: TDBCallParams;
  Res: TDBCallResult;
  CommentItem: TKCommentItem;
  RWParentProfile: TKUSerProfile;
  CommentOwnerProfile: TKUSerProfile;
  CommentOwnerProfileDurable: boolean;
  TmpMediaList: TKMediaList;
  FoundCommentOwnerKeys: boolean;

begin
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Write media op sequence, handle persist completion');
{$ENDIF}
  FOK := (FPersistOp as TDBMPersist).RetrieveResult(Res);
  LocalOK := (FPersistOp as TDBMPersist).RetrieveParams(Params);
  FOK := FOK and LocalOK;
  try
    if not FOK then
    begin
      FMsg := S_INTERNAL_ERROR;
      FPersistOp.Free;
      FPersistOp := nil;
    end
    else
    begin
      //ParentUserProfile := FParentStore.FData.SearchByInternalKeyOnly(FUserKey) as TKUserProfile;
      if FPersistOp is PersistClass(klMediaList) then
      begin
        Assert(Params.CallType = klMediaList);
        if Res.OK then
        begin
          if (Params.CallSubType = dbChangeSingle) then
          begin
{$IFDEF DEBUG_SEQUENCING}
            GLogLog(SV_INFO, 'Datastore: Write media op, Change single media OK, get single ' +
              FMediaItem.Key);
{$ENDIF}
            // We got a change single call that worked.
            // Now need to read back the media item we persisted to get the key.
            FOK := (FPersistOp as TItemPersister).GetSingleItemB(BlocksFromKeyedObject(FMediaItem));
            if not FOK then
            begin
              FMsg := S_ABORTED;
              FPersistOp.Free;
              FPersistOp := nil;
            end;
          end
          else if Params.CallSubType = dbGetSingle then
          begin
{$IFDEF DEBUG_SEQUENCING}
            GLogLog(SV_INFO, 'Datastore: Write media op, Change single mediaOK.');
            GLogLog(SV_INFO, 'Datastore: Write media op, FMediaItemKey: ' + FMediaItem.Key);
            GLogLog(SV_INFO, 'Datastore: Write media op, MergeWithNewer Key: ' + Res.Item.Key);
{$ENDIF}
            // Merge the new key information in from the Get call.
            FMediaItem.MergeWithNewer(Res.Item);
            FPersistOp.Free;
            FPersistOp := nil;
            if FPersistedKey = TGuid.Empty then
              FPersistedKey := FMediaItem.Key
            else
              Assert(FPersistedKey = FMediaItem.Key);

            // Now do the comments.

            // Have a bunch of comment items in the list which need
            // to have an owner GUID key set.
{$IFDEF DEBUG_SEQUENCING}
            GLogLog(SV_INFO, 'Datastore: WriteMediaOp: Fixing up comment owner keys.');
{$ENDIF}
            FoundCommentOwnerKeys := true;
            CommentItem := FMediaItem.Comments.AdjacentBySortVal(katFirst, ksvPointer, nil)
              as TKCommentItem;
            while Assigned(CommentItem) do
            begin
              //Datastore may not have had time to return to idle,
              //try both options for getting profile.
              CommentOwnerProfile := FParentStore.GetDurableReadOnlyTreeByBlockArray
                (CommentItem.SiteUserBlocks);
              CommentOwnerProfileDurable := Assigned(CommentOwnerProfile);
              if not Assigned(CommentOwnerProfile) then
                CommentOwnerProfile := FParentStore.GetRWCopyByBlockArray
                  (CommentItem.SiteUserBlocks);
              try
                if Assigned(CommentOwnerProfile) then
                begin
                  Assert(CommentOwnerProfile.Key <> TGuid.Empty);
                  CommentItem.OwnerKey := CommentOwnerProfile.Key
                end
                else
                begin
                  //TODO - If this fails, we could concievably search the r/w list,
                  //it would mean that we persisted comment owner profiles,
                  //and then this profile, without waiting for the datastore to become idle,
                  //thus writing comment owners into the durable list.
                  FoundCommentOwnerKeys := false;
                  break;
                end;
              finally
                if Assigned(CommentOwnerProfile) then
                begin
                  if CommentOwnerProfileDurable then
                    FParentStore.PutDurable(CommentOwnerProfile)
                  else
                    FParentStore.PutRWCopy(CommentOwnerProfile);
                end;
              end;
              CommentItem := FMediaItem.Comments.AdjacentBySortVal(katNext, ksvPointer,
                CommentItem) as TKCommentItem;
            end;
            if not FoundCommentOwnerKeys then
            begin
              GLogLog(SV_WARN,
                'Datastore: WriteMediaOp: FAILED. Not found all comment owner keys, Media' +
                GuidToString(FMediaItem.Key));
              FOK := false;
              FMsg := S_COMMENT_OWNERS_NOT_IN_DB;
              Assert(not Assigned(FPersistOp));
            end
            else
            begin
              FPersistOp := FParentStore.FDB.CreateItemPersister(klCommentList);
              FPersistOp.OnRequestCompleted := HandlePersistCompletion;

{$IFDEF DEBUG_SEQUENCING}
              GLogLog(SV_INFO, 'Updated media, persisted key: ' + FMediaItem.Key);
              GLogLog(SV_INFO, 'Datastore: WriteMediaOp: Writing comment list.');
{$ENDIF}
              FOK := (FPersistOp as TItemPersister).ChangeItemList(FMediaItem.Key,
                FMediaItem.Comments, FAllowableActions);
              if not FOK then
              begin
                FOK := LocalOK;
                FMsg := S_ABORTED;
                FPersistOp.Free;
                FPersistOp := nil;
              end;
            end;
          end
          else
            Assert(false);
        end
        else
        begin
          GLogLog(SV_WARN, 'Datastore: Write user op, Change single media FAILED ' +
            GuidToString(FMediaItem.Key));
          FOK := Res.OK;
          FMsg := Res.Msg;
          FPersistOp.Free;
          FPersistOp := nil;
        end;
      end
      else if FPersistOp is PersistClass(klCommentList) then
      begin
        Assert(Params.CallType = klCommentList);
        // if OK, then writethru local state to cached copy.
        if Res.OK then
        begin
          if Params.CallSubType = TDBCallSubType.dbChangeList then
          begin
{$IFDEF DEBUG_SEQUENCING}
            GLogLog(SV_INFO, 'Datastore: Write media op, Change comment list OK, try readback.');
{$ENDIF}
            // Okay, now need to read back the entire list to get the GUID key
            // information for list items, and merge the two lists.
            FOK := (FPersistOp as TItemPersister).GetItemListForParent(FMediaItem.Key);
            if not FOK then
            begin
              FMsg := S_ABORTED;
              FPersistOp.Free;
              FPersistOp := nil;
            end;
          end
          else if Params.CallSubType = TDBCallSubType.dbGetList then
          begin
{$IFDEF DEBUG_SEQUENCING}
            GLogLog(SV_INFO,
              'Datastore: Write media op, Readback comment list OK, merge to store.');
{$ENDIF}
            // Merge the read back list in to our user item.
            FMediaItem.Comments.MergeWithNewer(Res.List);
            // And now create a tmp list, to merge our updated media + comments into main store.
            TmpMediaList := TKMediaList.Create;
            RWParentProfile := FParentStore.GetRWCopyByKey(FUserKey);
            try
              Assert(Assigned(RWParentProfile));
              LocalOK := TmpMediaList.Add(FMediaItem);
              Assert(LocalOK);
              //No need for key changing list remove here.
              RWParentProfile.Media.MergeWithNewer(TmpMediaList);
            finally
              TmpMediaList.Free;
              FParentStore.PutRWCopy(RWParentProfile);
            end;
            FMediaItem := nil;
            FOK := Res.OK;
            FMsg := Res.Msg;
            FPersistOp.Free;
            FPersistOp := nil;
          end
          else
            Assert(false);
        end
        else
        begin
          GLogLog(SV_WARN, 'Datastore: WriteMediaOp: Writing comment list FAILED, MediaKey ' +
            GuidToString(FMediaItem.Key));
          FOK := Res.OK;
          FMsg := Res.Msg;
          FPersistOp.Free;
          FPersistOp := nil;
        end;
      end
      else
        Assert(false);
    end;
  finally
    Params.Free;
    FreeAndNil(Res);
  end;
  if not Assigned(FPersistOp) then
    FParentStore.HandlePersistCompletion(self);
end;

function TWriteMediaOpSequence.Start(ParentStore: TLoaderDataStore): boolean;
begin
  FParentStore := ParentStore;
  FPersistOp := FParentStore.FDB.CreateItemPersister(klMediaList);
  FPersistOp.OnRequestCompleted := HandlePersistCompletion;
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Datastore: Write media op, Started change single');
{$ENDIF}
  if Assigned(FMediaItem) and (FMediaItem.Key <> TGuid.Empty) then
    FPersistedKey := FMediaItem.Key;
  result := (FPersistOp as TItemPersister).ChangeSingleItem(FUserKey, FMediaItem,
    FAllowableActions);
  if result then
    ParentStore.AddSeq(self);
end;

destructor TWriteMediaOpSequence.Destroy;
begin
  FMediaItem.Free;
  inherited;
end;

{ TWriteUserListSequence }


procedure TWriteUserListSequence.HandlePersistCompletion(Sender: TObject);
var
  LocalOK: boolean;
  Params: TDBCallParams;
  Res: TDBCallResult;
  ResItem, RWItem: TKUserProfile;
begin
  FOK := (FPersistOp as TDBMPersist).RetrieveResult(Res);
  LocalOK := (FPersistOp as TDBMPersist).RetrieveParams(Params);
  FOK := FOK and LocalOK;
  try
    if not FOK then
    begin
      FMsg := S_INTERNAL_ERROR;
      FPersistOp.Free;
      FPersistOp := nil;
      GLogLog(SV_WARN, 'Write user list sequence: FAILED (1)');
    end
    else
    begin
      Assert(FPersistOp is PersistClass(klUserList));
      if Res.OK then
      begin
        Assert(FPersistOp is PersistClass(klUserList));
        Assert(Params.CallType = tkListLevel.klUserList);
        Assert(Params.CallSubType in [dbChangeList, dbGetList]);
        if Params.CallSubType = dbChangeList then
        begin
          FPersistOp.Free;
          FPersistOp := FParentStore.FDB.CreateItemPersister(klUserList);
          FPersistOp.OnRequestCompleted := HandlePersistCompletion;
          FOK := (FPersistOp as TItemPersister).GetItemListForParent(TGuid.Empty);
          if not FOK then
          begin
            FMsg := S_ABORTED;
            FPersistOp.Free;
            FPersistOp := nil;
          end;
        end
        else if Params.CallSubType = dbGetList then
        begin
          try
            Assert(Assigned(Res.List));
            ResItem := Res.List.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
            while Assigned(ResItem) do
            begin
              RWItem := FParentStore.GetRWCopyByKey(ResItem.Key);
              if not Assigned(RWItem) then
                RWItem := FParentStore.NewRWCopy;
              FParentStore.RWCopyKeyChanging(RWItem);
              try
                RWItem.MergeWithNewer(ResItem);
              finally
                FParentStore.RWCopyKeyChanged(RWItem);
                FParentStore.PutRWCopy(RWItem);
              end;
              ResItem := Res.List.AdjacentBySortVal(katNext, ksvPointer, ResItem) as TKUserProfile;
            end;
          finally
            FPersistOp.Free;
            FPersistOp := nil;
          end;
        end;
      end
      else
      begin
        GLogLog(SV_WARN, 'Datastore: Write user list FAILED (2)');
        // regardless, we are done.
        FOK := Res.OK;
        FMsg := Res.Msg;
      end;
    end;
  finally
    Params.Free;
    FreeAndNil(Res);
  end;
  if not Assigned(FPersistOp) then
    FParentStore.HandlePersistCompletion(self);
end;

function TWriteUserListSequence.Start(ParentStore: TLoaderDataStore): boolean;
begin
  FParentStore := ParentStore;
  FPersistOp := FParentStore.FDB.CreateItemPersister(klUserList);
  FPersistOp.OnRequestCompleted := HandlePersistCompletion;
  result := (FPersistOp as TItemPersister).ChangeItemList(TGuid.Empty, FUserList, FAllowableActions);
  if result then
    ParentStore.AddSeq(self);
end;

destructor TWriteUserListSequence.Destroy;
begin
  FUserList.Free;
  inherited;
end;

{ TReadUserListSequence }

procedure TReadUserListSequence.HandlePersistCompletion(Sender: TObject);
var
  LocalOK: boolean;
  Params: TDBCallParams;
  Res: TDBCallResult;
  ResItem: TKUserProfile;
  RWItem: TKUserProfile;
begin
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Read user list sequence: handle persist completion.');
{$ENDIF}
  FOK := (FPersistOp as TDBMPersist).RetrieveResult(Res);
  LocalOK := (FPersistOp as TDBMPersist).RetrieveParams(Params);
  FOK := FOK and LocalOK;
  try
    if not FOK then
    begin
      FMsg := S_INTERNAL_ERROR;
      FPersistOp.Free;
      FPersistOp := nil;
      GLogLog(SV_WARN, 'Read user list sequence: FAILED (1)');
    end
    else
    begin
      Assert(FPersistOp is PersistClass(klUserList));
      if Res.OK then
      begin
        Assert(Assigned(Res.List));
        ResItem := Res.List.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
        while Assigned(ResItem) do
        begin
          RWItem := FParentStore.GetRWCopyByKey(ResItem.Key);
          if not Assigned(RWItem) then
            RWItem := FParentStore.NewRWCopy;
          FParentStore.RWCopyKeyChanging(RWItem);
          try
            RWItem.MergeWithNewer(ResItem);
          finally
            FParentStore.RWCopyKeyChanged(RWItem);
            FParentStore.PutRWCopy(RWItem);
          end;
          ResItem := Res.List.AdjacentBySortVal(katNext, ksvPointer, ResItem) as TKUserProfile;
        end;
      end
      else
        GLogLog(SV_WARN, 'Datastore: Read user list FAILED (2)');
      // regardless, we are done.
      FOK := Res.OK;
      FMsg := Res.Msg;
    end;
  finally
    FPersistOp.Free;
    FPersistOp := nil;
    Params.Free;
    FreeAndNil(Res);
  end;
  if not Assigned(FPersistOp) then
    FParentStore.HandlePersistCompletion(self);
end;

function TReadUserListSequence.Start(ParentStore: TLoaderDataStore): boolean;
begin
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Read user list sequence: start.');
{$ENDIF}
  FParentStore := ParentStore;
  FPersistOp := FParentStore.FDB.CreateItemPersister(klUserList);
  FPersistOp.OnRequestCompleted := HandlePersistCompletion;
  result := (FPersistOp as TItemPersister).GetItemListForParent(TGuid.Empty);
  if result then
    ParentStore.AddSeq(self);
end;

{ TReadUserTreeSequence }

procedure TReadUserTreeSequence.HandlePersistCompletion(Sender: TObject);
var
  LocalOK: boolean;
  Params: TDBCallParams;
  Res: TDBCallResult;
  RWCopy: TKUserProfile;
  ObRef: TDataObRefCounts;
begin
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Read user tree sequence: handle persist completion.');
{$ENDIF}
  FOK := (FPersistOp as TDBMPersist).RetrieveResult(Res);
  LocalOK := (FPersistOp as TDBMPersist).RetrieveParams(Params);
  FOK := FOK and LocalOK;
  try
    if not FOK then
    begin
      FMsg := S_INTERNAL_ERROR;
      GLogLog(SV_WARN, 'Read user tree sequence: FAILED(1)');
    end
    else
    begin
      if Res.OK then
      begin
        Assert(FPersistOp is PersistClass(klUserList));
        Assert(Params.CallType = klUserList);
        Assert(Params.CallSubType = dbGetTree);
        Assert(Assigned(Res.Item));
        Assert(Res.Item is TKUserProfile);
        Assert((Res.Item as TKUserProfile).Key <> TGuid.Empty);
        FUserProfile.Assign(Res.Item);
        //Update user profile info, but don't merge trees..
        //No need to worry about hint bits, this op doesn't change them.

        //Result read is always complete and atomic.
        RWCopy := FParentStore.GetRWCopyByKey(FUserProfile.Key);
        if not Assigned(RWCopy) then
          RWCopy := FParentStore.NewRWCopy;
        //May be reading via blocks, not key...
        FParentStore.RWCopyKeyChanging(RWCopy);
        try
          //So blat original child list and substitute with newest tree from DB.
          RWCopy.Media.DeleteChildren;
          RWCopy.MergeWithNewer(Res.Item);
          ObRef := RWCopy.Ref as TDataObRefCounts;
          Assert(Assigned(ObRef));
          ObRef.HintBits := ObRef.HintBits + [obhTreeUpToDate];
        finally
          FParentStore.RWCopyKeyChanged(RWCopy);
          FParentStore.PutRWCopy(RWCopy);
        end;
      end
      else
      begin
        GLogLog(SV_WARN, 'Datastore: Read user tree FAILED, ' + GuidToString(FUserProfile.Key) + ' ' +
          FPersistOp.ClassName);
        FOK := Res.OK;
        FMsg := Res.Msg;
      end;
    end;
  finally
    Params.Free;
    FreeAndNil(Res);
    FPersistOp.Free;
    FPersistOp := nil;
  end;
  FParentStore.HandlePersistCompletion(self);
end;

function TReadUserTreeSequence.Start(ParentStore: TLoaderDataStore): boolean;
begin
  FParentStore := ParentStore;
  FPersistOp := FParentStore.FDB.CreateItemPersister(klUserList);
  FPersistOp.OnRequestCompleted := HandlePersistCompletion;
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Read user tree sequence: start ' + FUserProfile.Key);
{$ENDIF}
  if FUserProfile.Key <> TGuid.Empty then
  begin
    FReadKey := FUserProfile.Key;
    result := (FPersistOp as TItemPErsister).GetItemTreeG(FUserProfile.Key);
  end
  else
    result := (FPersistOp as TItemPersister).GetItemTreeB(BlocksFromKeyedObject(FUserProfile));
  if result then
    ParentStore.AddSeq(self);
end;

destructor TReadUserTreeSequence.Destroy;
begin
  FUserProfile.Free;
  inherited;
end;

{ TDeleteDataSequence }

procedure TDeleteDataSequence.DeleteCommentsForUser(User: TKUserProfile);
var
  ROUserCursor, RWUSer: TKUserProfile;
  ROMediaCursor, RWMedia: TKMediaItem;
  ROComment, RWComment: TKCommentItem;
  EphemUserList: TKUserList;
begin
  Assert(User.Key <> TGuid.Empty);
  // Have to go through all media (slow), but can search via comment
  // owner key (fast-ish).

  //Bit of cleverness here as to whether we search the read-only, or the read-write
  //tree. Once we have madea change, the innermost loop searches the RW tree.
  EphemUserList := FParentStore.GetTemporaryTopLevelList;
  try
    ROUserCursor := EphemUserList.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
    while Assigned(ROUserCursor) do
    begin
      RWUser := nil;
      try
        ROMediaCursor := ROUserCursor.Media.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKMediaItem;
        while Assigned(ROMediaCursor) do
        begin
          RWMedia := nil;
          ROComment := ROMediaCursor.Comments.SearchByCommentOwner(User);
          while Assigned(ROComment) do
          begin
            if not Assigned(RWUSer) then
              RWUSer := FParentStore.GetRWCopyByKey(ROUSerCursor.Key);
            //No need to do key changing logic here (keys not changed).
            if not Assigned(RWMedia) then
            begin
              RWMedia := RWUser.Media.SearchBySortVal(ksvKey, ROMediaCursor) as TKMediaItem;
              Assert(Assigned(RWMedia));
            end;
            RWComment := RWMedia.Comments.SearchBySortVal(ksvKey, ROComment) as TKCommentItem;
            Assert(Assigned(RWComment));
            RWMedia.Comments.Remove(RWComment);
            RWComment.Free;
            //NB. Next line is not a bug. Once we have deleted one item,
            //Need to keep going through the RW list not the RO one.
            ROComment := RWMedia.Comments.SearchByCommentOwner(User)
          end;
          ROMediaCursor := ROUserCursor.Media.AdjacentBySortVal(katNext, ksvPointer, ROMediaCursor)
            as TKMediaItem;
        end;
      finally
        if Assigned(RWUSer) then
          FParentStore.PutRWCopy(RWUser);
      end;
      ROUserCursor := EphemUserList.AdjacentBySortVal(katNext, ksvPointer, ROUserCursor)
        as TKUserProfile;
    end;
  finally
    FParentStore.PutTopLevelList(EphemUserList);
  end;
end;

procedure TDeleteDataSequence.HandlePersistCompletion(Sender: TObject);
var
  CL: TKCommentList;
  DBProfile: TKUserProfile;
  DBMI: TKMediaItem;
  Params: TDBCallParams;
  Res: TDBCallResult;
  LocalOK: boolean;
  ObRef: TDataObRefCounts;
begin
  FOK := (FPersistOp as TDBMPersist).RetrieveResult(Res);
  LocalOK := (FPersistOp as TDBMPersist).RetrieveParams(Params);
  FOK := FOK and LocalOK;
  try
    if not FOK then
    begin
      FMsg := S_ABORTED;
      FPersistOp.Free;
      FPersistOp := nil;
    end
    else
    begin
      if Res.OK then
      begin
        DBProfile := FParentStore.GetRWCopyByKey(FUserProfile.Key);
        if FDeleteType = dtMemOnly then
        begin
          ObRef := DBProfile.Ref as TDataObRefCounts;
          Assert(Assigned(ObRef));
          ObRef.HintBits := ObRef.HintBits - [obhTreeUpToDate];
        end;
        //No need to do keys changing code here.
        //We only delete sub-items, and/or add delete pending flag.
        try
          Assert(Assigned(DBProfile));
          if (FPersistOp is PersistClass(klUserList)) then
          begin
            // We deleted the user profile in the DB,
            // now delete in mem.
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Delete mem comments for user ' + DBProfile.Key);
{$ENDIF}
            DeleteCommentsForUser(DBProfile);
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'DB profile marked as delete pending' + DBProfile.Key);
{$ENDIF}
            (DBProfile.Ref as TDataObRefCounts).DeletePending := true;
            FreeAndNil(FPersistOp);
          end
          else if (FPersistOp is PersistClass(klMediaList)) then
          begin
            // We have deleted media and comments for selected user profile.
            // Now delete in mem.
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Delete mem media for user ' + DBProfile.Key);
{$ENDIF}
            DBProfile.Media.DeleteChildren;
            FreeAndNil(FPersistOp);
          end
          else if (FPersistOp is PersistClass(klCommentList)) then
          begin
            // We have deleted comments for selected media item,
            // Now delete in mem.

            // Find media item.
            DBMI := DBProfile.Media.SearchByInternalKeyOnly(FLastMediaKey) as TKMediaItem;
            Assert(Assigned(DBMI));
            // Mem delete comments.
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Delete mem comments for media ' + DBMI.Key);
{$ENDIF}
            DBMI.Comments.DeleteChildren;
            // IRec still points to Media item, find next.
            DBMI := DBProfile.Media.AdjacentBySortVal(katNext, ksvKey, DBMI) as TKMediaItem;
            if Assigned(DBMI) then
            begin
              FLastMediaKey := DBMI.Key;
              if FDeleteType = dtMemOnly then
                FOK := (FPersistOp as TItemPersister).DoNop
              else
              begin
                CL := TKCommentList.Create;
                try
                  FOK := (FPersistOp as TItemPersister).ChangeItemList(FLastMediaKey, CL,
                    [patDelete]);
                  if not FOK then
                  begin
                    FMsg := S_ABORTED;
                    FPersistOp.Free;
                    FPersistOp := nil;
                  end;
                finally
                  CL.Free;
                end;
              end;
            end
            else
            begin
              FPersistOp.Free;
              FPersistOp := nil;
            end;
          end
          else
            Assert(false);
        finally
          FParentStore.PutRWCopy(DBProfile);
        end;
      end
      else
      begin
        GLogLog(SV_WARN, 'Datastore: Delete data sequence FAILED, ' + GuidToString(FUserProfile.Key) + ' ' +
          FPersistOp.ClassName);
        FOK := Res.OK;
        FMsg := Res.Msg;
        FPersistOp.Free;
        FPersistOp := nil;
      end;
    end;
  finally
    FreeAndNil(Res);
    Params.Free;
  end;
  if not Assigned(FPersistOp) then
    FParentStore.HandlePersistCompletion(self);
end;

function TDeleteDataSequence.Start(ParentStore: TLoaderDataStore): boolean;
var
  UL: TKUserList;
  ML: TKMediaList;
  CL: TKCommentList;
  DBProfile: TKUserProfile;
  MediaItem: TKMediaItem;
begin
  FParentStore := ParentStore;
  // klUserList = Delete user and all below.
  // Just one ChangeSingleUser call will suffice.
  // klMediaList = Delete media and all below.
  // Just one ChangeMediaListForUser call will suffice.
  // klCommentList = Delete comments on media.
  // Multiple ChangeCommentListForMedia calls required.
  // Tricky catch is can only delete comments for media in DB.

  // Once DB op completed (which might be a nop), then do same in mem.
  FPersistOp := FParentStore.FDB.CreateItemPersister(FDeleteLevel);
  FPersistOp.OnRequestCompleted := HandlePersistCompletion;

{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Delete data sequence: start ' + FUserProfile.Key);
{$ENDIF}

  UL := FParentStore.GetTemporaryTopLevelList;
  try
    DBProfile := UL.SearchBySortVal(ksvKey, FUSerProfile) as TKUSerProfile;
    result := Assigned(DBProfile);
    if result then
    begin
      if FDeleteType = TDeleteType.dtMemOnly then
        result := (FPersistOp as TItemPersister).DoNop
      else
      begin
        case FDeleteLevel of
          klUserList:
          begin
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Delete data sequence: del user ' + FUserProfile.Key);
{$ENDIF}
            result := (FPersistOp as TItemPersister).ChangeSingleItem(TGuid.Empty, FUserProfile, [patDelete]);
          end;
          klMediaList:
            begin
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Delete data sequence: make media list empty for user ' + FUserProfile.Key);
{$ENDIF}
              ML := TKMediaList.Create;
              try
                result := (FPersistOp as TItemPersister).ChangeItemList(FUserProfile.Key, ML,
                  [patDelete]);
              finally
                ML.Free;
              end;
            end;
          klCommentList:
            begin
              // IRec should point to DB user.
              MediaItem := DBProfile.Media.AdjacentBySortVal(katFirst, ksvKey, nil) as TKMediaItem;
              result := Assigned(MediaItem);
              if result then
              begin
                FLastMediaKey := MediaItem.Key;
                CL := TKCommentList.Create;
                try
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Delete data sequence: make comment list empty for media ' + FLastMediaKey);
{$ENDIF}
                  result := (FPersistOp as TItemPersister).ChangeItemList(FLastMediaKey, CL,
                    [patDelete]);
                finally
                  CL.Free;
                end;
              end;
            end;
        end;
      end;
    end;
    if result then
      ParentStore.AddSeq(self);
  finally
    FParentStore.PutTopLevelList(UL);
  end;
end;

destructor TDeleteDataSequence.Destroy;
begin
  FUserProfile.Free;
  inherited;
end;

{ TExpireDataSequence }

  {
    A better sequence of ops.

    Sequence of operations:
    1. Read media list.
    2. Prune DB profile media not in read list.
    3. Merge in any new media.
    4. Go through each media.
       a) Issue expire command (if applicable).
       b) Re-read comments.
          Delete local list and re-load from read comments.

    5. Issue media expire command (if applicable).
       a) Re-read media list.
       b) Prune DB profile media not in list.
  }

procedure TExpireDataSequence.HandlePersistCompletion(Sender: TObject);

  function FinalListPruneAndRead(Profile: TKUserProfile): boolean;
  begin
    if not (FPersistOp is PersistClass(klMediaList)) then
    begin
      FPersistOp.Free;
      FPersistOp := FParentStore.FDB.CreateItemPersister(klMediaList);
      FPersistOp.OnRequestCompleted := HandlePersistCompletion;
    end;
    Assert(FLastMediaKey = TGuid.Empty);
    if klMediaList in FLevelSet then
      result := (FPersistOp as TItemPersister).ExpireItemList(Profile.Key, FExpireBefore, FExpiryType)
    else
      result := (FPersistOp as TItemPersister).GetItemListForParent(Profile.Key);
  end;

  function IncrementMediaStartOp(Profile: TKUserProfile; Adj: TKAdjacencyType): boolean;
  var
    MI: TKMediaItem;
  begin
    case Adj of
      katFirst: MI := Profile.Media.AdjacentBySortVal(katFirst, ksvKey, nil) as TKMediaItem;
      katNext:
      begin
        MI := Profile.Media.SearchByInternalKeyOnly(FLastMediaKey) as TKMediaItem;
        Assert(Assigned(MI));
        MI := Profile.Media.AdjacentBySortVal(katNext, ksvKey, MI) as TKMediaItem;
      end
    else
      result := false;
      Assert(false);
      exit;
    end;
    if not Assigned(MI) then
    begin
      result := false;
      FLastMediaKey := TGuid.Empty;
      exit;
    end;
    FLastMediaKey := MI.Key;
    if not (FPersistOp is PersistClass(klCommentList)) then
    begin
      FPersistOp.Free;
      FPersistOp := FParentStore.FDB.CreateItemPersister(klCommentList);
      FPersistOp.OnRequestCompleted := HandlePersistCompletion;
    end;
    if klCommentList in FLevelSet then
    begin
      //Issue expiry for comments
      result := (FPersistOp as TItemPersister).ExpireItemList(FLastMediaKey, FExpireBefore, FExpiryType);
    end
    else
    begin
      //Re-read comment list
      result := (FPersistOp as TItemPersister).GetItemListForParent(FLastMediaKey);
    end;
  end;

var
  LocalOK: boolean;
  Params: TDBCallParams;
  Res: TDBCallResult;
  DBProfile: TKUserProfile;
  Media: TKMediaItem;
  Comment: TKCommentItem;
  Tmp: TObject;
begin
  //Read initial Media list,
  //Then issue expire commands for comments.
  //Then re-read comment lists.
  //Then issue expire commands for media.
  //Then re-read media lists and delete items from memory not re-read.
  FOK := (FPersistOp as TDBMPersist).RetrieveResult(Res);
  LocalOK := (FPersistOp as TDBMPersist).RetrieveParams(Params);
  FOK := FOK and LocalOK;
  try
    if not FOK then
    begin
      FMsg := S_ABORTED;
      FPersistOp.Free;
      FPersistOp := nil;
    end
    else
    begin
      if not Res.OK then
      begin
        //Fingers crossed, underlying data not changed.
        FOK := Res.OK;
        FMsg := Res.Msg;
        FPersistOp.Free;
        FPersistOp := nil
      end
      else
      begin
        DBProfile := FParentStore.GetRWCopyByKey(FUserProfile.Key);
        try
          if FPersistOp is PersistClass(klMediaList) then
          begin
            if Params.CallSubType = dbGetList then
            begin
              //Prune and merge so DB copy up to date.
              Media := DBProfile.Media.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKMediaItem;
              while Assigned(Media) do
              begin
                if not Assigned(Res.List.SearchByInternalKeyOnly(Media.Key)) then
                  Tmp := Media
                else
                  Tmp := nil;
                Media := DBProfile.Media.AdjacentBySortVal(katNext, ksvPointer, Media) as TKMediaItem;
                if Assigned(Tmp) then
                begin
                  DBProfile.Media.Remove(Tmp as TKKeyedObject);
                  Tmp.Free;
                end;
              end;
              //Just prune, and don't change tree up to date flags on user profile.
              if not FSecondMediaRead then
              begin
                FSecondMediaRead := true;
                if not IncrementMediaStartOp(DBProfile, katFirst) then
                begin
                  if not FinalListPruneAndRead(DBProfile) then
                  begin
                    FPersistOp.Free;
                    FPersistOp := nil;
                  end;
                end;
              end
              else
              begin
                FPersistOp.Free;
                FPersistOp := nil;
              end;
            end
            else if Params.CallSubType = dbExpireList then
            begin
              if not (FPersistOp as TItemPersister).GetItemListForParent(DBProfile.Key) then
              begin
                FPersistOp.Free;
                FPersistOp := nil;
              end;
            end
            else
            begin
              FOK := false;
              FMsg := S_INTERNAL_ERROR;
              FPersistOp.Free;
              FPersistOp := nil;
            end;
          end
          else if FPersistOp is PersistClass(klCommentList) then
          begin
            if Params.CallSubType = dbExpireList then
            begin
              //Do the get list for same media parent
              if not (FPersistOp as TItemPersister).GetItemListForParent(FLastMediaKey) then
              begin
                FPersistOp.Free;
                FPersistOp := nil;
              end;
            end
            else if Params.CallSubType = dbGetList then
            begin
              Media := DBProfile.Media.SearchByInternalKeyOnly(FLastMediaKey) as TKMediaItem;
              Assert(Assigned(Media));
              Comment := Media.Comments.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKCommentItem;
              while Assigned(Comment) do
              begin
                if not Assigned(Res.List.SearchByInternalKeyOnly(Comment.Key)) then
                  Tmp := Comment
                else
                  Tmp := nil;
                Comment := Media.Comments.AdjacentBySortVal(katNext, ksvPointer, Comment) as TKCommentItem;
                if Assigned(Tmp) then
                begin
                  Media.Comments.Remove(Tmp as TKKeyedObject);
                  Tmp.Free;
                end;
              end;
              //Just prune, and don't change tree up to date flags on user profile.
              //Increment to next media.
              if not IncrementMediaStartOp(DBProfile, katNext) then
              begin
                if not FinalListPruneAndRead(DBProfile) then
                begin
                  FPersistOp.Free;
                  FPersistOp := nil;
                end;
              end;
            end
            else
            begin
              FOK := false;
              FMsg := S_INTERNAL_ERROR;
              FPersistOp.Free;
              FPersistOp := nil;
            end;
          end
          else
          begin
            FOK := false;
            FMsg := S_INTERNAL_ERROR;
            FPersistOp.Free;
            FPersistOp := nil;
          end;
        finally
          FParentStore.PutRWCopy(DBProfile);
        end;
      end;
    end;
  finally
    FreeAndNil(Res);
    Params.Free;
  end;
  if not Assigned(FPersistOp) then
    FParentStore.HandlePersistCompletion(self);
end;

function TExpireDataSequence.Start(ParentStore: TLoaderDataStore): boolean;
begin
  FParentStore := ParentStore;
  result := false;
  if klUserList in FLevelSet then
    exit;
  if FLevelSet = [] then
    exit;
  if not Assigned(FUserProfile) then
    exit;
  if FUserProfile.Key = TGuid.Empty then
    exit;
  FPersistOp := FParentStore.FDB.CreateItemPersister(klMediaList);
  FPersistOp.OnRequestCompleted := HandlePersistCompletion;
  result := (FPersistOp as TItemPersister).GetItemListForParent(FUserProfile.Key);
  if result then
    ParentStore.AddSeq(self);
end;


destructor TExpireDataSequence.Destroy;
begin
  FUserProfile.Free;
  inherited;
end;

{ TPruneUnusedSequence }

procedure TPruneUnusedSequence.HandlePersistCompletion(Sender:TObject);
var
  LocalOK: Boolean;
  Params: TDBCallParams;
  Res: TDBCallResult;
  ResItem, RWItem, ROItem: TKUserProfile;
  ROList: TKUSerList;
begin
  FOK := (FPersistOp as TDBMPersist).RetrieveResult(Res);
  LocalOK := (FPersistOp as TDBMPersist).RetrieveParams(Params);
  FOK := FOK and LocalOK;
  try
    if not FOK then
    begin
      FMsg := S_ABORTED;
      FPersistOp.Free;
      FPersistOp := nil;
    end
    else
    begin
      if not Res.OK then
      begin
        //Fingers crossed, underlying data not changed.
        FOK := Res.OK;
        FMsg := Res.Msg;
        FPersistOp.Free;
        FPersistOp := nil
      end
      else
      begin
        Assert(FPersistOp is PersistClass(klUserList));
        if Params.CallSubType = dbPruneUnused then
        begin
          if not (FPersistOp as TItemPersister).GetItemListForParent(TGuid.Empty) then
          begin
            FOK := false;
            FPersistOp.Free;
            FPersistOp := nil;
          end;
        end
        else if Params.CallSubType = dbGetList then
        begin
          //Re-merge / update user-list using approved method...
          //Remove items no longer in underlying DB (res list).
          ROList := FParentStore.GetTemporaryTopLevelList;
          try
            ROItem := ROList.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
            while Assigned(ROItem) do
            begin
              ResItem := Res.List.SearchByInternalKeyOnly(ROItem.Key) as TKUserProfile;
              if not Assigned(ResItem) then
              begin
                //OK, RW User profile copy needs to go away, somehow / evetually.
                //If we got R/O copy, should be able to get R/W copy.
                RWItem := FParentStore.GetRWCopyByKey(ROItem.Key);
                if Assigned(RWItem) then
                begin
                  (RWItem.Ref as TDataObRefCounts).DeletePending := true;
                  FParentStore.PutRWCopy(RWItem);
                end
                else
                  Assert(false);
              end;
              ROItem := ROList.AdjacentBySortVal(katNext, ksvPointer, ROItem) as TKUserProfile;
            end;
          finally
            FParentStore.PutTopLevelList(ROList);
          end;
          //Just prune, and don't change tree up to date flags on user profiles.

          //All done.
          FPersistOp.Free;
          FPersistOp := nil;
        end;
      end;
    end;
  finally
    FreeAndNil(Res);
    Params.Free;
  end;
  if not Assigned(FPersistOp) then
    FParentStore.HandlePersistCompletion(self);
end;

function TPruneUnusedSequence.Start(ParentStore: TLoaderDataStore): boolean;
begin
  FParentStore := ParentStore;
  FPersistOp := FParentStore.FDB.CreateItemPersister(klUserList);
  FPersistOp.OnRequestCompleted := HandlePersistCompletion;
  result := (FPersistOp as TItemPersister).PruneUnused;
  if result then
    ParentStore.AddSeq(self);
end;

{ TCustomQueryOpSequence }


procedure TCustomQueryOpSequence.HandlePersistCompletion(Sender: TObject);
var
  LocalOK: boolean;
  Params: TDBCallParams;
  Res: TDBCallResult;

begin
  FOK := (FPersistOp as TDBMPersist).RetrieveResult(Res);
  LocalOK := (FPersistOp as TDBMPersist).RetrieveParams(Params);
  FOK := FOK and LocalOK;
  try
    if not FOK then
    begin
      FMsg := S_ABORTED;
      FPersistOp.Free;
      FPersistOp := nil;
    end
    else
    begin
      if Res.OK then
      begin
        Assert(Assigned(Res.List));
        Assert(Res.List is TKUserList);
        if not Assigned(FResult) then
        begin
          FResult := Res.List as TKUserList;
          Res.List := nil;
        end
        else
          FResult.MergeWithNewer(Res.List);
        if Assigned(FQueryUserCursor) then
        begin
          FQueryUsersFilter.Remove(FQueryUserCursor);
          FQueryUserCursor.Free;
          FQueryUserCursor := FQueryUsersFilter.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
        end;
        if Assigned(FQueryUserCursor) then
        begin
          Assert(FFilter = Params.Filter);
          Params.Filter := nil; //Make sure it doesn't get freed, we need to re-use it.
          FOK := (FPersistOp as TItemPersister).CustomQuery
            (FQueryUserCursor.Key, ckmCommentOwnerKey, FFilter);
          if not FOK then
          begin
            FMsg := S_ABORTED;
            FPersistOp.Free;
            FPersistOp := nil;
          end;
        end
        else
        begin
          //All done.
          FOK := Res.OK;
          FMsg := Res.Msg;
          FPersistOp.Free;
          FPersistOp := nil;
        end;
      end
      else
      begin
        if Assigned(FQueryUserCursor) then
        begin
          GLogLog(SV_WARN, 'Datastore: Custom query sequence FAILED, ' + GuidToString(FQueryUserCursor.Key) + ' ' +
            FPersistOp.ClassName);
        end
        else
          GLogLog(SV_WARN, 'Datastore: Custom query sequence FAILED, ' + FPersistOp.ClassName);
        FOK := Res.OK;
        FMsg := Res.Msg;
        FPersistOp.Free;
        FPersistOp := nil;
      end;
    end;
  finally
    FreeAndNil(Res);
    Params.Free;
  end;
  if not Assigned(FPersistOp) then
    FParentStore.HandlePersistCompletion(self);
end;

//TODO - Possibly more fancy params to custom queries...
function TCustomQueryOpSequence.Start(ParentStore: TLoaderDataStore): boolean;
begin
  FParentStore := ParentStore;
  if not (Assigned(FQueryUsersFilter) or Assigned(FFilter)) then
  begin
    result := false;
    exit;
  end;
  FPersistOp := FParentStore.FDB.CreateItemPersister(FQueryLevel);
  FPersistOp.OnRequestCompleted := HandlePersistCompletion;
  if Assigned(FQueryUsersFilter) then
  begin
    FQueryUserCursor := FQueryUsersFilter.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
    if Assigned(FQueryUserCursor) then
    begin
      //At the moment this only make sense if level is klComment.
      result := (FPersistOp as TItemPersister).CustomQuery
        (FQueryUserCursor.Key, ckmCommentOwnerKey, FFilter);
    end
    else
      result := false;
  end
  else
  begin
    result := (FPersistOp as TItemPersister).CustomQuery(TGuid.Empty, ckmNone, FFilter);
  end;
  if result then
    ParentStore.AddSeq(self);
end;

destructor TCustomQueryOpSequence.Destroy;
begin
  FQueryUsersFilter.Free;
  FFilter.Free;
  FResult.Free;
  inherited;
end;

{ TDatastoreMemMirror }

function TDatastoreMemMirror.SearchByBlockArray(List: TKUserList; const Blocks: TKUserBlockArray): TKUSerProfile;
var
  ST: TKSiteType;
  UP: TKUserProfile;
begin
  UP := TKUserProfile.Create;
  try
    for ST := Low(ST) to High(ST) do
    begin
      UP.SiteUserBlock[ST].Assign(Blocks[ST]);
      result := List.SearchByDataBlock(ST, UP) as TKUSerProfile;
      if Assigned(result) then
        break;
    end;
  finally
    UP.Free;
  end;
end;

{$IFDEF DEBUG_MEM_MIRROR}
function LogBlocks(const Blocks: TKUserBlockArray): string;
var
  ST: TKSiteType;
begin
  result := '';
    for ST := Low(ST) to High(ST) do
    begin
    if Blocks[ST].Valid then
    begin
      result :=
        SiteTypeStrings[ST] + ' UID: ' +
        Blocks[ST].UserId + ' Uname: ' +
        Blocks[ST].Username;
    end;
  end;
end;
{$ENDIF}

function TDatastoreMemMirror.GetRWCopyCommon(const Key: TGuid; PBlocks: PKUserBlockArray): TKUSerProfile;
var
  Existing: TKUSerProfile;
  ResultRef: TDataObRefCounts;
begin
  Assert((Key <> TGuid.Empty) or Assigned(PBlocks));
  Assert(not ((Key <> TGuid.Empty) and Assigned(PBlocks)));
  //Already a r/w copy?
  if Key <> TGuid.Empty then
  begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Get RWCopy from changelist by Key: ' + Key);
{$ENDIF}
    result := FChangeList.SearchByInternalKeyOnly(Key) as TKUserProfile
  end
  else
  begin
{$IFDEF DEBUG_MEM_MIRROR}
    Assert(Assigned(PBlocks));
    GLogLog(SV_INFO, 'Get RWCopy from changelist by blocks :' + LogBlocks(PBlocks^));
{$ENDIF}
    result := SearchByBlockArray(FChangeList, PBlocks^) as TKUserProfile;
  end;
  if Assigned(result) then
  begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'RWCopy found in changelist.');
{$ENDIF}
    Assert(Assigned(Result.Ref));
    ResultRef := Result.Ref as TDataObRefCounts;
    Assert(ResultRef.FRORefCount = 0);
    //Code might recursively get RW copies, ok provided single op sequence.
{$IFDEF DEBUG_MEM_MIRROR}
    if Assigned(ResultRef.FLinkedPresentationCopy) then
    begin
      GLogLog(SV_INFO, 'Has presentation copy: '
        + IntToHex(Int64(ResultRef.FLinkedPresentationCopy), 16));
    end
    else
      GLogLog(SV_INFO, 'No presentation copy');
{$ENDIF}
    //If re-getting something newly created, not necessarily a presentation copy.
    Inc(ResultRef.FRWRefCount);
    //FChangeList.Remove(result);
  end
  else
  begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, '... not found.');
{$ENDIF}
    if Key <>TGuid.Empty then
    begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Get RWCopy from presentation list by Key: ' + Key);
{$ENDIF}
      Existing := FPresentationList.SearchByInternalKeyOnly(Key) as TKUserProfile
    end
    else
    begin
{$IFDEF DEBUG_MEM_MIRROR}
    Assert(Assigned(PBlocks));
    GLogLog(SV_INFO, 'Get RWCopy from presentation list by blocks :' + LogBlocks(PBlocks^));
{$ENDIF}
      Existing := SearchByBlockArray(FPresentationList, PBlocks^) as TKUserProfile;
    end;
    if Assigned(Existing) then
    begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Found, create R/W copy from presentation');
{$ENDIF}
      //Regardless of whether R/O refcount, need to create separate R/W copy.
      result := TKUSerProfile.DeepClone(Existing) as TKUserProfile;
      result.Ref := TDataObRefCounts.Create;
      ResultRef := Result.Ref as TDataObRefCounts;
      ResultRef.FRWRefCount := 1;
      ResultRef.FLinkedPresentationCopy := Existing;
{$IFDEF DEBUG_MEM_MIRROR}
      GLogLog(SV_INFO, 'Set presentation copy: '
        + IntToHex(Int64(ResultRef.FLinkedPresentationCopy), 16));
{$ENDIF}

{$IFDEF DEBUG_MEM_MIRROR}
      Inc(FRWCopiesCreated);
{$ENDIF}
      FChangeList.Add(Result);
    end;
  end;
  if Assigned(Result) then
    Inc(FRWCopiesCheckedOut);
end;

function TDatastoreMemMirror.GetRWCopyByKey(const Key: TGuid): TKUSerProfile;
begin
  Assert(Key <> TGuid.Empty);
  result := GetRWCopyCommon(Key, nil);
end;

function TDatastoreMemMirror.GetRWCopyByBlockArray(Blocks: TKUserBlockArray): TKUSerProfile;
begin
  result := GetRWCopyCommon(TGuid.Empty, @Blocks);
end;

function TDatastoreMemMirror.NewRWCopy: TKUserProfile;
begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Brand new R/W copy (no presentation link).');
{$ENDIF}
  result := TKUserProfile.Create;
  result.Ref := TDataObRefCounts.Create;
  (Result.Ref as TDataObRefCounts).FRWRefCount := 1;
  Inc(FRWCopiesCheckedOut);
{$IFDEF DEBUG_MEM_MIRROR}
  Inc(FRWCopiesCreated);
{$ENDIF}
  FChangeList.Add(result);
end;

procedure TDatastoreMemMirror.PutRWCopy(Profile: TKUSerProfile);
var
  TmpKeyed: TKKeyedObject;
  ProfileRef: TDataObRefCounts;
begin
{$IFDEF DEBUG_MEM_MIRROR}
  GLogLog(SV_INFO, 'R/W copy back into change list, Key: '
  + Profile.Key + ' Blocks: ' +LogBlocks(Profile.SiteUserBlocks));
  if Assigned(Profile.Ref) then
  begin
    if Assigned((Profile.Ref as TDataObRefCounts).FLinkedPresentationCopy) then
    begin
      GLogLog(SV_INFO, 'Has presentation copy: '
        + IntToHex(Int64((Profile.Ref as TDataObRefCounts).FLinkedPresentationCopy), 16));
    end
    else
      GLogLog(SV_INFO, 'No presentation copy');
  end;
{$ENDIF}
  //Should be in R/W list.
  //Should have Linked presentation copy
  //Should have RW refcount > 0.
  TmpKeyed := FChangeList.SearchBySortVal(ksvPointer, Profile);
  Assert(Assigned(TmpKeyed));
  Assert(Assigned(Profile.Ref));
  ProfileRef := Profile.Ref as TDataObRefCounts;
  Assert(ProfileRef.FRWRefCount > 0 );
  Assert(ProfileRef.FRORefCount = 0);
  Dec(ProfileRef.FRWRefCount);
  Dec(FRWCopiesCheckedOut);
  Assert(FRWCopiesCheckedOut >= 0);
  //FChangeList.Add(Profile);
end;

procedure TDatastoreMemMirror.RWCopyKeyChanging(Profile: TKUserProfile);
var
  Keyed: TKUserProfile;
begin
  Keyed := FChangeList.SearchBySortVal(ksvPointer, Profile) as TKUserProfile;
  Assert(Assigned(Keyed));
  FChangeList.Remove(Profile);
end;

procedure TDatastoreMemMirror.RWCopyKeyChanged(Profile: TKUserProfile);
var
  Keyed: TKUserProfile;
  OK: boolean;
begin
  Keyed := FChangeList.SearchBySortVal(ksvPointer, Profile) as TKUSerProfile;
  Assert(not Assigned(Keyed));
  OK := FChangeList.Add(Profile);
  Assert(OK);
end;


procedure TDatastoreMemMirror.Commit;
var
 UP, NextUP, PresUP: TKUSerProfile;
 UPRef, UP2Ref: TDataObRefCounts;
 MoveFlag: boolean; //Used twice, for different reasons.
{$IFDEF DEBUG_MEM_MIRROR}
  TmpUP: TKUSerProfile;
{$ENDIF}
begin
  Assert(FRWCopiesCheckedOut = 0);
  //Go through all R/W copies, and shift to the presentation list,
  //except if the presentation list has R/O links, at which case,
  //move presentation copy from presentation list to old list.
{$IFDEF DEBUG_MEM_MIRROR}
  GLogLog(SV_INFO, 'Commit, go through change list...');
{$ENDIF}
  UP := FChangeList.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
  while Assigned(UP) do
  begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Found item, Key: ' + UP.Key
      + ' Blocks: ' + LogBlocks(UP.SiteUserBlocks));
{$ENDIF}
    PresUP := nil;

    Assert(Assigned(UP.Ref));
    UPRef := UP.Ref as TDataObRefCounts;
    //See if change profile connected to good presentation profile.
    Assert(UPRef.FRWRefCount = 0);
    Assert(UPRef.FRORefCount = 0);
    MoveFlag := false;
    if Assigned(UPRef.FLinkedPresentationCopy) then
    begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Item has linked presentation copy: ' +
      IntToHex(Int64(UPRef.FLinkedPresentationCopy), 16));
{$ENDIF}
      PresUP := FPresentationList.SearchBySortVal(ksvPointer,
        UPRef.FLinkedPresentationCopy) as TKUserProfile;
      if not Assigned(PresUP) then
      begin
{$IFDEF DEBUG_MEM_MIRROR}
        GLogLog(SV_INFO, '... which was NOT found in the presentation list.');
        TmpUP := FOldList.SearchBySortVal(ksvPointer,
          UPRef.FLinkedPresentationCopy) as TKUserProfile;
        if Assigned(TmpUP) then
          GLogLog(SV_INFO, 'but WAS found in the old list.')
        else
          GLogLog(SV_INFO, 'also not found in the old list');
{$ENDIF}
      end;
      //TODO - Assertion here, when we delete user profiles.....
      Assert(Assigned(PresUP));
      Assert(PresUP = UPRef.FLinkedPresentationCopy);
      UP2Ref := PresUP.Ref as TDataObRefCOunts;

      //UP2, move to old list if RO refcounts, else free.
      FPresentationList.Remove(PresUP);
      if Assigned(UP2Ref) then
      begin
        Assert(UP2Ref.FRWRefCount = 0);
        Assert(not Assigned(UP2Ref.FLinkedPresentationCopy));
        Assert(not UP2Ref.DeletePending);
        if UP2Ref.FRORefCount > 0 then
        begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Moving linked presentation copy to old: ' +
     IntToHex(Int64(PresUP), 16));
{$ENDIF}
          FOldList.Add(PresUP);
          MoveFlag := true;
{$IFDEF DEBUG_MEM_MIRROR}
          Inc(FPresentationCopiesMovedToOld);
{$ENDIF}
        end;
      end;
    end
    else
    begin
{$IFDEF DEBUG_MEM_MIRROR}
      GLogLog(SV_INFO, 'Item no presentation copy.');
{$ENDIF}
    end;
    if (not MoveFlag) and Assigned(PresUP) then
    begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Deleting linked presentation copy: ' +
     IntToHex(Int64(PresUP), 16));
{$ENDIF}
      PresUP.Ref.Free;
      PresUP.Ref := nil;
      PresUP.Free;
{$IFDEF DEBUG_MEM_MIRROR}
      Inc(FPresentationCopiesDeleted);
{$ENDIF}
    end;
    NextUP := FChangeList.AdjacentBySortVal(katNext, ksvPointer, UP) as TKUserProfile;
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Removing item from changelist: ' + IntToHex(Int64(UP), 16));
{$ENDIF}
    FChangeList.Remove(UP);
    MoveFlag := not UPRef.DeletePending; //Actually "to move"
    //Don't delete UP.Ref when we move it, because it has hint bits on the state
    //of the tree.
    if MoveFlag then
    begin
{$IFDEF DEBUG_MEM_MIRROR}
      Inc(FRWCopiesMovedToPresentation);
{$ENDIF}
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, '... and adding to presentation: ' + IntToHex(Int64(UP), 16));
{$ENDIF}
      Assert(UPRef = UP.Ref); //Just check it hasn't changed.
      UPRef.FLinkedPresentationCopy := nil; //We are the presentation copy.
      FPresentationList.Add(UP)
    end
    else
    begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, '... and deleting: ' + IntToHex(Int64(UP), 16));
{$ENDIF}
{$IFDEF DEBUG_MEM_MIRROR}
      Inc(FRWCopiesDeleted);
{$ENDIF}
      UP.Ref.Free;
      UP.Ref := nil;
      UP.Free;
    end;
    UP := NextUP;
  end;
{$IFDEF DEBUG_MEM_MIRROR}
  //Check presentation list had objs, with only R/O refs, if
  //refobj present should have R/O ref.
  UP := FPresentationList.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
  while Assigned(UP) do
  begin
    if Assigned(UP.Ref) then
    begin
      UPRef := UP.Ref as TDataObRefCounts;
      Assert(UPRef.FRWRefCount = 0);
      Assert(not Assigned(UPRef.FLinkedPresentationCopy));
      Assert(not UPRef.FDeletePending);
      Assert(UPRef.FRORefCount > 0);
    end;
    UP := FPresentationList.AdjacentBySortVal(katNext, ksvPointer, UP) as TKUserProfile;
  end;
{$ENDIF}
end;


function TDatastoreMemMirror.GetTemporaryTopLevelList: TKUSerList;
begin
  Assert(FPresRefCnt >= 0);
  Inc(FPresRefCnt);
  result := FPresentationList as TKUserList;
end;

procedure TDatastoreMemMirror.PutTopLevelList(List: TKUserList);
begin
  Assert(FPresRefCnt > 0);
  Dec(FPresRefCnt);
  Assert(List = FPresentationList);
end;

function TDatastoreMemMirror.GetDurableCopyCommon(const Key: TGuid; PBlocks: PKUserBlockArray): TKUSerProfile;
var
  UPRef: TDataObRefCounts;
begin
  Assert((Key <> TGuid.Empty) or Assigned(PBlocks));
  Assert(not ((Key <> TGuid.Empty) and Assigned(PBlocks)));

  //Always get durable copy from presentation list.
  if Key <> TGuid.Empty then
  begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Get Durable from presentation list by Key: ' + Key);
{$ENDIF}
    result := FPresentationList.SearchByInternalKeyOnly(Key) as TKUserProfile
  end
  else
  begin
{$IFDEF DEBUG_MEM_MIRROR}
    Assert(Assigned(PBlocks));
    GLogLog(SV_INFO, 'Get Durable from presentation list by blocks :' + LogBlocks(PBlocks^));
{$ENDIF}
    result := SearchByBlockArray(FPresentationList, PBlocks^)  as TKUserProfile;
  end;
  if Assigned(result) then
  begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, 'Durable copy found in presentation list: ' +
      IntToHex(Int64(result), 16));
{$ENDIF}
    if Assigned(Result.Ref) then
    begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, '... use existing ref.');
{$ENDIF}
      UPRef := Result.Ref as TDataObRefCounts;
      Assert(UPRef.FRWRefCount = 0);
      Assert(not Assigned(UPRef.FLinkedPresentationCopy));
      Assert(not UPRef.DeletePending);
      Inc(UPRef.FRORefCount);
    end
    else
    begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, '... create new ref.');
{$ENDIF}
      UPRef := TDataObRefCounts.Create;
      result.Ref := UPRef;
      UPRef.FRORefCount := 1;
    end;
  end
  else
  begin
{$IFDEF DEBUG_MEM_MIRROR}
    GLogLog(SV_INFO, '... not found.');
{$ENDIF}
  end;
end;

function TDatastoreMemMirror.GetDurableReadOnlyTreeByKey(const Key: TGuid): TKUserProfile;
begin
  result := GetDurableCopyCommon(Key, nil);
end;

function TDatastoreMemMirror.GetDurableReadOnlyTreeByBlockArray(Blocks: TKUserBlockArray): TKUserProfile;
begin
  result := GetDurableCopyCommon(TGuid.Empty, @Blocks);
end;

procedure TDatastoreMemMirror.PutDurable(Profile: TKUserProfile);
var
  TmpKeyed: TKKeyedObject;
  UPRef: TDataObRefCounts;
  PresentationList: boolean;
begin
{$IFDEF DEBUG_MEM_MIRROR}
  GLogLog(SV_INFO, 'Put durable, pointer: ' +
    IntToHex(Int64(Profile), 16)
    + 'Key: ' + Profile.Key + ' Blocks: ' + LogBlocks(Profile.SiteUserBlocks));
{$ENDIF}
  TmpKeyed := FPresentationList.SearchBySortVal(ksvPointer, Profile);
  PresentationList := Assigned(TmpKeyed);
  if Assigned(TmpKeyed) then
  begin
{$IFDEF DEBUG_MEM_MIRROR}
  GLogLog(SV_INFO, '... found in presentation list.');
{$ENDIF}
  end
  else
  begin
    TmpKeyed := FOldList.SearchBySortVal(ksvPointer, Profile);
    if Assigned(TmpKeyed) then
    begin
{$IFDEF DEBUG_MEM_MIRROR}
  GLogLog(SV_INFO, '... found in old list.');
{$ENDIF}
    end
    else
    begin
      Assert(false);
      exit;
    end;
  end;

  UPRef := Profile.Ref as TDataObRefCounts;
  Assert(Assigned(UPRef));
  Assert(UPRef.FRWRefCount = 0);
  Assert(not Assigned(UPRef.FLinkedPresentationCopy));
  Assert(not UPRef.DeletePending);
  Assert(UPRef.FRORefCount > 0);
  Dec(UPRef.FRORefCount);
{$IFDEF DEBUG_MEM_MIRROR}
  GLogLog(SV_INFO, 'Dec ref count, now ' + IntToStr(UPRef.FRORefCount));
{$ENDIF}
  if UPRef.FRORefCount = 0 then
  begin
    if not PresentationList then
    begin
{$IFDEF DEBUG_MEM_MIRROR}
      GLogLog(SV_INFO, 'and deleting from old list.');
{$ENDIF}
{$IFDEF DEBUG_MEM_MIRROR}
      Inc(FOldCopiesDeleted);
{$ENDIF}
      FOldList.Remove(Profile);
      Profile.Ref.Free;
      Profile.Ref := nil;
      Profile.Free;
    end;
  end;
end;

constructor TDatastoreMemMirror.Create;
begin
  inherited;
  FPresentationList := TKUserList.Create;
  FChangeList := TKUserList.Create;
  FOldList := TKUserList.Create;
end;

destructor TDatastoreMemMirror.Destroy;
var
  List: TKItemList;
  UP: TKUserProfile;
  idx: integer;
begin
  if FRWCopiesCheckedOut <> 0 then
    AppGlobalLog.Log(SV_WARN, S_DS_MIRROR_CHECKED_OUT_RW_COPIES);
  if FPresRefCnt <> 0 then
    AppGlobalLog.Log(SV_WARN, S_DS_MIRROR_EPHEM_LIST_REF_NONZERO);
  for idx := 0 to 2 do
  begin
    case idx of
      0: List := FChangeList;
      1: List := FPresentationList;
      2: List := FOldList;
    else
      List := nil;
      Assert(false);
    end;
    if (List = FChangeList) or (List = FOldList) then
    begin
      if List.Count > 0 then
        AppGlobalLog.Log(SV_WARN, S_DS_MIRROR_EXPECTED_EMPTY_LIST);
    end;
    UP := List.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
    while Assigned(UP) do
    begin
      if Assigned(UP.Ref) then
      begin
        UP.Ref.Free;
        UP.Ref := nil;
      end;
{$IFDEF DEBUG_MEM_MIRROR}
      case idx of
        0: Inc(FRWCopiesDeleted);
        1: Inc(FPresentationCopiesDeleted);
        2: Inc(FOldCopiesDeleted);
      end;
{$ENDIF}
      List.Remove(UP);
      UP.Free;
      UP := List.AdjacentBySortVal(katFirst, ksvPointer, nil) as TKUserProfile;
    end;
  end;
  FPresentationList.Free;
  FChangeList.Free;
  FOldList.Free;
  inherited;
end;

{ TLoaderDataStore }

procedure TLoaderDataStore.SetStoreState(NewState: TLoaderDataStoreState);
begin
  if NewState <> FStoreState then
  begin
    if NewState = ldsIdle then
      FDataMirror.Commit;
  end;
  FStoreState := NewState;
end;

procedure TLoaderDataStore.DoSequenceCompletion(Seq: TOpSequence);
begin
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'TLoaderDatastore do sequence completion, self: '
    +IntToHex(Int64(self), 16) + ', seq: '
    +IntToHex(Int64(seq), 16));
{$ENDIF}
  if Assigned(FOnOpCompletion) then
    FOnOpCompletion(self, Seq);
  // Can free seq here only because signalling events is last thing we do.
  Seq.Free;
end;

procedure TLoaderDataStore.HandlePersistCompletion(Sender: TObject);
var
  SenderSequence: TOpSequence;
begin
  //Generic.
  SenderSequence := (Sender as TOpSequence);
  //Specific hack.
  if SenderSequence is TReadUserListSequence then
  begin
    if SenderSequence.OK then
    begin
      if FUserListHintState < ulhLoaded then
        FUserListHintState := ulhLoaded;
    end
    else
    begin
      if FUserListHintState < ulhLoaded then
        FUserListHintState := ulhNone;
    end;
  end;
  //Generic
  RemoveSeq(SenderSequence);
  DoSequenceCompletion(SenderSequence);
end;

procedure TLoaderDataStore.AddSeq(Seq: TOpSequence);
begin
  DLListInsertTail(@FInProgressOps, @Seq.FSequenceLink);
  Inc(FInProgressOpsCount);
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Added sequence, sequences in progress: ' + IntToStr(FInProgressOpsCount));
{$ENDIF}
end;

procedure TLoaderDataStore.RemoveSeq(Seq: TOpSequence);
begin
  DLListRemoveObj(@Seq.FSequenceLink);
  Dec(FInProgressOpsCount);
{$IFDEF DEBUG_SEQUENCING}
  GLogLog(SV_INFO, 'Removed sequence, sequences in progress: ' + IntToStr(FInProgressOpsCount));
{$ENDIF}
  if DlItemIsEmpty(@FInProgressOps) then
  begin
    { Not any more, now that custom queries don't change store state.
    Assert((StoreState = ldsUpdatingImport) or (StoreState = ldsReadingFromDB) or
      (StoreState = ldsPruning));
    }
    StoreState := ldsIdle;
  end;
end;

constructor TLoaderDataStore.Create;
begin
  inherited;
  FDataMirror := TDatastoreMemMirror.Create;
  DLItemInitList(@FInProgressOps);
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'TLoaderDatastore create: ' +IntToHex(Int64(self), 16));
{$ENDIF}
end;

destructor TLoaderDataStore.Destroy;
begin
{$IFDEF DEBUG_SEQ_MEMORY}
  GLogLog(SV_INFO, 'TLoaderDatastore destroy: ' +IntToHex(Int64(self), 16));
{$ENDIF}
  try
    Assert(DlItemIsEmpty(@FInProgressOps));
    Assert(FInProgressOpsCount = 0);
    Assert(StoreState = ldsIdle);
    FDataMirror.Free;
  except
{$IFDEF DEBUG_SEQ_MEMORY}
    GLogLog(SV_INFO, 'TLoaderDatastore destroy exception: ' +IntToHex(Int64(self), 16)
    + ' In progress ops: ' + IntToStr(FInProgressOpsCount));
{$ENDIF}
  end;
  inherited;
end;

function TLoaderDataStore.Stop: boolean;
begin
  FClosing := true;
  result := StoreState = ldsIdle;
end;

function TLoaderDataStore.ConditionalUnstop: boolean;
begin
  if FClosing and (StoreState = ldsIdle) then
    FClosing := false;
  result := not FClosing;
end;

function TLoaderDataStore.CheckStartOp(OpResultState: TLoaderDataStoreState): boolean;
begin
  result := (not FClosing) and (StoreState in [ldsIdle, OpResultState]);
end;

function TLoaderDataStore.GetRWCopyByKey(const Key: TGuid): TKUSerProfile;
begin
  Assert(Key <> TGuid.Empty);
  result := FDataMirror.GetRWCopyByKey(Key);
end;

function TLoaderDataStore.GetRWCopyByBlockArray(Blocks: TKUserBlockArray): TKUSerProfile;
begin
  result := FDataMirror.GetRWCopyByBlockArray(Blocks);
end;

function TLoaderDataStore.NewRWCopy: TKUserProfile;
begin
  result := FDataMirror.NewRWCopy;
end;

procedure TLoaderDataStore.PutRWCopy(Profile: TKUSerProfile);
begin
  FDataMirror.PutRWCopy(Profile);
end;

procedure TLoaderDataStore.RWCopyKeyChanging(Profile: TKUserProfile);
begin
  FDataMirror.RWCopyKeyChanging(Profile);
end;

procedure TLoaderDataStore.RWCopyKeyChanged(Profile: TKUserProfile);
begin
  FDataMirror.RWCopyKeyChanged(Profile);
end;

procedure TLoaderDataStore.Commit;
begin
  FDataMirror.Commit;
end;


function TLoaderDataStore.ReadUserListToMem(Ref, Ref2: TObject): boolean;
var
  Seq: TReadUserListSequence;
begin
  result := false;
  if CheckStartOp(ldsReadingFromDB) then
  begin
    Seq := TReadUserListSequence.Create;
    Seq.FRef := Ref;
    Seq.FRef2 := Ref2;
    result := Seq.Start(self);
    if result then
    begin
      StoreState := ldsReadingFromDB;
      if FUserListHintState < ulhLoading then
        FUserListHintState := ulhLoading;
    end
    else
      Seq.Free;
  end;
end;

//Write user list mainly for DB import/export. Expects an empty datastore.
function TLoaderDataStore.WriteUserList(UserList: TKUserList; Ref, Ref2: TObject): boolean;
var
  Seq: TWriteUserListSequence;
begin
  result := false;
  if CheckStartOp(ldsUpdatingImport) then
  begin
     if FDataMirror.FPresentationList.Count = 0 then
     begin
       //Only attempt this with an empty store.
       Seq := TWriteUserListSequence.Create;
       Seq.FUserList := UserList.DeepClone(UserList) as TKUserList;
       Seq.FRef := Ref;
       Seq.FRef2 := Ref2;
       Seq.FAllowableActions := [patAdd, patUpdate];
       result := Seq.Start(self);
       if result then
        StoreState := ldsUpdatingImport
       else
         Seq.Free;
     end;
  end;
end;


function TLoaderDataStore.UpdateImportedUser(User: TKUserProfile; Ref, Ref2: TObject;
  AllowableActions: TPersistActionSet): boolean; // User and media list.

var
  Seq: TWriteUserOpSequence;
begin
  result := false;
  if patDelete in AllowableActions then
    exit; //That can of worms we will deal with later.
  if CheckStartOp(ldsUpdatingImport) then
  begin
{$IFDEF DEBUG_SEQUENCING}
    GLogLog(SV_INFO, 'Update imported user: ' + User.Key + ' IUID: ' + User.SiteUserBlocks
      [tstInstagram].UserId + ' IUName:' + User.SiteUserBlocks[tstInstagram].Username + ' TUID: ' +
      User.SiteUserBlocks[tstTwitter].UserId + ' TUName:' + User.SiteUserBlocks[tstTwitter]
      .Username);
{$ENDIF}
    Seq := TWriteUserOpSequence.Create;
    Seq.FUserProfile := User.CloneSelfAndChildKeyed as TKUserProfile;
    Seq.FRef := Ref;
    Seq.FRef2 := Ref2;
    Seq.FAllowableActions := AllowableActions;
    result := Seq.Start(self);
    if result then
      StoreState := ldsUpdatingImport
    else
      Seq.Free; // Frees copy as well.
  end;
end;

function TLoaderDataStore.UpdateImportedMedia(Media: TKMediaItem; Ref, Ref2: TObject;
  AllowableActions: TPersistActionSet): boolean; // Media and comment list.

  function FindUserProfileByMediaSiteBlocks(Media: TKMediaItem;
    var UserProfIsDurable: boolean): TKUserProfile;
  var
    ST: TKSiteType;
    SearchUP: TKUserProfile;
  begin
    SearchUP := TKUserProfile.Create;
    try
      for ST := Low(ST) to High(ST) do
      begin
        if Length(Media.SiteMediaBlock[ST].OwnerID) > 0 then
        begin
          SearchUP.SiteUserBlock[ST].UserId := Media.SiteMediaBlock[ST].OwnerID;
          SearchUP.SiteUserBlock[ST].Valid := Media.SiteMediaBlock[ST].Valid;
        end;
      end;
      result := GetDurableReadOnlyTreeByBlockArray(SearchUP.SiteUserBlocks);
      UserProfIsDurable := Assigned(result);
      if not Assigned(result) then
        result := GetRWCopyByBlockArray(SearchUP.SiteUserBlocks)
    finally
      SearchUP.Free;
    end;
  end;

var
  Seq: TWriteMediaOpSequence;
  UP: TKUserProfile;
  UserProfIsDurable: boolean;
begin
  result := false;
  if patDelete in AllowableActions then
    exit; //That can of worms we will deal with later.
  if CheckStartOp(ldsUpdatingImport) then
  begin
    //When updating media from import
    //don't necessarily know any of the GUID keys.
    //Might be able to optimise in the batch loader...
    UP := FindUserProfileByMediaSiteBlocks(Media, UserProfIsDurable);
    try
      if Assigned(UP) then
      begin
        // Look up user based on Site OwnerID for site type,
        // and get GUID key.
{$IFDEF DEBUG_SEQUENCING}
        GLogLog(SV_INFO, 'Update imported media, owning ukey: ' + UP.Key + ' IUID: ' +
          Media.SiteMediaBlocks[tstInstagram].MediaID + ' IMCode:' + Media.SiteMediaBlocks[tstInstagram]
          .MediaCode + ' TUID: ' + Media.SiteMediaBlocks[tstTwitter].MediaID + ' TMCode:' +
          Media.SiteMediaBlocks[tstTwitter].MediaCode);
{$ENDIF}
        Seq := TWriteMediaOpSequence.Create;
        Seq.FMediaItem := Media.CloneSelfAndChildKeyed as TKMediaItem;
        Seq.FUserKey := UP.Key;
        Seq.FRef := Ref;
        Seq.FRef2 := Ref2;
        Seq.FAllowableActions := AllowableActions;
        result := Seq.Start(self);
        if result then
          StoreState := ldsUpdatingImport
        else
          Seq.Free;
      end;
    finally
      if Assigned(UP) then
      begin
        if UserProfIsDurable then        
          FDataMirror.PutDurable(UP)
        else
          FDataMirror.PutRWCopy(UP);
      end;
    end;
  end;
end;


function TLoaderDataStore.ReadUserTreeToMem(User: TKUserProfile; Ref, Ref2: TObject): boolean;
var
  Seq: TReadUserTreeSequence;
begin
  result := false;
  if CheckStartOp(ldsReadingFromDB) then
  begin
    Seq := TReadUserTreeSequence.Create;
    Seq.FUserProfile := TKDataObject.Clone(User) as TKUserProfile;
    Seq.FRef := Ref;
    Seq.FRef2 := Ref2;
    result := Seq.Start(self);
    if result then
      StoreState := ldsReadingFromDB
    else
      Seq.Free;
  end;
end;


function TLoaderDataStore.DeleteUserData(User: TKUserProfile; DeleteLevel: TKListLevel;
  DeleteType: TDeleteType; Ref, Ref2: TObject): boolean;
var
  Seq: TDeleteDataSequence;
begin
  result := false;
  if CheckStartOp(ldsPruning) then
  begin
    Seq := TDeleteDataSequence.Create;
    Seq.FUserProfile := TKDataObject.Clone(User) as TKUserProfile;
    Seq.FDeleteLevel := DeleteLevel;
    Seq.FDeleteType := DeleteType;
    Seq.FRef := Ref;
    Seq.FRef2 := Ref2;
    result := Seq.Start(self);
    if result then
      StoreState := ldsPruning
    else
      Seq.Free;
  end;
end;

//Expire is always both in-mem and in-DB.
function TLoaderDataStore.ExpireUserData(User: TKUserProfile; ExpireBefore: TDateTime;
  ExpiryType: TDBExpiryType; LevelSet: TKListLevelSet; Ref, Ref2: TObject): boolean;
var
  Seq: TExpireDataSequence;
begin
  result := false;
  if CheckStartOp(ldsPruning) then
  begin
    Seq := TExpireDataSequence.Create;
    Seq.FUserProfile := TKDataObject.Clone(User) as TKUSerProfile;
    Seq.FExpireBefore := ExpireBefore;
    Seq.FExpiryType := ExpiryType;
    Seq.FLevelSet := LevelSet;
    Seq.FRef := Ref;
    Seq.FRef2 := Ref2;
    result := Seq.Start(self);
    if result then
      StoreState := ldsPruning
    else
      Seq.Free;
  end;
end;

function TLoaderDataStore.PruneUnusedData(Ref, Ref2: TObject): boolean;
var
  Seq: TPruneUnusedSequence;
begin
  result := false;
  if CheckStartOp(ldsPruning) then
  begin
    Seq := TPruneUnusedSequence.Create;
    Seq.FRef := Ref;
    Seq.FRef2 := Ref2;
    result := Seq.Start(self);
    if result then
      StoreState := ldsPruning
    else
      Seq.Free;
  end;
end;

function TLoaderDataStore.CustomQuery(Level: TKListLevel; FilterByUsers: TKUserList; AdditionalFilter: TAdditionalFilter; Ref, Ref2: TObject): boolean;
var
  Seq: TCustomQueryOpSequence;
begin
  result := not FClosing;
  if result then
  begin
    Seq := TCustomQueryOpSequence.Create;
    Seq.FQueryLevel := Level;
    Seq.FQueryUsersFilter := TKList.CloneListAndItemsOnly(FilterByUsers) as TKUserList;
    Seq.FFilter := AdditionalFilter;
    Seq.FRef := Ref;
    Seq.FRef2 := Ref2;
    result := Seq.Start(self);
    if not result then //Don't change the store state.
      Seq.Free;
  end;
end;

function TLoaderDataStore.GetTemporaryTopLevelList: TKUserList;
begin
  result := FDataMirror.GetTemporaryTopLevelList;
end;

procedure TLoaderDataStore.PutTopLevelList(List: TKUserList);
begin
  FDataMirror.PutTopLevelList(List);
end;

function TLoaderDataStore.GetDurableReadOnlyTreeByKey(const Key: TGuid): TKUserProfile;
begin
  result := FDataMirror.GetDurableReadOnlyTreeByKey(Key);
end;

function TLoaderDataStore.GetDurableReadOnlyTreeByBlockArray(Blocks: TKUserBlockArray): TKUserProfile;
begin
  result := FDataMirror.GetDurableReadOnlyTreeByBlockArray(Blocks);
end;

procedure TLoaderDataStore.PutDurable(Profile: TKUserProfile);
begin
  FDataMirror.PutDurable(Profile);
end;

{$IFDEF DEBUG_MEM_MIRROR}
function TLoaderDataStore.GetDBGInfo: TStringList;
begin
  result := TStringList.Create;
  result.Add('RW copies created' + IntToStr(FDataMirror.FRWCopiesCreated));
  result.Add('RW copies deleted' + IntToStr(FDataMirror.FRWCopiesDeleted));
  result.Add('RW copies moved to presentation' + IntToStr(FDataMirror.FRWCopiesMovedToPresentation));
  result.Add('Presentation copies deleted' + IntToStr(FDataMirror.FPresentationCopiesDeleted));
  result.Add('Presentation copies moved to old' + IntToStr(FDataMirror.FPresentationCopiesMovedToOld));
  result.Add('Old copies deleted' + IntToStr(FDataMirror.FOldCopiesDeleted));
end;
{$ENDIF}

end.
