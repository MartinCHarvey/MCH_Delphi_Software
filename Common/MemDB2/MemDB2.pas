unit MemDB2;
{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

{ In memory database.
  Top level synchronization, session and transaction handling.
  Ephemeral session-based data. }

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SysUtils, Classes, MemDB2Misc, MemDB2Streamable, MemDB2Journal,
  MemDB2Buffered, MemDB2API, LockAbstractions, RWWLock, MemDB2BufBase, Reffed;

type
  PReffedList = ^TReffedList;
  TMemDB = class;
  TMemDBSession = class;

  TInProgressType = (tiptNone, tiptStart, tiptCommitRollback);

  //Currently used for garbage collection from multiple async/parallel operations.
  TXTransactionLocalContext = class(TTXLocalContext)
  private
    FRegisteredListLock: TCriticalSection;
    PSnapshotList, PRegisteredList: PReffedList;
  public
    //Misc cruft for node caching
    //Public at the moment for simplicity.

    //Lock is for cache list.
    FCacheListLock: TCriticalSection;
    //In future perhaps have separate lists of TObject and TReffed.
    FCacheList: TList;

    procedure AddCache(Cache: TObject);

    constructor Create;
    destructor Destroy; override;

    //Entity lists slightly more critical to handle correctly,
    //hence procedures to ensure correct access.

    procedure SetInitialEntityLists(List: TReffedList);
    procedure ValidateEntityLists(Canonical: TReffedList);
    function GetEntityList(Iso: TMDBIsolationLevel): TReffedList;
    procedure PutEntityList(List: TReffedList);
    procedure SetPtrs(Snapshot, Registered: PReffedList);

    procedure AddEntityProxyToRegistered(EProxy: TReffed);
    procedure AddEntityProxyToSnapshot(EProxy: TReffed);
  end;

{$IFDEF USE_TRACKABLES}
  TMemDBTransaction = class(TTrackedReffed)
{$ELSE}
  TMemDBTransaction = class(TReffed)
{$ENDIF}
  private
    //Links to datastructures.
    FDB: TMemDB;
    FSession: TMemDBSession;
    //Local mode fields.
    FMode: TMDBAccessMode;
    FSync: TMDBSyncMode;
    FTid: TTransactionId;
    //Txions register with entities at Txion start and end.
    //These to ensure consistent register/deregister.
    FEntitiesSnapshot: TReffedList;
    FEntitiesRegistered: TReffedList;

    //RWWLock handling slightly messier than I wanted ...
    //We expect the case where this flag is false to be vanishingly rare;
    FRWWLocked: integer;
    //Changeset states.
    FChangeset: TStream;
    FCommitRollbackInProgress: TInProgressType;
    FCommitedOrRolledBack: boolean;
    FFlushFinishedEvent: TEvent;
    FApiObjects: TList;
    FApiObjectLock: TCriticalSection;
    FCRInterface: TMemAPIDatabaseInternal;
    FLocalContext: TXTransactionLocalContext;
  protected
    //Could make this public if needed later by MemDB2 API.
  public
    procedure CleanJournalledTransaction;
    //Public but internal use only.
    procedure HandleNewEntityUnderDBLocks(Entity: TMemDBEntity);

    procedure CommitAndFree;
    procedure RollbackAndFree(LowMemory: boolean = false);

    procedure RegisterCreatedApi(Api: TMemDBApi);
    procedure DeregisterCreatedApi(Api: TMemDBApi);
    procedure CheckNoDanglingTransactionRefs(CanRaise: boolean);

    constructor Create;
    destructor Destroy; override;

    function GetAPI: TMemAPIDatabase;

    property Mode: TMDBAccessMode read FMode;
    property Sync: TMDBSyncMode read FSync;

    property Tid: TTransactionId read FTid;
    property FlushFinishedEvent: TEvent read FFlushFinishedEvent
      write FFlushFinishedEvent;
    property ParentSession: TMemDBSession read FSession;
    property Changeset: TStream read FChangeset write FChangeset;
    property Session: TMemDBSession read FSession;
    property LocalContext: TXTransactionLocalContext read FLocalContext;
  end;

{$IFDEF USE_TRACKABLES}
  TMemDBSession = class(TTrackable)
{$ELSE}
  TMemDBSession = class
{$ENDIF}
  private
    FDB: TMemDB;
    FSessionTransactions: TList;
    FTempStorageMode: TTempStorageMode;
    FClosing: boolean;
  protected
    function GetTempStorageMode: TTempStorageMode;
    procedure SetTempStorageMode(NewMode: TTempStorageMode);
  public
    function StartTransaction(Mode: TMDBAccessMode;
                              Sync: TMDBSyncMode = amLazyWrite;
                              Iso: TMDBIsolationLevel = ilReadRepeatable): TMemDBTransaction;
    constructor Create;
    destructor Destroy; override;
    property ParentDB: TMemDB read FDB;
    property TempStorageMode:TTempStorageMode read
      GetTempStorageMode write SetTempStorageMode;
  end;

  TMemDbAPICallbacks = class(TMemDBAPICallbackInt)
  protected
    FDB: TMemDB;
  public
    //Although DB err flag used async by journal,
    //also use it synchronously, so that inconsistencies between
    //written to journal / commited to memory can be caught immediately.
    procedure CheckDBErrFlagSynchronous; override;
    procedure SetDBErrFlag(Msg: string); override;
    procedure JournalTxion(T: TReffed); override;
  end;

{$IFDEF USE_TRACKABLES}
  TMemDB = class(TTrackable)
{$ELSE}
  TMemDB = class
{$ENDIF}
  private
    // Synchronise DB state, sessions, transactions.
    FSessionLock: TCriticalSection;
    // Synchronize access to database state.
    FRWWLock: TRWWLock;
    FSessionList: TList;
    FCheckpointRefs: integer; //Also used to reference number of oustanding stats calls.
    FTransactionList: TList;
    FInitWait: TEvent;
    FClientWait: TEvent;
    FPersistWait: TEvent;
    FRunningTeardown: boolean;
    FJournal: TMemDbDefaultJournal;
    FDatabase: TMemDbDatabasePersistent;
    FPhase: TMemDBPhase;
    FLastError: string;
    FOnUIStateChange: TNotifyEvent;
    FCommitApiCallbacks: TMemDBAPiCallbacks;
    FSeenLowMem: boolean;
  protected
    procedure RemoveSession(Session: TMemDBSession);
    procedure RemoveTransaction(Transaction: TMemDBTransaction; Commit: boolean; LowMemory: boolean);

    procedure StopDBLocked;
    function CheckClientsDone: boolean;
    function StartTransaction(Mode: TMDBAccessMode; Sync: TMDBSyncMode;
                              Iso: TMDBIsolationLevel; Session: TMemDBSession)
      : TMemDBTransaction;

    procedure HandleJournalInitialized(Sender: TObject; OK: boolean; AnyFiles: boolean;
      CreateCheckpoint: boolean; ErrMsg:string);
    procedure HandleJournalFinished(Sender: TObject);
    procedure HandleJournalError(Sender: TObject; ErrMsg:string);
    procedure HandleJournalInitReplayTransaction(Sender: TObject; Changesets: TObject;
      Initial: boolean);
    procedure HandleJournalTransactionWriteFlush(Sender: TObject; Transaction: TObject);
    procedure HandleUIStateChange(Sender: TObject);

    procedure HandleNewEntityUnderDBLocks(Entity: TMemDBEntity; OwnerTx: TMemDBTransaction);

    procedure CommitAPICheckDBErrFlagSynchronous;
    procedure CommitAPISetDBErrFlag(Msg: string);
    procedure CommitAPIJournalTxion(T: TReffed);
    function GetLastError: string;
  public

    constructor Create;
    destructor Destroy; override;
    function InitDB(RootLocation: string;
                    JournalType: TMemDBJournalType = jtV2;
                    Async: boolean = false;
                    AutoInitialCheckpoint: boolean = true): boolean;
    procedure StopDB(Force: boolean = false);

    //WARNING: Asynchronous checkpoint may change the row order
    //permanently when iterating not by index.
    //(same order as iterate with isolation >= snapshot)

    //Asynchronous here means it doesn't block other DB transactions in
    //sharedReadWrite mode....
    function Checkpoint(Asynchronous:boolean = false): boolean;
    function StartSession: TMemDBSession;

    property DBState: TMemDBPhase read FPhase;
    //Reason for the most recent move to an error phase. Empty if none
    //since the last InitDB.
    property LastError: string read GetLastError;
    property OnUIStateChange: TNotifyEvent read FOnUIStateChange write FOnUIStateChange;
  end;

implementation

uses IoUtils, Types;

const
  S_DB_CLOSING_ERROR_OR_NOT_OPEN = 'DB closing, error state or not open: ';
  S_DB_SESSION_NOT_FOUND = 'Session not found';
  S_DB_HAS_SESSIONS = 'DB has open sessions.';
  S_DB_SESSION_HAS_TRANSACTIONS = 'Session has open transactions';
  S_DB_TRANSACTION_NOT_FOUND = 'Transaction not found';
  S_COMMIT_ROLLBACK_IN_PROGRESS = 'Commit or rollback for this transaction already in progress.';
  S_COMMIT_OR_ROLLBACK_BEFORE_FREE = 'Transactions should be committed or rolled back before destroying. Are you calling an inherited ''Free'' function? ';
  S_ERRORPHASE_ERROR = 'Can''t start transaction, DB in error state: ';
  S_ERRORPHASE_COMMIT_ERROR = 'DB in internal error state, Can''t commit transaction.';
  S_BADPHASE_ERROR = 'Can''t start transaction, DB not running or closing.';
  S_REINIT_DIFF_LOCATION = 'Database loaded at different location, unload first.';
  S_UNEXPECTED_CLEANUP_OPS_FAILED = 'Fatal: Unexpected rollback or cleanup failed, state uncertain: ';
  S_READONLY_HAS_CHANGED_DATA = 'Read-only transaction has changed data!';
  S_TRANSACTION_HAS_API_OBJECTS = 'Transaction has associated API objects. You should have freed them before commit/rollback';
  S_ENTITIES_OUT_OF_DATE = 'Aborted: Concurrency conflict: table/key list changed, not fixed up in time.';

  { TXLocalContext }

constructor TXTransactionLocalContext.Create;
begin
  inherited;
  FCacheListLock := TCriticalSection.Create;
  FCacheList := TList.Create;
  //Set this way early so as little alloc as possible on rollback path.
  FCacheList.Capacity := 64;
  FRegisteredListLock := TCriticalSection.Create;
end;

destructor TXTransactionLocalContext.Destroy;
var
  i: integer;
begin
  //Expect transaction synchronous cleanup to have already cleaned most of this
  //up, but just in case...
  for i := 0 to Pred(FCacheList.Count) do
    TObject(FCacheList[i]).Free;
  FCacheList.Free;
  FCacheListLock.Free;
  FRegisteredListLock.Free;
  //Do not destroy PReffedLists here.
  inherited;
end;

procedure TXTransactionLocalContext.AddCache(Cache: TObject);
begin
  if Assigned(Cache) then
  begin
    FCacheListLock.Acquire;
    try
      FCacheList.Add(Cache);
    finally
      FCacheListLock.Release;
    end;
  end;
end;

procedure TXTransactionLocalContext.SetInitialEntityLists(List: TReffedList);
begin
  FRegisteredListLock.Acquire;
  try
    Assert(PSnapshotList^ = nil);
    PSnapshotList^ := List;
    Assert(PRegisteredList^ = nil);
    PRegisteredList^ := PSnapshotList.CloneByRef;
  finally
    FRegisteredListLock.Release;
  end;
end;

procedure TXTransactionLocalContext.ValidateEntityLists(Canonical: TReffedList);
begin
  FRegisteredListLock.Acquire;
  try
    //At pre commit, need to check against all entities present in the DB.
    if not Canonical.SameMembers(PRegisteredList^) then
      raise EMemDBConcurrencyException.Create(S_ENTITIES_OUT_OF_DATE);
  finally
    FRegisteredListLock.Release;
  end;
end;

function TXTransactionLocalContext.GetEntityList(Iso: TMDBIsolationLevel): TReffedList;
begin
  FRegisteredListLock.Acquire;
  try
    if Iso >= ilSnapshot then
    begin
      if Assigned(PSnapshotList^) then
        result := PSnapshotList.CloneByRef
      else
        result := nil;
    end
    else
    begin
      if Assigned(PRegisteredList^) then
        result := PRegisteredList.CloneByRef
      else
        result := nil;
    end;
  finally
    FRegisteredListLock.Release;
  end;
end;

procedure TXTransactionLocalContext.PutEntityList(List: TReffedList);
begin
  List.Release;
end;

procedure TXTransactionLocalContext.SetPtrs(Snapshot, Registered: PReffedList);
begin
  PSnapshotList := Snapshot;
  PRegisteredList := Registered;
end;

procedure TXTransactionLocalContext.AddEntityProxyToRegistered(EProxy: TReffed);
begin
  FRegisteredListLock.Acquire;
  try
    Assert(Assigned(EProxy));
    Assert(PRegisteredList.IndexOf(EProxy) < 0);
    PRegisteredList.AddNoRef(EProxy.AddRef);
  finally
    FRegisteredListLock.Release;
  end;
end;

procedure TXTransactionLocalContext.AddEntityProxyToSnapshot(EProxy: TReffed);
begin
  FRegisteredListLock.Acquire;
  try
    Assert(Assigned(EProxy));
    Assert(PSnapshotList.IndexOf(EProxy) < 0);
    PSnapshotList.AddNoRef(EProxy.AddRef);
  finally
    FRegisteredListLock.Release;
  end;
end;

  { TMemDBTransaction }

procedure TMemDBTransaction.CleanJournalledTransaction;
begin
  FCRInterface.Free;
  FCRInterface := nil;
  FLocalContext.Free;
  FLocalContext := nil;
  FEntitiesSnapshot.Release;
  FEntitiesSnapshot := nil;
  FEntitiesRegistered.Release;
  FEntitiesRegistered := nil;
end;

procedure TMemDBTransaction.HandleNewEntityUnderDBLocks(Entity: TMemDBEntity);
begin
  FDB.HandleNewEntityUnderDBLocks(Entity, self);
end;

function TMemDBTransaction.GetAPI: TMemAPIDatabase;
begin
  Assert(not FCommitedOrRolledBack);
  Assert(FCommitRollbackInProgress = tiptNone);
  result := FDB.FDatabase.Interfaced.GetAPIObject(self, APIDatabase) as TMemAPIDatabase;
end;

procedure TMemDBTransaction.CommitAndFree;
begin
  FDB.RemoveTransaction(self, true, false);
end;

procedure TMemDBTransaction.RollbackAndFree(LowMemory: boolean);
begin
  FDB.RemoveTransaction(self, false, LowMemory);
end;

constructor TMemDBTransaction.Create;
begin
  inherited;
  FApiObjects := TList.Create;
  FApiObjectLock := TCriticalSection.Create;
  FLocalContext := TXTransactionLocalContext.Create;
  FLocalContext.PSnapshotList := @FEntitiesSnapshot;
  FLocalContext.PRegisteredList := @FEntitiesRegistered;
end;

procedure TMemDbTransaction.RegisterCreatedApi(Api: TMemDBApi);
begin
  FApiObjectLock.Acquire;
  try
    Assert(FApiObjects.IndexOf(Api) < 0);
    FApiObjects.Add(Api);
  finally
    FApiObjectLock.Release;
  end;
end;

procedure TMemDbTransaction.DeregisterCreatedApi(Api: TMemDBApi);
begin
  FApiObjectLock.Acquire;
  try
    Assert(FApiObjects.IndexOf(Api) >= 0);
    FApiObjects.Remove(Api);
  finally
    FApiObjectLock.Release;
  end;
end;

procedure TMemDbTransaction.CheckNoDanglingTransactionRefs(CanRaise: boolean);
var
  Tmp: TObject;
  i: Integer;
begin
  if CanRaise then
  begin
    FApiObjectLock.Acquire;
    try
      //Expect only API object to be that of the FCR interface.
      for i := 0 to Pred(FApiObjects.Count) do
      begin
        if FApiObjects.Items[i] <> FCRInterface then
          raise EMemDBAPIException.Create(S_TRANSACTION_HAS_API_OBJECTS);
      end;
    finally
      FApiObjectLock.Release;
    end;
  end
  else
  begin
    //I am going to quietly clear down non-internal API objects for this transaction.
    //This allows us in error / shutdown cases to quietly rollback all txions,
    //remove API's, delete sessions, and have a chance at refcounts returning to zero.

    //Don't allocate any memory in quiet clear-up case
    //Out of memory here is a distinct possibility.
    repeat
      FApiObjectLock.Acquire;
      try
        Tmp := nil;
        for i := 0 to Pred(FApiObjects.Count) do
        begin
          if FApiObjects.Items[i] <> FCRInterface then
          begin
            Tmp := FApiObjects.Items[i];
            break;
          end;
        end;
      finally
        FApiObjectLock.Release;
      end;
      Tmp.Free;
    until not Assigned(Tmp);
  end;
end;

destructor TMemDBTransaction.Destroy;
begin
  if not FCommitedOrRolledBack then
    raise EMemDBAPIException.Create(S_COMMIT_OR_ROLLBACK_BEFORE_FREE);
  FCRInterface.Free;
  FChangeset.Free;
  Assert((not Assigned(FApiObjects)) or (FAPIObjects.Count = 0));
  FApiObjects.Free;
  FApiObjectLock.Free;
  FLocalContext.Free;
  FFlushFinishedEvent.Free;
  FEntitiesSnapshot.Release;
  FEntitiesRegistered.Release;
  inherited;
end;

{ TMemDBSession }

function TMemDBSession.GetTempStorageMode: TTempStorageMode;
begin
  FDB.FSessionLock.Acquire;
  try
    result := FTempStorageMode;
  finally
    FDB.FSessionLock.Release;
  end;
end;

procedure TMemDBSession.SetTempStorageMode(NewMode: TTempStorageMode);
begin
  FDB.FSessionLock.Acquire;
  try
  FTempStorageMode := NewMode;
  finally
    FDB.FSessionLock.Release;
  end;
end;

function TMemDBSession.StartTransaction(Mode: TMDBAccessMode; Sync: TMDBSyncMode; Iso: TMDBIsolationLevel)
  : TMemDBTransaction;
begin
  result := FDB.StartTransaction(Mode, Sync, Iso, self);
end;

constructor TMemDBSession.Create;
begin
  inherited;
  FSessionTransactions := TList.Create;
end;

destructor TMemDBSession.Destroy;
begin
  if Assigned(FDB) then
    FDB.RemoveSession(self);
  Assert((not Assigned(FSessionTransactions)) or (FSessionTransactions.Count = 0));
  FSessionTransactions.Free;
  inherited;
end;

{ TMemDB }

function TMemDB.CheckClientsDone: boolean;
begin
  result := (FSessionList.Count = 0) and (FCheckpointRefs <= 0);
  Assert(FPhase in [mdbInitCheckpoint, mdbRunning, mdbRunningError,
    mdbClosingWaitClients, mdbClosingWaitClientsError ]);
  if result and (FPhase in [mdbClosingWaitClients, mdbClosingWaitClientsError]) then
    FClientWait.SetEvent;
end;

procedure TMemDB.HandleNewEntityUnderDBLocks(Entity: TMemDBEntity; OwnerTx: TMemDBTransaction);
var
  IdleTxionList: TList;
  i: integer;
  Tx: TMemDBTransaction;
begin
  //Oh this is nasty.

  //Holding the session lock ensures that IdleTxionList is always accurate,
  //and that the commit/rollback in progress state is accurate.

  //Holding the commit and meta index locks ensures that Txions we reference
  //cannot start accessing entity lists as part of commit or rollback cycle,
  //if they transition from no commit rollbacks in progress to commit
  //rollbacks in progress.

  //However, for txions which have a commit/rollback in progress,
  //or are committed / rolled back  we should not
  //change the entity lists for them:
  //  - Entity lists need to be stable over in progress commit / rollback.
  //  - Entity list should not be added to once Txion comitted / rolled back.

  ///Exception cases, erm. I think they're ok ...
  IdleTxionList := TList.Create;
  try
    FSessionLock.Acquire;
    try
      //Get all transactions where CR not yet in progress, and stopped
      //before commit / meta index lock.
      for i := 0 to Pred(FTransactionList.Count) do
      begin
        Tx := TMemDBTransaction(FTransactionList.Items[i]);
        if (Tx.FCommitRollbackInProgress = TInProgressType.tiptNone)
          and not (Tx.FCommitedOrRolledBack) then
          IdleTxionList.Add(Tx);
      end;
    finally
      FSessionLock.Release;
    end;
    //Even after releasing session lock, Txions may start a commit/rollback,
    //but will not get as far as accessing entity lists or DB.

    //Touch wood, we don't even need to add-ref, they won't get any further down
    //the destruction path due to locking.

    //For Txions we have had to skip for some reason, PreCommit catches incomplete
    //entity lists, and Rollback is by definition only rollback of entities that
    //have been touched by that txion.
    for i := 0 to Pred(IdleTxionList.Count) do
    begin
      Tx := TMemDBTransaction(IdleTxionList.Items[i]);
      //Also, do not update entity lists for serialisable transactions unless
      //we are the transaction adding the new table, that way
      //entity additions by other transactions result in an abort.
      if (Tx.Tid.Iso < ilSerialisable) or (Tx = OwnerTx) then
      begin
        Entity.StartTransaction(Tx.Tid, Tx.LocalContext);
        //OK. If some other transaction, then update entity list.
        //If our own transaction, then update entity list and snapshot list.
        Tx.LocalContext.AddEntityProxyToRegistered(Entity.Proxy);
        if Tx = OwnerTx then
          Tx.LocalContext.AddEntityProxyToSnapshot(Entity.Proxy);
      end;
    end;
  finally
    IdleTxionList.Free;
  end;
end;

//Check and set DB error flag synchronously, so that
//any variance in data committed to memory and data
//written to journal will result in immediate stop.
procedure TMemDBAPICallbacks.CheckDBErrFlagSynchronous;
begin
  FDB.CommitAPICheckDbErrFlagSynchronous;
end;

procedure TMemDBAPICallbacks.SetDBErrFlag(Msg: string);
begin
  FDB.CommitAPISetDBErrFlag(Msg);
end;

procedure TMemDBAPICallbacks.JournalTxion(T: TReffed);
begin
  FDB.CommitAPIJournalTxion(T);
end;

procedure TMemDB.CommitAPICheckDBErrFlagSynchronous;
begin
{$IFDEF DBG_UNDER_COMMIT_LOCK}
  Assert(DbgUnderCommitLock > 0);
{$ENDIF}

  FSessionLock.Acquire;
  try
    Assert(FPhase in [mdbInitCheckpoint,mdbRunning, mdbRunningError,
      mdbClosingWaitClients, mdbClosingWaitClientsError]);
    if FPhase  in [mdbRunningError, mdbClosingWaitClientsError] then
      raise EMemDBException.Create(S_ERRORPHASE_COMMIT_ERROR);
  finally
    FSessionLock.Release;
  end;
end;

procedure TMemDB.CommitAPISetDBErrFlag(Msg: string);
var
  InitState, FinalState: TMemDBPhase;
begin
  FSessionLock.Acquire;
  InitState := FPhase;
  try
    Assert(FPhase in [mdbInitCheckpoint,mdbRunning, mdbRunningError,
                      mdbClosingWaitClients, mdbClosingWaitClientsError]);
    if FPhase in [mdbInitCheckpoint, mdbRunning] then
    begin
      FPhase := mdbRunningError;
      FLastError := S_UNEXPECTED_CLEANUP_OPS_FAILED + Msg;
    end
    else if FPhase in [mdbClosingWaitClients] then
    begin
      FPhase := mdbClosingWaitClientsError;
      FLastError := S_UNEXPECTED_CLEANUP_OPS_FAILED + Msg;
    end;
  finally
    FinalState := FPhase;
    FSessionLock.Release;
  end;
  if InitState <> FinalState then
    FJournal.SynchronizeStateChange;
end;

procedure TMemDB.CommitAPIJournalTxion(T: TReffed);
begin
  T.AddRef;
  try
{$IFDEF DBG_UNDER_COMMIT_LOCK}
  Assert(DbgUnderCommitLock > 0);
{$ENDIF}
    FJournal.TransactionCommitChangeset(T);
  except
    T.Release;
    raise;
  end;
end;


procedure TMemDB.RemoveTransaction(Transaction: TMemDBTransaction; Commit: boolean; LowMemory: boolean);
var
  idx: integer;
  WaitJournalDone: boolean;
  AsyncError: boolean;

  label txion_remove;

begin
  WaitJournalDone := false;
  AsyncError := false; //Placate compiler.
  FSessionLock.Acquire;
  try
    if Transaction.FCommitedOrRolledBack then
      exit;
    if (Transaction.FCommitRollbackInProgress <> tiptNone) then
      raise EMemDBException.Create(S_COMMIT_ROLLBACK_IN_PROGRESS);
    idx := FTransactionList.IndexOf(Transaction);
    if idx < 0 then
      raise EMemDBException.Create(S_DB_TRANSACTION_NOT_FOUND);
    Assert(FPhase in [mdbRunning, mdbRunningError,
                      mdbClosingWaitClients, mdbClosingWaitClientsError]);

    AsyncError := FPhase in [mdbRunningError, mdbClosingWaitClientsError];
    if not AsyncError then
      Transaction.FCommitRollbackInProgress := tiptCommitRollback;
    FSeenLowMem := FSeenLowMem or LowMemory;
  finally
    FSessionLock.Release;
  end;

  //DB hosed is DB hosed.
  if AsyncError then
  begin
    if Commit then
      raise EMemDBException.Create(S_ERRORPHASE_COMMIT_ERROR)
    else
      goto txion_remove;
      //See error case note.
      //Unfortunately, we lose tipCommitRollback serialisation here.
      //RWW Lock dropped OK.

  end;

  try
    //Things get referenced from the API objects ...
    //If it's a commit, then I'll check you've cleared those down.
    //If it's a rollback then I won't, and will clean up for you.
    Transaction.CheckNoDanglingTransactionRefs(Commit);

    if not (Transaction.FMode in [amReadWriteShared, amWriteExclusive]) then
    begin
      if Commit then
      begin
        if FDatabase.AnyChangesForTid(Transaction.Tid, Transaction.FLocalContext) then
          raise EMemDBInternalException.Create(S_READONLY_HAS_CHANGED_DATA);
        //Internally read-only txions go through the rollback cycle to clear pins/refs etc.
        Commit := false;
      end;
    end;

    //Some pathalogical error cases, will have allocated txion,
    //and added to list, but not got transaction commit/rollback API object.

    //If not got FCRInterface, then cannot have done ROBReserve.
    if Assigned(Transaction.FCRInterface) then
    begin
      if Commit then
      begin
        if FJournal.NeedFlowControl then
          Transaction.FSync := amFlushBuffers;

        //Commit cycle also puts DB into error state if one critical
        //commit path fails.
        Transaction.FCRInterface.UserCommitCycle;
        WaitJournaldone := Assigned(Transaction.FLushFinishedEvent);
      end
      else
      begin
        //Since everything is multi-buffered, barring out of memory
        //exceptions we do not expect this to fail.
        Transaction.FCRInterface.UserRollbackCycle(LowMemory);
        //We really need rollback to work to clear pins and refcounts.
        //If it doesn't then something's very very broken.
      end;
    end;
  except
    FSessionLock.Acquire;
    try
      Transaction.FCommitRollbackInProgress := tiptNone;
    finally
      FSessionLock.Release;
    end;
    raise;
  end;
  //If failed commit, hold the R/W lock.
  //If good commit, drop the lock, and flush data.
  //If failed rollback, DB in error, exception swallowed, and drop the R/W lock.

  Assert(Transaction.FMode in [amReadShared, amWriteExclusive, amReadWriteShared]);

  //Normal RWW lock drop.
  if LockAbstractions.InterlockedCompareExchange(Transaction.FRWWLocked,
    Ord(false), Ord(true)) = Ord(True) then
    FRWWLock.Release(DBAccessModeToLockReason(Transaction.FMode));

  if WaitJournalDone then
    Transaction.FlushFinishedEvent.WaitFor(INFINITE);

txion_remove:
  //DB errorphase RWW (txions block on flush) lock drop.
  if LockAbstractions.InterlockedCompareExchange(Transaction.FRWWLocked,
    Ord(false), Ord(true)) = Ord(True) then
    FRWWLock.Release(DBAccessModeToLockReason(Transaction.FMode));

  FSessionLock.Acquire;
  try
    idx := Transaction.FSession.FSessionTransactions.IndexOf(Transaction);
    Assert(idx >= 0);
    Transaction.FCommitedOrRolledBack := true;
    Transaction.FCommitRollbackInProgress := tiptNone;
    Transaction.FSession.FSessionTransactions.Delete(idx);
    idx := FTransactionList.IndexOf(Transaction);
    Assert(idx >= 0);
    FTransactionList.Delete(idx);
  finally
    FSessionLock.Release;
  end;
  Transaction.Release;
end;


function TMemDB.StartTransaction(Mode: TMDBAccessMode;
                                 Sync: TMDBSyncMode;
                                 Iso: TMDBIsolationLevel;Session: TMemDBSession)
  : TMemDBTransaction;
var
  idx: integer;
begin
  result := nil;
  try
    FSessionLock.Acquire;
    try
      idx := FSessionList.IndexOf(Session);
      if (idx < 0)  or Session.FClosing then
        raise EMemDBException.Create(S_DB_SESSION_NOT_FOUND);
      if FPhase <> mdbRunning then
      begin
        //DB hosed is DB hosed.
        if FPhase in [mdbRunningError, mdbClosingWaitClientsError] then
          raise EMemDBException.Create(S_ERRORPHASE_ERROR + FLastError)
        else
          raise EMemDBException.Create(S_BADPHASE_ERROR + FLastError)
      end;
      try
        result := TMemDBTransaction.Create;
        result.FDB := self;
        result.FSession := Session;
        result.FMode := Mode;
        result.FSync := Sync;
        result.FTid := TTransactionId.NewTransactionID(Iso);
        result.FCommitRollbackInProgress := tiptStart;
      except
        if Assigned(result) then
        begin
          result.FCommitedOrRolledBack := true; //just got make dtor OK.
          result.FCommitRollbackInProgress := tiptNone;
          result.Release;
          result := nil;
        end;
        raise;
      end;
      //FRWWLocked flag only necessary for exceptions here, which
      //we expect to be vanishingly rare.
      Session.FSessionTransactions.Add(result);
      FTransactionList.Add(result);
    finally
      FSessionLock.Release;
    end;
    Assert(Mode in [amReadShared, amWriteExclusive, amReadWriteShared]);
    FRWWLock.Acquire(DBAccessModeToLockReason(Mode));
    result.FRWWLocked := Ord(true);

    result.FCRInterface := FDatabase.Interfaced.GetAPIObject(result, APIInternalCommitRollback)
      as TMemAPIDatabaseInternal;
    result.FCRInterface.SetCallbackInterface(self.FCommitApiCallbacks);

    result.FCRInterface.TransactionStartCycle;

    FSessionLock.Acquire; //Really just a fence...
    try
      result.FCommitRollbackInProgress := tiptNone;
    finally
      FSessionLock.Release;
    end;
  except
    if Assigned(result) then
    begin
      //Just set the flag here to let the rollback proceed.
      result.FCommitRollbackInProgress := tiptNone;
      result.RollbackAndFree;
    end;
    raise;
  end;
end;

procedure TMemDB.RemoveSession(Session: TMemDBSession);
var
  idx: integer;
  TRList: TList;
begin
  TRList := TList.Create;
  try
    FSessionLock.Acquire;
    try
      idx := FSessionList.IndexOf(Session);
      if not(idx >= 0) then
        raise EMemDBException.Create(S_DB_SESSION_NOT_FOUND);

{$IF COMPLAIN_LEAKED_TXIONS}
      if Session.FSessionTransactions.Count > 0 then
        raise EMemDBException.Create(S_DB_SESSION_HAS_TRANSACTIONS);
{$ELSE}
      Session.FClosing := true;
      //Quietly clear the transactions down for this session.
      for idx := 0 to Pred(Session.FSessionTransactions.Count) do
        TrList.Add(TMemDBTransaction(Session.FSessionTransactions.Items[idx]).AddRef);
{$ENDIF}
    finally
      FSessionLock.Release;
    end;

    for idx := 0 to Pred(TrList.Count) do
      try
        TMemDBTransaction(TrList[idx]).RollbackAndFree;
      except
        ; //Swallow the exception (wrong state, not in lists). TODO - Anything else?
      end;

    FSessionLock.Acquire;
    try
      idx := FSessionList.IndexOf(Session);
      if idx >= 0 then
        FSessionList.Delete(idx);
      CheckClientsDone;
    finally
      FSessionLock.Release;
    end;
  finally
    for idx := 0 to Pred(TrList.Count) do
      TMemDBTransaction(TrList[idx]).Release;
    TRList.Free;
  end;
end;

function TMemDB.StartSession: TMemDBSession;
begin
  result := nil;
  FSessionLock.Acquire;
  try
    if FRunningTeardown or not(FPhase in [mdbInit, mdbInitCheckpoint, mdbRunning]) then
      raise EMemDBException.Create(S_DB_CLOSING_ERROR_OR_NOT_OPEN + FLastError);
    if FPhase in [mdbInit, mdbInitCheckpoint] then
    begin
      FSessionLock.Release;
      try
        FInitWait.WaitFor(INFINITE);
      finally
        FSessionLock.Acquire;
      end;
      if FPhase <> mdbRunning then
        raise EMemDBException.Create(S_DB_CLOSING_ERROR_OR_NOT_OPEN + FLastError);
    end;
    result := TMemDBSession.Create;
    result.FDB := self;
    FSessionList.Add(result);
    //View out of memory exceptions here as v. unlikely.
  finally
    FSessionLock.Release;
  end;
end;

constructor TMemDB.Create;
begin
  FSessionLock := TCriticalSection.Create;
  FRWWLock := RWWLock.TRWWLock.Create;
  FSessionList := TList.Create;
  FTransactionList := TList.Create;
  FInitWait := TEvent.Create;
  FClientWait := TEvent.Create;
  FPersistWait := TEvent.Create;
  FInitWait.ResetEvent;
  FClientWait.ResetEvent;
  FPersistWait.ResetEvent;
  FJournal := TMemDbDefaultJournal.Create;
  FJournal.OnJournalInitialized := HandleJournalInitialized;
  FJournal.OnJournalFinished := HandleJournalFinished;
  FJournal.OnJournalReplay := HandleJournalInitReplayTransaction;
  FJournal.OnJournalWriteFlush := HandleJournalTransactionWriteFlush;
  FJournal.OnJournalError := HandleJournalError;
  FJournal.OnUIStateChange := HandleUIStateChange;
  FDatabase := TMemDBDatabase.Create;
  FCommitApiCallbacks := TMemDbAPICallbacks.Create;
  FCommitAPICallbacks.FDB := self;
  (FDatabase as TMemDBDatabase).Init(self);
  inherited;
end;


destructor TMemDB.Destroy;
begin
  StopDB(true);
  Assert(FPhase in [mdbNull, mdbClosed, mdbClosedError]);
  Assert(FSessionList.Count = 0);
  Assert(FTransactionList.Count = 0);
  FSessionList.Free;
  FTransactionList.Free;
  FInitWait.Free;
  FClientWait.Free;
  FPersistWait.Free;
  FJournal.Free;
  FDatabase.Free;
  FRWWLock.Free;
  FSessionLock.Free;
  FCommitAPICallbacks.Free;
  inherited;
end;

function TMemDB.InitDB(RootLocation: string;
                       JournalType: TMemDBJournalType;
                       Async: boolean;
                       AutoInitialCheckpoint: boolean): boolean;

  procedure StartupActions;
  begin
    AppendTrailingDirSlash(RootLocation);
    FInitWait.ResetEvent;
    FClientWait.ResetEvent;
    FPersistWait.ResetEvent;
    FSeenLowMem := false;
    FJournal.BaseDirectory := RootLocation;
    FJournal.JournalType := JournalType;
    FJournal.Initialise(AutoInitialCheckpoint);
    FPhase := mdbInit;
    FLastError := '';
  end;

var
  FirstTime: boolean;
  CompareLoc: string;

begin
  FSessionLock.Acquire;
  try
    if ASync then
    begin
      result := false;
      case FPhase of
        mdbClosedError: begin
          //TODO - Any re-init from closed error state.
          FPhase := mdbClosed;
          StartupActions;
        end;
        mdbNull, mdbClosed: StartupActions;
        mdbInit, mdbInitCheckpoint,
        mdbRunning, mdbRunningError: result := true;
        mdbClosingWaitClients, mdbClosingWaitClientsError,
        mdbClosingWaitPersist: ;
      else
        Assert(false);
      end;
    end
    else
    begin
      FirstTime := true;
      while (not (FPhase in [mdbRunning, mdbRunningError, mdbClosedError]))
            or (FirstTime and (FPhase in [mdbClosedError])) do
      begin
        FirstTime := false;
        case FPhase of
          mdbClosedError: begin
            //TODO - Any re-init from closed error state.
            FPhase := mdbClosed;
          end;
          mdbNull, mdbClosed: StartupActions;
          mdbInit, mdbInitCheckpoint:
            begin
              FSessionLock.Release;
              try
                FInitWait.WaitFor(INFINITE);
              finally
                FSessionLock.Acquire;
                Assert(FPhase >= mdbRunning);
              end;
            end;
          mdbClosingWaitClients, mdbClosingWaitClientsError:
            begin
              FSessionLock.Release;
              try
                FClientWait.WaitFor(INFINITE);
              finally
                FSessionLock.Acquire;
              end;
            end;
          mdbClosingWaitPersist:
            begin
              FSessionLock.Release;
              try
                FPersistWait.WaitFor(INFINITE);
              finally
                FSessionLock.Acquire;
              end;
            end;
        else
          Assert(false);
        end;
      end;
      //You can get a good start, and still be in a
      //running but errored state.
      result := FPhase in [mdbRunning, mdbRunningError];
    end;
    if result then
    begin
      if Length (FJournal.BaseDirectory) > 0 then
      begin
        CompareLoc := RootLocation;
        AppendTrailingDirSlash(CompareLoc);
        if (CompareLoc <> FJournal.BaseDirectory) or
          (JournalType <> FJournal.JournalType) then
          raise EMemDBException.Create(S_REINIT_DIFF_LOCATION);
      end;
    end;
  finally
    FSessionLock.Release;
  end;
end;

//This is a blocking state machine, and I'm starting to wonder whether
//I should have coded a Asynchronous one ...
procedure TMemDB.StopDBLocked;
var
  DBTmp: TMemDbDatabasePersistent;
  DelErrFlag: boolean;
  DelErrMsg: string;
begin
  while not(FPhase in [mdbNull, mdbClosed]) do
  begin
    case FPhase of
      mdbInit, mdbInitCheckpoint:
        begin
          FSessionLock.Release;
          try
            FInitWait.WaitFor(INFINITE);
          finally
            FSessionLock.Acquire;
            Assert(FPhase >= mdbRunning);
          end;
        end;
      mdbRunning, mdbRunningError:
        begin
          if CheckClientsDone then
          begin
            FJournal.Finish;
            FPhase := mdbClosingWaitPersist
          end
          else
          begin
            if FPhase = mdbRunningError then
              FPhase := mdbClosingWaitClientsError
            else
              FPhase := mdbClosingWaitClients;
          end;
        end;
      mdbClosingWaitClients, mdbClosingWaitClientsError:
        begin
          FSessionLock.Release;
          try
            FClientWait.WaitFor(INFINITE);
          finally
            FSessionLock.Acquire;
            Assert(FPhase >= mdbClosingWaitClients);
          end;
          if FPhase in [mdbClosingWaitClients, mdbClosingWaitClientsError] then
          begin
            FJournal.Finish;
            FPhase := mdbClosingWaitPersist;
          end;
        end;
      mdbClosingWaitPersist:
        begin
          //Can free DB here provided transactions having state journalled have
          //freed all API objects.
          DBTmp := FDatabase;
          FDatabase := TMemDBDatabase.Create;
          (FDatabase as TMemDBDatabase).Init(self);
          FSessionLock.Release;

          DelErrFlag := false;
          try
            DBTmp.FastTeardown := not FSeenLowMem;
            DBTmp.Free;
          except
            on E: Exception do
            begin
              DelErrFlag := true;
              DelErrMsg := E.Message; //Session lock not held here.
            end;
          end;
          try
            FPersistWait.WaitFor(INFINITE);
          finally
            FSessionLock.Acquire;
            Assert(FPhase >= mdbClosingWaitPersist);
            if FPhase = mdbClosingWaitPersist then
            begin
              if not DelErrFlag then
                FPhase := mdbClosed
              else
              begin
                FPhase := mdbClosedError;
                FLastError := DelErrMsg;
              end;
            end;
          end;
        end;
      mdbClosedError:
      begin
        //TODO - Any odd final cleanup from closed error state.
        FPhase := mdbClosed;
      end
    else
      Assert(false);
    end;
  end;
end;

procedure TMemDB.StopDB(Force: boolean);
var
  SSList: TList;
  idx: integer;
  Owner: boolean;
begin
  Owner := false;
  SSList := TList.Create;
  try
    FSessionLock.Acquire;
    try
      if not Force then
      begin
  {$IF COMPLAIN_LEAKED_SESSIONS}
        if FSessionList.Count > 0 then
          raise EMemDBException.Create(S_DB_HAS_SESSIONS);
  {$ENDIF}
        //Otherwise will wait for sessions / txions to complete.
      end
      else
      begin
        for idx := 0 to Pred(FSessionList.Count) do
          SSList.Add(FSessionList[idx]);
      end;
    finally
      FSessionLock.Release;
    end;

    //Unfortunately TMemDBSession not ref counted.
    for idx := 0 to Pred(SSList.Count) do
      TMemDBSession(SSList[idx]).Free;

    FSessionLock.Acquire;
    try
      if not FRunningTeardown then
      begin
        //User might well try stop, followed by force stop...
        FRunningTeardown := true;
        Owner := true;
        StopDBLocked;
      end;
    finally
      if Owner then
        FRunningTeardown := false;
      FSessionLock.Release;
    end;
  finally
    SSList.Free;
  end;
end;

procedure TMemDB.HandleJournalInitialized(Sender: TObject; OK: boolean; AnyFiles: boolean;
  CreateCheckpoint: boolean; ErrMsg:string);
begin
  try
    FSessionLock.Acquire;
    try
      if OK then
      begin
        if CreateCheckpoint then
          FPhase := mdbInitCheckpoint
        else
        begin
          FPhase := mdbRunning;
          FInitWait.SetEvent;
        end;
      end
      else
      begin
        CreateCheckpoint := false;
        HandleJournalError(self, ErrMsg);
        FInitWait.SetEvent;
      end;
    finally
      FSessionLock.Release;
    end;

    //If initial checkpoint, then do it outside the session lock,
    //but, given we've now put the DB into InitCheckpoint, make darn
    //sure that the DB either ends up in running or error state.
    if CreateCheckpoint then
    begin
      try
        try
          Checkpoint;
        except
          on E: Exception do
          begin
            HandleJournalError(self, E.Message);
            raise;
          end;
        end;
      finally
        FSessionLock.Acquire;
        try
          if FPhase in [mdbInitCheckpoint] then
            FPhase := mdbRunning;
          //Whether final state was to running, or to error.
          FInitWait.SetEvent;
        finally
          FSessionLock.Release;
        end;
      end;
    end;
  finally
    FJournal.SynchronizeStateChange;
  end;
end;

procedure TMemDB.HandleJournalFinished(Sender: TObject);
begin
  FSessionLock.Acquire;
  try
    Assert(FPhase = mdbClosingWaitPersist);
    FPhase := mdbClosed;
    FPersistWait.SetEvent;
  finally
    FSessionLock.Release;
  end;
  FJournal.SynchronizeStateChange;
end;

procedure TMemDB.HandleJournalError(Sender: TObject; ErrMsg:string);
var
  InitState, FinalState: TMemDBPhase;
begin
  //Can put DB into error state from almost any other state.
  FSessionLock.Acquire;
  InitState := FPhase;
  try
    Assert(FPhase in
    [ mdbInit, mdbInitCheckpoint,
      mdbRunning, mdbRunningError,
      mdbClosingWaitClients, mdbClosingWaitClientsError,
      mdbClosingWaitPersist, mdbClosedError ]);
    case FPhase of
      mdbInit: begin
        FPhase := mdbClosedError; //Journal error at init stops the journal thread.
        FLastError := ErrMsg;
      end;
      mdbInitCheckpoint, mdbRunning: begin
        FPhase := mdbRunningError; //Up and running - ish but toasted.
        FLastError := ErrMsg;
      end;
      mdbRunningError: {NOP} ;
      mdbClosingWaitClients: begin
        FPhase := mdbClosingWaitClientsError; //Toasted but closing...
        FLastError := ErrMsg;
      end;
      mdbClosingWaitClientsError,
      mdbClosingWaitPersist, // I can't really recover from a write failure
                             // at this late stage.
      mdbClosedError: {NOP} ;
    else
      Assert(false);
    end;
  finally
    FinalState := FPhase;
    FSessionLock.Release;
  end;
  if InitState <> FinalState then
    FJournal.SynchronizeStateChange;
end;

function TMemDB.GetLastError: string;
begin
  FSessionLock.Acquire;
  try
    result := FLastError;
  finally
    FSessionLock.Release;
  end;
end;

procedure TMemDB.HandleUIStateChange(Sender: TObject);
begin
  if Assigned(FOnUIStateChange) then
    FOnUIStateChange(self);
end;

procedure TMemDB.HandleJournalInitReplayTransaction(Sender: TObject; Changesets: TObject;
  Initial: boolean);
var
  API: TMemAPIDatabaseInternal;
  CStream: TStream;
begin
  API := FDatabase.Interfaced.GetAPIObject(nil, APIInternalCommitRollback)
    as TMemAPIDatabaseInternal;
  try
    Assert(Assigned(Changesets));
    Assert(Changesets is TStream);
    CStream := Changesets as TStream;
    while CStream.Position < CStream.Size do
    begin
      API.JournalReplayCycle(CStream, Initial);
      Initial := false;
    end;
  finally
    API.Free;
  end;
end;

procedure TMemDB.HandleJournalTransactionWriteFlush(Sender: TObject; Transaction: TObject);
var
  T: TMemDBTransaction;
begin
  T := Transaction as TMemDBTransaction;
  begin
    Assert(Assigned(T.FlushFinishedEvent));
    T.FlushFinishedEvent.SetEvent;
  end;
end;

//WARNING: Non-blocking checkpoint may change the row order
//permanently when iterating not by index.
//(same order as iterate with isolation >= snapshot)
function TMemDB.Checkpoint(Asynchronous: boolean): boolean;

var
  ChangesetStream: TStream;
  PseudoTid: TTransactionId;
  Ctxt: TXTransactionLocalContext;
  EntSnapshot, EntRegistered: TReffedList;
  ChkGuid: TGUID;
  ReservedFile: boolean;

begin
  Ctxt := nil;
  EntSnapshot := nil;
  EntRegistered := nil;
  ChangesetStream := nil;
  ReservedFile := false;
  CreateGuid(ChkGuid);

  FSessionLock.Acquire;
  try
    result := FPhase in [mdbInitCheckpoint, mdbRunning];
    if result then
      Inc(FCheckpointRefs);

  finally
    FSessionLock.Release;
  end;
  if result then
  begin
    if Asynchronous then
      FRWWLock.Acquire(lrSharedWrite)
    else
      FRWWLock.Acquire(lrSharedRead);
    //Totally consistent worldview - but will do list atomicity
    //inside DB classes as well.
    try
      Ctxt := TXTransactionLocalContext.Create;
      Ctxt.SetPtrs(@EntSnapshot, @EntRegistered);
      Ctxt.FAsynchronousCheckpoint := Asynchronous;
      try
        ChangesetStream := MakeStream(tsmDisk); //Most conservative stream making mode.

        PseudoTid := TTransactionId.NewTransactionID(ilSerialisable); //if no writes, should definitley be serialisable.
        try
          FDatabase.CommitLock.Acquire;
  {$IFDEF DBG_UNDER_COMMIT_LOCK}
          Inc(DbgUnderCommitLock);
  {$ENDIF}
          try
            CommitAPICheckDBErrFlagSynchronous;
            try
              //Serialisable, lrSharedRead - no changes in progress.
              FDatabase.MetaIndexLock.Acquire;
              try
                FDatabase.StartTransaction(PseudoTid, Ctxt);
              finally
                FDatabase.MetaIndexLock.Release;
              end;
              //Once started transaction have pins to clear.
              //Before started transaction, no entity lists(!).
              if Asynchronous then
              begin
      {$IFDEF DBG_UNDER_COMMIT_LOCK}
                Assert(DbgUnderCommitLock > 0);
      {$ENDIF}
                FJournal.Checkpoint(jctReserve, ChkGuid, nil);
                ReservedFile := true;
      {$IFDEF DBG_UNDER_COMMIT_LOCK}
                Assert(DbgUnderCommitLock > 0);
                Dec(DbgUnderCommitLock);
      {$ENDIF}
                FDatabase.CommitLock.Release;
              end;
              try
                FDatabase.ToScratch(PseudoTid, ChangesetStream, Ctxt);
                if Asynchronous then
                begin
                  FJournal.Checkpoint(jctCommit, ChkGuid, ChangesetStream);
                  ReservedFile := false;
                end
                else
                  FJournal.Checkpoint(jctComplete, TGUID.Empty, ChangesetStream);

                ChangesetStream := nil;
              finally
                if Asynchronous then
                begin
                  FDatabase.CommitLock.Acquire;
          {$IFDEF DBG_UNDER_COMMIT_LOCK}
                  Inc(DbgUnderCommitLock);
          {$ENDIF}
                end;
              end;
            finally
              try
                if ReservedFile then
                begin
                  FJournal.Checkpoint(jctAbort, ChkGuid, nil);
                  ReservedFile := false;
                end;

                FDatabase.MetaIndexLock.Acquire;
                try
                  FDatabase.Rollback(PseudoTid, rbpIndexRollback, [], Ctxt);
                  FDatabase.Rollback(PseudoTid, rbpMetaRollback, [], Ctxt);
                finally
                  FDatabase.MetaIndexLock.Release;
                end;
              except
                on E: Exception do
                begin
                  //Fatal if not able to rollback pins etc.
                  CommitAPISetDBErrFlag(E.Message);
                  raise;
                end;
              end;
            end;
          finally
  {$IFDEF DBG_UNDER_COMMIT_LOCK}
            Assert(DbgUnderCommitLock > 0);
            Dec(DbgUnderCommitLock);
  {$ENDIF}
            FDatabase.CommitLock.Release;
          end;
        finally
          //Rollbacks now also check whether entity lists to roll back on.
          try
            FDatabase.Rollback(PseudoTid, rbpDelayedRollback, CleardownOptSet, Ctxt);
          except
            on E: Exception do
            begin
              //Fatal if not able to rollback pins etc.
              CommitAPISetDBErrFlag(E.Message);
              raise;
            end;
          end;
        end;
      finally
        if Assigned(ChangesetStream) then
          ChangesetStream.Free;
        Ctxt.Free;
        EntSnapshot.Release;
        EntRegistered.Release;
      end;
    finally
      if Asynchronous then
        FRWWLock.Release(lrSharedWrite)
      else
        FRWWLock.Release(lrSharedRead);

      FSessionLock.Acquire;
      try
        Assert(FCheckpointRefs >= 0);
        Dec(FCheckpointRefs);
        CheckClientsDone;
      finally
        FSessionLock.Release;
      end;
    end;
  end;
end;

end.
