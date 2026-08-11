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
  MemDB2Buffered, MemDB2API, LockAbstractions, RWWLock, MemDB2BufBase, Reffed,
  ReOrderBuffer;

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

    //Journalling not under same lock as commit, need a re-order buffer
    //to make sure things are journalled in same order they are comitted.
    FROBReservation: PReservation;

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
  public
    //Public but internal use only.
    procedure HandleNewEntityUnderDBLocks(Entity: TMemDBEntity);
    procedure ROBReserve;
    procedure ROBAbort;

    procedure CommitAndFree;
    procedure RollbackAndFree;

    procedure RegisterCreatedApi(Api: TMemDBApi);
    procedure DeregisterCreatedApi(Api: TMemDBApi);
    procedure CheckNoDanglingTransactionRefs(CanRaise: boolean);

    constructor Create;
    destructor Destroy; override;

    function GetAPI: TMemAPIDatabase;

    property Mode: TMDBAccessMode read FMode;
    property Sync: TMDBSyncMode read FSync;

    property Tid: TTransactionId read FTid;
    property FlushFinishedEvent: TEvent read FFlushFinishedEvent;
    property ParentSession: TMemDBSession read FSession;
    property Changeset: TStream read FChangeset;
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
    FROB: TReorderBuffer;
    FDrainLock: TCriticalSection;
    FRunningTeardown: boolean;
    FJournal: TMemDbDefaultJournal;
    FDatabase: TMemDbDatabasePersistent;
    FPhase: TMemDBPhase;
    FLastError: string;
    FOnUIStateChange: TNotifyEvent;
  protected
    procedure RemoveSession(Session: TMemDBSession);
    procedure RemoveTransaction(Transaction: TMemDBTransaction; Commit: boolean);

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
    procedure DrainReorderBuffer;

    procedure HandleNewEntityUnderDBLocks(Entity: TMemDBEntity; OwnerTx: TMemDBTransaction);
    function ROBReserve: PReservation;
    procedure ROBAbort(var Reservation: PReservation);
  public

    constructor Create;
    destructor Destroy; override;
    function InitDB(RootLocation: string;
                    JournalType: TMemDBJournalType = jtV2;
                    Async: boolean = false): boolean;
    procedure StopDB(Force: boolean = false);
    function Checkpoint: boolean;
    function StartSession: TMemDBSession;

    property DBState: TMemDBPhase read FPhase;
    property OnUIStateChange: TNotifyEvent read FOnUIStateChange write FOnUIStateChange;
  end;

implementation

uses IoUtils, Types;

const
  S_DB_CLOSING_OR_NOT_OPEN = 'DB closing, or not open: ';
  S_DB_SESSION_NOT_FOUND = 'Session not found';
  S_DB_HAS_SESSIONS = 'DB has open sessions.';
  S_DB_SESSION_HAS_TRANSACTIONS = 'Session has open transactions';
  S_DB_TRANSACTION_NOT_FOUND = 'Transaction not found';
  S_COMMIT_ROLLBACK_IN_PROGRESS = 'Commit or rollback for this transaction already in progress.';
  S_COMMIT_OR_ROLLBACK_BEFORE_FREE = 'Transactions should be committed or rolled back before destroying. Are you calling an inherited ''Free'' function? ';
  S_ERRORPHASE_ERROR = 'Can''t start transaction, error state: ';
  S_ERRORPHASE_COMMIT_ERROR = 'Can''t stop transaction, error state: ';
  S_BADPHASE_ERROR = 'Can''t start transaction, DB not running or closing.';
  S_REINIT_DIFF_LOCATION = 'Database loaded at different location, unload first.';
  S_UNEXPECTED_ROLLBACK_FAILED = 'Unexpected rollback failed, state uncertain: ';
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
      result := PSnapshotList.CloneByRef
    else
      result := PRegisteredList.CloneByRef;
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

procedure TMemDBTransaction.HandleNewEntityUnderDBLocks(Entity: TMemDBEntity);
begin
  FDB.HandleNewEntityUnderDBLocks(Entity, self);
end;

procedure TMemDBTransaction.ROBReserve;
begin
  Assert(not Assigned(FROBReservation));
  FROBReservation := FDB.ROBReserve;
end;

procedure TMemDBTransaction.ROBAbort;
begin
  FDB.ROBAbort(FROBReservation);
end;

function TMemDBTransaction.GetAPI: TMemAPIDatabase;
begin
  Assert(not FCommitedOrRolledBack);
  Assert(FCommitRollbackInProgress = tiptNone);
  result := FDB.FDatabase.Interfaced.GetAPIObject(self, APIDatabase) as TMemAPIDatabase;
end;

procedure TMemDBTransaction.CommitAndFree;
begin
  FDB.RemoveTransaction(self, true);
end;

procedure TMemDBTransaction.RollbackAndFree;
begin
  FDB.RemoveTransaction(self, false);
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
  Assert(not Assigned(FROBReservation));
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
  if result and (FPhase = mdbClosingWaitClients) then
    FClientWait.SetEvent;
end;

procedure TMemDB.HandleNewEntityUnderDBLocks(Entity: TMemDBEntity; OwnerTx: TMemDBTransaction);
var
  IdleTxionList: TList;
  i: integer;
  Tx: TMemDBTransaction;
begin
  //Oh this is nasty.

  //Holding the commit and meta index locks ensures that Txions we reference
  //cannot start accessing entity lists as part of commit or rollback cycle,
  //if they transition from no commit rollbacks in progress to commit
  //rollbacks in progress.

  //However, for txions which have a commit/rollback in progress, we should not
  //change the entity lists for them, because they access those lists
  //(rollback / cleardown cases) after dropping commit/metaindex locks,
  //and we can't safely change the entity lists at that time.

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

function TMemDB.ROBReserve: PReservation;
begin
  result := FROB.Reserve;
end;

procedure TMemDB.ROBAbort(var Reservation: PReservation);
begin
  FROB.Abort(Reservation);
end;

procedure TMemDB.RemoveTransaction(Transaction: TMemDBTransaction; Commit: boolean);
var
  idx: integer;
  WaitJournalDone: boolean;
  AsyncError: boolean;
  CommitStream: TStream;

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
    Transaction.FCommitRollbackInProgress := tiptCommitRollback;
    AsyncError := FPhase = mdbError;
  finally
    FSessionLock.Release;
  end;

  //DB hosed is DB hosed.
  if AsyncError then
  begin
    if Commit then
      raise EMemDBException.Create(S_ERRORPHASE_COMMIT_ERROR + FLastError)
    else
    //Allow DB force close / rollback cases to quietly clear up as much as they can.
      goto txion_remove;
  end;

  try
    //Things get referenced from the API objects ...
    //If it's a commit, then I'll check you've cleared those down.
    //If it's a rollback then I won't, and will clean up for you.
    Transaction.CheckNoDanglingTransactionRefs(Commit);

    if not (Transaction.FMode in [amReadWriteShared, amWriteExclusive]) then
    begin
      if FDatabase.AnyChangesForTid(Transaction.Tid, Transaction.FLocalContext) then
        raise EMemDBInternalException.Create(S_READONLY_HAS_CHANGED_DATA);
      //Internally read-only txions go through the rollback cycle to clear pins/refs etc.
      Commit := false;
    end;

    //Some pathalogical error cases, will have allocated txion,
    //and added to list, but not got transaction commit/rollback API object.
    if Assigned(Transaction.FCRInterface) then
    begin
      if Commit then
      begin
        if FJournal.NeedFlowControl then
          Transaction.FSync := amFlushBuffers;

        //This may raise exceptions if pre-commit checks fail.
        //Arranges streams in transaction for final journalling;
        CommitStream := Transaction.FCRInterface.UserCommitCycle;

        //Exceptions after here less likely / not expected.
        Assert(not Assigned(Transaction.Changeset));
        Transaction.FChangeset := CommitStream;
        //And only set up the wait if no prior exception.
        WaitJournalDone := Commit and (Transaction.FSync = amFlushBuffers);
        if WaitJournalDone then
        begin
          Assert(not Assigned(Transaction.FlushFinishedEvent));
          Transaction.FFlushFinishedEvent := TEvent.Create(nil, true, false, '');
        end;

        //Now remove internal API object for transaction:
        //No longer needed, db restart case frees underlying DB obejcts.

        //Also cleardown all refs on internal entities; leaving those refs
        //for a while would result in old tables etc not being cleared,
        //under continuous overlapping transactions.
        Transaction.FCRInterface.Free;
        Transaction.FCRInterface := nil;
        Transaction.FLocalContext.Free;
        Transaction.FLocalContext := nil;
        Transaction.FEntitiesSnapshot.Release;
        Transaction.FEntitiesSnapshot := nil;
        Transaction.FEntitiesRegistered.Release;
        Transaction.FEntitiesRegistered := nil;

        //And now journal it.
        Transaction.AddRef;
        FROB.Commit(Transaction.FROBReservation, Transaction);
      end
      else
      begin
        try
          //Very likely that transaction does not have a
          //re-order buffer reservation, but just in case.
          //Additionally, we need to drain the ROB, in case
          //another txion is waiting for writeback...
          FROB.Abort(Transaction.FROBReservation);

          //Since everything is multi-buffered, barring out of memory
          //exceptions we do not expect this to fail.
          Transaction.FCRInterface.UserRollbackCycle;
          //We really need rollback to work to clear pins and refcounts.
          //If it doesn't then something's very very broken.

        except
          on E: Exception do
          begin
            FSessionLock.Acquire;
            try
              Assert(FPhase in [mdbRunning, mdbClosingWaitClients, mdbError]);
              if FPhase = mdbRunning then
              begin
                FPhase := mdbError;
                FLastError := S_UNEXPECTED_ROLLBACK_FAILED + E.Message;
              end;
            finally
              FSessionLock.Release;
            end;
            //We will swallow the exception having put the DB into the error
            //state. Unfortunately pins and refcounts will be hosed, but at least we
            //drop the R/W lock.
          end;
        end;
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
  //If good commit, drop the lock.
  //If failed rollback, DB in error, exception swallowed, and drop the R/W lock.

  Assert(Transaction.FMode in [amReadShared, amWriteExclusive, amReadWriteShared]);
  FRWWLock.Release(DBAccessModeToLockReason(Transaction.FMode));

  //Before, not after waiting for flush finished...
  DrainReorderBuffer;

  if WaitJournalDone then
    Transaction.FlushFinishedEvent.WaitFor(INFINITE);

txion_remove:

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

procedure TMemDB.DrainReorderBuffer;
var
  Obj: TObject;
begin
  FDrainLock.Acquire;
  try
    Obj := FROB.Drain;
    while Assigned(Obj) do
    begin
      if Obj is TMemDBTransaction then
        FJournal.TransactionCommitChangeset(Obj)
      else if Obj is TStream then
        FJournal.Checkpoint(Obj)
      else
        Assert(false);
      Obj:= FROB.Drain;
    end;
  finally
    FDrainLock.Release;
  end;
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
      if idx < 0 then
        raise EMemDBException.Create(S_DB_SESSION_NOT_FOUND);
      if FPhase <> mdbRunning then
      begin
        //DB hosed is DB hosed.
        if FPhase = mdbError then
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
      Session.FSessionTransactions.Add(result);
      FTransactionList.Add(result);
    finally
      FSessionLock.Release;
    end;
    Assert(Mode in [amReadShared, amWriteExclusive, amReadWriteShared]);
    FRWWLock.Acquire(DBAccessModeToLockReason(Mode));

    result.FCRInterface := FDatabase.Interfaced.GetAPIObject(result, APIInternalCommitRollback)
      as TMemAPIDatabaseInternal;

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
  Tr: TMemDBTransaction;
begin
  FSessionLock.Acquire;
  try
    idx := FSessionList.IndexOf(Session);
    if not(idx >= 0) then
      raise EMemDBException.Create(S_DB_SESSION_NOT_FOUND);

    try
{$IF COMPLAIN_LEAKED_TXIONS}
      if Session.FSessionTransactions.Count > 0 then
        raise EMemDBException.Create(S_DB_SESSION_HAS_TRANSACTIONS);
{$ELSE}
      //Quietly clear the transactions down for this session.
      //Can do this recursively acquiring the session lock.
      //Not pretty, but works, and is safe.
      while Session.FSessionTransactions.Count > 0 do
      begin
        Tr := TMemDBTransaction(Session.FSessionTransactions.Items[0]);
        Tr.RollbackAndFree;
      end;
{$ENDIF}
    finally
      FSessionList.Delete(idx);
      CheckClientsDone;
    end;
  finally
    FSessionLock.Release;
  end;
end;

function TMemDB.StartSession: TMemDBSession;
begin
  result := nil;
  FSessionLock.Acquire;
  try
    if not(FPhase in [mdbInit, mdbRunning]) then
      raise EMemDBException.Create(S_DB_CLOSING_OR_NOT_OPEN + FLastError);
    if FPhase = mdbInit then
    begin
      FSessionLock.Release;
      try
        FInitWait.WaitFor(INFINITE);
      finally
        FSessionLock.Acquire;
      end;
      if FPhase <> mdbRunning then
        raise EMemDBException.Create(S_DB_CLOSING_OR_NOT_OPEN + FLastError);
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
  FROB := TReorderBuffer.Create;
  FDrainLock := TCriticalSection.Create;
  FDatabase := TMemDBDatabase.Create;
  (FDatabase as TMemDBDatabase).Init(self);
  inherited;
end;


destructor TMemDB.Destroy;
begin
  StopDB(true);
  Assert(FPhase in [mdbNull, mdbClosed, mdbError]);
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
  FROB.Free;
  FDrainLock.Free;
  inherited;
end;

function TMemDB.InitDB(RootLocation: string;
                       JournalType: TMemDBJournalType;
                       Async: boolean): boolean;

  procedure StartupActions;
  begin
    AppendTrailingDirSlash(RootLocation);
    FInitWait.ResetEvent;
    FClientWait.ResetEvent;
    FPersistWait.ResetEvent;
    FJournal.BaseDirectory := RootLocation;
    FJournal.JournalType := JournalType;
    FJournal.Initialise;
    FPhase := mdbInit;
  end;

begin
  FSessionLock.Acquire;
  try
    if ASync then
    begin
      result := false;
      case FPhase of
        mdbNull, mdbClosed: StartupActions;
        mdbInit, mdbRunning: result := true;
        mdbClosingWaitClients, mdbClosingWaitPersist, mdbError: ;
      else
        Assert(false);
      end;
    end
    else
    begin
      while not(FPhase in [mdbRunning, mdbError]) do
      begin
        case FPhase of
          // Can't open when in error state, call close to reset from failed
          // load, (or other persist ops), and try again from state closed.
          mdbNull, mdbClosed: StartupActions;
          mdbInit:
            begin
              FSessionLock.Release;
              try
                FInitWait.WaitFor(INFINITE);
              finally
                FSessionLock.Acquire;
                Assert(FPhase >= mdbRunning);
              end;
            end;
          mdbClosingWaitClients:
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
      result := FPhase = mdbRunning;
    end;
    if result then
    begin
      if Length (FJournal.BaseDirectory) > 0 then
      begin
        if (RootLocation <> FJournal.BaseDirectory) or
          (JournalType <> FJournal.JournalType) then
          raise EMemDBException.Create(S_REINIT_DIFF_LOCATION);
      end;
    end;
  finally
    FSessionLock.Release;
  end;
end;

procedure TMemDB.StopDBLocked;
var
  DBTmp: TMemDbDatabasePersistent;
begin
  while not(FPhase in [mdbNull, mdbClosed, mdbError]) do
  begin
    case FPhase of
      mdbInit:
        begin
          FSessionLock.Release;
          try
            FInitWait.WaitFor(INFINITE);
          finally
            FSessionLock.Acquire;
            Assert(FPhase >= mdbRunning);
          end;
        end;
      mdbRunning:
        begin
          if CheckClientsDone then
          begin
            FJournal.Finish;
            FPhase := mdbClosingWaitPersist
          end
          else
            FPhase := mdbClosingWaitClients;
        end;
      mdbClosingWaitClients:
        begin
          FSessionLock.Release;
          try
            FClientWait.WaitFor(INFINITE);
          finally
            FSessionLock.Acquire;
            Assert(FPhase >= mdbClosingWaitClients);
          end;
          if FPhase = mdbClosingWaitClients then
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
          DBTmp.Free;

          try
            FPersistWait.WaitFor(INFINITE);
          finally
            FSessionLock.Acquire;
            Assert(FPhase >= mdbClosingWaitPersist);
            if FPhase = mdbClosingWaitPersist then
              FPhase := mdbClosed;
          end;
        end;
      mdbError:
        begin
          FPhase := mdbClosed;
        end
    else
      Assert(false);
    end;
  end;
end;

procedure TMemDB.StopDB(Force: boolean);
var
  Session: TMemDBSession;
begin
  FSessionLock.Acquire;
  try
    if not Force then
    begin
{$IF COMPLAIN_LEAKED_SESSIONS}
      if Session.FSessionTransactions.Count > 0 then
        raise EMemDBException.Create(S_DB_HAS_SESSIONS);
{$ENDIF}
      //Otherwise will wait for sessions / txions to complete.
    end
    else
    begin
      while FSessionList.Count > 0 do
      begin
        Session := self.FSessionList.Items[0];
        Session.Free;
      end;
    end;
    try
      if not FRunningTeardown then
      begin
        //User might well try stop, followed by force stop...
        FRunningTeardown := true;
        StopDBLocked;
      end;
    finally
      FRunningTeardown := false;
    end;
  finally
    FSessionLock.Release;
  end;
end;

procedure TMemDB.HandleJournalInitialized(Sender: TObject; OK: boolean; AnyFiles: boolean;
  CreateCheckpoint: boolean; ErrMsg:string);
begin
  FSessionLock.Acquire;
  try
    Assert(FPhase = mdbInit);
    if OK then
      FPhase := mdbRunning
    else
    begin
      FPhase := mdbError;
      FLastError := ErrMsg;
    end;
    FInitWait.SetEvent;
  finally
    FSessionLock.Release;
  end;
  //Could make this atomic with reservations, but then we'd have
  //to guarantee that we'd be the first to grab the commit lock,
  //which we can't easily, so better to leave a small window, and
  //reservations ensure txions journal in the same order they took
  //the commit lock.

  if OK and CreateCheckpoint then
    Checkpoint;
  FJournal.SynchronizeStateChange;
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
begin
  FSessionLock.Acquire;
  try
    //Assert(FPhase = mdbRunning);
    FPhase := mdbError;
    //Unblocking of threads etc handled by user destruction of DB.
    FLastError := ErrMsg;
  finally
    FSessionLock.Release;
  end;
  FJournal.SynchronizeStateChange;
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

function TMemDB.Checkpoint: boolean;
var
  ChangesetStream: TStream;
  TmpName: string;
  PseudoTid: TTransactionId;
  Ctxt: TXTransactionLocalContext;
  EntSnapshot, EntRegistered: TReffedList;
  Reservation: PReservation;

begin
  EntSnapshot := nil;
  EntRegistered := nil;
  Reservation := nil;
  FSessionLock.Acquire;
  try
    result := FPhase = mdbRunning;
    if result then
      Inc(FCheckpointRefs);
  finally
    FSessionLock.Release;
  end;
  if result then
  begin
    FRWWLock.Acquire(lrSharedRead);
    //Totally consistent worldview - but will do list atomicity
    //inside DB classes as well.
    try
      Ctxt := TXTransactionLocalContext.Create;
      Ctxt.PSnapshotList := @EntSnapshot;
      Ctxt.PRegisteredList := @EntRegistered;
      try
        TmpName := TPath.GetTempFileName();
        ChangesetStream := TMemDBTempFileStream.Create(TmpName);
        PseudoTid := TTransactionId.NewTransactionID(ilSerialisable); //if no writes, should definitley be serialisable.
        try
          FDatabase.CommitLock.Acquire; //This if we don't acquire at lrSharedRead for ever.
{$IFDEF DBG_UNDER_COMMIT_LOCK}
          DbgUnderCommitLock := true;
{$ENDIF}
          try
            Reservation := FROB.Reserve;
            //Serialisable so StartTransaction under commit lock.
            FDatabase.StartTransaction(PseudoTid, Ctxt);
            try
              FDatabase.ToScratch(PseudoTid, ChangesetStream, Ctxt);
            finally
              //We probably shouldn't need to to this at all, but keep
              //it here for safety.
              FDatabase.MetaIndexLock.Acquire;
              try
                FDatabase.Rollback(PseudoTid, rbpIndexRollback, [], Ctxt);
                FDatabase.Rollback(PseudoTid, rbpMetaRollback, [], Ctxt);
              finally
                FDatabase.MetaIndexLock.Release;
              end;
            end;
          finally
{$IFDEF DBG_UNDER_COMMIT_LOCK}
            DbgUnderCommitLock := false;
{$ENDIF}
            FDatabase.CommitLock.Release;
            FDatabase.Rollback(PseudoTid, rbpDelayedRollback, CleardownOptSet, Ctxt);
          end;
        except
          FROB.Abort(Reservation);
          ChangesetStream.Free;
          DeleteFile(TmpName);
          raise;
        end;
        FROB.Commit(Reservation, ChangesetStream);

      finally
        Ctxt.Free;
        EntSnapshot.Release;
        EntRegistered.Release;
      end;
    finally
      FRWWLock.Release(lrSharedRead);

      DrainReorderBuffer;

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
