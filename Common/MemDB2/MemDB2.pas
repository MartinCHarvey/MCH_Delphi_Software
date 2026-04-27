unit MemDB2;
{

Copyright � 2020 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the �Software�), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
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
  TMemDB = class;
  TMemDBSession = class;

  TInProgressType = (tiptNone, tiptCommitRollback);

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
    //Changeset states.
    FChangeset: TStream;
    FCommitRollbackInProgress: TInProgressType;
    FCommitedOrRolledBack: boolean;
    FFlushFinishedEvent: TEvent;
    FApiObjects: TList;
    FApiObjectLock: TCriticalSection;
  protected
  public
    procedure CommitAndFree;
    procedure RollbackAndFree;

    //TODO - These should not be client accessible, used internally.
    //Find a way to sort the visibility on this.
    procedure RegisterCreatedApi(Api: TMemDBApi);
    procedure DeregisterCreatedApi(Api: TMemDBApi);
    procedure CheckNoDanglingTransactionRefs(CanRaise: boolean);

    constructor Create;
    destructor Destroy; override;

    function GetAPI: TMemAPIDatabase;

    property Mode: TMDBAccessMode read FMode;
    property Sync: TMDBSyncMode read FSync;
    //TODO - Need selector at this level, or gets generated
    //more in MemDBBuffered?
    property Tid: TTransactionId read FTid;
    property FlushFinishedEvent: TEvent read FFlushFinishedEvent;
    property ParentSession: TMemDBSession read FSession;
    property Changeset: TStream read FChangeset;
    property Session: TMemDBSession read FSession;
  end;

  //TODO - Might like to think about whether we actually need a session
  //at all, but just hold a list of transactions.
  //at the moment, no-per-user privilege checking in DB, and only
  //one transaction per session.

  //TODO - Keep an eye on the size / number of session / transaction lists and
  //whether they are sorted or not - for further profiling.
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
  public
    constructor Create;
    destructor Destroy; override;
    function InitDB(RootLocation: string;
                    JournalType: TMemDBJournalType = jtV2;
                    Async: boolean = false): boolean;
    procedure StopDB;
    function Checkpoint: boolean;
    function StartSession: TMemDBSession;

    property DBState: TMemDBPhase read FPhase;
    property OnUIStateChange: TNotifyEvent read FOnUIStateChange write FOnUIStateChange;
  end;

implementation

uses IoUtils, Types
{$IFDEF DEBUG_DATABASE}
 , GlobalLog
{$ENDIF}
  ;

const
  S_DB_CLOSING_OR_NOT_OPEN = 'DB closing, or not open: ';
  S_DB_SESSION_NOT_FOUND = 'Session not found';
  S_DB_SESSION_HAS_TRANSACTIONS = 'Session has open transactions';
  S_DB_TRANSACTION_NOT_FOUND = 'Transaction not found';
  S_COMMIT_ROLLBACK_IN_PROGRESS = 'Commit, rollback or mini-op for this transaction already in progress.';
  S_COMMIT_OR_ROLLBACK_BEFORE_FREE = 'Transactions should be committed or rolled back before destroying. Are you calling an inherited ''Free'' function? ';
  S_ERRORPHASE_ERROR = 'Can''t start transaction, error state: ';
  S_ERRORPHASE_COMMIT_ERROR = 'Can''t stop transaction, error state: ';
  S_BADPHASE_ERROR = 'Can''t start transaction, DB not running or closing.';
  S_REINIT_DIFF_LOCATION = 'Database loaded at different location, unload first.';
  S_UNEXPECTED_ROLLBACK_FAILED = 'Unexpected rollback failed, state uncertain: ';
  S_READONLY_HAS_CHANGED_DATA = 'Read-only transaction has changed data!';
  S_TRANSACTION_HAS_API_OBJECTS = 'Transaction has associated API objects. You should have freed them before commit/rollback';

  { TMemDBTransaction }

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
  //No Add-Ref, initial refcount 1.
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
  TmpList: TList;
  i: Integer;
begin
  if CanRaise then
  begin
    FApiObjectLock.Acquire;
    try
      if FApiObjects.Count > 0 then
        raise EMemDBAPIException.Create(S_TRANSACTION_HAS_API_OBJECTS);
    finally
      FApiObjectLock.Release;
    end;
  end
  else
  begin
    //I am going to quietly clear down API objects for this transaction.
    //This allows us in error / shutdown cases to quietly rollback all txions,
    //remove API's, delete sessions, and have a chance at refcounts returning to zero.

    //TODO - Further cleardown cases aroundabout session/db destruction.
    //Depends how carefully client code handles its session and tranactions.
    //Current assumption is it keeps track of sessions and txions.
    TmpList := TList.Create;
    try
      FApiObjectLock.Acquire;
      try
        TmpList.Assign(FApiObjects, laCopy);
      finally
        FApiObjectLock.Release;
      end;
      for i := 0 to Pred(TmpList.Count) do
        TMemDbAPI(TmpList.Items[i]).Free;
    finally
      TmpList.Free;
    end;
  end;
end;

destructor TMemDBTransaction.Destroy;
begin
  if not FCommitedOrRolledBack then
    raise EMemDBAPIException.Create(S_COMMIT_OR_ROLLBACK_BEFORE_FREE);
  FChangeset.Free;
  Assert(FAPIObjects.Count = 0);
  FApiObjects.Free;
  FApiObjectLock.Free;
  inherited;
end;

{ TMemDBSession }

function TMemDBSession.GetTempStorageMode: TTempStorageMode;
begin
  FDB.FSessionLock.Acquire;
  result := FTempStorageMode;
  FDB.FSessionLock.Release;
end;

procedure TMemDBSession.SetTempStorageMode(NewMode: TTempStorageMode);
begin
  FDB.FSessionLock.Acquire;
  FTempStorageMode := NewMode;
  FDB.FSessionLock.Release;
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
  FDB.RemoveSession(self);
  Assert(FSessionTransactions.Count = 0);
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

procedure TMemDB.RemoveTransaction(Transaction: TMemDBTransaction; Commit: boolean);
var
  idx: integer;
  WaitJournalDone: boolean;
  API: TMemAPIDatabaseInternal;
  AsyncError: boolean;
  CommitStream: TStream;
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
    raise EMemDBException.Create(S_ERRORPHASE_COMMIT_ERROR + FLastError);

  try
    //Things get referenced from the API objects ...
    //If it's a commit, then I'll check you've cleared those down.
    //If it's a rollback then I won't, and will clean up for you.
    Transaction.CheckNoDanglingTransactionRefs(Commit);

    if not (Transaction.FMode in [amWriteShared, amWriteExclusive]) then
    begin
      if FDatabase.AnyChangesForTid(Transaction.Tid) then
        raise EMemDBInternalException.Create(S_READONLY_HAS_CHANGED_DATA);
      //Internally read-only txions go through the rollback cycle to clear pins/refs etc.
      Commit := false;
    end;

    //Database interfaced not refcounted, but could move this inside
    //"not async error".
    API := FDatabase.Interfaced.GetAPIObject(Transaction, APIInternalCommitRollback)
      as TMemAPIDatabaseInternal;
    try
      if Commit then
      begin
        if FJournal.NeedFlowControl then
          Transaction.FSync := amFlushBuffers;

        //This may raise exceptions if pre-commit checks fail.
        //Arranges streams in transaction for final journalling;
        CommitStream := API.UserCommitCycleV3;

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
        //And now journal it.
        Transaction.AddRef;
        FJournal.TransactionCommitChangeset(Transaction);
      end
      else
      begin
        try
          //Since everything is multi-buffered, barring out of memory
          //exceptions we do not expect this to fail.
          API.UserRollbackCycleV3;
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
    finally
      API.Free;
    end;
  finally
    FSessionLock.Acquire;
    try
      Transaction.FCommitRollbackInProgress := tiptNone;
    finally
      FSessionLock.Release;
    end;
  end;
  //If failed commit, hold the R/W lock.
  //If good commit, drop the lock.
  //If failed rollback, DB in error, exception swallowed, and drop the R/W lock.

  Assert(Transaction.FMode in [amReadShared, amWriteExclusive, amWriteShared]);
  FRWWLock.Release(DBAccessModeToLockReason(Transaction.FMode));

  if WaitJournalDone then
    Transaction.FlushFinishedEvent.WaitFor(INFINITE);
  FSessionLock.Acquire;
  try
    idx := Transaction.FSession.FSessionTransactions.IndexOf(Transaction);
    Assert(idx >= 0);
    Transaction.FCommitedOrRolledBack := true;
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
  API: TMemAPIDatabaseInternal;
begin
  result := nil;
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
    result := TMemDBTransaction.Create;
    result.FDB := self;
    result.FSession := Session;
    result.FMode := Mode;
    result.FSync := Sync;
    result.FTid := TTransactionId.NewTransactionID(Iso);
    Session.FSessionTransactions.Add(result);
    FTransactionList.Add(result);
  finally
    FSessionLock.Release;
  end;
  Assert(Mode in [amReadShared, amWriteExclusive, amWriteShared]);
  FRWWLock.Acquire(DBAccessModeToLockReason(Mode));

  API := FDatabase.Interfaced.GetAPIObject(result, APIInternalCommitRollback)
    as TMemAPIDatabaseInternal;
  try
    API.TransactionStartCycle;
  finally
    API.Free;
  end;
end;

procedure TMemDB.RemoveSession(Session: TMemDBSession);
var
  idx: integer;
begin
  FSessionLock.Acquire;
  try
    idx := FSessionList.IndexOf(Session);
    if not(idx >= 0) then
      raise EMemDBException.Create(S_DB_SESSION_NOT_FOUND);
    //Allow the session removal if valid session even if dangling transactions,
    //However, throw back into the calling thread.
    //This to allow a cleanup-case where things have got badly out of sync
    //or threads blocked/hung.
    //It's a pretty nasty error case however you do it.

    //TODO - Think a bit more here about force clear-down of txions,
    //allowing us to get refcounts back to zero.
    //StopDB case?
    try
      if Session.FSessionTransactions.Count > 0 then
        raise EMemDBException.Create(S_DB_SESSION_HAS_TRANSACTIONS);
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
  inherited;
end;


destructor TMemDB.Destroy;
begin
  //TODO - Stopping DB when in phase error, and can't clear everything down.
  StopDB;
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
          //All client ops basically finished, waiting for journal to flush.
          //Now would be a good time to free up our actual data, so
          //restarts start with a fresh copy of TMemDBPersistent.
          DBTmp := FDatabase;
          FDatabase := TMemDBDatabase.Create;
          FSessionLock.Release;

          //TODO TODO - Check this reset of DB state is the same
          //as it was in MemDB1.
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

procedure TMemDB.StopDB;
begin
  FSessionLock.Acquire;
  try
    StopDBLocked;
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
  //TODO - There is a race here, ensuring that the checkpoint xaction
  //is the very first xaction written to disk when initializing.

  {ref BUG_MCH_1_2_2025 }
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
    Assert(FPhase = mdbRunning);
    FPhase := mdbError;
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
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, DateTimeToStr(Now) +  ' Journal replay transaction Start');
{$ENDIF}
  API := FDatabase.Interfaced.GetAPIObject(nil, APIInternalCommitRollback)
    as TMemAPIDatabaseInternal;
  try
    Assert(Assigned(Changesets));
    Assert(Changesets is TStream);
    CStream := Changesets as TStream;
    while CStream.Position < CStream.Size do
    begin
      API.JournalReplayCycleV3(CStream, Initial);
      Initial := false;
    end;
  finally
    API.Free;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, DateTimeToStr(Now) +  ' Journal replay transaction Done');
{$ENDIF}
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
begin
  FSessionLock.Acquire;
  try
    result := FPhase = mdbRunning;
    if result then
      Inc(FCheckpointRefs);
  finally
    FSessionLock.Release;
  end;
  //TODO - There is a race here, ensuring that the checkpoint xaction
  //is the very first xaction written to disk when initializing.

  {ref BUG_MCH_1_2_2025 }
  if result then
  begin
    FRWWLock.Acquire(lrSharedRead);
    //Totally consistent worldview - but will do list atomicity
    //inside DB classes as well.
    try
      TmpName := TPath.GetTempFileName();
      ChangesetStream := TMemDBTempFileStream.Create(TmpName);
      PseudoTid := TTransactionId.NewTransactionID(ilSerialisable); //if no writes, should definitley be serialisable.
      try
        FDatabase.StartTransaction(PseudoTid);
        FDatabase.CommitLock.Acquire; //This if we don't acquire at lrSharedRead for ever.
        try
          try
            FDatabase.ToScratch(PseudoTid, ChangesetStream);
          finally
            //We probably shouldn't need to to this at all, but keep
            //it here for safety.
            FDatabase.MetaIndexLock.Acquire;
            try
              FDatabase.Rollback(PseudoTid, rbpIndexRollback);
              FDatabase.Rollback(PseudoTid, rbpMetaRollback);
            finally
              FDatabase.MetaIndexLock.Release;
            end;
          end;
        finally
          FDatabase.CommitLock.Release;
          FDatabase.Rollback(PseudoTid, rbpDelayedRollback);
        end;
      except
        on E: Exception do
        begin
          ChangesetStream.Free;
          DeleteFile(TmpName);
          raise;
        end;
      end;
      FJournal.Checkpoint(ChangesetStream);
    finally
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
