unit MemDB;
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

{ In memory database.
  Top level synchronization, session and transaction handling.
  Ephemeral session-based data. }

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SysUtils, SyncObjs, Classes, MemDBMisc, MemDBStreamable, MemDBJournal,
  MemDBBuffered, MemDBAPI;

type
  TMemDB = class;
  TMemDBSession = class;

  TInProgressType = (tiptNone, tiptCommitRollback, tiptMiniOp);

{$IFDEF USE_TRACKABLES}
  TMemDBTransaction = class(TTrackable)
{$ELSE}
  TMemDBTransaction = class
{$ENDIF}
  private
    //Links to datastructures.
    FDB: TMemDB;
    FSession: TMemDBSession;
    //Local mode fields.
    FMode: TMDBAccessMode;
    FSync: TMDBSyncMode;
    FIsolation: TMDBIsolationLevel;
    //Changeset states.
    FMiniChangesets: TList;
    FFinalStreams: TList;
    //If A/B changes made by this transaction do not have an inverse.
    FBufChangesOneWay: boolean;
    //Local synchronization.
    FCommitRollbackInProgress: TInProgressType;
    FCommitedOrRolledBack: boolean;
    FFlushFinishedEvent: TEvent;
    FInterlockedRefCount: integer;
  protected
  public
    procedure AddRef;
    procedure CommitAndFree;
    procedure RollbackAndFree;
    procedure DecRefConditionalFree;
    constructor Create;
    destructor Destroy; override;

    function GetAPI: TMemAPIDatabase;

    property Mode: TMDBAccessMode read FMode;
    property Sync: TMDBSyncMode read FSync;
    property Isolation: TMDBIsolationLevel read FIsolation;
    property FlushFinishedEvent: TEvent read FFlushFinishedEvent;
    property ParentSession: TMemDBSession read FSession;
    //List of MiniSets, each miniset has fwd and inverse stream.
    property MiniChangesets: TList read FMiniChangesets;
    //List of streams, 1st and last are tokens, rest are changesets
    property FinalStreams: TList read FFinalStreams;
    property BufChangesOneWay: boolean read FBufChangesOneWay write FBufChangesOneWay;
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
    FTransaction: TMemDBTransaction;
    FTempStorageMode: TTempStorageMode;
  protected
    function GetTempStorageMode: TTempStorageMode;
    procedure SetTempStorageMode(NewMode: TTempStorageMode);
  public
    function StartTransaction(Mode: TMDBAccessMode;
                              Sync: TMDBSyncMode = amLazyWrite;
                              Iso: TMDBIsolationLevel = ilDirtyRead): TMemDBTransaction;

    procedure StartMiniOp(Transaction: TMemDBTransaction);
    procedure FinishMiniOp(Transaction: TMemDBTransaction);

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
    FRWLock: TMultiReadExclusiveWriteSynchronizer;
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
    procedure StartMiniOp(Transaction: TMemDBTransaction);
    procedure FinishMiniOp(Transaction: TMemDBTransaction);

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
    function GetDBStats: TMemDBStats;
    function Checkpoint: boolean;
    function StartSession: TMemDBSession;

    property DBState: TMemDBPhase read FPhase;
    property OnUIStateChange: TNotifyEvent read FOnUIStateChange write FOnUIStateChange;
  end;

implementation

uses IoUtils
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
  S_BADPHASE_ERROR = 'Can''t start transaction, DB not running or closing.';
  S_REINIT_DIFF_LOCATION = 'Database loaded at different location, unload first.';
  S_MULTI_ROLLBACK_FAILED = 'Multi-changeset rollback failed, state uncertain: ';
  S_MINIOP_TRANSACTION_FINISHED = 'Mini-commit/rollback: cannot. Transaction already committed or rolled back';
  S_MINIOP_CONCURRENT = 'Mini-commit/rollback: cannot. Other mini (or full size) commit/rollbacks in progress';

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

procedure TMemDbTransaction.AddRef;
begin
  TInterlocked.Increment(FInterlockedRefCount);
end;

procedure TMemDbTransaction.DecRefConditionalFree;
begin
  if TInterlocked.Decrement(FInterlockedRefCount) > 0 then
    exit; //Don't destroy it yet
  Free;
end;

constructor TMemDBTransaction.Create;
begin
  inherited;
  FMiniChangesets := TList.Create;
  FFinalStreams := TList.Create;
  AddRef;
end;

destructor TMemDBTransaction.Destroy;
var
  i: integer;
  O: Tobject;
begin
  if not FCommitedOrRolledBack then
    raise EMemDBAPIException.Create(S_COMMIT_OR_ROLLBACK_BEFORE_FREE);
  //Generally will do nothing if previous commit or rollback was successful
  for i := 0 to Pred(FMiniChangesets.Count) do
  begin
    O := FMiniChangesets.Items[i];
    O.Free;
  end;
  FMiniChangesets.Free;
  for i := 0 to Pred(FFinalStreams.Count) do
  begin
    O := FFinalStreams.Items[i];
    O.Free;
  end;
  FFinalStreams.Free;
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

procedure TMemDBSession.StartMiniOp(Transaction: TMemDBTransaction);
begin
  FDB.StartMiniOp(Transaction);
end;

procedure TMemDBSession.FinishMiniOp(Transaction: TMemDBTransaction);
begin
  FDB.FinishMiniOp(Transaction);
end;


destructor TMemDBSession.Destroy;
begin
  FDB.RemoveSession(self);
  inherited;
end;

{ TMemDB }

procedure TMemDB.StartMiniOp(Transaction: TMemDBTransaction);
begin
  FSessionLock.Acquire;
  try
    if Transaction.FCommitedOrRolledBack then
      raise EMemDBException.Create(S_MINIOP_TRANSACTION_FINISHED);
    if (Transaction.FCommitRollbackInProgress <> tiptNone) then
      raise EMemDBException.Create(S_MINIOP_CONCURRENT);
    Transaction.FCommitRollbackInProgress := tiptMiniOp;
  finally
    FSessionLock.Release;
  end;
end;

procedure TMemDB.FinishMiniOp(Transaction: TMemDBTransaction);
begin
  FSessionLock.Acquire;
  try
    Assert(not Transaction.FCommitedOrRolledBack);
    Assert(Transaction.FCommitRollbackInProgress = tiptMiniOp);
    Transaction.FCommitRollbackInProgress := tiptNone;
  finally
    FSessionLock.Release;
  end;
end;

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
  try
    //Try and remove outstanding refs.
    //Can sensibly throw in the non-error commit case,
    //else quietly clean-up. Internal API is the last use of an API object in this
    //transaction.
    FDatabase.CheckNoDanglingTransactionRefs(Transaction, Commit and not AsyncError);

    if Transaction.FMode = amReadWrite then
    begin
      API := FDatabase.Interfaced.GetAPIObject(Transaction, APIInternalCommitRollback)
        as TMemAPIDatabaseInternal;
      try
        //If async journal error or multi-rollback failed, state is hosed, or
        //not persistable, so no point even trying, just cleanup.
        if not AsyncError then
        begin
          if Commit then
          begin
            if FJournal.NeedFlowControl then
              Transaction.FSync := amFlushBuffers;

            //This may raise exceptions if pre-commit checks fail.
            //Arranges streams in transaction for final journalling;
            API.UserCommitCycleV3;

            //Exceptions after here less likely.
            Transaction.AddRef;
            //And only set up the wait if no prior exception.
            WaitJournalDone := (Transaction.FMode = amReadWrite) and Commit and
              (Transaction.FSync = amFlushBuffers);
            if WaitJournalDone then
            begin
              Assert(not Assigned(Transaction.FlushFinishedEvent));
              Transaction.FFlushFinishedEvent := TEvent.Create(nil, true, false, '');
            end;
            //And now journal it.
            FJournal.TransactionCommitChangeset(Transaction);
          end
          else
          begin
            try
              //This may raise exceptions if things go horribly wrong.
              //Unlike mini-rollbacks, we really require that this
              //full rollback works.
              API.UserRollbackCycleV3;
            except
              on E: Exception do
              begin
                FSessionLock.Acquire;
                try
                  Assert(FPhase in [mdbRunning, mdbClosingWaitClients, mdbError]);
                  if FPhase = mdbRunning then
                  begin
                    FPhase := mdbError;
                    FLastError := S_MULTI_ROLLBACK_FAILED + E.Message;
                  end;
                finally
                  FSessionLock.Release;
                end;
                //We will swallow the exception having put the DB into the error
                //state so that we can release locks and free the transaction,
                //and the user does not need to handle rollbacks raising
                //exceptions (which should never really happen anyway).
              end;
            end;
          end;
        end;
      finally
        API.Free;
      end;
    end
    else
    begin
{$IFOPT C+}
      FDatabase.CheckNoChanges;
{$ENDIF}
    end;
  finally
    if Transaction.FMode = amReadWrite then
    begin
      FRWLock.EndWrite;
    end
    else
      FRWLock.EndRead;
    FSessionLock.Acquire;
    try
      Transaction.FCommitRollbackInProgress := tiptNone;
    finally
      FSessionLock.Release;
    end;
  end;
  if WaitJournalDone then
    Transaction.FlushFinishedEvent.WaitFor(INFINITE);
  FSessionLock.Acquire;
  try
    Assert(Transaction.FSession.FTransaction = Transaction);
    Transaction.FCommitedOrRolledBack := true;
    Transaction.FSession.FTransaction := nil;
    idx := FTransactionList.IndexOf(Transaction);
    FTransactionList.Delete(idx);
  finally
    FSessionLock.Release;
  end;
  Transaction.DecRefConditionalFree;
end;

function TMemDB.StartTransaction(Mode: TMDBAccessMode;
                                 Sync: TMDBSyncMode;
                                 Iso: TMDBIsolationLevel;Session: TMemDBSession)
  : TMemDBTransaction;
var
  idx: integer;
begin
  result := nil;
  FSessionLock.Acquire;
  try
    if Assigned(Session.FTransaction) then
      raise EMemDBException.Create(S_DB_SESSION_HAS_TRANSACTIONS);
    idx := FSessionList.IndexOf(Session);
    if idx < 0 then
      raise EMemDBException.Create(S_DB_SESSION_NOT_FOUND);
    if FPhase <> mdbRunning then
    begin
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
    result.FIsolation := Iso;
    Session.FTransaction := result;
    FTransactionList.Add(result);
  finally
    FSessionLock.Release;
  end;
  if Mode = amRead then
    FRWLock.BeginRead
  else
    FRWLock.BeginWrite;
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
    try
      if Assigned(Session.FTransaction) then
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
  FRWLock := TMultiReadExclusiveWriteSynchronizer.Create;
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
  FRWLock.Free;
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

function TMemDB.GetDBStats: TMemDBStats;
var
  DoEntityStats: boolean;
  MemStats: TMemStats;
begin
  result := TMemDBStats.Create;
  FSessionLock.Acquire;
  try
    DoEntityStats := FPhase = mdbRunning;
    result.Phase := FPhase;
    if DoEntityStats then
      Inc(FCheckpointRefs);
  finally
    FSessionLock.Release;
  end;
  if DoEntityStats then
  begin
    FRWLock.BeginRead;
    try
      MemStats := result;
      FDatabase.GetStats(MemStats);
    finally
      FRWLock.EndRead;
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


function TMemDB.Checkpoint: boolean;
var
  ChangesetStream: TStream;
  TmpName: string;
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
  //TODO - Also a problem with stop/restart DB races with user xations.

  //and XE 4 MRSW lock (which should not happen at all, regardless of
  // this race). MCH Investigating.

  {ref BUG_MCH_1_2_2025 }
  if result then
  begin
    FRWLock.BeginRead;
    try
      //TODO - This to follow same conventions as other temp stream creation.
      TmpName := TPath.GetTempFileName();
      ChangesetStream := TMemDBTempFileStream.Create(TmpName);
      try
        //NB. No multi-transactions in checkpoint. See journal replay
        //function as to why.
        FDatabase.ToScratch(ChangesetStream);
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
      FRWLock.EndRead;
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
