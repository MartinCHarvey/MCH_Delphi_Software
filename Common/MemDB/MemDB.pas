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

{$IFDEF USE_TRACKABLES}
  TMemDBTransaction = class(TTrackable)
{$ELSE}
  TMemDBTransaction = class
{$ENDIF}
  private
    FDB: TMemDB;
    FSession: TMemDBSession;
    FMode: TMDBAccessMode;
    FSync: TMDBSyncMode;
    FIsolation: TMDBIsolationLevel;
    FV2Changeset: TStream;
    FCommitRollbackInProgress: boolean;
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
    property V2Changeset: TStream read FV2Changeset;
    property FlushFinishedEvent: TEvent read FFlushFinishedEvent;
    property ParentSession: TMemDBSession read FSession;
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
  S_COMMIT_ROLLBACK_IN_PROGRESS = 'Commit or rollback for this transaction already in progress.';
  S_COMMIT_OR_ROLLBACK_BEFORE_FREE = 'Transactions should be committed or rolled back before destroying.';
  S_ASYNC_JOURNAL_ERROR = 'Can''t start transaction, journal has failed: ';
  S_REINIT_DIFF_LOCATION = 'Database loaded at different location, unload first.';

  { TMemDBTransaction }

function TMemDBTransaction.GetAPI: TMemAPIDatabase;
begin
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
  AddRef;
end;

destructor TMemDBTransaction.Destroy;
var
  DelFileName: string;
begin
  SetLength(DelFileName, 0);
  if not FCommitedOrRolledBack then
    raise EMemDBAPIException.Create(S_COMMIT_OR_ROLLBACK_BEFORE_FREE);
  //Generally will do nothing if previous commit or rollback was successful
  if FV2Changeset is TFileStream then
    DelFileName := (FV2Changeset as TFileStream).FileName
  else if (FV2Changeset is TMemDBWriteCachedFileStream) then
    DelFileName := (FV2Changeset as TMemDBWriteCachedFileStream).FileName;
  FV2Changeset.Free;
  if Length(DelFileName) > 0 then
    DeleteFile(DelFileName);
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

destructor TMemDBSession.Destroy;
begin
  FDB.RemoveSession(self);
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
  CommitStream: TStream;
  API: TMemAPIDatabaseInternal;
  StorageMode: TTempStorageMode;
begin
  WaitJournalDone := false;
  FSessionLock.Acquire;
  try
    if Transaction.FCommitedOrRolledBack then
      exit;
    if Transaction.FCommitRollbackInProgress then
      raise EMemDBException.Create(S_COMMIT_ROLLBACK_IN_PROGRESS);
    idx := FTransactionList.IndexOf(Transaction);
    if idx < 0 then
      raise EMemDBException.Create(S_DB_TRANSACTION_NOT_FOUND);
    Transaction.FCommitRollbackInProgress := true;
    StorageMode := Transaction.FSession.TempStorageMode;
  finally
    FSessionLock.Release;
  end;
  try
    if Transaction.FMode = amReadWrite then
    begin
      API := FDatabase.Interfaced.GetAPIObject(Transaction, APIInternalCommitRollback)
        as TMemAPIDatabaseInternal;
      try
        if Commit then
        begin
          if FJournal.NeedFlowControl then
            Transaction.FSync := amFlushBuffers;

          WaitJournalDone := (Transaction.FMode = amReadWrite) and Commit and
            (Transaction.FSync = amFlushBuffers);
          if WaitJournalDone then
          begin
            Assert(not Assigned(Transaction.FlushFinishedEvent));
            Transaction.FFlushFinishedEvent := TEvent.Create(nil, true, false, '');
          end;
          //This may raise exceptions if pre-commit checks fail.
          CommitStream := API.UserCommitCycleV2(StorageMode);
          Assert(not Assigned(Transaction.FV2Changeset));
          Transaction.FV2Changeset := CommitStream;
          Transaction.AddRef;
          FJournal.TransactionCommitChangeset(Transaction);
        end
        else
          API.UserRollbackCycle;
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
      Transaction.FCommitRollbackInProgress := false;
    finally
      FSessionLock.Release;
    end;
  end;
  if WaitJournalDone then
    Transaction.FlushFinishedEvent.WaitFor(INFINITE);
  FSessionLock.Acquire;
  try
    Transaction.FCommitedOrRolledBack := true;
    Assert(Transaction.FSession.FTransaction = Transaction);
    Transaction.FSession.FTransaction := nil;
    idx := FTransactionList.IndexOf(Transaction);
    FTransactionList.Delete(idx);
    Transaction.DecRefConditionalFree;
  finally
    FSessionLock.Release;
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
  FSessionLock.Acquire;
  try
    if Assigned(Session.FTransaction) then
      raise EMemDBException.Create(S_DB_SESSION_HAS_TRANSACTIONS);
    idx := FSessionList.IndexOf(Session);
    if idx < 0 then
      raise EMemDBException.Create(S_DB_SESSION_NOT_FOUND);
    if FPhase <> mdbRunning then
    begin
      Assert(FPhase = mdbError);
      //Asynchronous journal error, don't start transactions.
      raise EMemDBException.Create(S_ASYNC_JOURNAL_ERROR + FLastError);
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
    if Assigned(Session.FTransaction) then
      raise EMemDBException.Create(S_DB_SESSION_HAS_TRANSACTIONS);
    FSessionList.Delete(idx);
    CheckClientsDone;
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
          FSessionLock.Release;
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
      API.JournalReplayCycleV2(CStream, Initial);
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
  if result then
  begin
    FRWLock.BeginRead;
    try
      //TODO - Consider making this temporary write cached file, not
      //memory stream, to help mem usage.
      //Not sure whether ca make it the right name / location for
      //it to also be the final file.
      TmpName := TPath.GetTempFileName();
      ChangesetStream := TMemDBWriteCachedFileStream.Create(TmpName);
      try
        FDatabase.ToScratchV2(ChangesetStream);
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
