unit MemDB2Journal;
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

interface

{$DEFINE JOURNAL_WRITE_FLOW_CONTROL}
//Journal recovery handles the situation where the last file is blank,
//but not where it is corrupted. That's as bad as I expect situations to be
//in cases involving power loss on NTFS. Anything worse is h/w failure.
//If you need something better, mod the code yourself.


uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  Classes, DLThreadQueue, MemDB2Streamable, MemDB2Misc, LockAbstractions, Reffed;

type
  TMemDbJournal = class;

  TMemDBJournalThread = class(TThread)
  private
    FParentJournal: TMemDbJournal;
  public
    procedure Execute; override;
  end;

  //High priority actions (wake thread up immediately) are:
  //Initial load,
  //Commit transaction if immediate writeback specified for any.
  //LastAction.
  TMemDBJournalActionType =
    (jatInitialLoad, jatCommitTransaction, jatCheckPoint, jatLastAction);

{$IFDEF USE_TRACKABLES}
  TMemDBJournalAction = class(TTrackable)
{$ELSE}
  TMemDBJournalAction = class
{$ENDIF}
  private
    FActionType: TMemDBJournalActionType;
    FActionObj: TObject;
    //jatCommit: Transaction object (not owned).
    //jatCheckPoint: TMemDatabaseItem (changeset, owned).
  public
    property ActionObj: TObject read FActionObj write FActionObj;
    property ActionType: TMemDBJournalActionType read FActionType write FActionType;
  end;

  TJournalInitializedEvent = procedure(Sender: TObject; OK: boolean;
                                       AnyFiles: boolean;
                                       CreateCheckpoint: boolean;
                                       ErrMsg: string) of object;
  TJournalInitReplayTransactionEvent =
    procedure (Sender: TObject; Changesets: TObject; Initial: boolean) of object;
  TJournalTransactionWriteFlushEvent =
    procedure(Sender: TObject; Transaction: TObject) of object;
  TJournalErrorEvent =
    procedure(Sender: TObject; ErrMsg:string) of object;

  TJournalState =      (jscCreated,
                        jscInitializing, //Feed changesets to memdb, first action
                        jscRunning, //Process transactions from memdb, many actions
                        jscRunningError, //Error state where we bin data, but
                                         //Try to keep queueing / processing going.
                        jscFinishing, //Final checkpoint, last action.
                        jscFinishingError, //Error state where we bin data, but
                                         //Try to keep queueing / processing going.
                        jscFinished  //No more actions, awaiting destruction.
                        );

{$IFDEF USE_TRACKABLES}
  TMemDBJournal = class(TTrackable)
{$ELSE}
  TMemDBJournal = class
{$ENDIF}
  private
    FJournalThread: TMemDBJournalThread;
    //We'll use the thread queue lock to protect any other datastructures we need.
    FActionsPendingQueue: TDLProxyThreadQueue;
    //Most sync meant to be performed by MemDB, but debug checks here.
    FState: TJournalState;
    FHighPriorityActionCount: integer;
    FHighPriorityEvent: TEvent;
    FOnUIStateChange: TNotifyEvent;

    FOnJournalInitialized: TJournalInitializedEvent;
    FOnJournalFinished: TNotifyEvent;
    FOnJournalReplay: TJournalInitReplayTRansactionEvent;
    FOnJournalWriteFlush: TJournalTransactionWriteFlushEvent;
    FOnJournalError: TJournalErrorEvent;

    procedure DoJournalInitialized(OK: boolean; AnyFiles: boolean; CreateCheckpoint: boolean; ErrMsg: string);
    procedure DoJournalFinished(Signal: boolean = true);
    procedure ErrorDropTransaction(T: TReffed);
  protected
    procedure DoJournalReplay(Changesets: TObject; Initial: boolean);
    procedure DoJournalWriteFlush(Transaction: TObject);
    procedure DoJournalError(ErrMsg: string);

    function PerformInitialLoad(Action: TMemDbJournalAction;
      var AnyFiles: boolean;
      var CreateCheckpoint: boolean; var ErrMsg:string): boolean; virtual; abstract;
    procedure PerformMultiTransactionWrite(Transactions: TList; JState: TJournalState); virtual;abstract;
    procedure PerformCheckpoint(Action: TMemDBJournalAction; JState: TJournalState); virtual; abstract;
    procedure PerformLastAction(Action: TMemDBJournalAction); virtual; abstract;

    procedure ThreadFunc;
    procedure HighPriorityDone;
    procedure DoUIStateChange;
  public
    constructor Create;
    destructor Destroy; override;

    //Called by MemDB.
    procedure Initialise;
    procedure TransactionCommitChangeset(Transaction: TObject);
    procedure Checkpoint(Changeset: TObject);
    procedure Finish;
    function NeedFlowControl: boolean;
    procedure SynchronizeStateChange;

    property OnJournalInitialized: TJournalInitializedEvent
      read FOnJournalInitialized write FOnJournalInitialized;
    property OnJournalFinished: TNotifyEvent
      read FOnJournalFinished write FOnJournalFinished;
    property OnJournalReplay: TJournalInitReplayTRansactionEvent
      read FOnJournalReplay write FOnJournalReplay;
    property OnJournalWriteFlush: TJournalTransactionWriteFlushEvent
      read FOnJournalWriteFlush write FOnJournalWriteFlush;
    property OnJournalError:TJournalErrorEvent
      read FOnJournalError write FOnJournalError;
    property OnUIStateChange: TNotifyEvent
      read FOnUIStateChange write FOnUIStateChange;
  end;


  //Default journal implementation which journals to a local filesystem.
  TMemDBDefaultJournal = class(TMemDbJournal)
  private
    FBaseDirectory: string;
    FJournalType : TMemDBJournalType;
    FWriteSeq: UInt64;
    procedure FreeRList(var RList: TList);
  protected
    function GetInitialFileList: TList;
    function GetListAndInitialSeqs(var RList: TList; var InitialSeq:Uint64; var FinalSeq: UInt64): boolean;
    procedure CleanupUnusedFiles(RList: TList; InitialSeq:Uint64; FinalSeq: UInt64);

    function PerformInitialLoad(Action: TMemDbJournalAction;
      var AnyFiles: boolean;
      var CreateCheckpoint: boolean; var ErrMsg:string): boolean; override;
    procedure PerformMultiTransactionWrite(Transactions: TList; JState: TJournalState); override;
    procedure PerformCheckpoint(Action: TMemDBJournalAction; JState: TJournalState); override;
    procedure PerformLastAction(Action: TMemDBJournalAction); override;
  public
    property BaseDirectory:string read FBaseDirectory write FBaseDirectory;
    property JournalType: TMemDBJournalType read FJournalType write FJournalType;
  end;

implementation

{$IFDEF MSWINDOWS}
{$DEFINE WINDOWS_FLUSH}
{$ENDIF}

uses MemDB2,
     SysUtils, BufferedFileStream
{$IFDEF WINDOWS_FLUSH}
     , Windows
{$ELSE}
    , Posix.Unistd
{$ENDIF}
     ;

const
  JOURNAL_PENDING_QUEUE_LIMIT = 4096;
  MAX_INCR_JOURNAL_FILES = 10 * 1024;
  S_COULDNT_CREATE_DIR = 'Couldn''t create or open directory: ';
  S_COULDNT_FIND_EXPECTED_DB_FILES = 'Couldn''t find sequentially numbered db files.';
  S_DB_FILES_NUMBERING_BAD = 'Db file numbering and types disagree.';
  S_EXCEPTION_REPLAY = 'Exception on journal replay: ';
  S_UNHANDLED_INITIAL_LOAD = 'Exception on intial DB load: ';
  S_UNHANDLED_TXION_WRITE = 'Unhandled Txion write: ';
  S_SKIPPED_PREV_JOURNAL_ERROR = 'Txion write skipped, previous journal error.';
  S_EXCEPTION_TRANS_WRITE = 'Exception during journal write of transaction: ';
  S_EXCEPTION_CHECKPOINT_WRITE = 'Exception during journal write of transaction: ';
  S_FLUSH_FAILED = 'File flush failed: ';
  S_FILE_RENAME_FAILED  = 'File rename failed: ';
  S_UNHANDLED_EXCEPT_IN_JTHREAD = 'Unhandled in journal thread, DB toast: ';

  CHECKPOINT_RATIO = 10; { More than 1/10th data in journal, re-checkpoint }

type
  TJournalFileType = (jftInitOrCheckpoint, jftIncremental,
                      jftInitOrCheckpointTemp, jftIncrementalTemp);
  TJournalFileExts = array[TJournalFileType] of string;

{$IFDEF USE_TRACKABLES}
  TDBFileInfo = class(TTrackable)
{$ELSE}
  TDBFileInfo = class
{$ENDIF}
  public
    FileSeq: UInt64;
    FileType: TJournalFileType;
    FileSize: UInt64;
  end;

const
  JOURNAL_THREAD_POLL_LATENCY = 5000;
  FileExts: TJournalFileExts = ('.dbinit', '.dbincr',
                                '.dbinit_partial', '.dbincr_partial');

{ TMemDBJournalThread }

procedure TMemDBJournalThread.Execute;
begin
  FParentJournal.ThreadFunc;
end;

{ TMemDBJournal }

procedure TMemDBJournal.ErrorDropTransaction(T: TReffed);
var
  Tr: TMemDBTransaction;
begin
  if Assigned(T) then
  begin
    Assert(T is TMemDBTransaction);
    Tr := T as TMemDBTransaction;
    if Assigned(Tr.FlushFinishedEvent) then
      DoJournalWriteFlush(Tr);
    if Tr.Sync = amFlushBuffers then
      HighPriorityDone;
    Tr.Release;
  end;
end;


procedure TMemDBJournal.DoJournalReplay(Changesets: TObject; Initial: boolean);
begin
{$IFOPT C+}
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState = jscInitializing);
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
{$ENDIF}
  if Assigned(FOnJournalReplay) then
    FOnJournalReplay(self, Changesets, Initial)
  else
    Assert(false);
end;

procedure TMemDBJournal.DoJournalWriteFlush(Transaction: TObject);
begin
{$IFOPT C+}
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState in [jscRunning, jscRunningError,
                      jscFinishing, jscFinishingError]);
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
{$ENDIF}
  if Assigned(FOnJournalWriteFlush) then
    FOnJournalWriteFlush(Self, Transaction);
end;

procedure TMemDBJournal.DoJournalInitialized(OK, AnyFiles: boolean; CreateCheckpoint: boolean; ErrMsg: string);
begin
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState = jscInitializing);
    if OK then
      FState := jscRunning
    else
      //Best way of making sure we don't add any files after a bad load...
      DoJournalFinished(false);
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
  try
    if Assigned(FOnJournalInitialized) then
      FOnJournalInitialized(self, OK, AnyFiles, CreateCheckpoint, ErrMsg);
  except
    on E:Exception do
    begin
      if OK then
      begin
        //Exception handling startup of OK DB load, can go from running
        //to running error.
        DoJournalError(E.Message);
      end;
      //Else journal thread already toast.
    end;
  end;
end;

function TMemDBJournal.NeedFlowControl: boolean;
begin
//Was worried main thread would run away from journal thread, but probably not.
//Since at restart time, many small file reads are slow, don't enable this
//option unless the write speed of the FS is much much slower than the read speed.
{$IFDEF JOURNAL_WRITE_FLOW_CONTROL}
  FActionsPendingQueue.AcquireLock;
  try
   result := FActionsPendingQueue.Count > JOURNAL_PENDING_QUEUE_LIMIT;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
{$ELSE}
  result := false;
{$ENDIF}
end;

procedure TMemDBJournal.DoJournalFinished(Signal: boolean);
begin
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState in [jscCreated, jscInitializing, jscFinishing, jscFinishingError]);
    Assert((not Assigned(FJournalThread)) or (not FJournalThread.FreeOnTerminate));
    FState := jscFinished;
    FHighPriorityEvent.SetEvent;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
  if Signal then
    if Assigned(FOnJournalFinished) then
      FOnJournalFinished(self);
end;

procedure TMemDbJournal.DoJournalError(ErrMsg: string);
var
  Propagate: boolean;
begin
  Propagate := false;
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState in
      [jscInitializing, jscRunning, jscRunningError,
       jscFinishing, jscFinishingError, jscFinished ]);
    if FState in [jscInitializing, jscRunning] then
    begin
      FState := jscRunningError;
      Propagate := true;
    end
    else if FState in [jscFinishing] then
    begin
      FState := jscFinishingError;
      Propagate := true;
    end;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
  if Assigned(FOnJournalError) and Propagate then
    FOnJournalError(self, ErrMsg);
end;

procedure TMemDBJournal.HighPriorityDone;
begin
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FHighPriorityActionCount > 0);
    Dec(FHighPriorityActionCount);
    if FHighPriorityActionCount = 0 then
      FHighPriorityEvent.ResetEvent;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
end;

procedure TMemDBJournal.ThreadFunc;

var
  OAmalgamatedTrans: TList;
  Action: TMemDBJournalAction;
  OK: boolean;
  OAnyFiles: boolean;
  OCreateCheckpoint: boolean;
  ErrMsg: string;
  ActionState: TJournalState;
  UnhandledExcept: boolean;

  function AmalgamateTransactions: TList;
  var
    LT: TMemDBTransaction;
    LSync: TMDBSyncMode;
    LAction: TMemDBJournalAction;
    i: integer;

  begin
    result := nil;
    try
      result := TList.Create;
      LT := TMemDBTransaction(Action.ActionObj);

      //Exception case, OAction freed in outer function.
      LSync := LT.Sync;
      try
        result.Add(LT);
      except
        ErrorDropTransaction(LT);
        raise;
      end;

      //Optimization, batch bunches of lazy write transactions together.
      //Dont amalgamate in error cases.
      if (LSync = amLazyWrite) and
        (ActionState in [jscRunning, jscFinishing]) then
      begin
        FActionsPendingQueue.AcquireLock;
        try
          ActionState := FState;
          LAction := FActionsPendingQueue.PeekHeadObj as TMemDBJournalAction;
          while Assigned(LAction)
            and (ActionState in [jscRunning, jscFinishing])
            and (LAction.ActionType = jatCommitTransaction)
            and ((LAction.ActionObj as TMemDbTransaction).Sync = amLazyWrite) do
          begin
            LAction := FActionsPendingQueue.RemoveHeadObj as TMemDBJournalAction;
            try
              LT := LAction.ActionObj as TMemDBTransaction;
              try
                result.Add(LT);
              except
                ErrorDropTransaction(LT);
                raise;
              end;
            finally
              LAction.Free;
            end;
            LAction := FActionsPendingQueue.PeekHeadObj as TMemDBJournalAction;
          end;
        finally
          FActionsPendingQueue.ReleaseLock;
        end;
      end;
    except
      on E:Exception do
      begin
        //We have amalgamated some transactions, but they will never execute.
        //Need to release them as best we can.
        if Assigned(result) then
        begin
          for i := 0 to Pred(result.Count) do
          begin
            LT := TMemDBTransaction(result[i]);
            ErrorDropTransaction(LT);
          end;
          result.Free;
        end
        else
        begin
          //List never created: first transaction not yet owned by it.
          LT := TMemDBTransaction(Action.ActionObj);
          ErrorDropTransaction(LT);
        end;
        raise;
      end;
    end;
  end;

begin
  UnhandledExcept := false;
  FActionsPendingQueue.AcquireLock;
  try
    ActionState := FState;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
  while (ActionState <> jscFinished) do
  begin
    try
      FHighPriorityEvent.WaitFor(JOURNAL_THREAD_POLL_LATENCY);
      //Pull things out of the Action pending queue.
      //We can amalgamate commits where none of the transactions have
      //the write-back flag set. Checkpoints also not amalgamated.
      FActionsPendingQueue.AcquireLock;
      try
        Action := FActionsPendingQueue.RemoveHeadObj as TMemDbJournalAction;
        Assert(Assigned(Action) or (FHighPriorityActionCount = 0));
        ActionState := FState;
      finally
        FActionsPendingQueue.ReleaseLock;
      end;
      //We must keep the journal thread processing events even
      //when exceptions occur. All non-trivial call-outs from this
      //loop must handle exceptions.
      if Assigned(Action) then
      begin
        try
          case Action.ActionType of
            jatInitialLoad:
            begin
              HighPriorityDone;
              OK := false;
              try
                OK := PerformInitialLoad(Action, OAnyFiles, OCreateCheckpoint, ErrMsg);
              except
                on E:Exception do
                begin
                  //Signal initial load failure in the normal manner.
                  OK := false;
                  ErrMsg := S_UNHANDLED_INITIAL_LOAD + E.Message;
                end;
              end;
              //Might call DoJournalFinished (but not signal up) on bad load ...
              DoJournalInitialized(OK, OAnyFiles, OCreateCheckpoint, ErrMsg);
            end;
            jatCommitTransaction:
            begin
              try
                OAmalgamatedTrans := AmalgamateTransactions;
                try
                  //Executes even when journal in jscRunningError,
                  //in which case performs only essential actions.
                  PerformMultiTransactionWrite(OAmalgamatedTrans, ActionState);
                finally
                  OAmalgamatedTrans.Free;
                end;
              except
                //Possible amalgamation actions might raise.
                on E: Exception do
                  DoJournalError(S_UNHANDLED_TXION_WRITE + E.Message);
              end;
            end;
            jatCheckPoint: PerformCheckPoint(Action, ActionState);
            jatLastAction:
            begin
              HighPriorityDone;
              PerformLastAction(Action);
              DoJournalFinished;
            end
          else
            Assert(false);
          end;
        finally
          Action.Free;
        end;
      end;
    except
      //Shouldn't ever get here at all, buuut ...
      on E:Exception do
      begin
        if not UnhandledExcept then //first totally unhandled exception
        begin
          DoJournalError(S_UNHANDLED_EXCEPT_IN_JTHREAD + E.Message);
          UnhandledExcept := true;
        end
        else
          raise;
         //More than one totally unhandled except in journal thread
         //dont even do DoJournalFinished, just let it die.
      end;
    end;
  end;
end;

constructor TMemDBJournal.Create;
begin
  FActionsPendingQueue := TDLProxyThreadQueue.Create;
  FHighPriorityEvent := TEvent.Create(nil, true, false, '');
  inherited;
end;

destructor TMemDBJournal.Destroy;
begin
  FActionsPendingQueue.AcquireLock;
  try
    if FState <> jscFinished then
      DoJournalFinished(false);
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
  if Assigned(FJournalThread) then
  begin
    FJournalThread.WaitFor;
    FJournalThread.Free;
  end;
{$IFOPT C+}
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FActionsPendingQueue.Count = 0);
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
{$ENDIF}
  FActionsPendingQueue.Free;
  Assert(FHighPriorityActionCount = 0);
  FHighPriorityEvent.Free;
  inherited;
end;

procedure TMemDBJournal.Initialise;
var
  Action: TMemDbJournalAction;
  WaitOld: boolean;
begin
  FActionsPendingQueue.AcquireLock;
  try
    WaitOld := FState = jscFinished;
    Assert(Assigned(FJournalThread) or (not WaitOld));
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
  if WaitOld then
    FJournalThread.WaitFor;
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState in [jscCreated, jscFinished]);
    if WaitOld then
    begin
      FJournalThread.Free;
      FJournalThread := nil;
      FState := jscCreated;
    end;
    if not Assigned(FJournalThread) then
    begin
      FJournalThread := TMemDBJournalThread.Create(true);
      FJournalThread.FParentJournal := self;
      FJournalThread.Start;
    end;
    Action := TMemDbJournalAction.Create;
    Action.ActionType := jatInitialLoad;
    FActionsPendingQueue.AddTailobj(Action);
    //Needs to happen rapidly.
    Inc(FHighPriorityActionCount);
    FHighPriorityEvent.SetEvent;
    FState := jscInitializing;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
end;

procedure TMemDBJournal.TransactionCommitChangeset(Transaction: TObject);
var
  Action: TMemDbJournalAction;
  T: TMemDBTransaction;
begin
  Assert(Assigned(Transaction));
  Assert(Transaction is TMemDBTransaction);
  T := Transaction as TMemDBTransaction;
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState in [jscRunning, jscRunningError]);
    Action := TMemDbJournalAction.Create;
    Action.ActionType := jatCommitTransaction;
    Action.ActionObj := T;
    FActionsPendingQueue.AddTailObj(Action);
    Assert(T.Mode in [amWriteExclusive, amReadWriteShared]); //No read-only transactions here plz!
    if T.Sync = amFlushBuffers then
    begin
      //Needs to happen rapidly.
      Inc(FHighPriorityActionCount);
      FHighPriorityEvent.SetEvent;
    end;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
end;

procedure TMemDBJournal.Checkpoint(Changeset: TObject);
var
  Action: TMemDbJournalAction;
begin
  Assert(Assigned(Changeset));
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState in [jscRunning, jscRunningError]);
    Action := TMemDbJournalAction.Create;
    Action.ActionType := jatCheckPoint;
    Action.ActionObj := Changeset;
    FActionsPendingQueue.AddTailObj(Action);
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
end;

procedure TMemDBJournal.Finish;
var
  Action: TMemDbJournalAction;
begin
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState in [jscRunning, jscRunningError]);
    Action := TMemDbJournalAction.Create;
    Action.ActionType := jatLastAction;
    FActionsPendingQueue.AddTailObj(Action);
    Inc(FHighPriorityActionCount);
    FHighPriorityEvent.SetEvent;
    if FState = jscRunningError then
      FState := jscFinishingError
    else
      FState := jscFinishing
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
end;

procedure TMemDBJournal.SynchronizeStateChange;
begin
  if Assigned(FJournalThread) then
    FJournalThread.Queue(DoUIStateChange);
end;

procedure TMemDBJournal.DoUIStateChange;
begin
  if Assigned(FOnUIStateChange) then
    FOnUIStateChange(self);
end;

{ TMemDbDefaultJournal }

function CompareDBInfos(Item1, Item2: Pointer): Integer;
var
  DB1,DB2: TDBFileInfo;
begin
  DB1 := TDBFileInfo(Item1);
  DB2 := TDBFIleInfo(Item2);
  result := 0;
  if DB1.FileSeq > DB2.FileSeq then
    result := 1
  else if DB1.FileSeq < DB2.FileSeq then
    result := -1
  else if Ord(DB1.FileType) > Ord(DB2.FileType) then
    result := 1
  else if Ord(DB1.FileType) < Ord(DB2.FileType) then
    result := -1;
end;

function TMemDbDefaultJournal.GetInitialFileList: TList;
var
  SearchStr: string;
  FileType: TJournalFileType;
  FindRet: integer;
  FindData: TSearchRec;
  FileName: string;
  RList: TList;
  FileSeq: UInt64;
  DBInfo: TDBFileInfo;
begin
  result := nil;
  RList := TList.Create;
  FillChar(FindData, sizeof(FindData) ,0);
  try
    for FileType := Low(FileType) to High(FileType) do
    begin
      SearchStr := FBaseDirectory + '*' + FileExts[FileType];
      FindRet := SysUtils.FindFirst(SearchStr, faNormal, FindData);
      try
        while FindRet = 0 do
        begin
          FileName := FindData.Name;
          //Check filename is some sort of UInt64 we understand, if not, throw out.

          if Pos(FileExts[FileType], FileName) <>
            Succ(Length(FileName) - Length(FileExts[FileType])) then
          begin
            Assert(false);
            exit;
          end;
          FileName := Copy(FileName, 1,
            Length(FileName) - Length(FileExts[FileType]));
          try
            FileSeq := StrToInt64(FileName);

            DBInfo := TDBFileInfo.Create;
            DBInfo.FileSeq := FileSeq;
            DBInfo.FileType := FileType;
            DBInfo.FileSize := FindData.Size;
            try
              RList.Add(DBInfo)
            except
              DBInfo.Free; raise;
            end;
          except
            on E: EConvertError do ; //Continue round loop.
          end;
          FindRet := SysUtils.FindNext(FindData);
        end;
      finally
        SysUtils.FindClose(FindData);
      end;
    end;
    RList.Sort(CompareDBInfos);
    result := RList;
    RList := nil;
  finally
    FreeRList(RList);
  end;
end;

procedure TMemDbDefaultJournal.FreeRList(var RList: TList);
var
  LocalIdx: integer;
begin
  if Assigned(RList) then
  begin
    for LocalIdx := 0 to Pred(RList.Count) do
      TObject(RList[LocalIdx]).Free;
    RList.Free;
    RList := nil;
  end;
end;

//FinalSeq ends up being the first *write* sequence number.
function TMemDbDefaultJournal.GetListAndInitialSeqs(var RList: TList; var InitialSeq:Uint64; var FinalSeq: UInt64): boolean;
var
  Idx, StartIdx: integer;
  Found: boolean;
  DBInfo: TDBFileInfo;

begin
  result := false;
  RList := GetInitialFileList;
  if not Assigned(RList) then
    exit;
  try
    //OK, got list of relevant files in numerical order.
    //Things that are valid:
    //1. No files at all.
    //2. An initial file, followed by a bunch of incremental files.
    //   - We want to find the newest initial file.
    //   - and take as many incremental as we can after that,
    //   - and they must be contiguous.
    //   - non-contiguous, we'll just stop there, and warn about later
    //     incremental files being missed.
    //Expect the provided list to be in numerical order.
    //If higher numbered files (that we would subsequently overwite),
    //then fail the load process.
    //Lower numbered files can be trimmed at the end of start-up.

    //Since we now have potentially temporary files, we build the list of sequence
    //numbers for all final files.
    InitialSeq := 0;
    FinalSeq := 0;
    if RList.Count = 0 then
    begin
      result := true;
      exit;
    end;

    Found := false;
    StartIdx := 0;
    //Find latest dbinit
    for Idx := Pred(RList.Count) downto 0 do
    begin
      DBInfo := TDBFileInfo(RList[Idx]);
      if DBInfo.FileType = jftInitOrCheckpoint then
      begin
        Found := true;
        InitialSeq := DBInfo.FileSeq;
        StartIdx := Idx;
        break;
      end;
    end;
    if not Found then exit;
    FinalSeq := Succ(InitialSeq);
    //Found initial file number. Expect all subsequent (if they exist) to be in numerical order,
    //with no gaps.
    for Idx := Succ(StartIdx) to Pred(RList.Count) do
    begin
      DBInfo := TDBFileInfo(RList[Idx]);
      //Ignore temporaries whilst finding incremental updates.
      if DBInfo.FileType in [jftInitOrCheckpoint, jftIncremental] then
      begin
        if (DBInfo.FileType <> jftIncremental) or
           (DBInfo.FileSeq <> FinalSeq) then
          exit; //Result = false.
        Inc(FinalSeq);
      end;
    end;
    result := true;
  finally
    if not result then
    begin
      FreeRList(RList);
      InitialSeq := 0;
      FinalSeq := 0;
    end;
  end;
end;

procedure TMemDbDefaultJournal.CleanupUnusedFiles(RList: TList; InitialSeq:Uint64; FinalSeq: UInt64);
var
  Idx: integer;
  DBInfo: TDBFileInfo;
  FileName: string;
begin
  //Cleanup files before the initial sequence number we started loading from,
  //And all temporary files (which might prevent further writing...)
  for Idx := 0 to Pred(RList.Count) do
  begin
    DBInfo := TDBFileInfo(RList[Idx]);
    if (DBInfo.FileSeq < InitialSeq)
      or (DBInfo.FileType in [jftInitOrCheckpointTemp, jftIncrementalTemp]) then
    begin
      FileName := FBaseDirectory + IntToStr(DBInfo.FileSeq)
        + FileExts[DbInfo.FileType];
      SysUtils.DeleteFile(FileName);
    end;
  end;
end;

function TMemDbDefaultJournal.PerformInitialLoad(Action: TMemDbJournalAction;
  var AnyFiles: boolean; var CreateCheckpoint: boolean; var ErrMsg: string): boolean;
var
  InitialSeq, FinalSeq, ReadSeq: UInt64;
  RList: TList;
  Idx: integer;
  DBInfo: TDBFileInfo;
  Stream: TStream;
  FileName: string;
  TotalInitFileSize: UInt64;
  TotalIncrFileSize: UInt64;
  ReadFileCount: integer;
begin
  Assert(Assigned(Action));
  Assert(Action.ActionType = jatInitialLoad);
  //First of all, create (or open the) initial directory.
  AppendTrailingDirSlash(FBaseDirectory);
  if not ForceDirectories(FBaseDirectory) then
  begin
    result := false;
    ErrMsg := S_COULDNT_CREATE_DIR + FBaseDirectory;
    exit;
  end;
  result := GetListAndInitialSeqs(RList, InitialSeq, FinalSeq);
  if not result then
  begin
    ErrMsg := S_COULDNT_FIND_EXPECTED_DB_FILES;
    exit;
  end;
  TotalInitFileSize := 0;
  TotalIncrFileSize := 0;
  try
    result := false;
    AnyFiles := false;
    ReadFileCount := 0;
    ReadSeq := InitialSeq;
    //Okay, now can do the initial load.
    for Idx := 0 to Pred(RList.Count) do
    begin
      DBInfo := TDBFileInfo(RList[Idx]);
      //Ignore the temporary files
      if DBInfo.FileType in [jftInitOrCheckpoint, jftIncremental] then
      begin
        if (DBInfo.FileSeq >= InitialSeq) and (DBInfo.FileSeq < FinalSeq) then
        begin
          if ((DBInfo.FileType = jftInitOrCheckpoint) <> (DbInfo.FileSeq = InitialSeq))
            or (ReadSeq <> DBInfo.FileSeq) then
          begin
            ErrMsg := S_DB_FILES_NUMBERING_BAD;
            exit; //Result = false, types or numbering in a mess.
          end;
          AnyFiles := true;
          Inc(ReadFileCount);
          FileName := FBaseDirectory + IntToStr(DBInfo.FileSeq)
            + FileExts[DbInfo.FileType];
          Stream := nil;
          try
  {$IFDEF MSWINDOWS}
            Stream := TReadOnlyCachedFileStream.Create(FileName, FILE_CACHE_SIZE);
  {$ELSE}
            Stream := TReadOnlyCachedFileStream.Create(FileName, fmOpenRead);
  {$ENDIF}
            try
              DoJournalReplay(Stream, (DBInfo.FileSeq = InitialSeq));
            except
              on E: Exception do
              begin
                ErrMsg := S_EXCEPTION_REPLAY + E.Message + '(' + FileName + ')';
                exit; //Result := false;
              end;
            end;
          finally
            Stream.Free;
          end;
          case DBInfo.FileType of
            jftInitOrCheckpoint: Inc(TotalInitFileSize, DBInfo.FileSize);
            jftIncremental: Inc(TotalIncrFileSize, DBInfo.FileSize);
            jftInitOrCheckpointTemp,
            jftIncrementalTemp: ;
          else
            Assert(false);
          end;
          Inc(ReadSeq);
        end;
      end;
    end;
    //Only if everything else has worked do we then clean out unused files.
    FWriteSeq := FinalSeq;
    CleanupUnusedFiles(RList, InitialSeq, FinalSeq);
    CreateCheckpoint :=
         (not AnyFiles)
      or
         ((TotalIncrFileSize > (TotalInitFileSize div CHECKPOINT_RATIO))
         and (TotalIncrFileSize > ONE_MEG))
      or (ReadFileCount > MAX_INCR_JOURNAL_FILES);
    result := true;
  finally
    for Idx := 0 to Pred(RList.Count) do
      TDBFileInfo(RList.Items[Idx]).Free;
    RList.Free;
  end;
end;

procedure TMemDbDefaultJournal.PerformMultiTransactionWrite(Transactions: TList; JState: TJournalState);
var
  result: boolean;
  OutputStream: TStream;
  FileName, FinalFileName: string;
  Idx: integer;
  Transaction: TMemDBTransaction;
  DoFlush: boolean;
  FlushOK: boolean;
  ErrMsg: string;

  procedure CopyTransactionStreamToOutput;
  var
    S: TStream;
  begin
    S := Transaction.Changeset;
    S.Seek(0, TSeekOrigin.soBeginning);
    OutputStream.CopyFrom(S, S.Size);
  end;

begin
  result := false;
  try
    try
      Assert(JState in [jscRunning, jscRunningError, jscFinishing, jscFinishingError]);
      if not (JState in [jscRunning, jscFinishing]) then
      begin
        result := false;
        ErrMsg := S_SKIPPED_PREV_JOURNAL_ERROR;
      end
      else
      begin
        FileName := FBaseDirectory + IntToStr(FWriteSeq) + FileExts[jftIncrementalTemp];
        FinalFileName := FBaseDirectory + IntToStr(FWriteSeq) + FileExts[jftIncremental];
        try
      {$IFDEF MSWINDOWS}
          //Make it slightly less likely ENoMem destroys journal thread.
          try
            OutputStream := TWriteCachedFileStream.Create(FileName, FILE_CACHE_SIZE);
          except
            //Streams bulk-copied from, performance hit not so bad in the out of
            //memory case.
            on E:EOutOfMemory do
              OutputStream := TFileStream.Create(FileName, fmCreate or fmShareExclusive);
          end;
      {$ELSE}
          OutputStream := TWriteCachedFileStream.Create(FileName, fmCreate);
      {$ENDIF}
          DoFlush := false;
          try
            for Idx := 0 to Pred(Transactions.Count) do
            begin
              Transaction := TMemDbTransaction(Transactions[idx]);
              Assert(Transaction.Mode in [amWriteExclusive, amReadWriteShared]);
              DoFlush := DoFlush or (Transaction.Sync = amFlushBuffers);
              CopyTransactionStreamToOutput;
            end;
            if DoFlush then
            begin
      {$IFDEF WINDOWS_FLUSH}
              if OutputStream is TWriteCachedFileStream then
              begin
                (OutputStream as TWriteCachedFileStream).FlushCache;
                FlushOK := FlushFileBuffers((OutputStream as TWriteCachedFileStream).Handle);
              end
              else
                FlushOK := FlushFileBuffers((OutputStream as TFileStream).Handle);
      {$ELSE}
              FlushOK := (fsync(Integer((OutputStream as TFileStream).Handle)) = 0);
      {$ENDIF}
              if not FlushOK then
                raise EMemDBException.Create(S_FLUSH_FAILED + FileName);
            end;
          finally
            OutputStream.Free;
          end;
          if not RenameFile(FileName, FinalFileName) then
            raise EMemDBException.Create(S_FILE_RENAME_FAILED + FileName);
          result := true; //Errors in this path caught via exceptions.
        finally
          //Increment even on failure, because async stop not immediate.
          Inc(FWriteSeq);
        end;
      end;
    finally
      //Even in failure cases, must unblock transactions to allow for later
      //DB cleardown.
      for Idx := 0 to Pred(Transactions.Count) do
      begin
        Transaction := TMemDbTransaction(Transactions[idx]);
        ErrorDropTransaction(Transaction);
      end;
    end;
  except
    on E: Exception do
    begin
      result := false;
      ErrMsg := S_EXCEPTION_TRANS_WRITE + E.Message;
    end;
  end;
  if not result then
    DoJournalError(ErrMsg);
end;

procedure TMemDbDefaultJournal.PerformCheckpoint(Action: TMemDBJournalAction; JState: TJournalState);
var
  FileName, FinalFileName: string;
  OutputStream: TStream;
  result: boolean;
  ErrMsg: string;
  ActionStream: TStream;
  FlushOK: boolean;
begin
  result := false;
  try
    try
      Assert(JState in [jscRunning, jscRunningError, jscFinishing, jscFinishingError]);
      if JState in [jscRunning, jscFinishing] then
      begin
        try
          FileName := FBaseDirectory + IntToStr(FWriteSeq) + FileExts[jftInitOrCheckpointTemp];
          FinalFileName := FBaseDirectory + IntToStr(FWriteSeq) + FileExts[jftInitOrCheckpoint];
      {$IFDEF MSWINDOWS}
          //Make it slightly less likely ENoMem destroys journal thread.
          try
            OutputStream := TWriteCachedFileStream.Create(FileName, FILE_CACHE_SIZE);
          except
            //Streams bulk-copied from, performance hit not so bad in the out of
            //memory case.
            on E:EOutOfMemory do
              OutputStream := TFileStream.Create(FileName, fmCreate or fmShareExclusive);
          end;
      {$ELSE}
          OutputStream := TWriteCachedFileStream.Create(FileName, fmCreate);
      {$ENDIF}
          try
            Assert(Assigned(Action));
            Assert(Assigned(Action.ActionObj));
            Assert(Action.ActionObj is TStream);
            ActionStream := Action.ActionObj as TStream;
            ActionStream.Seek(0, soFromBeginning);
            OutputStream.CopyFrom(ActionStream, ActionStream.Size);
    {$IFDEF WINDOWS_FLUSH}
            if OutputStream is TWriteCachedFileStream then
            begin
              (OutputStream as TWriteCachedFileStream).FlushCache;
              FlushOK := FlushFileBuffers((OutputStream as TWriteCachedFileStream).Handle);
            end
            else
              FlushOK := FlushFileBuffers((OutputStream as TFileStream).Handle);
    {$ELSE}
            FlushOK := (fsync(Integer((OutputStream as TFileStream).Handle)) = 0);
    {$ENDIF}
            if not FlushOK then
              raise EMemDBException.Create(S_FLUSH_FAILED + FileName);
          finally
            OutputStream.Free;
          end;
          if not RenameFile(FileName, FinalFileName) then
            raise EMemDBException.Create(S_FILE_RENAME_FAILED + FileName);
          result := true; //Errors on this path caught by exceptions.
        finally
          //Increment even on failure, because async stop not immediate.
          Inc(FWriteSeq);
        end;
      end
      else
      begin
        result := false;
        ErrMsg := S_SKIPPED_PREV_JOURNAL_ERROR;
      end;
    finally
      //Do free changeset data, no associated transaction obj.
      Action.ActionObj.Free;
      Action.ActionObj := nil;
    end;
  except
    on E: Exception do
    begin
      result := false;
      ErrMsg := S_EXCEPTION_CHECKPOINT_WRITE + E.Message;
    end;
  end;
  if not result then
    DoJournalError(ErrMsg);
end;

procedure TMemDbDefaultJournal.PerformLastAction(Action: TMemDBJournalAction);
begin
  //Nothing to do here at the moment? Don't think so.
  //Might write a final "closed DB" transaction at some point,
  //if we want belt and braces.
end;

end.
