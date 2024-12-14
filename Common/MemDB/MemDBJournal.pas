unit MemDBJournal;
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

{$DEFINE JOURNAL_WRITE_FLOW_CONTROL}
//Journal recovery handles the situation where the last file is blank,
//but not where it is corrupted. That's as bad as I expect situations to be
//in cases involving power loss on NTFS. Anything worse is h/w failure.
//If you need something better, mod the code yourself.


uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  Classes, DLThreadQueue, MemDBStreamable, SyncObjs, MemDBMisc;

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
                        jscFinishing, //Final checkpoint, last action.
                        jscFinished); //No more actions, awaiting destruction.

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
    procedure DoJournalFinished;
  protected
    procedure DoJournalReplay(Changesets: TObject; Initial: boolean);
    procedure DoJournalWriteFlush(Transaction: TObject);
    procedure DoJournalError(ErrMsg: string);

    function PerformInitialLoad(Action: TMemDbJournalAction;
      var AnyFiles: boolean;
      var CreateCheckpoint: boolean; var ErrMsg:string): boolean; virtual; abstract;
    procedure PerformMultiTransactionWrite(Transactions: TList); virtual;abstract;
    procedure PerformCheckpoint(Action: TMemDBJournalAction); virtual; abstract;
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
    procedure PerformMultiTransactionWrite(Transactions: TList); override;
    procedure PerformCheckpoint(Action: TMemDBJournalAction); override;
    procedure PerformLastAction(Action: TMemDBJournalAction); override;
  public
    property BaseDirectory:string read FBaseDirectory write FBaseDirectory;
    property JournalType: TMemDBJournalType read FJournalType write FJournalType;
  end;

//TODO - V2 changesets:
//Generally a memory stream if not too large, unless
//a checkpoint, in which case assume may be huge, and use a temporary file.

implementation

uses MemDB, Windows, SysUtils, BufferedFileStream;

const
  JOURNAL_PENDING_QUEUE_LIMIT = 4096;
  MAX_INCR_JOURNAL_FILES = 10 * 1024;
  S_COULDNT_CREATE_DIR = 'Couldn''t create or open directory: ';
  S_COULDNT_FIND_EXPECTED_DB_FILES = 'Couldn''t find sequentially numbered db files.';
  S_DB_FILES_NUMBERING_BAD = 'Db file numbering and types disagree.';
  S_CORRUPTED_FILE = 'Corrupted file: ';
  S_EXCEPTION = 'Internal error, exception: ';
  S_STREAM_SYSTEM_INTERNAL = 'Stream system internal error writing to file.';

  CHECKPOINT_RATIO = 10; { More than 1/10th data in journal, re-checkpoint }

type
  TJournalFileType = (jftInitOrCheckpoint, jftIncremental);
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
  FileExts: TJournalFileExts = ('.dbinit', '.dbincr');

{ TMemDBJournalThread }

procedure TMemDBJournalThread.Execute;
begin
  FParentJournal.ThreadFunc;
end;

{ TMemDBJournal }

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
    Assert(FState = jscRunning);
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
      FState := jscFinished;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
  if Assigned(FOnJournalInitialized) then
    FOnJournalInitialized(self, OK, AnyFiles, CreateCheckpoint, ErrMsg);
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

procedure TMemDBJournal.DoJournalFinished;
begin
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState = jscFinishing);
    FState := jscFinished;
    FJournalThread.FreeOnTerminate := true;
    FJournalThread := nil;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
  if Assigned(FOnJournalFinished) then
    FOnJournalFinished(self);
end;

procedure TMemDbJournal.DoJournalError(ErrMsg: string);
begin
  FActionsPendingQueue.AcquireLock;
  try
    Assert(FState = jscRunning);
    FState := jscFinished;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
  if Assigned(FOnJournalError) then
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
  AmalgamatedTrans: TList;
  Action: TMemDBJournalAction;
  T: TMemDBTransaction;
  OK: boolean;
  AnyFiles: boolean;
  CreateCheckpoint: boolean;
  Sync: TMDBSyncMode;
  ErrMsg: string;
begin
  while (FState <> jscFinished) do
  begin
    FHighPriorityEvent.WaitFor(JOURNAL_THREAD_POLL_LATENCY);
    //Pull things out of the Action pending queue.
    //We can amalgamate commits where none of the transactions have
    //the write-back flag set. Checkpoints also not amalgamated.
    FActionsPendingQueue.AcquireLock;
    try
      Action := FActionsPendingQueue.RemoveHeadObj as TMemDbJournalAction;
      Assert(Assigned(Action) or (FHighPriorityActionCount = 0));
    finally
      FActionsPendingQueue.ReleaseLock;
    end;
    if Assigned(Action) then
    begin
      try
        case Action.ActionType of
          jatInitialLoad:
          begin
            OK := PerformInitialLoad(Action, AnyFiles, CreateCheckpoint, ErrMsg);
            HighPriorityDone;
            DoJournalInitialized(OK, AnyFiles, CreateCheckpoint, ErrMsg);
          end;
          jatCommitTransaction:
          begin
            AmalgamatedTrans := TList.Create;
            try
              T := Action.ActionObj as TMemDBTransaction;
              Sync := T.Sync;
              AmalgamatedTrans.Add(T);
              Action.Free;
              Action := nil;
              //Optimization, batch bunches of lazy write transactions together.
              if Sync = amLazyWrite then
              begin
                FActionsPendingQueue.AcquireLock;
                try
                  Action := FActionsPendingQueue.PeekHeadObj as TMemDBJournalAction;
                  while Assigned(Action)
                    and (Action.ActionType = jatCommitTransaction)
                    and ((Action.ActionObj as TMemDbTransaction).Sync = amLazyWrite) do
                  begin
                    Action := FActionsPendingQueue.RemoveHeadObj as TMemDBJournalAction;
                    T := Action.ActionObj as TMemDBTransaction;
                    AmalgamatedTrans.Add(T);
                    Action.Free;
                    Action := FActionsPendingQueue.PeekHeadObj as TMemDBJournalAction;
                  end;
                  Action := nil; //Might be a peek that we don't want to use.
                finally
                  FActionsPendingQueue.ReleaseLock;
                end;
              end
              else
              begin
                Assert(Sync = amFlushBuffers);
                HighPriorityDone;
              end;
              PerformMultiTransactionWrite(AmalgamatedTrans);
            finally
              AmalgamatedTrans.Free;
            end;
          end;
          jatCheckPoint: PerformCheckPoint(Action);
          jatLastAction:
          begin
            PerformLastAction(Action);
            HighPriorityDone;
            DoJournalFinished;
          end
        else
          Assert(false);
        end;
      finally
        Action.Free;
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
  Assert(FState = jscFinished);
  if Assigned(FJournalThread) then
  begin
    FJournalThread.WaitFor;
    //Mem barrier - FJournalThread may free itself.
    FActionsPendingQueue.AcquireLock;
    FActionsPendingQueue.ReleaseLock;
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
begin
  FActionsPendingQueue.AcquireLock;
  try
    Assert((FState = jscCreated) or (FState = jscFinished));
    if FState = jscFinished then FState := jscCreated;
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
    Assert(FState = jscRunning);
    Action := TMemDbJournalAction.Create;
    Action.ActionType := jatCommitTransaction;
    Action.ActionObj := T;
    FActionsPendingQueue.AddTailObj(Action);
    Assert(T.Mode = amReadWrite); //No read-only transactions here plz!
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
    Assert(FState = jscRunning);
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
    Assert(FState = jscRunning);
    Action := TMemDbJournalAction.Create;
    Action.ActionType := jatLastAction;
    FActionsPendingQueue.AddTailObj(Action);
    Inc(FHighPriorityActionCount);
    FHighPriorityEvent.SetEvent;
    FState := jscFinishing;
  finally
    FActionsPendingQueue.ReleaseLock;
  end;
end;

procedure TMemDBJournal.SynchronizeStateChange;
begin
  if Assigned(FJournalThread) then //Not always around at shutdown.
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
    result := -1;
end;

function TMemDbDefaultJournal.GetInitialFileList: TList;
var
  SearchStr: string;
  FileType: TJournalFileType;
  FindHandle: THandle;
  FindData: TWin32FindDataW;
  FileName: string;
  RList: TList;
  FileSeq: UInt64;
  DBInfo: TDBFileInfo;
begin
  RList := TList.Create;
  result := nil;
  try
    for FileType := Low(FileType) to High(FileType) do
    begin
      SearchStr := FBaseDirectory + '*' + FileExts[FileType];
      FindHandle := FindFirstFileW(@SearchStr[1], FindData);
      while FindHandle <> INVALID_HANDLE_VALUE do
      begin
        //Filter out things which are not regular files.
        if (FindData.dwFileAttributes and (FILE_ATTRIBUTE_SYSTEM or
          FILE_ATTRIBUTE_DIRECTORY or FILE_ATTRIBUTE_DEVICE)) = 0 then
        begin
          //Check filename is some sort of UInt64 we understand, if not, throw out.
          FileName := FindData.cFileName; //Maybe ....
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
            DBInfo.FileSize := FindData.nFileSizeLow or
              (UInt64(FindData.nFileSizeHigh) shl 32);
            RList.Add(DBInfo)
          except
            on E: EConvertError do ; //Continue round loop.
          end;
        end;
        if not FindNextFileW(FindHandle, FindData) then
          FindHandle := INVALID_HANDLE_VALUE;
      end;
    end;
    RList.Sort(CompareDBInfos);
    result := RList;
    RList := nil;
  finally
    RList.Free;
  end;
end;

procedure TMemDbDefaultJournal.FreeRList(var RList: TList);
var
  LocalIdx: integer;
begin
  for LocalIdx := 0 to Pred(RList.Count) do
    TObject(RList[LocalIdx]).Free;
  RList.Free;
  RList := nil;
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
    InitialSeq := 0;
    FinalSeq := 0;
    if RList.Count = 0 then
    begin
      result := true;
      exit;
    end;

    Found := false;
    StartIdx := 0;
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
      if (DBInfo.FileType <> jftIncremental) or
         (DBInfo.FileSeq <> FinalSeq) then
        exit; //Result = false.
      Inc(FinalSeq);
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
  //Cleanup files before the initial sequence number we started loading from.
  for Idx := 0 to Pred(RList.Count) do
  begin
    DBInfo := TDBFileInfo(RList[Idx]);
    if DBInfo.FileSeq < InitialSeq  then
    begin
      FileName := FBaseDirectory + IntToStr(DBInfo.FileSeq)
        + FileExts[DbInfo.FileType];
      DeleteFile(FileName);
    end;
  end;
end;

function TMemDbDefaultJournal.PerformInitialLoad(Action: TMemDbJournalAction;
  var AnyFiles: boolean; var CreateCheckpoint: boolean; var ErrMsg: string): boolean;
var
  InitialSeq, FinalSeq: UInt64;
  RList: TList;
  Idx: integer;
  DBInfo: TDBFileInfo;
  Stream: TStream;
  FileName: string;
  TotalInitFileSize: UInt64;
  TotalIncrFileSize: UInt64;
  LastFileBlank: boolean;
  ReadFileCount: integer;
begin
  Assert(Assigned(Action));
  Assert(Action.ActionType = jatInitialLoad);
  //First of all, create (or open the) initial directory.
  AppendTrailingDirSlash(FBaseDirectory);
  if not CreateDirectoryW(@FBaseDirectory[1], nil) then
  begin
    if not GetLastError = ERROR_ALREADY_EXISTS then
    begin
      result := false;
      ErrMsg := S_COULDNT_CREATE_DIR + FBaseDirectory;
      exit;
    end;
  end;
  result := GetListAndInitialSeqs(RList, InitialSeq, FinalSeq);
  if not result then
  begin
    ErrMsg := S_COULDNT_FIND_EXPECTED_DB_FILES;
    exit;
  end;
  TotalInitFileSize := 0;
  TotalIncrFileSize := 0;
  LastFileBlank := false;
  try
    result := false;
    AnyFiles := false;
    ReadFileCount := 0;
    //Okay, now can do the initial load.
    for Idx := 0 to Pred(RList.Count) do
    begin
      DBInfo := TDBFileInfo(RList[Idx]);
      if (DBInfo.FileSeq >= InitialSeq) and (DBInfo.FileSeq < FinalSeq) then
      begin
        if (DBInfo.FileType = jftInitOrCheckpoint) <> (DbInfo.FileSeq = InitialSeq) then
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
          Stream := TReadOnlyCachedFileStream.Create(FileName, FILE_CACHE_SIZE);
          try
            DoJournalReplay(Stream, (DBInfo.FileSeq = InitialSeq));
          except
            on E: Exception do
            begin
              ErrMsg := S_EXCEPTION + E.Message + '(' + FileName + ')';
              exit; //Result := false;
            end;
          end;
        finally
          Stream.Free;
        end;
        case DBInfo.FileType of
          jftInitOrCheckpoint: Inc(TotalInitFileSize, DBInfo.FileSize);
          jftIncremental: Inc(TotalIncrFileSize, DBInfo.FileSize);
        else
          Assert(false);
        end;
        if (DBInfo.FileSeq = Pred(FinalSeq))
          and LastFileBlank then
        begin
          Dec(FinalSeq);
          DeleteFile(FileName);
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

procedure TMemDbDefaultJournal.PerformMultiTransactionWrite(Transactions: TList);
var
  result: boolean;
  OutputStream: TStream;
  FileName: string;
  Idx: integer;
  Transaction: TMemDBTransaction;
  DoFlush: boolean;
  FlushOK: boolean;
  ErrMsg: string;

  procedure CopyTransactionStreamsToOutput;
  var
    i: integer;
    S: TStream;
  begin
    for i := 0 to Pred(Transaction.FinalStreams.Count) do
    begin
      S := TObject(Transaction.FinalStreams[i]) as TStream;
      S.Seek(0, TSeekOrigin.soBeginning);
      OutputStream.CopyFrom(S, S.Size);
    end;
  end;

begin
  try
    FileName := FBaseDirectory + IntToStr(FWriteSeq) + FileExts[jftIncremental];
    OutputStream := TWriteCachedFileStream.Create(FileName, FILE_CACHE_SIZE);
    DoFlush := false;
    try
      for Idx := 0 to Pred(Transactions.Count) do
      begin
        Transaction := TMemDbTransaction(Transactions[idx]);
        Assert(Transaction.Mode = amReadWrite);
        DoFlush := DoFlush or (Transaction.Sync = amFlushBuffers);
        CopyTransactionStreamsToOutput;
      end;
      if DoFlush then
      begin
        (OutputStream as TWriteCachedFileStream).FlushCache;
        FlushOK := FlushFileBuffers((OutputStream as TWriteCachedFileStream).Handle);
        Assert(FlushOK);
      end;
      for Idx := 0 to Pred(Transactions.Count) do
      begin
        Transaction := TMemDbTransaction(Transactions[idx]);
        if Transaction.Sync = amFlushBuffers then
          DoJournalWriteFlush(Transaction);
        Transaction.DecRefConditionalFree;
      end;
      result := true; //Errors in this path caught via exceptions.
    finally
      Inc(FWriteSeq);
      OutputStream.Free;
    end;
  except
    on E: Exception do
    begin
      result := false;
      ErrMsg := S_EXCEPTION + E.Message;
    end;
  end;
  if not result then
    DoJournalError(ErrMsg);
end;

//TODO - Potentially consider whether to remove copying of streams in a bit,
//depends when/where temporary stream gets created,
//whether it's a file stream, and whether we can do a suitable copy/move/rename
procedure TMemDbDefaultJournal.PerformCheckpoint(Action: TMemDBJournalAction);
var
  FileName: string;
  OutputStream: TStream;
  result: boolean;
  ErrMsg: string;
  ActionStream: TStream;
begin
  try
    FileName := FBaseDirectory + IntToStr(FWriteSeq) + FileExts[jftInitOrCheckpoint];
    OutputStream := TWriteCachedFileStream.Create(FileName, FILE_CACHE_SIZE);
    try
      try
        Assert(Assigned(Action));
        Assert(Assigned(Action.ActionObj));
        Assert(Action.ActionObj is TStream);
        ActionStream := Action.ActionObj as TStream;
        ActionStream.Seek(0, soFromBeginning);
        OutputStream.CopyFrom(ActionStream, ActionStream.Size);
        result := true; //Errors on this path caught by exceptions.
      finally
        //Do free changeset data, no associated transaction obj.
        Action.ActionObj.Free;
        Action.ActionObj := nil;
      end;
    finally
      Inc(FWriteSeq);
      OutputStream.Free;
    end;
  except
    on E: Exception do
    begin
      result := false;
      ErrMsg := S_EXCEPTION + E.Message;
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
