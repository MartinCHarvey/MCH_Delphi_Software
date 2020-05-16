unit GlobalLog;
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

{
  This file defines a global logging object that can be used by any class
  in the application. It will log to file, or alternatively, to a string list.
  It also provides an event that can be handled for custom handling of
  the application's logging.
}

interface

uses SysUtils, Windows, Classes, Messages, Trackables;

const
  MinLogFileSize = 1; //In K
  MinLogStringSize = 1;
  MaxLogFileSize = 1024 * 100; //100 Meg logfile.
  MaxLogStringSize = 65535;

  S_NOT_YET_IMPL =
    ' Code path not yet required or implemented.';
  S_SPACE = ' ';

type
  TLogSeverity = (SV_TRACE, SV_INFO, SV_WARN, SV_FAIL, SV_CRIT);
                { These categories basically mean:
                  For Your Information,
                  Recoverable problem, but perhaps shouldn't have happened,
                  Unrecoverable problem (data lost, or operation aborted),
                  Internal software error.}
  TSeveritySet = set of TLogSeverity;

  TLogEvent = procedure(Sender: TObject; FinalText: string) of object;
  TIdxChangeEvent = procedure(Sender: TObject; Index: integer) of object;

  ELogException = class(Exception);

  TQueuedLog = record
    Severity: TLogSeverity;
    Msg: string;
  end;
  PQueuedLog = ^TQueuedLog;

  //Don't make this a child of TTrackable - it's the one thing
  //still in existance when mem tracking shut down.
  TGlobalLog = class
  private
    FLoggingAppException: boolean;
    FFile: TFileStream;
    FFileName: string;
    FStrings: TStrings;
    FMaxNumStrings: integer;
    FStringWritePos: integer;
    FMessageLogged: TLogEvent;
    FIdxChanged: TIdxChangeEvent;
    FSeverities: TSeveritySet;

    FCrit: TRTLCriticalSection;
    FQueuedLogs: TList;
    FShowUnhandledExceptions: boolean;
    FOnMessageToDisplay: TLogEvent;
  protected
    function GetNumStrings: integer;
    procedure SetNumStrings(NewSize: integer);
    function GetLoggingToFile: boolean;
    procedure StrIdxChanged(Ind: integer);
    function GetSeveritySet: TSeveritySet;
    procedure SetSeveritySet(NewSet: TSeveritySet);
    function GetFileName: string;
    function GetStrings: TStrings;
    procedure SetStrings(NewStrings: TStrings);
    function GetMessageLogged: TLogEvent;
    procedure SetMessageLogged(NewMessageLogged: TLogEvent);
    function GetIdxChanged: TIdxChangeEvent;
    procedure SetIdxChanged(NewChanged: TIdxChangeEvent);
    function GetStringWritePos: integer;
    procedure MessageToDisplay(Msg: string);
    procedure SetMessageToDisplay(NewMsgProc: TLogEvent);
    function GetMessageToDisplay: TLogEvent;

    procedure MainThreadLog(Severity: TLogSeverity; Text: string);
    procedure OtherThreadLog(Severity: TLogSeverity; Text: string);
    procedure OtherThreadNotify();
  public
    constructor Create();
    destructor Destroy; override;
    function OpenFileLog(FileName: string): boolean;
    procedure CloseFileLog;
    procedure Log(Severity: TLogSeverity; Text: string);
    procedure LogStream(Severity: TLogSeverity; Stream: TStream);
    procedure LogUnhandledException(Sender: TObject; E: Exception);

    property NumStrings: integer read GetNumStrings write SetNumStrings;
    property LoggingToFile: boolean read GetLoggingToFile;
    property LogFileName: string read GetFileName;

    property Strings: TStrings read GetStrings write SetStrings;
    property WritePos: integer read GetStringWritePos;

    property OnMessageLogged: TLogEvent read GetMessageLogged write
      SetMessageLogged;
    property OnStrIdxChanged: TIdxChangeEvent read GetIdxChanged write
      SetIdxChanged;
    property Severities: TSeveritySet read GetSeveritySet write SetSeveritySet;
    property ShowUnhandledExceptions: boolean
      read FShowUnhandledExceptions write FShowUnhandledExceptions;
    property OnMessageToDisplay: TLogEvent read GetMessageToDisplay
      write SetMessageToDisplay;
  end;

function GLogFormatSysError(LastError: integer): string; forward;
function AmVCLThread: boolean;

type
  TQueueThread = class(TThread)
  protected
    procedure Execute; override;
  public
    procedure Queue(AMethod: TThreadMethod);
  end;

procedure GLogLogSelf(Sender: TObject; Sev: TLogSeverity; Line: string);
procedure GLogLog(Sev: TLogSeverity; Line: string);
procedure GLogLogStream(Sev: TLogSeverity; Stream: TStream);

var
  AppGlobalLog: TGlobalLog;
  SyncToMainThread: TQueueThread;

const
  AllSeverities: TSeveritySet = [SV_TRACE, SV_INFO, SV_WARN, SV_FAIL, SV_CRIT];

implementation

uses
  IoUtils;

const
  WM_LOG_QUEUED_MSGS = WM_APP;

  S_NOT_LOGGING = 'Warning: Can''t write log entry (Logging disabled).';
  S_UNHANDLED = 'Unhandled exception: ';
  SeverityStrings: array[TLogSeverity] of string
    = ('[Trace] ', '[Info] ', '[Warn] ', '[Fail] ', '[Crit] ');
  NewLine = #13#10;
  S_LB1 = '[';
  S_LB4 = ']';
  S_TIME_FMT = ' ddddd tt ';
  S_LEAKED_INSTANCES =
    'Memory leaks detected! (Log warnings to file for details).';
  S_EXPECTED_TRACKER = 'Expected global tracker to be present, but it isn''t';
  S_STRINGS_OP_FROM_OTHER_THREAD =
    'Not allowed to manipulate log strings from non VCL thread.';
  S_EVENT_OP_FROM_OTHER_THREAD =
    'Not allowed to manipulate VCL events from non VCL thread.';
  S_THREAD_STARTED = 'Thread started: ';
  S_THREAD_FINISHED = 'Thread finished: ';
  S_ONMSG_OP_FROM_OTHER_THREAD =
    'Not allowed to manipulate log events from non VCL thread';

var
  VCLTHreadID: DWORD;

(************************************
 * TQueueThread                     *
 ************************************)

procedure TQueueThread.Queue(AMethod: TThreadMethod);
begin
  inherited Queue(AMethod);
end;

procedure TQueueThread.Execute;
begin
  //Executes, and terminates immediately, but we use queueing functionality
  //later on.
end;

(************************************
 * TGlobalLog                       *
 ************************************)


procedure TGlobalLog.SetNumStrings(NewSize: integer);
var
  Excess, NumDel, DelPos: integer;
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
    begin
      if NewSize < MinLogStringSize then NewSize := MinLogStringSize;
      if NewSize > MaxLogStringSize then NewSize := MaxLogStringSize;
      FMaxNumStrings := NewSize;
      if Assigned(FStrings) then
      begin
        //Need to cut things down to size by removing the oldest strings.
        Excess := FStrings.Count - NewSize;
        if Excess > 0 then
        begin
          FStrings.BeginUpdate;
          //Want to delete the oldest strings.
          //Very oldest is where we're about to write,
          //modulo number of strings currently in list.
          DelPos := FStringWritePos;
          NumDel := 0;
          while NumDel < Excess do
          begin
            if DelPos >= FStrings.Count then DelPos := 0;
            FStrings.Delete(DelPos);
            if FStringWritePos >= FStrings.Count then FStringWritePos := 0;
            Inc(NumDel);
          end;
          //Okay, and now indicate to interested parties where the most
          //recently written string can be found. DelPos contains oldest string.
          Dec(DelPos);
          if DelPos < 0 then //Should always be -1.
            Inc(DelPos, FStrings.Count);
          StrIdxChanged(DelPos);
          FStrings.EndUpdate;
        end;
      end;
    end
    else
      Log(SV_CRIT, S_STRINGS_OP_FROM_OTHER_THREAD);
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

function TGlobalLog.GetLoggingToFile: boolean;
begin
  EnterCriticalSection(FCrit);
  try
    result := Assigned(FFile);
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

procedure TGlobalLog.StrIdxChanged(Ind: integer);
begin
  if Assigned(FIdxChanged) then
    FIdxChanged(self, Ind);
end;

procedure TGlobalLog.SetSeveritySet(NewSet: TSeveritySet);
begin
  EnterCriticalSection(FCrit);
  try
    FSeverities := NewSet + [SV_CRIT];
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

procedure TGlobalLog.CloseFileLog;
begin
  EnterCriticalSection(FCrit);
  try
    FFile.Free;
    FFile := nil;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

procedure TGlobalLog.LogUnhandledException(Sender: TObject; E: Exception);
begin
  if not (E is EAbort) then
  begin
    EnterCriticalSection(FCrit);
    try
      if not FLoggingAppException then
      begin
      { No try-finally block here. In the unpleasant case where logging an
        unhandled exception causes an unhandled exception, probably best
        to disable logging of unhandled exceptions! }
        FLoggingAppException := true;
        Log(SV_CRIT, S_UNHANDLED + E.Message);
        FLoggingAppException := false;
      end;
    finally
      LeaveCriticalSection(FCrit);
    end;
  end;
end;

constructor TGlobalLog.Create;
begin
  inherited;
  InitializeCriticalSection(FCrit);
  FQueuedLogs := TList.Create;
end;

destructor TGlobalLog.Destroy;
begin
  CloseFileLog;
  FQueuedLogs.Free;
  DeleteCriticalSection(FCrit);
  inherited;
end;

function TGlobalLog.OpenFileLog(FileName: string): boolean;
begin
  EnterCriticalSection(FCrit);
  try
    if Assigned(FFile) then
    begin
      FFile.Free;
      FFile := nil;
    end;
    try
      FFileName := FileName;
      FFile := TFileStream.Create(FileName, fmCreate or fmShareDenyWrite);
      result := true;
    except
      on EStreamError do result := false;
    end;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

procedure TGlobalLog.MainThreadLog(Severity: TLogSeverity; Text: string);
var
  FinString: string;
  AnsiFinString: AnsiString;
  TimeString: string;
  LineLen: integer;

begin
  //No critical section here - already in it.
  if not (Severity in FSeverities) then exit;
  DateTimeToString(TimeString, S_TIME_FMT, Now);
  FinString := TimeString.Trim + SeverityStrings[Severity] + Text;
  if Assigned(FStrings) then
  begin
    if FStringWritePos >= FStrings.Count then
      FStrings.Add(FinString)
    else
      FStrings.Strings[FStringWritePos] := FinString;
    Inc(FStringWritePos);
    if FStringWritePos >= FMaxNumStrings then FStringWritePos := 0;
  end;
  if Assigned(FFile) then
  begin
    AnsiFinString := UTF8Encode(FinString);
    AnsiFinString := AnsiFinString + NewLine;
    LineLen := Length(AnsiFinString);
    FFile.WriteBuffer(AnsiFinString[1], LineLen);
  end;
  if (Severity = SV_CRIT) then
  begin
{$IFNDEF DEBUG}
    if FShowUnhandledExceptions then
      MessageToDisplay(Text);
{$ELSE}
    if not FLoggingAppException then
    begin
      try
        Assert(false, Text);
      except on EAssertionFailed do
      end;
    end;
{$ENDIF}
  end;
end;

function TGlobalLog.GetNumStrings: integer;
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      result := FMaxNumStrings
    else
    begin
      result := 0;
      Log(SV_CRIT, S_STRINGS_OP_FROM_OTHER_THREAD);
    end;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

function TGlobalLog.GetSeveritySet: TSeveritySet;
begin
  EnterCriticalSection(FCrit);
  try
    result := FSeverities;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

function TGlobalLog.GetFileName: string;
begin
  EnterCriticalSection(FCrit);
  try
    result := FFileName;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

function TGlobalLog.GetStrings: TStrings;
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      result := FStrings
    else
    begin
      result := nil;
      Log(SV_CRIT, S_STRINGS_OP_FROM_OTHER_THREAD);
    end;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

procedure TGlobalLog.SetStrings(NewStrings: TStrings);
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      FStrings := NewStrings
    else
      Log(SV_CRIT, S_STRINGS_OP_FROM_OTHER_THREAD)
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

function TGlobalLog.GetMessageLogged: TLogEvent;
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      result := FMessageLogged
    else
    begin
      result := nil;
      Log(SV_CRIT, S_EVENT_OP_FROM_OTHER_THREAD);
    end;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

procedure TGlobalLog.SetMessageLogged(NewMessageLogged: TLogEvent);
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      FMessageLogged := NewMessageLogged
    else
      Log(SV_CRIT, S_EVENT_OP_FROM_OTHER_THREAD);
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

function TGlobalLog.GetIdxChanged: TIdxChangeEvent;
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      result := FIdxChanged
    else
    begin
      result := nil;
      Log(SV_CRIT, S_EVENT_OP_FROM_OTHER_THREAD);
    end;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

procedure TGlobalLog.SetIdxChanged(NewChanged: TIdxChangeEvent);
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      FIdxChanged := NewChanged
    else
      Log(SV_CRIT, S_EVENT_OP_FROM_OTHER_THREAD);
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

function TGlobalLog.GetStringWritePos: integer;
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      result := FStringWritePos
    else
    begin
      result := 0;
      Log(SV_CRIT, S_STRINGS_OP_FROM_OTHER_THREAD);
    end;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

procedure TGlobalLog.OtherThreadLog(Severity: TLogSeverity; Text: string);
var
  QueuedItem: PQueuedLog;
  WakeupVCL: boolean;
begin
  //No need for critical section - we're already in one.
  WakeupVCL := FQueuedLogs.Count = 0;
  New(QueuedItem);
  QueuedItem.Severity := Severity;
  QueuedItem.Msg := Text;
  FQueuedLogs.Add(QueuedItem);
  if WakeupVCL then
    SyncToMainThread.Queue(self.OtherThreadNotify);
end;

procedure TGlobalLog.Log(Severity: TLogSeverity; Text: string);
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      MainThreadLog(Severity, Text)
    else
      OtherThreadLog(Severity, Text);
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

procedure TGlobalLog.LogStream(Severity: TLogSeverity; Stream: TStream);
var
  AnsiS: AnsiString;
  InitPos: integer;
begin
  //TODO - update for poper unicode handling, 64 bit stream sizes.
  if Assigned(Stream) then
  begin
    InitPos := Stream.Position;
    SetLength(AnsiS, Stream.Size);
    Stream.Seek(0, soFromBeginning);
    Stream.Read(AnsiS[1], Stream.Size);
    Stream.Seek(InitPos, soFromBeginning);
    Log(Severity, AnsiS); //Converts to wchars.
  end;
end;

procedure TGlobalLog.OtherThreadNotify();
var
  Idx: integer;
  QItem: PQueuedLog;
begin
  EnterCriticalSection(FCrit);
  try
    for Idx := 0 to Pred(FQueuedLogs.Count) do
    begin
      QItem := PQueuedLog(FQueuedLogs.Items[Idx]);
      Log(QItem.Severity, QItem.Msg);
      Dispose(QItem);
    end;
    FQueuedLogs.Clear;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

procedure TGlobalLog.MessageToDisplay(Msg: string);
begin
  if Assigned(FOnMessageToDisplay) then
    FOnMessageToDisplay(self, Msg);
end;

procedure TGlobalLog.SetMessageToDisplay(NewMsgProc: TLogEvent);
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      FOnMessageToDisplay := NewMsgProc
    else
      Log(SV_CRIT, S_ONMSG_OP_FROM_OTHER_THREAD);
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

function TGlobalLog.GetMessageToDisplay: TLogEvent;
begin
  EnterCriticalSection(FCrit);
  try
    if AmVCLThread then
      result := FOnMessageToDisplay
    else
    begin
      result := nil;
      Log(SV_CRIT, S_ONMSG_OP_FROM_OTHER_THREAD);
    end;
  finally
    LeaveCriticalSection(FCrit);
  end;
end;

(************************************
 * Global functions                 *
 ************************************)

procedure CleanupChecking;
var
  TrackInfo: TStrings;
  Idx: integer;
  ErrStr: string;
begin
  { By this stage, we expect that just about everything else except the app
    tracker has been cleaned up, and so we can check that all trackable
    objects have been freed }
{$IFOPT C+}
  if Assigned(AppGlobalTracker) then
  begin
    if AppGlobalTracker.HasItems then
    begin
      ErrStr := S_LEAKED_INSTANCES;
      AppGlobalLog.Log(SV_WARN, ErrStr);
      TrackInfo := AppGlobalTracker.GetTrackedDetails;
      try
        for Idx := 0 to Pred(TrackInfo.Count) do
          AppGlobalLog.Log(SV_WARN, TrackInfo.Strings[Idx]);
      finally
        TrackInfo.Free;
      end;
    end;
  end
  else
    AppGlobalLog.Log(SV_CRIT, S_EXPECTED_TRACKER);
{$ENDIF}
end;

procedure GLogLog(Sev: TLogSeverity; Line: string);
begin
  AppGlobalLog.Log(Sev, Line);
end;

procedure GLogLogStream(Sev: TLogSeverity; Stream: TStream);
begin
  AppGlobalLog.LogStream(Sev, Stream);
end;

procedure GLogLogSelf(Sender: TObject; Sev: TLogSeverity; Line: string);
begin
{$IFOPT C+}
  AppGlobalLog.Log(Sev, S_LB1 + Sender.ClassName + S_LB4 + Line)
{$ELSE}
  AppGlobalLog.Log(Sev, Line);
{$ENDIF}
end;

function GLogFormatSysError(LastError: integer): string;
const
  S_NEWLINE = #13 + #10;
var
  SysError: PChar;
  StrSysError: string;
  SL: integer;
begin
  FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER or FORMAT_MESSAGE_FROM_SYSTEM,
    nil, LastError, 0, @SysError, 0, nil);
  StrSysError := StrPas(SysError); //Copies.
  SL := Pos(S_NEWLINE, StrSysError);
  while SL > 0 do
  begin
    Delete(StrSysError, SL, 2);
    Insert(S_SPACE, StrSysError, SL);
    SL := Pos(S_NEWLINE, StrSysError);
  end;
  LocalFree(Cardinal(SysError));
  result := Trim(StrSysError);
end;

function AmVCLThread: boolean;
begin
  result := GetCurrentThreadID = VCLThreadID;
end;

var
  TmpName: string;

initialization
  VCLThreadID := GetCurrentThreadID;
  AppGlobalLog := TGlobalLog.Create;
  AppGlobalLog.SetSeveritySet([SV_TRACE, SV_INFO, SV_WARN, SV_FAIL, SV_CRIT]);
  TmpName := TPath.GetTempFileName;
{$IFOPT C+}
  AppGlobalLog.OpenFileLog(TmpName + '.log');
{$ENDIF}
  SyncToMainThread := TQueueThread.Create;
finalization
{$IFOPT C+}
  CleanupChecking;
{$ENDIF}
  AppGlobalLog.Free;
  AppGlobalLog := nil;
  SyncToMainThread.Free;
  SyncToMainThread := nil;
  SysUtils.DeleteFile(TmpName)
end.

