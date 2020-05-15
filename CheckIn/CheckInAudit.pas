unit CheckInAudit;

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
  SysUtils, DLThreadQueue, SyncObjs, Classes;

type
{$IFDEF USE_TRACKABLES}
  TAuditLogEntry = class(TTrackable)
{$ELSE}
  TAuditLogEntry = class
{$ENDIF}
  public
    Timestamp: TDateTime;
    Username: string;
    Details: string;
  end;

  TAuditLogPersistEvent = procedure(Sender: TObject;
                                    const LogEntries: TList) of object;
  TAuditLogPruneEvent = procedure(Sender: TObject;
                                  const LogEntries: TList;
                                  Before: TDateTime) of object;
  TAuditLogGetLastPruneEvent = procedure(Sender: TObject;
                                         var LastPrune: TDateTime) of object;

{$IFDEF USE_TRACKABLES}
  TAuditLog = class(TTrackable)
{$ELSE}
  TAuditLog = class
{$ENDIF}
  private
    FQuitFlag: boolean;
    FQueue: TDLProxyThreadQueuePublicLock;
    FLock: TCriticalSection;
    FOnPersistRecentToDB: TAuditLogPersistEvent;
    FOnPruneDBToFile: TAuditLogPruneEvent;
    FOnGetLastPrune: TAuditLogGetLastPruneEvent;
    FLastPersistEvent: TDateTime;
    FLastPruneEvent: TDateTime;
    FArchiveFilePrefix: string;
  protected
    procedure DoPersistRecent;
    procedure DoPruneDB(Before: TDateTime);
    procedure UpdateLastPruneEvent;
  public
    constructor Create;
    destructor Destroy; override;
    procedure AuditLog(Username: string; Time: TDateTime; Details: string);
    procedure DoPeriodic;
    procedure Flush; //Belt and braces - not sure about finalization order...

    property ArchiveFilePrefix: string read FArchiveFilePrefix write FArchiveFilePrefix;
    property OnPersistRecentToDb: TAuditLogPersistEvent read FOnPersistRecentToDB write FOnPersistRecentToDB;
    property OnPruneDBToFile: TAuditLogPruneEvent read FOnPruneDBToFile write FOnPruneDBToFile;
    property OnGetLastPrune: TAuditLogGetLastPruneEvent read FOnGetLastPrune write FOnGetLastPrune;
  end;

procedure AudLog(Username: string; Details: string);

var
  GAuditLog: TAuditLog;

implementation

uses
  GlobalLog, CheckInAppConfig, BufferedFileStream;

const
{$IFOPT C+}
  PERSIST_FREQUENCY = 1 / (60 * 60 * 24); //Persist logs to DB every second.
  PRUNE_FREQUENCY = 1; //Prune old logs from DB once per day.
{$ELSE}
  PERSIST_FREQUENCY = 5 (60 * 60 * 24); //Persist logs to DB every five seconds.
  PRUNE_FREQUENCY = 14; //Prune old logs from DB once every couple of weeks.
{$ENDIF}

constructor TAuditLog.Create;
begin
  inherited;
  FQueue := TDLProxyThreadQueuePublicLock.Create;
  FLock := TCriticalSection.Create;
  FQueue.Lock := FLock;
end;

destructor TAuditLog.Destroy;
var
  Obj: TObject;
begin
  FQuitFlag := true;
  FQueue.AcquireLock;
  try
    Obj := FQueue.RemoveHeadObj;
    while Assigned(Obj) do
    begin
      Obj.Free;
      Obj := FQueue.RemoveHeadObj;
    end;
  finally
    FQueue.ReleaseLock;
  end;
  FQueue.Lock := nil;
  FLock.Free;
  FQueue.Free;
  inherited;
end;

procedure TAuditLog.AuditLog(Username: string; Time: TDateTime; Details: string);
var
  ALog: TAuditLogEntry;
begin
  if not FQuitFlag then
  begin
    ALog := TAuditLogEntry.Create;
    ALog.Username := Username;
    ALog.Timestamp := Time;
    ALog.Details := Details;
    FQueue.AcquireLock;
    try
      if not Assigned(FQueue.AddTailobj(ALog)) then
      begin
        ALog.Free;
        Assert(false);
      end;
    finally
      FQueue.ReleaseLock;
    end;
  end;
end;

procedure TAuditLog.DoPersistRecent;
var
  List: TList;
  Obj: TObject;
  i: integer;
  OK: boolean;
begin
  if Assigned(FOnPersistRecentToDB) then
  begin
    FQueue.AcquireLock;
    try
      OK := FQueue.Count > 0;
    finally
      FQueue.ReleaseLock;
    end;

    if OK then
    begin
      List := TList.Create;
      try
        FQueue.AcquireLock;
        try
          Obj := FQueue.RemoveHeadObj;
          while Assigned(Obj) do
          begin
            List.Add(Obj);
            Obj := FQueue.RemoveHeadObj;
          end;
        finally
          FQueue.ReleaseLock;
        end;
        FOnPersistRecentToDB(self, List);
      finally
        for i := 0 to Pred(List.Count) do
          TObject(List.Items[i]).Free;
        List.Free;
      end;
    end;
  end;
end;

procedure TAuditLog.DoPruneDB(Before: TDateTime);

  function MakeFileSuffix: string;
  const
    ExclSet: TSysCharSet = ['.', '\', '/', ' ', ':'];
  var
    i: integer;
  begin
    result := DateTimeToStr(Before);
    for i := 1 to Length(result) do
    begin
      if CharInSet(result[i], ExclSet) or (Ord(result[i]) > 255) then
        result[i] := '_';
    end;
  end;

var
  List: TList;
  FileWriteStr: string;
  FileName: string;
  FileStream: TWriteCachedFileStream;
  i: integer;
  Log: TAuditLogEntry;
begin
  //For the moment, write in UTF-8 unicode.
  if Assigned(FOnPruneDBToFile) then
  begin
    List := TList.Create;
    try
      FOnPruneDBToFile(self, List, Before);
      FileName := RootDir + FArchiveFilePrefix
        + MakeFileSuffix + '.audit.log';
      FileStream := TWriteCachedFileStream.Create(FileName);
      try
        for i := 0 to Pred(List.Count) do
        begin
          Log := TObject(List.Items[i]) as TAuditLogEntry;
          FileWriteStr :=  DateTimeToStr(Log.Timestamp)
            + #9 + Log.Username + ': ' + #9 + Log.Details + #13 + #10;
          FileStream.Write(FileWriteStr[1],
            Length(FileWriteStr) * sizeof(FileWriteStr[1]));
        end;
      finally
        FileStream.Free;
      end;
    finally
      for i := 0 to Pred(List.Count) do
        TObject(List.Items[i]).Free;
      List.Free;
    end;
  end;
end;

procedure TAuditLog.UpdateLastPruneEvent;
begin
  if Double(FLastPruneEvent) = 0.0 then
  begin
    if Assigned(FOnGetLastPrune) then
      FOnGetLastPrune(self, FLastPruneEvent)
    else
      FLastPruneEvent := Now;
  end;
end;

procedure TAuditLog.DoPeriodic;
var
  RightNow: TDateTime;
  Before: TDateTime;
begin
  RightNow := Now;
  if RightNow > FLastPersistEvent + PERSIST_FREQUENCY then
  begin
    FLastPersistEvent := RightNow;
    DoPersistRecent;
  end;
  UpdateLastPruneEvent;
  if RightNow > FLastPruneEvent + PRUNE_FREQUENCY then
  begin
    Before := FLastPruneEvent;
    FLastPruneEvent := RightNow;
    DoPruneDB(Before);
  end;
end;

procedure TAuditLog.Flush;
begin
  DoPersistRecent;
end;

procedure AudLog(Username: string; Details: string);
begin
  GAuditLog.AuditLog(Username, Now, Details);
end;

initialization
{$IFOPT C+}
  AppGlobalLog.OpenFileLog('c:\temp\checkin.log');
{$ELSE}
  AppGlobalLog.OpenFileLog(RootDir + 'checkin.log');
{$ENDIF}
  GAuditLog := TAuditLog.Create;
finalization
  GAuditLog.Free;
  GAuditLog := nil;
end.
