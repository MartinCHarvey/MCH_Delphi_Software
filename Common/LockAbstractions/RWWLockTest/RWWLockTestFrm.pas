unit RWWLockTestFrm;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls;

type
  TRWWLockTestForm = class(TForm)
    SimpleBtn: TButton;
    ContendBtn: TButton;
    Memo1: TMemo;
    TimingsBtn: TButton;
    procedure SimpleBtnClick(Sender: TObject);
    procedure TimingsBtnClick(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure ContendBtnClick(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  RWWLockTestForm: TRWWLockTestForm;

implementation

{$R *.dfm}

uses RWWLock, SyncObjs;

type
  TLockStats = array[TRWWLockReason] of integer;
  PLockStats = ^TLockStats;

  TContendThread = class(TThread)
  public
    FLock: TRWWLock;
    POngoing: PLockStats;
    PHighest: PLockStats;
  protected
    procedure Execute; override;
  end;

var
  OngoingStats: TLockStats;
  HighestStats: TLockStats;


procedure TContendThread.Execute;
const
  Range = Succ(Ord(lrExclusiveWrite) - Ord(lrSharedRead));
var
 i, c : Integer;
 Reason, Reason2: TRWWLockReason;

begin
  while not Terminated do
  begin
    i := Random(Range) + Ord(lrSharedRead);
    Reason := TRWWLockReason(i);
    FLock.Acquire(Reason);
    try
      c:= InterlockedIncrement(POngoing[Reason]);
      if c > PHighest[reason] then //Not atomic, but don't mind.
        PHighest[reason] := c;
      for Reason2 := Low(Reason2) to High(Reason2) do
      begin
        if Reason2 <> Reason then
          Assert(POngoing[Reason2] = 0);
          //This however, is atomic, cos we have a lock (doh!).
      end;
      Sleep(1); //Raise contention - this loop is so tiny.
      InterlockedDecrement(POngoing[Reason]);
    finally
      FLock.Release(Reason);
    end;
  end;
end;

const
  LIMIT_THREADS = 200;
  RUNTIME_MSECS = 5000;

procedure TRWWLockTestForm.ContendBtnClick(Sender: TObject);
var
  Threads: array[0..LIMIT_THREADS] of TContendThread;
  i: integer;
  Lock: TRWWLock;
  Ongoing: TLockStats;
  Highest: TLockStats;
begin
  FillChar(Ongoing, sizeof(Ongoing), 0);
  FillChar(Highest, sizeof(Highest), 0);
  FillChar(Threads, sizeof(Threads), 0);
  Lock := TRWWLock.Create;
  try
    for i := 0 to LIMIT_THREADS do
    begin
      Threads[i] := TContendThread.Create(true);
      with Threads[i] do
      begin
        FLock := Lock;
        POngoing := @Ongoing;
        PHighest := @Highest;
        Resume;
      end;
    end;
    Sleep(RUNTIME_MSECS);
    for i := 0 to LIMIT_THREADS do
      Threads[i].Terminate;
    for i := 0 to LIMIT_THREADS do
      Threads[i].WaitFor;
    Memo1.Lines.Add('Highest lrSharedRead:' + IntToStr(Highest[lrSharedRead])
      + ' Ratio: ' + IntToStr(Lock.Ratios[lrSharedRead]));
    Memo1.Lines.Add('Highest lrSharedWrite:' + IntToStr(Highest[lrSharedWrite])
      + ' Ratio: ' + IntToStr(Lock.Ratios[lrSharedWrite]));
    Memo1.Lines.Add('Highest lrExclusiveWrite:' + IntToStr(Highest[lrExclusiveWrite])
      + ' Ratio: ' + IntToStr(Lock.Ratios[lrExclusiveWrite]));
  finally
    Lock.Free;
    for i := 0 to LIMIT_THREADS do
      Threads[i].Free;
  end;
end;

procedure TRWWLockTestForm.FormCreate(Sender: TObject);
begin
  Memo1.Lines.Clear;
end;

procedure TRWWLockTestForm.SimpleBtnClick(Sender: TObject);
var
  Lock: TRWWLock;
  i: integer;
  Reason: TRWWLockReason;
begin
  Lock := TRWWLock.Create;
  try
    for Reason := Low(Reason) to High(Reason) do
    begin
      for i := 0 to 100 do
      begin
        Lock.Acquire(Reason);
        Lock.Release(Reason);
      end;
    end;
  finally
    Lock.Free;
  end;
  Memo1.Lines.Add('Simple lock test completed.')
end;

const
  PERF_CYCLES = 1000000;
  SECS_PER_DAY = (60 * 60 *24);
  ONE_E3 = 1000;
  ONE_E6 = ONE_E3 * ONE_E3;
  ONE_E9 = ONE_E6 * ONE_E3;

function NiceStr(X:double): string;
begin
  if X > ONE_E9 then begin
    result := IntToStr(Trunc(X / ONE_E9)) + 'G';
  end  else if X > ONE_E6 then begin
    result := IntToStr(Trunc(X / ONE_E6)) + 'M';
  end  else if X > ONE_E3 then begin
    result := IntToStr(Trunc(X / ONE_E3)) + 'K';
  end
  else
    result := IntToStr(Trunc(X));
end;

procedure TRWWLockTestForm.TimingsBtnClick(Sender: TObject);
var
  Start: TDateTime;
  Elapsed: double;
  Crit: TCriticalSection;
  i: integer;
  Event: TEvent;
  Lock: TRWWLock;
  Reason: TRWWLockReason;

begin
  Crit := TCriticalSection.Create;
  try
    Start := Now;
    for i := 0 to PERF_CYCLES do
    begin
      Crit.Acquire;
      Crit.Release
    end;
    Elapsed := (Now - Start) * SECS_PER_DAY;
  finally
    Crit.Free;
  end;
  Memo1.Lines.Add('Critical section: '
    +  NiceStr(PERF_CYCLES / Elapsed)
    + ' lock cycles per second');

  Event := TEvent.Create(nil, true, false, '');
  try
    Start := Now;
    for i := 0 to PERF_CYCLES do
    begin
      Event.SetEvent;
      Event.ResetEvent;
    end;
    Elapsed := (Now - Start) * SECS_PER_DAY;
  finally
    Event.Free;
  end;
  Memo1.Lines.Add('Event: '
    +  NiceStr(PERF_CYCLES / Elapsed)
    + ' set-reset per second');

  Lock := TRWWLock.Create;
  try
    Start := Now;
    for i := 0 to PERF_CYCLES do
    begin
      Lock.Acquire(lrSharedRead);
      Lock.Release(lrSharedRead);
    end;
    Elapsed := (Now - Start) * SECS_PER_DAY;
  finally
    Lock.Free;
  end;

  Memo1.Lines.Add('RWW Lock: '
    +  NiceStr(PERF_CYCLES / Elapsed)
    + ' lock cycles (one reason, uncontended) per second');

    Lock := TRWWLock.Create;
  try
    Start := Now;
    Reason := lrSharedRead;
    for i := 0 to PERF_CYCLES do
    begin
      Lock.Acquire(Reason);
      Lock.Release(Reason);
      IncRWWReason(Reason);
    end;
    Elapsed := (Now - Start) * SECS_PER_DAY;
  finally
    Lock.Free;
  end;

  Memo1.Lines.Add('RWW Lock: '
    +  NiceStr(PERF_CYCLES / Elapsed)
    + ' lock cycles (all reasons, uncontended) per second');

end;

initialization
  Randomize;
end.
