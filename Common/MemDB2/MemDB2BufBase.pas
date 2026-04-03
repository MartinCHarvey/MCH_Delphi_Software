unit MemDB2BufBase;
{

Copyright � 2026 Martin Harvey <martin_c_harvey@hotmail.com>

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

{
  In memory database.
  Foundation Classes and Datastructures which are buffered,
  and create or consume journal items.
}

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  MemDB2Misc, Classes, MemDb2Streamable, DLList, TinyLock, LockAbstractions;

type
  TMemDBTransReason = (mtrUserOp,
                       mtrReplayFromScratch,
                       mtrReplayFromJournal);

{
   Isolation / serialization

   Done:
   1. Minimum isolation levels for Metadata / Implied row changes.
     Now done.  => ReadCommitted. RMW race free.
     Addition lock for table restructure.

   TODO - Serializable.
   2. Serializable isolation:
      - Need row addition lock from start of Txion:
        To prevent phantoms & keep index traversal consistent.
      - Need all meta/index pinning to happen atomically at Txion start.
}

  //Object which can write changes between current and next version to journal.
{$IFDEF USE_TRACKABLES}
  TMemDBJournalCreator = class(TTrackable)
{$ELSE}
  TMemDBJournalCreator = class
{$ENDIF}
  protected
    class procedure ChangeFlagsFromPinned(PinnedCurrent, PinnedNext: TMemDBStreamable;
      var Added:boolean;
      var Changed: boolean;
      var Deleted: boolean;
      var Null: boolean);

  public
    procedure ToJournal(const Tid: TTransactionId; Stream: TStream); virtual; abstract;
    procedure ToScratch(const PseudoTid:TTransactionId; Stream: TStream); virtual; abstract;
    procedure FromJournal(const PseudoTid: TTransactionId; Stream: TStream); virtual; abstract;
    procedure FromScratch(const PseudoTid: TTransactionId; Stream: TStream); virtual; abstract;

    //In order of operation from one consistent state to another:
    //Check no A/B buffered changes. (Possibly debug only).
    //Then journal setup replay changes
    function AnyChangesForTid(const TId: TTransactionId): boolean; virtual; abstract;
     //Just for journal replay, which should be atomic sequential
     //Tid to allow a pin to determine internal structure if necessary.
    function AnyChanges(const Tid: TTransactionId): boolean; virtual; abstract;

    //TODO - Still not convinced AnyChanges (ForTid) here is the right way to do it.

    //Pins etc managed by Multi-buffered class indicating how pins/changes have changed.

    //No changes, no data, nothing pinned, is pretty much the final critera for can delete,
    //but this class is not ref counted. Hopefully will be used as final double-check
    //against referencing.

    //Check A/B buffered changes consistent and logical (and/or atomic) before commit.
    procedure PreCommit(const TId: TTransactionId; Reason: TMemDBTransReason); virtual; abstract;
    //Save to journal / Make the changes.
    procedure Commit(const TId: TTransactionId; Reason: TMemDbTransReason); virtual; abstract;
    procedure Rollback(const TId: TTransactionId; Reason: TMemDbTransReason); virtual; abstract;
  end;

  //Generally deep clone unless some list magic requires otherwise.
  TBufCloneType = (bctDeepClone, bctClone, bctRef);

  TMemDBChangeable = class(TMemDBJournalCreator)
  public
    //TODO - Check whether still need this after all is said and done.
    //TODO - Nullness here is binary-tid-local, not same as
    //multibuf nullness, which indicates empty and nothing pinned.
    procedure Delete(const TId: TTransactionId;
      MinIso: TMDBIsolationLevel = Low(TMDBIsolationLevel)); virtual; abstract;
    procedure RequestChange(const Tid: TTransactionId;
      MinIso: TMDBIsolationLevel = Low(TMDBIsolationLevel)); virtual; abstract;
    procedure RequestReffedChange(const Tid: TTransactionId;
      MinIso: TMDBIsolationLevel = Low(TMDBIsolationLevel)); virtual; abstract;
    procedure DirectSetNext(const Tid: TTransactionID; Data: TMemDbStreamable;
      MinIso: TMDBIsolationLevel = Low(TMDBIsolationLevel)); virtual; abstract;
  end;

  //OK, let's think about what referencing information we need, when
  //determining how long / when to keep things alive.

  //Metadata:
  //
  // Needed for referencing:
  // - Has data or changes or pins.
  //
  // Needed for change / other tracking.
  // - None needed, always precommit / commit / rollback

  //Row data:
  //
  // Needed for referencing (global):
  // - Has data or changes or pins.

  //
  // Needed for row management: (Which rows to commit / rollback / clear pins).
  // - Has changes or pins for a Tid.

  //Locking and timing of posting events.
  //
  // Assemble before and after under lock.
  // Can post events out of lock? Would like to because TTinyLock not recursive.

  // Row management definitely can, because change/pin for Tid definitely
  // occurs in context of thread handling that tid.

  // Referencing, think also can (ish), because notified before returned to caller.
  // there's just enough synchronization around that we're not gonna get
  // notification events out of order.

  TReferenceUpdate = record
   Pre: boolean;
   Post: boolean;
  end;

  TTidUpdate = record
    Ref: TReferenceUpdate; //Actually changes or pins.
    Tid: TTransactionId;
  end;

{$IFOPT C+}
  TTidReference = class(TTrackable)
   Link: TDLEntry;
   Tid: TTransactionId;
  end;
  PTidReference = TTidReference;
{$ENDIF}

  //TODO - DCPHandle don't call down where it's not necessary.
  //       CPTid handling always required.

  //Assumed all these except the final handler and destructor under lock.
  TMemDBReferenceReporter = class(TMemDbChangeable)
  private
{$IFOPT C+}
    FRefLastAdded: boolean;
    FTidsReffingList: TDLEntry;
{$ENDIF}
  protected
{$IFOPT C+}
    function FindTidReffing(const Tid: TTransactionId): PTidReference;
{$ENDIF}
    procedure DCPPre(Data, Changes, Pins: boolean; var Update: TReferenceUpdate);
    procedure DCPPost(Data, Changes, Pins: boolean; var Update: TReferenceUpdate);
    procedure DCPHandle(const Update: TReferenceUpdate); virtual;

    procedure CPTidPre(Changes, Pins: boolean; const Tid: TTransactionId; var Update:TTidUpdate);
    procedure CPTidPost(Changes, Pins: boolean; const Tid: TTransactionId; var Update:TTidUpdate);
    procedure CPTidHandle(const Update: TTidUpdate); virtual;
  public
    constructor Create;
    destructor Destroy; override;
  end;

{
  General handling:

  - Current buffers can be pinned. Not quite the same as ref counting:
    - Keeps the buffer fixed until txion commit / rollback.
    - Pins cleared at commit / rollback time.

  - No unpinning. If you've read it, it probably needs to stay the
    same until commit.

  - Tid's used for pinning consistency and Next buffers.
  - Local lightweight lock.

  - Also needs pre-commit to commit lock in DB, but used *only* because
    pinning is insufficient to check global consistency
    (although may have per-key locking later...)
}

{$IFOPT C+}
  TMemDbMultiItem = class(TTrackable)
{$ELSE}
  TMemDBMultiItem = record
{$ENDIF}
    Link: TDLEntry;
    Sel: TBufSelector;
    Item: TMemDBStreamable;
    ParentTid: TTransactionId;
  end;
{$IFOPT C+}
  PMemDbMultiItem = TMemDBMultiItem;
{$ELSE}
  PMemDbMultiItem = ^TMemDbMultiItem;
{$ENDIF}

{$IFOPT C+}
  TMemDBPinnedItem = class(TTrackable)
{$ELSE}
  TMemDbPinnedItem = record
{$ENDIF}
    Link: TDLEntry;
    Item: TMemDBStreamable;
    PinnedCurrentTid: TTransactionId;
    PinnedBy: TTransactionId;
    FinalCheck: boolean;
    //Pre-commit does final checks. (whether under lock or in a more
    //distributed fashion). Once final checking, do not expect user / journal
    //ops to pin for any other reason.
  end;
{$IFOPT C+}
  PMemDBPinnedItem = TMemDBPinnedItem;
{$ELSE}
  PMemDbPinnedItem = ^TMemDbPinnedItem;
{$ENDIF}

  TPinReason = (pinEvolve, pinFinalCheck);

  TMemDBMultiBuffered = class(TMemDBReferenceReporter)
  private
  protected
    FMultiItems: TDLEntry; //Current, Next, TID1, Next TID 2, etc.
    FPinnedItems: TDLEntry; //Pinned 1, 2, 3 etc.

    procedure DCPMake(var Data: boolean; var Changes: boolean; var Pins: boolean);
    procedure DCPPre(var Update: TReferenceUpdate);
    procedure DCPPost(var Update: TReferenceUpdate);

    procedure CPTidMake(var Changes: boolean; var Pins: boolean; const Tid: TTransactionId);
    procedure CPTidPre(const Tid: TTransactionId; var Update:TTidUpdate);
    procedure CPTidPost(const Tid: TTransactionId; var Update:TTidUpdate);

    procedure LockSelf; virtual; abstract;
    procedure UnlockSelf; virtual; abstract;

    //Current, Next (Tid 1), Next (Tid 2) ...
    function FindCurMultiItem: PMemDBMultiItem; inline;
    function FindNxtMultiItem(const TId: TTransactionId): PMemDBMultiItem; inline;
    function FindPin(const TId: TTransactionId): PMemDbPinnedItem; inline;
    //First multi-item should always be current,
    //and always exists (item ptr may be null).
    //Other multi-items are pre-transaction, and item ptr should never be NULL.

    //TODO - Move this somewhere else?
    procedure CheckABStreamableListChange(Current, Next: TMemStreamableList);
    procedure LookaheadHelper(const PseudoTid: TTransactionId; Stream: TStream);

    //From Journal, and/or initial creation, have "direct set" functions.
    procedure ToJournalPinnedMB(const Tid: TTransactionId; Stream: TStream;
                              Cur, Next: TMemDBStreamable);
    procedure ToScratchPinnedMB(Stream: TStream; Cur: TMemDBStreamable);

    procedure RequestChangeInternal(const Tid: TTransactionId; DupType: TBufCloneType; MinIso: TMDBIsolationLevel);
  public
    //Determining changes for Tid's is easy to do for composite objects,
    //if you don't mind accruing pins along the way (internal structure).

    //Overrides of this might have to pin to get to internal structure.
    function AnyChangesForTid(const TId: TTransactionId): boolean; override;
    //Overrides of this need a Tid to possibly pin to find internal structure.
    //Ideally tables maintain their own
    function AnyChanges(const Tid: TTransactionId): boolean; override;


    procedure Delete(const TId: TTransactionId;
      MinIso: TMDBIsolationLevel = Low(TMDBIsolationLevel)); override;
    procedure RequestChange(const Tid: TTransactionId;
      MinIso: TMDBIsolationLevel = Low(TMDBIsolationLevel)); override;
    procedure RequestReffedChange(const Tid: TTransactionId;
      MinIso: TMDBIsolationLevel = Low(TMDBIsolationLevel)); override;
    procedure DirectSetNext(const Tid: TTransactionID; Data: TMemDbStreamable;
      MinIso: TMDBIsolationLevel = Low(TMDBIsolationLevel)); override;

    constructor Create;
    destructor Destroy; override;

    //The "To" functions need to stream atomically from some point in time,
    //even if (currently) these things will be serialised under a commit lock.
    procedure ToJournal(const Tid: TTransactionId; Stream: TStream);override;
    procedure ToScratch(const PseudoTid:TTransactionId; Stream: TStream);override;

    //With the "From" functions, we can be sure re-play is single threaded,
    //and serial.
    procedure FromJournal(const PseudoTid: TTransactionId; Stream: TStream);override;
    procedure FromScratch(const PseudoTid: TTransactionId; Stream: TStream);override;

    procedure PreCommit(const Tid: TTransactionId; Reason: TMemDbTransReason); override;
    procedure Commit(const Tid: TTransactionId; Reason: TMemDbTransReason); override;
    procedure Rollback(const Tid: TTransactionId; Reason: TMemDbTransReason); override;

    //Navigated to via row list.
    function PinForCursor(const Tid: TTransactionId): boolean;
    procedure UnPinCursor(const Tid: TTransactionId);

    //TODO - PinCursorFromIndex

    //TODO - Can these return TMemDBStreamable instead of TMemDBStreamable?
    //can get rid of a lot of "as" conversions.
    function PinCurrent(const Tid: TTransactionId; Reason: TPinReason): TMemDBStreamable;
    //Generally, no un-pin for data. Commit/Rollback will clear.

    //Pins/Refs checked/ cleared at Txion time.
    function GetNext(const Tid: TTransactionId): TMemDBStreamable;
    //No pin of next, but you can Ref it.

    function GetPinLatest(const Tid: TTransactionId;
                            var BufSelected: TAbSelType; Reason: TPinReason): TMemDBStreamable;
  end;

  TMemDBMultiBufferedTiny = class(TMemDbMultiBuffered)
  private
    FLock: TTinyLock;
  protected
    procedure LockSelf; override;
    procedure UnlockSelf; override;
  end;

  TMemDbMultiBufferedCrit = class(TMemDBMultiBuffered)
  private
    FLock: TCriticalSection;
  protected
    procedure LockSelf; override;
    procedure UnlockSelf; override;
  public
    constructor Create;
    destructor Destroy; override;
  end;

implementation

uses
  SysUtils;

const
  S_CHANGESET_BAD_LISTS = 'Changeset has lists of items that are not consistent';
  S_DBL_BUF_LOOKAHEAD_FAILED = 'Double buffered lookahead failed, tag: ';
  S_REQUESTED_CHANGE_OF_DELETE_SENTINEL = 'Changing a deleted row would undelete it, not allowed.';
  S_REQUESTED_CHANGE_OF_NULL_CURRENT = 'Requested change but item is null';
  S_DIRECT_SET_OVER_PREVIOUS = 'Direct set function used, but would overwrite pre-exusting data.';
  S_MODIFIED_NO_PRECOMMIT = 'Error. Pre-commit check not performed on concurrent data.';
  S_JOURNAL_REPLAY_DUP_INST = 'Journal replay failed, trying to overwrite duplicate changes.';
  S_JOURNAL_REPLAY_CHANGETYPE_DISAGREES = 'Journal replay failed, change type disagrees with data.';
  S_CANNOT_PIN_INSIDE_CHECKLOCK = 'Cannot pin between pre-commit and commit';
  S_MODIFIED_CONCURRENT_ABORT_WAR = 'Aborted. Concurrency conflict (write-after-read). (Case 1). Retry?';
  S_MODIFIED_CONCURRENT_ABORT_WAR_2 = 'Aborted. Concurrency conflict (write-after-read). (Case 2). Retry?';
  S_MODIFIED_CONCURRENT_ABORT_WAR_3 = 'Aborted. Concurrency conflict (write-after-read). (Case 3). Retry?';
  S_MODIFIED_CONCURRENT_ABORT_WAR_4 = 'Aborted. Concurrency conflict (write-after-read). (Case 4). Retry?';
  S_MODIFIED_CONCURRENT_ABORT_WAW = 'Aborted. Concurrency conflict (write-after-write). Retry?';
  S_MODIFIED_CONCURRENT_ABORT_RAW = 'Aborted. Concurrency conflict (read-after-write). Retry?';
  S_PIN_EVOLVE_TOO_LATE_1 = 'Cannot pin for change at this time (already final-checking). (Case 1)';
  S_PIN_EVOLVE_TOO_LATE_2 = 'Cannot pin for change at this time (already final-checking). (Case 2)';
  S_MODIFIED_CONCURRENT_WOULD_UNDELETE = 'Aborted. Non-repeatable reads should not allow you to undelete.';

{ Misc }

function NewMulti: PMemDbMultiItem; inline;
begin
{$IFOPT C+}
  result := TMemDBMultiItem.Create;
{$ELSE}
  New(result);
{$ENDIF}
end;

function NewPinned: PMemDbPinnedItem; inline;
begin
{$IFOPT C+}
  result := TMemDBPinnedItem.Create;
{$ELSE}
  New(result);
{$ENDIF}
end;

procedure DisposeMulti(Multi: PMemDbMultiItem); inline;
begin
{$IFOPT C+}
  Multi.Free;
{$ELSE}
  Dispose(Multi);
{$ENDIF}
end;

procedure DisposePinned(Pinned: PMemdbPinnedItem); inline;
begin
{$IFOPT C+}
  Pinned.Free;
{$ELSE}
  Dispose(Pinned);
{$ENDIF}
end;

{ TMemDBJournalCreator }

class procedure TMemDBJournalCreator.ChangeFlagsFromPinned(PinnedCurrent, PinnedNext: TMemDBStreamable;
  var Added:boolean;
  var Changed: boolean;
  var Deleted: boolean;
  var Null: boolean);
begin
  Added := NotAssignedOrSentinel(PinnedCurrent) and AssignedNotSentinel(PinnedNext);
  Changed := AssignedNotSentinel(PinnedCurrent) and AssignedNotSentinel(PinnedNext);
  Deleted := AssignedNotSentinel(PinnedCurrent)
    and Assigned(PinnedNext) and (PinnedNext is TMemDeleteSentinel);
  Null := NotAssignedOrSentinel(PinnedCurrent) and NotAssignedOrSentinel(PinnedNext);
  Assert((not Assigned(PinnedCurrent)) or (not (PinnedCurrent is TMemDeleteSentinel)));
end;

{ TMemDBChangeable }

{ TMemDbReferenceReporter }

procedure TMemDBReferenceReporter.DCPPre(Data, Changes, Pins: boolean; var Update: TReferenceUpdate);
begin
  Update.Pre := Data or Changes or Pins;
end;

procedure TMemDBReferenceReporter.DCPPost(Data, Changes, Pins: boolean; var Update: TReferenceUpdate);
begin
  Update.Post := Data or Changes or Pins;
end;

procedure TMemDBReferenceReporter.DCPHandle(const Update: TReferenceUpdate);
begin
{$IFOPT C+}
  if Update.Pre <> Update.Post then
  begin
    Assert(FRefLastAdded <> Update.Post);
    FRefLastAdded := Update.Post;
  end;
{$ENDIF}
end;

procedure TMemDBReferenceReporter.CPTidPre(Changes, Pins: boolean; const Tid: TTransactionId; var Update:TTidUpdate);
begin
  Update.Ref.Pre := Changes or Pins;
  Update.Tid := Tid;
end;

procedure TMemDBReferenceReporter.CPTidPost(Changes, Pins: boolean; const Tid: TTransactionId; var Update:TTidUpdate);
begin
  Update.Ref.Post := Changes or Pins;
  Assert(Update.Tid = Tid);
end;

{$IFOPT C+}
function TMemDbReferenceReporter.FindTidReffing(const Tid: TTransactionId): PTidReference;
begin
  result := PTidReference(FTidsReffingList.FLink.Owner);
  while Assigned(result) do
  begin
    if result.Tid = Tid then
      exit;
    result := PTidReference(result.Link.FLink.Owner);
  end;
end;
{$ENDIF}


procedure TMemDBReferenceReporter.CPTidHandle(const Update: TTidUpdate);
{$IFOPT C+}
var
  LastReffing: PTidReference;
{$ENDIF}
begin
{$IFOPT C+}
  if Update.Ref.Pre <> Update.Ref.Post then
  begin
    LastReffing := FindTidReffing(Update.Tid);
    Assert(Assigned(LastReffing) <> Update.Ref.Post);
    if Update.Ref.Post then
    begin
      if not Assigned(LastReffing) then
      begin
        LastReffing := TTidReference.Create;
        LastReffing.Tid := Update.Tid;
        DLItemInitObj(TObject(LastReffing), @LastReffing.Link);
        DLListInsertTail(@FTidsReffingList, @LastReffing.Link);
      end;
    end
    else
    begin
      if Assigned(LastReffing) then
      begin
        DLList.DLListRemoveObj(@LastReffing.Link);
        LastReffing.Free;
      end;
    end;
  end;
{$ENDIF}
end;

constructor TMemDBReferenceReporter.Create;
begin
  inherited;
{$IFOPT C+}
  DLItemInitList(@FTidsReffingList);
{$ENDIF}
end;

destructor TMemDbReferenceReporter.Destroy;
{$IFOPT C+}
var
  Reffing: PTIdReference;
{$ENDIF}
begin
{$IFOPT C+}
  Assert(DLItemIsEmpty(@FTidsReffingList)); //For the moment, be cautious and assert no pins / changes / etc etc.
  Reffing := PTidReference(FTidsReffingList.FLink.Owner);
  while Assigned(Reffing) do
  begin
    DLListRemoveObj(@Reffing.Link);
    Reffing.Free;
    Reffing := PTidReference(Reffing.Link.FLink.Owner);
  end;
{$ENDIF}
  inherited;
end;

{ TMemDBMultiBuffered }

procedure TMemDBMultiBuffered.DCPMake(var Data: boolean; var Changes: boolean; var Pins: boolean);
var
  Cur: PMemDbMultiItem;
  PNext: PDLEntry;
begin
  //Any data, at all.
  Cur := FindCurMultiItem;
  Data := Assigned(Cur.Item);
  //Any changes at all.
  PNext := Cur.Link.Flink;
  Changes := not DLItemIsList(PNext);
  //Any pins at all.
  Pins := not DlItemIsEmpty(@FPinnedItems);
end;

procedure TMemDBMultiBuffered.DCPPre(var Update: TReferenceUpdate);
var
  Data, Changes, Pins: boolean;
begin
  DCPMake(Data, Changes, Pins);
  inherited DCPPre(Data, Changes, Pins, Update);
end;

procedure TMemDBMultiBuffered.DCPPost(var Update: TReferenceUpdate);
var
  Data, Changes, Pins: boolean;
begin
  DCPMake(Data, Changes, Pins);
  inherited DCPPost(Data, Changes, Pins, Update);
end;

procedure TMemDBMultiBuffered.CPTidMake(var Changes: boolean; var Pins: boolean; const Tid: TTransactionId);
begin
  //Any changes for Tid, any pins for Tid.
  Changes := Assigned(FindNxtMultiItem(Tid));
  Pins := Assigned(FindPin(Tid));
end;

procedure TMemDBMultiBuffered.CPTidPre(const Tid: TTransactionId; var Update:TTidUpdate);
var
  Changes, Pins: boolean;
begin
  CPTidMake(Changes, Pins, Tid);
  inherited CPTidPre(Changes, Pins, Tid, Update);
end;

procedure TMemDBMultiBuffered.CPTidPost(const Tid: TTransactionId; var Update:TTidUpdate);
var
  Changes, Pins: boolean;
begin
  CpTidMake(Changes, Pins, Tid);
  inherited CPTidPost(Changes, Pins, Tid, Update);
end;

function TMemDBMultiBuffered.FindCurMultiItem: PMemDBMultiItem;
begin
  //Under lock.
  Assert(not DLItemIsEmpty(@FMultiItems));
  result := PMemDbMultiItem(FMultiItems.Flink.Owner);
  Assert(Assigned(result) and (result.Sel.SelType = abCurrent));
end;

function TMemDBMultiBuffered.FindNxtMultiItem(const TId: TTransactionId): PMemDBMultiItem;
begin
  //Under lock.
  result := PMemDBMultiItem(FMultiItems.Flink.Owner);
  while Assigned(result) do
  begin
    if (result.Sel.SelType = abNext) then
    begin
      Assert(Assigned(result.Item));
      if result.Sel.Tid = Tid then
        exit;
    end;
    result := PMemDBMultiItem(result.Link.FLink.Owner);
  end;
end;

function TMemDbMultiBuffered.FindPin(const TId: TTransactionID): PMemDbPinnedItem;
begin
  //Under lock.
  result := PMemDBPinnedItem(FPinnedItems.FLink.Owner);
  while Assigned(result) do
  begin
    if result.PinnedBy = Tid then
      exit;
    result := PMemDbPinnedItem(result.Link.FLink.Owner);
  end;
end;

procedure TMemDbMultiBuffered.CheckABStreamableListChange(Current, Next: TMemStreamableList);
var
  Idx: integer;
begin
  if Assigned(Current) then
  begin
    //No delete sentinels or nils in current.
    for Idx := 0 to Pred(Current.Count) do
    begin
      if NotAssignedOrSentinel(Current.Items[idx]) then
        raise EMemDBException.Create(S_CHANGESET_BAD_LISTS);
    end;
  end;
  if Assigned(Next) then
  begin
    //No Nil's in next.
    for Idx := 0 to Pred(Next.Count) do
    begin
      if not Assigned(Next.Items[Idx]) then
        raise EMemDBException.Create(S_CHANGESET_BAD_LISTS);
    end;
    if Assigned(Current) then
    begin
      //Next has same or more items as current.
      if Next.Count < Current.Count then
        raise EMemDBException.Create(S_CHANGESET_BAD_LISTS);
      //No delete sentinels in *new* items unstreamed from disk?
      for Idx := Current.Count to Pred(Next.Count) do
      begin
        if Next.Items[Idx] is TMemDeleteSentinel then
          raise EMemDBException.Create(S_CHANGESET_BAD_LISTS);
      end;
    end;
  end;
end;

procedure TMemDBMultiBuffered.LookaheadHelper(const PseudoTid: TTransactionId; Stream: TStream);
var
  Data: TMemDBStreamable;
  Pos: Int64;
  Tag: TMemStreamTag;
begin
  Pos := Stream.Position;
  Tag := RdTag(Stream);
  //No delete sentinels here.
  case Tag of
    mstStreamableListStart: Data := TMemStreamableList.Create;
    mstTableMetadataStart: Data := TMemTableMetadataItem.Create;
    mstFKMetadataStart: Data := TMemForeignKeyMetadataItem.Create;
  else
    raise EMemDBException.Create(S_DBL_BUF_LOOKAHEAD_FAILED + IntToStr(Ord(Tag)));
  end;
  Stream.Seek(Pos, TSeekOrigin.soBeginning);
  DirectSetNext(PseudoTid, Data);
end;

function TMemDbMultiBuffered.AnyChangesForTid(const TId: TTransactionId): boolean;
var
  PNext: PMemDBMultiItem;
begin
  LockSelf;
  try
    PNext := FindNxtMultiItem(Tid);
    Assert((not Assigned(PNext)) or Assigned(PNext.Item));
    result := Assigned(PNext);
  finally
    UnlockSelf;
  end;
end;

function TMemDBMultiBuffered.AnyChanges(const TId: TTransactionID): boolean;
var
  Cur: PMemDbMultiItem;
  PNext: PDLEntry;
begin
  LockSelf;
  try
    Cur := FindCurMultiItem;
    Assert(Assigned(Cur));
    PNext := Cur.Link.Flink;
    result := not DLItemIsList(PNext);
  finally
    UnlockSelf;
  end;
end;

procedure TMemDBMultiBuffered.Delete(const Tid: TTransactionId; MinIso: TMDBIsolationLevel);
var
  Cur, Nxt: PMemDbMultiItem;
  CurPin: PMemDbPinnedItem;
  RefUp: TReferenceUpdate;
  TidUp: TTidUpdate;
  Iso: TMDBIsolationLevel;
begin
  if Tid.Iso > MinIso then Iso := Tid.Iso else Iso := MinIso;
  LockSelf;
  try
    DCPPre(RefUp);
    CPTidPre(Tid, TidUp);

    Cur := FindCurMultiItem;
    Assert(Assigned(Cur));
    CurPin := FindPin(Tid);
    if Assigned(CurPin) then
    begin
      //Less stringent checking (repeatable read), where we read, modify, write
      if Iso >= ilReadRepeatable then
        if Cur.Sel.TId <> CurPin.PinnedCurrentTid then
          raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_ABORT_WAR_2);
    end;

    Nxt := FindNxtMultiItem(Tid);
    if not Assigned(Nxt) then
    begin
      Nxt := NewMulti;
      Nxt.Sel := MakeNextBufSelector(Tid);
      Assert(Cur.Sel.Tid <> Cur.Sel.Tid.Empty); //Should never have to delete NULL
      Nxt.ParentTid := Cur.Sel.Tid;
      DLItemInitObj(TObject(Nxt), @Nxt.Link);
      DLListInsertTail(@FMultiItems, @Nxt.Link);
    end;

    //Yes allow next buffer overwrites if it deleted stuff.
    //But don't change the original parent Tid (someone else changed stuff in meantime).
    //transaction ID
    Nxt.Item.Release;
    Nxt.Item := TMemDeleteSentinel.Create;

    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPHandle(RefUp);
end;

//TODO - This atomically works when updating
//Need to check the Row append case where there is no initial data.

procedure TMemDBMultiBuffered.RequestChangeInternal(const Tid: TTransactionId; DupType: TBufCloneType; MinIso: TMDBIsolationLevel);
var
  Cur, Nxt: PMemDbMultiItem;
  CurPin: PMemDbPinnedItem;
  RefUp: TReferenceUpdate;
  TidUp: TTidUpdate;
  Iso: TMDBIsolationLevel;
begin
  if Tid.Iso > MinIso then Iso := Tid.Iso else Iso := MinIso;
  LockSelf;
  try
    DCPPre(RefUp);
    CPTidPre(Tid, TidUp);

    Cur := FindCurMultiItem;
    Assert(Assigned(Cur));
    CurPin := FindPin(Tid);

    if Assigned(CurPin) then
    begin
      //Less stringent checking (repeatable read), where we read, modify, write
      //Except see undelete case below.
      if Iso >= ilReadRepeatable then
        if Cur.Sel.TId <> CurPin.PinnedCurrentTid then
          raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_ABORT_WAR_3);
    end;

    Nxt := FindNxtMultiItem(Tid);
    //Do all exception raising before allocating or making changes.
    if Assigned(Nxt) then
    begin
      Assert(Assigned(Nxt.Item));
      if Nxt.Item is TMemDeleteSentinel then
        raise EMemDBException.Create(S_REQUESTED_CHANGE_OF_DELETE_SENTINEL);
    end
    else
    begin
      //We can reasonably get here in some concurrency cases.
      //(Non-repeatable reads).
      //Because of the uniqueness of identifiers (guids), we're not allowed
      //to arbitrarily undelete stuff.
      if not Assigned(Cur.Item) then
        raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_WOULD_UNDELETE)
      else
        Assert(not (Cur.Item is TMemDeleteSentinel));
    end;

    if not Assigned(Nxt) then
    begin
      Nxt := NewMulti;
      Nxt.Sel := MakeNextBufSelector(Tid);
      Assert(Cur.Sel.Tid <> Cur.Sel.Tid.Empty); //Should never have to request change of NULL.
      Nxt.ParentTid := Cur.Sel.Tid;
      DLItemInitObj(TObject(Nxt), @Nxt.Link);
      DLListInsertTail(@FMultiItems, @Nxt.Link);
    end;
    if not Assigned(Nxt.Item) then
    begin
      case DupType of
        bctDeepClone: Nxt.Item := TMemDBStreamable.DeepClone(Cur.Item);
        bctClone: Nxt.Item := TMemDBStreamable.Clone(Cur.Item);
        bctRef: Nxt.Item := Cur.Item.AddRef as TMemDBStreamable;
      else
        Assert(false);
      end;
    end;
    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPHandle(RefUp);
end;

procedure TMemDBMultiBuffered.RequestChange(const Tid: TTransactionId;  MinIso: TMDBIsolationLevel);
begin
  RequestChangeInternal(Tid, bctDeepClone, MinIso);
end;

procedure TMemDbMultiBuffered.RequestReffedChange(const Tid: TTransactionId;  MinIso: TMDBIsolationLevel);
begin
  RequestChangeInternal(Tid, bctClone, MinIso);
end;

procedure TMemDBMultiBuffered.DirectSetNext(const Tid: TTransactionID; Data: TMemDbStreamable;  MinIso: TMDBIsolationLevel);
var
  Cur, Nxt: PMemDbMultiItem;
  CurPin: PMemDbPinnedItem;
  RefUp: TReferenceUpdate;
  TidUp: TTidUpdate;
  Iso: TMDBIsolationLevel;
begin
  if Tid.Iso > MinIso then Iso := Tid.Iso else Iso := MinIso;
  LockSelf;
  try
    DCPPre(RefUp);
    CPTidPre(Tid, TidUp);

    Cur := FindCurMultiItem;
    Assert(Assigned(Cur));

    CurPin := FindPin(Tid);
    if Assigned(CurPin) then
    begin
      //Less stringent checking (repeatable read), where we read, modify, write
      if Iso >= ilReadRepeatable then
        if Cur.Sel.TId <> CurPin.PinnedCurrentTid then
          raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_ABORT_WAR_4);
    end;

    Nxt := FindNxtMultiItem(Tid);
    Assert(Assigned(Data));
    Assert(not (Data is TMemDeleteSentinel));
    if Assigned(Nxt) then
      raise EMemDBException.Create(S_DIRECT_SET_OVER_PREVIOUS);

    Nxt := NewMulti;
    Nxt.Sel := MakeNextBufSelector(Tid);
    Nxt.ParentTid := Cur.Sel.Tid; //Might or might not be NULL.
    DLItemInitObj(TObject(Nxt), @Nxt.Link);
    DLListInsertTail(@FMultiItems, @Nxt.Link);
    Nxt.Item := Data; //No change in ref, takes ownership.

    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPHandle(RefUp);
end;

//TODO - How to make sure that MultiBuffered "No pins after pre-commit"
//are set/cleared in all cases. Having a No-Pin hanging around after a commit
//or rollback would be very bad indeed.

procedure TMemDBMultiBuffered.PreCommit(const Tid: TTransactionId; Reason: TMemDbTransReason);
var
  Cur, Nxt: PMemDbMultiItem;
  Pin: PMemDbPinnedItem;
begin
  LockSelf;
  try
    inherited; //TODO Will do this under lock, check it doesn't do much.
    Cur := FindCurMultiItem;
    Nxt := FindNxtMultiItem(Tid);
    Pin := FindPin(Tid);
    Assert(Assigned(Cur));
    if Assigned(Pin) then
    begin
      //More stringent checking (serialisable), where we read,
      //but do not modify the value (Used algorithmically later...)
      if Tid.Iso >= ilSerialisable then
        if Pin.PinnedCurrentTid <> Cur.Sel.TId then
          raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_ABORT_WAR);
    end;
    if Assigned(Nxt) then
    begin
      Assert(Assigned(Nxt.Item));
      //Check next item parented from current (Write-after-write).
      if Nxt.ParentTid <> Cur.Sel.Tid then
        raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_ABORT_WAW);
    end;
  finally
    UnlockSelf;
  end;
end;

//TODO - Prohibit pin mechanism requires that we pre-commit eveything before commit?

procedure TMemDBMultiBuffered.Commit(const Tid: TTransactionId; Reason: TMemDbTransReason);
var
  Cur, Nxt: PMemDbMultiItem;
  Pin: PMemDbPinnedItem;
  RefUp: TReferenceUpdate;
  TidUp: TTidUpdate;

begin
  LockSelf;
  try
    DCPPre(RefUp);
    CPTidPre(Tid, TidUp);

    inherited; //TODO Will do this under lock, check it doesn't do much.
    Cur := FindCurMultiItem;
    Nxt := FindNxtMultiItem(Tid);
    Pin := FindPin(Tid);
    Assert(Assigned(Cur));
    if Assigned(Pin) then
    begin
      //More stringent checking (serialisable), where we read,
      //but do not modify the value (Used algorithmically later...)
      if Tid.Iso >= ilSerialisable then
        if Pin.PinnedCurrentTid <> Cur.Sel.TId then
          raise EMemDBInternalException.Create(S_MODIFIED_NO_PRECOMMIT);
    end;
    if Assigned(Nxt) then
    begin
      //Check next item parented from current (Write-after-write).
      Assert(Assigned(Nxt.Item));
      if Nxt.ParentTid <> Cur.Sel.Tid then
        raise EMemDBInternalException.Create(S_MODIFIED_NO_PRECOMMIT);

      //Remove Cur.
      Cur.Item.Release;
      DLListRemoveObj(@Cur.Link);
      DisposeMulti(Cur);

      //Replace Cur with Next.
      DLListRemoveObj(@Nxt.Link);
      DLListInsertHead(@FMultiItems, @Nxt.Link);
      Nxt.ParentTid := TTransactionId.Empty;

      Assert(Nxt.Sel.Tid = Tid);
      Nxt.Sel.SelType := abCurrent;
      Assert(Nxt.Sel = MakeCurrentBufSelector(Tid)); //Should not change the tid.
      //Nxt.Sel.Tid is Tid used for parent tid for later xactions.

      if Nxt.Item is TMemDeleteSentinel then
      begin
        Nxt.Item.Release;
        Nxt.Item := nil;
      end
      else
        Nxt.Item.CommitPack; //Now cur.item
    end;

    //Remove all pins for this Tid.
    Pin := FindPin(Tid);
    if Assigned(Pin) then
    begin
      Assert(Assigned(Pin.Item));
      Pin.Item.Release;
      DLListRemoveObj(@Pin.Link);
      DisposePinned(Pin);
    end;
    Assert(not Assigned(FindPin(Tid)));
    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPHandle(RefUp);
end;

procedure TMemDBMultiBuffered.Rollback(const Tid: TTransactionId; Reason: TMemDbTransReason);
var
  Nxt: PMemDBMultiItem;
  Pin: PMemDbPinnedItem;
  RefUp: TReferenceUpdate;
  TidUp: TTidUpdate;

begin
  LockSelf;
  try
    DCPPre(RefUp);
    CPTidPre(Tid, TidUp);

    //Clear Next items
    inherited;
    Nxt := FindNxtMultiItem(Tid);
    if Assigned(Nxt) then
    begin
      Assert(Assigned(Nxt.Item));
      Nxt.Item.Release;
      DLListRemoveObj(@Nxt.Link);
      DisposeMulti(Nxt);
    end;
    Assert(not Assigned(FindNxtMultiItem(Tid)));
    //Clear pins
    Pin := FindPin(Tid);
    if Assigned(Pin) then
    begin
      Assert(Assigned(Pin.Item));
      Pin.Item.Release;
      DLListRemoveObj(@Pin.Link);
      DisposePinned(Pin);
    end;
    Assert(not Assigned(FindPin(Tid)));

    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPHandle(RefUp);
end;

function TMemDBMultiBuffered.PinForCursor(const Tid: TTransactionId): boolean;
var
  selType: TABSelType;
begin
  //Don't pin on deleted rows.
  result := AssignedNotSentinel(GetPinLatest(Tid, selType, pinEvolve));
end;

procedure TMemDBMultiBuffered.UnPinCursor(const Tid: TTransactionId);
begin
  //NOP.
end;

function TMemDBMultiBuffered.PinCurrent(const Tid: TTransactionId; Reason: TPinReason): TMemDBStreamable;
var
  Cur, Nxt: PMemDBMultiItem;
  Pin: PMemDBPinnedItem;
  RefUp: TReferenceUpdate;
  TidUp: TTidUpdate;

begin
  LockSelf;
  try
    DCPPre(RefUp);
    CPTidPre(Tid, TidUp);
    result := nil;
    //Always return pin before current.
    Pin := FindPin(Tid);
    if Assigned(Pin) then
    begin
      if (Reason = pinEvolve) and (Pin.FinalCheck) then
        raise EMemDbInternalException.Create(S_PIN_EVOLVE_TOO_LATE_1);
      Pin.FinalCheck := Pin.FinalCheck or (Reason = pinFinalCheck);
      Assert(Assigned(Pin.Item));
      result := Pin.Item;
    end
    else
    begin
      //If we have already requested a change, but not pinned, and it's not
      //whatever we're about to pin, then stop.
      Cur := FindCurMultiItem;
      Assert(Assigned(Cur));

      Nxt := FindNxtMultiItem(Tid);
      if Assigned(Nxt) then
      begin
        //In unlikely case where we have requested modify, and then pin current later.
        //Less stringent checking (repeatable read), where we read, modify, write
        if Tid.Iso >= ilReadRepeatable then
          if Nxt.ParentTid <> Cur.Sel.Tid then
            raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_ABORT_RAW);
      end;

      if Assigned(Cur.Item) then
      begin
        //New pin.
        Pin := NewPinned;
        Pin.Item := Cur.Item.AddRef as TMemDBStreamable;
        DLItemInitObj(TObject(Pin), @Pin.Link);
        Pin.PinnedCurrentTid := Cur.Sel.TId;
        Pin.PinnedBy := Tid;
        Pin.FinalCheck := (Reason = pinFinalCheck);
        DLListInsertTail(@FPinnedItems, @Pin.Link);
        result := Pin.Item;
      end
    end;
    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPHandle(RefUp);
end;

function TMemDbMultiBuffered.GetNext(const Tid: TTransactionId): TMemDBStreamable;
var
  Nxt: PMemDbMultiItem;
begin
  LockSelf;
  try
    Nxt := FindNxtMultiItem(Tid);
    if Assigned(Nxt) then
    begin
      Assert(Assigned(Nxt.Item));
      result := Nxt.Item;
    end
    else
      result := nil;
  finally
    UnlockSelf;
  end;
end;

function TMemDBMultiBuffered.GetPinLatest(const Tid: TTransactionId;
                        var BufSelected: TAbSelType; Reason: TPinReason): TMemDBStreamable;
var
  Cur, Nxt: PMemDBMultiItem;
  Pin: PMemDbPinnedItem;
  RefUp: TReferenceUpdate;
  TidUp: TTidUpdate;

begin
  LockSelf;
  try
    DCPPre(RefUp);
    CPTidPre(Tid, TidUp);

    result := nil;
    //Find next preferentially.
    Nxt := FindNxtMultiItem(Tid);
    if Assigned(Nxt) then
    begin
      Assert(Assigned(Nxt.Item));
      result := Nxt.Item;
      BufSelected := abNext;
    end
    else
    begin
      //Then pin if we have one.
      Pin := FindPin(Tid);
      if Assigned(Pin) then
      begin
        if (Reason = pinEvolve) and (Pin.FinalCheck) then
          raise EMemDbInternalException.Create(S_PIN_EVOLVE_TOO_LATE_2);
        Pin.FinalCheck := Pin.FinalCheck or (Reason = pinFinalCheck);
        Assert(Assigned(Pin.Item));
        result := Pin.Item;
        BufSelected := abCurrent;
      end
      else
      begin
        //OK, current, creating a new pin if we have to.
        Cur := FindCurMultiItem;
        Assert(Assigned(Cur));
        if Assigned(Cur.Item) then
        begin
          //New pin.
          Pin := NewPinned;
          Pin.Item := Cur.Item.AddRef as TMemDBStreamable;
          Pin.FinalCheck := (Reason = pinFinalCheck);
          DLItemInitObj(TObject(Pin), @Pin.Link);
          Pin.PinnedCurrentTid := Cur.Sel.TId;
          Pin.PinnedBy := Tid;
          DLListInsertTail(@FPinnedItems, @Pin.Link);
          result := Pin.Item;
          BufSelected := abCurrent;
        end;
      end;
    end;
    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPHandle(RefUp);
end;

constructor TMemDbMultiBuffered.Create;
var
  Cur: PMemDBMultiItem;
begin
  inherited;
  DLItemInitList(@FMultiItems);
  DLItemInitList(@FPinnedItems);
  Cur := NewMulti;
  Cur.Sel := MakeCurrentBufSelector(TTransactionId.Empty);
  DLItemInitObj(TObject(Cur), @Cur.Link);
  DLListInsertHead(@FMultiItems, @Cur.Link);
end;

destructor TMemDBMultiBuffered.Destroy;
var
  Item: PMemDbMultiItem;
  Pin: PMemDBPinnedItem;
begin
  LockSelf;
  try
    //Check for and avoid multiple frees for pinned items.
    Item := PMemDbMultiItem(FMultiItems.FLink.Owner);
    while Assigned(Item) do
    begin
       //For the moment, be cautious and assert no pins / changes / etc etc.
      Assert(Item.Sel.SelType = TABSelType.abCurrent);
      Item.Item.Release;
      DLListRemoveObj(@Item.Link);
      DisposeMulti(Item);
      Item := PMemDbMultiItem(FMultiItems.FLink.Owner);
    end;
    //For the moment, be cautious and assert no pins / changes / etc etc.
    Assert(DlItemIsEmpty(@FPinnedItems));
    Pin := PMemDBPinnedItem(FPinnedItems.FLink.Owner);
    while Assigned(Pin) do
    begin
      Pin.Item.Release;
      DLListRemoveObj(@Pin.Link);
      DisposePinned(Pin);
      Pin := PMemDBPinnedItem(FPinnedItems.FLink.Owner);
    end;
  finally
    UnlockSelf;
  end;
  inherited;
end;

procedure TMemDbMultiBuffered.ToJournal(const Tid: TTransactionId; Stream: TStream);
var
  Current, Next: TMemDbStreamable;
begin
  inherited;
  Current := PinCurrent(Tid, pinEvolve);
  Next := GetNext(Tid);
  ToJournalPinnedMB(Tid, Stream, Current, Next);
end;

procedure TMemDBMultiBuffered.ToJournalPinnedMB(const Tid: TTransactionId; Stream: TStream;
                          Cur, Next: TMemDBStreamable);
var
  Added,Changed,Deleted, Null: boolean;
begin
  ChangeFlagsFromPinned(Cur, Next, Added, Changed, Deleted, Null);

  Assert(Added or Changed or Deleted);
  WrTag(Stream, mstDblBufferedStart);
  if NotAssignedOrSentinel(Cur) then
  begin
    Assert(not (Next is TMemDeleteSentinel));
    Assert(Added);
    WrStreamChangeType(Stream, mctAdd);
    Next.ToStream(Stream);
  end
  else
  begin
    if Next is TMemDeleteSentinel then
    begin
      Assert(Deleted);
      WrStreamChangeType(Stream, mctDelete);
      Cur.ToStream(Stream);
    end
    else
    begin
      Assert(Changed);
      WrStreamChangeType(Stream, mctChange);
      Cur.ToStream(Stream);
      Next.ToStream(Stream);
    end;
  end;
  WrTag(Stream, mstDblBufferedEnd);
end;

procedure TMemDBMultiBuffered.ToScratch(const PseudoTid: TTransactionId; Stream: TStream);
var
  Current: TMemDbStreamable;
begin
  inherited;
  Current := PinCurrent(PseudoTid, pinEvolve);
  ToScratchPinnedMB(Stream, Current);
end;

procedure TMemDbMultiBuffered.ToScratchPinnedMB(Stream: TStream; Cur: TMemDBStreamable);
begin
  Assert(AssignedNotSentinel(Cur));
  WrTag(Stream, mstDblBufferedStart);
  WrStreamChangeType(Stream, mctAdd);
  Cur.ToStream(Stream);
  WrTag(Stream, mstDblBufferedEnd);
end;

//With the "From" functions, we can be sure re-play is single threaded,
//and serial.
procedure TMemDBMultiBuffered.FromJournal(const PseudoTid: TTransactionId; Stream: TStream);
var
  ChangeType: TMDBChangeType;
  Next:TMemDBStreamable;
  Cur: TMemDBStreamable;
begin
  inherited;
  //This not atomic, but journal replay, so not so worried.
  if AnyChanges(PseudoTid) then
    raise EMemDbInternalException.Create(S_JOURNAL_REPLAY_DUP_INST);

  Cur := PinCurrent(PseudoTid, pinEvolve);
  ExpectTag(Stream, mstDblBufferedStart);
  ChangeType := RdStreamChangeType(Stream);
  case ChangeType of
    mctAdd:
    begin
      if Assigned(Cur) then
        raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
      LookaheadHelper(PseudoTid, Stream);
      Next := GetNext(PseudoTid);
      Next.FromStream(Stream);
    end;
    mctChange, mctDelete:
    begin
      if not Assigned(Cur) then
        raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);
      Cur.CheckSameAsStream(Stream);
      if ChangeType = mctChange then
      begin
        LookaheadHelper(PseudoTid, Stream);
        Next := GetNext(PseudoTid);
        Next.FromStream(Stream);
      end
      else //mctDelete
        Delete(PseudoTid);
    end;
  else
    Assert(false);
  end;
  ExpectTag(Stream, mstDblBufferedEnd);
end;

procedure TMemDbMultiBuffered.FromScratch(const PseudoTid: TTransactionId; Stream: TStream);
var
  ChangeType: TMDBChangeType;
  Cur, Next: TMemDBStreamable;
begin
  inherited;
  if AnyChanges(PseudoTid) then
    raise EMemDbInternalException.Create(S_JOURNAL_REPLAY_DUP_INST);

  Cur := PinCurrent(PseudoTid, pinEvolve);
  if AssignedNotSentinel(Cur) then
    raise EMemDBException.Create(S_JOURNAL_REPLAY_INCONSISTENT);

  ExpectTag(Stream, mstDblBufferedStart);
  ChangeType := RdStreamChangeType(Stream);
  case ChangeType of
    mctAdd:
    begin
      //Expect transaction reason is always mtrReplayFromScratch
      LookaheadHelper(PseudoTid, Stream);
      Next := GetNext(PseudoTid);
      Next.FromStream(Stream);
    end;
  else
    raise EMemDBException.Create(S_JOURNAL_REPLAY_CHANGETYPE_DISAGREES);
  end;
  ExpectTag(Stream, mstDblBufferedEnd);
end;

{  TMemDBMultiBufferedTiny }

procedure TMemDbMultiBufferedTiny.LockSelf;
begin
  AcquireTinyLock(FLock);
end;

procedure TMemDbMultiBufferedTiny.UnlockSelf;
begin
  ReleaseTinyLock(FLock);
end;

{ TMemDbMultiBufferedCrit }

constructor TMemDbMultiBufferedCrit.Create;
begin
  inherited;
  FLock := LockAbstractions.TCriticalSection.Create;
end;

destructor TMemDbMultiBufferedCrit.Destroy;
var
  Tmp: TCriticalSection;
begin
  Tmp := FLock;
  inherited;
  Tmp.Free;
end;

procedure TMemDbMultiBufferedCrit.LockSelf;
begin
  FLock.Acquire;
end;

procedure TMemDbMultiBufferedCrit.UnlockSelf;
begin
  FLock.Release;
end;

end.
