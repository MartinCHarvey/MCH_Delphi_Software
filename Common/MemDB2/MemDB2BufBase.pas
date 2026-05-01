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
  MemDB2Misc, Classes, MemDb2Streamable, DLList, TinyLock, LockAbstractions,
  MemDB2Indexing;

{
   Isolation / serialization

Some notes on pinning and consistency / isolation:

1. Plain pinning (not considering INodes).

- Pin
  - Pin latest current.
  Re-pin: re-use the pin.

- Delete, Request Change, SetNext:
  - >= readRepeatable, pin must be up to date else abort.
- PreCommit
  - >= Serialisable, require all up to date at pre-commit (even just read).
  - Always require writes in order, regardless of iso.

2a. INode pinning:
  - PinForINode, copy Tid across from source as expected.

2b. Pin for cursor (regular pin from INode Pin):

  - If existing pin, then use that.
    - >= readRepeatable, pin must agree with iNodePin.
  - If no existing pin.
    - Can make *older* pin which keeps read  / preducate / set consistency.
    - Same checking when it comes to modify time.

TODO - Serializable.

3. Serializable isolation:
  - Need row addition/change lock from start of Txion:
    To prevent phantoms & keep index traversal consistent.
  - Need all meta/index pinning to happen atomically at Txion start.
}
type
  TMemDBPreCommitPhase = (pcpFKeys, pcpTables);
  TMemDBCommitPhase = (ccpData, ccpMetaIndex, ccpCleardown);
  TMemDBRollbackPhase = (rbpIndexRollback, rbpMetaRollback, rbpDelayedRollback);

  //Object which can write changes between current and next version to journal.
{$IFDEF USE_TRACKABLES}
  TMemDBJournalCreator = class(TTrackable)
{$ELSE}
  TMemDBJournalCreator = class
{$ENDIF}
  protected
    //TODO - Check all cases where this is used, and we should be using what the
    //*actual* change flags are.
    class procedure ChangeFlagsFromPinned(PinnedCurrent, PinnedNext: TMemDBStreamable;
      var Added:boolean;
      var Changed: boolean;
      var Deleted: boolean;
      var Null: boolean);
  public
    procedure Prepare(const Tid: TTransactionId); virtual; abstract;

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

    procedure StartTransaction(const Tid: TTRansactionId); virtual; abstract;
    //Check A/B buffered changes consistent and logical (and/or atomic) before commit.
    procedure PreCommit(const TId: TTransactionId; Phase: TMemDBPreCommitPhase); virtual; abstract;
    //Save to journal / Make the changes.
    procedure Commit(const TId: TTransactionId; Phase: TMemDBCommitPhase); virtual; abstract;
    //Rollback changes, possibly promptly or delayed.
    procedure Rollback(const TId: TTransactionId; Phase: TMemDBRollbackPhase); virtual; abstract;
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
    Changes: TReferenceUpdate;
    Pins: TReferenceUpdate;
    Tid: TTransactionId;
  end;

{$IFOPT C+}
  TTidReference = class(TTrackable)
   Link: TDLEntry;
   Tid: TTransactionId;
  end;
  PTidReference = TTidReference;
{$ENDIF}

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
    //DCPUpdates can be sent lazily.
{$IFOPT C+}
    procedure DCPPre(Data, Changes, Pins: boolean; var Update: TReferenceUpdate);
    procedure DCPPost(Data, Changes, Pins: boolean; var Update: TReferenceUpdate);
    procedure DCPLazyHandle(const Update: TReferenceUpdate);
{$ELSE}
    procedure DCPPre(Data, Changes, Pins: boolean; var Update: TReferenceUpdate); inline;
    procedure DCPPost(Data, Changes, Pins: boolean; var Update: TReferenceUpdate); inline;
    procedure DCPLazyHandle(const Update: TReferenceUpdate); inline;
{$ENDIF}
    procedure DCPHandle(const Update: TReferenceUpdate); virtual;

    //CPTid updates can't cos they are also used by TidLocal to determine TidLocal.ChangesForTid
    procedure CPTidPre(Changes, Pins: boolean; const Tid: TTransactionId; var Update:TTidUpdate); inline;
    procedure CPTidPost(Changes, Pins: boolean; const Tid: TTransactionId; var Update:TTidUpdate); inline;
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

//TODO - May be able to optimise this structure out with
//an extra link field in TMemDbIndexLeafGeneric.
{$IFOPT C+}
  TMemDBIndexPin = class(TTrackable)
{$ELSE}
  TMemDbIndexPin = record
{$ENDIF}
    Link:TDLEntry;
    PinnedTid: TTransactionId;
    INode: TMemDbIndexLeafGeneric;
  end;
{$IFOPT C+}
  PMemDBIndexPin = TMemDbIndexPin;
{$ELSE}
  PMemDBIndexPin = ^TMemDbIndexPin;
{$ENDIF}

  TPinReason = (pinEvolve, pinFinalCheck);

  TMemDBMultiBuffered = class(TMemDBReferenceReporter)
  private
  protected
    FListCreateClass: TMemDbStreamableClass;
    FMultiItems: TDLEntry; //Current, Next, TID1, Next TID 2, etc.
    FPinnedItems: TDLEntry; //Pinned 1, 2, 3 etc.
    FIndexPins: TDLEntry;

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
    function FindNxtMultiItem(const TId: TTransactionId): PMemDBMultiItem;
    function FindPin(const TId: TTransactionId): PMemDbPinnedItem;
    function FindIndexPin(IndexNode: TMemDBIndexLeafGeneric): PMemDBINdexPin;
    //First multi-item should always be current,
    //and always exists (item ptr may be null).
    //Other multi-items are pre-transaction, and item ptr should never be NULL.

    function NewCurrentPinInternal(const Tid:TTransactionId; Reason:TPinReason): TMemDBStreamable;
    function ReUseCurrentPinInternal(Pin: PMemDbPinnedItem; Reason: TPinReason): TMemDbStreamable;

    function NewCurrentPinFromINode(const Tid:TTransactionId; IPin: PMemDBIndexPin; Reason:TPinReason): TMemDBStreamable;
    function ReUseCurrentPinForINode(const Tid: TTransactionId; Pin: PMemDbPinnedItem; IPin: PMemDBIndexPin; Reason: TPinReason): TMemDbStreamable;

    //TODO - Move this somewhere else?
    procedure CheckABStreamableListChange(Current, Next: TMemStreamableList);
    procedure LookaheadHelper(const PseudoTid: TTransactionId; Stream: TStream);

    //From Journal, and/or initial creation, have "direct set" functions.
    procedure ToJournalPinnedMB(const Tid: TTransactionId; Stream: TStream;
                              Cur, Next: TMemDBStreamable);
    procedure ToScratchPinnedMB(Stream: TStream; Cur: TMemDBStreamable);

    procedure RequestChangeInternal(const Tid: TTransactionId; DupType: TBufCloneType; MinIso: TMDBIsolationLevel);

    procedure SetListCreateClass(New: TMemDBStreamableClass);

    function HoldIndexPinInLock(Pin: PMemDbIndexPin): PMemDBIndexPin;
    procedure ReleaseIndexPinOutsideLock(Pin: PMemDbIndexPin);
  public
    //Determining changes for Tid's is easy to do for composite objects,
    //if you don't mind accruing pins along the way (internal structure).

    //Overrides of this might have to pin to get to internal structure.
    function AnyChangesForTid(const TId: TTransactionId): boolean; override;
    //Overrides of this need a Tid to possibly pin to find internal structure.
    //Ideally tables maintain their own
    function AnyChanges(const Tid: TTransactionId): boolean; override;

    procedure ChangeFlagsUnderCommitLock(const Tid: TTransactionId;
      var Added:boolean;
      var Changed: boolean;
      var Deleted: boolean;
      var Null: boolean);

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

    procedure StartTransaction(const Tid: TTRansactionId); override;
    procedure Prepare(const Tid: TTransactionId); override;
    procedure PreCommit(const TId: TTransactionId; Phase: TMemDBPreCommitPhase); override;
    procedure Commit(const TId: TTransactionId; Phase: TMemDBCommitPhase); override;
    procedure Rollback(const TId: TTransactionId; Phase: TMemDBRollbackPhase); override;

    //Navigated to via row list.
    function PinForCursor(const Tid: TTransactionId): boolean;
    procedure UnPinCursor(const Tid: TTransactionId);
    //Navigated to via INode.

    //User traversal.
    function PinForCursorFromInode(const Tid: TTRansactionId; INode: TMemDBIndexLeafGeneric; Reason: TPinReason): boolean;
    //Index maintenance.

    //Generally, no un-pin for data. Commit/Rollback will clear.
    //Pins/Refs checked/ cleared at Txion time.
    function PinCurrent(const Tid: TTransactionId; Reason: TPinReason): TMemDBStreamable;

    //And in addition to all the ISO checking.
    procedure RequireCurrentAtomic(const Tid: TTransactionId);

    function GetNext(const Tid: TTransactionId): TMemDBStreamable;
    function GetPinLatest(const Tid: TTransactionId;
                            var BufSelected: TAbSelType; Reason: TPinReason): TMemDBStreamable;

    //Index pinning is a bit different. We can get the item based on Tid,
    //but it's pinned across transactions. No pin reason, you either add or
    //remove the pin.

    //Connects / disconnects INode to this class atomically, but does not do any
    //tree maniupulation. See MemDBRow for that.
    function PinForIndex(const Tid: TTransactionId; ItemSel: TAbSelType;
                         IndexNode: TMemDbIndexLeafGeneric): boolean;
    procedure UnpinFromIndex(IndexNode: TMemDbIndexLeafGeneric);
    procedure DupIndexPin(SourceNode, DestNode: TMemDBIndexLeafGeneric);
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
  SysUtils, Reffed;

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
  S_MODIFIED_CONCURRENT_ABORT_WAR_5 = 'Aborted. Concurrency conflict (write-after-read). (Case 5). Retry?';
  S_MODIFIED_CONCURRENT_ABORT_WAR_6 = 'Aborted. Concurrency conflict (write-after-read). (Case 6). Retry?';
  S_MODIFIED_CONCURRENT_ABORT_WAR_7 = 'Aborted. Concurrency conflict (write-after-read). (Case 7). Retry?';
  S_MODIFIED_CONCURRENT_ABORT_WAW = 'Aborted. Concurrency conflict (write-after-write). Retry?';
  S_MODIFIED_CONCURRENT_ABORT_RAW = 'Aborted. Concurrency conflict (read-after-write). Retry?';
  S_PIN_EVOLVE_TOO_LATE_1 = 'Cannot pin for change at this time (already final-checking). (Case 1)';
  S_PIN_EVOLVE_TOO_LATE_2 = 'Cannot pin for change at this time (already final-checking). (Case 2)';
  S_MODIFIED_CONCURRENT_WOULD_UNDELETE = 'Aborted. Non-repeatable reads should not allow you to undelete.';
  S_CREATE_CLASS_NOT_SLIST = 'Class type for list creation not streamable list.';
  S_CREATE_CLASS_NOT_SET = 'Class type for list creation not set';
  S_INDEX_ALREADY_PINNED = 'Index node already pinned to an item/row';
  S_INDEX_PIN_NOT_FOUND = 'Index pin not found when unpinning';
  S_ERROR_BAD_INODE_DURING_PIN = 'Bad INode getting cursor pin from INode.';
  S_ERROR_BAD_IPIN_DURING_PIN = 'Bad IPin getting cursor pin from INode.';
  S_ERROR_BAD_IPIN_DURING_PIN_CURRENT = 'Bad IPin getting current pin from INode';
  S_ERROR_BAD_INODE_DURING_PIN_CURRENT = 'Bad INode getting current pin from INode';

{ Misc }

function NewMulti: PMemDbMultiItem; inline;
begin
{$IFOPT C+}
  result := TMemDBMultiItem.Create;
{$ELSE}
  New(result);
  FillChar(result^, sizeof(result^), 0);
{$ENDIF}
end;

function NewPinned: PMemDbPinnedItem; inline;
begin
{$IFOPT C+}
  result := TMemDBPinnedItem.Create;
{$ELSE}
  New(result);
  FillChar(result^, sizeof(result^), 0);
{$ENDIF}
end;

function NewIndexPin: PMemDBIndexPin; inline;
begin
{$IFOPT C+}
  result := TMemDBIndexPin.Create;
{$ELSE}
  New(result);
  FillChar(result^, sizeof(result^), 0);
{$ENDIF}
end;

procedure DisposeIndexPin(Pin: PMemDbIndexPin); inline;
begin
{$IFOPT C+}
  Pin.Free;
{$ELSE}
  Dispose(Pin);
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

procedure TMemDBReferenceReporter.DCPLazyHandle(const Update: TReferenceUpdate);
begin
  if Update.Pre <> Update.Post then
    DCPHandle(Update);
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
  Update.Changes.Pre := Changes;
  Update.Pins.Pre := Pins;
  Update.Tid := Tid;
end;

procedure TMemDBReferenceReporter.CPTidPost(Changes, Pins: boolean; const Tid: TTransactionId; var Update:TTidUpdate);
begin
  Update.Changes.Post := Changes;
  Update.Pins.Post := Pins;
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
  GeneralPre, GeneralPost: boolean;
{$ENDIF}
begin
{$IFOPT C+}
  GeneralPre := Update.Changes.Pre or Update.Pins.Pre;
  GeneralPost := Update.Changes.Post or Update.Pins.Post;
  if GeneralPre <> GeneralPost then
  begin
    LastReffing := FindTidReffing(Update.Tid);
    Assert(Assigned(LastReffing) <> GeneralPost);
    if GeneralPost then
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
  IndexPins: boolean;
  ItemPins: boolean;
begin
  //Any data, at all.
  Cur := FindCurMultiItem;
  Data := Assigned(Cur.Item);
  //Any changes at all.
  PNext := Cur.Link.Flink;
  Changes := not DLItemIsList(PNext);
  //Any pins at all.
  IndexPins := not DlItemIsEmpty(@FIndexPins);
  ItemPins := not DlItemIsEmpty(@FPinnedItems);
  Pins := IndexPins or ItemPins;
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

function TMemDbMultiBuffered.FindIndexPin(IndexNode: TMemDBIndexLeafGeneric): PMemDBINdexPin;
begin
  //Under lock.
  result := PMemDBIndexPin(FIndexPins.FLink.Owner);
  while Assigned(result) do
  begin
    if result.INode = IndexNode then
      exit;
    result := PMemDBIndexPin(result.Link.FLink.Owner);
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

procedure TMemDBMultiBuffered.SetListCreateClass(New: TMemDBStreamableClass);
begin
  if not Assigned(New) and New.InheritsFrom(TMemStreamableList) then
    raise EMemDBInternalException.Create(S_CREATE_CLASS_NOT_SLIST);
  FListCreateClass := New;
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
    mstStreamableListStart: begin
      if not Assigned(FListCreateClass) then
        raise EMemDbInternalException.Create(S_CREATE_CLASS_NOT_SET);
      Data := FListCreateClass.Create;
    end;
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

procedure TMemDbMultiBuffered.ChangeFlagsUnderCommitLock(const Tid: TTransactionId;
  var Added:boolean;
  var Changed: boolean;
  var Deleted: boolean;
  var Null: boolean);
var
  Cur: PMemDBMultiItem;
  Nxt: PMemDBMultiItem;
  CurDat, NxtDat: TMemDBStreamable;
begin
  LockSelf;
  try
    Cur := FindCurMultiItem;
    CurDat := Cur.Item;
    Nxt := FindNxtMultiItem(Tid);
    if Assigned(Nxt) then
      NxtDat := Nxt.Item
    else
      NxtDat := nil;
    ChangeFlagsFromPinned(CurDat, NxtDat, Added, Changed, Deleted, Null);
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
    if Assigned(Cur.Item) or Assigned(Nxt) then
    begin
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
    end;
    //If not (Assigned Cur.Item or Assigned(Nxt)) then is NULL,
    //in datastructures probably for referencing reasons, ignore
    //duplicate delete.

    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPLazyHandle(RefUp);
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
      if not Assigned(Cur.Item) then
        raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_WOULD_UNDELETE)
        //We can reasonably get here in some concurrency cases.
        //(Non-repeatable reads).
        //Because of the uniqueness of identifiers (guids), we're not allowed
        //to arbitrarily undelete stuff.
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
  DCPLazyHandle(RefUp);
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
  DCPLazyHandle(RefUp);
end;

procedure TMemDBMultiBuffered.StartTransaction(const Tid: TTRansactionId);
begin
  Assert(false); //Generally not for double buffered, but yes for entities and DB.
  //TODO - Refactor class heirarchy in due course.
end;

procedure TMemDBMultiBuffered.Prepare(const Tid: TTransactionId);
begin
  Assert(false); //Generally not for double buffered, but yes for entities and DB.
  //TODO - Refactor class heirarchy in due course.
end;

procedure TMemDBMultiBuffered.RequireCurrentAtomic(const Tid: TTransactionId);
var
  Cur: PMemDbMultiItem;
  Pin: PMemDbPinnedItem;
begin
  LockSelf;
  try
    Cur := FindCurMultiItem;
    Pin := FindPin(Tid);
    Assert(Assigned(Cur));
    if Assigned(Pin) then
    begin
      //Strict metadata currency checking.
      //Note, checks even if Cur.Item = NIL. (Delete / resurrect races).
      if Pin.PinnedCurrentTid <> Cur.Sel.TId then
        raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_ABORT_WAR_7);
    end;
  finally
    UnlockSelf;
  end;
end;

procedure TMemDBMultiBuffered.PreCommit(const TId: TTransactionId; Phase: TMemDBPreCommitPhase);
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

procedure TMemDBMultiBuffered.Commit(const TId: TTransactionId; Phase: TMemDBCommitPhase);
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
  DCPLazyHandle(RefUp);
end;

procedure TMemDBMultiBuffered.Rollback(const Tid: TTransactionId; Phase: TMemDBRollbackPhase);
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
  DCPLazyHandle(RefUp);
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

function TMemDbMultiBuffered.NewCurrentPinFromINode(const Tid:TTransactionId; IPin: PMemDBIndexPin; Reason:TPinReason): TMemDBStreamable;
var
  INode: TMemDbIndexLeafGeneric;
  Pin: PMemDbPinnedItem;
begin
  { OK, this is a bit different. If we don't have a current pin,
    we can create one from the INode pin. It's potentially older
    than latest current, but gives us "snapshot-like" read consistency
    when initially traversing by INode.
    Subsequent modification still has consistency requirements. }

  Assert(not Assigned(FindPin(Tid)));
  Assert(Assigned(IPin));
  INode := IPin.INode;
  Assert(Assigned(INode));
  Assert(Assigned(INode.Pinned));

  Pin := NewPinned;
  DLItemInitObj(TObject(Pin), @Pin.Link);
  Pin.Item := INode.Pinned.AddRef as TMemDbStreamable;
  Assert(AssignedNotSentinel(Pin.Item));
  Pin.PinnedCurrentTid := IPin.PinnedTid;
  Pin.PinnedBy := Tid;
  Pin.FinalCheck := (Reason = pinFinalCheck);
  DLListInsertTail(@FPinnedItems, @Pin.Link);
  result := Pin.Item;
  Assert(Assigned(result));
end;

function TMemDbMultiBuffered.ReUseCurrentPinForINode(const Tid: TTransactionId; Pin: PMemDbPinnedItem; IPin: PMemDBIndexPin; Reason: TPinReason): TMemDbStreamable;
begin
  result := ReUseCurrentPinInternal(Pin, reason);
  Assert(Assigned(result));
  //Transitive consistency again, given we already have current pin,
  //then it must be the same at repeatable read or greater.
  if Tid.Iso >= ilReadRepeatable then
    if Pin.PinnedCurrentTid <> IPin.PinnedTid then
      raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_ABORT_WAR_6);
end;

function TMemDBMultiBuffered.PinForCursorFromInode(const Tid: TTRansactionId; INode: TMemDBIndexLeafGeneric; Reason: TPinReason): boolean;
var
  RefUp: TReferenceUpdate;
  TidUp: TTidUpdate;
  IPin: PMemDBIndexPin;
  CurPin: PMemDBPinnedItem;
  Item: TMemDBStreamable;
  Nxt, Cur: PMemDbMultiItem;
begin
  result := false;
  LockSelf;
  try
    DCPPre(RefUp);
    CPTidPre(Tid, TidUp);
    Assert(Tid <> TTransactionId.Empty);
    //First things first, check the INode is actually one of ours.
    if not ((Assigned(INode)) and (Assigned(INode.Row))
       and (Assigned(INode.Pinned)) and (INode.Row = self)) then
      raise EMemDbInternalException.Create(S_ERROR_BAD_INODE_DURING_PIN);
    IPin := FindIndexPin(INode);
    if not (Assigned(IPin) and (IPin.INode = INode)) then
      raise EMemDBInternalException.Create(S_ERROR_BAD_IPIN_DURING_PIN);

    //This is effectively GetPinLatest, but with a twist.

    //Two earlier cases most likely when row found not by index, and read/written.
    //so have a change and no current pin, or current pin not reached from iNode.
    Nxt := FindNxtMultiItem(Tid);
    Cur := FindCurMultiItem;
    if Assigned(Nxt) then
    begin
      //Consistency requirements => Transitive,
      //from INode, to CurPin, to Cur.Sel.Tid.
      //INode Tid must be same as Cur.Sel.Tid
      //If we have next at all, then CurPin, Cur.Sel.Tid has already been checked.

      //We could just run with it, and provide next, but then PreCommit check would fail,
      //and set of rows found by index not consistent with set of rows found
      //by non-index traversal.
      if Tid.Iso >= ilReadRepeatable then
        if IPin.PinnedTid <> Cur.Sel.TId then
          raise EMemDBConcurrencyException.Create(S_MODIFIED_CONCURRENT_ABORT_WAR_5);

      Assert(Assigned(Nxt.Item));
      Assert(not (INode.Pinned is TMemDeleteSentinel));
      if not (Nxt.Item is TMemDeleteSentinel) then
        Item := Nxt.Item
      else
        Item := nil;
    end
    else
    begin
      //Again, make set of rows found by index traversal same as set of
      //rows found by non-index traversal.
      CurPin := FindPin(Tid);
      if Assigned(CurPin) then
        Item := ReUseCurrentPinForINode(Tid, CurPin, IPin, Reason)
      else
        Item := NewCurrentPinFromINode(Tid, IPin, Reason); //The twist is here.
    end;
    result := Assigned(Item);
    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPLazyHandle(RefUp);
end;


function TMemDbMultiBuffered.PinForIndex(const Tid: TTransactionId; ItemSel: TAbSelType;
                     IndexNode: TMemDbIndexLeafGeneric): boolean;
var
  RefUp: TReferenceUpdate;
  Pin: PMemDBIndexPin;
  Item: TMemDBStreamable;
  Multi: PMemDBMultiItem;
  PinTid: TTransactionId;
begin
  result := false;
  LockSelf;
  try
    DCPPre(RefUp);
    if Assigned(IndexNode.Pinned) or Assigned(IndexNode.Row) then
      raise EMemDbInternalException.Create(S_INDEX_ALREADY_PINNED);
    Assert(not Assigned(FindIndexPin(IndexNode)));
    //Always pin with respect to current, next or latest, regardless of
    //other pins / links.
    case ItemSel of
      abCurrent: begin
        Multi := FindCurMultiItem;
        Assert(Assigned(Multi));
        Item := Multi.Item;
        PinTid := Multi.Sel.TId;
        Assert((not Assigned(Item)) or (not (Item is TMemDeleteSentinel)));
      end;
      abNext: begin
        Multi := FindNxtMultiItem(Tid);
        Item := nil;
        if Assigned(Multi) then
        begin
          Assert(Assigned(Multi.Item));
          if not (Multi.Item is TMemDeleteSentinel) then
          begin
            Item := Multi.Item;
            PinTid := Multi.Sel.TId;
          end;
        end;
      end;
      abLatest: begin
        Multi := FindNxtMultiItem(Tid);
        Item := nil;
        if Assigned(Multi) then
        begin
          Assert(Assigned(Multi.Item));
          if not (Multi.Item is TMemDeleteSentinel) then
          begin
            Item := Multi.Item;
            PinTid := Multi.Sel.TId;
          end;
        end
        else
        begin
          //Don't pin from TidLocal pins, pin what's "really" there.
          Multi := FindCurMultiItem;
          Assert(Assigned(Multi));
          Item := Multi.Item;
          PinTid := Multi.Sel.TId;
          Assert((not Assigned(Item)) or (not (Item is TMemDeleteSentinel)));
        end;
      end;
    else
      Assert(false);
      Item := nil;
    end;
    if not Assigned(Item) then
      exit;

    Pin := NewIndexPin;
    DLItemInitObj(TObject(Pin), @Pin.Link);

    Pin.INode := IndexNode; //No add-ref, node owns pin and will remove it.
    Pin.PinnedTid := PinTid;
    IndexNode.Pinned := Item.AddRef as TMemDBStreamable;
    IndexNode.Row := self;

    DLListInsertTail(@FIndexPins, @Pin.Link);
    Assert(Assigned(FindIndexPin(IndexNode)));
    DCPPost(RefUp);
    result := true;
  finally
    UnlockSelf;
  end;
  DCPLazyHandle(RefUp);
end;

procedure TMemDBMultiBuffered.DupIndexPin(SourceNode, DestNode: TMemDBIndexLeafGeneric);
var
  RefUp: TReferenceUpdate;
  Pin, NewPin: PMemDBIndexPin;

begin
  //No race condition here: both nodes are known to exist in a tree
  //which is undergoing manipulation, and the dop is called at the point
  //where both are known to exist.
  LockSelf;
  try
    DCPPre(RefUp);
    //Check existing pin is all wired up good.
    Assert(SourceNode.Row = Self);
    Pin := FindIndexPin(SourceNode);
    Assert(Assigned(Pin));
    Assert(Pin.INode = SourceNode);
    Assert(Assigned(SourceNode.Pinned));

    //New pin which is just like the existing pin.
    NewPin := NewIndexPin;
    DLItemInitObj(TObject(NewPin), @NewPin.Link);
    NewPin.INode := DestNode;
    NewPin.PinnedTid := Pin.PinnedTid;
    DestNode.Pinned := SourceNode.Pinned.AddRef as TMemDBStreamable;
    DestNode.Row := self;

    DLListInsertTail(@FIndexPins, @NewPin.Link);
    Assert(FindIndexPin(DestNode) = NewPin);
    DCPPost(RefUp);
  finally
    UnlockSelf;
  end;
  DCPLazyHandle(RefUp);
end;


procedure TMemDBMultiBuffered.UnpinFromIndex(IndexNode: TMemDbIndexLeafGeneric);
var
  RefUp: TReferenceUpdate;
  Pin: PMemDBIndexPin;
begin
  LockSelf;
  try
    DCPPre(RefUp);
    Pin := FindIndexPin(IndexNode);
    if not Assigned(Pin) then
      raise EMemDBInternalException.Create(S_INDEX_PIN_NOT_FOUND);
    Pin.INode := nil;  ///Belt and braces.
    DLListRemoveObj(@Pin.Link);
    DisposeIndexPin(Pin);
    DCPPost(RefUp);
  finally
    UnlockSelf;
  end;
  DCPLazyHandle(RefUp);
end;

function TMemDBMultiBuffered.HoldIndexPinInLock(Pin: PMemDbIndexPin): PMemDbIndexPin;
var
  RefNode: TReffed;
begin
  //Need to hold on to both the pin, and the index node.
  //Which gives us a race we need to fix:

  //Add-ref of index node above pin before any competing code deletes it (ok).
  //Add ref of index node above pin after has been decremented to 0, but
  //before unpin has acquired our lock here. (not ok).
  //Hence the TryAddRef call.

  //INode never NIL even in recursive hold situations? AddRef will handle it anyway.
  RefNode := Pin.INode.TryAddRef;
  if Assigned(RefNode) then
  begin
    //The INode really isn't going away.
    result := Pin;
  end
  else
    result := nil;
  //No DCP update: gone from >0 refs to >0 refs.
end;

//N.B Must be careful to hold/release in pairs, because it releases INode
//without further ado.
procedure TMemDBMultiBuffered.ReleaseIndexPinOutsideLock(Pin: PMemDbIndexPin);
begin
  //No UnpinFromIndex should have happened if we AddReffed OK.
  Assert(Assigned(Pin.INode));
  Pin.INode.Release; //Should call through to UnPinFromIndex.
end;

function TMemDBMultiBuffered.NewCurrentPinInternal(const Tid:TTransactionId; Reason:TPinReason): TMemDBStreamable;
var
  Cur, Nxt: PMemDBMultiItem;
  Pin: PMemDBPinnedItem;
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
    Pin := NewPinned;
    Pin.Item := Cur.Item.AddRef as TMemDBStreamable;
    DLItemInitObj(TObject(Pin), @Pin.Link);
    Pin.PinnedCurrentTid := Cur.Sel.TId;
    Pin.PinnedBy := Tid;
    Pin.FinalCheck := (Reason = pinFinalCheck);
    DLListInsertTail(@FPinnedItems, @Pin.Link);
    result := Pin.Item;
  end
  else
    result := nil;
end;

function TMemDBMultiBuffered.ReUseCurrentPinInternal(Pin: PMemDbPinnedItem; Reason: TPinReason): TMemDbStreamable;
begin
  //At the moment, at commit time, things are pinned for "final check", to ensure
  //user & code does not / cannot pin some later value.

  //This prevents transactions which have previously failed a commit from being
  //edited and recomitted. In most cases, there isn't much point: they are
  //either out of date (concurrency), or have something a bit wrong.

  //Email martin_c_harvey@hotmail.com if you'd like this changed.
  if (Reason = pinEvolve) and (Pin.FinalCheck) then
    raise EMemDBException.Create(S_PIN_EVOLVE_TOO_LATE_1);
  Pin.FinalCheck := Pin.FinalCheck or (Reason = pinFinalCheck);
  Assert(Assigned(Pin.Item));
  result := Pin.Item;
end;

function TMemDBMultiBuffered.PinCurrent(const Tid: TTransactionId; Reason: TPinReason): TMemDBStreamable;
var
  Pin: PMemDBPinnedItem;
  RefUp: TReferenceUpdate;
  TidUp: TTidUpdate;

begin
  LockSelf;
  try
    DCPPre(RefUp);
    CPTidPre(Tid, TidUp);
    Assert(Tid <> TTransactionId.Empty);
    result := nil;
    //Always return pin before current.
    Pin := FindPin(Tid);
    if Assigned(Pin) then
      result := ReUseCurrentPinInternal(Pin, Reason)
    else
      result := NewCurrentPinInternal(Tid, Reason);

    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPLazyHandle(RefUp);
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
  Nxt: PMemDBMultiItem;
  Pin: PMemDbPinnedItem;
  RefUp: TReferenceUpdate;
  TidUp: TTidUpdate;

begin
  LockSelf;
  try
    DCPPre(RefUp);
    CPTidPre(Tid, TidUp);
    Assert(Tid <> TTransactionId.Empty);

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
        result := ReUseCurrentPinInternal(Pin, Reason)
      else
        result := NewCurrentPinInternal(Tid, Reason);
      BufSelected := abCurrent;
    end;
    DCPPost(RefUp);
    CPTidPost(Tid, TidUp);
  finally
    UnlockSelf;
  end;
  CPTidHandle(TidUp);
  DCPLazyHandle(RefUp);
end;

constructor TMemDbMultiBuffered.Create;
var
  Cur: PMemDBMultiItem;
begin
  inherited;
  DLItemInitList(@FMultiItems);
  DLItemInitList(@FPinnedItems);
  DLItemInitList(@FIndexPins);
  Cur := NewMulti;
  Cur.Sel := MakeCurrentBufSelector(TTransactionId.Empty);
  DLItemInitObj(TObject(Cur), @Cur.Link);
  DLListInsertHead(@FMultiItems, @Cur.Link);
end;

destructor TMemDBMultiBuffered.Destroy;
var
  Item: PMemDbMultiItem;
  Pin: PMemDBPinnedItem;
  IdxPin: PMemDBIndexPin;
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

    Assert(DlItemIsEmpty(@FIndexPins));
    IdxPin := PMemDBIndexPin(FIndexPins.FLink.Owner);
    while Assigned(IdxPin) do
    begin
      //Oh dear. Index pins depend on tree ref counting, which seems to
      //have gone awry. We can't fix that here, just clean up memory and exit.
      DLListRemoveObj(@IdxPin.Link);
      DisposeIndexPin(IdxPin);
      IdxPin := PMemDBIndexPin(FIndexPins.FLink.Owner);
    end;
  finally
    UnlockSelf;
  end;
  inherited;
end;

procedure TMemDbMultiBuffered.ToJournal(const Tid: TTransactionId; Stream: TStream);
var
  Cur, Nxt: PMemDbMultiItem;
  Current, Next: TMemDbStreamable;
begin
  //Called under commit lock, cur/nxt should not change.
  inherited;
  LockSelf;
  try
    Cur := FindCurMultiItem;
    Current := Cur.Item;
    Nxt := FindNxtMultiItem(Tid);
    Next := Nxt.Item;
  finally
    UnlockSelf;
  end;
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
