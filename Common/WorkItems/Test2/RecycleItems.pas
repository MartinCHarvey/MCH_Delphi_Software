unit RecycleItems;

{ Martin Harvey 2015 }

interface

uses WorkItems, IndexedStore, SyncObjs, Winapi.Windows,
    System.Classes;

{ A quick demonstration of how to use workitems to split and join work.
  Find all prime numbers between 0 and a lot.

  We do not disambiguate between workitems and the buffers they work on
  - that's for later examples on affinity. }

const
  BITS_PER_CARDINAL = (sizeof(cardinal) * 8);
  //64 bit compiler does not optimise out constant div and mod by
  //powers of two. Oh dear.
  BITS_PER_CARDINAL_SHIFT = 5;
  BITS_PER_CARDINAL_MASK = $1F;
  CHUNK_SIZE = 65536 * BITS_PER_CARDINAL;
  E_WORKITEM_INTERNAL_ERROR = -1;
  E_WORKITEM_ALLOC_FAILED = -2;
  E_WORKITEM_BMP_COUNT_ERROR = -3;
  E_WORKITEM_TREE_OP_ERROR = -4;
  E_BAD_PARAMS = -5;
  E_NO_DATA = -6;
  E_WORKITEM_REQUEUE_FAILED_Q_FLUSH = -7;
  E_WORKITEM_REQUEUE_CANCELLED_FIRST = -8;

type
  PCard = ^cardinal;
  TReusableObjStore = class;

  TPrimeWorkItemPhase = (wipForkChildren, wipCalcOwnBitmap,
                         wipMergeBitmaps);

  TRequeueState = (rsIdle, rsQOrExec, rsQOrExecRequeue,
                   rsQOrExecCancel, rsCancelled);

  { A workitem with a task or results for prime numbers between X and Y }
  TPrimeWorkItem = class(TWorkItem)
  private
    FPhase: TPrimeWorkItemPhase;
    //Carry state over from work function to completion handler,
    //to determine next action upon completion.
    FNextPhase: boolean;
    FRequeueState: integer;
    FLower, FUpper: cardinal;
    FParentStore: TReusableObjStore;
    FBmp: PCard;
    FBmpCount: cardinal;
  protected
    function GetBmpDwordSize: cardinal;
    procedure SetBit(BmpBitOfs: cardinal; Val: boolean);

    procedure SetLower(NewLower: cardinal);
    procedure SetUpper(NewUpper: cardinal);

    function ForkChildren: integer; //0 or neg error code.
    function CalcBmpCount: integer; //0 or neg error code.
    function CalcBmp: integer; //0 or neg error code.
    function MergeBmps: integer; //# merges or neg error code.
    function InsertAndRequeue(Requeue: boolean): integer;
    function RemoveFromTree: integer;

    function DoWork: integer; override;
    procedure DoNormalCompletion; override;

    class function CombineBlocks(ResultBlock, HigherBlock: TPrimeWorkItem): integer;

    function RequestRequeue: integer;
    function RequestCancel: integer;
    function PerformDelayedRequeues: integer;
    function PerformDelayedCancels: integer;
  public
    procedure DoCancelledCompletion; override;
    destructor Destroy; override;
    procedure ResetLocalState;
    function CheckBit(BmpBitOfs: cardinal): boolean;

    property Lower: cardinal read Flower write SetLower;
    property Upper: cardinal read FUpper write SetUpper;
    property Phase:TPrimeworkItemPhase read FPhase;
    property ParentStore: TReusableObjStore read FParentStore write FParentStore;
    property BmpCount: cardinal read FBmpCount;
  end;

// A store for the various reusable workitems.
// Contains both a free queue for unused work items,
// and a results store (which is avl tree indexed) for completed items.

  TReusableObjStoreState = (rosIdle, rosCalcing, rosResults);

  TStatusStruct = record
    NumPrimes: cardinal;
    LowBound: cardinal;
    HighBound: cardinal;
    Spawned: integer;
    Queued: integer;
    Merges: integer;
  end;

  TReusableObjStore = class
  private
    //Free workitems.
    FWorkItemFreeQueue: TOwnLockWorkQueue;
    //Threadpool
    FWorkFarm: TWorkFarm;
    //Results store and associated lock.
    FResultsStore: TIndexedStore;
    FResultsLock: TCriticalSection;
    FOnCalcFinished: TNotifyEvent;
    FStoreState: TReusableObjStoreState;
    FRequestedUpper, FRequestedLower: cardinal;

    //And some statistics.
    FBlocksSpawned: integer;
    FBlocksQueued: integer;
    FMergeOps: integer;
  protected
    function AllocWI:TPrimeWorkItem;
    procedure FreeWI(Old: TPrimeWorkItem);
    property WorkFarm:TWorkFarm read FWorkFarm;
    procedure CheckFinishedMerge;
  public
    constructor Create;
    destructor Destroy; override;

    function CalcPrimes(Lower, Upper:cardinal):integer;
    function CheckPrime(Prime: cardinal; var Yes: boolean): integer;
    procedure Reset;
    procedure GetStatus(var State:TReusableObjStoreState;
                        var Status:TStatusStruct);

    property OnCalcFinished: TNotifyEvent
      read FOnCalcFinished write FOnCalcFinished;
  end;

{ Classes below here for implementation of indexing on results store }

  TPrimeWorkItemIndex = class(TIndexNode)
  protected
    FNodeIsSearchVal: boolean;
    FSearchLow: boolean;
    FSearchVal: cardinal;
    function CompareItems(OwnItem, OtherItem: TObject): integer; override;
  public
    property NodeIsSearchVal: boolean read FNodeIsSearchVal write FNodeIsSearchVal;
    property SearchVal: cardinal read FSearchVal write FSearchVal;
  end;

  TLowerBoundIndex = class(TPrimeWorkItemIndex)
  public
    constructor Create; override;
  end;

  TUpperBoundIndex = class(TPrimeWorkItemIndex)
  public
    constructor Create; override;
  end;

  TIndexTags = (iTagPointer, iTagLower, iTagUpper);

implementation

{ TPrimeWorkItem }

function TPrimeWorkItem.GetBmpDwordSize: cardinal;
begin
  result := FUpper - FLower;
  Assert((result and BITS_PER_CARDINAL_MASK) = 0);
  result := result shr BITS_PER_CARDINAL_SHIFT;
end;

procedure TPrimeWorkItem.SetLower(NewLower: cardinal);
begin
  Assert((NewLower and BITS_PER_CARDINAL_MASK) = 0);
  FLower := NewLower;
end;

procedure TPrimeWorkItem.SetUpper(NewUpper: cardinal);
begin
  Assert((NewUpper and BITS_PER_CARDINAL_MASK) = 0);
  FUpper := NewUpper;
end;

function TPrimeWorkItem.CheckBit(BmpBitOfs: cardinal): boolean;
var
  mask: cardinal;
  pp: PCard;
begin
  Assert(BmpBitOfs < (GetBmpDwordSize * BITS_PER_CARDINAL));
  mask := Cardinal(1) shl (BmpBitOfs and BITS_PER_CARDINAL_MASK);
  pp := FBmp;
  Inc(pp, BmpBitOfs shr BITS_PER_CARDINAL_SHIFT);
  result := ((pp^) and mask) <> 0;
end;

procedure TPrimeWorkItem.SetBit(BmpBitOfs: cardinal; Val: boolean);
var
  mask: cardinal;
  pp: PCard;
begin
  Assert(BmpBitOfs < (GetBmpDwordSize * BITS_PER_CARDINAL));
  mask := Cardinal(1) shl (BmpBitOfs and BITS_PER_CARDINAL_MASK);
  pp := FBmp;
  Inc(pp, BmpBitOfs shr BITS_PER_CARDINAL_SHIFT);
  if Val then
    pp^ := pp^ or mask
  else
    pp^ := (pp^) and not mask;
end;

function TPrimeWorkItem.ForkChildren: integer;
var
  Child: TPrimeWorkItem;
  SplitPoint: cardinal;
  BmpDwSize: cardinal;

begin
  if FUpper - FLower > CHUNK_SIZE then
  begin
    SplitPoint := (FUpper div 2) + (FLower div 2);
    SplitPoint := SplitPoint - (SplitPoint and BITS_PER_CARDINAL_MASK);
    Assert(SplitPoint < FUpper);
    Assert(SplitPoint > FLower);
    InterlockedIncrement(FParentStore.FBlocksSpawned);
    Child := FParentStore.AllocWI;
    Child.Lower := SplitPoint;
    Child.Upper := FUpper;
    FUpper := SplitPoint;
    Child.FPhase := wipForkChildren;
    if Child.RequestRequeue <> 0 then
      Child.RequestCancel;
  end
  else
  begin
    BmpDwSize := GetBmpDwordSize;
    FBmp := AllocMem(BmpDwSize * sizeof(cardinal)); //Mem is zeroed.
    if not Assigned(FBmp) then
    begin
      result := E_WORKITEM_ALLOC_FAILED;
      exit;
    end;
    FNextPhase := true;
  end;
  result := 0;
end;

function TPrimeWorkItem.CalcBmp: integer;
var
  Divisor, Mark: cardinal;

begin
  //Have to deal with 0 and 1 as a special case.
  if Lower < 2 then
    SetBit(1 - Lower, true);
  if Lower < 1 then
    SetBit(0, true);

  //What range of divisors do we need?
  //Lowest is going to be 2.
  //Highest will be that which when doubled, might stand a chance
  //of hitting our bitmap.

  for Divisor := 2 to (Upper div 2) do
  begin
    Mark := Lower - (Lower mod Divisor);
    if Mark < (2*Divisor) then
      Mark := 2 * Divisor;
    while (Mark < Upper) do
    begin
      if (Mark >= Lower) then
        SetBit((Mark - Lower), true);
      Mark := Mark + Divisor;
    end;
  end;
  result := 0;
end;

function TPrimeWorkItem.CalcBmpCount: integer;
var
  BmpBitOfs: cardinal;
  count : cardinal;
begin
  count := 0;
  Assert(Upper > Lower);
  for BmpBitOfs := 0 to Pred(Upper) - Lower do
  begin
    if not CheckBit(BmpBitOfs) then
      Inc(Count);
  end;
  result := count;
end;

function TPrimeWorkItem.InsertAndRequeue(Requeue: boolean): integer;
var
  IRec: TItemRec;
  Ins: boolean;
begin
  FParentStore.FResultsLock.Acquire;
  try
    Assert((FPhase = wipMergeBitmaps) or (FPhase = wipCalcOwnBitmap));
    ins := (FParentStore.FResultsStore.AddItem(self, IRec) = rvOK);
    if ins then
      result := 0
    else
      result := E_WORKITEM_TREE_OP_ERROR;
    //Need to request requeue under tree lock, otherwise,
    //merge from elsewhere can take the block out of the tree, and cancel
    //it before we get round to requesting a requeue.
    if (Result = 0) and ReQueue then
      Result := RequestRequeue;

    if Result <> 0 then
    begin
      FParentStore.FResultsStore.RemoveItem(IRec);
    end;
  finally
    FParentStore.FResultsLock.Release;
  end;
end;

function TPrimeWorkItem.RemoveFromTree: integer;
var
  IRec: TItemRec;
  irv: TIsRetVal;
  MyINode: TSearchPointerINode;
begin
  FParentStore.FResultsLock.Acquire;
  MyINode := nil;
  try
    MyINode := TSearchPointerINode.Create;
    MyINode.SearchVal := self;
    irv := FParentStore.FResultsStore.FindByIndex(Ord(iTagPointer), MyINode, IRec);
    if irv = rvOK then
    begin
      irv := FParentStore.FResultsStore.RemoveItem(IRec);
    end;
  finally
    MyINode.Free;
    FParentStore.FResultsLock.Release;
  end;
  if irv = rvOK then
    result := 0
  else
    result := E_WORKITEM_TREE_OP_ERROR;
end;

class function TPrimeWorkItem.CombineBlocks(ResultBlock, HigherBlock: TPrimeWorkItem): integer;
var
 LowSize, HighSize, ResSize: cardinal;
 CountCheck:cardinal;
 Tmp, Tmp2: PCard;
 StoredUpper: cardinal;
begin
  //Calc sizes.
  Assert(ResultBlock.FUpper = HigherBlock.FLower);
  Assert(Assigned(ResultBlock.FBmp));
  Assert(Assigned(HigherBlock.FBmp));

  LowSize := ResultBlock.GetBmpDwordSize;
  HighSize := HigherBlock.GetBmpDwordSize;

  StoredUpper := ResultBlock.FUpper;
  ResultBlock.FUpper := HigherBlock.FUpper;

  ResSize := ResultBlock.GetBmpDwordSize;
  Assert(LowSize + HighSize = ResSize);

  //Reallocate memory blocks.
  Tmp := AllocMem(ResSize * sizeof(cardinal));
  if Assigned(Tmp) then
  begin
    CopyMemory(Tmp, ResultBlock.FBmp, LowSize * sizeof(cardinal));
    Tmp2 := Tmp;
    Inc(Tmp2, LowSize); //Check pointer arithmetic.
    CopyMemory(Tmp2, HigherBlock.FBmp, HighSize * sizeof(cardinal));
    FreeMem(ResultBlock.FBmp);
    ResultBlock.FBmp := Tmp;
    //Check the count is correct.
    ResultBlock.FBmpCount := ResultBlock.FBmpCount + HigherBlock.FBmpCount;
    CountCheck := ResultBlock.CalcBmpCount;
    Assert(CountCheck = ResultBlock.FBmpCount);
    result := 0;
  end
  else
  begin
    //Reset sizes and counts
    ResultBlock.FUpper := StoredUpper;
    result := E_WORKITEM_ALLOC_FAILED;
  end;
end;

function TPrimeWorkItem.MergeBmps:integer;
var
  MyINode: TSearchPointerINode;
  UpperINode: TUpperBoundIndex;
  LowerINode: TLowerBoundIndex;
  MyIRec, UpperIRec, LowerIRec: TItemRec;
  LowerBlock, UpperBlock, MiddleBlock: TPrimeWorkItem;
begin
  //If workitems are in the tree, then they are candidates for merge.
  //If not in the tree, then can't be merged.
  //Workitems in consistent state when in tree.
  InterlockedIncrement(FParentStore.FMergeOps);

  //INode to search for self.
  MyINode := TSearchPointerINode.Create;
  MyINode.SearchVal := self;
  //INode to search for block below us.
  UpperINode := TUpperBoundIndex.Create;
  UpperINode.NodeIsSearchVal := true;
  UpperINode.SearchVal := Self.Lower;
  //INode to search for block above us.
  LowerINode := TLowerBoundIndex.Create;
  LowerINode.NodeIsSearchVal := true;
  LowerINode.SearchVal := Self.Upper;
  //Zero IRecs
  MyIRec := nil;
  LowerIRec := nil;
  UpperIRec := nil;
  //And pointers to other two blocks if appropriate.
  LowerBlock := nil;
  UpperBlock := nil;
  MiddleBlock := nil;

  FParentStore.FResultsLock.Acquire;
  try
    FParentStore.FResultsStore.FindByIndex(Ord(iTagPointer), MyINode, MyIRec);
    FParentStore.FResultsStore.FindByIndex(Ord(iTagLower), LowerINode, LowerIRec);
    FParentStore.FResultsStore.FindByIndex(Ord(ITagUpper), UpperINode, UpperIRec);
    if Assigned(MyIRec) then
    begin
      MiddleBlock := MyIRec.Item as TPrimeWorkItem;
      FParentStore.FResultsStore.RemoveItem(MyIRec);
      if Assigned(LowerIRec) then //Searching on lower index, hence block above.
      begin
        UpperBlock := LowerIRec.Item as TPrimeWorkItem;
        FParentStore.FResultsStore.RemoveItem(LowerIRec);
      end;
      if Assigned(UpperIRec) then //Searching on higher index, hence block below.
      begin
        LowerBlock := UpperIRec.Item as TPrimeWorkItem;
        FParentStore.FResultsStore.RemoveItem(UpperIRec);
      end;
    //Blocks atomically removed.
    end;
  finally
    FParentStore.FResultsLock.Release;
  end;

  result := 0;
  if Assigned(MiddleBlock) then
  begin
    Assert(MiddleBlock = self);
    if Assigned(UpperBlock) then
    begin
       //Merge upper block into middle block.
      if CombineBlocks(MiddleBlock, UpperBlock) = 0 then
      begin
        Inc(result);
        UpperBlock.RequestCancel;
        UpperBlock := nil;
      end;
    end;
    if Assigned(LowerBlock) then
    begin
      if CombineBlocks(LowerBlock, MiddleBlock) = 0 then
      begin
        Inc(result);
        MiddleBlock.RequestCancel;
        MiddleBlock := LowerBlock;
        LowerBlock := nil;
      end;
    end;
  end;

  if Assigned(MiddleBlock) then
  begin
    //Have we done any merging, if so, try to keep merging, by requeueing.
    if MiddleBlock.InsertAndRequeue(result > 0) <> 0 then
      MiddleBlock.RequestCancel;
  end;
  if Assigned(UpperBlock) then
  begin
    if UpperBlock.InsertAndRequeue(true) <> 0 then
      UpperBlock.RequestCancel;
  end;
  if Assigned(LowerBlock) then
  begin
    if LowerBlock.InsertAndRequeue(true) <> 0 then
      LowerBlock.RequestCancel;
  end;
  //If did not manage to merge anything, set FNextPhase,
  //to get GUI logic to check whether we are done.
  FNextPhase := (result = 0);
end;

function TPrimeWorkItem.DoWork: integer;
begin
  FNextPhase := false;
  case FPhase of
    wipForkChildren:
      result := ForkChildren; //May set next phase.
    wipCalcOwnBitmap:
    begin
      result := CalcBmp;
      if result = 0 then
      begin
        FBmpCount := CalcBmpCount;
        result := InsertAndRequeue(true);
      end;
      if result = 0 then
        FNextPhase := true;
    end;
    wipMergeBitmaps:
    begin
        result := MergeBmps;
    end;
  else
    result := E_WORKITEM_INTERNAL_ERROR;
  end;
  if Result < 0 then
    RequestCancel;
end;

procedure TPrimeWorkItem.DoNormalCompletion;
var
  ReQueue: boolean;
begin
  ReQueue := false;
  case FPhase of
    wipForkChildren:
    begin
      ReQueue := true;
      if FNextPhase then
        FPhase := wipCalcOwnBitmap;
    end;
    wipCalcOwnBitmap:
    begin
      Assert(FNextPhase); //If not then cancelled completion.
      ReQueue := true;
      FPhase := wipMergeBitmaps;
    end;
    wipMergeBitmaps:
    begin
      ReQueue := false;
      if FNextPhase then
        FParentStore.CheckFinishedMerge;
    end;
  else
    Assert(false);
  end;
  //If we have not cancelled ourself, and we need to re-queue...
  if ReQueue then
  begin
    if RequestRequeue <> 0 then
      RequestCancel;
  end;
  if PerformDelayedRequeues <> 0 then
    RequestCancel;
end;

procedure TPrimeWorkItem.DoCancelledCompletion;
begin
  PerformDelayedCancels;
end;

destructor TPrimeWorkItem.Destroy;
begin
  FreeMem(FBmp);
  inherited;
end;

procedure TPrimeWorkItem.ResetLocalState;
begin
  FPhase := wipForkChildren;
  FNextPhase:= false;
  FLower := 0;
  FUpper := 0;
  FreeMem(FBmp);
  FBmp := nil;
  FBmpCount:= 0;
  FRequeueState := Ord(rsIdle);
end;

//  TRequeueState = (rsIdle, rsQOrExec, rsQOrExecRequeue);

function TPrimeWorkItem.RequestRequeue: integer;
var
  Again: boolean;
  LocalState, BackoutState: integer;
begin
  Again := true;
  result := E_WORKITEM_INTERNAL_ERROR;
  repeat
    LocalState := FRequeueState;
    case LocalState of
      Ord(rsIdle):
      begin
        if InterlockedCompareExchange(FRequeueState, Ord(rsQOrExec), Ord(rsIdle)) = Ord(rsIdle) then
        begin
          Again := false;
          if FParentStore.FWorkFarm.AddWorkItem(self) then
          begin
            InterlockedIncrement(FParentStore.FBlocksQueued);
            result := 0
          end
          else
          begin
            InterlockedExchange(FRequeueState, Ord(rsIdle));
            //Don't try to cancel here, surriounding code will almost certainly
            //cancel, when this fn fails,
            //and if we try to cancel twice, bad things will happen.
            result := E_WORKITEM_REQUEUE_FAILED_Q_FLUSH;
          end;
        end;
      end;
      Ord(rsQOrExec):
      begin
        if InterlockedCompareExchange(FRequeueState, Ord(rsQOrExecRequeue), Ord(rsQOrExec)) = Ord(rsQOrExec) then
        begin
          Again := false;
          result := 0;
        end;
      end;
      Ord(rsQOrExecRequeue):
      begin
        Again := false;
        result := 0;
      end;
      Ord(rsQOrExecCancel):
      begin
        //Queued workitem, cancel already requested, cannot requeue.
        Again := false;
        result := E_WORKITEM_REQUEUE_CANCELLED_FIRST;
      end;
    else
      //Also Ord(rsCancelled);
      Again := false;
      Assert(false);
      result := E_WORKITEM_INTERNAL_ERROR;
    end;
  until not Again;
end;

//TODO. Some failure paths may result in final cancellation not happening.
//check this.

function TPrimeWorkItem.PerformDelayedRequeues: integer;
var
  Again: boolean;
  LocalState: integer;
  BackoutState: integer;
begin
  Again := true;
  result := E_WORKITEM_INTERNAL_ERROR;
  repeat
    LocalState := FRequeueState;
    case LocalState of
      Ord(rsQOrExec):
      begin
        //Just finished executing, can requeue, but not requested.
        if InterlockedCompareExchange(FRequeueState, Ord(rsIdle), Ord(rsQOrExec)) = Ord(rsQOrExec) then
        begin
          Again := false;
          result := 0;
        end;
      end;
      Ord(rsQOrExecRequeue):
      begin
        //Just finished executing, and we need a requeue,
        //back to the am queued state, via a requeue op.
        if InterlockedCompareExchange(FRequeueState, Ord(rsQOrExec), Ord(rsQOrExecRequeue)) = Ord(rsQOrExecRequeue) then
        begin
          Again := false;
          if FParentStore.FWorkFarm.AddWorkItem(self) then
          begin
            InterlockedIncrement(FParentStore.FBlocksQueued);
            result := 0
          end
          else
          begin
            InterlockedExchange(FRequeueState, Ord(rsIdle));
            result := E_WORKITEM_REQUEUE_FAILED_Q_FLUSH;
          end;
        end;
      end;
      Ord(rsQOrExecCancel):
      begin
        //Async cancel, but we have got a long way down the completion
        //path.
        Again := false;
        result := PerformDelayedCancels;
      end;
    else
      //Also Ord(rsCancelled), also Ord(rsIdle)
      Again := false;
      Assert(false);
      result := E_WORKITEM_INTERNAL_ERROR;
    end;
  until not Again;
end;

function TPrimeWorkItem.RequestCancel: integer;
var
  Again: boolean;
  LocalState: integer;
begin
  Again := true;
  result := E_WORKITEM_INTERNAL_ERROR;
  repeat
    LocalState := FRequeueState;
    case LocalState of
      Ord(rsIdle):
      begin
        if InterlockedCompareExchange(FRequeueState, Ord(rsCancelled), Ord(rsIdle)) = Ord(rsIdle) then
        begin
          Again := false;
          result := 0;
          //Do the final teardown code now.
          RemoveFromTree;
          FParentStore.FreeWI(self);
        end;
      end;
      Ord(rsQOrExec):
      begin
        //Queued or running, put into pending cancel state.
        if InterlockedCompareExchange(FRequeueState, Ord(rsQOrExecCancel), Ord(rsQOrExec)) = Ord(rsQOrExec) then
        begin
          Cancel;
          Again := false;
          result := 0;
        end;
      end;
      Ord(rsQOrExecRequeue):
      begin
        //Queued or running, put into pending cancel state.
        //and in addition don't requeue.
        if InterlockedCompareExchange(FRequeueState, Ord(rsQOrExecCancel), Ord(rsQOrExecRequeue)) = Ord(rsQOrExecRequeue) then
        begin
          Cancel;
          Again := false;
          result := 0;
        end;
      end;
      Ord(rsQOrExecCancel):
      begin
        //Duplicate cancellation request,do not need to do anything.
        Again := false;
        result := 0;
      end;
    else
      //Also Ord(rsCancelled);
      Again := false;
      Assert(false);
      result := E_WORKITEM_INTERNAL_ERROR;
    end;
  until not Again;
end;

function TPrimeWorkItem.PerformDelayedCancels: integer;
var
  Again: boolean;
  LocalState: integer;

begin
  Again := true;
  result := E_WORKITEM_INTERNAL_ERROR;
  repeat
    LocalState := FRequeueState;
    if (LocalState = Ord(rsQOrExec))
      or (LocalState = Ord(rsQOrExecRequeue))
      or (LocalState = Ord(rsQOrExecCancel)) then
    begin
      if InterlockedCompareExchange(FRequeueState, Ord(rsCancelled), LocalState) = LocalState then
      begin
        Again := false;
        result := 0;
        //Do the final teardown code now.
        RemoveFromTree;
        FParentStore.FreeWI(self);
      end;
    end
    else
    begin
      //LocalState rsCancelled, rsIdle
      Again := false;
      Assert(false);
      result := E_WORKITEM_INTERNAL_ERROR;
    end;
  until not Again;
end;

{ TReusableObjStore }

function TReusableObjStore.AllocWI:TPrimeWorkItem;
var
  WI: TWorkItem;
begin
  WI := FWorkItemFreeQueue.RemoveWorkItem;
  if Assigned(WI) then
    result := (WI as TPrimeWorkItem)
  else
  begin
    result := TPrimeWorkItem.Create;
    result.CanAutoFree := false;
    result.CanAutoReset := true;
    result.ParentStore := self;
  end;
end;

procedure TReusableObjStore.FreeWI(Old: TPrimeWorkItem);
begin
  Old.ResetLocalState;
  FWorkItemFreeQueue.AddWorkItem(Old);
end;

constructor TReusableObjStore.Create;
var
  rv: TISRetVal;
begin
  inherited;
  FWorkFarm := TWorkFarm.Create;
  FWorkItemFreeQueue := TOwnLockWorkQueue.Create;
  FResultsLock := TCriticalSection.Create;
  FResultsStore := TIndexedStore.Create;
  rv := FResultsStore.AddIndex(TPointerINode, Ord(iTagPointer));
  Assert(rv = rvOK);
  rv := FResultsStore.AddIndex(TLowerBoundIndex, Ord(iTagLower));
  Assert(rv = rvOK);
  rv := FResultsStore.AddIndex(TUpperBoundIndex, Ord(iTagUpper));
  Assert(rv = rvOK);
  FStoreState := rosIdle;
end;

destructor TReusableObjStore.Destroy;
var
  WI: TWorkItem;
  IRec: TItemRec;
  IRetVal: TISRetVal;
begin
  FWorkFarm.Free; //This also flushes.

  WI := FWorkItemFreeQueue.RemoveWorkItem;
  while Assigned(WI) do
  begin
    WI.Free;
    WI := FWorkItemFreeQueue.RemoveWorkItem;
  end;
  FWorkItemFreeQueue.Free;

  IRec := FResultsStore.GetAnItem;
  while Assigned(IRec) do
  begin
    IRec.Item.Free;
    IRetVal := FResultsStore.RemoveItem(IRec);
    Assert(IRetVal = rvOK);
    IRec := FResultsStore.GetAnItem;
  end;
  FResultsStore.Free;
  FResultsLock.Free;

  inherited;
end;

procedure TReusableObjStore.Reset;
var
  IRec: TItemRec;
begin
  FWorkFarm.FlushAndWait(true);
  FResultsLock.Acquire;
  try
    IRec := FResultsStore.GetAnItem;
    while Assigned(IRec) do
    begin
      (IRec.Item as TPrimeWorkItem).RequestCancel; //Should remove and recycle.
      IRec := FResultsStore.GetAnItem;
    end;
    FStoreState := rosIdle;
    FRequestedUpper := 0;
    FRequestedLower := 0;

    FBlocksSpawned := 0;
    FBlocksQueued := 0;
    FMergeOps := 0;
  finally
    FResultsLock.Release;
  end;
end;

function TReusableObjStore.CalcPrimes(Lower, Upper:cardinal):integer;
var
  IRec: TItemRec;
  Tmp: cardinal;
  FirstItem: TPrimeWorkItem;
begin
  //Get params into some sensible shape.
  if Lower = Upper then
  begin
    result := E_BAD_PARAMS;
    exit;
  end;
  if Lower > Upper then
  begin
    Tmp := Lower;
    Lower := Upper;
    Upper := Tmp;
  end;
  if ((Lower) and BITS_PER_CARDINAL_MASK) <> 0 then
  begin
    Dec(Lower, (Lower and BITS_PER_CARDINAL_MASK));
  end;
  if Upper > (High(Cardinal) - (BITS_PER_CARDINAL * 2)) then
  begin
    result := E_BAD_PARAMS;
    exit;
  end;
  if ((Upper - Lower) and BITS_PER_CARDINAL_MASK) <> 0 then
  begin
    Inc(Upper, BITS_PER_CARDINAL - ((Upper - Lower) and BITS_PER_CARDINAL_MASK));
  end;
  //Okay, expected bitmap is some sensible multiple of bits per cardinal.
  //and bounds are OK as far as we can see.
  Reset;
  FStoreState := rosCalcing;
  FRequestedUpper := Upper;
  FRequestedLower := Lower;

  result := 0;
  FirstItem := AllocWI;
  if Assigned(FirstItem) then
  begin
    InterlockedIncrement(FBlocksSpawned);
    FirstItem.Lower := Lower;
    FirstItem.Upper := Upper;
    if FirstItem.RequestRequeue <> 0 then
    begin
      FreeWI(FirstItem);
      result := E_WORKITEM_INTERNAL_ERROR;
    end;
  end
  else
      result := E_WORKITEM_ALLOC_FAILED;
  if result <> 0 then
  begin
    FResultsLock.Acquire;
    try
      FStoreState := rosIdle;
      FRequestedUpper := 0;
      FRequestedLower := 0;
    finally
      FResultsLock.Release;
    end;
    if Assigned(FOnCalcFinished) then
      FOnCalcFinished(self);
  end;
end;

procedure TReusableObjStore.CheckFinishedMerge;
var
  IRec, IRec2: TItemRec;
  WI: TPrimeWorkItem;
  Done: boolean;
begin
  Done := false;
  FResultsLock.Acquire;
  try
    if FStoreState = rosCalcing then
    begin
      //Hopefully only one item in store, with upper and lower as expected.
      //anything else indicates some error whereby calculation did not
      //terminate, perhaps because of some internal error.
      IRec := FResultsStore.GetAnItem;
      IRec2 := IRec;
      FResultsStore.GetAnotherItem(IRec2);
      if Assigned(IRec) and not Assigned(IRec2) then
      begin
        //Exactly one item in object store.
        WI := IRec.Item as TPrimeWorkItem;
        if (WI.Upper = FRequestedUpper) and (WI.Lower = FRequestedLower) then
        begin
          FStoreState := rosResults;
          Done := true;
        end;
      end;
    end;
  finally
    FResultsLock.Release;
  end;
  if Done and Assigned(FOnCalcFinished) then
    FOnCalcFinished(self);
end;

function TReusableObjStore.CheckPrime(Prime: cardinal; var Yes: boolean): integer;
var
  IRec: TItemRec;
  WI: TPrimeWorkItem;
  BmpBitOfs: cardinal;
begin
  result := E_WORKITEM_INTERNAL_ERROR;
  FResultsLock.Acquire;
  try
    if FStoreState = rosResults then
    begin
      IRec := FResultsStore.GetAnItem;
      if Assigned(IRec) then
      begin
        WI := IRec.Item as TPrimeWorkItem;
        if (Prime >= WI.Lower) and (Prime < WI.Upper) then
        begin
          BmpBitOfs := Prime - WI.Lower;
          Yes := not WI.CheckBit(BmpBitOfs);
          result := 0;
        end
        else
          result := E_BAD_PARAMS;
      end
    end
    else
      result := E_NO_DATA;
  finally
    FResultsLock.Release;
  end;
end;

procedure TReusableObjStore.GetStatus(var State:TReusableObjStoreState;
                                      var Status: TStatusStruct);
var
  IRec: TItemRec;
  WI: TPrimeWorkItem;
begin
  FResultsLock.Acquire;
  try
    State := FStoreState;
    if FStoreState = rosResults then
    begin
      IRec := FResultsStore.GetAnItem;
      if Assigned(IRec) then
      begin
        WI := IRec.Item as TPrimeWorkItem;
        with Status do
        begin
          NumPrimes := WI.BmpCount;
          LowBound := WI.Lower;
          HighBound := WI.Upper;
          Spawned := FBlocksSpawned;
          Queued := FBlocksQueued;
          Merges := FMergeOps;
        end;
      end;
    end;
  finally
    FResultsLock.Release;
  end;
end;

{ TPrimeWorkItemIndex }

function TPrimeWorkItemIndex.CompareItems(OwnItem: TObject; OtherItem: TObject): integer;
var
  OwnVal: cardinal;
  OtherVal: cardinal;
begin
  Assert(Assigned(OtherItem));
  if FSearchLow then
    OtherVal := (OtherItem as TPrimeWorkItem).Lower
  else
    OtherVal := (OtherItem as TPrimeWorkItem).Upper;
  if NodeIsSearchVal then
  begin
    //Comparing search val against other.
    Assert(not Assigned(OwnItem));
    OwnVal := FSearchVal;
  end
  else
  begin
    //Comparing own against other.
    Assert(Assigned(OwnItem));
    if FSearchLow then
      OwnVal := (OwnItem as TPrimeWorkItem).Lower
    else
      OwnVal := (OwnItem as TPrimeWorkItem).Upper;
  end;
  if OtherVal > OwnVal then
    result := 1
  else if OtherVal < OwnVal then
    result := -1
  else
    result := 0;
end;

{ TLowerBoundIndex }

constructor TLowerBoundIndex.Create;
begin
  inherited;
  FSearchLow := true;
end;

{ TUpperBoundIndex }

constructor TUpperBoundIndex.Create;
begin
  inherited;
  FSearchLow := false;
end;

end.
