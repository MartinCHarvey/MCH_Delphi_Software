unit DBPersist;
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

uses Trackables, DataObjects, CommonPool, SyncObjs, Classes, SysUtils,
  DBGeneric, GlobalLog;

type
  TDBMPersist = class;

  TVarBlockArray = record
    case llTag: TKListLevel of
      klUserList:
        (UBlocks: TKUserBlockArray);
      klMediaList:
        (MBlocks: TKMediaBlockArray);
      klCommentList:
        (CBlocks: TKCommentBlockArray);
  end;

  TDBPersistWorkItem = class(TCommonPoolWorkItem)
  private
    FDBPersist: TDBMPersist;
  protected
    function VarBlockArrayAssigned(Blocks: TVarBlockArray): boolean;
  protected
    procedure DoWorkItemPersister;
    function DoWork: integer; override;
  public
    constructor Create;
    property DBPersist: TDBMPersist read FDBPersist;
  end;

  TDBPersistState = (stIdle, stRunning, stDeleting);

  TDBCallParams = class;
  TDBCallResult = class;

  TDBMPersist = class(TDBPersist)
  private
    FCommonPoolClientRec: TClientRec;
    FPreDeleteDone: boolean;
    FLock: TCriticalSection;
    FState: TDBPersistState;
    FDeleteWait: TEvent;
    FDBOpOngoing: boolean;

    FCallParams: TDBCallParams;
    FCallResult: TDBCallResult;

    function StartDeleteProcess: boolean; // Destructor has started,
    procedure WaitDeleteReady;
  protected
    property CallParams: TDBCallParams read FCallParams;
    property CallResult: TDBCallResult read FCallResult;

    function AttemptStartProcessing: boolean; // Idle, can start working on behalf of client.
    procedure FinishedProcessing(WorkItem: TDBPersistWorkItem);
    function SpawnWorkItem: boolean;
  public
    constructor Create; override;
    destructor Destroy; override;
    function Stop: boolean; override; // returns whether idle.
    function RetrieveResult(var Res: TDBCallResult): boolean; virtual;
    function RetrieveParams(var Params: TDBCallParams): boolean; virtual;
  end;

  TDBCallSubType = (dbGetSingle, dbGetList, dbChangeSingle, dbChangeList,
    dbChangeListForUser, dbDoNop, dbCustomQuery, dbExpireList, dbPruneUnused, dbGetTree);
  TDBCallSubTypeStrings = array [TDBCallSubType] of string;

{$IFDEF USE_TRACKABLES}

  TDBMarshalledCall = class(TTrackable)
{$ELSE}
  TDBMarshalledCall = class
{$ENDIF}
  protected
    procedure Clear; virtual;
    // Assign is a bit cheeky in some cases,
    // and transfers ownership of objects...
    procedure Assign(Src: TDBMarshalledCall); virtual;
  public
    CallType: TKListLevel;
    CallSubType: TDBCallSubType;
    procedure LogInfo(Sev: TLogSeverity); virtual;
    destructor Destroy; override;
  end;

  TDBMarshalCommon = class(TDBMarshalledCall)
  protected
    procedure Clear; override;
    // Assign is a bit cheeky in some cases,
    // and transfers ownership of objects...
    procedure Assign(Src: TDBMarshalledCall); override;

    procedure LogKeyed(Sev: TLogSeverity; Keyed: TKKeyedObject);
    procedure LogList(Sev: TLogSeverity; List: TKItemList);
    procedure LogUBlock(Sev: TLogSeverity; UBlock: TKSiteUserBlock);
    procedure LogMBlock(Sev: TLogSeverity; MBlock: TKSiteMediaBlock);
    procedure LogCBlock(Sev: TLogSeverity; CBlock: TKSiteCommentBlock);
  public
    Item: TKKeyedObject;
    List: TKItemList;
    procedure LogInfo(Sev: TLogSeverity); override;
  end;

  TDBCallParams = class(TDBMarshalCommon)
  protected
    procedure Clear; override;
    // Assign is a bit cheeky in some cases,
    // and transfers ownership of objects...
    procedure Assign(Src: TDBMarshalledCall); override;
  public
    // Shared call params
    ItemGUID: TGuid;
    ActionSet: TPersistActionSet;
    BlockArray: TVarBlockArray;
    OtherLevel: TKListLevel;
    KeyMeaning: TCustomKeyMeaning;
    Filter: TAdditionalFilter;
    ExpireBefore: TDateTime;
    ExpiryType: TDBExpiryType;
    procedure LogInfo(Sev: TLogSeverity); override;
  end;

  TDBCallResult = class(TDBMarshalCommon)
  private
  protected
    procedure Clear; override;
    procedure Assign(Src: TDBMarshalledCall); override;
  public
    // Shared call results
    OK: boolean;
    Msg: string;
    procedure LogInfo(Sev: TLogSeverity); override;
  end;

  TItemPersister = class(TDBMPersist)
  private
  protected
    procedure ObjectTypeCheck(Obj: TKDataObject);
    function GetObjectClass: TKDataClass;
    property ObjectClass: TKDataClass read GetObjectClass;
    function GetListClass: TKDataClass;
    function GetOtherListClass(Level: TKListLevel): TKDataClass;
    property ListClass: TKDataClass read GetListClass;

    //Added Ctx fields (session / transaction / whatever).
    //Optional, not used by all DB implementations.
    //Initially NIL for first  call.
    function GetSingleItemMTB(Ctx: TObject; Blocks: TVarBlockArray; var Item: TKKeyedObject): boolean;
      virtual; abstract;
    function GetSingleItemMTG(Ctx: TObject; const ItemKey: TGuid; var Item: TKKeyedObject): boolean; virtual; abstract;
    function GetItemListForParentMT(Ctx: TObject; const ParentKey: TGuid; var ItemList: TKItemList): boolean;
      virtual; abstract;

    function GetItemTreeMTB(Ctx: TObject; Blocks: TVarBlockArray; var Item: TKKeyedObject): boolean; virtual; abstract;
    function GetItemTreeMTG(Ctx: TObject; const ItemKey: TGuid; var Item: TKKeyedObject): boolean; virtual; abstract;

    function ChangeSingleItemMT(Ctx: TObject; const OwnerKey: TGuid; Item: TKKeyedObject; ActionSet: TPersistActionSet)
      : boolean; virtual; abstract;
    function ChangeItemListMT(Ctx: TObject; const ParentKey: TGuid; ItemList: TKItemList; ActionSet: TPersistActionSet)
      : boolean; virtual; abstract;
    function ChangeItemListForOtherLevelMT(Ctx: TObject; const ParentKey: TGuid; ParentLevel: TKListLevel;
      ItemList: TKItemList; ActionSet: TPersistActionSet): boolean; virtual; abstract;

    function ExpireItemListMT(Ctx: TObject; const ParentKey: TGuid; ExpireBefore: TDateTime;
      ExpiryType: TDBExpiryType): boolean; virtual; abstract;

    function PruneUnusedMT(Ctx: TObject): boolean; virtual; abstract;

    function DoNopMT: boolean;

    function CustomQueryMT(Ctx: TObject; const Key: TGuid; KeyMeaning: TCustomKeyMeaning;
      AdditionalFilter: TAdditionalFilter; var TopLevelList: TKItemList): boolean; virtual; abstract;

    function GetListLevel: TKListLevel; virtual; abstract;
    procedure PersisterBlocksFromKeyedObject(Item: TKKeyedObject; var Blocks: TVarBlockArray);

    function CheckBlockArray(Blocks: TVarBlockArray): boolean;
    procedure CloneBlockArray(Src: TVarBlockArray; var Dst: TVarBlockArray);
  public
    // These calls do not take ownership of the supplied datastructure,
    // and should clone it if they wish to access state outside the initial call.
    function GetSingleItemB(Blocks: TVarBlockArray): boolean;
    function GetSingleItemG(const ItemKey: TGuid): boolean;
    function GetItemListForParent(const ParentKey: TGuid): boolean;

    function GetItemTreeB(Blocks: TVarBlockArray): boolean;
    function GetItemTreeG(const ItemKey: TGuid): boolean;

    function CustomQuery(const Key: TGuid; KeyMeaning: TCustomKeyMeaning;
      AdditionalFilter: TAdditionalFilter): boolean;

    function ChangeSingleItem(const OwnerKey: TGuid; Item: TKKeyedObject;
      ActionSet: TPersistActionSet): boolean;
    function ChangeItemList(const ParentKey: TGuid; ItemList: TKItemList;
      ActionSet: TPersistActionSet): boolean;

    function ChangeItemListForOtherLevel(const ParentKey: TGuid; ParentLevel: TKListLevel;
      ItemList: TKItemList; ActionSet: TPersistActionSet): boolean;
    function DoNop: boolean;

    function ExpireItemList(const ParentKey: TGuid; ExpireBefore: TDateTime;
      ExpiryType: TDBExpiryType): boolean;

    function PruneUnused: boolean;

    function RetrieveResult(var Res: TDBCallResult): boolean; override;
    function RetrieveParams(var Params: TDBCallParams): boolean; override;

    property ListLevel: TKListLevel read GetListLevel;
  end;

function BlocksFromKeyedObject(Item: TKKeyedObject): TVarBlockArray;

const
  DBCallSubTypeStrings: TDBCallSubTypeStrings = ('dbGetSingle', 'dbGetList', 'dbChangeSingle',
    'dbChangeList', 'dbChangeListForUser', 'dbDoNop', 'dbCustomQuery', 'dbExpireList',
    'dbPruneUnused', 'dbGetTree');

implementation

uses
  IndexedStore;

const
  S_EXCEPTION = 'Unhandled exception in DB operation: ';
  S_BAD_CALL_TYPE = 'Bad call type marshalling arguments between threads';
  S_BAD_CALL_SUBTYPE = 'Bad call subtype marshalling arguments between threads';

{$IFDEF DEBUG_DB_PERSIST}

var
  CallsOutstanding: integer;
{$ENDIF}

function BlocksFromKeyedObject(Item: TKKeyedObject): TVarBlockArray;
begin
  FillChar(result, sizeof(result), 0);
  if Item is TKUserProfile then
  begin
    result.llTag := klUserList;
    Assert(Item is GetItemClass(result.llTag));
    result.UBlocks := (Item as TKUserProfile).SiteUserBlocks;
  end
  else if Item is TKMediaItem then
  begin
    result.llTag := klMediaList;
    Assert(Item is GetItemClass(result.llTag));
    result.MBlocks := (Item as TKMediaItem).SiteMediaBlocks;
  end
  else if Item is TKCommentItem then
  begin
    result.llTag := klCommentList;
    Assert(Item is GetItemClass(result.llTag));
    result.CBlocks := (Item as TKCommentItem).SiteCommentBlocks;
  end
  else
    Assert(false);
end;

{ TDBPersistWorkItem }

constructor TDBPersistWorkItem.Create;
begin
  inherited;
  CanAutoFree := true;
end;

function TDBPersistWorkItem.DoWork: integer;
begin
  Assert(Assigned(FDBPersist));
  Assert(Assigned(FDBPersist.CallParams));
  Assert(Assigned(FDBPersist.CallResult));
  try
    try
      with FDBPersist do
      begin
        CallResult.Clear;
        CallResult.CallType := CallParams.CallType;
        CallResult.CallSubType := CallParams.CallSubType;
        CallResult.Msg := '';
        CallResult.OK := false;
      end;
      DoWorkItemPersister;
{$IFDEF DEBUG_DB_PERSIST}
      with FDBPersist do
      begin
        if not CallResult.OK then
        begin
          GLogLog(SV_WARN, 'DATABASE call failed');
          GLogLog(SV_WARN, 'CallParams:');
          CallParams.LogInfo(SV_WARN);
          GLogLog(SV_WARN, 'CallResult:');
          CallResult.LogInfo(SV_WARN);
        end;
      end;
{$ENDIF}
    except
      on E: Exception do
      begin
        FDBPersist.CallResult.OK := false;
        FDBPersist.CallResult.Msg := S_EXCEPTION + E.Message;
        GLogLog(SV_WARN, FDBPersist.CallResult.Msg);
      end;
    end;
  finally
    FDBPersist.FinishedProcessing(self);
    result := 0;
  end;
end;

procedure TDBPersistWorkItem.DoWorkItemPersister;
begin
  with FDBPersist as TItemPersister do
  begin
    if CallParams.CallType = ListLevel then
    begin
      case CallParams.CallSubType of
        dbGetSingle:
          if VarBlockArrayAssigned(CallParams.BlockArray) then
            CallResult.OK := GetSingleItemMTB(nil, CallParams.BlockArray, CallResult.Item)
          else
            CallResult.OK := GetSingleItemMTG(nil, CallParams.ItemGUID, CallResult.Item);
        dbGetList:
          CallResult.OK := GetItemListForParentMT(nil, CallParams.ItemGUID, CallResult.List);
        dbChangeSingle:
          CallResult.OK := ChangeSingleItemMT(nil, CallParams.ItemGUID, CallParams.Item,
            CallParams.ActionSet);
        dbChangeList:
          CallResult.OK := ChangeItemListMT(nil, CallParams.ItemGUID, CallParams.List,
            CallParams.ActionSet);
        dbDoNop:
          CallResult.OK := DoNopMT;
        dbChangeListForUser:
          CallResult.OK := ChangeItemListForOtherLevelMT(nil, CallParams.ItemGUID, CallParams.OtherLevel,
            CallParams.List, CallParams.ActionSet);
        dbCustomQuery:
          CallResult.OK := CustomQueryMT(nil, CallParams.ItemGUID, CallParams.KeyMeaning, CallParams.Filter, CallResult.List);
        dbExpireList:
          CallResult.OK := ExpireItemListMT(nil, CallParams.ItemGUID, CallParams.ExpireBefore, CallParams.ExpiryType);
        dbPruneUnused:
          CallResult.OK := PruneUnusedMT(nil);
        dbGetTree:
          if VarBlockArrayAssigned(CallParams.BlockArray) then
            CallResult.OK := GetItemTreeMTB(nil, CallParams.BlockArray, CallResult.Item)
          else
            CallResult.OK := GetItemTreeMTG(nil, CallParams.ItemGUID, CallResult.Item);
      else
        raise EDBPersistError.Create(S_BAD_CALL_SUBTYPE);
      end;
    end
    else
      raise EDBPersistError.Create(S_BAD_CALL_TYPE);
  end;
end;

function TDBPersistWorkItem.VarBlockArrayAssigned(Blocks: TVarBlockArray): boolean;
var
  ST: TKSiteType;
begin
  result := false;
  for ST := Low(ST) to High(ST) do
  begin
    case Blocks.llTag of
      klUserList:
        result := Assigned(Blocks.UBlocks[ST]);
      klMediaList:
        result := Assigned(Blocks.MBlocks[ST]);
      klCommentList:
        result := Assigned(Blocks.CBlocks[ST]);
    else
      Assert(false);
    end;
    if result then
      break;
  end;
end;

{ TDBMPersist }

constructor TDBMPersist.Create;
begin
  inherited;
  FDeleteWait := TEvent.Create(nil, true, false, '');
  FLock := TCriticalSection.Create;
  FCallParams := TDBCallParams.Create;
  FCallResult := TDBCallResult.Create;
  FCommonPoolClientRec := GCommonPool.RegisterClient(self, nil, nil);
end;

function TDBMPersist.Stop: boolean;
begin
  result := not StartDeleteProcess;
  FPreDeleteDone := true;
end;

destructor TDBMPersist.Destroy;
begin
  if not FPreDeleteDone then
    Stop;
  WaitDeleteReady;
  GCommonPool.DeRegisterClient(FCommonPoolClientRec);
  FDeleteWait.Free;
  FCallParams.Free;
  FCallResult.Free;
  FLock.Free;
  inherited;
end;

function TDBMPersist.AttemptStartProcessing: boolean;
// Idle, can start working on behalf of client.
begin
  FLock.Acquire;
  try
    result := (FState = stIdle);
    if result then
    begin
      FState := stRunning;
      FDBOpOngoing := true;
    end;
  finally
    FLock.Release;
  end;
  if result then
  begin
    FCallParams.Clear;
    FCallResult.Clear;
  end;
end;

procedure TDBMPersist.FinishedProcessing(WorkItem: TDBPersistWorkItem);
var
  RunCompletion: boolean; // Shouldn't really try to delete when ops outstanding...
{$IFDEF DEBUG_DB_PERSIST}
  Tmp: integer;
{$ENDIF}
begin
  RunCompletion := true;
  FLock.Acquire;
  try
    Assert(FState <> stIdle);
    // Shouldn't call finish twice, only call if nothing in progress.
    if FState = stRunning then
      FState := stIdle
    else if FState = stDeleting then
    begin
      FDeleteWait.SetEvent;
      RunCompletion := false;
    end;
    FDBOpOngoing := false;
  finally
    FLock.Release;
  end;
  if RunCompletion then
  begin
    if Assigned(WorkItem) then
    begin
  {$IFDEF DEBUG_DB_PERSIST}
      Tmp := TInterlocked.Decrement(CallsOutstanding);
      GLogLog(SV_INFO, 'DB Persist, workitem deleted, outstanding ops: ' + IntToStr(Tmp));
  {$ENDIF}
      WorkItem.ThreadRec.Thread.Queue(DoRequestCompleted);
    end
    else
    begin
  {$IFDEF DEBUG_DB_PERSIST}
      Tmp := TInterlocked.Decrement(CallsOutstanding);
      DoRequestCompleted;
      GLogLog(SV_WARN, 'DB Persist, complete in Gui thread, outstanding ops: ' +
        IntToStr(CallsOutstanding));
  {$ENDIF}
    end;
  end
  else
  begin
{$IFDEF DEBUG_DB_PERSIST}
    GLogLog(SV_WARN, 'DB Persist, workitem not completed, outstanding ops: ' +
      IntToStr(CallsOutstanding));
{$ENDIF}
  end;
end;

function TDBMPersist.SpawnWorkItem: boolean;
var
  WorkItem: TDBPersistWorkItem;
{$IFDEF DEBUG_DB_PERSIST}
  Tmp: integer;
{$ENDIF}
begin
  WorkItem := TDBPersistWorkItem.Create;
  WorkItem.FDBPersist := self;
  result := GCommonPool.AddWorkItem(FCommonPoolClientRec, WorkItem);
  if not result then
  begin
    WorkItem.Free;
{$IFDEF DEBUG_DB_PERSIST}
    GLogLog(SV_WARN, 'DB Persist, workitem not added, outstanding ops: ' +
      IntToStr(CallsOutstanding));
{$ENDIF}
  end
  else
  begin
{$IFDEF DEBUG_DB_PERSIST}
    Tmp := TInterlocked.Increment(CallsOutstanding);
    GLogLog(SV_INFO, 'DB Persist, workitem added, outstanding ops: ' + IntToStr(Tmp));
{$ENDIF}
  end;
end;

function TDBMPersist.RetrieveResult(var Res: TDBCallResult): boolean;
begin
  Res := nil;
  FLock.Acquire;
  try
    result := FState = stIdle;
    if result then
    begin
      Res := TDBCallResult.Create;
      Res.Assign(CallResult);
      CallResult.Clear;
    end;
  finally
    FLock.Release;
  end;
end;

function TDBMPersist.RetrieveParams(var Params: TDBCallParams): boolean;
begin
  Params := nil;
  FLock.Acquire;
  try
    result := FState = stIdle;
    if result then
    begin
      Params := TDBCallParams.Create;
      Params.Assign(CallParams);
      CallParams.Clear;
    end;
  finally
    FLock.Release;
  end;
end;

function TDBMPersist.StartDeleteProcess: boolean;
begin
  FLock.Acquire;
  try
    result := FDBOpOngoing;
    if FState = stIdle then
      FDeleteWait.SetEvent
    else if FState = stRunning then
      FDeleteWait.ResetEvent;

    FState := stDeleting;
  finally
    FLock.Release;
  end;
end;

procedure TDBMPersist.WaitDeleteReady;
begin
  FLock.Acquire;
  try
    Assert(FState = stDeleting);
  finally
    FLock.Release;
  end;
  FDeleteWait.WaitFor(INFINITE);
end;

{ TDBMarshalledCall }

destructor TDBMarshalledCall.Destroy;
begin
  Clear;
  inherited;
end;

procedure TDBMarshalledCall.Clear;
begin
end;

procedure TDBMarshalledCall.LogInfo(Sev: TLogSeverity);
begin
  GLogLog(Sev, '  Call type:' + ListLevelStrings[CallType]);
  GLogLog(Sev, '  Call subtype:' + DBCallSubTypeStrings[CallSubType]);
end;

procedure TDBMarshalledCall.Assign(Src: TDBMarshalledCall);
begin
  CallType := Src.CallType;
  CallSubType := Src.CallSubType;
end;

{ TDBMarshalCommon }

procedure TDBMarshalCommon.LogUBlock(Sev: TLogSeverity; UBlock: TKSiteUserBlock);
begin
  if not Assigned(UBlock) or not UBlock.Valid then
    GLogLog(Sev, '  <not valid>')
  else
  begin
    GLogLog(Sev, '  UID: ' + UBlock.UserId);
    GLogLog(Sev, '  Uname: ' + UBlock.Username);
  end;
end;

procedure TDBMarshalCommon.LogMBlock(Sev: TLogSeverity; MBlock: TKSiteMediaBlock);
begin
  if not Assigned(MBlock) or not MBlock.Valid then
    GLogLog(Sev, '  <not valid>')
  else
  begin
    GLogLog(Sev, '  MediaID: ' + MBlock.MediaID);
    GLogLog(Sev, '  MediaOwnerID: ' + MBlock.OwnerID);
    GLogLog(Sev, '  MediaCode: ' + MBlock.MediaCode);
  end;
end;

procedure TDBMarshalCommon.LogCBlock(Sev: TLogSeverity; CBlock: TKSiteCommentBlock);
begin
  if not Assigned(CBlock) or not CBlock.Valid then
    GLogLog(Sev, '  <not valid>')
  else
  begin
    GLogLog(Sev, '  CommentID: ' + CBlock.CommentId);
  end;
end;

procedure TDBMarshalCommon.LogKeyed(Sev: TLogSeverity; Keyed: TKKeyedObject);
var
  ST: TKSiteType;
begin
  if not Assigned(Keyed) then
    GLogLog(Sev, '  nil')
  else
  begin
    GLogLog(Sev, '**********');
    GLogLog(Sev, '  Key:' + GuidToString(Keyed.Key));
    for ST := Low(ST) to High(ST) do
    begin
      GLogLog(Sev, 'Blocks for site type: ' + SiteTypeStrings[ST]);
      if Keyed is TKUserProfile then
        LogUBlock(Sev, (Keyed as TKUserProfile).SiteUserBlock[ST])
      else if Keyed is TKMediaItem then
        LogMBlock(Sev, (Keyed as TKMediaItem).SiteMediaBlock[ST])
      else if Keyed is TKCommentItem then
        LogCBlock(Sev, (Keyed as TKCommentItem).SiteCommentBlock[ST]);
    end;
    GLogLog(Sev, '**********');
  end;
end;

procedure TDBMarshalCommon.LogList(Sev: TLogSeverity; List: TKItemList);
var
  Keyed: TKKeyedObject;
begin
  if not Assigned(List) then
    GLogLog(Sev, '  nil')
  else
  begin
    GLogLog(Sev, '  ' + IntToStr(List.Count) + ' item list');
    if List is TKItemList then
      GLogLog(Sev, '  List level: ' + ListLevelStrings[(List as TKIdList).GetListLevel]);
    Keyed := List.AdjacentBySortVal(katFirst, ksvPointer, nil);
    while Assigned(Keyed) do
    begin
      GLogLog(Sev, '    ' + GuidToString(Keyed.Key));
      Keyed := List.AdjacentBySortVal(katNext, ksvPointer, Keyed);
    end;
  end;
end;

procedure TDBMarshalCommon.Assign(Src: TDBMarshalledCall);
var
  S: TDBMarshalCommon;
begin
  inherited Assign(Src);
  S := Src as TDBMarshalCommon;

  List := S.List;
  S.List := nil;
  Item := S.Item;
  S.Item := nil;
end;

procedure TDBMarshalCommon.Clear;
begin
  inherited;
  List.Free;
  List := nil;
  Item.Free;
  Item := nil;
end;

procedure TDBMarshalCommon.LogInfo(Sev: TLogSeverity);
begin
  inherited;
  GLogLog(Sev, 'Item');
  LogKeyed(Sev, Item);
  GLogLog(Sev, 'List');
  LogList(Sev, List);
end;

{ TDBCallParams }

procedure TDBCallParams.Assign(Src: TDBMarshalledCall);
var
  S: TDBCallParams;
begin
  Clear;
  inherited Assign(Src);
  S := Src as TDBCallParams;
  ItemGUID := S.ItemGUID;
  ActionSet := S.ActionSet;
  BlockArray := S.BlockArray;
  FillChar(S.BlockArray, sizeof(S.BlockArray), 0);
  S.BlockArray.llTag := BlockArray.llTag;

  OtherLevel := S.OtherLevel;
  KeyMeaning := S.KeyMeaning;
  Filter := S.Filter; S.Filter := nil;
  ExpireBefore := S.ExpireBefore;
  ExpiryType := S.ExpiryType;
end;

procedure TDBCallParams.Clear;
var
  ST: TKSiteType;
  llTag: TKListLevel;
begin
  inherited;
  // Clear the var block array.
  for ST := Low(ST) to High(ST) do
  begin
    llTag := BlockArray.llTag;
    case llTag of
      klUserList:
        BlockArray.UBlocks[ST].Free;
      klMediaList:
        BlockArray.MBlocks[ST].Free;
      klCommentList:
        BlockArray.CBlocks[ST].Free;
    else
      Assert(false);
    end;
  end;
  FillChar(BlockArray, sizeof(BlockArray), 0);
  BlockArray.llTag := llTag;
  Filter.Free;
end;

procedure TDBCallParams.LogInfo(Sev: TLogSeverity);
var
  ST: TKSiteType;
  Tmp: string;
  Act: TPersistAction;
begin
  inherited;
  Tmp := '';
  for Act := Low(Act) to High(Act) do
  begin
    if Act in ActionSet then
      Tmp := Tmp + PersistActionStrs[Act] + ', '
  end;
  GLogLog(Sev, 'ActionSet: ' + Tmp);
  GLogLog(Sev, 'Item GUID: ' + GuidToString(ItemGUID));
  GLogLog(Sev, 'BlockArray list level: ' + ListLevelStrings[BlockArray.llTag]);
  for ST := Low(ST) to High(ST) do
  begin
    GLogLog(Sev, 'Block Site type: ' + SiteTypeStrings[ST]);
    case BlockArray.llTag of
      klUserList:
        begin
          GLogLog(Sev, 'UserBlock');
          LogUBlock(Sev, BlockArray.UBlocks[ST]);
        end;
      klMediaList:
        begin
          GLogLog(Sev, 'MediaBlock');
          LogMBlock(Sev, BlockArray.MBlocks[ST]);

        end;
      klCommentList:
        begin
          GLogLog(Sev, 'CommentBlock');
          LogCBlock(Sev, BlockArray.CBlocks[ST]);
        end
    else
      Assert(false);
    end;
  end;
end;

{ TDBCallResult }

procedure TDBCallResult.Clear;
begin
  inherited;
  OK := false;
  Msg := '';
end;

procedure TDBCallResult.Assign(Src: TDBMarshalledCall);
var
  S: TDBCallResult;
begin
  Clear;
  inherited Assign(Src);
  S := Src as TDBCallResult;
  OK := S.OK;
  Msg := S.Msg;
end;

procedure TDBCallResult.LogInfo(Sev: TLogSeverity);
begin
  inherited;
  if OK then
    GLogLog(Sev, 'OK')
  else
    GLogLog(Sev, 'FAILED');
  GLogLog(Sev, Msg);
end;

{ TItemPersister }

procedure TItemPersister.ObjectTypeCheck(Obj: TKDataObject);
begin
  if Assigned(Obj) then
  begin
    if Obj is TKList then
    begin
      Assert(Obj is TKIdList);
      Assert((Obj as TKIdList).GetListLevel = ListLevel);
      Assert(Obj is GetListClass);
    end
    else if Obj is TKKeyedObject then
    begin
      Assert(Obj is GetObjectClass);
    end
    else
      Assert(false);
  end;
end;

function TItemPersister.GetObjectClass: TKDataClass;
begin
  case ListLevel of
    klUserList:
      result := TKUserProfile;
    klMediaList:
      result := TKMediaItem;
    klCommentList:
      result := TKCommentItem;
  else
    Assert(false);
    result := TKDataObject;
  end;
end;

function TItemPersister.GetOtherListClass(Level: TKListLevel): TKDataClass;
begin
  case Level of
    klUserList:
      result := TKUserList;
    klMediaList:
      result := TKMediaList;
    klCommentList:
      result := TKCommentList;
  else
    Assert(false);
    result := TKDataObject;
  end;
end;

function TItemPersister.GetListClass: TKDataClass;
begin
  result := GetOtherListClass(ListLevel);
end;

function TItemPersister.DoNopMT;
begin
  result := true;
end;

function TItemPersister.DoNop: boolean;
begin
  if AttemptStartProcessing then
  begin
    FCallParams.CallType := ListLevel;
    FCallParams.CallSubType := dbDoNop;
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

procedure TItemPersister.PersisterBlocksFromKeyedObject(Item: TKKeyedObject;
  var Blocks: TVarBlockArray);
begin
  // Not quite same as DBPersist version, becase we are sure of our list level.
  FillChar(Blocks, sizeof(Blocks), 0);
  Blocks.llTag := ListLevel;
  case ListLevel of
    klUserList:
      Blocks.UBlocks := (Item as TKUserProfile).SiteUserBlocks;
    klMediaList:
      Blocks.MBlocks := (Item as TKMediaItem).SiteMediaBlocks;
    klCommentList:
      Blocks.CBlocks := (Item as TKCommentItem).SiteCommentBlocks;
  end;
end;

function TItemPersister.CheckBlockArray(Blocks: TVarBlockArray): boolean;
begin
  result := Blocks.llTag = ListLevel;
  Assert(result);
end;

procedure TItemPersister.CloneBlockArray(Src: TVarBlockArray; var Dst: TVarBlockArray);
begin
  Dst.llTag := Src.llTag;
  case Dst.llTag of
    klUserList:
      CloneUserBlockArray(Src.UBlocks, Dst.UBlocks);
    klMediaList:
      CloneMediaBlockArray(Src.MBlocks, Dst.MBlocks);
    klCommentList:
      CloneCommentBlockArray(Src.CBlocks, Dst.CBlocks);
  else
    Assert(false);
  end;
end;

function TItemPersister.GetSingleItemB(Blocks: TVarBlockArray): boolean;
begin
  if CheckBlockArray(Blocks) and AttemptStartProcessing then
  begin
    FCallParams.CallType := ListLevel;
    FCallParams.CallSubType := dbGetSingle;
    CloneBlockArray(Blocks, FCallParams.BlockArray);
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

function TItemPersister.GetSingleItemG(const ItemKey: TGuid): boolean;
begin
  if AttemptStartProcessing then
  begin
    FCallParams.CallType := ListLevel;
    FCallParams.CallSubType := dbGetSingle;
    FCallParams.ItemGUID := ItemKey;
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

function TItemPersister.GetItemListForParent(const ParentKey: TGuid): boolean;
begin
  if AttemptStartProcessing then
  begin
    FCallParams.CallType := ListLevel;
    FCallParams.CallSubType := dbGetList;
    FCallParams.ItemGUID := ParentKey;
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

function TItemPersister.GetItemTreeB(Blocks: TVarBlockArray): boolean;
begin
  if CheckBlockArray(Blocks) and AttemptStartProcessing then
  begin
    FCallParams.CallType := ListLevel;
    FCallParams.CallSubType := dbGetTree;
    CloneBlockArray(Blocks, FCallParams.BlockArray);
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

function TItemPersister.GetItemTreeG(const ItemKey: TGuid): boolean;
begin
  if AttemptStartProcessing then
  begin
    FCallParams.CallType := ListLevel;
    FCallParams.CallSubType := dbGetTree;
    FCallParams.ItemGUID := ItemKey;
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

function TItemPersister.CustomQuery(const Key: TGuid; KeyMeaning: TCustomKeyMeaning;
  AdditionalFilter: TAdditionalFilter): boolean;
begin
  if AttemptStartProcessing then
  begin
    FCallParams.CallType := ListLevel;
    FCallParams.CallSubType := dbCustomQuery;
    FCallParams.ItemGUID := Key;
    FCallParams.Filter := AdditionalFilter;
    FCallParams.KeyMeaning := KeyMeaning;
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

function TItemPersister.ChangeSingleItem(const OwnerKey: TGuid; Item: TKKeyedObject;
  ActionSet: TPersistActionSet): boolean;
begin
  if AttemptStartProcessing then
  begin
    FCallParams.CallType := GetListLevel;
    FCallParams.CallSubType := dbChangeSingle;
    ObjectTypeCheck(Item);
    FCallParams.Item := TKDataObject.Clone(Item) as TKKeyedObject;
    FCallParams.ActionSet := ActionSet;
    FCallParams.ItemGUID := OwnerKey;
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

function TItemPersister.ChangeItemList(const ParentKey: TGuid; ItemList: TKItemList;
  ActionSet: TPersistActionSet): boolean;
begin
  if AttemptStartProcessing then
  begin
    FCallParams.CallType := GetListLevel;
    FCallParams.CallSubType := dbChangeList;
    ObjectTypeCheck(ItemList);
    FCallParams.List := TKList.CloneListAndItemsOnly(ItemList) as TKItemList;
    FCallParams.ActionSet := ActionSet;
    FCallParams.ItemGUID := ParentKey;
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

function TItemPersister.ChangeItemListForOtherLevel(const ParentKey: TGuid; ParentLevel: TKListLevel;
  ItemList: TKItemList; ActionSet: TPersistActionSet): boolean;
begin
  if AttemptStartProcessing then
  begin
    FCallParams.CallType := GetListLevel;
    FCallParams.CallSubType := dbChangeListForUser;
    ObjectTypeCheck(ItemList);
    FCallParams.List := TKList.CloneListAndItemsOnly(ItemList) as TKItemList;
    FCallParams.ActionSet := ActionSet;
    FCallParams.ItemGUID := ParentKey;
    FCallParams.OtherLevel := ParentLevel;
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

function TItemPersister.ExpireItemList(const ParentKey: TGuid; ExpireBefore: TDateTime;
  ExpiryType: TDBExpiryType): boolean;
begin
  if AttemptStartProcessing then
  begin
    FCallParams.CallType := GetListLevel;
    FCallParams.CallSubType := dbExpireList;
    FCallParams.ExpireBefore := ExpireBefore;
    FCallParams.ExpiryType := ExpiryType;
    FCallParams.ItemGUID := ParentKey;
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;

function TItemPersister.PruneUnused: boolean;
begin
  if AttemptStartProcessing then
  begin
    FCallParams.CallType := GetListLevel;
    FCallParams.CallSubType := dbPruneUnused;
    result := SpawnWorkItem;
    if not result then
      FinishedProcessing(nil);
  end
  else
    result := false;
end;


function TItemPersister.RetrieveResult(var Res: TDBCallResult): boolean;
begin
  result := inherited;
  if result then
  begin
    ObjectTypeCheck(Res.Item);
    if Res.CallSubType <> TDBCallSubType.dbCustomQuery then
      ObjectTypeCheck(Res.List);
  end;
end;

function TItemPersister.RetrieveParams(var Params: TDBCallParams): boolean;
begin
  result := inherited;
  if result then
  begin
    ObjectTypeCheck(Params.Item);
    ObjectTypeCheck(Params.List);
  end;
end;

end.
