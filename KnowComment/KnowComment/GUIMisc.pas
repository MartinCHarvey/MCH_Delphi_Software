unit GUIMisc;
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
  FMX.ListView, Classes, DataObjects, FMX.StdCtrls,
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  System.Generics.Collections
  ;

type
  TListViewSizeIndex = 0 .. 5;
  TListViewSizes = array [TListViewSizeIndex] of integer;
  TListViewSizeTexts = array [TListViewSizeIndex] of string;

const
  LVSizes: TListViewSizes = (128,
                             1024,
                             10 * 1024,
                             100* 1024,
                             1024 * 1024,
                             High(integer));
  LVTexts: TListViewSizeTexts = ('next thousand',
                                 'next ten thousand',
                                 'next hundred thousand',
                                 'next million',
                                 'all items',
                                 '');

type
  TFetchUserListEvent = procedure(Sender: TObject; var UL: TKUserList) of object;
  TPutUserListEvent = procedure(Sender: TObject; UL: TKUserList) of object;
  TSelectionUpdatedEvent = procedure(Sender: TObject;
                                     const OldSelectedKey, NewSelectedKey: TGuid;
                                     Force: boolean) of object;

{$IFDEF USE_TRACKABLES}
  TItemListViewHelper = class(TTrackable)
{$ELSE}
  TItemListViewHelper = class
{$ENDIF}
  protected
    FListView: TListView;
    FLoadMoreButton: TButton;
    FPendingImageRequests: TList;
    FKeyList: TList<TGuid>;
    FSelectedItemKey, FSelectingItemKey: TGuid;
    FListSizeIndex: TListViewSizeIndex;
    FOnFetchUserList: TFetchUserListEvent;
    FOnPutUserList: TPutUserListEvent;
    FOnSelectionUpdated: TSelectionUpdatedEvent;
    procedure SetListView(NewView: TListView);
    procedure SetLoadMoreButton(NewButton: TButton);
    procedure SetSelectedItemKey(const NewKey: TGuid);
    procedure AddPendingImageRequest(const ItemKey: TGuid; ImageUrl: string);
    procedure ClearPendingImageRequests;
    procedure RequestImage(URL: string; const Ref2: TGuid);
    procedure RefreshItem(ListLevel: TKListLevel; Item: TListViewItem; KeyedObj: TKKeyedObject; ReloadIcon: boolean);
    procedure ReconcileLists(List: TKIdList; KeyedObj: TKKeyedObject);
    procedure UpdateLoadMoreBtn(List: TKItemList);
  public
    constructor Create;
    destructor Destroy; override;
    procedure ReloadList(List: TKIdList);
    //TODO - For the moment, simple username filtering only.
    procedure ReloadWithFilter(List: TKIdList; FilterStr: string);
    procedure DelayedImageLoad(Sender: TObject; var Done: boolean);
    procedure HandleViewChange(Sender: TObject);
    procedure UpdateSelection(Sender: TObject; Force: boolean);
    procedure HandleLoadMoreClicked(Sender: TObject);
    procedure HandleImageLoaded(Sender: TObject; var Data: TStream; Ref1: TObject; Ref2: string; OK: boolean);
    property ListView: TListView read FListView write SetListView;
    property LoadMoreBtn: TButton read FLoadMoreButton write SetLoadMoreButton;
    property KeyList: TList<TGuid> read FKeyList;
    property SelectedItemKey:TGuid read FSelectedItemKey write SetSelectedItemKey;
    property SelectingItemKey: TGuid read FSelectingItemKey;
    property OnFetchUserList: TFetchUserListEvent read FOnFetchUserList write FOnFetchUserList;
    property OnPutUserList: TPutUserListEvent read FOnPutUserList write FOnPutUserList;
    property OnSelectionUpdated: TSelectionUpdatedEvent read FOnSelectionUpdated write FOnSelectionUpdated;
    property ListSizeIndex: TListViewSizeIndex read FListSizeIndex write FListSizeIndex;
  end;

const
  S_LINK = '<link>';
  S_LOAD = 'Load ';

procedure ImageLoadFailed(Guid: string; ListLevel:TKListLevel; WorkingList: TKIdList);

implementation

uses
  ImageCache, IndexedStore, SysUtils, System.UITypes, MemDBMisc, DBContainer;

type
{$IFDEF USE_TRACKABLES}
  TPendingImageRequest = class(TTrackable)
{$ELSE}
  TPendingImageRequest = class
{$ENDIF}
    View: TListView;
    ItemKey: TGuid;
    ImageUrl: string;
  end;

constructor TItemListViewHelper.Create;
begin
  inherited;
  FKeyList := TList<TGuid>.Create;
  FPendingImageRequests := TList.Create;
end;

destructor TItemListViewHelper.Destroy;
begin
  ClearPendingImageRequests;
  FListView := nil;
  FKeyList.Free;
  FSelectedItemKey := TGuid.Empty;
  FPendingImageRequests.Free;
  inherited;
end;

procedure TItemListViewHelper.SetListView(NewView: TListView);
begin
  Assert(not Assigned(FListView));
  Assert(Assigned(NewView));
  FListView := NewView;
end;

procedure TItemListVIewHelper.SetLoadMoreButton(NewButton: TButton);
begin
  Assert(not Assigned(FLoadMoreButton));
  Assert(Assigned(NewButton));
  FLoadMoreButton := NewButton;
end;

procedure TItemListViewHelper.AddPendingImageRequest(const ItemKey: TGuid; ImageUrl: string);
var
  NewReq: TPendingImageRequest;
begin
  NewReq := TPendingImageRequest.Create;
  NewReq.ItemKey := ItemKey;
  NewReq.ImageUrl := ImageUrl;
  NewReq.View := FListView;
  FPendingImageRequests.Add(NewReq);
end;

procedure TItemListViewHelper.ClearPendingImageRequests;
var
  Idx: integer;
begin
  for Idx := 0 to Pred(FPendingImageRequests.Count) do
    TObject(FPendingImageRequests.Items[Idx]).Free;
  FPendingImageRequests.Count := 0;
end;

procedure TItemListViewHelper.RequestImage(URL: string; const Ref2: TGuid);
begin
  ImgCache.RequestImage(URL, FListView, GuidToString(Ref2));
end;

//List reconciliation internal types and functions for the ReconcileLists function.
type
{$IFDEF USE_TRACKABLES}
  TItemReconcile = class(TTrackable)
{$ELSE}
  TItemReconcile = class
{$ENDIF}
    GuidKey: TGuid;
    InitialIndex: integer;
    FinalIndex: integer;
    constructor Create;
  end;

  TReconcileINode = class(TDuplicateValIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  end;

  TReconcileSearchVal= class(TReconcileINode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  public
    GuidSearchVal: TGuid;
    IntSearchVal: Integer;
  end;

  TReconcileTag = (rtGuidKey, rtInitialIndex, rtFinalIndex);

constructor TItemReconcile.Create;
begin
  inherited;
  InitialIndex := -1;
  FinalIndex := -1;
end;

function TReconcileINode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  OtherReconcile, OwnReconcile: TItemReconcile;
begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  OtherReconcile := OtherItem as TItemReconcile;
  OwnReconcile := OwnItem as TItemReconcile;
  case IndexTag of
    Ord(rtGuidKey): result := CompareGuids(OtherReconcile.GuidKey, OwnReconcile.GuidKey);
    Ord(rtInitialIndex): result := OtherReconcile.InitialIndex - OwnReconcile.InitialIndex;
    Ord(rtFinalIndex): result := OtherReconcile.FinalIndex - OwnReconcile.FinalIndex;
  else
    Assert(false);
    result := ComparePointers(OwnItem, OtherItem);
  end;
end;

function TReconcileSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  OtherReconcile: TItemReconcile;
begin
  Assert(not Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  OtherReconcile := OtherItem as TItemReconcile;
  case IndexTag of
    Ord(rtGuidKey): result := CompareGuids(OtherReconcile.GuidKey, GuidSearchVal);
    Ord(rtInitialIndex): result := OtherReconcile.InitialIndex - IntSearchVal;
    Ord(rtFinalIndex): result := OtherReconcile.FinalIndex - IntSearchVal;
  else
    Assert(false);
    result := ComparePointers(self, OtherItem);
  end;
end;

procedure TItemListViewHelper.RefreshItem(ListLevel: TkListLevel; Item: TListViewItem; KeyedObj: TKKeyedObject; ReloadIcon: boolean);
var
  UL: TKUserList;
  U: TKUserProfile;
  M: TKMediaItem;
  C: TKCommentItem;
  ST: TKSiteType;
  UB: TKSiteUSerBlock;
  Txt: string;
begin
  Item.Checked := false;
  case ListLevel of
    klUserList:
      begin
        U := KeyedObj as TKUserProfile;
        // TODO - Just going for the first one at the moment.
        for ST := Low(ST) to High(ST) do
        begin
          if Assigned(U.SiteUserBlocks[ST]) and U.SiteUserBlocks[ST].Valid then
          begin
            UB := U.SiteUserBlocks[ST];
            Txt := UB.Username;
            if (U.InterestLevel > kpiFetchUserForRefs) or ProfileV(U) then
            begin
              Txt := Txt + ' (';
              if ProfileV(U) then
                Txt := Txt + 'V';
              if (U.InterestLevel > kpiFetchUserForRefs) then
              begin
                if ProfileV(U) then
                  Txt := Txt + ', ';
                Txt := Txt + 'I';
              end;
              Txt := Txt + ')';
            end;
            Item.Text := Txt;

            Item.Detail := UB.FullName + ' ' + UB.Bio;
            if ReloadIcon then
            begin
              if Length(UB.ProfilePicUrl) > 0 then
                AddPendingImageRequest(U.Key, UB.ProfilePicUrl);
            end;

            Item.Objects.AccessoryObject.Visible := U.InterestLevel >
              kpiFetchUserForRefs;
            Item.Checked := UB.Verified;
            break;
          end;
        end;
      end;
    klMediaList:
      begin
        M := KeyedObj as TKMediaItem;
        // TODO - Handling of media data correctly
        // Currently have HTML and metalinks.
        case M.MediaType of
          mitPlainText:
            Item.Text := M.MediaData;
          mitHTML:
            Item.Text := M.MediaData; // TODO - Richtextify it?
          mitMetaLink:
            Item.Text := S_LINK;
        end;
        Item.Objects.AccessoryObject.Visible := (M.Comments.Count > 0);
        if ReloadIcon then
        begin
          if Length(M.ResourceURL) > 0 then
            AddPendingImageRequest(M.Key, M.ResourceURL);
        end;

      end;
    klCommentList:
      begin
        C := KeyedObj as TKCommentItem;
        Item.Text := C.CommentData;
        Item.Objects.AccessoryObject.Visible := false;
        OnFetchUserList(Self, UL);
{ TODO - Remove commented once impl in client form
        if not Assigned(FFixedList) then
          UL := DBCont.DataStore.GetTemporaryTopLevelList
        else
          UL := FFixedList;
}
        try
          U := UL.SearchByInternalKeyOnly(C.OwnerKey) as TKUserProfile;
          if Assigned(U) then // We certainly hope so!
          begin
            for ST := Low(ST) to High(ST) do
            begin
              if Assigned(U.SiteUserBlocks[ST]) and U.SiteUserBlocks[ST].Valid
              then
              begin
                UB := U.SiteUserBlocks[ST];
                Item.Text := UB.Username + ': ' + Item.Text;
                if ReloadIcon then
                begin
                  if Length(UB.ProfilePicUrl) > 0 then
                    AddPendingImageRequest(C.Key, UB.ProfilePicUrl);
                end;
                break;
              end;
            end;
          end;
          Item.Objects.AccessoryObject.Visible := false;
        finally
          OnPutUserList(Self, UL);
{ TODO - Remove once impl in client form
          if not Assigned(FFixedList) then
            DBCont.DataStore.PutTopLevelList(UL);
}
        end;
      end;
  end;
end;

//This may not be the best way to reload / reconcile lists, it's just a method
//that isn't awful.
procedure TItemListViewHelper.ReconcileLists(List: TKIdList; KeyedObj: TKKeyedObject);

var
  ReconcileList: TIndexedStore;
  SearchVal: TReconcileSearchVal;
{$IFOPT C+}
  InitialKeyedObj: TKKeyedObject;
{$ENDIF}
  InitialIterator, FinalIterator: TItemRec;
  InitialReconcile, FinalReconcile: TItemReconcile;
  InitialInOtherIndex, FinalInOtherIndex: integer;
  LVItem: TListViewItem;
  RecTag: TReconcileTag;
  Item: TObject;
  IRec: TItemRec;
  Idx: integer;
  RV: TISRetVal;

  procedure InitInitial;
  begin
    ReconcileList.FirstByIndex(Ord(rtInitialIndex), InitialIterator);
    if Assigned(InitialIterator) then
      InitialReconcile := InitialIterator.Item as TItemReconcile
    else
      InitialReconcile := nil;
    while Assigned(InitialReconcile) and (InitialReconcile.InitialIndex < 0) do
    begin
      ReconcileList.NextByIndex(Ord(rtInitialIndex), InitialIterator);
      if Assigned(InitialIterator) then
        InitialReconcile := InitialIterator.Item as TItemReconcile
      else
        InitialReconcile := nil;
    end;
  end;

  procedure InitFinal;
  begin
    ReconcileList.FirstByIndex(Ord(rtFinalIndex), FinalIterator);
    if Assigned(FinalIterator) then
      FinalReconcile := FinalIterator.Item as TItemReconcile
    else
      FinalReconcile := nil;
    while Assigned(FinalReconcile) and (FinalReconcile.FinalIndex < 0) do
    begin
      ReconcileList.NextByIndex(Ord(rtFinalIndex), FinalIterator);
      if Assigned(FinalIterator) then
        FinalReconcile := FinalIterator.Item as TItemReconcile
      else
        FinalReconcile := nil;
    end;
  end;

  procedure IncInitial;
  begin
    //Inc initial iterator.
    ReconcileList.NextByIndex(Ord(rtInitialIndex), InitialIterator);
    if Assigned(InitialIterator) then
      InitialReconcile := InitialIterator.Item as TItemReconcile
    else
      InitialReconcile := nil;
  end;

  procedure IncFinal;
  begin
    //Inc final iterator.
    ReconcileList.NextByIndex(Ord(rtFinalIndex), FinalIterator);
    if Assigned(FinalIterator) then
      FinalReconcile := FinalIterator.Item as TItemReconcile
    else
      FinalReconcile := nil;
  end;

  procedure Setup;
  var
    Reconcile: TItemReconcile;
    Idx: integer;
  begin
    //Add (update) existing (initial) items.
    for Idx := 0 to Pred(FKeyList.Count) do
    begin
{$IFOPT C+}
      SearchVal.GuidSearchVal := FKeyList[Idx];
      SearchVal.IntSearchVal := Idx;
      RV := ReconcileList.FindByIndex(Ord(rtGuidKey), SearchVal, IRec);
      Assert(RV = rvNotFound);
      RV := ReconcileList.FindByIndex(Ord(rtInitialIndex), SearchVal, IRec);
      Assert(RV = rvNotFound);
{$ENDIF}
      Reconcile := TItemReconcile.Create;
      Reconcile.GuidKey := FKeyList[Idx];
      Reconcile.InitialIndex := Idx;
      RV := ReconcileList.AddItem(Reconcile, IRec);
      Assert(RV = rvOK);
    end;
    //Add (update) final items.
    Idx := 0;
    //KeyedObj already set.
    while (Idx < LVSizes[FListSizeIndex]) and Assigned(KeyedObj) do
    begin
      SearchVal.GuidSearchVal := KeyedObj.Key;
      RV := ReconcileList.FindByIndex(Ord(rtGuidKey), SearchVal, IRec);
      if RV = rvOK then
      begin
        //In both initial and final lists.
        Reconcile := IRec.Item as TItemReconcile;
        Assert(Reconcile.InitialIndex >= 0);
        Assert(Reconcile.FinalIndex < 0);
        //Changing indexed values, remove and re-insert.
        ReconcileList.RemoveItem(IRec);
      end
      else
      begin
        //Only in final list, not in initial.
        Reconcile := TItemReconcile.Create;
        Reconcile.GuidKey := KeyedObj.Key;
      end;
      Reconcile.FinalIndex := Idx;
      RV := ReconcileList.AddItem(Reconcile, IRec);
      Assert(RV = rvOK);

      KeyedObj := List.AdjacentBySortVal(katNext, ksvPresentationOrder,
        KeyedObj);
      Inc(Idx);
    end;
  end;

begin
  ReconcileList := TIndexedStore.Create;
  for RecTag := Low(RecTag) to High(RecTag) do
    ReconcileList.AddIndex(TReconcileINode, Ord(RecTag));
  SearchVal := TReconcileSearchVal.Create;
{$IFOPT C+}
  InitialKeyedObj := KeyedObj;
{$ENDIF}
  try
    Setup;
    InitInitial;
    InitFinal;
    while Assigned(InitialReconcile) or Assigned(FinalReconcile) do
    begin
      if Assigned(InitialReconcile) and Assigned(FinalReconcile) then
      begin
        if InitialReconcile = FinalReconcile then
        begin
          Assert(InitialReconcile.GuidKey = FinalReconcile.GuidKey);
          LVItem := FListView.Items[FinalReconcile.FinalIndex];
          KeyedObj := List.SearchByInternalKeyOnly(FinalReconcile.GuidKey);
          Assert(Assigned(KeyedObj));
          RefreshItem(List.GetListLevel, LVItem, KeyedObj, false);
          IncInitial;
          IncFinal;
        end
        else
        begin
          Assert(InitialReconcile.GuidKey <> FinalReconcile.GuidKey);
          //Now, look in the "other" list for the same key. This lets us
          //determine, whether an add, delete, or move.
          InitialInOtherIndex := InitialReconcile.FinalIndex;
          FinalInOtherIndex := FinalReconcile.InitialIndex;
          if InitialInOtherIndex > FinalReconcile.FinalIndex then
          begin
            //Just treat this as a new insertion, because there's no
            //good "CopyFrom"
            FKeyList.Insert(FinalReconcile.FinalIndex, FinalReconcile.GuidKey);
            LVItem := FListView.Items.Insert(FinalReconcile.FinalIndex);
            KeyedObj := List.SearchByInternalKeyOnly(FinalReconcile.GuidKey);
            Assert(Assigned(KeyedObj));
            RefreshItem(List.GetListLevel, LVItem, KeyedObj, true);
            IncFinal;
          end
          else
          begin
            if FinalInOtherIndex > InitialReconcile.InitialIndex then
            begin
              //Deletion.
              FKeyList.Delete(FinalReconcile.FinalIndex);
              FListView.Items.Delete(FinalReconcile.FinalIndex);
              IncInitial;
            end
            else
            begin
              //Object changed, not lower in either previous list, no change in counts-ish
              LVItem := FListView.Items[FinalReconcile.FinalIndex];
              KeyedObj := List.SearchByInternalKeyOnly(FinalReconcile.GuidKey);
              Assert(Assigned(KeyedObj));
              Assert(KeyedObj.Key = FinalReconcile.GuidKey);
              //Changed key, changed item.
              FKeyList[FinalReconcile.FinalIndex] := FinalReconcile.GuidKey;
              RefreshItem(List.GetListLevel, LVItem, KeyedObj, true);
              IncInitial;
              IncFinal;
            end;
          end;
        end;
      end
      else if Assigned(InitialReconcile) then
      begin
        //Final reconcile done, all we need to do is delete remaining items
        //from initial.
        ReconcileList.LastByIndex(Ord(rtFinalIndex), FinalIterator);
        if Assigned(FinalIterator) then
          FinalReconcile := FinalIterator.Item as TItemReconcile
        else
          FinalReconcile := nil;
        if Assigned(FinalReconcile) and (FinalReconcile.FinalIndex >= 0) then
          Idx := Succ(FinalReconcile.FinalIndex)
        else
          Idx := 0;

        while (FKeyList.Count > Idx) do
          FKeyList.Delete(Pred(FKeyList.Count));
        while FListView.Items.Count > Idx do
          FListView.Items.Delete(Pred(FListView.Items.Count));

        Assert(FKeyList.Count = Idx);
        Assert(FListView.Items.Count = Idx);
        InitialIterator := nil;
        InitialReconcile := nil;
        FinalIterator := nil;
        FinalReconcile := nil;
      end
      else if Assigned(FinalReconcile) then
      begin
        //From here on in, we are adding brand new items.
        //Not changing any indexes in reconcile or existing list.
        Assert(FinalReconcile.FinalIndex = FKeyList.Count);
        FKeyList.Insert(FinalReconcile.FinalIndex, FinalReconcile.GuidKey);
        LVItem := FListView.Items.Insert(FinalReconcile.FinalIndex);
        KeyedObj := List.SearchByInternalKeyOnly(FinalReconcile.GuidKey);
        Assert(Assigned(KeyedObj));
        RefreshItem(List.GetListLevel, LVItem, KeyedObj, true);
        IncFinal;
      end
      else
      begin
        //Neither assigned, and yet we checked indexes at start of loop!
        Assert(false);
      end;
    end;

{$IFOPT C+}
    //Debug check one, Going through reconcile in final index order gives us
    //correct list keys.
    Idx := 0;
    RV := ReconcileList.FirstByIndex(Ord(rtFinalIndex), IRec);
    while (RV = rvOK) do
    begin
      FinalReconcile := IRec.Item as TItemReconcile;
      if FinalReconcile.FinalIndex >= 0 then
      begin
        Assert((Idx >= 0) and (Idx < FKeyList.Count));
        Assert(FinalReconcile.GuidKey = FKeyList[idx]);
        Inc(Idx);
      end;
      RV := ReconcileList.NextByIndex(Ord(rtFinalIndex), IRec);
    end;

    //Debug check two, Going through working list in presentation order
    //gives us correct list keys.
    KeyedObj := InitialKeyedObj;
    Idx := 0;
    while (Idx < LVSizes[FListSizeIndex]) and Assigned(KeyedObj) do
    begin
      Assert((Idx >= 0) and (Idx < FKeyList.Count));
      Assert(KeyedObj.Key = FKeyList[idx]);
      KeyedObj := List.AdjacentBySortVal(katNext, ksvPresentationOrder, KeyedObj);
      Inc(Idx);
    end;
{$ENDIF}
  finally
    //Clear and delete reoncile list.
    for RecTag := Low(RecTag) to High(RecTag) do
      ReconcileList.DeleteIndex(Ord(RecTag));
    ReconcileList.DeleteChildren;
    ReconcileList.Free;
    SearchVal.Free;
  end;
end;

procedure TItemListViewHelper.ReloadList(List: TKIdList);
var
  KeyedObj, NextKeyedObj: TKKeyedObject;
  i: integer;
begin
  if not Assigned(FListView) then
    exit;
  FListView.Selected := nil;
  if Assigned(List) then
  begin
    KeyedObj := nil;
    if FSelectedItemKey <> TGuid.Empty then
      KeyedObj := List.SearchByInternalKeyOnly(FSelectedItemKey);
    // If assigned keyedobj, then back up by half list size if we can.
    i := 0;

    NextKeyedObj := KeyedObj;
    while (i < (LVSizes[FListSizeIndex] div 2)) and Assigned(NextKeyedObj) do
    begin
      NextKeyedObj := List.AdjacentBySortVal(katPrevious,
        ksvPresentationOrder, NextKeyedObj);
      if Assigned(NextKeyedObj) then
        KeyedObj := NextKeyedObj;
      Inc(i);
    end;
    if not Assigned(KeyedObj) then
      KeyedObj := List.AdjacentBySortVal(katFirst, ksvPresentationOrder, nil);

    //Could reload entire list if things go wrong...
    ReconcileLists(List, KeyedObj);
    // Now go through and load as much of view as possible.
    // And in addition, select the  thing we selected...
  end
  else
  begin
    // TODO - Durables if GUI does not like having things
    // removed under its feet.
    FListView.Items.Clear;
    FKeyList.Clear;
  end;
  UpdateLoadMoreBtn(List);
  i := FKeyList.IndexOf(FSelectedItemKey);
  if i >= 0 then
    FListView.Selected := FListView.Items[i]
  else
    SelectedItemKey := TGuid.Empty;
end;

procedure TItemListViewHelper.ReloadWithFilter(List: TKIdList; FilterStr: string);
var
  FilteredList: TKIdList;
  LLevel: TKListLevel;
  InitialItem, CurItem: TKKeyedObject;
  Adj: TKAdjacencyType;
  Idx: integer;

  function IdMatches(FilterStr, ItemStr: string): boolean;
  begin
    result := (Pos(FilterStr, ItemStr) = 1);
  end;

begin
  //TODO - Could extend filter from previous filtered list,
  //but at the moment, don't store state even if re-filter is more
  //computationally expensive.
  LLevel := List.GetListLevel;
  FilteredList := GetListClass(LLevel).Create as TKIdList;
  try
    InitialItem := List.SearchNearBySortVal(ksvIdentifierOrder, FilterStr);
    if Assigned(InitialItem) then
    begin
      if IdMatches(FilterStr, IdentifierStringFromItem(InitialItem, LLevel)) then
        FilteredList.Add(TKDataObject.Clone(InitialItem) as TKKeyedObject);

      for Adj := katNext to katPrevious do
      begin
        Idx := 0;
        CurItem := List.AdjacentBySortVal(Adj, ksvIdentifierOrder, InitialItem);
        while Assigned(CurItem) and
          IdMatches(FilterStr, IdentifierStringFromItem(CurItem, LLevel))
          and (Idx < LVSizes[FListSizeIndex]) do
        begin
          FilteredList.Add(TKDataObject.Clone(CurItem) as TKKeyedObject);
          CurItem := List.AdjacentBySortVal(Adj, ksvIdentifierOrder, CurItem);
          Inc(Idx);
        end;
      end;
    end;
    ReloadList(FilteredList);
  finally
    FilteredList.DeleteChildren;
    FilteredList.Free;
  end;
end;

procedure TItemListViewHelper.UpdateLoadMoreBtn(List: TKItemList);
var
  Count: integer;
begin
  if Assigned(LoadMoreBtn) then
  begin
    if Assigned(List) then
      Count := List.Count
    else
      Count := 0;

    LoadMoreBtn.Text := S_LOAD + LVTexts[FListSizeIndex];
    LoadMoreBtn.Visible := (Count > LVSizes[FListSizeIndex]);
  end;
end;

procedure TItemListViewHelper.DelayedImageLoad(Sender: TObject; var Done: boolean);
var
  Req: TPendingImageRequest;
  Idx: integer;
begin
  Done := FPendingImageRequests.Count = 0;
  if not Done then
  begin
    for Idx := 0 to Pred(FPendingImageRequests.Count) do
    begin
      Req := TPendingImageRequest(FPendingImageRequests[Idx]);
      RequestImage(Req.ImageUrl, Req.ItemKey);
      Req.Free;
    end;
    FPendingImageRequests.Clear;
  end;
end;

procedure TItemListViewHelper.HandleViewChange(Sender: TObject);
begin
  UpdateSelection(Sender, false);
end;

procedure TItemListViewHelper.UpdateSelection(Sender: TObject; Force: boolean);
var
  SelectedIdx: integer;
  NewSelectedKey: TGuid;
begin
  if Assigned(FListView.Selected) then
  begin
    SelectedIdx := FListView.ItemIndex;
    Assert(FListView.Items[SelectedIdx] = FListView.Selected);
    NewSelectedKey := FKeyList[SelectedIdx];
  end
  else
    NewSelectedKey := TGuid.Empty;
  //TODO - could make client form code explicit about
  //which selected keys are being referenced, dereferenced.
  FSelectingItemKey := NewSelectedKey;
  if Assigned(FOnSelectionUpdated) then
    FOnSelectionUpdated(self, FSelectedItemKey, FSelectingItemKey, Force);
  FSelectedItemKey := NewSelectedKey;
  FSelectingItemKey := TGuid.Empty;
end;

procedure TItemListViewHelper.SetSelectedItemKey(const NewKey: TGuid);
var
  ListIdx: integer;
begin
  ListIdx := FKeyList.IndexOf(NewKey);
  if NewKey = TGuid.Empty then
    FListView.Selected := nil
  else if ListIdx >= 0 then
    FListView.Selected := FListView.Items[ListIdx];
  UpdateSelection(self, false);
end;

procedure TItemListViewHelper.HandleLoadMoreClicked(Sender: TObject);
begin
  if FListSizeIndex < High(FListSizeIndex) then
    Inc(FListSizeIndex);
  //Reload in container object.
end;

procedure TItemListViewHelper.HandleImageLoaded(Sender: TObject; var Data: TStream; Ref1: TObject; Ref2: string; OK: boolean);
var
  Idx: integer;
  LI: TListViewItem;
  Ref2Guid: TGuid;
begin
  Assert(Ref1 = FListView);
  Assert(Assigned(FListView));
  //By this point, string should be a GUID representation.
  Ref2Guid := StringToGuid(Ref2);
  Idx := FKeyList.IndexOf(Ref2Guid);
  if (Idx >= 0) and (Idx < FListView.Items.Count) then
  begin
    LI := FListView.Items[Idx];
    if OK and Assigned(Data) then
      LI.Bitmap.LoadFromStream(Data)
    else
      LI.Bitmap.Clear(TAlphaColorRec.Alpha);
  end;
end;

procedure ImageLoadFailed(Guid: string; ListLevel:TKListLevel; WorkingList: TKIdList);
var
  Ref2Guid: TGuid;
  Comment: TKCommentItem;
begin
  try
    Ref2Guid := StringToGuid(Guid);
  except
    on EConvertError do exit;
  end;
    case ListLevel of
      klUserList: DBCont.BatchLoader.MinimalUserRefesh(Ref2Guid);
      klMediaList: ; //TODO . Refresh owning user? Later?
      klCommentList:
      begin
        //Need to get from comment to owning user.
        if Assigned(WorkingList) then
        begin
          //Might not be the current list. If not, we've navigated away,
          //don't worry about it.
          Comment := WorkingList.SearchByInternalKeyOnly(Ref2Guid) as TKCommentItem;
          if Assigned(Comment) then
            DBCont.BatchLoader.MinimalUserRefesh(Comment.OwnerKey);
        end;
      end;
    end;
end;

end.
