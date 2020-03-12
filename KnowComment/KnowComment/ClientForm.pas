unit ClientForm;
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
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, FMX.ListView.Types, FMX.Layouts, FMX.ListView, BatchLoader,
  DataObjects, ImageCache, DBContainer, FMX.Objects, FMX.Memo, FMX.Menus,
  GUIMisc;

type
  TClientFormType = (cftList, cftDetail);

  TClientFrm = class(TForm)
    ItemListView: TListView;
    ItemClientLayout: TLayout;
    ListLayout: TLayout;
    LoadMoreBtn: TButton;
    MasterLayout: TLayout;
    Splitter1: TSplitter;
    DetailLayout: TLayout;
    DetailImage: TImage;
    MediaDetailLayout: TLayout;
    UserDetailLayout: TLayout;
    UserLbl: TLabel;
    UserFollowsLbl: TLabel;
    MediaTypeImage: TImage;
    UserTypeImage: TImage;
    UserVerifiedChk: TCheckBox;
    UserFollowedLbl: TLabel;
    UserFullNameLbl: TLabel;
    MediaDateLbl: TLabel;
    MediaPosterLbl: TLabel;
    UserBioLabel: TLabel;
    MediaTextIntroLabel: TLabel;
    PopupMenu1: TPopupMenu;
    MenuItem1: TMenuItem;
    MenuItem2: TMenuItem;
    MenuItem3: TMenuItem;
    MenuItem4: TMenuItem;
    MenuItem5: TMenuItem;
    MenuItem6: TMenuItem;
    MenuItem7: TMenuItem;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure ItemListViewChange(Sender: TObject);
    procedure ItemListViewEnter(Sender: TObject);
    procedure LoadMoreBtnClick(Sender: TObject);
  private
    { Private declarations }
    FFormType: TClientFormType;
    FFixedList: TKUserList;
    FListLevel: TKListLevel;
    FListHelper: TItemListViewHelper;
    FSubForm: TClientFrm;
    FDetailForm: TClientFrm;

    FSelectedItemRefs: integer;
    FParentItem: TKKeyedObject;

    FDetailView: boolean;
    FListLevelSet: boolean;
    FDetailViewSet: boolean;
    FFocusMyLevel: boolean;
    FDetailViewSeq: integer;
    FQuickFilterStr: string;

    function RefSelectedItem: TKKeyedObject;
    procedure DerefSelectedItem(Item: TKKeyedObject);
    function GetTopLevelForm: TForm;
    function GetLowestClientLayout: TLayout;
    procedure RequestImage(URL: string; Ref2: string);
    function GetParentList: TKIdList;
    procedure FreeParentList(List: TKIdList);
    procedure UpdateDetailPre(NewFocussedForm: TObject);
    procedure UpdateDetailPost(const NewOrExistingKey: TGuid);
    procedure ReloadDetails;
    procedure HandleLVUserListFetch(Sender: TObject; var UL: TKUserList);
    procedure HandleLVUserListPut(Sender: TObject; UL: TKUserList);
    procedure HandleLVSelectionUpdated(Sender: TObject;
                                     const OldSelectedKey, NewSelectedKey: TGuid;
                                     Force: boolean);

    procedure SetListLevel(NewLevel: TKListLevel);
    procedure SetDetailView(NewDetailView: boolean);
    procedure SetQuickFilterStr(NewFilterStr: string);
  public
    { Public declarations }
    procedure HandleBatchLoaderCompletion(Sender: TObject; MultiOp: TMultiOp);
    procedure HandleImageLoaded(Sender: TObject; var Data: TStream;
      Ref1: TObject; Ref2: string; OK: boolean);
    procedure HandleFocusTaken(Sender: TObject);
    function GetFocussedItemGuid(LevelSet: TKListLevelSet): TGuid;
    procedure DelayedImageLoad(Sender: TObject; var Done: boolean);
    procedure ReloadList;
    function Filtered: boolean;
    procedure ApplyFilter(FilteredData: TKUserList);
    procedure ClearFilter;
    property ListLevel: TKListLevel read FListLevel write SetListLevel;
    property DetailView: boolean read FDetailView write SetDetailView;
    property ListHelper: TItemListViewHelper read FListHelper;
    property QuickFilterStr: string read FQuickFilterStr write SetQuickFilterStr;
    property FormType:TClientFormType read FFormType write FFormType;
  end;

  {
    //TODO - Glyphs go awry on form resize. Perhaps another paint/invalidate,
    //or refresh list items?
  }

var
  ClientFrm: TClientFrm;

implementation

{$R *.fmx}

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  MainForm, DataStore, IndexedStore, System.Generics.Collections;

const
  S_DETAIL_VIEW_IMAGE = 'S_DETAIL_VIEW_IMAGE';

var
  DetailViewSeq: integer;

function TClientFrm.Filtered: boolean;
begin
  result := Assigned(FFixedList);
end;

procedure TClientFrm.ApplyFilter(FilteredData: TKUserList);
begin
  if ListLevel <> klUserList then
  begin
    Assert(false);
    exit;
  end;
  //Reffing and dereffeing selected items gets very complicated,
  //when changing filter refs, hence the following line.
  FListHelper.SelectedItemKey := TGuid.Empty;
  //Destroy and deref from children before
  //changing the tree.
  Assert(Assigned(FilteredData));
  Assert(not Assigned(FFixedList));
  FFixedList := FilteredData;
  ReloadList;
end;

procedure TClientFrm.ClearFilter;
begin
  //Reffing and dereffeing selected items gets very complicated,
  //when changing filter refs, hence the following line.
  FListHelper.SelectedItemKey := TGuid.Empty;
  //Destroy and deref from children before
  //changing the tree.
  FFixedList.Free;
  FFixedList := nil;
  ReloadList;
end;

procedure TClientFrm.DelayedImageLoad(Sender: TObject; var Done: boolean);
var
  ChildDone: boolean;
begin
  FListHelper.DelayedImageLoad(Sender, Done);
  if Assigned(FSubForm) then
  begin
    FSubForm.DelayedImageLoad(Self, ChildDone);
    Done := Done and ChildDone;
  end;
  if Assigned(FDetailForm) then
  begin
    FDetailForm.DelayedImageLoad(Self, ChildDone);
    Done := Done and ChildDone;
  end;
end;

procedure TClientFrm.FormCreate(Sender: TObject);
begin
  FListHelper := TItemListViewHelper.Create;
  FListHelper.OnFetchUserList := HandleLVUserListFetch;
  FListHelper.OnPutUserList := HandleLVUserListPut;
  FListHelper.OnSelectionUpdated := HandleLVSelectionUpdated;
  FListHelper.ListView := ItemListView;
  FListHelper.LoadMoreBtn := LoadMoreBtn;

  if Assigned(Owner) and (Owner is TClientFrm) then
    FParentItem := (Owner as TClientFrm).RefSelectedItem;
  FDetailViewSeq := DetailViewSeq;
  Inc(DetailViewSeq);
end;

procedure TClientFrm.FormDestroy(Sender: TObject);
begin
  if Assigned(Owner) and (Owner is TClientFrm) then
    (Owner as TClientFrm).DerefSelectedItem(FParentItem);
  FDetailForm.Free;
  FSubForm.Free;
  FListHelper.Free;
  Assert(FSelectedItemRefs = 0);
end;

function TClientFrm.GetFocussedItemGuid(LevelSet: TKListLevelSet): TGuid;
var
  LL: TKListLevel;
  QuerySub: boolean;
  CommentGuid: TGuid;
  CommentList: TKCommentList;
  Comment: TKCommentItem;
begin
  result := TGuid.Empty;
  if FListLevelSet and (FListLevel in LevelSet) then
  begin
    Assert(FListHelper.SelectingItemKey = TGuid.Empty);
    if FFocusMyLevel then
    begin
      case FListLevel of
        klUserList: result := FListHelper.SelectedItemKey;
        klCommentList:
        begin
          CommentGuid := FListHelper.SelectedItemKey;
          //Hummm... need to get hold of comment data obj to find
          //comment owner key.
          CommentList := GetParentList as TKCommentList;
          if Assigned(CommentList) then
          begin
            try
              Comment := CommentList.SearchByInternalKeyOnly(CommentGuid) as TKCommentItem;
              if Assigned(Comment) then
                result := Comment.OwnerKey;
            finally
              FreeParentList(CommentList);
            end;
          end;
        end;
      end;
    end;
  end;
  if result = TGuid.Empty then
  begin
    // Not me, worth passing on to child lists?
    QuerySub := false;
    for LL := Succ(FListLevel) to High(LL) do
    begin
      if LL in LevelSet then
      begin
        QuerySub := true;
        break;
      end;
    end;
    if QuerySub then
    begin
      if Assigned(FSubForm) then
        result := FSubForm.GetFocussedItemGuid(LevelSet)
      else if Assigned(FDetailForm) then
        result := FDetailForm.GetFocussedItemGuid(LevelSet);
    end;
  end;
end;

procedure TClientFrm.HandleLVUserListFetch(Sender: TObject; var UL: TKUserList);
begin
  UL := DBCont.Datastore.GetTemporaryTopLevelList;
end;

procedure TClientFrm.HandleLVUserListPut(Sender: TObject; UL: TKUserList);
begin
  DBCont.Datastore.PutTopLevelList(UL);
end;

function TClientFrm.RefSelectedItem: TKKeyedObject;
var
  List: TKIdList;
  Key: TGuid;
begin
  //TODO Could make explicit ref counts on particular keys, with more elaborate
  //string list etc, instead of having LV helper indicate what the key is moving to.
  if (FListHelper.SelectingItemKey <> TGuid.Empty) and
    (FListHelper.SelectingItemKey <> FListHelper.SelectedItemKey) then
    Key := FListHelper.SelectingItemKey
  else
    Key := FListHelper.SelectedItemKey;

  if ListLevel = klUserList then
  begin
    if not Assigned(FFixedList) then
      result := DBCont.DataStore.GetDurableReadOnlyTreeByKey(Key)
    else
      result := FFixedList.SearchByInternalKeyOnly(Key);
  end
  else
  begin
    List := GetParentList;
    try
      result := List.SearchByInternalKeyOnly(Key);
    finally
      FreeParentList(List);
    end;
  end;
  if Assigned(result) then
    Inc(FSelectedItemRefs);
end;

procedure TClientFrm.DerefSelectedItem(Item: TKKeyedObject);
var
  List: TKIdList;
  Tmp: TKKeyedObject;
begin
  if Assigned(Item) then
  begin
    if ListLevel = klUserList then
    begin
      if not Assigned(FFixedList) then
        DBCont.DataStore.PutDurable(Item as TKUserProfile)
    end
    else
    begin
      //For de-refs, do not assume item key if selecting / selected
      //item key in list helper, because we can
      //de-ref prev selected when selecting new item,
      //as well as more obvious cases....
      //But it should be one of the two, old or new...

      //TODO - Assertion here when child form item is selected when expiring items. Needs fixing.

      Assert((FListHelper.SelectingItemKey = Item.Key)
        or (FListHelper.SelectedItemKey = Item.Key));
      List := GetParentList;
      try
        Tmp := List.SearchByInternalKeyOnly(Item.Key);
        Assert(Tmp = Item);
      finally
        FreeParentList(List);
      end;
    end;
    Assert(FSelectedItemRefs > 0);
    Dec(FSelectedItemRefs);
  end;
end;

function TClientFrm.GetParentList: TKIdList;
begin
  if ListLevel = klUserList then
  begin
    if not Assigned(FFixedList) then
      result := DBCont.DataStore.GetTemporaryTopLevelList
    else
      result := FFixedList;
  end
  else
  begin
    Assert(Assigned(FParentItem));
    if FParentItem is TKUserProfile then
      result := (FParentItem as TKUserProfile).Media
    else if FParentItem is TKMediaItem then
      result := (FParentItem as TKMediaItem).Comments
    else
    begin
      result := nil;
      Assert(false);
    end;
  end;
end;

procedure TClientFrm.FreeParentList(List: TKIdList);
begin
  if ListLevel = klUserList then
  begin
    if not Assigned(FFixedList) then
      DBCont.DataStore.PutTopLevelList(List as TKUserList);
  end
  else
  begin
    Assert(Assigned(FParentItem));
    if FParentItem is TKUserProfile then
      Assert((FParentItem as TKUserProfile).Media = List)
    else if FParentItem is TKMediaItem then
      Assert((FParentItem as TKMediaItem).Comments = List)
    else
      Assert(false);
  end;
end;

procedure TClientFrm.ReloadList;
var
  WorkingList: TKIdList;
begin
  if not Assigned(ItemListView) and Assigned (FListHelper) then
    exit;
  WorkingList := GetParentList;
  try
    if Length(FQuickFilterStr) > 0 then
      FListHelper.ReloadWithFilter(WorkingList, FQuickFilterStr)
    else
      FListHelper.ReloadList(WorkingList);
  finally
    FreeParentList(WorkingList);
  end;
end;

procedure TClientFrm.SetQuickFilterStr(NewFilterStr: string);
begin
  if NewFilterStr <> FQuickFilterStr then
  begin
    FQuickFilterStr := NewFilterStr;
    ReloadList;
  end;
end;

procedure TClientFrm.LoadMoreBtnClick(Sender: TObject);
begin
  FListHelper.HandleLoadMoreClicked(Sender);
  ReloadList;
end;

procedure TClientFrm.HandleBatchLoaderCompletion(Sender: TObject;
  MultiOp: TMultiOp);
var
  UKMOp: TUserKeyedMultiOp;
  PurgeOp: TDBPurgeMultiOp;
  ExpireOp: TDBExpireMultiOp;
  IMop: TImportMultiOp;
  KeyList: TList<TGuid>;
begin
  // N.B. This just runs in top level client form - not sent to children.
  Assert(ListLevel = klUserList);
  //Does not run as part of an LV re-select.
  Assert(FListHelper.SelectingItemKey = TGuid.Empty);

  // TODO - Arguable how much refresh logic we should run if a command fails...
  if not MultiOp.OK then
    exit; // May need revisiting.

  case MultiOp.OpType of
    motDbLoadUserList:
      ReloadList;
    motDbLoadUserTrees:
      begin
        UKMOp := MultiOp as TUserKeyedMultiOp;
        if Assigned(UKMOp.PresentationUserKeysRemaining.SearchByInternalKeyOnly
          (FListHelper.SelectedItemKey)) then
          FListHelper.UpdateSelection(Self, true);
        // If update when error, beware of infinite recursion reloading tree...
      end;
    motImportOrIncrease:
      begin
        IMop := MultiOp as TImportMultiOp;
        if IMOp.IncreasedInterestOrNewScan then
          ReloadList;
        KeyList := IMop.GetSortedKeyList;
        if KeyList.IndexOf(FListHelper.SelectedItemKey) >= 0 then
          FListHelper.UpdateSelection(self, true);
        // If update when error, beware of infinite recursion reloading tree...
        //TODO - need to reload the list if user interest level has changed.
      end;
    motPurgeOrDecrease:
      begin
        //For All purge or expire ops, unfortunately, we always need to reload
        //the user list.
        PurgeOp := MultiOp as TDBPurgeMultiOp;
        if (PurgeOp.PurgeLevel = klUserList) or
          PurgeOp.DecreasedInterest then
          ReloadList;
        //If purge up changes downstream contents of selected item key...
        if Assigned(PurgeOp.PresentationUserKeysRemaining.
          SearchByInternalKeyOnly(FListHelper.SelectedItemKey)) then
          FListHelper.UpdateSelection(self, true)
      end;
    motExpire:
      begin
        ReloadList;
        ExpireOp := MultiOp as TDBExpireMultiOp;
        //Do not need to reload our own list (no interest change or
        //LV item change, but may need to refresh lower level views.
        if Assigned(ExpireOp.PresentationUserKeysRemaining.
          SearchByInternalKeyOnly(FListHelper.SelectedItemKey)) then
          FListHelper.UpdateSelection(self, true)
        else
          FListHelper.UpdateSelection(self, false);
      end;
  end;
end;

function TClientFrm.GetLowestClientLayout: TLayout;
begin
  if Assigned(FSubForm) then
    result := (FSubForm as TClientFrm).GetLowestClientLayout
  else
    result := ItemClientLayout;
end;

function TClientFrm.GetTopLevelForm: TForm;
begin
  if Assigned(Owner) then
  begin
    if Owner is TClientFrm then
      result := (Owner as TClientFrm).GetTopLevelForm
    else if Owner is TMainFrm then
      result := Owner as TForm
    else
    begin
      Assert(Owner is TApplication);
      result := MainFrm;
    end;
  end
  else
    result := MainFrm;
end;

procedure TClientFrm.RequestImage(URL: string; Ref2: string);
begin
  ImgCache.RequestImage(URL, Self, Ref2);
end;

procedure TClientFrm.HandleImageLoaded(Sender: TObject; var Data: TStream;
  Ref1: TObject; Ref2: string; OK: boolean);
var
  WorkingList: TKIdList;
begin
  if Ref1 = Self then
  begin
    if Ref2 = S_DETAIL_VIEW_IMAGE + IntToStr(FDetailViewSeq) then
    begin
      if OK and Assigned(Data) then
        DetailImage.Bitmap.LoadFromStream(Data)
      else
        DetailImage.Bitmap.Clear(TAlphaColorRec.Alpha);
    end;
    //Else hopefully old detail view image (switch windows faster than load)
  end
  else if Ref1 = ItemListView then
  begin
    FListHelper.HandleImageLoaded(Sender, Data, Ref1, Ref2, OK);
    if not OK then
    begin
      WorkingList := GetParentList;
      try
        GuiMisc.ImageLoadFailed(Ref2, ListLevel, WorkingList);
      finally
        FreeParentList(WorkingList);
      end;
    end;
  end
  else
  begin
    // Pass to clients?
    if Assigned(FSubForm) then
      (FSubForm as TClientFrm).HandleImageLoaded(Sender, Data, Ref1, Ref2, OK);
    if Assigned(FDetailForm) then
      (FDetailForm as TClientFrm).HandleImageLoaded(Sender, Data, Ref1,
        Ref2, OK);
  end;
end;

procedure TClientFrm.HandleLVSelectionUpdated(Sender: TObject;
                                     const OldSelectedKey, NewSelectedKey: TGuid;
                                     Force: boolean);
var
  SelectedUP: TKUserProfile;
  ObRef: TDataObRefCounts;
  PList: TKIdList;
  KeyCurrent: boolean;
begin
  KeyCurrent := true;
  // In some delete cases, key might be out of date.
  if NewSelectedKey <> TGuid.Empty then
  begin
    PList := GetParentList;
    try
      if not Assigned(PList.SearchByInternalKeyOnly(NewSelectedKey)) then
      begin
        //I need to investigate what happens here.
        try
          Assert(false);
        except
          on EAssertionFailed do ;
        end;
        KeyCurrent := false;
      end;
    finally
      FreeParentList(PList);
    end;
  end;

  //The out of date case may be different when we are called via the list helper.
  if (NewSelectedKey <> OldSelectedKey) or Force then
  begin
    if Assigned(FSubForm) then
      FreeAndNil(FSubForm);
    UpdateDetailPre(nil);
    if (NewSelectedKey <> TGuid.Empty) and KeyCurrent then
    begin
      if ListLevel < Pred(High(ListLevel)) then
      begin
        FSubForm := TClientFrm.Create(Self);
        FSubForm.FormType := cftList;
        FSubForm.ListLevel := Succ(Self.ListLevel);
        FSubForm.DetailView := false;
        FSubForm.MasterLayout.Parent := Self.ItemClientLayout;
        FSubForm.ReloadList;
        if (ListLevel = klUserList) and (not Assigned(FFixedList)) then
        begin
          // May need to load user tree into ram, so for some cases,
          // we might initially get an empty list.
          SelectedUP := RefSelectedItem as TKUserProfile;
          try
            if (SelectedUP.InterestLevel > kpiFetchUserForRefs) then
            begin
              ObRef := SelectedUP.Ref as TDataObRefCounts;
              Assert(Assigned(ObRef));
              if not(obhTreeUpToDate in ObRef.HintBits) then
                DBCont.BatchLoader.LoadUserTree(NewSelectedKey);
            end;
          finally
            DerefSelectedItem(SelectedUP);
          end;
        end;
      end;
    end;
    UpdateDetailPost(NewSelectedKey);
  end;
end;

procedure TClientFrm.ItemListViewChange(Sender: TObject);
begin
  FListHelper.HandleViewChange(Sender);
end;

procedure TClientFrm.ItemListViewEnter(Sender: TObject);
var
  TopLevel: TForm;
begin
  //TODO - Bit wrong. Focus calcs here are freeing child detail forms
  //just as it gets focus.
  FFocusMyLevel := true;
  TopLevel := GetTopLevelForm;
  if TopLevel is TMainFrm then
    (TopLevel as TMainFrm).BroadcastFocusChange(Self);
    //Check we're not active changing keys here.
  Assert(FListHelper.SelectingItemKey = TGuid.Empty);
  UpdateDetailPost(FListHelper.SelectedItemKey);
end;

procedure TClientFrm.HandleFocusTaken(Sender: TObject);
begin
  if Sender <> Self then
  begin
    FFocusMyLevel := false;
    UpdateDetailPre(Sender);
  end;
  if Assigned(FSubForm) then
    FSubForm.HandleFocusTaken(Sender);
  if Assigned(FDetailForm) then
    FDetailForm.HandleFocusTaken(Sender);
end;

procedure TClientFrm.ReloadDetails;
var
  U: TKUserProfile;
  MI: TKMediaItem;
  UB: TKSiteUSerBlock;
  ST: TKSiteType;
begin
  if not Assigned(DetailLayout) then
    exit;
  if ListLevel = klMediaList then
  begin
    // Loading user details
    U := FParentItem as TKUserProfile;
    for ST := Low(ST) to High(ST) do
    begin
      if Assigned(U.SiteUserBlocks[ST]) and U.SiteUserBlocks[ST].Valid then
      begin
        UB := U.SiteUserBlocks[ST];
        UserLbl.Text := UB.Username;
        UserFollowsLbl.Text := UserFollowsLbl.Text + ' ' +
          IntToStr(UB.FollowsCount);
        UserVerifiedChk.IsChecked := UB.Verified;
        UserFollowedLbl.Text := UserFollowedLbl.Text + ' ' +
          IntToStr(UB.FollowerCount);
        UserFullNameLbl.Text := UB.FullName;
        UserBioLabel.Text := UB.Bio;
        RequestImage(UB.ProfilePicUrl, S_DETAIL_VIEW_IMAGE +
          IntToStr(FDetailViewSeq));
        break;
      end;
    end;
  end
  else if ListLevel = klCommentList then
  begin
    // Loading media details.
    MI := FParentItem as TKMediaItem;
    MediaDateLbl.Text := DateTimeToStr(MI.Date);
    // TODO - Handle HTML.
    case MI.MediaType of
      mitPlainText, mitHTML:
        MediaTextIntroLabel.Text := MI.MediaData;
      mitMetaLink:
        MediaTextIntroLabel.Text := GuiMisc.S_LINK;
    end;
    RequestImage(MI.ResourceURL, S_DETAIL_VIEW_IMAGE +
      IntToStr(FDetailViewSeq));
    // TODO - Username of original poster.
  end
  else
    Assert(false);
end;

procedure TClientFrm.UpdateDetailPre(NewFocussedForm: TObject);
begin
  if (not Assigned(NewFocussedForm)) //Key change.
    or ((NewFocussedForm <> Self) and (NewFocussedForm <> FDetailForm)) then
    FreeAndNil(FDetailForm);
{
  if ParentKeyChanging or
    (not(FFocusMyLevel and (FListHelper.SelectedItemKey <> TGuid.Empty) and
    (ListLevel < High(ListLevel)))) then
  begin
    FreeAndNil(FDetailForm);
  end;
}
end;

procedure TClientFrm.UpdateDetailPost(const NewOrExistingKey: TGuid);
begin
  if (FFocusMyLevel and (NewOrExistingKey <> TGuid.Empty) and
    (ListLevel < High(ListLevel))) then
  begin
    FreeAndNil(FDetailForm);
    FDetailForm := TClientFrm.Create(Self);
    FDetailForm.FormType := cftDetail;
    FDetailForm.ListLevel := Succ(Self.ListLevel);
    FDetailForm.DetailView := true;
    FDetailForm.MasterLayout.Parent := GetLowestClientLayout;
    FDetailForm.ReloadList;
    FDetailForm.ReloadDetails;
  end;
end;

procedure TClientFrm.SetListLevel(NewLevel: TKListLevel);
begin
  Assert(not FDetailViewSet);
  FListLevel := NewLevel;
  FListLevelSet := true;
end;

procedure TClientFrm.SetDetailView(NewDetailView: boolean);
begin
  Assert(FListLevelSet);
  Assert(not FDetailViewSet);
  FDetailView := NewDetailView;
  FDetailViewSet := true;
  if not FDetailView then
  begin
    FreeAndNil(DetailLayout);
    UserLbl := nil;
    UserFollowsLbl := nil;
    UserTypeImage := nil;
    UserVerifiedChk := nil;
    UserFollowedLbl := nil;
    UserFullNameLbl := nil;
    UserBioLabel := nil;

    MediaDateLbl := nil;
    MediaTextIntroLabel := nil;
    MediaPosterLbl := nil;
    MediaTypeImage := nil;
  end
  else
  begin
    DetailLayout.Align := TAlignLayout.alClient;
    if ListLevel = klMediaList then
    begin
      // TODO - Might change visible flags instead
      FreeAndNil(ListLayout);
      FreeAndNil(Splitter1);
      ItemListView := nil;
      LoadMoreBtn := nil;

      FreeAndNil(MediaDetailLayout);
      MediaDateLbl := nil;
      MediaTextIntroLabel := nil;
      MediaPosterLbl := nil;
      MediaTypeImage := nil;

      UserDetailLayout.Align := TAlignLayout.alTop;
      DetailImage.Align := TAlignLayout.alClient;
    end
    else if ListLevel = klCommentList then
    begin
      // TODO - Might change visible flags instead.
      FreeAndNil(UserDetailLayout);
      UserLbl := nil;
      UserFollowsLbl := nil;
      UserTypeImage := nil;
      UserVerifiedChk := nil;
      UserFollowedLbl := nil;
      UserFullNameLbl := nil;
      UserBioLabel := nil;

      MediaDetailLayout.Align := TAlignLayout.alRight;
      ListLayout.Align := TAlignLayout.alBottom;
      ListLayout.Height := Self.Height div 2;
      Splitter1.Align := TAlignLayout.alBottom;
      DetailImage.Align := TAlignLayout.alClient;
    end;
  end;
end;

end.
