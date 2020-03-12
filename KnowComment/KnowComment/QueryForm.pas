unit QueryForm;
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
  FMX.StdCtrls, FMX.ListView.Types, FMX.Edit, FMX.ListView, FMX.Layouts,
  DataObjects, GUIMisc;

type
  TQueryFrm = class(TForm)
    AllUsersLayout: TLayout;
    AddedUsersLayout: TLayout;
    LoadMoreBtn: TButton;
    AllUsersView: TListView;
    AddedUsersView: TListView;
    AllUsersSearchEdit: TEdit;
    SelectButton: TButton;
    UnselectButton: TButton;
    RunFilterBtn: TButton;
    CloseBtn: TButton;
    AddVerifiedBtn: TButton;
    AddInterestedBtn: TButton;
    ClearBtn: TButton;
    Label1: TLabel;
    procedure CloseBtnClick(Sender: TObject);
    procedure FormShow(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure LoadMoreBtnClick(Sender: TObject);
    procedure AllUsersViewChange(Sender: TObject);
    procedure AddedUsersViewChange(Sender: TObject);
    procedure SelectButtonClick(Sender: TObject);
    procedure UnselectButtonClick(Sender: TObject);
    procedure RunFilterBtnClick(Sender: TObject);
    procedure ClearBtnClick(Sender: TObject);
    procedure AddVerifiedBtnClick(Sender: TObject);
    procedure AddInterestedBtnClick(Sender: TObject);
    procedure AllUsersSearchEditChange(Sender: TObject);
    procedure AllUsersSearchEditKeyUp(Sender: TObject; var Key: Word;
      var KeyChar: Char; Shift: TShiftState);
  private
    { Private declarations }
    FAddedUsers: TKUserList;
    FInitialLoadKey: TGuid;
    FAddedListHelper: TItemListViewHelper;
    FAllListHelper: TItemListViewHelper;
    procedure AddProfiles(Interested: boolean);
    procedure SelectOneItemByKey(const Key: TGuid);
  public
    { Public declarations }
    procedure HandleImageLoaded(Sender: TObject; var Data: TStream;
      Ref1: TObject; Ref2: string; OK: boolean);
    procedure DelayedImageLoad(Sender: TObject; var Done: boolean);
    property InitialLoadKey: TGuid read FInitialLoadKey write FInitialLoadKey;
  end;

var
  QueryFrm: TQueryFrm;

implementation

{$R *.fmx}

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  DBContainer, ImageCache;

procedure TQueryFrm.AddedUsersViewChange(Sender: TObject);
begin
  FAddedListHelper.HandleViewChange(Sender);
end;

procedure TQueryFrm.AllUsersSearchEditChange(Sender: TObject);
var
  SearchStr: string;
  MainList: TKUserList;
begin
  SearchStr := AllUsersSearchEdit.Text.Trim;
  if Assigned(DBCont) and Assigned(DBCont.Datastore) then
  begin
    MainList := DBCont.Datastore.GetTemporaryTopLevelList;
    try
      if Length(SearchStr) > 0 then
        FAllListHelper.ReloadWithFilter(MainList, SearchStr)
      else
        FAllListHelper.ReloadList(MainList);
    finally
      DBCont.Datastore.PutTopLevelList(MainList);
    end;
  end
  else
    FAllListHelper.ReloadList(nil);
end;

procedure TQueryFrm.AllUsersSearchEditKeyUp(Sender: TObject; var Key: Word;
  var KeyChar: Char; Shift: TShiftState);
begin
  AllUsersSearchEditChange(Sender);
end;

procedure TQueryFrm.AllUsersViewChange(Sender: TObject);
begin
  FAllListHelper.HandleViewChange(Sender);
end;

procedure TQueryFrm.DelayedImageLoad(Sender: TObject; var Done: boolean);
var
  ChildDone: boolean;
begin
  FAddedListHelper.DelayedImageLoad(Sender, ChildDone);
  Done := ChildDone;
  FAllListHelper.DelayedImageLoad(Sender, ChildDone);
  Done := Done and ChildDone;
end;

procedure TQueryFrm.CloseBtnClick(Sender: TObject);
begin
  Close;
end;

procedure TQueryFrm.FormCreate(Sender: TObject);
begin
  FAddedUsers := TKUserList.Create;
  FAddedListHelper := TItemListViewHelper.Create;
  FAddedListHelper.ListView := AddedUsersView;
  //No load more button, no callbacks.
  FAllListHelper := TItemListViewHelper.Create;
  FAllListHelper.ListView := AllUsersView;
  FAllListHelper.LoadMoreBtn := LoadMoreBtn;
  //No callbacks in this simple example.
end;

procedure TQueryFrm.FormDestroy(Sender: TObject);
begin
  FAddedListHelper.Free;
  FAllListHelper.Free;
  FAddedUsers.Free;
end;

procedure TQueryFrm.FormShow(Sender: TObject);
begin
  FAddedUsers.DeleteChildren;
  FAddedListHelper.ListSizeIndex := High(FAddedListHelper.ListSizeIndex);
  FAddedListHelper.ReloadList(FAddedUsers);
  FAllListHelper.ListSizeIndex := Low(FAllListHelper.ListSizeIndex);
  AllUsersSearchEditChange(Sender);
  SelectOneItemByKey(InitialLoadKey);
end;

procedure TQueryFrm.RunFilterBtnClick(Sender: TObject);
begin
  DBCont.BatchLoader.CustomQuery(klCommentList, FAddedUsers, nil);
  Close;
end;

procedure TQueryFrm.SelectOneItemByKey(const Key: TGuid);
var
  SrcUP: TKUSerProfile;
  DstUP: TKUSerProfile;
  MainList: TKUserList;
begin
  if Key <> TGuid.Empty then
  begin
    MainList := DBCont.Datastore.GetTemporaryTopLevelList;
    if Assigned(MainList) then
    begin
      try
        SrcUP := MainList.SearchByInternalKeyOnly(Key) as TKUSerProfile;
        if Assigned(SrcUP) then
        begin
          DstUP := FAddedUsers.SearchByInternalKeyOnly(Key) as TKUSerProfile;
          if not Assigned(DstUP) then
          begin
            DstUP := TKKeyedObject.Clone(SrcUP) as TKUserProfile;
            FAddedUsers.Add(DstUP);
            FAddedListHelper.ReloadList(FAddedUsers);
          end;
        end
        else //Hum. Main list changed?
          FAllListHelper.ReloadList(MainList);
      finally
        DBCont.Datastore.PutTopLevelList(MainList);
      end;
    end;
  end;
end;

procedure TQueryFrm.SelectButtonClick(Sender: TObject);
begin
  SelectOneItemByKey(FAllListHelper.SelectedItemKey);
end;

procedure TQueryFrm.AddProfiles(Interested: boolean);
var
  MainList: TKUSerList;
  SrcUP:TKUserProfile;
  DstUP:TKUserProfile;
  Selector: boolean;
begin
  MainList := DBCont.Datastore.GetTemporaryTopLevelList;
  try
    SrcUP := MainList.AdjacentBySortVal(katFirst, ksvPresentationOrder, nil) as TKUserProfile;
    while Assigned(SrcUP) and (ProfileVorI(SrcUP)) do
    begin
      if Interested then
        Selector := SrcUP.InterestLevel > kpiFetchUserForRefs
      else
        Selector := ProfileV(SrcUP);
      if Selector then
      begin
        DstUP := FAddedUsers.SearchByInternalKeyOnly(SrcUP.Key) as TKUSerProfile;
        if not Assigned(DstUP) then
        begin
          DstUP := TKKeyedObject.Clone(SrcUP) as TKUserProfile;
          FAddedUsers.Add(DstUP);
        end;
      end;
      SrcUP := MainList.AdjacentBySortVal(katNext, ksvPresentationOrder, SrcUP) as TKUserProfile;
    end;
    FAddedListHelper.ReloadList(FAddedUsers);
  finally
    DBCont.Datastore.PutTopLevelList(MainList);
  end;
end;

procedure TQueryFrm.AddVerifiedBtnClick(Sender: TObject);
begin
  AddProfiles(false);
end;

procedure TQueryFrm.AddInterestedBtnClick(Sender: TObject);
begin
  AddProfiles(true);
end;

procedure TQueryFrm.UnselectButtonClick(Sender: TObject);
var
  DelUP: TKUSerProfile;
begin
  if FAddedListHelper.SelectedItemKey <> TGuid.Empty then
  begin
    DelUP := FAddedUsers.SearchByInternalKeyOnly(FAddedListHelper.SelectedItemKey) as TKUserProfile;
    if Assigned(DelUP) then
    begin
      FAddedUSers.Remove(DelUP);
      DelUP.Free;
    end;
    FAddedListHelper.SelectedItemKey := TGuid.Empty;
    FAddedListHelper.ReloadList(FAddedUsers);
  end
end;

procedure TQueryFrm.ClearBtnClick(Sender: TObject);
begin
  FAddedUsers.DeleteChildren;
  FAddedListHelper.ReloadList(FAddedUsers);
end;

{
  if Assigned(DBCont) and Assigned(DBCont.Datastore) then
  begin
    MainList := DBCont.Datastore.GetTemporaryTopLevelList;
    try
      if Length(SearchStr) > 0 then
        FAllListHelper.ReloadWithFilter(MainList, SearchStr)
      else
        FAllListHelper.ReloadList(MainList);
    finally
      DBCont.Datastore.PutTopLevelList(MainList);
    end;
  end
  else
    FAllListHelper.ReloadList(nil);
}
procedure TQueryFrm.HandleImageLoaded(Sender: TObject; var Data: TStream;
  Ref1: TObject; Ref2: string; OK: boolean);
var
  WorkingList: TKUserList;
begin
  if Ref1 = FAllListHelper.ListView then
    FAllListHelper.HandleImageLoaded(Sender, Data, Ref1, Ref2, OK)
  else if Ref1 = FAddedListHelper.ListView then
    FAddedListHelper.HandleImageLoaded(Sender, Data, Ref1, Ref2, OK)
  else
    exit;
  if not OK then
  begin
    if Ref1 = FAddedListHelper.ListView then
      WorkingList := FAddedUsers
    else
    begin
      if Assigned(DBCont) and Assigned(DBCont.Datastore) then
        WorkingList := DBCont.Datastore.GetTemporaryTopLevelList
      else
        exit;
    end;
    try
      GuiMisc.ImageLoadFailed(Ref2, klUserList, WorkingList);
    finally
      if Ref1 <> FAddedListHelper.ListView then
        DBCont.Datastore.PutTopLevelList(WorkingList);
    end;
  end;
end;

procedure TQueryFrm.LoadMoreBtnClick(Sender: TObject);
var
  MainList: TKUserList;
begin
  FAllListHelper.HandleLoadMoreClicked(Sender);
  MainList := DBCont.Datastore.GetTemporaryTopLevelList;
  try
    FAllListHelper.ReloadList(MainList);
  finally
    DBCont.Datastore.PutTopLevelList(MainList);
  end;
end;

end.
