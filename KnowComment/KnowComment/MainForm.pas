unit MainForm;
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
  FMX.StdCtrls, FMX.Layouts, System.Actions, FMX.ActnList, FMX.Menus,
  DBContainer, FMX.StdActns, BatchLoader, ImageCache, MemDB, FMX.Edit;

type
  TMainFrm = class(TForm)
    MainMenu1: TMainMenu;
    MasterActions: TActionList;
    LeftToolBar: TToolBar;
    ClientLayout: TLayout;
    LoadingLbl: TLabel;
    FileMenu: TMenuItem;
    ExitMenuItem: TMenuItem;
    Exit: TAction;
    RefreshAll: TAction;
    RefreshUser: TAction;
    IncreaseUserInterest: TAction;
    DecreaseUserInterest: TAction;
    PurgeUserComments: TAction;
    PurgeUserMedia: TAction;
    DeleteUser: TAction;
    MenuItem1: TMenuItem;
    MenuItem2: TMenuItem;
    MenuItem3: TMenuItem;
    MenuItem4: TMenuItem;
    MenuItem5: TMenuItem;
    MenuItem6: TMenuItem;
    MenuItem7: TMenuItem;
    MenuItem8: TMenuItem;
    MenuItem9: TMenuItem;
    TopLayout: TLayout;
    RightToolBar: TToolBar;
    StopOrQuery: TAction;
    MenuItem10: TMenuItem;
    RefreshImageCtl: TImageControl;
    StopQueryImageCtl: TImageControl;
    RefreshUserImageCtl: TImageControl;
    IncUserInterestImgCtl: TImageControl;
    DecUserInterestImageCtl: TImageControl;
    DeleteUserImageCtrl: TImageControl;
    SpacerLayout: TLayout;
    Filter: TMenuItem;
    MenuItem11: TMenuItem;
    CreateFilter: TAction;
    FilterImageCtl: TImageControl;
    ClearFilter: TAction;
    MenuItem12: TMenuItem;
    UnfilterImageCtl: TImageControl;
    QuickSearchLayout: TLayout;
    QuickSearchEdit: TEdit;
    QuickSearchLabel: TLabel;
    ExpireUser: TAction;
    ExpireAll: TAction;
    MenuItem13: TMenuItem;
    MenuItem14: TMenuItem;
    AddNewUser: TAction;
    MenuItem15: TMenuItem;
    ImageControl1: TImageControl;
    ShowPrefs: TAction;
    MenuItem16: TMenuItem;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure ExitExecute(Sender: TObject);
    procedure FormCloseQuery(Sender: TObject; var CanClose: Boolean);
    procedure StopOrQueryExecute(Sender: TObject);
    procedure CreateFilterExecute(Sender: TObject);
    procedure CreateFilterUpdate(Sender: TObject);
    procedure ClearFilterExecute(Sender: TObject);
    procedure ClearFilterUpdate(Sender: TObject);
    procedure QuickSearchEditChange(Sender: TObject);
    procedure QuickSearchEditKeyUp(Sender: TObject; var Key: Word;
      var KeyChar: Char; Shift: TShiftState);
    procedure AddNewUserExecute(Sender: TObject);
    procedure AddNewUserUpdate(Sender: TObject);
    procedure ShowPrefsExecute(Sender: TObject);
  private
    { Private declarations }
    FClosePending: boolean;
    procedure HandleDBLoaded(Sender: TObject);
    procedure HandleDBUnavailable(Sender: TObject);
    procedure HandleBatchLoaderCompletion(Sender: TObject; MultiOp: TMultiOp);
    procedure HandleImageLoaded(Sender: TObject;
                                var Data: TStream;
                                Ref1: TObject;
                                Ref2: string;
                                OK: boolean);
    procedure HandleAppIdle(Sender: TObject; var Done: boolean);
    procedure InitActionHandlers;
    procedure HandleUserActionUpdate(Sender: TObject);
    procedure HandleUserActionExecute(Sender: TObject);
    procedure HandleDBActionUpdate(Sender: TObject);
    procedure HandleDBActionExecute(Sender: TObject);
    function CheckCanClose: boolean;
    procedure CloseIfPending;
    procedure PeriodicHandler;
  public
    { Public declarations }
    FClientForm: TForm;
    procedure BroadcastFocusChange(Sender: TObject);
  end;

var
  MainFrm: TMainFrm;

implementation

{$R *.fmx}

uses
{$IFOPT C+}
  Windows,
{$ENDIF}
  ClientForm, DataObjects, FetcherParser, StatsForm, IOUtils, GlobalLog,
  QueryForm, MemDBMisc, UserPrefs, AddNewUserForm, PrefsEditForm;

const
  S_DB_UNAVAILABLE = 'Database unavailable (failed load). Have you checked the default DB location?';
  S_USER_ACTIONS = 'UserActions';
  S_DB_AVAILABLE_ACTIONS = 'DBAvailableActions';

procedure TMainFrm.PeriodicHandler;
begin
  if Assigned(ImgCache) then
    ImgCache.AgeCache;
  if Assigned(GFetcherParser) then
    GFetcherParser.AgeCache;
end;


procedure TMainFrm.QuickSearchEditChange(Sender: TObject);
begin
  if Assigned(FClientForm) then
    (FClientForm as TClientFrm).QuickFilterStr := QuickSearchEdit.Text.Trim;
end;

procedure TMainFrm.QuickSearchEditKeyUp(Sender: TObject; var Key: Word;
  var KeyChar: Char; Shift: TShiftState);
begin
  QuickSearchEditChange(Sender);
end;

function TMainFrm.CheckCanClose: boolean;
var
  r1, r2: boolean;
begin
  //First off, check underlying DB not in transient state.
  result := DBCont.DB.MemDB.DBState in
   [mdbNull, mdbRunning, mdbClosed, mdbError];
  //Stop everything in parallel as far as we can.
  if result then
  begin
    r1 := DBCont.BatchLoader.Stop;
    r2 := ImgCache.Stop;
    result := r1 and r2;
    if Assigned(GFetcherParser) then
    begin
     GFetcherParser.Stop;
     if not result then GFetcherParser.CancelOutstandingFetches;
    end;
  end;
end;


procedure TMainFrm.CloseIfPending;
begin
  PeriodicHandler;
  if FClosePending then
  begin
    if CheckCanClose then
      Close;
  end
  else
    DBCont.BatchLoader.ConditionalUnstop;
end;

procedure TMainFrm.ExitExecute(Sender: TObject);
begin
  Close;
end;

procedure TMainFrm.FormCloseQuery(Sender: TObject; var CanClose: Boolean);
begin
  FClosePending := true;
  CanClose := CheckCanClose;
end;

procedure TMainFrm.FormCreate(Sender: TObject);
begin
  DBCont.OnDBLoaded := HandleDBLoaded;
  DBCont.OnDBUnavailable := HandleDBUnavailable;
  DBCont.InitDB(CurSessUserPrefs.DataRootDir, true);
  DBCont.BatchLoader.OnMultiOpCompleted := HandleBatchLoaderCompletion;
  ImgCache.OnImageLoaded := HandleImageLoaded;
  ImgCache.DB := DBCont.DB.MemDB;
  InitActionHandlers;
end;

procedure TMainFrm.FormDestroy(Sender: TObject);
begin
  DBCont.OnDBLoaded := nil;
  DBCont.OnDBUnavailable := nil;
  DBCont.BatchLoader.OnMultiOpCompleted := nil;
  ImgCache.OnImageLoaded := nil;
  Application.OnIdle := nil;
end;

procedure TMainFrm.HandleBatchLoaderCompletion(Sender: TObject; MultiOp: TMultiOp);
begin
  StatsFrm.HandleBatchLoaderCompletion(Sender, MultiOp);
  if MultiOp.OpType = TMultiOpType.motCustomQuery then
  begin
    if MultiOp.OK then
    begin
      if Assigned(FClientForm) and not (FClientForm as TClientFrm).Filtered then
      begin
        (FClientForm as TClientFrm).ApplyFilter((MultiOp as TCustomQueryMultiOp).Result);
        (MultiOp as TCustomQueryMultiOp).Result := nil;
      end;
    end;
  end
  else
  begin
    if Assigned(FClientForm) then
      (FClientForm as TClientFrm).HandleBatchLoaderCompletion(Sender, MultiOp);
  end;
  CloseIfPending;
end;

procedure TMainFrm.HandleAppIdle(Sender: TObject; var Done: boolean);
var
  LDone: boolean;
begin
  Done := true;
  if Assigned(FCLientForm) then
  begin
    LDone := false;
    (FClientForm as TClientFrm).DelayedImageLoad(Sender, LDone);
    Done := Done and LDone;
  end;
  if Assigned(QueryFrm) then
  begin
    LDone := false;
    QueryFrm.DelayedImageLoad(Sender, LDone);
    Done := Done and LDone;
  end;
end;

procedure TMainFrm.HandleDBLoaded(Sender: TObject);
var
  CF: TClientFrm;
begin
  ImgCache.Init(CurSessUserPrefs.DataRootDir);
  ImgCache.ExpireOldImages(CurSessUserPrefs.ImageExpireBefore);
  Application.OnIdle := HandleAppIdle;
  if not Assigned(FClientForm) then
  begin
    FClientForm := TClientFrm.Create(self);
  end;
  CF := FClientForm as TClientFrm;
  CF.FormType := cftList;
  CF.ListLevel := klUserList;
  CF.DetailView := false;
  CF.MasterLayout.Parent := ClientLayout;
  LoadingLbl.Visible := false;
  if not DBCont.BatchLoader.LoadUserList then
    HandleDBUnavailable(self);
  CloseIfPending;
end;

procedure TMainFrm.HandleDBUnavailable(Sender: TObject);
begin
  LoadingLbl.Visible := true;
  Application.OnIdle := nil;
  FreeAndNil(FClientForm);
  LoadingLbl.Text := S_DB_UNAVAILABLE;
  CloseIfPending;
end;

procedure TMainFrm.HandleImageLoaded(Sender: TObject;
                                      var Data: TStream;
                                      Ref1: TObject;
                                      Ref2: string;
                                      OK: boolean);
begin
  if Assigned(FClientForm) then
    (FClientForm as TClientFrm).HandleImageLoaded(Sender, Data, Ref1, Ref2, OK);
  if Assigned(QueryFrm) then
    QueryFrm.HandleImageLoaded(Sender, Data, Ref1, Ref2, OK);

  CloseIfPending;
end;

procedure TMainFrm.AddNewUserExecute(Sender: TObject);
begin
  AddNewUserFrm.ShowModal;
end;

procedure TMainFrm.AddNewUserUpdate(Sender: TObject);
var
  Enabled: boolean;
begin
  Enabled := DBCont.DB.MemDB.DBState = mdbRunning;
  if Enabled <> (Sender as TAction).Enabled then
    (Sender as TAction).Enabled := Enabled;
end;

procedure TMainFrm.BroadcastFocusChange(Sender: TObject);
begin
  if Assigned(FClientForm) then
    (FClientForm as TClientFrm).HandleFocusTaken(Sender);
end;

procedure TMainFrm.InitActionHandlers;
var
  Idx: integer;
  Action: TAction;
begin
  for idx := 0 to Pred(MasterActions.ActionCount) do
  begin
    Action := MasterActions.Actions[idx] as TAction;
    if (Action.Category = S_USER_ACTIONS) then
    begin
      Action.OnUpdate := HandleUserActionUpdate;
      Action.OnExecute := HandleUserActionExecute;
    end
    else if (Action.Category = S_DB_AVAILABLE_ACTIONS) then
    begin
      if not Assigned(Action.OnUpdate) then
        Action.OnUpdate := HandleDBActionUpdate;
      if not Assigned(Action.OnExecute) then
        Action.OnExecute := HandleDBActionExecute;
    end;
  end;
end;


procedure TMainFrm.ShowPrefsExecute(Sender: TObject);
begin
  PrefsEditFrm.ShowModal;
end;

procedure TMainFrm.StopOrQueryExecute(Sender: TObject);
begin
  StatsFrm.ShowModal;
end;

procedure TMainFrm.CreateFilterExecute(Sender: TObject);
begin
  if Assigned(FClientForm) and
    ((FClientForm as TCLientFrm).ListHelper.SelectedItemKey <> TGuid.Empty) then
  QueryFrm.InitialLoadKey := (FClientForm as TCLientFrm).ListHelper.SelectedItemKey;
  QueryFrm.ShowModal;
end;

procedure TMainFrm.CreateFilterUpdate(Sender: TObject);
var
  NewEnabled: boolean;
begin
  NewEnabled := Assigned(FClientForm)
    and not (FClientForm as TClientFrm).Filtered;
  if (Sender as TAction).Enabled <> NewEnabled then
    (Sender as TAction).Enabled := NewEnabled;
end;

procedure TMainFrm.ClearFilterExecute(Sender: TObject);
begin
  (FClientForm as TClientFrm).ClearFilter;
end;

procedure TMainFrm.ClearFilterUpdate(Sender: TObject);
var
  NewEnabled: boolean;
begin
  NewEnabled := Assigned(FClientForm)
    and (FClientForm as TClientFrm).Filtered;
  if (Sender as TAction).Enabled <> NewEnabled then
    (Sender as TAction).Enabled := NewEnabled;
end;


procedure TMainFrm.HandleUserActionUpdate(Sender: TObject);
var
  GuidKey: TGuid;
  Enable: boolean;
begin
  Enable := DBCont.HandleDBActionUpdate(Sender);
  if Enable then
  begin
    if Assigned(FClientForm) then
      GuidKey := (FClientForm as TClientFrm).GetFocussedItemGuid([klUserList, klCommentList])
    else
      GuidKey := TGuid.Empty;
    Enable := GuidKey <> TGuid.Empty;
  end;
  if Enable then
    Enable := DBCont.HandleUserActionUpdate(Sender, GuidKey);
  if Enable <> (Sender as TAction).Enabled then
    (Sender as TAction).Enabled := Enable;
end;

procedure TMainFrm.HandleUserActionExecute(Sender: TObject);
var
  GuidKey: TGuid;
begin
  if Assigned(FClientForm) then
  begin
    GuidKey := (FClientForm as TClientFrm).GetFocussedItemGuid([klUserList, klCommentList]);
    if GuidKey <> TGuid.Empty then
      DBCont.HandleUserActionExecute(Sender, GuidKey)
  end;
end;

procedure TMainFrm.HandleDBActionUpdate(Sender: TObject);
var
  Enable: boolean;
begin
  Enable := DBCont.HandleDBActionUpdate(Sender);
  if Enable <> (Sender as TAction).Enabled then
    (Sender as TAction).Enabled := Enable;
end;

procedure TMainFrm.HandleDBActionExecute(Sender: TObject);
begin
  DBCont.HandleDBActionExecute(Sender);
end;

initialization
  AppGlobalLog.Log(TLogSeverity.SV_INFO, 'Log file opened');
{$IFOPT C+}
  AppGlobalLog.OpenFileLog(TPath.GetTempPath + 'KnowCommentDebug.log');
{$ELSE}
  AppGlobalLog.OpenFileLog(TPath.GetTempPath + 'KnowComment.log');
{$ENDIF}
end.
