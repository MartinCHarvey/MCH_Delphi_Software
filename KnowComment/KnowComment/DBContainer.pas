unit DBContainer;
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
  MemDBInit, Datastore, BatchLoader, Classes
{$IFDEF USE_TRACKABLES}
  , Trackables
{$ENDIF}
  ;

type
{$IFDEF USE_TRACKABLES}
  TDBContainer = class(TTrackable)
{$ELSE}
  TDBContainer = class
{$ENDIF}
  private
    FDB: TMemKDB;
    FAsyncLoad: boolean;
    FDatastore: TLoaderDataStore;
    FBatchLoader: TBatchLoader;
    FOnDBLoaded: TNotifyEvent;
    FOnDBUnavailable: TNotifyEvent;
    PrevLoaded: boolean;
  protected
    procedure HandleDBStateChange(Sender: TObject);
  public
    property DB: TMemKDB read FDB;
    property Datastore: TLoaderDataStore read FDatastore;
    property BatchLoader: TBatchLoader read FBatchLoader;
    constructor Create;
    destructor Destroy; override;
    //Do asynchronous load.
    function InitDB(RootLocation: string;
                    BinaryOutput: boolean; Async: boolean = true): boolean;

    function HandleUserActionUpdate(Sender: TObject; const GuidKey: TGuid): boolean;
    procedure HandleUserActionExecute(Sender: TObject; const GuidKey: TGuid);
    function HandleDBActionUpdate(Sender: TObject): boolean;
    procedure HandleDBActionExecute(Sender: TObject);

    property OnDBLoaded: TNotifyEvent read FOnDBLoaded write FOnDbLoaded;
    property OnDBUnavailable: TNotifyEvent read FOnDBUnavailable write FOnDBUnavailable;
  end;

var
  DBCont: TDBContainer;

implementation

uses
  MemDB, FMX.ActnList, MainForm, DataObjects, MemDBMisc, UserPrefs;

procedure TDBContainer.HandleDBStateChange(Sender: TObject);
var
  Loaded: boolean;
  Error: boolean;
begin
  Loaded := FDB.MemDB.DBState = mdbRunning;
  Error := FDB.MemDB.DBState = mdbError;
  if (Loaded <> PrevLoaded) or Error then
  begin
    PrevLoaded := Loaded;
    if Loaded and not Error then
    begin
      if FAsyncLoad then
        Error := not FDB.InitAsyncCompletion;
    end;
    if Loaded and not Error then
    begin
      if Assigned(FOnDBLoaded) then
        FOnDBLoaded(self);
    end
    else
    begin
      if Assigned(FOnDBUnavailable) then
        FOnDBUnavailable(self);
    end;
  end;
end;

function TDBContainer.InitDB(RootLocation: string;
                             BinaryOutput: boolean; Async: boolean): boolean;
var
  Msg:string;
begin
  FDB.DBPath := RootLocation;
  FAsyncLoad := Async;
  if Async then
  begin
    FDB.InitAsync;
    result := true;
  end
  else
  begin
    result := FDB.InitDBAndTables(Msg);
  end;
end;


constructor TDBContainer.Create;
begin
  inherited;
  FDB := TMemKDB.Create;
  FDB.MemDB.OnUIStateChange := HandleDBStateChange;
  FDatastore := TLoaderDataStore.Create;
  FDatastore.DB := FDB;
  FBatchLoader := TBatchLoader.Create;
  FBatchLoader.DataStore := FDatastore;
  FDatastore.OnOpCompletion := FBatchLoader.HandleDataStoreCompletion;
end;

destructor TDBContainer.Destroy;
begin
  FBatchLoader.Free;
  FDatastore.Free;
  FDB.Free;
  inherited;
end;

function TDBContainer.HandleUserActionUpdate(Sender: TObject; const GuidKey: TGuid): boolean;
begin
  if Assigned(Sender) then
  begin
    if Sender = MainFrm.RefreshUser then
      result := FBatchLoader.CanRefreshUser(GuidKey)
    else if Sender = MainFrm.IncreaseUserInterest then
      result := FBatchLoader.CanChangeInterestLevel(GuidKey, kpiTreeForInfo)
    else if Sender = MainFrm.DecreaseUserInterest then
      result := FBatchLoader.CanChangeInterestLevel(GuidKey, kpiFetchUserForRefs)
    else if Sender = MainFrm.PurgeUserComments then
      result := FBatchLoader.CanPurgeUser(klCommentList, GuidKey)
    else if Sender = MainFrm.PurgeUserMedia then
      result := FBatchLoader.CanPurgeUser(klMediaList, GuidKey)
    else if Sender = MainFrm.DeleteUser then
      result := FBatchLoader.CanPurgeUser(klUserList, GuidKey)
    else if Sender = MainFrm.ExpireUser then
      result := FBatchLoader.CanExpireUser(CurSessUserPrefs.ExpireBefore,
                                           TDBExpiryType.dbxDate,
                                           CurSessUserPrefs.ExpireLevels,
                                           GuidKey)
    else
    begin
      result := false;
      Assert(false);
    end;
  end
  else
    result := false;
end;

procedure TDBContainer.HandleUserActionExecute(Sender: TObject; const GuidKey: TGuid);
begin
  if Assigned(Sender) then
  begin
    if Sender = MainFrm.RefreshUser then
      FBatchLoader.RefreshUsers(CurSessUserPrefs.LoadSince, GuidKey)
    else if Sender = MainFrm.IncreaseUserInterest then
      FBatchLoader.ChangeUserInterestLevel(GuidKey, kpiTreeForInfo, CurSessUserPrefs.LoadSince)
    else if Sender = MainFrm.DecreaseUserInterest then
      FBatchLoader.ChangeUserInterestLevel(GuidKey, kpiFetchUserForRefs, CurSessUserPrefs.LoadSince)
    else if Sender = MainFrm.PurgeUserComments then
      FBatchLoader.PurgeUser(klCommentList, dtMemAndDb, GuidKey)
    else if Sender = MainFrm.PurgeUserMedia then
      FBatchLoader.PurgeUser(klMediaList, dtMemAndDb, GuidKey)
    else if Sender = MainFrm.DeleteUser then
      FBatchLoader.PurgeUser(klUserList, dtMemAndDb, GuidKey)
    else if Sender = MainFrm.ExpireUser then
      FBatchLoader.ExpireUser(CurSessUserPrefs.ExpireBefore, dbxDate,
        CurSessUserPrefs.ExpireLevels, GuidKey)
    else
      Assert(false);
  end
end;

function TDBContainer.HandleDBActionUpdate(Sender: TObject): boolean;
begin
  if Assigned(Sender) then
  begin
    result := (DB.MemDB.DBState = mdbRunning);
  end
  else
    result := false;
end;

procedure TDBContainer.HandleDBActionExecute(Sender: TObject);
begin
  if Sender = MainFrm.RefreshAll then
    FBatchLoader.RefreshUsers(CurSessUserPrefs.LoadSince, TGuid.Empty)
  else if Sender = MainFrm.ExpireAll then
    FBatchLoader.ExpireUser(CurSessUserPrefs.ExpireBefore, dbxDate,
        CurSessUserPrefs.ExpireLevels, TGuid.Empty);
end;

initialization
  DBCont := TDBContainer.Create;
finalization
  DBCont.Free;
end.
