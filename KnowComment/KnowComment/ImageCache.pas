unit ImageCache;
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

{ Simple unit to retrieve (and later cache) images / thumbnails }

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  MemDB, Classes, HTTPDocFetcher, WorkItems, SyncObjs, CommonPool, DLList;

type
  TImageCacheWorkitem = class;

{$IFDEF USE_TRACKABLES}
  TImageCacheRequest = class(TTrackable)
{$ELSE}
  TImageCacheRequest = class
{$ENDIF}
  protected
    FLinkField: TDLEntry;
    URL: string;
    Ref1:TObject;
    Ref2: string;
    WI: TImageCacheWorkitem;
  public
    FData: TStream;
    OK: boolean;
    constructor Create;
    destructor Destroy; override;
  end;


  TImageLoadedEvent = procedure(Sender: TObject;
                                var Data: TStream; //Set to NULL if you want the stream.
                                Ref1: TObject;
                                Ref2: string;
                                OK: boolean) of object;
  //We don't run a state machine for this,
  //as it's all pretty simple and in the main thread.
{$IFDEF USE_TRACKABLES}
  TImageCache = class(TTrackable)
{$ELSE}
  TImageCache = class
{$ENDIF}
  private
    FLock: TCriticalSection;
    FFetcher: THTTPDocFetcher;
    FInit: boolean;
    FStopping: boolean;
    FDB: TMemDB;
    FRequests: TDLEntry;
    FCompletedNeedsSync: boolean;
    FCompletedRequests: TDLEntry;
    FOnImageLoaded: TImageLoadedEvent;
    FRootLocation: string;
    FPoolRec: TClientRec;
    FExpireRequests: integer;
  protected
    procedure HandleHTTPCompleted(Sender: TObject);
    procedure HandleHTTPAbandoned(Sender: TObject);
    procedure ClearRequests;

    procedure DBOpInThread(Workitem: TImageCacheWorkitem);
    procedure HandleDBNormalCompletion(Sender: TObject);
    procedure HandleDBCancelledCompletion(Sender: TObject);
    procedure HTTPFailurePath(DocItem: THTTPDocItem);
    procedure RequestToCompletedQueue(Req: TImageCacheRequest; OK: boolean; WorkerThread: boolean);
    procedure FlushCompletedRequestQueue;
  public
    constructor Create;
    destructor Destroy; override;
    procedure AgeCache;
    procedure Init(RootLocation: string);

    function RequestImage(URL: string; Ref1:TObject; Ref2: string): boolean;
    function ExpireOldImages(Before: TDateTime): boolean;
    function Stop: boolean;
    //Not yet, but soon ...
    property DB: TMemDB read FDB write FDB;
    property OnImageLoaded: TImageLoadedEvent read FOnImageLoaded write FOnImageLoaded;
  end;

  TImageCacheWorkitemTask = (ictReadLocally,
                             ictUpdateLocally,
                             ictBatchDelete);

  TImageCacheWorkitem = class(TCommonPoolWorkItem)
  private
    FRequest: TImageCacheRequest;
    FParentCache: TImageCache;
    FTask: TImageCacheWorkitemTask;
    FCompletedOK: boolean;
    FDeleteBefore: TDateTime;
  protected
    function DoWork: integer; override;
    procedure SyncCompleted;
    procedure SetTask(NewTask: TImageCacheWorkitemTask);
  public
    property Task: TImageCacheWorkitemTask read FTask write SetTask;
  end;

var
  ImgCache: TImageCache;

implementation

uses
  MemDBMisc, MemDBAPI, Windows, SysUtils;

const
  S_IMGCACHE_DIR = 'ImageCache\';
  S_IMAGE_CACHE = 'IMAGE_CACHE';

  S_URL_FIELD = 'URL';
  S_FILENAME_FIELD = 'FILENAME';
  S_DEPRECATED_LAST_MODIFIED_FIELD = 'LAST_MODIFIED';
  S_LAST_ACCESSED_FIELD = 'LAST_ACCESSED';

  //TODO - Prune these indices if we don't need them all.
  //Arguable whether we should refresh based on last modified.
  //At the moment, don't.
  S_URL_INDEX = 'URL_IDX';
  S_LAST_ACCESSED_INDEX = 'LAST_ACCESSED_IDX';
  S_FILENAME_INDEX = 'FILENAME_IDX';

{ TImageCacheRequest }

constructor TImageCacheRequest.Create;
begin
  inherited;
  DLItemInitObj(self, @FLinkField);
end;

destructor TImageCacheRequest.Destroy;
begin
  FData.Free;
  WI.Free;
  Assert(DlItemIsEmpty(@FLinkField));
  inherited;
end;

{ TImageCacheWorkitem }

function TImageCacheWorkitem.DoWork: integer;
begin
  FParentCache.DBOpInThread(self);
  result := 0;
end;


procedure TImageCacheWorkitem.SetTask(NewTask: TImageCacheWorkitemTask);
begin
  CanAutoFree := false;
  CanAutoReset := true;
  FTask := NewTask;
end;

procedure TImageCacheWorkitem.SyncCompleted;
var
  DoSync: boolean;
begin
  //Optimisation to prevent too many TThread.Queue.
  FParentCache.FLock.Acquire;
  try
    DoSync := FParentCache.FCompletedNeedsSync;
    FParentCache.FCompletedNeedsSync := false;
  finally
    FParentCache.FLock.Release;
  end;
  if DoSync then
    ThreadRec.Thread.Queue(FParentCache.FlushCompletedRequestQueue);
end;

{ TImageCache }

procedure TImageCache.Init(RootLocation: string);
var
  Sess: TMemDBSession;
  Trans: TMemDBTransaction;
  DBAPI: TMemAPIDatabase;
  TableMeta: TMemAPITableMetadata;
  TblHandle: TMemDBHandle;
  FieldNames: TStringList;
begin
  if not FInit then
  begin
    AppendTrailingDirSlash(RootLocation);
    FRootLocation := RootLocation + S_IMGCACHE_DIR;
    if not CreateDirectoryW(@FRootLocation[1], nil) then
    begin
      if not GetLastError = ERROR_ALREADY_EXISTS then
      begin
        FInit := false;
        exit;
      end;
    end;

    Sess := FDB.StartSession;
    try
      Trans := Sess.StartTransaction(amReadWrite);
      try
        DBAPI := Trans.GetAPI as TMemAPIDatabase;
        try
          TblHandle := DBAPI.OpenTableOrKey(S_IMAGE_CACHE);
          if not Assigned(TblHandle) then
          begin
            TblHandle := DBAPI.CreateTable(S_IMAGE_CACHE);
            TableMeta := DBAPI.GetApiObjectFromHandle(TblHandle, APITableMetadata) as TMemAPITableMetadata;
            try
              TableMeta.CreateField(S_URL_FIELD, ftUnicodeString);
              TableMeta.CreateField(S_FILENAME_FIELD, ftUnicodeString);
              TableMeta.CreateField(S_LAST_ACCESSED_FIELD, ftDouble);
              TableMeta.CreateIndex(S_URL_INDEX, S_URL_FIELD, [iaUnique, iaNotEmpty]);
              TableMeta.CreateIndex(S_LAST_ACCESSED_INDEX, S_LAST_ACCESSED_FIELD, []);
              TableMeta.CreateIndex(S_FILENAME_INDEX, S_FILENAME_FIELD, [iaUnique, iaNotEmpty]);
            finally
              TableMeta.Free;
            end;
          end
          else
          begin
            //TODO - Remove this code once datasets updated.
            TblHandle := DBAPI.OpenTableOrKey(S_IMAGE_CACHE);
            TableMeta := DBAPI.GetApiObjectFromHandle(TblHandle, APITableMetadata) as TMemAPITableMetadata;
            try
              FieldNames := TableMeta.GetFieldNames;
              try
                if FieldNames.IndexOf(S_DEPRECATED_LAST_MODIFIED_FIELD) >= 0 then
                begin
                  TableMeta.DeleteField(S_DEPRECATED_LAST_MODIFIED_FIELD);
                end;
              finally
                FieldNames.Free;
              end;
            finally
              TableMeta.Free;
            end;
          end;
        finally
          DBAPI.Free;
        end;
        Trans.CommitAndFree;
        FInit := true;
      except
        Trans.RollbackAndFree;
        FInit := false;
      end;
    finally
      Sess.Free;
    end;
  end;
end;

procedure TImageCache.ClearRequests;
var
  PEntry: PDLEntry;
  Req: TImageCacheRequest;
begin
  PEntry := DLListRemoveHead(@FRequests);
  while Assigned(PEntry) do
  begin
    Req := PEntry.Owner as TImageCacheRequest;
    Req.Free;
    PEntry := DLListRemoveHead(@FRequests)
  end;

  PEntry := DLListRemoveHead(@FCompletedRequests);
  while Assigned(PEntry) do
  begin
    Req := PEntry.Owner as TImageCacheRequest;
    Req.Free;
    PEntry := DLListRemoveHead(@FCompletedRequests)
  end;
  FCompletedNeedsSync := false;
end;

procedure TImageCache.FlushCompletedRequestQueue;
var
  RQ: PDLEntry;
  Req: TImageCacheRequest;
begin
  FLock.Acquire;
  try
    RQ := DLListRemoveHead(@FCompletedRequests);
    if not Assigned(RQ) then
      FCompletedNeedsSync := false;
  finally
    FLock.Release;
  end;
  while Assigned(RQ) do
  begin
    Req := RQ.Owner as TImageCacheRequest;
    if Assigned(FOnImageLoaded) then
      FOnImageLoaded(self, Req.FData, Req.Ref1, Req.Ref2, Req.OK);
    Req.Free;
    FLock.Acquire;
    try
      RQ := DLListRemoveHead(@FCompletedRequests);
      if not Assigned(RQ) then
        FCompletedNeedsSync := false;
    finally
      FLock.Release;
    end;
  end;
end;

procedure TImageCache.DBOpInThread(Workitem: TImageCacheWorkitem);

  function CleanUnreferenced: boolean;
  var
    FilenameList: TStringList;
    IndexList: TStringList;
    FindHandle: THandle;
    FindData: TWin32FindDataW;
    SearchStr, ReGuided, FName: string;
    Idx: integer;
    S: TMemDBSession;
    Trans: TMemDBTransaction;
    DBAPI: TMemAPIDatabase;
    TableData: TMemAPITableData;
    TableMeta: TMemAPITableMetadata;
    TblHandle: TMemDBHandle;
    Changed, DbgRet: boolean;
    SearchData: TMemDbFieldDataRec;
  begin
    result := false;
    //Sometimes for some odd reason, not all the files get deleted.
    //This is a final clean of files that couldn't be deleted earlier
    //for some bizaare unknown reason.
    FilenameList := TStringList.Create;
    try
      //First of all, find all the files in the directory.
      SearchStr := FRootLocation + '*';
      FindHandle := FindFirstFileW(@SearchStr[1], FindData);
      while FindHandle <> INVALID_HANDLE_VALUE do
      begin
        //Filter out things which are not regular files.
        if (FindData.dwFileAttributes and (FILE_ATTRIBUTE_SYSTEM or
          FILE_ATTRIBUTE_DIRECTORY or FILE_ATTRIBUTE_DEVICE)) = 0 then
        begin
          FilenameList.Add(FindData.cFileName);
        end;
        if not FindNextFileW(FindHandle, FindData) then
          FindHandle := INVALID_HANDLE_VALUE;
      end;
      //Now remove the ones that aren't valid guid-like filenames.
      Idx := 0;
      while Idx < FileNameList.Count do
      begin
        ReGuided := '{' + FilenameList.Strings[Idx] + '}';
        try
          StringToGuid(ReGuided);
          Inc(Idx);
        except
          on EConvertError do FileNameList.Delete(Idx);
        end;
      end;
      //Now remove the ones that are mentioned in the DB.


      S := FDB.StartSession;
      try
        //Do we have a disk filename index? Create one if we don't.
        Changed := false;
        Trans := S.StartTransaction(amReadWrite);
        try
          DBAPI := Trans.GetAPI;
          try
            TblHandle := DBAPI.OpenTableOrKey(S_IMAGE_CACHE);
            TableMeta := DBAPI.GetApiObjectFromHandle(TblHandle, APITableMetadata) as TMemAPITableMetadata;
            try
              IndexList := TableMeta.GetIndexNames;
              try
                if IndexList.IndexOf(S_FILENAME_INDEX) < 0 then
                begin
                  TableMeta.CreateIndex(S_FILENAME_INDEX, S_FILENAME_FIELD, [iaUnique, iaNotEmpty]);
                  Changed := true;
                end;
              finally
                IndexList.Free;
              end;
            finally
              TableMeta.Free;
            end;
          finally
            DBAPI.Free;
          end;

          if Changed then
            Trans.CommitAndFree
          else
            Trans.RollbackAndFree;
        except
          on Exception do
          begin
            Trans.RollbackAndFree;
            raise;
          end;
        end;

        //Cool, now prune filenames from the list that are in the DB.
        Trans := S.StartTransaction(amRead);
        try
          DBAPI := Trans.GetAPI;
          try
            TblHandle := DBAPI.OpenTableOrKey(S_IMAGE_CACHE);
            TableData := DBAPI.GetApiObjectFromHandle(TblHandle, APITableData) as TMemAPITableData;
            try
              Idx := 0;
              SearchData.FieldType := ftUnicodeString;
              while Idx < FilenameList.Count do
              begin
                SearchData.sVal := FilenameList.Strings[idx];
                if TableData.FindByIndex(S_FILENAME_INDEX, SearchData) then
                  FileNameList.Delete(Idx)
                else
                  Inc(Idx);
              end;
            finally
              TableData.Free;
            end;
          finally
            DBAPI.Free;
          end;
        finally
          Trans.RollbackAndFree;
        end;
      finally
        S.Free;
      end;

      //OK, and now try to do the delete operation on all files in the filename list.
      for Idx := 0 to Pred(FilenameList.Count) do
      begin
        FName := FRootLocation + FilenameList[Idx];
        DbgRet := DeleteFile(FName);
        Assert(DbgRet);
      end;
      result := true;
    finally
      FilenameList.Free;
    end;
  end;

  function BatchDelete: boolean;

  const
    BATCH_SIZE = 128;

  var
    FilenameList: TStringList;
    Sess: TMemDBSession;
    Trans: TMemDBTransaction;
    DBAPI: TMemAPIDatabase;
    TableData: TMemAPITableData;
    TblHandle: TMemDBHandle;
    Data: TMemDbFieldDataRec;
    BatchCounter: integer;
    RowValid: boolean;
    Filename: string;
    DbgRet: boolean;
  begin
    result := true;
    try
      try
        FilenameList := TStringList.Create;
        try
          repeat
            if FStopping then
              Workitem.Cancel
            else
            begin
              FilenameList.Clear;

              //Do in batches.
              Sess := FDB.StartSession;
              try
                Trans := Sess.StartTransaction(amReadWrite, amLazyWrite, ilCommittedRead);
                try
                  DBAPI := Trans.GetAPI;
                  try
                    TblHandle := DBAPI.OpenTableOrKey(S_IMAGE_CACHE);
                    if Assigned(TblHandle) then
                    begin
                      TableData := DBAPI.GetApiObjectFromHandle(TblHandle, APITableData) as TMemAPITableData;
                      try
                        //Fingers crossed, ptFirst is *oldest*
                        BatchCounter := BATCH_SIZE;
                        RowValid := TableData.Locate(ptFirst, S_LAST_ACCESSED_INDEX);
                        while RowValid and (BatchCounter > 0) do
                        begin
                          TableData.ReadField(S_LAST_ACCESSED_FIELD, Data);
                          Assert(Data.FieldType = ftDouble);
                          if Data.dVal >= Workitem.FDeleteBefore then
                            RowValid := false
                          else
                          begin
                            TableData.ReadField(S_FILENAME_FIELD, Data);
                            Assert(Data.FieldType = ftUnicodeString);
                            FileNameList.Add(Data.sVal);
                            TableData.Delete;
                            Dec(BatchCounter);
                            RowValid := TableData.Locate(ptNext, S_LAST_ACCESSED_INDEX);
                          end;
                        end;
                      finally
                        TableData.Free;
                      end;
                    end
                    else
                      result := false;
                  finally
                    DBAPI.Free;
                  end;

                  Trans.CommitAndFree;
                except
                  Trans.RollbackAndFree;
                  result := false;
                  exit;
                end;
              finally
                Sess.Free;
              end;
              //OK, and now delete the files in the string list.

              for BatchCounter := 0 to Pred(FilenameList.Count) do
              begin
                Filename := FRootLocation + FilenameList[BatchCounter];
                DbgRet := DeleteFile(Filename);
                Assert(DbgRet);
              end;
            end;
          until Workitem.Cancelled or (FilenameList.Count = 0) or (not result);
        finally
          FilenameList.Free;
        end;
      except
        result := false;
      end;
    finally
      FLock.Acquire;
      try
        Dec(FExpireRequests);
      finally
        FLock.Release;
      end;
    end;
  end;

  function GetFilenameFromDB(var Filename:string): boolean;
  var
    Sess: TMemDBSession;
    Trans: TMemDBTransaction;
    DBAPI: TMemAPIDatabase;
    TableData: TMemAPITableData;
    TblHandle: TMemDBHandle;
    Data: TMemDbFieldDataRec;
  begin
    result := false;
    FillChar(Data, sizeof(Data), 0);
    Sess := FDB.StartSession;
    try
      Trans := Sess.StartTransaction(amReadWrite);
      try
        DBAPI := Trans.GetAPI as TMemAPIDatabase;
        try
          TblHandle := DBAPI.OpenTableOrKey(S_IMAGE_CACHE);
          if Assigned(TblHandle) then
          begin
            TableData := DBAPI.GetApiObjectFromHandle(TblHandle, APITableData) as TMemAPITableData;
            try
              Data.FieldType := ftUnicodeString;
              Data.sVal := Workitem.FRequest.URL;
              if TableData.FindByIndex(S_URL_INDEX, Data) then
              begin
                TableData.ReadField(S_FILENAME_FIELD, Data);
                Filename := Data.sVal;
                Data.FieldType := ftDouble;
                Data.dVal := Now;
                TableData.WriteField(S_LAST_ACCESSED_FIELD, Data);
                TableData.Post;
                result := true;
              end;
            finally
              TableData.Free;
            end;
          end;
        finally
          DBAPI.Free;
        end;
        Trans.CommitAndFree;
      except
        result := false;
        Trans.RollbackAndFree;
      end;
    finally
      Sess.Free;
    end;
  end;

  function ReadFileData(Filename:string): boolean;
  var
    FS: TFileStream;
    MS: TMemoryStream;
  begin
    result := false;
    Filename := FRootLocation + Filename;
    if Assigned(WorkItem.FRequest.FData) then
      FreeAndNil(WorkItem.FRequest.FData);
    //Think we can allow an expire thread to issue a file delete
    //call whilst this is open (wait for handle to close before delete
    //if share deny none.
    FS := TFileStream.Create(Filename, fmOpenRead, fmShareDenyNone);
    try
      MS := TMemoryStream.Create;
      MS.CopyFrom(FS, FS.Size);
      WorkItem.FRequest.FData := MS;
      result := true;
    finally
      FS.Free;
    end;
  end;

  function WriteFileData(var Filename: string): boolean;
  var
    LongName: string;
    FS: TFileStream;
    GUID: TGuid;
  begin
    result := false;
    //Invent a filename.
    CreateGuid(GUID);
    Filename := GUIDToString(GUID);
    //Top and tail the squirly brackets off the string.
    Filename := Filename.Substring(1, Length(Filename) - 2);
    LongName := FRootLocation + Filename;
    Assert(Assigned(WorkItem.FRequest.FData));
    FS := TFileStream.Create(LongName, fmCreate, fmShareExclusive);
    try
      WorkItem.FRequest.FData.Seek(0, soFromBeginning);
      FS.CopyFrom(WorkItem.FRequest.FData, WorkItem.FRequest.FData.Size);
      result := true;
    finally
      FS.Free;
    end;
  end;

  function WriteFilenameToDB(Filename:string): boolean;
  var
    Sess: TMemDBSession;
    Trans: TMemDBTransaction;
    DBAPI: TMemAPIDatabase;
    TableData: TMemAPITableData;
    TblHandle: TMemDBHandle;
    Data: TMemDbFieldDataRec;
  begin
    result := false;
    FillChar(Data, sizeof(Data), 0);
    Sess := FDB.StartSession;
    try
      Trans := Sess.StartTransaction(amReadWrite);
      try
        DBAPI := Trans.GetAPI as TMemAPIDatabase;
        try
          TblHandle := DBAPI.OpenTableOrKey(S_IMAGE_CACHE);
          if Assigned(TblHandle) then
          begin
            TableData := DBAPI.GetApiObjectFromHandle(TblHandle, APITableData) as TMemAPITableData;
            try
              Data.FieldType := ftUnicodeString;
              Data.sVal := Workitem.FRequest.URL;
              if not TableData.FindByIndex(S_URL_INDEX, Data) then
              begin
                TableData.Append;
                Data.FieldType := ftUnicodeString;
                Data.sVal := Workitem.FRequest.URL;
                TableData.WriteField(S_URL_FIELD, Data);
              end
              else
              begin
                //TODO - Warn here. For us to be reading a previous DB row,
                //must be that file open failed on previous good DB row,
                //user manually deleted files?
              end;
              Data.FieldType := ftUnicodeString;
              Data.sVal := FileName;
              TableData.WriteField(S_FILENAME_FIELD, Data);
              Data.FieldType := ftDouble;
              Data.dVal := Now;
              TableData.WriteField(S_LAST_ACCESSED_FIELD, Data);
              TableData.Post;
            finally
              TableData.Free;
            end;
          end;
        finally
          DBAPI.Free;
        end;
        Trans.CommitAndFree;
        result := true;
      except
        result := false;
        Trans.RollbackAndFree;
      end;
    finally
      Sess.Free;
    end;
  end;

var
  FN: string;

begin
  try
    case Workitem.FTask of
      ictReadLocally:
      begin
        Workitem.FCompletedOK :=
          GetFilenameFromDB(FN) and ReadFileData(FN);
      end;
      ictUpdateLocally:
        WorkItem.FCompletedOK :=
          WriteFileData(FN) and WriteFilenameToDB(FN);
      ictBatchDelete:
        WorkItem.FCompletedOK := BatchDelete and CleanUnreferenced;
    else
      Assert(false);
    end;
  except
    Workitem.FCompletedOK := false;
  end;
end;

procedure TImageCache.RequestToCompletedQueue(Req: TImageCacheRequest; OK: boolean; WorkerThread: boolean);
begin
  //These are some failed cases (DB writeback), where OK is false,
  //despite the fact we have loaded data. Deal with these conseratively,
  //free the data, so OK consistent with Assigned(Data).
  FLock.Acquire;
  try
    Req.OK := OK;
    if not OK then
      FreeAndNil(Req.FData);
    Assert(Assigned(Req.FData) = OK);
    DLListRemoveObj(@Req.FLinkField);
    DLListInsertTail(@FCompletedRequests, @Req.FLinkField);
    FCompletedNeedsSync := true;
  finally
    FLock.Release;
  end;
  //TODO - Remove cases where too many calls.
  if WorkerThread then
    Req.WI.SyncCompleted
  else
    FlushCompletedRequestQueue;
end;

procedure TImageCache.HandleDBNormalCompletion(Sender: TObject);
var
  Workitem: TImageCacheWorkitem;
  Req: TImageCacheRequest;
begin
  WorkItem := (Sender as TImageCacheWorkitem);
  case WorkItem.FTask of
    ictReadLocally:
    begin
      if Workitem.FCompletedOK then
        RequestToCompletedQueue(WorkItem.FRequest, true, true)
      else
      begin
        //OK, no joy getting data from DB / file locally.
        //Issue HTTP request for data.
        Req := WorkItem.FRequest;
        if not FFetcher.AddUrlToRetrieve(Req.URL, nil, nil, nil, fpmGet, nil, self, Req) then
          RequestToCompletedQueue(Req, false, true);
      end;
    end;
    ictUpdateLocally:
      RequestToCompletedQueue(Workitem.FRequest, Workitem.FCompletedOK, true);
    ictBatchDelete: ;
    else
      Assert(false);
  end;
end;

procedure TImageCache.HandleDBCancelledCompletion(Sender: TObject);
var
  Workitem: TImageCacheWorkitem;
begin
  WorkItem := (Sender as TImageCacheWorkitem);
  case WorkItem.FTask of
    ictReadLocally,
    ictUpdateLocally: RequestToCompletedQueue(WorkItem.FRequest, false, true);
    ictBatchDelete: ;
  else
    Assert(false);
  end;
end;

procedure TImageCache.HTTPFailurePath(DocItem: THTTPDocItem);
var
  Req: TImageCacheRequest;
begin
  Assert(DocItem.FPRef1 = self);
  Req := TImageCacheRequest(DocItem.FPRef2);
  RequestToCompletedQueue(Req, false, false);
  DocItem.Free;
end;

procedure TImageCache.HandleHTTPCompleted(Sender: TObject);
var
  DocItem: THTTPDocItem;
  Req: TImageCacheRequest;
  OK: boolean;
begin
  DocItem := FFetcher.GetRetrievedDoc;
  while Assigned(DocItem) do
  begin
    Assert(DocItem.FPRef1 = self);
    Req := TImageCacheRequest(DocItem.FPRef2);
    OK := (DocItem.FailReason = difNone) and (not DocItem.Cancelled);
    if OK then
    begin
      Req.FData := DocItem.Data;
      DocItem.Data := nil;
      //Write back to DB, and signal loaded once written back.
      Assert(Assigned(Req.FData));
      Req.WI.Task := ictUpdateLocally;
      Req.WI.FCompletedOK := false;
      GCommonPool.AddWorkItem(FPoolRec, Req.WI);
      DocItem.Free;
    end
    else
      HTTPFailurePath(DocItem);
    DocItem := FFetcher.GetRetrievedDoc;
  end;
end;

procedure TImageCache.HandleHTTPAbandoned(Sender: TObject);
var
  DocItem: THTTPDocItem;
begin
  //Main thread, can call completion handler directly.
  DocItem := FFetcher.GetAbandonedDoc;
  while Assigned(DocItem) do
  begin
    HTTPFailurePath(DocItem);
    DocItem := FFetcher.GetAbandonedDoc;
  end;
end;

constructor TImageCache.Create;
begin
  inherited;
  FFetcher := THTTPDocFetcher.Create;
  FFetcher.OnDocsCompleted := HandleHTTPCompleted;
  FFetcher.OnDocsAbandoned := HandleHTTPAbandoned;
  DLItemInitList(@FRequests);
  DLItemInitList(@FCompletedRequests);
  FLock := TCriticalSection.Create;
  FPoolRec := GCommonPool.RegisterClient(Self, HandleDBNormalCompletion, HandleDBCancelledCompletion);
end;

destructor TImageCache.Destroy;
begin
  GCommonPool.DeRegisterClient(FPoolRec);
  ClearRequests;
  FFetcher.Free;
  inherited;
end;

procedure TImageCache.AgeCache;
begin
  //Short term connection cache aging for HTTP.
  if Assigned(FFetcher) then
    FFetcher.AgeCache;
  //TODO - Longer term cache aging (persistent) independent of requests?
end;

function TImageCache.RequestImage(URL: string; Ref1:TObject; Ref2: string): boolean;
var
  Req: TImageCacheRequest;
  WI: TImageCacheWorkitem;
begin
  if Length(URL) = 0 then
  begin
    result := false;
    exit;
  end;

  FLock.Acquire;
  try
    result := FInit and (not FStopping);
    if result then
    begin
      Req := TImageCacheRequest.Create;
      Req.URL := URL;
      Req.Ref1 := Ref1;
      Req.Ref2 := Ref2;
      WI := TImageCacheWorkitem.Create;
      WI.FRequest := Req;
      WI.FParentCache := self;
      WI.FCompletedOK := false;
      WI.Task := ictReadLocally;
      Req.WI := WI;
      DLListInsertTail(@FRequests, @Req.FLinkField);
      result := GCommonPool.AddWorkItem(FPoolRec, WI);
      if not result then
      begin
        //TODO - This failure path untested.
        DLListRemoveObj(@Req.FLinkField);
        Req.WI := nil;
        WI.FRequest := nil;
        Req.Free;
        WI.Free;
      end;
    end;
  finally
    FLock.Release;
  end;
end;

function TImageCache.ExpireOldImages(Before: TDateTime): boolean;
var
  WI: TImageCacheWorkitem;
begin
  FLock.Acquire;
  try
    result := FInit and not FStopping;
    if result then
    begin
      WI := TImageCacheWorkitem.Create;
      WI.FParentCache := self;
      WI.FCompletedOK := false;
      WI.Task := ictBatchDelete;
      WI.FDeleteBefore := Before;
      WI.CanAutoFree := true; //Doesn't end up in any completed queue.
      result := GCommonPool.AddWorkItem(FPoolRec, WI);
      if result then
        Inc(FExpireRequests)
      else
        WI.Free;
    end;
  finally
    FLock.Release;
  end;
end;

function TImageCache.Stop: boolean;
begin
  FStopping := true;
  FFetcher.CancelOutstandingFetches;
  FLock.Acquire;
  try
    result := DlItemIsEmpty(@FRequests)
      and DLItemIsEmpty(@FCompletedRequests)
      and (FExpireRequests = 0);
      //TODO - but stop cycle not restarted when expire requests dec'd to zero ...
      //would need extra event to feed back into GUI logic.
  finally
    FLock.Release;
  end;
end;

initialization
  ImgCache := TImageCache.Create;
finalization
  ImgCache.Free;
end.
