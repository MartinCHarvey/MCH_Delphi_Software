unit HTTPServerDispatcher;

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

{
  This unit adds just a tiny bit more to the default Indy session handling
  and threading, specifically:

  1. Flow and load control: Indy threadpool just creates more threads at
     high load. We wish to limit the max load, by limiting the length
     of the request queue, thus implicitly throttling number of
     indy created threads.

  2. Thread affinity. Indy threadpool still processes all requests in
     parallel. We process requests one per threadpool thread (poss with affinity
     in future).

  3. Serialisation of requests per session: What happens if multiple requests
     on same (logon) session via different TCP connections?

  4. Some default handling for session timeouts, logoff etc.

  5. Handling for "flush work / wait etc" on threadpool: gracefully
     shut down server when no outstanding requests or sessions.

  6. RefCounting of requests being processed for each bit of session state.
}

uses
  GlobalLog, WorkItems, Trackables, IndexedStore, SyncObjs,
  IdHTTPServer, IdCustomHTTPServer, IdContext, IdGlobal, CommonPool,
  SysUtils;

const
  MAX_SIMUL_REQUESTS = 128;

type
  THTTPDispatcherSession = class;

{$IFDEF USE_TRACKABLES}
  TCustomPageProducer = class(TTrackable)
{$ELSE}
  TCustomPageProducer = class
{$ENDIF}
  private
    FSession: THTTPDispatcherSession;
  public
    constructor Create; virtual;
    destructor Destroy; override;
    property Session: THTTPDispatcherSession read FSession write FSession;
  end;

  TPageProducerClass = class of TCustomPageProducer;

  EPageProducer = class(Exception)
  end;

  THTTPRequestWorkItem = class(TCommonPoolWorkItem)
  private
    //TODO - Probably a more efficient way, but this will do.
    FCompletionEvent: TEvent;
    FDispatcherSession: THTTPDispatcherSession;
    FAContext: TIdContext;
    FARequestInfo: TIdHTTPRequestInfo;
    FAResponseInfo: TIdHTTPResponseInfo;
  protected
    function DoWork: integer; override;
  public
    constructor Create;
    destructor Destroy; override;
  end;

  THTTPServerDispatcher = class;

  TDispatcherIndexTag = (idtPointer, idtSessionString, idtLogonId);

{$IFDEF USE_TRACKABLES}
  THTTPLogonInfo = class(TTrackable)
{$ELSE}
  THTTPLogonInfo = class
{$ENDIF}
  private
    FLogonId: string;
    FRequestSync: TCriticalSection;
    FContainer: THTTPServerDispatcher;
    FRefCount: integer;
  protected
    procedure DoSessionAttached(Session: THTTPDispatcherSession); virtual;
    procedure DoSessionDetached(Session: THTTPDispatcherSession); virtual;
  public
    constructor Create; virtual;
    destructor Destroy; override;
    procedure IncRefAndIndexLocked;
    procedure DecAndReleaseLocked;

    property LogonId: string read FLogonId;
    property Container: THTTPServerDispatcher read FContainer;
  end;

  THTTPLogonInfoClass = class of THTTPLogonInfo;

  THTTPDispatcherSession = class(TIdHTTPSession)
  private
    FContainer: THTTPServerDispatcher;
    FLogonId: string;
    FLogonInfo: THTTPLogonInfo;
    FSync: TCriticalSection;
    FSessionSynchronize: boolean;
    FLogonSynchronize: boolean;
  protected
    procedure SetLogonId(NewId: string);

    procedure AfterSessionStart;virtual;
    procedure BeforeSessionEnd;virtual;
    procedure DoCommandIndyThread(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);
    procedure DoCommandWorkItem(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);
    procedure ProcessIdCommandGet(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);virtual; abstract;
  public
    constructor Create(AOwner: TIdHTTPCustomSessionList); override;
    constructor CreateInitialized(AOwner: TIdHTTPCustomSessionList; const SessionID,
                                  RemoteIP: string); override;
    destructor Destroy; override;
    property LogonId: string read FLogonId write SetLogonId;
    property LogonInfo: THTTPLogonInfo read FLogonInfo;
    property Container: THTTPServerDispatcher read FContainer;
    property SessionSynchronize: boolean read FSessionSynchronize write FSessionSynchronize;
    property LogonSynchronize: boolean read FLogonSynchronize write FLogonSynchronize;
  end;

  THTTPDispatcherSessionClass = class of THTTPDispatcherSession;

  //For the moment, we shove sessions in one list,
  //expect unique session ID's and potentially duplicate logon id's.

  //Logon ID's are in another list, we expect unqiue logon id's.

  //However, if Indy ends up giving us several sessions with the same session ID,
  //then might add an extra level of indirection:
  //Indy sessions
  //Our really unique sessions
  //Logon ID's.
  //Might have to do this if the client does nasty things with cookies,
  //but hopefully indy code should protect against this.

  TUniqueDispatcherINode = class(TIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  end;

  TDuplicateDispatcherINode = class(TDuplicateValIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  end;

  TUniqueDispatcherSearchINode = class(TUniqueDispatcherINode)
  private
    FSearchString: string;
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  public
    property SearchString: string read FSearchString write FSearchString;
  end;

  TDuplicateDispatcherSearchINode = class(TDuplicateDispatcherINode)
  private
    FSearchString: string;
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  public
    property SearchString: string read FSearchString write FSearchString;
  end;

  TIdHTTPConfigurableSessionList = class(TIdHTTPDefaultSessionList)
  private
    FSessionClass: THTTPDispatcherSessionClass;
  public
    constructor Create;
    function CreateSession(const RemoteIP, SessionID: String): TIdHTTPSession; override;
    property SessionClass: THTTPDispatcherSessionClass read FSessionClass write FSessionClass;
  end;

  //Polymorphic creation or event? Favour latter.
  THTTPDispatcherSessionCreateEvent = procedure (Sender: TObject;
                                             var NewSession: THTTPDispatcherSession);
  THTTPDispatcherSessionNotifyEvent = procedure (Sender: TObject;
                                             Session: THTTPDispatcherSession);

{$IFDEF USE_TRACKABLES}
  THTTPServerDispatcher = class(TTrackable)
{$ELSE}
  THTTPServerDispatcher = class
{$ENDIF}
  private
    FStateLock: TCriticalSection;
    FServerSessions: TIndexedStore;
    FServerLogons: TIndexedStore;
    FIdHTTPServer: TIdHTTPServer;
    FIdHTTPServerSessionList: TIdHTTPConfigurableSessionList;
    FPoolRec: TClientRec;
    FQueueLimit: integer;
    FSessionClass: THTTPDispatcherSessionClass;
    FLogonInfoClass: THTTPLogonInfoClass;
    FPageProducerClass: TPageProducerClass;
  protected
    procedure ClearSessions;
    procedure ClearLogons;
    procedure ClearStore(Store: TIndexedStore);

    procedure HandleIdInvalidSession(AContext: TIdContext;
      ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo;
      var VContinueProcessing: Boolean; const AInvalidSessionID: String);
    procedure HandleIdSessionStart(Sender: TIdHTTPSession);
    procedure HandleIdSessionEnd(Sender: TIdHTTPSession);
    procedure HandleIdCommandGet(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);

    procedure HandleNormalWIComplete(Sender: TObject);
    procedure HandleCancelledWIComplete(Sender: TObject);

    function GetPort: TIdPort;
    procedure SetPort(NewPort: TIdPort);
    function GetActive: boolean;
    procedure SetActive(NewActive: boolean);
    function GetSessionClass: THTTPDispatcherSessionClass;
    procedure SetSessionClass(NewClass: THTTPDispatcherSessionClass);
  public
    constructor Create;
    destructor Destroy; override;

    //TODO - Search sessions by (ptr, sess handle, etc), search sessions with same LogonID.
    property Port: TIdPort read GetPort write SetPort;
    property Active: boolean read GetActive write SetActive;
    property QueueLimit: integer read FQueueLimit write FQueueLimit
      default MAX_SIMUL_REQUESTS;
    property SessionClass: THTTPDispatcherSessionClass read GetSessionClass write SetSessionClass;
    property LogonInfoClass: THTTPLogonInfoClass read FLogonInfoClass write FLogonInfoClass;
    property PageProducerClass: TPageProducerClass read FPageProducerClass write FPageProducerClass;
    //String to be given to page producers if they need to create absolute link URL's
    //(not recommended...)
  end;

implementation

uses
  IdSchedulerOfThreadPool, Classes;

const
  S_NOT_IMPLEMENTED = 'Not implemented.';
  S_SERVICE_UNAVAILABLE = 'Service unavailable.';
  S_QUEUE_LENGTH_EXCEEDED = 'Workitem queue length exceeded, ditching request.';
  S_TOO_MANY_REQUESTS = 'Too many requests.';
  S_INTERNAL_SERVER_ERROR = 'Internal server error.';
  S_SESSION_NOT_FOUND = 'Session not found.';

{ TSrvPageProducer }

constructor TCustomPageProducer.Create;
begin
  inherited;
end;

destructor TCustomPageProducer.Destroy;
begin
  inherited;
end;

{ THTTPRequestWorkItem }

constructor THTTPRequestWorkItem.Create;
begin
  inherited;
  FCompletionEvent := TEvent.Create(nil, true, false, '');
end;

destructor THTTPRequestWorkItem.Destroy;
begin
  FCompletionEvent.Free;
  inherited;
end;

function THTTPRequestWorkItem.DoWork: integer;
begin
  FDispatcherSession.DoCommandWorkItem(FAContext, FARequestInfo, FAResponseInfo);
  result := FAResponseInfo.ResponseNo;
end;

{ THTTPLogonInfo }

procedure THTTPLogonInfo.DoSessionAttached(Session: THTTPDispatcherSession);
begin
end;

procedure  THTTPLogonInfo.DoSessionDetached(Session: THTTPDispatcherSession);
begin
end;

constructor  THTTPLogonInfo.Create;
begin
  FRequestSync := TCriticalSection.Create;
  inherited;
end;

destructor  THTTPLogonInfo.Destroy;
begin
  FRequestSync.Free;
  inherited;
end;

procedure THTTPLogonInfo.IncRefAndIndexLocked;
var
  RV: TISRetVal;
  IRec: TITemRec;
begin
  Assert(FRefCount >= 0);
  Assert(Assigned(FContainer));
  Inc(FRefCount);
  if Pred(FRefCount) = 0 then
  begin
    RV := FContainer.FServerLogons.AddItem(self, IRec);
    Assert(RV = rvOK);
  end;
end;

procedure THTTPLogonInfo.DecAndReleaseLocked;
var
  PtrSearch: TSearchPointerINode;
  RV: TIsRetVal;
  IRec: TItemRec;
begin
  Assert(FRefCount >= 0);
  Assert(Assigned(FContainer));
  Dec(FRefCount);
  if FRefCount = 0 then
  begin
    PtrSearch := TSearchPointerINode.Create;
    try
      PtrSearch.SearchVal := self;
      RV := FContainer.FServerLogons.FindByIndex(Ord(idtPointer), PtrSearch, IRec);
      Assert(RV = rvOK);
      Assert(IRec.Item = self);
      RV := FContainer.FServerLogons.RemoveItem(IRec);
      Assert(RV = rvOK);
      Free;
    finally
      PtrSearch.Free;
    end;
  end;
end;

{ THTTPDispatcherSession }

constructor THTTPDispatcherSession.Create(AOwner: TIdHTTPCustomSessionList);
begin
  inherited;
{$IFDEF DEBUG_SESSION_LIFETIME}
  GLogLog(SV_INFO, 'THTTPDispatcherSession.Create' + IntToHex(Int64(self), 16)
    + ' AOwner: ' + IntToHex(Int64(AOwner), 16));
{$ENDIF}
  FSync := TCriticalSection.Create;
end;

destructor THTTPDispatcherSession.Destroy;
begin
{$IFDEF DEBUG_SESSION_LIFETIME}
  GLogLog(SV_INFO, 'THTTPDispatcherSession.Destroy' + IntToHex(Int64(self), 16));
{$ENDIF}
  //LogonId should have been cleared elsewhere.
  FSync.Free;
  inherited;
end;

//Unfortunately we have to duplicate stuff here. Nasty. Sorry.
constructor THTTPDispatcherSession.CreateInitialized(AOwner: TIdHTTPCustomSessionList; const SessionID,
                              RemoteIP: string);
begin
  inherited;
{$IFDEF DEBUG_SESSION_LIFETIME}
  GLogLog(SV_INFO, 'THTTPDispatcherSession.Create' + IntToHex(Int64(self), 16)
    + ' AOwner: ' + IntToHex(Int64(AOwner), 16)
    + ' SessionId: ' + SessionId
    + ' RemoteIP: ' + RemoteIp);
{$ENDIF}
  FSync := TCriticalSection.Create;
end;

procedure THTTPDispatcherSession.SetLogonId(NewId: string);
var
  OldId: string;
  IRec: TItemRec;
  RV: TIsRetVal;
  PtrSearch: TSearchPointerINode;
  StrSearch: TUniqueDispatcherSearchINode;
  OldLogon: THTTPLogonInfo;
  NewLogon: THTTPLogonInfo;
begin
{$IFDEF DEBUG_SESSION_LIFETIME}
  GLogLog(SV_INFO, 'THTTPDispatcherSession.SetLogonId' + IntToHex(Int64(self), 16)
    + ' NewId: ' + NewId);
{$ENDIF}
  PtrSearch := TSearchPointerINode.Create;
  StrSearch := TUniqueDispatcherSearchINode.Create;
  FContainer.FStateLock.Acquire;
  try
    OldId := FLogonId;
    if OldId <> NewId then
    begin
      if Length(OldId) > 0  then
      begin
        //Detach old logon info.
        Assert(Assigned(FLogonInfo));
        Assert(FLogonInfo.FLogonId = self.FLogonId);
        FLogonInfo.DoSessionDetached(Self);
        OldLogon := FLogonInfo;

        //Remove and re-insert to re-index.
        PtrSearch.SearchVal := self;
        RV := FContainer.FServerSessions.FindByIndex(Ord(idtPointer), PtrSearch, IRec);
        Assert(RV = rvOK);
        RV := FContainer.FServerSessions.RemoveItem(IRec);
        Assert(RV = rvOK);
        FLogonId := '';
        FLogonInfo := nil;
        RV := FContainer.FServerSessions.AddItem(self, IRec);
        Assert(RV = rvOK);

        OldLogon.DecAndReleaseLocked;
      end;

      if Length(NewId) > 0 then
      begin
        //Check whether we have an existing Logon state struct for session.
        StrSearch.SearchString := NewId;
        RV := FContainer.FServerLogons.FindByIndex(Ord(idtLogonId), StrSearch, IRec);
        Assert(RV in [rvOK, rvNotFound]);
        if RV = rvNotFound then
        begin
          NewLogon := FContainer.FLogonInfoClass.Create;
          NewLogon.FLogonId := NewId;
          NewLogon.FContainer := FContainer;
        end
        else
          NewLogon := IRec.Item as THTTPLogonInfo;

        NewLogon.IncRefAndIndexLocked;

        //Remove and re-insert to re-index.
        PtrSearch.SearchVal := self;
        RV := FContainer.FServerSessions.FindByIndex(Ord(idtPointer), PtrSearch, IRec);
        Assert(RV = rvOK);
        RV := FContainer.FServerSessions.RemoveItem(IRec);
        Assert(RV = rvOK);
        FLogonId := NewId;
        FLogonInfo := NewLogon;
        RV := FContainer.FServerSessions.AddItem(self, IRec);
        Assert(RV = rvOK);

        FLogonInfo.DoSessionAttached(Self);
        Assert(Assigned(FLogonInfo));
        Assert(FLogonInfo.FLogonId = self.FLogonId);
      end;
    end;
  finally
    FContainer.FStateLock.Release;
    PtrSearch.Free;
    StrSearch.Free;
  end;
end;

procedure THTTPDispatcherSession.AfterSessionStart;
begin
{$IFDEF DEBUG_SESSION_LIFETIME}
  GLogLog(SV_INFO, 'THTTPDispatcherSession.AfterSessionStart' + IntToHex(Int64(self), 16));
{$ENDIF}
end;

procedure THTTPDispatcherSession.BeforeSessionEnd;
begin
{$IFDEF DEBUG_SESSION_LIFETIME}
  GLogLog(SV_INFO, 'THTTPDispatcherSession.BeforeSessionEnd' + IntToHex(Int64(self), 16));
{$ENDIF}
end;

procedure THTTPDispatcherSession.DoCommandIndyThread(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);
var
  WI: THTTPRequestWorkItem;
begin
  WI := THTTPRequestWorkItem.Create;
  try
    WI.CanAutoFree := false;
    WI.CanAutoReset := false;
    //TODO - Event initial state.
    //FCompletionEvent: TEvent;
    WI.FDispatcherSession := self;
    WI.FAContext := AContext;
    WI.FARequestInfo := ARequestInfo;
    WI.FAResponseInfo := AResponseInfo;
    //A default "not implemented" would be a good idea, maybe 404?
    AResponseInfo.ResponseNo := 501;
    AResponseInfo.ResponseText := S_NOT_IMPLEMENTED;
    if GCommonPool.AddWorkItem(FContainer.FPoolRec, WI, FContainer.QueueLimit) then
    begin
      WI.FCompletionEvent.WaitFor(INFINITE);
      if WI.Cancelled then
      begin
        AResponseInfo.ResponseNo := 503;
        AResponseInfo.ResponseText := S_SERVICE_UNAVAILABLE;
      end;
      // else TODO - Anything for the OK case?

      //Calling WriteContent more than once should not cause a problem.
      AResponseInfo.WriteContent;
    end
    else
    begin
      GLogLog(SV_WARN, S_QUEUE_LENGTH_EXCEEDED);
      AResponseInfo.ResponseNo := 429;
      AResponseInfo.ResponseText := S_TOO_MANY_REQUESTS;
    end;
  finally
    WI.Free;
  end;
end;

procedure THTTPDispatcherSession.DoCommandWorkItem(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);
var
  LSessionSync, LLogonSync: boolean;
  LLogonInfo: THTTPLogonInfo;
begin
  LSessionSync := SessionSynchronize;
  LLogonSync := LogonSynchronize;
  LLogonInfo := nil;

  if LSessionSync then
    FSync.Acquire;

  if LLogonSync then
  begin
    FContainer.FStateLock.Acquire;
    try
      if Assigned(FLogonInfo) then
      begin
        LLogonInfo := FLogonInfo;
        LLogonInfo.IncRefAndIndexLocked;
      end
    finally
      FContainer.FStateLock.Release;
    end;
    if Assigned(LLogonInfo) then
      LLogonInfo.FRequestSync.Acquire;
  end;

  try
    ProcessIdCommandGet(AContext, ARequestInfo, AResponseInfo);
  except
    on E:Exception do
    begin
      AResponseInfo.ResponseNo := 501;
      AResponseInfo.ResponseText := S_INTERNAL_SERVER_ERROR;
      GLogLog(SV_FAIL, E.ClassName + ': ' + E.Message);
    end;
  end;

  if LLogonSync then
  begin
    if Assigned(LLogonInfo) then
    begin
      LLogonInfo.FRequestSync.Release;
      FContainer.FStateLock.Acquire;
      try
        LLogonInfo.DecAndReleaseLocked;
      finally
        FContainer.FStateLock.Release;
      end;
    end;
  end;

  if LSessionSync then
    FSync.Release;
end;

{ TUniqueDispatcherINode, TDuplicateDispatcherINode }

function DispatcherCompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
begin
  case IndexTag of
    Ord(idtSessionString):
    begin
      result := CompareText((OtherItem as THTTPDispatcherSession).SessionID,
                            (OwnItem as THTTPDispatcherSession).SessionID);
    end;
    Ord(idtLogonId):
    begin
      if OtherItem is THTTPDispatcherSession then
      begin
        result := CompareText((OtherItem as THTTPDispatcherSession).LogonId,
                              (OwnItem as THTTPDispatcherSession).LogonId);
      end
      else if OtherItem is THTTPLogonInfo then
      begin
        result := CompareText((OtherItem as THTTPLogonInfo).LogonId,
                              (OwnItem as THTTPLogonInfo).LogonId);
      end
      else
      begin
        Assert(false);
        result := 0;
      end;
    end
  else
    Assert(false);
    result := 0;
  end;
end;

function TUniqueDispatcherINode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
begin
  result := DispatcherCompareItems(OwnItem, OtherItem, IndexTag, OtherNode);
end;

function TDuplicateDispatcherINode.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
begin
  result := DispatcherCompareItems(OwnItem, OtherItem, IndexTag, OtherNode);
end;

{ TUniqueDispatcherSearchINode, TDuplicateDispatcherSearchINode }

function DispatcherSearchCompareItems(SearchString: string; OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
begin
  case IndexTag of
    Ord(idtSessionString):
    begin
      result := CompareText((OtherItem as THTTPDispatcherSession).SessionID,
                            SearchString);
    end;
    Ord(idtLogonId):
    begin
      if OtherItem is THTTPDispatcherSession then
      begin
        result := CompareText((OtherItem as THTTPDispatcherSession).LogonId,
                              SearchString);
      end
      else if OtherItem is THTTPLogonInfo then
      begin
        result := CompareText((OtherItem as THTTPLogonInfo).LogonId,
                              SearchString);
      end
      else
      begin
        Assert(false);
        result := 0;
      end;
    end
  else
    Assert(false);
    result := 0;
  end;
end;

function TUniqueDispatcherSearchINode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
begin
  result := DispatcherSearchCompareItems(FSearchString, OtherItem, IndexTag, OtherNode);
end;

function TDuplicateDispatcherSearchINode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
begin
  result := DispatcherSearchCompareItems(FSearchString, OtherItem, IndexTag, OtherNode);
end;

{ TIdHTTPConfigurableSessionList }

constructor TIdHTTPConfigurableSessionList.Create;
begin
  inherited Create(nil);
  FSessionClass := THTTPDispatcherSession;
end;

function TIdHTTPConfigurableSessionList.CreateSession(const RemoteIP, SessionID: String): TIdHTTPSession;
begin
  Result := FSessionClass.CreateInitialized(Self, SessionID, RemoteIP);
  SessionList.Add(Result);
end;

{ THTTPServerDispatcher }

procedure THTTPServerDispatcher.HandleIdInvalidSession(AContext: TIdContext;
  ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo;
  var VContinueProcessing: Boolean; const AInvalidSessionID: String);
begin
  VContinueProcessing := true;
end;


procedure THTTPServerDispatcher.HandleIdSessionStart(Sender: TIdHTTPSession);
var
  RV: TIsRetVal;
  IRec: TItemRec;
begin
{$IFDEF DEBUG_SESSION_LIFETIME}
  GLogLog(SV_INFO, 'THTTPServerDispatcher.HandleIdSessionStart ' + IntToHex(Int64(self), 16)
    + ' Session: ' + IntToHex(Int64(Sender), 16));
{$ENDIF}
  Assert((Sender as THTTPDispatcherSession).SessionID <> '');
  FStateLock.Acquire;
  try
    (Sender as THTTPDispatcherSession).FContainer := self;
    RV := FServerSessions.AddItem(Sender, IRec);
    Assert(RV = rvOK);
  finally
    FStateLock.Release;
  end;
  (Sender as THTTPDispatcherSession).AfterSessionStart;
end;

procedure THTTPServerDispatcher.HandleIdSessionEnd(Sender: TIdHTTPSession);
var
  RV: TIsRetVal;
  Search: TSearchPointerINode;
  IRec: TItemRec;
begin
{$IFDEF DEBUG_SESSION_LIFETIME}
  GLogLog(SV_INFO, 'THTTPServerDispatcher.HandleIdSessionEnd ' + IntToHex(Int64(self), 16)
    + ' Session: ' + IntToHex(Int64(Sender), 16));
{$ENDIF}
  try
    (Sender as THTTPDispatcherSession).BeforeSessionEnd;
    (Sender as THTTPDispatcherSession).LogonId := '';
  finally
    Search := TSearchPointerINode.Create;
    Search.SearchVal := Sender;
    FStateLock.Acquire;
    try
      RV := FServerSessions.FindByIndex(Ord(idtPointer), Search, IRec);
      if RV = rvOK then
      begin
        RV := FServerSessions.RemoveItem(IRec);
        Assert(RV = rvOK);
      end
      else
        Assert(False);
    finally
      FStateLock.Release;
    end;
  end;
end;

procedure THTTPServerDispatcher.HandleIdCommandGet(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);
var
  RV: TIsRetVal;
  Search: TSearchPointerINode;
  IRec: TItemRec;
{$IFDEF DEBUG_HTTP_REQUESTS}
  Idx: integer;
{$ENDIF}
begin
{$IFDEF DEBUG_HTTP_REQUESTS}
  GLogLog(SV_INFO, 'HTTP command.');
  GLogLog(SV_INFO, 'RequestInfo: ');
  GLogLog(SV_INFO, 'Session :' + IntToHex(Int64(ARequestInfo.Session), 16));
  GLogLog(SV_INFO, 'AuthExists: ' + BoolToStr(ARequestInfo.AuthExists, true));
  GLogLog(SV_INFO, 'AuthUsername: ' + ARequestInfo.AuthUsername);
  GLogLog(SV_INFO, 'AuthPassWord: ' + ARequestInfo.AuthPassword);
  if Assigned(ARequestInfo.Authentication) then
    GLogLog(SV_INFO, 'Authentication: ' + ARequestInfo.Authentication.Authentication)
  else
    GLogLog(SV_INFO, 'Authentication: <none>');
  GLogLog(SV_INFO, 'Command: ' + ARequestInfo.Command);
  GLogLog(SV_INFO, 'CommandType: ' + IntToHex(Ord(ARequestInfo.CommandType), 4));
  GLogLog(SV_INFO, 'Cookies: ');
  for Idx := 0 to Pred(ARequestInfo.Cookies.Count) do
    GLogLog(SV_INFO, 'Cookie Name: ' + ARequestInfo.Cookies.Cookies[Idx].CookieName
    + ' Text: ' + ARequestInfo.Cookies.Cookies[Idx].CookieText);
  GLogLog(SV_INFO, 'Document: ' + ARequestInfo.Document);
  GLogLog(SV_INFO, 'URI: ' + ARequestInfo.URI);
  GLogLog(SV_INFO, 'Params: ');
  for Idx := 0 to Pred(ARequestInfo.Params.Count) do
    GLogLog(SV_INFO, 'Param: ' + ARequestInfo.Params[idx]);
  GLogLog(SV_INFO, 'RawHTTPCommand: ' + ARequestInfo.RawHTTPCommand);
  GLogLog(SV_INFO, 'RemoteIP: ' + ARequestInfo.RemoteIP);
  GLogLog(SV_INFO, 'UnparsedParams: ' + ARequestInfo.UnparsedParams);
  GLogLog(SV_INFO, 'FormParams: ' + ARequestInfo.FormParams);
  GLogLog(SV_INFO, 'QueryParams: ' + ARequestInfo.QueryParams);
  GLogLog(SV_INFO, 'Version: ' + ARequestInfo.Version);
  GLogLog(SV_INFO, '');
  GLogLog(SV_INFO, '---');
  GLogLog(SV_INFO, 'Accept: ' + ARequestInfo.Accept);
  GLogLog(SV_INFO, 'AcceptCharSet: ' + ARequestInfo.AcceptCharSet);
  GLogLog(SV_INFO, 'AcceptEncoding: ' + ARequestInfo.AcceptEncoding);
  GLogLog(SV_INFO, 'AcceptLanguage: ' + ARequestInfo.AcceptLanguage);
  GLogLog(SV_INFO, 'Basic authentication: ' + BoolToStr(ARequestInfo.BasicAuthentication, true));
  GLogLog(SV_INFO, 'Host: ' + ARequestInfo.Host);
  GLogLog(SV_INFO, 'From: ' + ARequestInfo.From);
  GLogLog(SV_INFO, 'Password: ' + ARequestInfo.Password);
  GLogLog(SV_INFO, 'Referer: ' + ARequestInfo.Referer);
  GLogLog(SV_INFO, 'UserAgent: ' + ARequestInfo.UserAgent);
  GLogLog(SV_INFO, 'UserName: ' + ARequestInfo.UserName);
  GLogLog(SV_INFO, 'ProxyConnection: ' + ARequestInfo.ProxyConnection);
  GLogLog(SV_INFO, 'Range: ' + ARequestInfo.Range);
  GLogLog(SV_INFO, '');
  GLogLog(SV_INFO, '---');
{$ENDIF}

{$IFDEF DEBUG_SESSION_LIFETIME}
  GLogLog(SV_INFO, 'THTTPServerDispatcher.HandleIdCommandGet ' + IntToHex(Int64(self), 16)
    + ' Session: ' + IntToHex(Int64(ARequestInfo.Session), 16));
{$ENDIF}
  FStateLock.Acquire;
  try
    Search := TSearchPointerINode.Create;
    Search.SearchVal := ARequestInfo.Session;
    RV := FServerSessions.FindByIndex(Ord(idtPointer), Search, IRec);
  finally
    FStateLock.Release;
  end;
  if RV = RVOK then
    (ARequestInfo.Session as THTTPDispatcherSession).DoCommandIndyThread
      (AContext, ARequestInfo, AResponseInfo)
  else
  begin
    AResponseInfo.ResponseNo := 500;
    AResponseInfo.ResponseText := S_INTERNAL_SERVER_ERROR;
    GLogLog(SV_FAIL, S_SESSION_NOT_FOUND);
    AResponseInfo.WriteHeader;
    AResponseInfo.WriteContent
  end;
end;

constructor THTTPServerDispatcher.Create;
begin
  inherited;
  FStateLock := TCriticalSection.Create;
  FServerSessions := TIndexedStore.Create;
  FServerSessions.AddIndex(TPointerINode, Ord(idtPointer));
  FServerSessions.AddIndex(TUniqueDispatcherINode, Ord(idtSessionString));
  FServerSessions.AddIndex(TDuplicateDispatcherINode, Ord(idtLogonId));
  FServerLogons := TIndexedStore.Create;
  FServerLogons.AddIndex(TPointerINode, Ord(idtPointer));
  FServerLogons.AddIndex(TUniqueDispatcherINode, Ord(idtLogonId));

  FIdHTTPServer := TIdHTTPServer.Create(nil);
  FIdHTTPServerSessionList := TIdHTTPConfigurableSessionList.Create;
  FIdHTTPServer.SessionList := FIdHTTPServerSessionList;
  FIdHTTPServer.OnCommandGet := HandleIdCommandGet;
  // TODO FIdHTTPServer.OnCommandError
  // TODO FIdHTTPServer.OnCommandOther
  FIdHTTPServer.OnInvalidSession := HandleIdInvalidSession;
  FIdHTTPServer.OnSessionStart := HandleIdSessionStart;
  FIdHTTPServer.OnSessionEnd := HandleIdSessionEnd;
  FIdHTTPServer.SessionState := true;
  FIdHTTPServer.AutoStartSession := true;
  FIdHTTPServer.SessionTimeOut := 600 * 1000; //Millisecs.
  FIdHTTPServer.KeepAlive := true;
  FIdHTTPServer.Scheduler := TIdSchedulerOfThreadPool.Create(FIdHTTPServer);

  FSessionClass := THTTPDispatcherSession;
  FLogonInfoClass := THTTPLogonInfo;

  FPoolRec := GCommonPool.RegisterClient(self, HandleNormalWIComplete, HandleCancelledWIComplete);
end;

destructor THTTPServerDispatcher.Destroy;
begin
  FIdHTTPServer.Active := false;
  GCommonPool.DeRegisterClient(FPoolRec);
  FIdHTTPServer.Free;
  ClearSessions;
  ClearLogons;
  FServerSessions.Free;
  FServerLogons.Free;
  FStateLock.Free;
  inherited;
end;

procedure THTTPServerDispatcher.HandleNormalWIComplete(Sender: TObject);
begin
  (Sender as THTTPRequestWorkItem).FCompletionEvent.SetEvent;
end;

procedure THTTPServerDispatcher.HandleCancelledWIComplete(Sender: TObject);
begin
  (Sender as THTTPRequestWorkItem).FCompletionEvent.SetEvent;
end;

procedure THTTPServerDispatcher.ClearStore(Store: TIndexedStore);
var
  IRec: TItemRec;
  Obj: TObject;
begin
  FStateLock.Acquire;
  try
    IRec := Store.GetAnItem;
    while Assigned(IRec) do
    begin
      Obj := IRec.Item;
      Store.RemoveItem(IRec);
      Obj.Free;
    end;
  finally
    FStateLock.Release;
  end;
end;

procedure THTTPServerDispatcher.ClearSessions;
begin
  ClearStore(FServerSessions);
end;

procedure THTTPServerDispatcher.ClearLogons;
begin
  ClearStore(FServerLogons);
end;

function THTTPServerDispatcher.GetPort: TIdPort;
begin
  result := FIdHTTPServer.DefaultPort;
end;

procedure THTTPServerDispatcher.SetPort(NewPort: TIdPort);
begin
  FIdHTTPServer.DefaultPort := NewPort;
end;

function THTTPServerDispatcher.GetActive: boolean;
begin
  result := FIdHTTPServer.Active;
end;

procedure THTTPServerDispatcher.SetActive(NewActive: boolean);
begin
  FIdHTTPServer.Active := NewActive;
end;

function THTTPServerDispatcher.GetSessionClass: THTTPDispatcherSessionClass;
begin
  result := FIdHTTPServerSessionList.SessionClass;
end;

procedure THTTPServerDispatcher.SetSessionClass(NewClass: THTTPDispatcherSessionClass);
begin
  FIdHTTPServerSessionList.SessionClass := NewClass;
end;

end.
