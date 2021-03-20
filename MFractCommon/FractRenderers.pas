unit FractRenderers;

{ Martin Harvey 16 May 2013 }

interface


uses
  System.Types, FractMisc, FractSettings, Classes,
{$IF DEFINED(IS_FIREMONKEY)}
  FMX.Types,
  FMX.Controls,
  FMSizeableImage,
{$ELSE}
  VCL.Graphics,
  SizeableImage,
{$ENDIF}
  DLThreadQueue,
  WorkItems,
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SyncObjs
  ;

type
  TLocatedBitmap = class(TBitmap)
  private
    FLocation: TPoint;
{$IF DEFINED(IS_FIREMONKEY)}
    FBitmapData: TBitmapData;
    FBitmapDataMapped: boolean;
{$ENDIF}
  public
    procedure CopyIntoBitmap(Dest: TBitmap);
    destructor Destroy; override;
{$IF DEFINED(IS_FIREMONKEY)}
    property BitmapData:TBitmapData read FBitmapData write FBitmapData;
    property BitmapDataMapped: boolean read FBitmapDataMapped write FBitmapDataMapped;
{$ENDIF}
  end;

  PImagePortion = ^TImagePortion;

  TFractWorkItem = class(TWorkItem)
  private
    FImagePortion: PImagePortion;
    FBitmap: TLocatedBitmap;
  protected
    function DoWork: integer; override;
    procedure DoNormalCompletion; override;
    procedure DoCancelledCompletion; override;
   //TODO - Move this into a settings related class eventually.
    function CalcFract(RPel: TRealPoint): cardinal;
    procedure SetupLocatedBitmapMainThread;
  public
    property ImagePortion: PImagePortion read FImagePortion write FImagePortion;
  end;

  TFractRenderer = class(TObject)
  private
  protected
    FWorkFarm: TWorkFarm;
    FResultQueue: TDLProxyThreadQueue;
    FDiscardQueue: TDLProxyThreadQueue;
    FOnProgress: TNotifyEvent;

    procedure StopThreads; virtual;
    procedure StartThreads; virtual; abstract;
    procedure MainThreadResultMsgHandler;
    function MainThreadPickupResults: boolean; virtual;
    procedure MainThreadFlushResults; virtual;

{$IF Defined(MSWINDOWS)}
    function GetThreadPriority: TThreadPriority;
    procedure SetThreadPriority(NewPriority: TThreadPriority);
{$ELSEIF Defined(POSIX)}
    function GetThreadPriority: integer;
    procedure SetThreadPriority(NewPriority: integer);
{$ENDIF}

    procedure Progress;
    function GetPercentComplete: integer; virtual; abstract;
    function GetSecsRemaining: integer; virtual; abstract;
    function GetRunning: boolean; virtual; abstract;
    property ResultQueue: TDLProxyThreadQueue read FResultQueue write FResultQueue;
  public
    procedure Stop; virtual;
    constructor Create;
    destructor Destroy; override;

{$IF Defined(MSWINDOWS)}
    property ThreadPriority: TThreadPriority
      read GetThreadPriority write SetThreadPriority;
{$ELSEIF Defined(POSIX)}
    property ThreadPriority: integer
      read GetThreadPriority write SetThreadPriority;
{$ENDIF}
    property PercentComplete: integer read GetPercentComplete;
    property OnProgress: TNotifyEvent read FOnProgress write FOnProgress;
    property Running: boolean read GetRunning;
    property SecsRemaining: integer read GetSecsRemaining;
  end;

  TFractStillRenderer = class;

  TFractData = array of cardinal;

  TImagePortion = record
    WorkItem: TFractWorkItem;
    Renderer: TFractStillRenderer;
    PelRect: TRect;
    ImgCenter: TRealPoint;
    ImgTotalRect: TPoint;
    ImgScalePPU: double;
    ImgRotRads: double;
    FractData: TFractData; //if 0 length, not valid.
  end;

  TImagePortions = array of TImagePortion;

  TFractVideoRenderer = class;

  TFractStillRenderer = class(TFractRenderer)
  private
    FEnvironment: TFractV2Environment;
    FPortions: TImagePortions;
    FPortionCount: cardinal;
    FPortionRectsValid: boolean;
    FBitmap: TBitmap;
    FChunksStarted, FChunksDone: integer;
    FStartTime: TDateTime;
    FLastChunkTime: TDateTime;
  protected
    procedure StopThreads; override;
    procedure StartThreads; override;
    procedure MainThreadFlushResults; override;

    procedure InvalidatePortionCount;
    procedure InvalidatePortionRects;
    procedure InvalidatePortionData;
    procedure InitPortionCount;
    procedure InitPortionRects;

    procedure CompareAndInvalidate(OldSettings, NewSettings: TFractV2SettingsBundle);
    procedure InvalidateAll;
    procedure SetPortionCount(NewPortionCount: cardinal);

    procedure ResetChunkCount;
    procedure SetChunksStarted(NewStarted: integer);
    function IncChunksDone: boolean;

    function GetRunning: boolean; override;
    function GetPercentComplete: integer; override;
    function GetSecsRemaining: integer; override;

    property Bitmap: TBitmap read FBitmap write FBitmap;
    procedure FileLoadAll(NewSettings: TFractStreamable; var ErrStr: string); virtual; abstract;
    procedure FileLoadPalette(NewPalette: TFractStreamable; var ErrStr: string); virtual; abstract;
  public
    constructor Create;
    destructor Destroy; override;

    property Environment: TFractV2Environment read FEnvironment;
  end;

  TFractImageRenderer = class(TFractStillRenderer)
  private
    FImage: TSizeableImage;
  protected
    procedure SetImage(NewImage: TSizeableImage);
    procedure HandleImageResize(Sender: TObject);
    function MainThreadPickupResults: boolean; override;
    procedure DrawParamsChanged;
  public
    procedure StartThreads; override;
    procedure ZoomDefault;
    procedure ZoomBy(LogScale: double);
    procedure ChangeDetail(PowerIncrement: integer);
    function CanChangeDetail(PowerIncrement: integer):boolean;
    procedure Recalc;
    procedure Refresh;
    procedure MoveByPels(Offset: TPoint);
    procedure ZoomAtBy(Location: TPoint; LogScale: double);
{$IFDEF IS_FIREMONKEY}
    procedure MoveByPelsF(Offset: TPointF);
    procedure ZoomAtByF(Location: TPointF; LogScale: double);
{$ENDIF}
    procedure RotateBy(Radians: double);
    procedure ChangeSettingsOnly(NewSettings: TFractV2SettingsBundle);
    procedure ChangePalletteOnly(NewPalette: TFractPalette);
    procedure FileLoadAll(NewSettings: TFractStreamable; var ErrStr: string); override;
    procedure FileLoadPalette(NewPalette: TFractStreamable; var ErrStr: string); override;
    procedure PaletteCopyFromVideo(OtherRenderer: TFractVideoRenderer);

    procedure Undo;
    function CanUndo: boolean;
    procedure ReDo;
    function CanReDo: boolean;

    property Image: TSizeableImage read FImage write SetImage;
  end;

  TFractBitmapRenderer = class(TFractStillRenderer)
  private
    FOutputStream: TStream;
    FSaved: boolean;
  protected
    function MainThreadPickupResults: boolean; override;
    procedure SetSize(NewSize: TPoint);
    function GetSize: TPoint;
  public
    constructor Create;
    destructor Destroy; override;
    procedure StartRender;
    procedure CancelRender;
    procedure FileLoadAll(NewSettings: TFractStreamable; var ErrStr: string); override;
    procedure FileLoadPalette(NewPalette: TFractStreamable; var ErrStr: string); override;
    property OutputStream: TStream read FOutputStream write FOutputStream;
    property Saved: boolean read FSaved;
    property Size: TPoint read GetSize write SetSize;
  end;

  TFractVideoRenderer = class(TFractRenderer)
  private
    FEnvironment: TFractV2VideoEnv;
  protected
  public
    constructor Create;
    destructor Destroy; override;

    procedure PaletteCopyFromStill(OtherRenderer: TFractStillRenderer);
    procedure WaypointAppend(OtherRenderer: TFractStillRenderer);
    procedure WaypointInsert(OtherRenderer: TFractStillRenderer);
    //TODO - Load and save settings.
    procedure WaypointMove(NewIdx: cardinal);
    procedure WaypointDelete;
    procedure WaypointClearAll;

    function WaypointCanDelete: boolean;
    function WaypointCanMove(NewIdx:cardinal): boolean;

    property Environment: TFractV2VideoEnv read FEnvironment;
  end;

implementation

uses
  SysUtils
{$IF DEFINED(IS_FIREMONKEY)}
  , FMX.PixelFormats,
  System.UITypes
{$ENDIF}
  ;

{ TFractWorkItem }

function TFractWorkItem.CalcFract(RPel: TRealPoint): cardinal;
var
  Settings: TFractV2SettingsBundle;
  bail, iter: cardinal;
  ZReStart, ZImStart: double;
  CReStart, CImStart: double;
{$IFDEF CPUX64}
  CRe, CIm: double;
  ZRe, ZIm: double;
  OldZreSq, OldZimSq: double;
  Const2: double;
{$ENDIF}
{$IFDEF CPUX86}
label
  done, again, done_morepops;
{$ENDIF}
begin
  Settings := FImagePortion.Renderer.Environment.HistoryCurrentBundle;
  bail := Pow2Val(Settings.Location.BailoutOrder);
  if Settings.Formula.Formula = ffMandel then
  begin
    CReStart := RPel.X;
    CImStart := RPel.Y;
    ZReStart := 0;
    ZImStart := 0;
  end
  else if Settings.Formula.Formula = ffJulia then
  begin
    CReStart := Settings.Formula.JuliaConst.X;
    CImStart := Settings.Formula.JuliaConst.Y;
    ZReStart := RPel.X;
    ZImStart := RPel.Y;
  end else
  begin
    Assert(false);
    result := 0;
    exit;
  end;
{$IFDEF CPUX64}
  Const2 := 2;
  CRe := CReStart;
  CIm := CImStart;
  ZRe := ZReStart;
  ZIm := ZImStart;
{$ENDIF}
  Iter := 0;
{$IFDEF CPUX64}
  while (iter < bail) do
  begin
    OldZReSq := ZRe * ZRe;
    OldZImSq := ZIm * ZIm;
    if OldZreSq + OldZImSq >= (Const2 + Const2) then break;
    ZIm := Const2 * ZRe * Zim + CIm;
    ZRe := OldZReSq - OldZImSq + CRe;
    Inc(Iter);
  end;
{$ENDIF}

{$IFDEF CPUX86}
  asm
  push edx
  push ecx
  mov ecx, iter
  mov edx, bail

  fld1
  fadd st(0), st(0)
  fadd st(0), st(0)
  {0 = $4}
  fld ZReStart
  fld ZImStart
  {0 = ZIm, 1 = ZRe, 2 = $4}
  fld CReStart
  fld CImStart
  {0 = Cim, 1 = CRe, 2 = ZIm, 3 = ZRe, 4 = $4}
again:

//test.
  cmp ecx, edx
  jae done

//calc squares for re and im.
  {0 = Cim, 1 = CRe, 2 = ZIm, 3 = ZRe, 4 = $4}
  fld st(3)
  {0 = ZReSq 1 = Cim, 2 = CRe, 3 = ZIm, 4 = ZRe, 5 = $4}
  fmul st(0), st(4)

  {0 = ZReSq 1 = Cim, 2 = CRe, 3 = ZIm, 4 = ZRe, 5 = $4}
  fld st(3)
  {0 = ZImSq 1 = ZReSq 2 = Cim, 3 = CRe, 4 = ZIm, 5 = ZRe, 6 = $4}
  fmul st(0), st(4)
  {0 = ZImSq 1 = ZReSq 2 = Cim, 3 = CRe, 4 = ZIm, 5 = ZRe, 6 = $4}

//calc sum of squares
  fld st(0)
  {0 = Sum of sqs, 1 = ZImSq 2 = ZReSq 3 = Cim, 4 = CRe, 5 = ZIm, 6 = ZRe, 7 = $4}
  fadd st(0), st(2)
  fcomp st(7)
  fstsw ax
  sahf
  jnb done_morepops

  {0 = ZImSq 1 = ZReSq 2 = Cim, 3 = CRe, 4 = ZIm, 5 = ZRe, 6 = $4}

//calc new zre.
  fld st(1)
  {0 = New zre, 1 = ZImSq 2 = ZReSq 3 = Cim, 4 = CRe, 5 = ZIm, 6 = ZRe, 7 = $4}
  fsub st(0), st(1)
  fadd st(0), st(4)
  fstp st(2)
  {0 = ZImSq 1 = New zre 2 = Cim, 3 = CRe, 4 = ZIm, 5 = ZRe, 6 = $4}
  fstp st //pop 1 reg
  {0 = New ZRe, 1 = Cim, 2 = CRe, 3 = ZIm, 4 = ZRe, 5 = $4}

//calc new zim
  fld st(4)
  {0 = New ZIm, 1 = New ZRe, 2 = Cim, 3 = CRe, 4 = ZIm, 5 = ZRe, 6 = $4}
  fadd st(0), st(5)
  fmul st(0), st(4)
  fadd st(0), st(2)

//move results to right place on fp stack.
  {0 = New ZIm, 1 = New ZRe, 2 = Cim, 3 = CRe, 4 = ZIm, 5 = ZRe, 6 = $4}
  fstp st(4)
  {0 = New ZRe, 1 = Cim, 2 = CRe, 3 = New ZIm, 4 = ZRe, 5 = $4}
  fstp st(4)

  {0 = Cim, 1 = CRe, 2 = New ZIm, 4 = New ZRe, 4 = $4}
  inc ecx
  jmp again
done_morepops:
  fcompp //pop 2 regs.
done:
  fcompp //pop 2 regs.
  fcompp //pop 2 regs.
  fstp st //pop 1 reg
  mov iter, ecx
  pop ecx
  pop edx
  end;
{$ENDIF}
  if iter = bail then
    result := High(result)
  else
    result := iter;
end;

procedure TFractWorkItem.SetupLocatedBitmapMainThread;
var
{$IF DEFINED (IS_FIREMONKEY)}
  IntBitmapAccess: IBitmapAccess;
  SupportsIBitmapAccess: boolean;
{$ENDIF}
  OurRectWidth, OurRectHeight: integer;
begin
  Assert(FImagePortion.WorkItem = Self);
  OurRectWidth := FImagePortion.PelRect.Right - FImagePortion.PelRect.Left;
  OurRectHeight := FImagePortion.PelRect.Bottom - FImagePortion.PelRect.Top;
{$IF DEFINED (IS_FIREMONKEY)}
    FBitmap := TLocatedBitmap.Create(OurRectWidth, OurRectHeight);
    SupportsIBitmapAccess := Supports(FBitmap, IBitmapAccess, IntBitmapAccess);
    Assert(SupportsIBitmapAccess);
    Assert(Assigned(IntBitmapAccess));
    IntBitmapAccess.PixelFormat := pfA8R8G8B8;
    //No need to release, I think.
{$ELSE}
    Assert(not Assigned(FBitmap));
    FBitmap := TLocatedBitmap.Create;
    FBitmap.Width := OurRectWidth;
    FBitmap.Height := OurRectHeight;
    FBitmap.PixelFormat := pf32Bit;
{$ENDIF}
    FBitmap.FLocation.x := FImagePortion.PelRect.Left;
    FBitmap.FLocation.y := FImagePortion.PelRect.Top;
{$IF DEFINED (IS_FIREMONKEY)}
    FBitmap.BitmapDataMapped := FBitmap.Map(TMapAccess.maReadWrite, FBitmap.FBitmapData);
    Assert(FBitmap.BitmapDataMapped);
    Assert(FBitmap.FBitmapData.PixelFormat = pfA8R8G8B8);
{$ENDIF}
end;

function TFractWorkItem.DoWork: integer;
var
  NextPel: boolean;
  pX, pY: integer;
  RPelXLookup: array of TRealPoint;
  RPelYLookup: array of TRealPoint;
  OurRectWidth, OurRectHeight: integer;

  ScreenPoint: TPoint;
  RPel, Datum: TRealPoint;
  calc_result: cardinal;
  NPels, idx: integer;
  PelPtr: PPackedColor;
  Settings: TFractV2SettingsBundle;
begin
  //Allocate fract data.
  result := 0;
  Assert(FImagePortion.WorkItem = Self);
  OurRectWidth := FImagePortion.PelRect.Right - FImagePortion.PelRect.Left;
  OurRectHeight := FImagePortion.PelRect.Bottom - FImagePortion.PelRect.Top;
  Npels := OurRectWidth * OurRectHeight;
  Assert(NPels > 0);

  //Need to recalc fract data?
  if (Length(FImagePortion.FractData) = 0)
    and (Self.WorkItemState = wisExecuting) then
  begin
    SetLength(FImagePortion.FractData, NPels);

    //Calculate lookups.
    SetLength(RPelXLookup, OurRectWidth);
    for idx := 0 to Pred(OurRectWidth) do
    begin
      ScreenPoint.x := FImagePortion.PelRect.Left + idx;
      ScreenPoint.y := FImagePortion.PelRect.Top;
      RPelXLookup[idx] := ScreenPointToRealPoint(ScreenPoint,
        FImagePortion.ImgTotalRect.x,
        FImagePortion.ImgTotalRect.y,
        FImagePortion.ImgCenter,
        FImagePortion.ImgScalePPU,
        FImagePortion.ImgRotRads);
    end;
    SetLength(RPelYLookup, OurRectHeight);
    for idx := 0 to Pred(OurRectHeight) do
    begin
      ScreenPoint.x := FImagePortion.PelRect.Left;
      ScreenPoint.y := FImagePortion.PelRect.Top + idx;
      RPelYLookup[idx] := ScreenPointToRealPoint(ScreenPoint,
        FImagePortion.ImgTotalRect.x,
        FImagePortion.ImgTotalRect.y,
        FImagePortion.ImgCenter,
        FImagePortion.ImgScalePPU,
        FImagePortion.ImgRotRads);
    end;

    Assert(RealPointsSame(RPelXLookup[0], RPelYLookup[0]));

    Datum := RPelXLookup[0];
    for idx := 0 to Pred(OurRectWidth) do
      RPelXLookup[idx] := SubRealPoints(RPelXLookup[idx], Datum);
    for idx := 0 to Pred(OurRectHeight) do
      RPelYLookup[idx] := SubRealPoints(RPelYLookup[idx], Datum);

    idx := 0;
    pX := 0;
    pY := 0;
    NextPel := true;
    while (Self.WorkItemState = wisExecuting) and NextPel do
    begin
      Assert(pX < OurRectWidth);
      Assert(pY < OurRectHeight);
      RPel.X := RPelXLookup[pX].X;
      RPel.Y := RPelYLookup[pY].Y;
      RPel := AddRealPoints(RPelXLookup[pX], RPelYLookup[pY]);
      RPel := AddrealPoints(RPel, Datum);
      calc_result := CalcFract(RPel);
      FImagePortion.FractData[idx] := calc_result;
      Inc(idx);
      Inc(pX);
      if pX = OurRectWidth then
      begin
        Inc(pY);
        pX := 0;
        if pY = OurRectHeight then
        begin
          Assert(idx = NPels);
          NextPel := false;
        end;
      end;
    end;
  end;

  //Need to apply fract data to bitmap?
  if (Self.WorkItemState = wisExecuting) then
  begin
    idx := 0;
    Settings := FImagePortion.Renderer.Environment.HistoryCurrentBundle;
    for pY := 0 to Pred(FBitmap.Height) do
    begin
{$IF DEFINED (IS_FIREMONKEY)}
      PelPtr := FBitmap.BitmapData.GetScanline(pY);
{$ELSE}
      PelPtr := FBitmap.ScanLine[pY];
{$ENDIF}
      for pX := 0 to Pred(FBitmap.Width) do
      begin
        PelPtr^ := Settings.Palette.ShadePaletteLen(FImagePortion.FractData[idx]);
        Inc(idx);
        Inc(PelPtr);
      end;
    end;
    Assert(idx = NPels);
  end;
end;

procedure TFractWorkItem.DoNormalCompletion;
var
  R: TFractRenderer;
  DoNotify: boolean;
begin
  R := FImagePortion.Renderer;
  R.FResultQueue.AcquireLock;
  DoNotify := R.FResultQueue.Count = 0;
  R.FResultQueue.AddHeadObj(FBitmap);
  R.FResultQueue.ReleaseLock;
  FBitmap := nil;
  if DoNotify then
    ThreadRec.Thread.Queue(FImagePortion.Renderer.MainThreadResultMsgHandler);
end;

procedure TFractWorkItem.DoCancelledCompletion;
var
  R:TFractRenderer;
  DoNotify: boolean;
begin
  SetLength(FImagePortion.FractData, 0);
  R := FImagePortion.Renderer;
  R.FDiscardQueue.AcquireLock;
  DoNotify := R.FDiscardQueue.Count = 0;
  R.FDiscardQueue.AddHeadObj(FBitmap);
  R.FDiscardQueue.ReleaseLock;
  FBitmap := nil;
  if DoNotify then
    ThreadRec.Thread.Queue(FImagePortion.Renderer.MainThreadResultMsgHandler);
end;

{ TFractRenderer }

procedure TFractRenderer.StopThreads;
begin
  FWorkFarm.FlushAndWait(true);
  MainThreadFlushResults;
end;

procedure TFractRenderer.Progress;
begin
  if Assigned(FOnProgress) then
    FOnProgress(Self);
end;

constructor TFractRenderer.Create;
begin
  inherited;
  FWorkFarm := TWorkFarm.Create;
  FResultQueue := TDLProxyThreadQueue.Create;
  FDiscardQueue := TDLProxyThreadQueue.Create;
end;

destructor TFractRenderer.Destroy;
begin
  StopThreads;
  FWorkFarm.Free;
  FResultQueue.Free;
  FDiscardQueue.Free;
  inherited;
end;

procedure TFractRenderer.Stop;
begin
  StopThreads;
end;

{$IF Defined(MSWINDOWS)}
function TFractRenderer.GetThreadPriority: TThreadPriority;
begin
  result := FWorkFarm.ThreadPriority;
end;

procedure TFractRenderer.SetThreadPriority(NewPriority: TThreadPriority);
begin
  FWorkFarm.ThreadPriority := NewPriority;
end;

{$ELSEIF Defined(POSIX)}
function TFractRenderer.GetThreadPriority: integer;
begin
  result := FWorkFarm.ThreadPriority;
end;

procedure TFractRenderer.SetThreadPriority(NewPriority: integer);
begin
  FWorkFarm.ThreadPriority := NewPriority;
end;
{$ENDIF}

procedure TFractRenderer.MainThreadResultMsgHandler;
begin
  MainThreadPickupResults;
end;

function TFractRenderer.MainThreadPickupResults: boolean;
var
  Obj: TObject;
begin
  result := true;
  FDiscardQueue.AcquireLock;
  Obj := FDiscardQueue.RemoveTailObj;
  while Assigned(Obj) do
  begin
    Obj.Free;
    Obj := FDiscardQueue.RemoveTailObj;
  end;
  FDiscardQueue.ReleaseLock;
end;

procedure TFractRenderer.MainThreadFlushResults;
var
  Obj: TObject;
begin
  FResultQueue.AcquireLock;
  Obj := FResultQueue.RemoveTailObj;
  while Assigned(Obj) do
  begin
    Obj.Free;
    Obj := FResultQueue.RemoveTailObj;
  end;
  FResultQueue.ReleaseLock;

  FDiscardQueue.AcquireLock;
  Obj := FDiscardQueue.RemoveTailObj;
  while Assigned(Obj) do
  begin
    Obj.Free;
    Obj := FDiscardQueue.RemoveTailObj;
  end;
  FDiscardQueue.ReleaseLock;
end;

{ TFractStillRenderer }

procedure TFractStillRenderer.StopThreads;
var
  idx: integer;
begin
  inherited;
  for idx := Low(FPortions) to High(FPortions) do
    FPortions[idx].WorkItem.Reset;
end;

procedure TFractStillRenderer.StartThreads;
var
  idx: integer;
  WItems: array of TFractWorkItem;
begin
  InitPortionCount;
  InitPortionRects;
  SetLength(WItems, Length(FPortions));
  for idx := Low(FPortions) to High(FPortions) do
  begin
    WItems[idx] := FPortions[idx].WorkItem;
    WItems[idx].SetupLocatedBitmapMainThread;
  end;
  SetChunksStarted(Length(WItems));
  FWorkFarm.AddWorkItemBatch(@Witems[0], Length(WItems));
end;

procedure TFractStillRenderer.ResetChunkCount;
begin
  FChunksStarted := 0;
  FChunksDone := 0;
  FStartTime := 0;
  FLastChunkTime := 0;
  Progress;
end;

procedure TFractStillRenderer.SetChunksStarted(NewStarted: integer);
begin
  Assert(FChunksDone = 0);
  FChunksStarted := NewStarted;
  FStartTime := Now;
  FLastChunkTime := FStartTime;
  Progress;
end;

function TFractStillRenderer.IncChunksDone: boolean;
begin
  Assert(FChunksDone < FChunksStarted);
  Inc(FChunksDone);
  FLastChunkTime := Now;
  result := FChunksDone = FChunksStarted;
  if result then
    ResetChunkCount
  else
    Progress;
end;

function TFractStillRenderer.GetRunning: boolean;
begin
  Assert(FChunksDone <= FChunksStarted);
  result := FChunksDone < FChunksStarted;
end;

function TFractStillRenderer.GetPercentComplete: integer;
begin
  if FChunksStarted > 0 then
    result := (FChunksDone * 100) div FChunksStarted
  else
    result := 0;
  Assert(result >= 0);
  Assert(result <= 100);
end;

function TFractStillRenderer.GetSecsRemaining: integer;
var
  ChunksTodo: integer;
  TimeDone, TimeToGo: TDateTime;
begin
  result := 0;
  if (FChunksStarted > 0) and (FChunksDone > 0) then
  begin
    ChunksTodo := FChunksStarted - FChunksDone;
    TimeDone := FLastChunkTime - FStartTime;
    TimeToGo := ChunksToDo * (TimeDone / FChunksDone);
    result := Trunc(TimeToGo * 24 * 3600);
  end;
end;

procedure TFractStillRenderer.InvalidatePortionCount;
begin
  InvalidatePortionRects;
  SetPortionCount(0);
end;

procedure TFractStillRenderer.InvalidatePortionRects;
begin
  InvalidatePortionData;
  FPortionRectsValid := false;
end;

procedure TFractStillRenderer.InvalidatePortionData;
var
  idx: integer;
begin
  ResetChunkCount;
  for idx := Low(FPortions) to High(FPortions) do
    SetLength(FPortions[idx].FractData, 0);
end;

procedure TFractStillRenderer.SetPortionCount(NewPortionCount: cardinal);
var
  OldSq, NewSq: cardinal;
  idx: cardinal;
begin
  OldSq := FPortionCount * FPortionCount;
  Assert(cardinal(Length(FPortions)) = OldSq);
  NewSq := NewPortionCount * NewPortionCount;
  if OldSq > NewSq then
  begin
    Assert(not FPortionRectsValid); //Check we'll deffo recalc rects.
    for idx := NewSq to Pred(OldSq) do
    begin
      with FPortions[idx] do
      begin
        WorkItem.Free;
        SetLength(FractData, 0);
      end;
    end;
    SetLength(FPortions, NewSq);
  end
  else if OldSq < NewSq then
  begin
    Assert(not FPortionRectsValid); //Check we'll deffo recalc rects.
    SetLength(FPortions, NewSq);
    for idx := OldSq to Pred(NewSq) do
    begin
      with FPortions[idx] do
      begin
        Renderer := self;
        WorkItem := TFractWorkItem.Create;
        WorkItem.ImagePortion := @FPortions[idx];
        SetLength(FractData, 0);
      end;
    end;
  end;
  FPortionCount := NewPortionCount;
end;

procedure TFractStillRenderer.InitPortionCount;
begin
  SetPortionCount(FEnvironment.HistoryCurrentBundle.Formula.PortionCount);
end;

procedure TFractStillRenderer.InitPortionRects;
var
  x, y, i: cardinal;
  RX, NRX, Xinc: double;
  RY, NRY, Yinc: double;
  YStart, YEnd: integer;
  XStart, XEnd: integer;
  Settings: TFractV2SettingsBundle;
begin
  if FPortionRectsValid then
    exit;

  i := 0;
  RX := 0;
  Xinc := Bitmap.Width / FPortionCount;
  for x := 0 to Pred(FPortionCount) do
  begin
    NRX := RX + XInc;
    XStart := Trunc(RX);
    if x = Pred(FPortionCount) then
      XEnd := Bitmap.Width
    else
      XEnd := Trunc(NRX);
    Assert(XEnd > XStart);

    RY := 0;
    Yinc := Bitmap.Height / FPortionCount;
    for y := 0 to Pred(FPortionCount) do
    begin
      NRY := RY + Yinc;
      YStart := Trunc(RY);
      if y = Pred(FPortionCount) then
        YEnd := Bitmap.Height
      else
        Yend := Trunc(NRY);
      Assert(YEnd > YStart);
      Settings := FEnvironment.HistoryCurrentBundle;
      with FPortions[i] do
      begin
        PelRect.Left := XStart;
        PelRect.Right := XEnd;
        PelRect.Top := YStart;
        PelRect.Bottom := YEnd;
        ImgCenter := Settings.Location.Center;
        ImgTotalRect.x := Bitmap.Width;
        ImgTotalrect.y := Bitmap.Height;
        ImgScalePPU := Settings.Location.ScalePPU;
        ImgRotRads := Settings.Location.RotRadians;
        Assert(Length(FractData) = 0);
      end;

      Inc(i);
      RY := NRY;
    end;

    RX := NRX;
  end;
  FPortionRectsValid := true;
end;

procedure TFractStillRenderer.CompareAndInvalidate(OldSettings, NewSettings: TFractV2SettingsBundle);
var
  DiffPortionCount,
    DiffPortionRects,
    DiffPortionData: boolean;
begin
  Assert(Assigned(NewSettings));
  Assert(Assigned(NewSettings.Palette));
  OldSettings.CompareDifferencesForFrame(NewSettings,
    DiffPortionCount,
    DiffPortionRects,
    DiffPortionData);
  if DiffPortionCount then
    InvalidatePortionCount;
  if DiffPortionRects then
    InvalidatePortionRects;
  if DiffPortionData then
    InvalidatePortionData;
end;

procedure TFractStillRenderer.InvalidateAll;
begin
  InvalidatePortionCount;
  InvalidatePortionRects;
  InvalidatePortionData;
end;

constructor TFractStillRenderer.Create;
begin
  inherited;
  FEnvironment := TFractV2Environment.Create;
end;

destructor TFractStillRenderer.Destroy;
begin
  StopThreads;
  InvalidatePortionCount;
  FEnvironment.Free;
  inherited;
end;

procedure TFractStillRenderer.MainThreadFlushResults;
begin
  inherited;
  ResetChunkCount;
end;

{ TFractImageRenderer }

procedure TFractImageRenderer.StartThreads;
{$IF DEFINED (IS_FIREMONKEY)}
var
  BmpData: TBitmapData;
  i: integer;
  P:Pointer;
{$ENDIF}
begin
  if Assigned(FImage) then
  begin
{$IF DEFINED (IS_FIREMONKEY)}
  FImage.Bitmap.Map(TMapAccess.maReadWrite, BmpData);
  try
    for i := 0 to Pred(BmpData.Height) do
    begin
      P := BmpData.GetScanline(i);
      FillChar(P^, BmpData.BytesPerLine, $0);
    end;
  finally
    FImage.Bitmap.Unmap(BmpData);
  end;
  FImage.InvalidateRect(FImage.BoundsRect);
{$ELSE}
    FImage.Canvas.Brush.Color := clBtnFace;
    FImage.Canvas.FillRect(FImage.ClientRect);
{$ENDIF}
  end;
  inherited;
end;

procedure TFractImageRenderer.ZoomDefault;
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  NewSettings := Environment.HistoryNewBundleForChange(OldSettings);
  NewSettings.DoZoomDefault;
  NewSettings.HintString := 'Default Zoom';
  CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

procedure TFractImageRenderer.ZoomBy(LogScale: double);
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  NewSettings := Environment.HistoryNewBundleForChange(OldSettings);
  NewSettings.DoZoomBy(LogScale);
  if LogScale > 0 then
    NewSettings.HintString := 'Zoom In'
  else
    NewSettings.HintString := 'Zoom Out';
  CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

procedure TFractImageRenderer.ChangeDetail(PowerIncrement: integer);
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  NewSettings := Environment.HistoryNewBundleForChange(OldSettings);
  NewSettings.DoChangeDetail(PowerIncrement);
  if PowerIncrement > 0 then
    NewSettings.HintString := 'More detail'
  else
    NewSettings.HintString := 'Less detail';
  CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

function TFractImageRenderer.CanChangeDetail(PowerIncrement: integer):boolean;
begin
  //ReadOnly, so no thread sync needed.
  result := Environment.HistoryCurrentBundle.CanChangeDetail(PowerIncrement);
end;

procedure TFractImageRenderer.Recalc;
begin
  StopThreads;
  InvalidatePortionData;
  StartThreads;
end;

procedure TFractImageRenderer.Refresh;
begin
  StopThreads;
  StartThreads;
end;

procedure TFractImageRenderer.Undo;
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  OldSettings := Environment.HistoryCurrentBundle;
  Environment.HistoryUndo;
  NewSettings := Environment.HistoryCurrentBundle;
  CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

procedure TFractImageRenderer.ReDo;
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  OldSettings := Environment.HistoryCurrentBundle;
  Environment.HistoryRedo;
  NewSettings := Environment.HistoryCurrentBundle;
  CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

function TFractImageRenderer.CanUndo: boolean;
begin
  result := Environment.HistoryCanUndo;
end;

function TFractImageRenderer.CanReDo: boolean;
begin
  result := Environment.HistoryCanRedo;
end;

{$IF DEFINED (IS_FIREMONKEY)}
procedure TFractImageRenderer.MoveByPelsF(Offset: TPointF);
var
  Pt: TPoint;
begin
  Pt.X := Trunc(Offset.X);
  Pt.Y := Trunc(Offset.Y);
  MoveByPels(Pt);
end;

procedure TFractImageRenderer.ZoomAtByF(Location: TPointF; LogScale: double);
var
  Pt: TPoint;
begin
  Pt.X := Trunc(Location.X);
  Pt.Y := Trunc(Location.Y);
  ZoomAtBy(Pt, LogScale);
end;
{$ENDIF}

procedure TFractImageRenderer.MoveByPels(Offset: TPoint);
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
  OldCenter: TRealPoint;
  CenterDiff: TRealPoint;
begin
  StopThreads;
  NewSettings := Environment.HistoryNewBundleForChange(OldSettings);
  OldCenter := OldSettings.Location.Center;
  CenterDiff := IntPointToRealPoint(Offset, OldSettings.Location.ScalePPU);
  CenterDiff := RotateRealPt(CenterDiff, OldSettings.Location.RotRadians);
  NewSettings.Location.Center := SubRealPoints(OldCenter, CenterDiff);
  NewSettings.HintString := 'Move';
  CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

procedure TFractImageRenderer.RotateBy(Radians: double);
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  NewSettings := Environment.HistoryNewBundleForChange(OldSettings);
  NewSettings.DoRotateBy(Radians);
  NewSettings.HintString := 'Rotate';
  CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

procedure TFractImageRenderer.ZoomAtBy(Location: TPoint; LogScale: double);
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
  NewCenter: TRealPoint;
begin
  StopThreads;
  NewSettings := Environment.HistoryNewBundleForChange(OldSettings);
  NewCenter := ScreenPointToRealPoint(Location,
    Bitmap.Width,
    Bitmap.Height,
    OldSettings.Location.Center,
    OldSettings.Location.ScalePPU,
    OldSettings.Location.RotRadians);
  NewSettings.Location.Center := NewCenter;
  NewSettings.DoZoomBy(LogScale);
  NewSettings.HintString := 'Zoom at location';
  CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

procedure TFractImageRenderer.DrawParamsChanged;
begin
  InvalidatePortionRects;
  if Assigned(FImage) then
    StartThreads;
end;

procedure TFractImageRenderer.SetImage(NewImage: TSizeableImage);
begin
  StopThreads;
  if Assigned(FImage) then
    FImage.OnResize := nil;
  FImage := NewImage;
  if Assigned(FImage) then
  begin
    FImage.OnResize := self.HandleImageResize;
{$IF DEFINED (IS_FIREMONKEY)}
    Bitmap := FImage.Bitmap;
{$ELSE}
    Bitmap := FImage.Picture.Bitmap;
{$ENDIF}
    with Bitmap do
    begin
      //TODO - check this is not doing silly stuff.
{$IF DEFINED (IS_FIREMONKEY)}
      Width := Trunc(FImage.Width);
      Height := Trunc(FImage.Height);
{$ELSE}
      Width := FImage.Width;
      Height := FImage.Height;
{$ENDIF}
    end;
  end;
  DrawParamsChanged;
end;

procedure TFractImageRenderer.HandleImageResize(Sender: TObject);
begin
  if (FImage.Width > FPortionCount)
    and (FImage.Height > FPortionCount) then
  begin
    StopThreads;
    Assert(Assigned(Bitmap));
    Assert(Assigned(FImage));
{$IF DEFINED (IS_FIREMONKEY)}
    Bitmap.Width := Trunc(FImage.Width);
    Bitmap.Height := Trunc(FImage.Height);
{$ELSE}
    Bitmap.Width := FImage.Width;
    Bitmap.Height := FImage.Height;
{$ENDIF}
    DrawParamsChanged;
  end;
end;

procedure TFractImageRenderer.ChangeSettingsOnly(NewSettings: TFractV2SettingsBundle);
var
  IntNewSettings, OldSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  IntNewSettings := Environment.HistoryNewBundleForChange(OldSettings);
  IntNewSettings.Assign(NewSettings);
  IntNewSettings.HintString := 'Edit settings';
  CompareAndInvalidate(OldSettings, IntNewSettings);
  StartThreads;
end;

procedure TFractImageRenderer.ChangePalletteOnly(NewPalette: TFractPalette);
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  NewSettings := Environment.HistoryNewBundleForChange(OldSettings);
  NewSettings.Palette.Assign(NewPalette);
  NewSettings.HintString := 'Edit palette';
  CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

procedure TFractImageRenderer.FileLoadAll(NewSettings: TFractStreamable; var ErrStr: string);
var
  OldSettings, EndSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  OldSettings := Environment.FileLoadAllHelper(NewSettings, ErrStr);
  if Assigned(OldSettings) then
  begin
    try
      EndSettings := Environment.HistoryCurrentBundle;
      CompareAndInvalidate(OldSettings, EndSettings);
    finally
      OldSettings.Free;
    end;
  end;
  StartThreads;
end;

procedure TFractImageRenderer.FileLoadPalette(NewPalette: TFractStreamable; var ErrStr: string);
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  OldSettings := Environment.FileLoadPaletteHelper(NewPalette, NewSettings, ErrStr);
  NewSettings.HintString := 'Load Palette';
  if Assigned(OldSettings) then
    CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

function TFractImageRenderer.MainThreadPickupResults:boolean;
var
  Obj: TObject;
  Bmp: TLocatedBitmap;
{$IF DEFINED (IS_FIREMONKEY)}
  BmpRect: TRect;
  Origin: TPoint;
{$ENDIF}
begin
  result := false;
  ResultQueue.AcquireLock;
  Obj := ResultQueue.RemoveTailObj;
  ResultQueue.ReleaseLock;
  while Assigned(Obj) do
  begin
    Bmp := Obj as TLocatedBitmap;
{$IF DEFINED (IS_FIREMONKEY)}
    //TODO - another thing to check.
    Origin.X := 0;
    Origin.Y := 0;
    BmpRect.Create(Origin, Bmp.Width, Bmp.Height);
    Bmp.CopyIntoBitmap(FImage.Bitmap);
{$ELSE}
    FImage.Canvas.Draw(Bmp.FLocation.x,
      Bmp.FLocation.y,
      Bmp);
{$ENDIF}
    Obj.Free;
    result := IncChunksDone;
    ResultQueue.AcquireLock;
    Obj := ResultQueue.RemoveTailObj;
    ResultQueue.ReleaseLock;
    Assert((not result) or (not Assigned(Obj)));
  end;
  inherited;
end;

procedure TFractImageRenderer.PaletteCopyFromVideo(OtherRenderer: TFractVideoRenderer);
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
begin
  StopThreads;
  NewSettings := Environment.HistoryNewBundleForChange(OldSettings);
  NewSettings.Palette.Assign(OtherRenderer.Environment.Palette);
  NewSettings.HintString := 'Copy palette from video';
  CompareAndInvalidate(OldSettings, NewSettings);
  StartThreads;
end;

{ TFractBitmapRenderer }

function TFractBitmapRenderer.MainThreadPickupResults: boolean;
var
  Obj: TObject;
  Bmp: TLocatedBitmap;
begin
  result := false;
  ResultQueue.AcquireLock;
  Obj := FResultQueue.RemoveTailObj;
  ResultQueue.ReleaseLock;
  while Assigned(Obj) do
  begin
    Bmp := Obj as TLocatedBitmap;
    Bmp.CopyIntoBitmap(Bitmap);
    Obj.Free;
    result := IncChunksDone;
    ResultQueue.AcquireLock;
    Obj := ResultQueue.RemoveTailObj;
    ResultQueue.ReleaseLock;
    Assert((not result) or (not Assigned(Obj)));
  end;
  if Result then
  begin
    Bitmap.SaveToStream(FOutputStream);
    FSaved := true;
    Progress;
  end;
  inherited;
end;

procedure TFractBitmapRenderer.SetSize(NewSize: TPoint);
{$IF DEFINED (IS_FIREMONKEY)}
var
  BmpAccess: IBitmapAccess;
  SupBmpAccess: boolean;
{$ENDIF}
begin
  Assert(NewSize.x > 0);
  Assert(NewSize.y > 0);
  Bitmap.Width := NewSize.x;
  Bitmap.Height := NewSize.y;
{$IF DEFINED (IS_FIREMONKEY)}
  SupBmpAccess := Supports(Bitmap, IBitmapAccess, BmpAccess);
  Assert(SupBmpAccess);
  BmpAccess.SetPixelFormat(pfA8R8G8B8);
{$ELSE}
  Bitmap.PixelFormat := pf32Bit;
{$ENDIF}
  InvalidatePortionRects;
end;

function TFractBitmapRenderer.GetSize: TPoint;
begin
  result.x := Bitmap.Width;
  result.y := Bitmap.Height;
end;

constructor TFractBitmapRenderer.Create;
begin
  inherited;
{$IF DEFINED (IS_FIREMONKEY)}
  Bitmap := TBitmap.Create(0,0); //Size set later.
{$ELSE}
  Bitmap := TBitmap.Create;
{$ENDIF}
end;

destructor TFractBitmapRenderer.Destroy;
begin
  Bitmap.Free;
  inherited;
end;


procedure TFractBitmapRenderer.StartRender;
begin
  StartThreads;
end;

procedure TFractBitmapRenderer.CancelRender;
begin
  StopThreads;
end;

procedure TFractBitmapRenderer.FileLoadAll(NewSettings: TFractStreamable; var ErrStr: string);
var
  OldSettings, EndSettings: TFractV2SettingsBundle;
begin
  Assert(not Running);
  OldSettings := Environment.FileLoadAllHelper(NewSettings, ErrStr);
  if Assigned(OldSettings) then
  begin
    try
      EndSettings := Environment.HistoryCurrentBundle;
      CompareAndInvalidate(OldSettings, EndSettings);
    finally
      OldSettings.Free;
    end;
  end;
end;

procedure TFractBitmapRenderer.FileLoadPalette(NewPalette: TFractStreamable; var ErrStr: string);
var
  OldSettings, NewSettings: TFractV2SettingsBundle;
begin
  Assert(not Running);
  OldSettings := Environment.FileLoadPaletteHelper(NewPalette, NewSettings, ErrStr);
  if Assigned(OldSettings) then
    CompareAndInvalidate(OldSettings, NewSettings);
end;

{ TFractVideoRenderer }

{ TODO - these will require more specialisation than just passthru once
  renderer completed. }

constructor TFractVideoRenderer.Create;
begin
  inherited;
  FEnvironment := TFractV2VideoEnv.Create;
end;

destructor TFractVideoRenderer.Destroy;
begin
  FEnvironment.Free;
  inherited;
end;

procedure TFractVideoRenderer.PaletteCopyFromStill(OtherRenderer: TFractStillRenderer);
begin
  Assert(not Running);
  FEnvironment.Palette.Assign(OtherRenderer.Environment.HistoryCurrentBundle.Palette);
end;

procedure TFractVideoRenderer.WaypointAppend(OtherRenderer: TFractStillRenderer);
begin
  Assert(not Running);
  FEnvironment.WaypointAppendFromHistoryCurrent(OtherRenderer.Environment);
end;

procedure TFractVideoRenderer.WaypointInsert(OtherRenderer: TFractStillRenderer);
begin
  Assert(not Running);
  FEnvironment.WaypointInsertFromHistoryCurrent(OtherRenderer.Environment);
end;

procedure TFractVideoRenderer.WaypointMove(NewIdx: cardinal);
begin
  Assert(not Running);
  FEnvironment.WaypointMove(NewIdx);
end;

procedure TFractVideoRenderer.WaypointDelete;
begin
  Assert(not Running);
  FEnvironment.WaypointDelete;
end;

procedure TFractVideoRenderer.WaypointClearAll;
begin
  Assert(not Running);
  FEnvironment.WaypointClearAll;
end;

function TFractVideoRenderer.WaypointCanDelete: boolean;
begin
  //TODO - running check sensible here?
  result := FEnvironment.WaypointCanDelete;
end;

function TFractVideoRenderer.WaypointCanMove(NewIdx:cardinal): boolean;
begin
  //TODO - running check sensible here?
  result := FEnvironment.WaypointCanMove(NewIdx);
end;


{ TLocatedBitmap }

procedure TLocatedBitmap.CopyIntoBitmap(Dest: TBitmap);
var
  DstCordsDstRect: TRect;
  SrcCordsSrcRect: TRect;
  DstCordsSrcRect: TRect;
  DstCordsCopyRect: TRect;
  SrcCordsCopyRect: TRect;
  InvLocation: TPoint;
  CopyWidth, CopyHeight: integer;
  x, y: integer;
  pSrc, pDst: PPackedColor;
{$IF DEFINED (IS_FIREMONKEY))}
  DestData: TBitmapData;
  DestMapped: boolean;
{$ENDIF}
begin
  with DstCordsDstRect do
  begin
    Left := 0;
    Top := 0;
    Right := Dest.Width;
    Bottom := Dest.Height;
  end;
  with SrcCordsSrcRect do
  begin
    Left := 0;
    Top := 0;
    Right := self.Width;
    Bottom := self.Height;
  end;
  with InvLocation do
  begin
    x := -FLocation.x;
    y := -FLocation.y;
  end;

  DstCordsSrcRect := SrcCordsSrcRect;
  OffsetRect(DstCordsSrcRect, FLocation.X, FLocation.Y);
  IntersectRect(DstCordsCopyRect, DstCordsDstRect, DstCordsSrcRect);
  SrcCordsCopyRect := DstCordsCopyRect;
  OffsetRect(SrcCordsCopyRect, InvLocation.X, InvLocation.Y);

  CopyWidth := DstCordsCopyRect.Right - DstCordsCopyRect.Left;
  CopyHeight := DstCordsCopyrect.Bottom - DstCordsCopyRect.Top;

{$IF DEFINED (IS_FIREMONKEY))}
  Assert(PixelFormat = pfA8R8G8B8);
  Assert(Dest.PixelFormat = pfA8R8G8B8);
{$ELSE}
  Assert(PixelFormat = pf32Bit);
  Assert(Dest.PixelFormat = pf32Bit);
{$ENDIF}
  Assert(CopyWidth >= 0);
  Assert(CopyHeight >= 0);
{$IF DEFINED (IS_FIREMONKEY))}
  DestMapped := Dest.Map(TMapAccess.maWrite, DestData);
  try
{$ENDIF}
  for y := 0 to Pred(CopyHeight) do
  begin
{$IF DEFINED (IS_FIREMONKEY))}
    pSrc := BitmapData.GetScanline(SrcCordsCopyRect.Top + y);
    pDst := DestData.GetScanline(DstCordsCopyRect.Top + y);
{$ELSE}
    pSrc := ScanLine[SrcCordsCopyRect.Top + y];
    pDst := Dest.ScanLine[DstCordsCopyRect.Top + y];
{$ENDIF}
    Inc(pSrc, SrcCordsCopyRect.Left);
    Inc(pDst, DstCordsCopyRect.Left);
    for x := 0 to Pred(CopyWidth) do
    begin
      pDst^ := pSrc^;
      Inc(pSrc);
      Inc(pDst);
    end;
  end;
{$IF DEFINED (IS_FIREMONKEY))}
  finally
    if DestMapped then Dest.Unmap(DestData);
  end;
{$ENDIF}
end;

destructor TLocatedBitmap.Destroy;
begin
{$IF DEFINED(IS_FIREMONKEY)}
  if FBitmapDataMapped then
  begin
    Unmap(FBitmapData);
    FBitmapDataMapped := false;
  end;
{$ENDIF}
  inherited;
end;


end.
