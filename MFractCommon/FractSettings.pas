unit FractSettings;

{ Martin Harvey 16 May 2013 }

interface

uses
  SysUtils, System.Types, System.UITypes, SSStreamables, DLList, SyncObjs,
  StreamingSystem, Classes,
{$IF DEFINED(IS_FIREMONKEY)}
  FMX.Types,
{$ELSE}
  VCL.Graphics,
{$ENDIF}
  FractMisc
  ;

const
  InitialZoom = 100.0;
  InitialMult = 4.0;
  InitialBailoutOrder = 8;
  InitialPortions = 8;
  InitialMandelX = -0.5;
  InitialMandelY = 0;
  InitialJuliaX = 0;
  InitialJuliaY = 0;
  MaxColorLookupOrder = 16;
  MinBailoutOrder = 6;
  MaxBailoutOrder = 30;
  MinPaletteOrder = 4;
  MaxPaletteOrder = 32;
  DefaultPaletteOrder = 8;

  PaletteVersionTag = 2;
  SettingsVersionTag = 2;


type
  TColorMarkData = record
    Color: TPackedColor;
    Loc: double; { 0 <= Loc < 1 }
  end;

  //Version tags can be used by compatibility code in any way desired.
  TFractVersionTag = class(TObjStreamable)
  private
    FVersion, FVersion2: cardinal;
  public
    procedure Assign(Source: TObjStreamable); override;
  published
    property Version:cardinal read FVersion write FVersion;
    property Version2:cardinal read FVersion2 write FVersion2;
  end;

  TFractStreamable = class(TObjStreamable)
  protected
    FVerTag: TFractVersionTag;
  public
    destructor Destroy; override;
    function CheckSane(var Err: string): boolean; virtual;
    procedure Assign(Source: TObjStreamable); override; //Assign copies tag too.
    procedure AssignTo(Dest: TObjStreamable); override; //So can call up inheritance with no err.
  published
    property VerTag: TFractVersionTag read FVerTag write FVerTag;
  end;

  //Color marks should be in a list that is always sorted.
  TColorMark = class(TFractStreamable)
  protected
    function GetMarkLoc: double;
    procedure SetMarkLoc(Loc: double);
    function GetMarkColor: TColor;
    procedure SetMarkColor(Color: TColor);
  public
    Link: TDLEntry;
    Data: TColorMarkData;
    constructor Create; override;
    procedure Assign(Source: TObjStreamable); override;
    function CheckSane(var Err: string): boolean; override;
  published
    property STREAM_MarkLoc: double read GetMarkLoc write SetMarkLoc;
    property STREAM_Color: TColor read GetMarkColor write SetMarkColor;
  end;

  TColorLookup = array of TPackedColor;

  TFractPalette = class(TFractStreamable)
  private
    FHead: TDLEntry;
    FLengthOrder: cardinal;
    FColorLookup: TColorLookup;
    FLookupValid: boolean;
    FMarkCount: cardinal;
    FLock: TCriticalSection;
    FLogarithmic: boolean;
  protected
    procedure DebugValidateMarkList;
    procedure RefreshLookup;
    procedure InvalidateLookup;
    function GetColorMark(Idx: cardinal): TColorMarkData;
    function GetNumMarks: cardinal;
    procedure SetLengthOrder(NewOrder: cardinal);
    procedure SetLogarithmic(NewLogarithmic: boolean);

    procedure CustomMarshal(Sender: TDefaultSSController); override;
    procedure CustomUnmarshal(Sender: TDefaultSSController); override;
  public
    function CheckSane(var Err: string): boolean; override;
    procedure Assign(Source: TObjStreamable); override;
    function ShadePaletteLen(Iters: cardinal): TPackedColor;
    function ShadeArbitraryLen(Val: cardinal; Len: cardinal): TPackedColor;
    function ShadeRealLoc(Loc: double): TPackedColor;
    constructor Create; override;
    destructor Destroy; override;

    procedure SetColorMark(Data: TColorMarkData);
    procedure DeleteMark(Idx: cardinal);
    procedure ResetAllMarks;

    property NumMarks: cardinal read GetNumMarks;
    property ColorMarks[Idx: cardinal]: TColorMarkData read GetColorMark;
  published
    property LengthOrder: cardinal read FLengthOrder write SetLengthOrder;
    property Logarithmic: boolean read FLogarithmic write SetLogarithmic;
  end;

  TFractFormula = (ffMandel, ffJulia);

  TFractItemList = class(TFractStreamable)
  private
    FItems: TList;
    FItemType: TObjStreamableClass;
  protected
    procedure SetStreamableClassType(CType: TObjStreamableClass);
    procedure CustomMarshal(Sender: TDefaultSSController); override;
    procedure CustomUnmarshal(Sender: TDefaultSSController); override;
    function GetCapacity: integer;
    procedure SetCapacity(NewCapacity: integer);
    function GetCount: integer;
    procedure SetCount(NewCount: integer);
    function Get(Index: integer): TObjStreamable;
    procedure Put(Index: integer; New: TObjStreamable);
  public
    constructor Create; override;
    destructor Destroy; override;
    procedure Assign(Source: TObjStreamable); override;
    function CheckSane(var Err: string): boolean; override;
    function Add(Item: TObjStreamable): Integer;
    procedure Clear; virtual;
    procedure Delete(Index: Integer);
    procedure Exchange(Index1, Index2: Integer);
    function Expand: TFractItemList;
    function Extract(Item: TObjStreamable): TObjStreamable;
    function First: TObjStreamable;
    function IndexOf(Item: TObjStreamable): Integer;
    procedure Insert(Index: Integer; Item: TObjStreamable);
    function Last: TObjStreamable;
    procedure Move(CurIndex, NewIndex: Integer);
    function Remove(Item: TObjStreamable): Integer;
    procedure Pack;
    procedure Sort(Compare: TListSortCompare);
    property Capacity: Integer read GetCapacity write SetCapacity;
    property Count: Integer read GetCount write SetCount;
    property Items[Index: Integer]: TObjStreamable read Get write Put;
  end;

  TFractV2HistoryList = class(TFractItemList)
  public
    constructor Create; override;
  end;

  TFractV2WaypointList = class(TFractItemList)
  public
    constructor Create; override;
  end;

  TFractV2Comparable = class(TFractStreamable)
  public
    procedure CompareDifferencesForFrame(Other: TFractV2Comparable;
      var DiffPortionCount: boolean;
      var DiffPortionRects: boolean;
      var DiffPortionData: boolean); virtual;
  end;

  TFractV2LocationSettings = class(TFractV2Comparable)
  private
    FCenter: TRealPoint;
    FScalePPU: double;
    FRotRadians: double;
    FBailoutOrder: cardinal;
  protected
    procedure SetBailoutOrder(NewOrder: cardinal);
    procedure SetCenterX(X: double);
    procedure SetCenterY(Y: double);
    function GetCenterX: double;
    function GetCenterY: double;
    procedure SetRotRadians(NewRadians: double);
    function GetDescString: string;
  public
    function CheckSane(var Err: string): boolean; override;
    constructor Create; override; //Make some absolute defaults sensible.
    procedure CompareDifferencesForFrame(Other: TFractV2Comparable;
      var DiffPortionCount: boolean;
      var DiffPortionRects: boolean;
      var DiffPortionData: boolean); override;
    procedure Assign(Source: TObjStreamable); override;
    property DescString: string read GetDescString;
  published
    property Center: TRealPoint read FCenter write FCenter;
    property ScalePPU: double read FScalePPU write FScalePPU;
    property BailoutOrder: cardinal read FBailoutOrder write SetBailoutOrder;
    property RotRadians: double read FRotRadians write SetRotRadians;

    property STREAM_CenterX: double read GetCenterX write SetCenterX;
    property STREAM_CenterY: double read GetCenterY write SetCenterY;
  end;

  TFractV2FormulaSettings = class(TFractV2Comparable)
  private
    FJuliaConst: TRealPoint;
    FMultPPU: double;
    FSavedOtherPPU: double;
    FInitialPPU: double;
    FPortionCount: cardinal;
    FFormula: TFractFormula;
  protected
    procedure SetJuliaConstX(X: double);
    procedure SetJuliaConstY(Y: double);
    function GetJuliaConstX: double;
    function GetJuliaConstY: double;
  public
    function CheckSane(var Err: string): boolean; override;
    constructor Create; override; //Make some absolute defaults sensible.
    procedure CompareDifferencesForFrame(Other: TFractV2Comparable;
      var DiffPortionCount: boolean;
      var DiffPortionRects: boolean;
      var DiffPortionData: boolean); override;
    procedure Assign(Source: TObjStreamable); override;
  published
    property Formula: TFractFormula read FFormula write FFormula;
    property JuliaConst: TRealPoint read FJuliaConst write FJuliaConst;
    property MultPPU: double read FMultPPU write FMultPPU;
    property InitialPPU: double read FInitialPPU write FInitialPPU;
    property PortionCount: cardinal read FPortionCount write FPortionCount;
    property SavedOtherPPU:double read FSavedOtherPPU write FSavedOtherPPU;

    property STREAM_JuliaConstX: double read GetJuliaConstX write SetJuliaConstX;
    property STREAM_JuliaConstY: double read GetJuliaConstY write SetJuliaConstY;
  end;

  TFractV2SettingsBundle = class(TFractV2Comparable)
  private
    FPalette: TFractPalette;
    FLocation: TFractV2LocationSettings;
    FFormula: TFractV2FormulaSettings;
    FHintString: string;
  protected
  public
    function CheckSane(var Err: string): boolean; override;
    constructor Create; override; //Make some absolute defaults sensible.
    destructor Destroy; override;
    procedure CompareDifferencesForFrame(Other: TFractV2Comparable;
      var DiffPortionCount: boolean;
      var DiffPortionRects: boolean;
      var DiffPortionData: boolean); override;
    procedure Assign(Source: TObjStreamable); override;
    function GetDescString: string;

    procedure DoZoomDefault;
    procedure DoZoomBy(LogScale: double);
    procedure DoRotateBy(Radians: double);
    procedure DoChangeDetail(PowerIncrement: integer);
    function CanChangeDetail(PowerIncrement: integer): boolean;
    procedure FlipJulia;
    property DescString: string read GetDescString;
  published
    property Palette: TFractPalette read FPalette write FPalette;
    property Location: TFractV2LocationSettings
      read FLocation write FLocation;
    property Formula: TFractV2FormulaSettings
      read FFormula write FFormula;
    property HintString: string read FHintString write FHintString;
  end;

  //Strictly speaking don't need all of streamable
  //capabilities here, but hey, might come in useful.
  TFractV2Environment = class(TFractStreamable)
  private
    FHistoryList: TFractV2HistoryList;
    FHistoryIndex: cardinal;
  protected
    function HistoryGetDescString(Idx: integer): string;
    function HistoryGetCurrentBundle(): TFractV2SettingsBundle;
    function HistoryGetCount: cardinal;
  public
    function CheckSane(var Err: string): boolean; override;
    constructor Create; override;
    destructor Destroy; override;
    procedure Assign(Source: TObjStreamable); override;
    procedure CustomMarshal(Sender: TDefaultSSController); override;
    procedure CustomUnmarshal(Sender: TDefaultSSController); override;

    { Undo / Redo and History functionality }
    function HistoryNewBundleForChange(var LastBundle:TFractV2SettingsBundle):TFractV2SettingsBundle;
    procedure HistoryClear;
    procedure HistoryUndo;
    procedure HistoryRedo;
    function HistoryCanUndo: boolean;
    function HistoryCanRedo: boolean;
    function HistoryCanClear: boolean;
    property HistoryCount: cardinal read HistoryGetCount;
    property HistoryCurrentBundle: TFractV2SettingsBundle read HistoryGetCurrentBundle;
    property HistoryStrings[Idx: Integer]:string read HistoryGetDescString;
    property HistoryIndex: cardinal read FHistoryIndex;

    //Load helper returns old settings for renderer comparison purposes.
    function FileLoadAllHelper(LoadClass: TFractStreamable; var ErrStr: string): TFractV2SettingsBundle;
    function FileLoadPaletteHelper(LoadClass: TFractStreamable;
      var NewSettings:TFractV2SettingsBundle; var ErrStr: string): TFractV2SettingsBundle;
    function MakeSaveableCopy:TFractV2Environment;
  published
  end;

  TFractV2VideoEnv = class(TFractStreamable)
  private
    FWaypointList: TFractV2WaypointList;
    FPalette: TFractPalette;
    FFormula: TFractV2FormulaSettings;
    FCursor: cardinal;
  protected
    function WaypointGetDescString(Idx: integer): string;
    function WaypointGetCount: cardinal;
    function WaypointGetCursor: cardinal;
    procedure WaypointSetCursor(NewCursor: cardinal);
  public
    constructor Create; override;
    destructor Destroy; override;
    function CheckSane(var Err: string): boolean; override;
    procedure Assign(Source: TObjStreamable); override;

    procedure WaypointInsertFromHistoryCurrent(Env: TFractV2Environment); //Adds at cursor;
    procedure WaypointAppendFromHistoryCurrent(Env: TFractV2Environment); //Appends to end;
    procedure WaypointMove(NewIdx: cardinal); //Moves with cursor;
    procedure WaypointDelete; //Deletes at cursor;
    procedure WaypointClearAll;

    function WaypointCanDelete: boolean;
    function WaypointCanMove(NewIdx:cardinal): boolean;

    procedure GetPalleteFromStillEnv(OtherEnv: TFractV2Environment);
    procedure GetFormulaFromEnv(OtherEnv: TFractV2Environment);

    //TODO later - load and save.

    property WaypointCount: cardinal read WaypointGetCount;
    property WaypointStrings[Idx: integer]:string read WaypointGetDescString;
    property Cursor: cardinal read WaypointGetCursor write WaypointSetCursor;

    property Palette: TFractPalette read FPalette;
    property Formula: TFractV2FormulaSettings read FFormula;
  published
  end;

  EWaypointError = class(Exception);

implementation

uses
  Math, SSAbstracts;

{ TFractVersionTag }

procedure TFractVersionTag.Assign(Source: TObjStreamable);
var
  Src: TFractVersionTag;
begin
  if Assigned(Source) and (Source is TFractVersionTag) then
  begin
    Src := TFractVersionTag(Source);
    FVersion := Src.FVersion;
    FVersion2 := Src.FVersion2;
  end;
  inherited;
end;

{ TFractStreamable }

destructor TFractStreamable.Destroy;
begin
  FVerTag.Free;
  inherited;
end;

function TFractStreamable.CheckSane(var Err: string): boolean;
begin
  result := true;
end;

procedure TFractStreamable.Assign(Source: TObjStreamable);
begin
  //DO NOT Assign version tags across. They are there
  //precisely for differentiation / compat reasons.
  //The assignent functions are a major part of the
  //load/save process.
  inherited;
end;

procedure TFractStreamable.AssignTo(Dest: TObjStreamable);
begin
  //Do NOT call inherited method, so no error generated.
end;

{ TColorMark }

constructor TColorMark.Create;
begin
  inherited;
  DLItemInitObj(Self, @Link);
end;

procedure TColorMark.Assign(Source: TObjStreamable);
var
  Src: TColorMark;
begin
  if Assigned(Source) and (Source is TColorMark) then
  begin
    Src := TColorMark(Source);
    Self.Data := Src.Data;
  end;
  inherited;
end;

function TColorMark.CheckSane(var Err: string): boolean;
begin
  result := false;
  if not inherited CheckSane(Err) then exit;
  if (Data.Loc < 0) or (Data.Loc >= 1) then
  begin
    Err := 'Color Mark location out of range';
    exit;
  end;
  result := true;
end;

function TColorMark.GetMarkLoc: double;
begin
  result := Data.Loc;
end;

procedure TColorMark.SetMarkLoc(Loc: double);
begin
  Data.Loc := Loc;
end;

function TColorMark.GetMarkColor: TColor;
begin
  result := PackedColorToColor(Data.Color);
end;

procedure TColorMark.SetMarkColor(Color: TColor);
begin
  Data.Color := ColorToPackedColor(Color);
end;

{ TFractPalette }

constructor TFractPalette.Create;
begin
  inherited;
  if not CreatingInStreamer then
  begin
    FVerTag := TFractVersionTag.Create;
    FVerTag.Version := PaletteVersionTag;
  end;
  DLItemInitList(@FHead);
  FLock := TCriticalSection.Create;
  FLengthOrder := 8;
  ResetAllMarks;
end;

destructor TFractPalette.Destroy;
var
  Entry: PDLEntry;
begin
  Entry := DLListRemoveHead(@FHead);
  while Assigned(Entry) do
  begin
    Entry.Owner.Free;
    Entry := DLListRemoveHead(@FHead);
  end;
  FLock.Free;
  inherited;
end;

procedure TFractPalette.CustomMarshal(Sender: TDefaultSSController);
var
  Ent: PDLEntry;
begin
  inherited;
  Sender.StreamArrayStart('ColorMarkArray');
  Ent := FHead.FLink;
  while Ent <> @FHead do
  begin
    Sender.StreamClass('', Ent.Owner);
    Ent := Ent.FLink;
  end;
  Sender.StreamArrayEnd('ColorMarkArray');
end;

procedure TFractPalette.CustomUnmarshal(Sender: TDefaultSSController);
var
  Count, Idx: integer;
  Obj: TObject;
  Entry: PDLEntry;
begin
  inherited;
  //Remove all our own items.
  Entry := DLListRemoveHead(@FHead);
  while Assigned(Entry) do
  begin
    Entry.Owner.Free;
    Entry := DLListRemoveHead(@FHead);
  end;
  FMarkCount := 0;

  //And unstream marks.
  Sender.UnstreamArrayStart('ColorMarkArray', Count);
  for idx := 0 to Pred(Count) do
  begin
    Sender.UnstreamClass('', Obj);
    if not (Obj is TColorMark) then
      Sender.UserSignalError(sssError, 'Bad class type for color mark');
    DLListInsertTail(@FHead, @(Obj as TColorMark).Link);
    Inc(FMarkCount);
  end;
  Sender.UnstreamArrayEnd('ColorMarkArray');
  InvalidateLookup;
end;

procedure TFractPalette.Assign(Source: TObjStreamable);
var
  Src: TFractPalette;
  SrcItemClass: TObjStreamableClass;
  Entry: PDLEntry;
  DupItem: TObjStreamable;
begin
  if Assigned(Source) and (Source is TFractPalette) then
  begin
    Src := TFractPalette(Source);
    //Remove all our own items.
    Entry := DLListRemoveHead(@FHead);
    while Assigned(Entry) do
    begin
      Entry.Owner.Free;
      Entry := DLListRemoveHead(@FHead);
    end;
    FMarkCount := 0;
    //Clone src items.
    Entry := Src.FHead.FLink;
    while Entry <> @Src.FHead do
    begin
      SrcItemClass := TObjStreamableClass(Entry.Owner.ClassType);
      DupItem := SrcItemClass.Create;
      DupItem.Assign(Entry.Owner as TObjStreamable);
      //Remembering to insert in correct order.
      DLListInsertTail(@FHead, @(DupItem as TColorMark).Link);
      Entry := Entry.FLink;
      Inc(FMarkCount);
    end;
    FLengthOrder := Src.FLengthOrder;
    FLogarithmic := Src.FLogarithmic;
    InvalidateLookup;
    DebugValidateMarkList;
  end;
  inherited;
end;

function TFractPalette.CheckSane(var Err: string): boolean;
var
  MarkCount: cardinal;
  PEntry: PDLEntry;
  Mark, LastMark: TColorMark;
begin
  result := false;
  MarkCount := 0;
  PEntry := FHead.FLink;
  LastMark := nil;
  while Pentry <> @FHead do
  begin
    Mark := PEntry.Owner as TColorMark;
    Inc(MarkCount);
    if not Mark.CheckSane(Err) then
      exit;
    if Assigned(LastMark) then
    begin
      if Mark.Data.Loc <= LastMark.Data.Loc then
      begin
        Err := 'Color mark locations in wrong order.';
        exit;
      end;
    end;
    LastMark := Mark;
    PEntry := PEntry.FLink;
  end;
  if not (MarkCount > 0) then
  begin
    Err := 'Palette has no color marks.';
    exit;
  end;
  if not (MarkCount = FMarkCount) then
  begin
    Err := 'Palette has inconsistent internal mark count';
    exit;
  end;
  if (FLengthOrder < MinPaletteOrder)
    or (FLengthOrder > MaxPaletteOrder) then
  begin
    Err := 'Palette size power bad.';
    exit;
  end;
  result := true;
end;

procedure TFractPalette.DebugValidateMarkList;
{$IFOPT C+}
var
  DummyStr: string;
{$ENDIF}
begin
{$IFOPT C+}
  Assert(CheckSane(DummyStr));
{$ENDIF}
end;

procedure TFractPalette.RefreshLookup;
var
  PaletteLen: cardinal;
  PrevMark, NextMark: TColorMark;
  PrevMarkLoc, NextMarkLoc, ThisLoc: double;
  idx: cardinal;
  PrevMarkRCol, NextMarkRCol, RCol: TRealColor;
  DistPrev, DistNext: double;
begin
  if FLookupValid then
    exit;
  FLock.Acquire;
  if FLookupValid then
  begin
    FLock.Release;
    exit;
  end;
  DebugValidateMarkList;
  if FLengthOrder > MaxColorLookupOrder then
    PaletteLen := MaxColorLookupOrder
  else
    PaletteLen := FLengthOrder;
  PaletteLen := Pow2Val(PaletteLen) + 1;
  SetLength(FColorLookup, PaletteLen);
  PrevMark := (FHead.BLink.Owner) as TColorMark;
  NextMark := (FHead.FLink.Owner) as TColorMark;
  PrevMarkLoc := PrevMark.Data.Loc - 1;
  NextMarkLoc := NextMark.Data.Loc;
  PrevMarkRCol := IntColorToRealColor(PrevMark.Data.Color);
  NextMarkRCol := IntColorToRealColor(NextMark.Data.Color);
  for idx := 0 to Pred(PaletteLen) do
  begin
    ThisLoc := idx / PaletteLen;
    if ThisLoc >= NextMarkLoc then
    begin
      PrevMark := NextMark;
      PrevMarkLoc := NextMarkLoc;
      if NextMark.Link.FLink = @FHead then
      begin
        NextMark := (FHead.FLink.Owner) as TColorMark;
        NextMarkLoc := NextMark.Data.Loc + 1;
      end
      else
      begin
        NextMark := NextMark.Link.Flink.Owner as TColorMark;
        NextMarkLoc := NextMark.Data.Loc;
      end;
      PrevMarkRCol := IntColorToRealColor(PrevMark.Data.Color);
      NextMarkRCol := IntColorToRealColor(NextMark.Data.Color);
    end;
    DistPrev := ThisLoc - PrevMarkLoc;
    DistNext := NextMarkLoc - ThisLoc;
    DistPrev := DistPrev / (NextMarkLoc - PrevMarkLoc);
    DistNext := DistNext / (NextMarkLoc - PrevMarkLoc);
    RCol := RealColorAdd(RealColorMult(PrevMarkRCol, (1 - DistPrev)),
      RealColorMult(NextMarkRCol, (1 - DistNext)));
    FColorLookup[idx] := RealColorToIntColor(RCol);
  end;
  FLookupValid := true;
  FLock.Release;
end;

function TFractPalette.ShadeRealLoc(Loc: double): TPackedColor;
var
  PrevMark, NextMark: TColorMark;
  PrevMarkLoc, NextMarkLoc: double;
  PrevMarkRCol, NextMarkRCol, RCol: TRealColor;
  DistPrev, DistNext: double;
begin
  PrevMark := (FHead.BLink.Owner) as TColorMark;
  NextMark := (FHead.FLink.Owner) as TColorMark;
  PrevMarkLoc := PrevMark.Data.Loc - 1;
  NextMarkLoc := NextMark.Data.Loc;
  while Loc >= NextMarkLoc do
  begin
    PrevMark := NextMark;
    PrevMarkLoc := NextMarkLoc;
    if NextMark.Link.FLink = @FHead then
    begin
      NextMark := (FHead.FLink.Owner) as TColorMark;
      NextMarkLoc := NextMark.Data.Loc + 1;
    end
    else
    begin
      NextMark := NextMark.Link.Flink.Owner as TColorMark;
      NextMarkLoc := NextMark.Data.Loc;
    end;
  end;
  PrevMarkRCol := IntColorToRealColor(PrevMark.Data.Color);
  NextMarkRCol := IntColorToRealColor(NextMark.Data.Color);
  DistPrev := Loc - PrevMarkLoc;
  DistNext := NextMarkLoc - Loc;
  DistPrev := DistPrev / (NextMarkLoc - PrevMarkLoc);
  DistNext := DistNext / (NextMarkLoc - PrevMarkLoc);
  RCol := RealColorAdd(RealColorMult(PrevMarkRCol, (1 - DistPrev)),
    RealColorMult(NextMarkRCol, (1 - DistNext)));
  result := RealColorToIntColor(RCol);
end;


function TFractPalette.ShadeArbitraryLen(Val: cardinal; Len: cardinal):
  TPackedColor;
var
  Loc: double;
begin
  Assert(Val < Len);
  Loc := Val / Len;
  result := ShadeRealLoc(Loc);
  //Alpha channel.
  result.Spare := $FF;
end;

function TFractPalette.ShadePaletteLen(Iters: cardinal): TPackedColor;
var
  PaletteIdx: cardinal;
  PaletteOrder, OrderDif: cardinal;
  IdealMask: cardinal;
{$IFOPT C+}
  PaletteMask: cardinal;
{$ENDIF}
begin
  RefreshLookup;
  if FLengthOrder > MaxColorLookupOrder then
    PaletteOrder := MaxColorLookupOrder
  else
    PaletteOrder := FLengthOrder;
  IdealMask := Pow2Val(FLengthOrder);
{$IFOPT C+}
  PaletteMask := Pow2Val(PaletteOrder);
  Assert(Cardinal(Length(FColorLookup)) - 1 = PaletteMask);
{$ENDIF}
  if FLogarithmic then
  begin
    PaletteIdx := Iters;
    while PaletteIdx > IdealMask do
    begin
      Dec(PaletteIdx, IdealMask+1);
      PaletteIdx := PaletteIdx shr 1;
    end;
  end
  else
  begin
    PaletteIdx := Iters and IdealMask;
  end;
  OrderDif := FLengthOrder - PaletteOrder;
  PaletteIdx := PaletteIdx shr OrderDif;
  Assert(Integer(PaletteIdx) < Length(FColorLookup));
  result := FColorLookup[PaletteIdx];
  //Alpha channel.
  result.Spare := $FF;
end;

procedure TFractPalette.SetLengthOrder(NewOrder: cardinal);
begin
  FLock.Acquire;
  if (NewOrder >= MinPaletteOrder) and (NewOrder <= MaxPaletteOrder) then
  begin
    FLengthOrder := NewOrder;
    InvalidateLookup;
  end
  else
    Assert(false);
  FLock.Release;
end;

procedure TFractPalette.SetLogarithmic(NewLogarithmic: boolean);
begin
  FLock.Acquire;
  FLogarithmic := NewLogarithmic;
  FLock.Release;
end;


procedure TFractPalette.InvalidateLookup;
begin
  FLock.Acquire;
  FLookupValid := false;
  SetLength(FColorLookup, 0);
  FLock.Release;
end;

function TFractPalette.GetColorMark(Idx: cardinal): TColorMarkData;
var
  iter: cardinal;
  PEntry: PDLentry;
begin
  PEntry := FHead.Flink;
  iter := 0;
  Assert(Idx < FMarkCount);
  while (iter < Idx) do
  begin
    Assert(PEntry <> @FHead);
    PEntry := PEntry.Flink;
    Inc(iter);
  end;
  result := (PEntry.Owner as TColorMark).Data;
end;

function TFractPalette.GetNumMarks: cardinal;
begin
  result := FMarkCount;
end;

procedure TFractPalette.SetColorMark(Data: TColorMarkData);
var
  PEntry: PDLEntry;
  CurMark, NewMark: TColorMark;
begin
  //Can insert even when 0 marks in the list as a setup case.
  PEntry := FHead.FLink;
  Assert(Data.Loc >= 0);
  Assert(Data.Loc < 1);
  while true do
  begin
    CurMark := PEntry.Owner as TColorMark;
    if (PEntry = @FHead) or (Data.Loc < CurMark.Data.Loc) then
    begin
      //Add before Entry
      NewMark := TColorMark.Create;
      NewMark.Data := Data;
      DLItemInsertBefore(PEntry, @NewMark.Link);
      Inc(FMarkCount);
      break;
    end
    else if Data.Loc = CurMark.Data.Loc then
    begin
      CurMark.Data := Data;
      break;
    end
    else
      PEntry := PEntry.FLink; //move to next or head.
  end;
  InvalidateLookup;
  DebugValidateMarkList;
end;

procedure TFractPalette.DeleteMark(Idx: cardinal);
var
  iter: cardinal;
  PEntry: PDLentry;
begin
  PEntry := FHead.Flink;
  iter := 0;
  Assert(Idx < FMarkCount);
  if not (FMarkCount > 1) then
    exit;
  while (iter < Idx) do
  begin
    Assert(PEntry <> @FHead);
    Pentry := PEntry.Flink;
    Inc(Iter);
  end;
  DLListRemoveObj(PEntry);
  PEntry.Owner.Free;

  Dec(FMarkCount);
  InvalidateLookup;
  DebugValidateMarkList;
end;

procedure TFractPalette.ResetAllMarks;
var
  PEntry: PDLEntry;
  Data: TColorMarkData;
begin
  PEntry := DLListRemoveHead(@FHead);
  while Assigned(PEntry) do
  begin
    PEntry.Owner.Free;
    PEntry := DLListRemoveHead(@FHead);
  end;
  FMarkCount := 0;
  FillChar(Data, sizeof(Data), 0);
  SetColorMark(Data);
{$IF DEFINED(IS_FIREMONKEY)}
  Data.Color := ColorToPackedColor(TColorRec.White);
{$ELSE}
  Data.Color := ColorToPackedColor(clWhite);
{$ENDIF}
  Data.Loc := 1 / 2;
  SetColorMark(Data);
  FLengthOrder := DefaultPaletteOrder;
  FLogarithmic := false;
end;

{ TFractItemList }

function TFractItemList.CheckSane(var Err: string): boolean;
var
  idx: integer;
begin
  result := false;
  if not inherited CheckSane(Err) then exit;
  for idx := 0 to Pred(FItems.Count) do
  begin
    if not (Assigned(FItems[idx]) and (TObject(Items[idx]) is FItemType)) then
    begin
      Err := 'Item list has missing item or wrong class type';
      exit;
    end;
    if not (TObject(Items[idx]) as TFractStreamable).CheckSane(Err) then
      exit;
  end;
  result := true;
end;

procedure TFractItemList.SetStreamableClassType(CType: TObjStreamableClass);
begin
  Assert(CType <> nil);
  FItemType := CType;
end;

procedure TFractItemList.CustomMarshal(Sender: TDefaultSSController);
var
  Idx: integer;
begin
  inherited;
  Sender.StreamArrayStart('ItemList');
  for Idx := 0 to Pred(FItems.Count) do
    Sender.StreamClass('', TObjStreamable(FItems[idx]));
  Sender.StreamArrayEnd('ItemList');
end;

procedure TFractItemList.CustomUnmarshal(Sender: TDefaultSSController);
var
  Idx, Count: integer;
  Obj: TObject;
begin
  inherited;
  Sender.UnstreamArrayStart('ItemList', Count);
  FItems.Clear;
  for Idx := 0 to Pred(Count) do
  begin
    Sender.UnstreamClass('', Obj);
    if not (Obj is FItemType) then
      Sender.UserSignalError(sssError,  'Bad class type :' + Obj.ClassName +
        ' expected child of ' + FItemType.ClassName);
    FItems.Add(Obj);
  end;
  Sender.UnstreamArrayEnd('ItemList');
end;

procedure TFractItemList.Assign(Source: TObjStreamable);
var
  Idx: integer;
  SrcType: TObjStreamableClass;
  NewObj: TObjStreamable;
  S: TFractItemList;
begin
  if Assigned(Source) and (Source is TFractItemList) then
  begin
    S := TFractItemList(Source);
    for Idx := 0 to Pred(FItems.Count) do
      TObjStreamable(FItems[Idx]).Free;
    FItems.Clear;
    for Idx := 0 to Pred(S.FItems.Count) do
    begin
      SrcType := TObjStreamableClass(TObjStreamable(S.FItems[idx]).ClassType);
      NewObj := SrcType.Create;
      NewObj.Assign(TObjStreamable(S.FItems[idx]));
      FItems.Add(NewObj);
    end;
  end;
  inherited;
end;

function TFractItemList.GetCapacity: integer;
begin
  result := FItems.Capacity;
end;

procedure TFractItemList.SetCapacity(NewCapacity: integer);
begin
  FItems.Capacity := NewCapacity;
end;

function TFractItemList.GetCount: integer;
begin
  result := FItems.Count;
end;

procedure TFractItemList.SetCount(NewCount: integer);
begin
  FItems.Count := NewCount;
end;

function TFractItemList.Get(Index: integer): TObjStreamable;
begin
  result := TObjStreamable(FItems[Index]);
end;

procedure TFractItemList.Put(Index: integer; New: TObjStreamable);
begin
  FItems[Index] := New;
end;

constructor TFractItemList.Create;
begin
  inherited;
  FItems := TList.Create;
  FItemType := nil;
end;

destructor TFractItemList.Destroy;
var
  idx: integer;
begin
  for idx  := 0 to Pred(FItems.Count) do
    TFractStreamable(FItems.Items[idx]).Free;
  FItems.Free;
  inherited;
end;

function TFractItemList.Add(Item: TObjStreamable): Integer;
begin
  result := FItems.Add(Item);
end;

procedure TFractItemList.Clear;
begin
  FItems.Clear;
end;

procedure TFractItemList.Delete(Index: Integer);
begin
  FItems.Delete(Index);
end;

procedure TFractItemList.Exchange(Index1, Index2: Integer);
begin
  FItems.Exchange(Index1, Index2);
end;

function TFractItemList.Expand: TFractItemList;
var
  NList: TList;
begin
  NList := FItems.Expand;
  if NList <> FItems then
  begin
    FItems.Free;
    FItems := NList;
  end;
  result := Self;
end;

function TFractItemList.Extract(Item: TObjStreamable): TObjStreamable;
begin
  result := FItems.Extract(Item);
end;

function TFractItemList.First: TObjStreamable;
begin
  result := FItems.First;
end;

function TFractItemList.IndexOf(Item: TObjStreamable): Integer;
begin
  result := FItems.IndexOf(Item);
end;

procedure TFractItemList.Insert(Index: Integer; Item: TObjStreamable);
begin
  FItems.Insert(Index, Item);
end;

function TFractItemList.Last: TObjStreamable;
begin
  result := FItems.Last;
end;

procedure TFractItemList.Move(CurIndex, NewIndex: Integer);
begin
  FItems.Move(CurIndex, NewIndex);
end;

function TFractItemList.Remove(Item: TObjStreamable): Integer;
begin
  result := FItems.Remove(Item);
end;

procedure TFractItemList.Pack;
begin
  FItems.Pack;
end;

procedure TFractItemList.Sort(Compare: TListSortCompare);
begin
  FItems.Sort(Compare);
end;

{ TFractV2HistoryList }

constructor TFractV2HistoryList.Create;
begin
  inherited;
  SetStreamableClassType(TFractV2SettingsBundle);
end;

{ TFractV2WaypointList }

constructor TFractV2WaypointList.Create;
begin
  inherited;
  SetStreamableClassType(TFractV2LocationSettings);
end;

{ TFractV2Comparable }

procedure TFractV2Comparable.CompareDifferencesForFrame(Other: TFractV2Comparable;
  var DiffPortionCount: boolean;
  var DiffPortionRects: boolean;
  var DiffPortionData: boolean);
begin
  DiffPortionCount := false;
  DiffPortionRects := false;
  DiffPortionData := false;
end;

{ TFractV2LocationSettings }

procedure TFractV2LocationSettings.SetBailoutOrder(NewOrder: cardinal);
begin
  if (NewOrder >= MinBailoutOrder)
    and (NewOrder <= MaxBailoutOrder) then
    FBailoutOrder := NewOrder;
end;

procedure TFractV2LocationSettings.SetCenterX(X: double);
begin
  FCenter.X := X;
end;

procedure TFractV2LocationSettings.SetCenterY(Y: double);
begin
  FCenter.Y := Y;
end;

function TFractV2LocationSettings.GetCenterX: double;
begin
  result := FCenter.X;
end;

function TFractV2LocationSettings.GetCenterY: double;
begin
  result := FCenter.Y
end;

procedure TFractV2LocationSettings.SetRotRadians(NewRadians: double);
var
  Tmp: double;
  TmpInt: integer;
begin
  if (NewRadians <= -Pi) or (NewRadians > Pi) then
  begin
    Tmp := NewRadians / Pi;
    TmpInt := Trunc(Tmp);
    //-1, -2, -3, -4, -5: Add 2, 2, 4, 4
    //1, 2, 3, 4, 5 Sub: 2, 2, 4, 4.
    if TmpInt > 0 then
      Inc(TmpInt)
    else
      Dec(TmpInt);
    TmpInt := TmpInt div 2;
    NewRadians := NewRadians - (2 * Pi * TmpInt);
  end;
  FRotRadians := NewRadians;
end;

function TFractV2LocationSettings.CheckSane(var Err: string): boolean;
begin
  result := false;
  if not inherited CheckSane(Err) then
    exit;
  if (FBailoutOrder < MinBailoutOrder) or
    (FBailoutOrder > MaxBailoutOrder) then
  begin
    Err := 'Fractal bailout order bad.';
    exit;
  end;
  if (FRotRadians <= -Pi) or (FRotRadians > Pi) then
  begin
    Err := 'Fractal rotation not between -pi and pi.';
    exit;
  end;
  result := true;
end;

constructor TFractV2LocationSettings.Create;
begin
  inherited;
  FCenter.X := InitialMandelX;
  FCenter.Y := InitialMandelY;
  FScalePPU := InitialZoom;
  FRotRadians := 0;
  FBailoutOrder := InitialBailoutOrder;
end;

procedure TFractV2LocationSettings.CompareDifferencesForFrame(Other: TFractV2Comparable;
  var DiffPortionCount: boolean;
  var DiffPortionRects: boolean;
  var DiffPortionData: boolean);
var
  Ot: TFractV2LocationSettings;
begin
  inherited CompareDifferencesForFrame(Other,
                DiffPortionCount,
                DiffPortionRects,
                DiffPortionData);
  if (Other is TFractV2LocationSettings) then
  begin
    Ot := TFractV2LocationSettings(Other);
    DiffPortionRects := DiffPortionRects
      or (not RealPointsSame(FCenter, Ot.Center))
      or (FScalePPU <> Ot.FScalePPU)
      or (FRotRadians <> Ot.FRotRadians)
      or (FBailoutOrder <> Ot.FBailoutOrder);
  end
  else Assert(false);
end;

procedure TFractV2LocationSettings.Assign(Source: TObjStreamable);
var
  Src: TFractV2LocationSettings;
begin
  if Assigned(Source) and (Source is TFractV2LocationSettings) then
  begin
    Src := TFractV2LocationSettings(Source);
    FCenter := Src.FCenter;
    FScalePPU := Src.FScalePPU;
    FRotRadians := Src.FRotRadians;
    FBailoutOrder := Src.FBailoutOrder;
  end;
  inherited;
end;

function TFractV2LocationSettings.GetDescString;
begin
  result := Format('X: %4.4g Y: %4.4g Z: %4.4g R: %4.4g D: %u',
                      [Center.X,
                       Center.Y,
                       ScalePPU,
                       RotRadians,
                       BailoutOrder]);
end;

{ TFractV2FormulaSettings }

procedure TFractV2FormulaSettings.SetJuliaConstX(X: double);
begin
  FJuliaConst.X := X;
end;

procedure TFractV2FormulaSettings.SetJuliaConstY(Y: double);
begin
  FJuliaConst.Y := Y;
end;

function TFractV2FormulaSettings.GetJuliaConstX: double;
begin
  result := FJuliaConst.X;
end;

function TFractV2FormulaSettings.GetJuliaConstY: double;
begin
  result := FJuliaConst.Y;
end;

function TFractV2FormulaSettings.CheckSane(var Err: string): boolean;
begin
  result := false;
  if not inherited CheckSane(Err) then
    exit;
  //TODO Limits on FPortionCount.
  result := true;
end;

constructor TFractV2FormulaSettings.Create;
begin
  inherited;
  FJuliaConst.X := InitialJuliaX;
  FJuliaConst.Y := InitialJuliaY;
  FSavedOtherPPU := InitialZoom;
  FInitialPPU := InitialZoom;
  FMultPPU := InitialMult;
  FPortionCount := InitialPortions;
  FFormula := ffMandel;
end;

procedure TFractV2FormulaSettings.CompareDifferencesForFrame(Other: TFractV2Comparable;
  var DiffPortionCount: boolean;
  var DiffPortionRects: boolean;
  var DiffPortionData: boolean);
var
  Ot: TFractV2FormulaSettings;
begin
  inherited CompareDifferencesForFrame(Other,
                DiffPortionCount,
                DiffPortionRects,
                DiffPortionData);
  if (Other is TFractV2FormulaSettings) then
  begin
    Ot := TFractV2FormulaSettings(Other);
    DiffPortionCount := DiffPortionCount or (FPortionCount <> Ot.FPortionCount);
    DiffPortionData := DiffPortionData
      or (FFormula <> Ot.FFormula)
      or (not RealPointsSame(FJuliaConst, Ot.FJuliaConst));
  end
  else Assert(false);
end;

procedure TFractV2FormulaSettings.Assign(Source: TObjStreamable);
var
  Src: TFractV2FormulaSettings;
begin
  if Assigned(Source) and (Source is TFractV2FormulaSettings) then
  begin
    Src := TFractV2FormulaSettings(Source);
    FJuliaConst := Src.FJuliaConst;
    FSavedOtherPPU := Src.FSavedOtherPPU;
    FInitialPPU := Src.FInitialPPU;
    FMultPPU := Src.FMultPPU;
    FPortionCount := Src.FPortionCount;
    FFormula := Src.FFormula;
  end;
  inherited;
end;

{ TFractV2SettingsBundle }

function TFractV2SettingsBundle.CheckSane(var Err: string): boolean;
begin
  result := false;
  if not inherited CheckSane(err) then
    exit;
  if not Assigned(FPalette) then
  begin
    Err := 'Fractal does not have an associated palette.';
    exit;
  end;
  if not Assigned(FLocation) then
  begin
    Err := 'Factal does not have a current location.';
    exit;
  end;
  if not Assigned(FFormula) then
  begin
    Err := 'Fractal does not have an assigned formula.';
    exit;
  end;
  result := FPalette.CheckSane(Err)
        and FLocation.CheckSane(Err)
        and FFormula.CheckSane(Err);
end;

constructor TFractV2SettingsBundle.Create;
begin
  inherited;
  if not CreatingInStreamer then
  begin
    FVerTag := TFractVersionTag.Create;
    FVerTag.Version := SettingsVersionTag;
    FPalette := TFractPalette.Create;
    FLocation := TFractV2LocationSettings.Create;
    FFormula := TFractV2FormulaSettings.Create;
  end;
end;

destructor TFractV2SettingsBundle.Destroy;
begin
  FPalette.Free;
  FLocation.Free;
  FFormula.Free;
  inherited;
end;

procedure TFractV2SettingsBundle.CompareDifferencesForFrame(Other: TFractV2Comparable;
  var DiffPortionCount: boolean;
  var DiffPortionRects: boolean;
  var DiffPortionData: boolean);
var
  Ot: TFractV2SettingsBundle;
  DPC1, DPC2, DPC3: boolean;
  DPR1, DPR2, DPR3: boolean;
  DPD1, DPD2, DPD3: boolean;
begin
  if (Other is TFractV2SettingsBundle) then
  begin
    Ot := TFractV2SettingsBundle(Other);
    FLocation.CompareDifferencesForFrame(Ot.FLocation,
        DPC2, DPR2, DPD2);
    FFormula.CompareDifferencesForFrame(Ot.FFormula,
        DPC3, DPR3, DPD3);
    DPC1 := DPC2 or DPC3;
    DPR1 := DPR2 or DPR3;
    DPD1 := DPD2 or DPD3;
    DiffPortionCount := DPC1;
    DiffPortionRects := DiffPortionCount or DPR1;
    DiffPortionData := DiffPortionRects or DPD1;
  end
  else Assert(false);
end;

procedure TFractV2SettingsBundle.Assign(Source: TObjStreamable);
var
  Src: TFractV2SettingsBundle;
begin
  if Assigned(Source) and (Source is TFractV2SettingsBundle) then
  begin
    Src:= TFractV2SettingsBundle(Source);
    Assert(Assigned(FPalette));
    FPalette.Assign(Src.FPalette);
    Assert(Assigned(FLocation));
    FLocation.Assign(Src.FLocation);
    Assert(Assigned(FFormula));
    FFormula.Assign(Src.FFormula);
    HintString := Src.HintString;
  end;
  inherited;
end;

procedure TFractV2SettingsBundle.DoZoomDefault;
begin
  FLocation.ScalePPU := FFormula.InitialPPU;
end;

procedure TFractV2SettingsBundle.DoZoomBy(LogScale: double);
begin
  FLocation.ScalePPU := FLocation.ScalePPU
    * Power(FFormula.MultPPU, LogScale);
end;

procedure TFractV2SettingsBundle.DoRotateBy(Radians: double);
begin
  FLocation.RotRadians := FLocation.RotRadians
    + Radians;
end;

procedure TFractV2SettingsBundle.DoChangeDetail(PowerIncrement: integer);
var
  NewBail: integer;
begin
  //Have to convert from integer to cardinal here.
  NewBail := FLocation.BailoutOrder;
  Inc(NewBail, PowerIncrement);
  if NewBail < MinBailoutOrder then
    NewBail := MinBailoutOrder;
  if NewBail > MaxBailoutOrder then
    NewBail := MaxBailoutOrder;
  FLocation.BailoutOrder := NewBail;
end;

function TFractV2SettingsBundle.CanChangeDetail(PowerIncrement: integer): boolean;
var
  NewBail: integer;
begin
  NewBail := FLocation.BailoutOrder;
  Inc(NewBail, PowerIncrement);
  result := (NewBail >= MinBailoutOrder) and (NewBail <= MaxBailoutOrder);
end;

procedure TFractV2SettingsBundle.FlipJulia;
var
  tmp: double;
  tmp2: TRealPoint;
begin
  if FFormula.Formula = ffMandel then
  begin
    tmp2:= FFormula.JuliaConst;
    FFormula.JuliaConst := FLocation.Center;
    FLocation.Center := tmp2;

    tmp := FFormula.SavedOtherPPU;
    FFormula.SavedOtherPPU := FLocation.ScalePPU;
    FLocation.ScalePPU := tmp;
    FFormula.Formula := ffJulia;
  end
  else if FFormula.Formula = ffJulia then
  begin
    //Save julia "center" in the julia const.
    tmp2:= FFormula.JuliaConst;
    FFormula.JuliaConst := FLocation.Center;
    FLocation.Center := tmp2;

    tmp := FFormula.SavedOtherPPU;
    FFormula.SavedOtherPPU := FLocation.ScalePPU;
    FLocation.ScalePPU := tmp;

    FFormula.Formula := ffMandel;
  end else
    Assert(false);
end;

function TFractV2SettingsBundle.GetDescString: string;
begin
  result := FLocation.DescString;
end;

{ TFractV2Environment }

constructor TFractV2Environment.Create;
var
  InitBundle: TFractV2SettingsBundle;
begin
  inherited;
  FHistoryList := TFractV2HistoryList.Create;
  InitBundle := TFractV2SettingsBundle.Create;
  FHistoryList.Add(InitBundle);
  FHistoryIndex := 0;
end;

destructor TFractV2Environment.Destroy;
begin
  FHistoryList.Free;
  inherited;
end;

procedure TFractV2Environment.Assign(Source: TObjStreamable);
var
  S: TFractV2Environment;
begin
  if Assigned(Source) and (Source is TFractV2Environment) then
  begin
    S := TFractV2Environment(Source);
    Assert(Assigned(FHistoryList));
    FHistoryList.Assign(S.FHistoryList);
    FHistoryIndex := S.FHistoryIndex;
  end;
  inherited;
end;

procedure TFractV2Environment.CustomMarshal(Sender: TDefaultSSController);
begin
  inherited;
  Sender.StreamClass('HistoryList', FHistoryList);
  Sender.StreamULong('HistoryIndex', FHistoryIndex);
end;

procedure TFractV2Environment.CustomUnmarshal(Sender: TDefaultSSController);
var
  Val: TObject;
begin
  inherited;
  Sender.UnstreamClass('HistoryList', Val);
  if not (Val is TFractV2HistoryList) then
    Sender.UserSignalError(sssError, 'History list bad class type.');
  FHistoryList.Free;
  FHistoryList := Val as TFractV2HistoryList;
  Sender.UnstreamULong('HistoryIndex', FHistoryIndex);
end;

function TFractV2Environment.CheckSane(var Err: string): boolean;
begin
  result := false;
  if not inherited CheckSane(Err) then exit;
  if not Assigned(FHistoryList) and (FHistoryList is TFractV2HistoryList) then
  begin
    Err := 'History list missing or wrong class type';
    exit;
  end;
  if FHistoryList.Count > 0 then
  begin
    if not (FHistoryIndex < Cardinal(FHistoryList.Count)) then
    begin
      Err := 'History index out of range';
      exit;
    end;
  end
  else
  begin
    Err := 'History list must contain at least one item';
    exit;
  end;
  if not FHistoryList.CheckSane(Err) then exit;
  result := true;
end;


function TFractV2Environment.HistoryNewBundleForChange(var LastBundle:TFractV2SettingsBundle): TFractV2SettingsBundle;
var
  Idx: integer;
begin
  LastBundle := FHistoryList.Items[FHistoryIndex] as TFractV2SettingsBundle;
  //Remove history later than current item.
  for Idx := Succ(FHistoryIndex) to Pred(FHistoryList.Count) do
  begin
    FHistoryList.Items[Idx].Free;
  end;
  FHistoryList.Count := Succ(FHistoryIndex);
  //And add new item.
  result := TFractV2SettingsBundle.Create;
  result.Assign(LastBundle);
  FHistoryList.Add(result);
  FHistoryIndex := Pred(FHistoryList.Count);
end;

procedure TFractV2Environment.HistoryClear;
var
  tmp: TFractV2SettingsBundle;
  Idx: cardinal;
begin
  tmp := nil;
  for Idx := 0 to Pred(FHistoryList.Count) do
  begin
    if Idx = FHistoryIndex then
      tmp := FHistoryList.Items[Idx] as TFractV2SettingsBundle
    else
      FHistoryList.Items[Idx].Free;
  end;
  Assert(Assigned(tmp));
  FHistoryList.Clear;
  FHistoryList.Add(tmp);
  FHistoryIndex := Pred(FHistoryList.Count);
end;

function TFractV2Environment.HistoryCanClear: boolean;
begin
  result := FHistoryList.Count > 1;
end;

procedure TFractV2Environment.HistoryUndo;
begin
  if not HistoryCanUndo then exit;
  Dec(FHistoryIndex);
end;

procedure TFractV2Environment.HistoryRedo;
begin
  if not HistoryCanRedo then exit;
  Inc(FHistoryIndex);
end;

function TFractV2Environment.HistoryCanUndo;
begin
  result := FHistoryIndex > 0;
end;

function TFractV2Environment.HistoryCanRedo;
begin
  result := FHistoryIndex < Pred(Cardinal(FHistoryList.Count));
end;

function TFractV2Environment.HistoryGetCurrentBundle(): TFractV2SettingsBundle;
begin
  result := FHistoryList.Items[FHistoryIndex] as TFractV2SettingsBundle;
end;

function TFractV2Environment.HistoryGetDescString(Idx: integer): string;
var
  Bundle: TFractV2SettingsBundle;
begin
  Bundle := (FHistoryList.Items[Idx] as TFractV2SettingsBundle);
  result := Bundle.DescString + ' ' + Bundle.HintString;
end;

function TFractV2Environment.HistoryGetCount: cardinal;
begin
  result := FHistoryList.Count;
end;

//Load helper returns old settings for renderer comparison purposes.
function TFractV2Environment.FileLoadAllHelper(LoadClass: TFractStreamable;
  var ErrStr: string): TFractV2SettingsBundle;
begin
  if (LoadClass is TFractV2SettingsBundle) then
  begin
    if not LoadClass.CheckSane(ErrStr) then
    begin
      result := nil;
      exit;
    end;
    HistoryClear;
    result := TFractV2SettingsBundle.Create;
    result.Assign(FHistoryList.Items[FHistoryIndex]);
    FHistoryList.Items[FHistoryIndex].Assign(LoadClass);
  end
  else if (LoadClass is TFractV2Environment) then
  begin
    if not LoadClass.CheckSane(ErrStr) then
    begin
      result := nil;
      exit;
    end;
    HistoryClear;
    result := TFractV2SettingsBundle.Create;
    result.Assign(FHistoryList.Items[FHistoryIndex]);
    Assign(LoadClass);
  end
  else
  begin
    ErrStr := 'Load failed: Structure root bad class type.';
    result := nil;
  end;
end;

function TFractV2Environment.FileLoadPaletteHelper(LoadClass: TFractStreamable;
  var NewSettings:TFractV2SettingsBundle; var ErrStr: string): TFractV2SettingsBundle;
var
  Pal: TFractPalette;
begin
  Pal := nil;
  result := nil;
  if (LoadClass is TFractV2Environment)
     and (LoadClass as TFractV2Environment).CheckSane(ErrStr) then
  begin
    Pal := (LoadClass as TFractV2Environment).HistoryGetCurrentBundle.Palette;
  end
  else if (LoadClass is TFractV2SettingsBundle)
     and (LoadClass as TFractV2SettingsBundle).CheckSane(ErrStr) then
  begin
    Pal := (LoadClass as TFractV2SettingsBundle).Palette;
  end
  else if (LoadClass is TFractPalette)
     and (LoadClass as TFractPalette).CheckSane(ErrStr) then
  begin
    Pal := (LoadClass as TFractPalette);
  end;
  if not Assigned(Pal) then
  begin
    if Length(ErrStr) = 0 then
      ErrStr := 'Palette Load: Unknown Class Type';
    exit;
  end;

  NewSettings := HistoryNewBundleForChange(result);
  NewSettings.Palette.Assign(Pal);
end;

function TFractV2Environment.MakeSaveableCopy:TFractV2Environment;
begin
  result := TFractV2Environment.Create;
  result.Assign(self);
  result.HistoryClear;
end;

{ TFractV2VideoEnv }

constructor TFractV2VideoEnv.Create;
begin
  inherited;
  FWaypointList := TFractV2WaypointList.Create;
  FPalette := TFractPalette.Create;
  FFormula := TFractV2FormulaSettings.Create;
  FCursor := 0;
end;

destructor TFractV2VideoEnv.Destroy;
begin
  FWaypointList.Free;
  FPalette.Free;
  FFormula.Free;
  inherited;
end;

function TFractV2VideoEnv.CheckSane(var Err: string): boolean;
begin
  result := false;
  if not inherited CheckSane(Err) then exit;
  if not Assigned(FWaypointList) and (FWaypointList is TFractV2WaypointList) then
  begin
    Err := 'Waypoint list missing or wrong class type';
    exit;
  end;
  if FWaypointList.Count > 0 then
  begin
    if not (FCursor < Cardinal(FWaypointList.Count)) then
    begin
      Err := 'Waypoint index out of range';
      exit;
    end;
  end;
  //Waypoint list can be empty.
  if not FWaypointList.CheckSane(Err) then exit;
  if not Assigned(FPalette) and (FPalette is TFractPalette) then
  begin
    Err :=  'Palette missing or wrong class type.';
    exit;
  end;
  if not FPalette.CheckSane(Err) then exit;
  if not Assigned(FFormula) and (FFormula is TFractV2FormulaSettings) then
  begin
    Err := 'Formula missing or wrong class type';
    exit;
  end;
  if not FFormula.CheckSane(Err) then exit;
  result := true;
end;

procedure TFractV2VideoEnv.Assign(Source: TObjStreamable);
var
  S: TFractV2VideoEnv;
begin
  if Assigned(Source) and (Source is TFractV2VideoEnv) then
  begin
    S := TFractV2VideoEnv(Source);
    Assert(Assigned(FWaypointList));
    FWaypointList.Assign(S.FWaypointList);
    Assert(Assigned(FPalette));
    FPalette.Assign(S.FPalette);
    Assert(Assigned(FFormula));
    FFormula.Assign(S.FFormula);
    FCursor := S.FCursor;
  end;
end;

function TFractV2VideoEnv.WaypointGetDescString(Idx: integer): string;
begin
  result := (FWaypointList.Items[Idx] as TFractV2LocationSettings).DescString;
end;

function TFractV2VideoEnv.WaypointGetCount: cardinal;
begin
  result := FWaypointList.Count;
end;

function TFractV2VideoEnv.WaypointGetCursor: cardinal;
begin
  result := FCursor;
end;

procedure TFractV2VideoEnv.WaypointSetCursor(NewCursor: cardinal);
begin
  //Can always set cursor to 0 even if
  if (NewCursor < Cardinal(FWaypointList.Count)) or (NewCursor = 0) then
    FCursor := NewCursor
  else
    raise EWaypointError.Create('Invalid cursor or bad index');
end;

procedure TFractV2VideoEnv.WaypointDelete();
begin
  if (FCursor < Cardinal(FWaypointList.Count)) then
  begin
    FWaypointList.Items[FCursor].Free;
    FWaypointList.Delete(FCursor);
  end;
  if not (FCursor < Cardinal(FWaypointList.Count)) then
  begin
    if FWaypointList.Count > 0 then
      WaypointSetCursor(Pred(FWaypointList.Count))
    else
      WaypointSetCursor(0);
  end;
end;

function TFractV2VideoEnv.WaypointCanDelete: boolean;
begin
  result := (FCursor < Cardinal(FWaypointList.Count));
end;

procedure TFractV2VideoEnv.WaypointClearAll;
var
  idx: integer;
begin
  for idx := 0 to Pred(FWaypointList.Count) do
    FWaypointList.Items[idx].Free;
  FWaypointList.Clear;
  WaypointSetCursor(0);
end;

procedure TFractV2VideoEnv.WaypointMove(NewIdx: cardinal);
var
  OldIdx: cardinal;
begin
  OldIdx := FCursor;
  if (OldIdx < Cardinal(FWaypointList.Count))
    and (NewIdx < Cardinal(FWaypointList.Count))  then
  begin
    FWaypointList.Move(OldIdx, NewIdx);
    WaypointSetCursor(NewIdx);
  end
  else
    raise EWaypointError.Create('Move operation bad index');
end;

function TFractV2VideoEnv.WaypointCanMove(NewIdx:cardinal): boolean;
begin
  result := (FCursor < Cardinal(FWaypointList.Count))
    and (NewIdx < Cardinal(FWaypointList.Count));
end;


procedure TFractV2VideoEnv.WaypointAppendFromHistoryCurrent(Env: TFractV2Environment);
var
  NewWaypoint: TFractV2LocationSettings;
begin
  NewWaypoint := TFractV2LocationSettings.Create;
  NewWaypoint.Assign(Env.HistoryCurrentBundle.Location);
  FWaypointList.Add(NewWaypoint);
  //No change to cursor.
end;

procedure TFractV2VideoEnv.WaypointInsertFromHistoryCurrent(Env: TFractV2Environment);
var
  NewWaypoint: TFractV2LocationSettings;
begin
  NewWaypoint := TFractV2LocationSettings.Create;
  NewWaypoint.Assign(Env.HistoryCurrentBundle.Location);
  FWaypointList.Insert(FCursor, NewWaypoint);
end;

procedure TFractV2VideoEnv.GetPalleteFromStillEnv(OtherEnv: TFractV2Environment);
begin
  FPalette.Assign(OtherEnv.HistoryCurrentBundle.Palette);
end;

procedure TFractV2VideoEnv.GetFormulaFromEnv(OtherEnv: TFractV2Environment);
begin
  FFormula.Assign(OtherEnv.HistoryCurrentBundle.Formula);
end;

end.
