unit MFMainFrm;

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, Forms, Dialogs,
  Menus, ActnList, ComCtrls, ExtCtrls, ToolWin, ImgList,
  SizeableImage, FractRenderers, System.Actions;

type
  TFileOpType = (fotFract, fotPal, fotBmp, fotPng);

  TUpdateDynamicHelperFunc = procedure (Sender: TObject;
                                        ToUpdate: TMenuItem;
                                        DynIdx: integer;
                                        NumDynamics: integer) of object;

  TMFMainForm = class(TForm)
    MainMenu1: TMainMenu;
    StatusBar1: TStatusBar;
    ActionList1: TActionList;
    ExitApp: TAction;
    LoadFract: TAction;
    SaveFract: TAction;
    ExpImage: TAction;
    ZoomIn: TAction;
    ZoomOut: TAction;
    ResetZoom: TAction;
    EditPalette: TAction;
    FractSettings: TAction;
    HelpContents: TAction;
    HelpAbout: TAction;
    File1: TMenuItem;
    Help1: TMenuItem;
    New: TAction;
    New1: TMenuItem;
    N1: TMenuItem;
    LoadFractal1: TMenuItem;
    SaveFractal1: TMenuItem;
    ExportImage1: TMenuItem;
    N3: TMenuItem;
    Exit1: TMenuItem;
    Fractal1: TMenuItem;
    ZoomIn1: TMenuItem;
    ZoomOut1: TMenuItem;
    ResetZoom1: TMenuItem;
    N4: TMenuItem;
    EditPalette1: TMenuItem;
    FractalSettings1: TMenuItem;
    Contents1: TMenuItem;
    About1: TMenuItem;
    ToolBar1: TToolBar;
    Image1: TSizeableImage;
    FractStop: TAction;
    N5: TMenuItem;
    Stop1: TMenuItem;
    Refresh: TAction;
    Recalc: TAction;
    N6: TMenuItem;
    Refresh1: TMenuItem;
    Recalculate1: TMenuItem;
    ZoomAtCursor: TAction;
    ImageList1: TImageList;
    ToolButton1: TToolButton;
    ToolButton2: TToolButton;
    ToolButton4: TToolButton;
    ToolButton6: TToolButton;
    ToolButton7: TToolButton;
    ToolButton8: TToolButton;
    ToolButton9: TToolButton;
    ToolButton10: TToolButton;
    ToolButton11: TToolButton;
    ToolButton12: TToolButton;
    ToolButton13: TToolButton;
    ToolButton14: TToolButton;
    ToolButton15: TToolButton;
    ToolButton16: TToolButton;
    ToolButton17: TToolButton;
    OpenDialog1: TOpenDialog;
    SaveDialog1: TSaveDialog;
    MoreDetail: TAction;
    LessDetail: TAction;
    ToolButton3: TToolButton;
    ToolButton5: TToolButton;
    ToolButton18: TToolButton;
    ToolButton19: TToolButton;
    N7: TMenuItem;
    MoreDetail1: TMenuItem;
    LessDetail1: TMenuItem;
    Undo: TAction;
    Redo: TAction;
    History1: TMenuItem;
    Undo1: TMenuItem;
    Redo1: TMenuItem;
    N2: TMenuItem;
    HistoryClear: TAction;
    Clear1: TMenuItem;
    HistoryMenuAction: TAction;
    Video1: TMenuItem;
    VideoInsertWaypoint1: TMenuItem;
    Appendlocationaswaypoint1: TMenuItem;
    Deletewaypoint1: TMenuItem;
    Movewaypointup1: TMenuItem;
    Movewaypointdown1: TMenuItem;
    Setwaypointtostillimage1: TMenuItem;
    N8: TMenuItem;
    Usecurrentpalette1: TMenuItem;
    Setpalettetostillimage1: TMenuItem;
    Setrenderoptions1: TMenuItem;
    N9: TMenuItem;
    Loadsettings1: TMenuItem;
    Savesettings1: TMenuItem;
    N10: TMenuItem;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure ZoomInExecute(Sender: TObject);
    procedure ZoomOutExecute(Sender: TObject);
    procedure ResetZoomExecute(Sender: TObject);
    procedure FractStopExecute(Sender: TObject);
    procedure RefreshExecute(Sender: TObject);
    procedure RecalcExecute(Sender: TObject);
    procedure Image1DblClick(Sender: TObject);
    procedure ZoomAtCursorExecute(Sender: TObject);
    procedure Image1StartDrag(Sender: TObject;
      var DragObject: TDragObject);
    procedure NewExecute(Sender: TObject);
    procedure ExitAppExecute(Sender: TObject);
    procedure Image1DragOver(Sender, Source: TObject; X, Y: Integer;
      State: TDragState; var Accept: Boolean);
    procedure Image1DragDrop(Sender, Source: TObject; X, Y: Integer);
    procedure FractSettingsExecute(Sender: TObject);
    procedure EditPaletteExecute(Sender: TObject);
    procedure SaveFractExecute(Sender: TObject);
    procedure LoadFractExecute(Sender: TObject);
    procedure HelpAboutExecute(Sender: TObject);
    procedure ExpImageExecute(Sender: TObject);
    procedure HelpContentsExecute(Sender: TObject);
    procedure FormMouseWheel(Sender: TObject; Shift: TShiftState;
      WheelDelta: Integer; MousePos: TPoint; var Handled: Boolean);
    procedure MoreDetailExecute(Sender: TObject);
    procedure LessDetailExecute(Sender: TObject);
    procedure MoreDetailUpdate(Sender: TObject);
    procedure LessDetailUpdate(Sender: TObject);
    procedure UndoExecute(Sender: TObject);
    procedure UndoUpdate(Sender: TObject);
    procedure RedoExecute(Sender: TObject);
    procedure RedoUpdate(Sender: TObject);
    procedure HistoryClearExecute(Sender: TObject);
    procedure HistoryClearUpdate(Sender: TObject);
    procedure HistoryMenuActionUpdate(Sender: TObject);
    procedure HistoryMenuActionExecute(Sender: TObject);
    procedure FractStopUpdate(Sender: TObject);
  private
    { Private declarations }
    FRenderer: TFractImageRenderer;
    procedure SaveHelper(Obj: TObject; FName: string);
    function LoadHelper(FName: string): TObject;
    procedure StatusUpdate(Sender: TObject);
    procedure HandleBmpRequest(Sender: TObject; ScreenRect: TRect; var Bmp: TBitmap);
    function FilterIndexToOpType(Index: cardinal): TFileOpType;
    procedure FixupFileName(var FileName: string; OpType: TFileOpType);
    procedure UpdateDynamicMenuList(ParentItem: TMenuItem;
                                    PrecedingSeparator: TMenuItem;
                                    NumDynamics: integer;
                                    HelperFunc: TUpdateDynamicHelperFunc);
    procedure UpdateDynamicHistoryHelper(Sender: TObject;
                                         ToUpdate: TMenuItem;
                                         DynIdx: integer;
                                         NumDynamics: integer);
    procedure UpdateDynamicVideoHelper(Sender: TObject;
                                         ToUpdate: TMenuItem;
                                         DynIdx: integer;
                                         NumDynamics: integer);
    procedure CreateDynamicMenuItems;
    procedure HistoryDynamicMenuClick(Sender: TObject);
    procedure VideoDynamicMenuClick(Sender: TObject);
  public
    { Public declarations }
  end;

var
  MFMainForm: TMFMainForm;

implementation

uses FractSettingsDialog, PaletteSettingsDialog, FractStreaming,
  StreamSysXML, FractAboutDialog, ExportOptsDialog, ShellAPI, ReRenderForm,
  DragHelperFrm, FractSettings, FractMisc, FractLegacy, UITypes;

{$R *.DFM}

const
  FractFileFilter = 'Fractal Files (*.mff) |*.mff';
  PalFileFilter = 'Palette Files (*.mfp) |*.mfp';
  AllFileFilter = 'All Files (*.*) |*.*';
  BmpFileFilter = 'Bitmap files (*.bmp) |*.bmp';
  PngFileFilter = 'Portable network graphics (*.png) |*.png';

  LoadFilter = FractFileFilter + '|' + PalFileFilter + '|' + AllFileFilter;
  SaveFilter = FractFileFilter + '|' + PalFileFilter ;
  ExportImageFilter = BmpFileFilter;

  FractFileSuffix = '.mff';
  PalFileSuffix = '.mfp';
  BmpFileSuffix = '.bmp';
  PngFileSuffix = '.png';
  HelpFileLocation = 'Help\MFract.html';

  HelperFormOffset = 80;
  HistoryDynamicMenuItems = 10;

function TMFMainForm.FilterIndexToOpType(Index: cardinal): TFileOpType;
begin
  result := fotFract;
  case Index of
    1: result := fotFract;
    2: result := fotPal;
  else
    Assert(false);
  end;
end;

procedure TMFMainForm.FixupFileName(var FileName: string; OpType: TFileOpType);
var
  Trail: string;
  Suffix: string;
begin
  case OpType of
    fotFract: Suffix := FractFileSuffix;
    fotPal: Suffix := PalFileSuffix;
    fotBmp: Suffix := BmpFileSuffix;
    fotPng: Suffix := PngFileSuffix;
  else
    Assert(false);
  end;
  if Length(FileName) >= Length(Suffix) then
  begin
    Trail := Copy(FileName, Length(FileName) - Length(Suffix) + 1,
      Length(Suffix));
    if AnsiCompareText(Trail, Suffix) = 0 then
      exit;
  end;
  FileName := FileName + Suffix;
end;


procedure TMFMainForm.FormCreate(Sender: TObject);
begin
  FRenderer := TFractImageRenderer.Create;
  FRenderer.Image := Image1;
  FRenderer.ThreadPriority := tpLower;
  FRenderer.OnProgress := StatusUpdate;
  Mouse.DragImmediate := false;
  OpenDialog1.InitialDir := ExtractFilePath(Application.ExeName);
  SaveDialog1.InitialDir := ExtractFilePath(Application.ExeName);
  CreateDynamicMenuItems;
end;

procedure TMFMainForm.FormDestroy(Sender: TObject);
begin
  FRenderer.Free;
  FRenderer := nil;
end;

procedure TMFMainForm.ZoomInExecute(Sender: TObject);
begin
  FRenderer.ZoomBy(1);
end;

procedure TMFMainForm.ZoomOutExecute(Sender: TObject);
begin
  FRenderer.ZoomBy(-1);
end;

procedure TMFMainForm.ResetZoomExecute(Sender: TObject);
begin
  FRenderer.ZoomDefault;
end;

procedure TMFMainForm.FractStopExecute(Sender: TObject);
begin
  FRenderer.Stop;
end;

procedure TMFMainForm.FractStopUpdate(Sender: TObject);
begin
  (Sender as TAction).Enabled := FRenderer.Running;
end;

procedure TMFMainForm.RefreshExecute(Sender: TObject);
begin
  FRenderer.Refresh;
end;

procedure TMFMainForm.RecalcExecute(Sender: TObject);
begin
  FRenderer.Recalc;
end;

procedure TMFMainForm.RedoExecute(Sender: TObject);
begin
  FRenderer.Redo;
end;

procedure TMFMainForm.RedoUpdate(Sender: TObject);
begin
  (Sender as TAction).Enabled := FRenderer.CanRedo;
end;

procedure TMFMainForm.ZoomAtCursorExecute(Sender: TObject);
var
  Loc: TPoint;
begin
  Loc := Image1.ScreenToClient(Mouse.CursorPos);
  FRenderer.ZoomAtBy(Loc, 1);
end;

procedure TMFMainForm.NewExecute(Sender: TObject);
var
  siw: _STARTUPINFOW;
  pi: _PROCESS_INFORMATION;
begin
  ZeroMemory(@siw, sizeof(siw));
  siw.cb := sizeof(siw);
  ZeroMemory(@pi, sizeof(pi));
  CreateProcess(PWChar(Application.ExeName),
    nil, nil, nil, false, 0, nil, nil,
    siw, pi);
end;

procedure TMFMainForm.ExitAppExecute(Sender: TObject);
begin
  Close;
end;

procedure TMFMainForm.FractSettingsExecute(Sender: TObject);
begin
  with FractSettingsDlg do
  begin
    LoadFromClasses(FRenderer.Environment.HistoryCurrentBundle);
    ShowModal;
    if ModalResult = mrOK then
      FRenderer.ChangeSettingsOnly(DlgLocalSettings);
  end;

end;


procedure TMFMainForm.EditPaletteExecute(Sender: TObject);
begin
  with PaletteSettingsDlg do
  begin
    LoadFromClasses(FRenderer.Environment.HistoryCurrentBundle.Palette);
    ShowModal;
    if ModalResult = mrOK then
      FRenderer.ChangePalletteOnly(DlgLocalPalette);
  end;
end;

procedure TMFMainForm.SaveHelper(Obj: TObject; FName: string);
var
  MS: TMemoryStream;
  FS: TFileStream;
  SS: TStreamSysXML;
begin
  MS := TMemoryStream.Create;
  try
    SS := GetStreamSystemForTransaction;
    if not SS.WriteStructureToStream(Obj, MS) then
    begin
      MessageDlg('Internal error saving: ' +
        GetTransactionErr,
        mtError, [mbOK], 0);
    end;
    FS := TFileStream.Create(FName, fmCreate or fmShareDenyWrite);
    try
      FS.Seek(0, soFromBeginning);
      MS.Seek(0, soFromBeginning);
      FS.CopyFrom(MS, MS.Size);
    finally
      FS.Free;
    end;
  finally
    MS.Free;
  end;
end;

function TMFMainForm.LoadHelper(FName: string): TObject;
var
  MS: TMemoryStream;
  FS: TFileStream;
  SS: TStreamSysXML;
begin
  MS := TMemoryStream.Create;
  try
    FS := TFileStream.Create(FName, fmOpenRead or fmShareDenyWrite);
    try
      FS.Seek(0, soFromBeginning);
      MS.Seek(0, soFromBeginning);
      MS.CopyFrom(FS, FS.Size);
    finally
      FS.Free;
    end;
    SS := GetStreamSystemForTransaction;
    result := SS.ReadStructureFromStream(MS);
    if not Assigned(result) then
    begin
      MessageDlg('Error loading: ' +
        GetTransactionErr,
        mtError, [mbOK], 0);
    end;
  finally
    MS.Free;
  end;
end;

procedure TMFMainForm.SaveFractExecute(Sender: TObject);
var
  FileName: string;
  OpType: TFileOpType;
  Saveable: TFractV2Environment;
begin
  SaveDialog1.Filter := SaveFilter;
  SaveDialog1.FilterIndex := 0;
  if SaveDialog1.Execute then
  begin
    OpType := FilterIndexToOpType(SaveDialog1.FilterIndex);
    FileName := SaveDialog1.FileName;
    FixupFilename(FileName, OpType);
    case OpType of
      fotFract:
      begin
        Saveable := FRenderer.Environment.MakeSaveableCopy;
        try
          SaveHelper(Saveable, FileName);
        finally
          Saveable.Free;
        end;
      end;
      fotPal: SaveHelper(FRenderer.Environment.HistoryCurrentBundle.Palette, FileName);
    else
      Assert(false);
    end;
  end;
end;

procedure TMFMainForm.LoadFractExecute(Sender: TObject);
var
  Obj, Obj2: TObject;
  Err: string;
  FileName: string;
  OpType: TFileOpType;
begin
  OpenDialog1.Filter := LoadFilter;
  OpenDialog1.FilterIndex := 0;
  if OpenDialog1.Execute then
  begin
    FileName := OpenDialog1.FileName;
    OpType := FilterIndexToOpType(OpenDialog1.FilterIndex);
    FixupFileName(FileName, OpType);
    Obj := LoadHelper(FileName);
    Err := '';
    if Assigned(Obj) and (Obj is TFractSettings) then
    begin
      Obj2 := (Obj as TFractSettings).ConvertLegacySettings(Err);
      if Assigned(Obj2) then
      begin
        Obj.Free;
        Obj := Obj2;
      end;
    end;
    try
      if Assigned(Obj) and (Obj is TFractStreamable) then
      begin
          case OpType of
            fotFract: FRenderer.FileLoadAll(TFractStreamable(Obj), Err);
            fotPal: FRenderer.FileLoadPalette(TFractStreamable(Obj), Err);
          end;
      end
      else
        Err:= 'Error. No valid data found in input file';
    finally
      Obj.Free;
    end;
    if Length(Err) > 0 then
      MessageDlg(Err, mtError, [mbOK], 0);
  end;
end;

procedure TMFMainForm.HelpAboutExecute(Sender: TObject);
begin
  FractAboutDlg.ShowModal;
end;

procedure TMFMainForm.ExpImageExecute(Sender: TObject);
var
  FileName: string;
  FileNamePng: string;
  FS: TFileStream;
  RDlg: TReRenderFrm;
  HostSize: TPoint;
begin
  ExportOptsDlg.ShowModal;
  if ExportOptsDlg.ModalResult <> mrOk then
    exit;
  SaveDialog1.Filter := ExportImageFilter;
  SaveDialog1.FilterIndex := 0;
  if SaveDialog1.Execute then
  begin
    FileName := SaveDialog1.FileName;
    FileNamePng := FileName;
    FixupFileName(FileName, fotBmp);
    FixupFileName(FileNamePng, fotPng);
    FS := TFileStream.Create(FileName, fmCreate or fmShareDenyWrite);
    try
      if ExportOptsDlg.JustSave then
      begin
        Image1.Picture.Bitmap.SaveToStream(FS);
      end
      else if ExportOptsDlg.ReRender then
      begin
        RDlg := TReRenderFrm.Create(Application);
        HostSize.x := Image1.Width;
        HostSize.y := Image1.Height;
        RDlg.BackgroundRender(FRenderer.Environment.HistoryCurrentBundle,
          FS,
          HostSize,
          ExportOptsDlg.RenderSize,
          ExportOptsDlg.MakeDesktop,
          FileName,
          FileNamePng);
        FS := nil;
      end;
    finally
      FS.Free;
    end;
  end;
end;

procedure TMFMainForm.HelpContentsExecute(Sender: TObject);
var
  HelpFileDoc: string;
  Inst: HINST;
begin
  HelpFileDoc := ExtractFilePath(Application.ExeName) + HelpFileLocation;
  Inst := ShellExecute(Self.Handle,
    'open',
    PChar(HelpFileDoc),
    nil,
    nil,
    SW_SHOW);
  if not (Inst > 32) then
    MessageDlg('Sorry, couldn''t open the help file.', mtError, [mbOK], 0);
end;

procedure TMFMainForm.HistoryClearExecute(Sender: TObject);
begin
  FRenderer.Environment.HistoryClear;
  FRenderer.Environment.HistoryCurrentBundle.HintString := '';
end;

procedure TMFMainForm.HistoryClearUpdate(Sender: TObject);
begin
  (Sender as TAction).Enabled := FRenderer.Environment.HistoryCanClear;
end;

procedure TMFMainForm.StatusUpdate(Sender: TObject);
var
  Running: boolean;
begin
  Running := FRenderer.Running;
  if Running then
  begin
    StatusBar1.Panels[0].Text := SecsToStr(FRenderer.SecsRemaining)
      + IntToStr(FRenderer.PercentComplete) + '% done.';
  end
  else
    StatusBar1.Panels[0].Text := 'Idle';
end;

procedure TMFMainForm.UndoExecute(Sender: TObject);
begin
  FRenderer.Undo;
end;

procedure TMFMainForm.UndoUpdate(Sender: TObject);
begin
  (Sender as TAction).Enabled := FRenderer.CanUndo;
end;

procedure TMFMainForm.Image1DblClick(Sender: TObject);
begin
  ZoomAtCursor.Execute;
end;

procedure TMFMainForm.Image1StartDrag(Sender: TObject;
  var DragObject: TDragObject);
var
  HelperPos: TPoint;
begin
  HelperPos := MFMainForm.ClientOrigin;
  Inc(HelperPos.X, HelperFormOffset);
  Inc(HelperPos.Y, HelperFormOffset);
  DragHelperForm.OnGetBackgroundBmp := HandleBmpRequest;
  DragHelperForm.SetBounds(HelperPos.X, HelperPos.Y,
                           DragHelperForm.Width, DragHelperForm.Height);
  DragHelperForm.UpdateForm(Image1.DragOffset,
                            Image1.DragMagnitude,
                            Image1.DragAngle,
                            Image1.DraggingButton);
  DragHelperForm.Show;
end;

procedure TMFMainForm.Image1DragOver(Sender, Source: TObject; X,
  Y: Integer; State: TDragState; var Accept: Boolean);
begin
  DragHelperForm.UpdateForm(Image1.DragOffset,
                            Image1.DragMagnitude,
                            Image1.DragAngle,
                            Image1.DraggingButton);
end;

procedure TMFMainForm.Image1DragDrop(Sender, Source: TObject; X,
  Y: Integer);
begin
  DragHelperForm.UpdateForm(Image1.DragOffset,
                            Image1.DragMagnitude,
                            Image1.DragAngle,
                            Image1.DraggingButton);
  DragHelperForm.Hide;
  case DragHelperForm.DragType of
    fdtMove: FRenderer.MoveByPels((Sender as TSizeableImage).DragOffset);
    fdtZoom: FRenderer.ZoomAtBy(Image1.StartDragLoc, DragHelperForm.DragZoomConstant);
    fdtRotate: FRenderer.RotateBy(DragHelperForm.DragRotateConstant);
  else
    Assert(false);
  end;

end;

procedure TMFMainForm.FormMouseWheel(Sender: TObject; Shift: TShiftState;
  WheelDelta: Integer; MousePos: TPoint; var Handled: Boolean);
var
  ZoomDelta: double;
begin
  if WheelDelta = 0 then
    exit
  else if WheelDelta > 0 then
    ZoomDelta := 0.25
  else if WheelDelta < 0 then
    ZoomDelta := -0.25;
  FRenderer.ZoomBy(ZoomDelta);
end;

procedure TMFMainForm.HandleBmpRequest(Sender: TObject; ScreenRect: TRect; var Bmp: TBitmap);
var
  DR, SR: TRect;
begin
  Assert(not Assigned(Bmp));
  Bmp := TBitmap.Create;
  Bmp.Width := ScreenRect.Right - ScreenRect.Left;
  Bmp.Height := ScreenRect.Bottom - ScreenRect.Top;
  DR.Top := 0;
  DR.Left := 0;
  DR.Bottom := Bmp.Height;
  DR.Right := Bmp.Width;
  SR.TopLeft := Image1.ScreenToClient(ScreenRect.TopLeft);
  SR.BottomRight := Image1.ScreenToClient(ScreenRect.BottomRight);
  Bmp.Canvas.CopyRect(DR, Image1.Canvas, SR);
end;

procedure TMFMainForm.MoreDetailExecute(Sender: TObject);
begin
  FRenderer.ChangeDetail(+1);
end;

procedure TMFMainForm.LessDetailExecute(Sender: TObject);
begin
  FRenderer.ChangeDetail(-1);
end;

procedure TMFMainForm.MoreDetailUpdate(Sender: TObject);
begin
  (Sender as TAction).Enabled := FRenderer.CanChangeDetail(+1);
end;

procedure TMFMainForm.LessDetailUpdate(Sender: TObject);
begin
  (Sender as TAction).Enabled := FRenderer.CanChangeDetail(-1);
end;

procedure TMFMainForm.HistoryMenuActionExecute(Sender: TObject);
begin
  //Intentionally left blank.
end;

procedure TMFMainForm.HistoryMenuActionUpdate(Sender: TObject);
begin
  UpdateDynamicMenuList(History1, N2,
                        HistoryDynamicMenuItems,
                        UpdateDynamicHistoryHelper);
end;

procedure TMFMainForm.UpdateDynamicHistoryHelper(Sender: TObject;
                                                 ToUpdate: TMenuItem;
                                                 DynIdx: integer;
                                                 NumDynamics: integer);
var
  HistFirstIdx, HistLastIdx: integer;
  HistIdx: integer;

begin
  HistFirstIdx := Integer(FRenderer.Environment.HistoryIndex) - (NumDynamics div 2);
  HistLastIdx := HistFirstIdx + Pred(NumDynamics);
  while HistLastIdx > Pred(Integer(FRenderer.Environment.HistoryCount)) do
  begin
    Dec(HistFirstIdx);
    Dec(HistLastIdx);
  end;
  while HistFirstIdx < 0 do
    Inc(HistFirstIdx);

  HistIdx := HistFirstIdx + DynIdx;
  if (HistIdx > HistLastIdx) or (HistIdx < HistFirstIdx) then
  begin
    ToUpdate.Enabled := false;
    ToUpdate.Visible := false;
  end
  else
  begin
    ToUpdate.Enabled := true;
    ToUpdate.Visible := true;
    ToUpdate.Tag := HistIdx;
    ToUpdate.Caption := IntToStr(HistIdx) + ': ' + FRenderer.Environment.HistoryStrings[HistIdx];
    ToUpdate.Checked := (Cardinal(HistIdx) = FRenderer.Environment.HistoryIndex);
  end;
end;

procedure TMFMainForm.UpdateDynamicVideoHelper(Sender: TObject;
                                                 ToUpdate: TMenuItem;
                                                 DynIdx: integer;
                                                 NumDynamics: integer);
begin
  ToUpdate.Enabled := false;
  ToUpdate.Visible := false;
end;


procedure TMFMainForm.UpdateDynamicMenuList(
  ParentItem: TMenuItem;
  PrecedingSeparator: TMenuItem;
  NumDynamics: integer;
  HelperFunc: TUpdateDynamicHelperFunc);

var
  idx: Integer;
  DynOffset: integer;
  IsDyn: boolean;
begin
  IsDyn := false;
  DynOffset := 0;
  for idx := 0 to Pred(ParentItem.Count) do
  begin
    if not IsDyn then
    begin
      if ParentItem.Items[idx] = PrecedingSeparator then
      begin
        IsDyn := true;
        DynOffset := Succ(idx);
      end;
    end
    else
    begin
      HelperFunc(ParentItem, ParentItem.Items[idx], idx - DynOffset, NumDynamics);
    end;
  end;
end;

procedure TMFMainForm.CreateDynamicMenuItems;
var
  idx: integer;
  It: TMenuItem;
begin
  for idx := 0 to Pred(HistoryDynamicMenuItems) do
  begin
    It := TMenuItem.Create(History1);
    It.Name := 'HistoryTmp' + IntToStr(idx);
    It.Tag := idx;
    It.Visible := false;
    It.OnClick := HistoryDynamicMenuClick;
    History1.Add(It);
  end;
end;

procedure TMFMainForm.HistoryDynamicMenuClick(Sender: TObject);
var
  Snd: TMenuItem;
  HistIdx: integer;
begin
  Snd := (Sender as TMenuItem);
  HistIdx := Snd.Tag;
  if (HistIdx >= 0) and (HistIdx < Integer(FRenderer.Environment.HistoryCount)) then
  begin
    while (Integer(FRenderer.Environment.HistoryIndex) > HistIdx) do
    begin
      Assert(FRenderer.CanUndo);
      FRenderer.Undo;
    end;
    while (Integer(FRenderer.Environment.HistoryIndex) < HistIdx) do
    begin
      Assert(FRenderer.CanRedo);
      FRenderer.Redo;
    end;
    Assert(HistIdx = Integer(FRenderer.Environment.HistoryIndex));
  end;
end;

procedure TMFMainForm.VideoDynamicMenuClick(Sender: TObject);
begin
  //TODO - write this.
end;

end.

