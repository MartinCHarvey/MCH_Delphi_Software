unit ReRenderForm;

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, Forms, Dialogs,
  StdCtrls, ComCtrls, FractRenderers, FractSettings, ActiveX, ShlObj;

type
  TReRenderFrm = class(TForm)
    RenderInfo: TLabel;
    DstFileInfo: TLabel;
    ProgressBar: TProgressBar;
    CancelBtn: TButton;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure CancelBtnClick(Sender: TObject);
  private
    { Private declarations }
    FRenderer: TFractBitmapRenderer;
    FMakeDesktop: boolean;
    FFileNameCopy: string;
    FFileNamePngCopy: string;
    procedure HandleRenderProgress(Sender: TObject);
    procedure SetDesktopWallpaper;
  public
    { Public declarations }
    procedure BackgroundRender(Settings: TFractV2SettingsBundle;
      OutputStream: TStream;
      HostSize: TPoint;
      RenderSize: TPoint;
      MakeDesktop: boolean;
      FileName: string;
      FileNamePng: string);
  end;

var
  ReRenderFrm: TReRenderFrm;

implementation

uses FractMisc, UITypes, Math;

{$R *.DFM}

const
  WIDE_ARRAY_LEN = 1024;

procedure TReRenderFrm.SetDesktopWallpaper;
var
  hr: HRESULT;
  pActiveDesktop: IActiveDesktop;
  WStrFileName: WideString;
  WICImage, WICImage2: TWICImage;
begin
  WICImage := TWICImage.Create;
  WICImage2 := TWICImage.Create;
  try
    WICImage.ImageFormat := wifBmp;
    WICImage.LoadFromStream(FRenderer.OutputStream);
    WICImage2.ImageFormat := wifPng;
    WICImage2.Assign(WICImage);
    WICImage2.SaveToFile(FFileNamePngCopy);

    WStrFileName := FFileNamePngCopy;
    //TODO .. Need to take the output bmp file, and make it jpg or png
    //happy, so can be set as background.
    hr := CoCreateInstance(CLSID_ActiveDesktop, nil, CLSCTX_INPROC_SERVER,
                           IID_IActiveDesktop, pActiveDesktop);
    if hr = 0 then
    begin
      pActiveDesktop._AddRef(); //Only partly necessary...
      try
        hr := pActiveDesktop.SetWallpaper(PWideChar(WStrFileName), 0);
        if hr = 0 then
        begin
          hr := pActiveDesktop.ApplyChanges(AD_APPLY_ALL);
          Assert(hr = 0);
        end;
      finally
        pActiveDesktop._Release(); //Only partly necessary...
      end;
    end;
  finally
    WICImage.Free;
    WICImage2.Free;
  end;
end;

procedure TReRenderFrm.HandleRenderProgress(Sender: TObject);
var
  Size: TPoint;

begin
  Size := FRenderer.Size;
  if FRenderer.Running then
  begin
    RenderInfo.Caption := 'Calculating ('
      + IntToStr(Size.x)
      + 'x'
      + IntToStr(Size.y)
      + ') estimate ' + SecsToStr(FRenderer.SecsRemaining);
  end
  else
  begin
    if FRenderer.Saved then
      RenderInfo.Caption := 'Done'
    else
      RenderInfo.Caption := 'Starting';
  end;
  ProgressBar.Position := FRenderer.PercentComplete;
  if FRenderer.Saved then
  begin
    if FMakeDesktop then
      SetDesktopWallpaper;
    Release;
  end;
end;

procedure TReRenderFrm.BackgroundRender(Settings: TFractV2SettingsBundle;
  OutputStream: TStream;
  HostSize: TPoint;
  RenderSize: TPoint;
  MakeDesktop: boolean;
  FileName: string;
  FileNamePng: string);
var
  TmpSettings: TFractV2SettingsBundle;
  ErrStr: string;
  ZoomScale: double;
begin
  TmpSettings := TFractV2SettingsBundle.Create;
  try
    TmpSettings.Assign(Settings);
    //Zoom by zooms by MultPPU ^ x.
    ZoomScale := Sqrt((RenderSize.x * RenderSize.y) / (HostSize.x * HostSize.y));
    ZoomScale := LogN(TmpSettings.Formula.MultPPU, ZoomScale);
    TmpSettings.DoZoomBy(ZoomScale);
    FRenderer.OutputStream := OutputStream;
    FRenderer.OnProgress := HandleRenderProgress;
    FRenderer.FileLoadAll(TmpSettings, ErrStr);
    if Length(ErrStr) > 0 then
    begin
      MessageDlg(ErrStr, mtError, [mbOK], 0);
      Release;
    end
    else
    begin
      FRenderer.Size := RenderSize;
      FRenderer.ThreadPriority := tpLower;
      FRenderer.StartRender;
      FMakeDesktop := MakeDesktop;
      DstFileInfo.Caption := FileName;
      FFileNameCopy := FileName;
      FFileNamePngCopy := FileNamePng;
      Show;
    end;
  finally
    TmpSettings.Free;
  end;
end;

procedure TReRenderFrm.FormCreate(Sender: TObject);
begin
  FRenderer := TFractBitmapRenderer.Create;
end;

procedure TReRenderFrm.FormDestroy(Sender: TObject);
begin
  FRenderer.OnProgress := nil;
  FRenderer.OutputStream.Free;
  FRenderer.Free;
end;

procedure TReRenderFrm.CancelBtnClick(Sender: TObject);
begin
  Release;
end;

end.

