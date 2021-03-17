object FractSettingsDlg: TFractSettingsDlg
  Left = 321
  Top = 178
  BorderStyle = bsDialog
  Caption = 'Fractal Settings'
  ClientHeight = 325
  ClientWidth = 439
  Color = clBtnFace
  ParentFont = True
  OldCreateOrder = True
  Position = poScreenCenter
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  PixelsPerInch = 96
  TextHeight = 13
  object Bevel1: TBevel
    Left = 8
    Top = 8
    Width = 425
    Height = 273
    Shape = bsFrame
  end
  object Label1: TLabel
    Left = 24
    Top = 96
    Width = 41
    Height = 13
    Caption = 'Center X'
  end
  object Label2: TLabel
    Left = 240
    Top = 96
    Width = 41
    Height = 13
    Caption = 'Center Y'
  end
  object Label3: TLabel
    Left = 24
    Top = 128
    Width = 64
    Height = 13
    Caption = 'Current Zoom'
  end
  object Label4: TLabel
    Left = 24
    Top = 160
    Width = 54
    Height = 13
    Caption = 'Initial Zoom'
  end
  object Label5: TLabel
    Left = 24
    Top = 192
    Width = 41
    Height = 13
    Caption = 'Zoom by'
  end
  object Label6: TLabel
    Left = 24
    Top = 224
    Width = 62
    Height = 13
    Caption = 'Grid divisions'
  end
  object Label7: TLabel
    Left = 24
    Top = 256
    Width = 52
    Height = 13
    Caption = 'Detail level'
  end
  object ZoomInitLbl: TLabel
    Left = 104
    Top = 160
    Width = 55
    Height = 13
    Caption = 'ZoomInitLbl'
  end
  object ZoomByLbl: TLabel
    Left = 104
    Top = 192
    Width = 53
    Height = 13
    Caption = 'ZoomByLbl'
  end
  object DivLbl: TLabel
    Left = 104
    Top = 224
    Width = 30
    Height = 13
    Caption = 'DivLbl'
  end
  object DetLbl: TLabel
    Left = 104
    Top = 256
    Width = 31
    Height = 13
    Caption = 'DetLbl'
  end
  object Label8: TLabel
    Left = 24
    Top = 64
    Width = 38
    Height = 13
    Caption = 'Julia CX'
  end
  object Label9: TLabel
    Left = 240
    Top = 64
    Width = 38
    Height = 13
    Caption = 'Julia CY'
  end
  object Label10: TLabel
    Left = 24
    Top = 24
    Width = 24
    Height = 13
    Caption = 'Type'
  end
  object TypeLbl: TLabel
    Left = 96
    Top = 24
    Width = 38
    Height = 13
    Caption = 'TypeLbl'
  end
  object Label11: TLabel
    Left = 240
    Top = 128
    Width = 40
    Height = 13
    Caption = 'Rotation'
  end
  object OKBtn: TButton
    Left = 127
    Top = 292
    Width = 75
    Height = 25
    Caption = 'OK'
    Default = True
    ModalResult = 1
    TabOrder = 0
    OnClick = OKBtnClick
  end
  object CancelBtn: TButton
    Left = 207
    Top = 292
    Width = 75
    Height = 25
    Cancel = True
    Caption = 'Cancel'
    ModalResult = 2
    TabOrder = 1
  end
  object EditX: TEdit
    Left = 96
    Top = 88
    Width = 121
    Height = 21
    TabOrder = 2
    Text = 'EditX'
  end
  object EditY: TEdit
    Left = 296
    Top = 88
    Width = 121
    Height = 21
    TabOrder = 3
    Text = 'EditY'
  end
  object EditZoom: TEdit
    Left = 96
    Top = 120
    Width = 121
    Height = 21
    TabOrder = 4
    Text = 'EditZoom'
  end
  object ZoomInitTrack: TTrackBar
    Left = 168
    Top = 152
    Width = 249
    Height = 25
    Max = 20
    Min = 1
    Orientation = trHorizontal
    Frequency = 1
    Position = 1
    SelEnd = 0
    SelStart = 0
    TabOrder = 5
    TickMarks = tmBottomRight
    TickStyle = tsAuto
    OnChange = TracksChanged
  end
  object ZoomByTrack: TTrackBar
    Left = 168
    Top = 184
    Width = 249
    Height = 25
    Max = 20
    Min = 2
    Orientation = trHorizontal
    Frequency = 1
    Position = 2
    SelEnd = 0
    SelStart = 0
    TabOrder = 6
    TickMarks = tmBottomRight
    TickStyle = tsAuto
    OnChange = TracksChanged
  end
  object DivTrack: TTrackBar
    Left = 168
    Top = 216
    Width = 249
    Height = 25
    Max = 20
    Min = 2
    Orientation = trHorizontal
    Frequency = 1
    Position = 2
    SelEnd = 0
    SelStart = 0
    TabOrder = 7
    TickMarks = tmBottomRight
    TickStyle = tsAuto
    OnChange = TracksChanged
  end
  object DetTrack: TTrackBar
    Left = 168
    Top = 248
    Width = 249
    Height = 25
    Max = 32
    Min = 1
    Orientation = trHorizontal
    Frequency = 1
    Position = 2
    SelEnd = 0
    SelStart = 0
    TabOrder = 8
    TickMarks = tmBottomRight
    TickStyle = tsAuto
    OnChange = TracksChanged
  end
  object JuliaX: TEdit
    Left = 96
    Top = 56
    Width = 121
    Height = 21
    TabOrder = 9
    Text = 'JuliaX'
  end
  object JuliaY: TEdit
    Left = 296
    Top = 56
    Width = 121
    Height = 21
    TabOrder = 10
    Text = 'JuliaY'
  end
  object FlipBtn: TButton
    Left = 216
    Top = 16
    Width = 81
    Height = 25
    Caption = 'Flip'
    TabOrder = 11
    OnClick = FlipBtnClick
  end
  object EditRotate: TEdit
    Left = 296
    Top = 120
    Width = 121
    Height = 21
    TabOrder = 12
    Text = 'EditRotate'
  end
end
