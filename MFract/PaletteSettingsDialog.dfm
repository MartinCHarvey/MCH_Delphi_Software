object PaletteSettingsDlg: TPaletteSettingsDlg
  Left = 208
  Top = 115
  BorderStyle = bsDialog
  Caption = 'Palette Settings'
  ClientHeight = 258
  ClientWidth = 440
  Color = clBtnFace
  ParentFont = True
  OldCreateOrder = True
  Position = poScreenCenter
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  OnShow = FormShow
  PixelsPerInch = 96
  TextHeight = 13
  object Bevel1: TBevel
    Left = 8
    Top = 8
    Width = 424
    Height = 210
    Anchors = [akLeft, akTop, akRight, akBottom]
    Shape = bsFrame
  end
  object Label1: TLabel
    Left = 24
    Top = 160
    Width = 72
    Height = 13
    Caption = 'Palette Length:'
  end
  object PalLenLbl: TLabel
    Left = 104
    Top = 160
    Width = 47
    Height = 13
    Caption = 'PalLenLbl'
  end
  object OKBtn: TButton
    Left = 144
    Top = 228
    Width = 75
    Height = 25
    Anchors = [akLeft, akBottom]
    Caption = 'OK'
    Default = True
    ModalResult = 1
    TabOrder = 0
  end
  object CancelBtn: TButton
    Left = 224
    Top = 228
    Width = 75
    Height = 25
    Anchors = [akLeft, akBottom]
    Cancel = True
    Caption = 'Cancel'
    ModalResult = 2
    TabOrder = 1
  end
  object NewBtn: TButton
    Left = 119
    Top = 180
    Width = 75
    Height = 25
    Anchors = [akLeft, akBottom]
    Caption = 'New'
    TabOrder = 2
    OnClick = NewBtnClick
  end
  object DelBtn: TButton
    Left = 199
    Top = 180
    Width = 75
    Height = 25
    Anchors = [akLeft, akBottom]
    Caption = 'Delete'
    TabOrder = 3
    OnClick = DelBtnClick
  end
  object ResetBtn: TButton
    Left = 279
    Top = 180
    Width = 75
    Height = 25
    Anchors = [akLeft, akBottom]
    Caption = 'Reset'
    TabOrder = 4
    OnClick = ResetBtnClick
  end
  object Panel1: TPanel
    Left = 16
    Top = 16
    Width = 407
    Height = 129
    Anchors = [akLeft, akTop, akRight]
    Caption = 'Panel1'
    TabOrder = 5
    object PaintBox1: TPaintBox
      Left = 1
      Top = 33
      Width = 405
      Height = 16
      Align = alTop
      DragCursor = crSizeWE
      OnDragDrop = PaintBox1DragDrop
      OnDragOver = PaintBox1DragOver
      OnMouseDown = PaintBox1MouseDown
      OnPaint = PaintBox1Paint
    end
    object Image1: TImage
      Left = 1
      Top = 49
      Width = 405
      Height = 79
      Align = alClient
    end
    object DrawGrid1: TDrawGrid
      Left = 1
      Top = 1
      Width = 405
      Height = 32
      Align = alTop
      FixedCols = 0
      FixedRows = 0
      TabOrder = 0
      OnDblClick = DrawGrid1DblClick
      OnDrawCell = DrawGrid1DrawCell
      OnSelectCell = DrawGrid1SelectCell
    end
  end
  object TrackBar1: TTrackBar
    Left = 176
    Top = 152
    Width = 249
    Height = 25
    Orientation = trHorizontal
    Frequency = 1
    Position = 0
    SelEnd = 0
    SelStart = 0
    TabOrder = 6
    TickMarks = tmBottomRight
    TickStyle = tsAuto
    OnChange = TrackBar1Change
  end
  object LogCheck: TCheckBox
    Left = 24
    Top = 184
    Width = 81
    Height = 17
    Caption = 'Logarithmic'
    TabOrder = 7
    OnClick = LogCheckClick
  end
  object ColorDialog1: TColorDialog
    Ctl3D = True
    Left = 400
    Top = 184
  end
end
