object PageMapperForm: TPageMapperForm
  Left = 0
  Top = 0
  Caption = 'PageMapper'
  ClientHeight = 318
  ClientWidth = 527
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  PixelsPerInch = 96
  TextHeight = 13
  object Splitter1: TSplitter
    Left = 0
    Top = 237
    Width = 527
    Height = 3
    Cursor = crVSplit
    Align = alBottom
    ExplicitTop = 41
    ExplicitWidth = 199
  end
  object Splitter2: TSplitter
    Left = 121
    Top = 41
    Height = 196
    ExplicitLeft = 272
    ExplicitTop = 128
    ExplicitHeight = 100
  end
  object Panel1: TPanel
    Left = 0
    Top = 0
    Width = 527
    Height = 41
    Align = alTop
    TabOrder = 0
    object LoadBtn: TButton
      Left = 320
      Top = 8
      Width = 75
      Height = 25
      Caption = 'Load'
      Default = True
      TabOrder = 0
      OnClick = LoadBtnClick
    end
    object ScriptCheck: TCheckBox
      Left = 416
      Top = 12
      Width = 105
      Height = 17
      Caption = 'Load Scripts too'
      TabOrder = 1
    end
    object UrlEdit: TEdit
      Left = 16
      Top = 8
      Width = 297
      Height = 21
      TabOrder = 2
      Text = 'http://'
    end
  end
  object ErrMemo: TMemo
    Left = 0
    Top = 240
    Width = 527
    Height = 78
    Align = alBottom
    ScrollBars = ssBoth
    TabOrder = 1
  end
  object TreeView: TTreeView
    Left = 124
    Top = 41
    Width = 403
    Height = 196
    Align = alClient
    Indent = 19
    TabOrder = 2
  end
  object ListBox: TListBox
    Left = 0
    Top = 41
    Width = 121
    Height = 196
    Align = alLeft
    ItemHeight = 13
    ScrollWidth = 5000
    TabOrder = 3
    OnDblClick = ListBoxDblClick
  end
end
