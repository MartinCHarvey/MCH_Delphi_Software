object fmTest-->Grammar<--: TfmTest-->Grammar<--
  Left = 141
  Top = 134
  Width = 544
  Height = 375
  Caption = 'Test -->Grammar<--'
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'MS Sans Serif'
  Font.Style = []
  OldCreateOrder = False
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  PixelsPerInch = 96
  TextHeight = 13
  object splSourceOutput: TSplitter
    Left = 0
    Top = 181
    Width = 536
    Height = 6
    Cursor = crVSplit
    Align = alBottom
    Beveled = True
  end
  object pnlButtons: TPanel
    Left = 0
    Top = 0
    Width = 536
    Height = 29
    Align = alTop
    TabOrder = 0
    object btnOpen: TButton
      Left = 4
      Top = 2
      Width = 75
      Height = 25
      Hint = 'Load a source file'
      Caption = '&Open'
      ParentShowHint = False
      ShowHint = True
      TabOrder = 0
      OnClick = btnOpenClick
    end
    object btnExecute: TButton
      Left = 161
      Top = 2
      Width = 75
      Height = 25
      Hint = 'Parse the source file'
      Caption = '&Execute'
      ParentShowHint = False
      ShowHint = True
      TabOrder = 1
      OnClick = btnExecuteClick
    end
    object btnSave: TButton
      Left = 83
      Top = 2
      Width = 75
      Height = 25
      Hint = 'Save this source file'
      Caption = '&Save'
      Enabled = False
      ParentShowHint = False
      ShowHint = True
      TabOrder = 2
      OnClick = btnSaveClick
    end
    -->VersionBtnDFM<--
  end
  object pnlSource: TPanel
    Left = 0
    Top = 0
    Width = 200
    Height = 100
    Align = alClient
    TabOrder = 1
    object memSource: -->MemoType<--
      Left = 0
      Top = 0
      Width = 200
      Height = 100
      Align = alClient
      Font.Charset = ANSI_CHARSET
      Font.Color = clWindowText
      Font.Height = -11
      Font.Name = 'Courier New'
      Font.Style = []
      ParentFont = False
      ScrollBars = ssBoth
      WordWrap = False
      TabOrder = 0
      OnChange = memSourceChange
      -->MemoProperties<--
    end
  end
  object pnlOutput: TPanel
    Left = 0
    Top = 187
    Width = 536
    Height = 161
    Align = alBottom
    TabOrder = 2
    object memOutput: -->MemoType<--
      Left = 0
      Top = 0
      Width = 200
      Height = 100
      Align = alClient
      Font.Charset = ANSI_CHARSET
      Font.Color = clWindowText
      Font.Height = -11
      Font.Name = 'Courier New'
      Font.Style = []
      ParentFont = False
      ScrollBars = ssBoth
      WordWrap = False
      TabOrder = 0
      -->MemoProperties<--
    end
  end
  object OpenDialog: TOpenDialog
    Left = 100
    Top = 50
  end
  object SaveDialog: TSaveDialog
    Options = [ofOverwritePrompt, ofHideReadOnly, ofEnableSizing]
    Left = 200
    Top = 50
  end
end
