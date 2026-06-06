object fmTestHomographTest: TfmTestHomographTest
  Left = 51
  Top = 70
  Width = 695
  Height = 459
  Caption = 'Test HomographTest'
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
    Top = 265
    Width = 687
    Height = 6
    Cursor = crVSplit
    Align = alBottom
    Beveled = True
  end
  object pnlButtons: TPanel
    Left = 0
    Top = 0
    Width = 687
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
    object btnVersion: TButton
      Left = 275
      Top = 2
      Width = 75
      Height = 25
      Hint = 'DirectiveTest Version'
      Caption = '&Version'
      ParentShowHint = False
      ShowHint = True
      TabOrder = 3
      OnClick = btnVersionClick
    end
    object chkUseHomograph: TCheckBox
      Left = 389
      Top = 7
      Width = 108
      Height = 17
      Caption = 'Use Homographs'
      Checked = True
      State = cbChecked
      TabOrder = 4
      OnClick = chkUseHomographClick
    end
  end
  object pnlSource: TPanel
    Left = 0
    Top = 29
    Width = 687
    Height = 236
    Align = alClient
    TabOrder = 1
    object memSource: TMemo
      Left = 1
      Top = 1
      Width = 685
      Height = 234
      Align = alClient
      Font.Charset = ANSI_CHARSET
      Font.Color = clWindowText
      Font.Height = -11
      Font.Name = 'Courier New'
      Font.Style = []
      Lines.Strings = (
        'property Successful;'
        'property MajorVersion : integer read GetMajorVersion;'
        
          'property MinorVersion : integer read GetMinorVersion write SetMi' +
          'norVersion;'
        'property Release : integer write SetRelease;'
        ''
        'hello(x,y);'
        'write(x);'
        'read;'
        'write(x, y, z);'
        ''
        'procedure test;'
        'procedure test1(var x : integer);'
        'procedure test2(var y : string);'
        'procedure out(out out : boolean);')
      ParentFont = False
      ScrollBars = ssBoth
      TabOrder = 0
      WordWrap = False
      OnChange = memSourceChange
    end
  end
  object pnlOutput: TPanel
    Left = 0
    Top = 271
    Width = 687
    Height = 161
    Align = alBottom
    TabOrder = 2
    object memOutput: TMemo
      Left = 1
      Top = 1
      Width = 685
      Height = 159
      Align = alClient
      Font.Charset = ANSI_CHARSET
      Font.Color = clWindowText
      Font.Height = -11
      Font.Name = 'Courier New'
      Font.Style = []
      ParentFont = False
      ScrollBars = ssBoth
      TabOrder = 0
      WordWrap = False
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
