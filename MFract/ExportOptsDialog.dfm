object ExportOptsDlg: TExportOptsDlg
  Left = 292
  Top = 186
  BorderStyle = bsDialog
  Caption = 'Export options'
  ClientHeight = 223
  ClientWidth = 293
  Color = clBtnFace
  ParentFont = True
  OldCreateOrder = True
  Position = poScreenCenter
  OnCloseQuery = FormCloseQuery
  OnShow = FormShow
  PixelsPerInch = 96
  TextHeight = 13
  object Bevel1: TBevel
    Left = 8
    Top = 8
    Width = 276
    Height = 169
    Anchors = [akLeft, akTop, akRight]
    Shape = bsFrame
  end
  object Label1: TLabel
    Left = 176
    Top = 98
    Width = 5
    Height = 13
    Caption = 'x'
  end
  object OKBtn: TButton
    Left = 69
    Top = 188
    Width = 75
    Height = 25
    Caption = 'OK'
    Default = True
    ModalResult = 1
    TabOrder = 0
  end
  object CancelBtn: TButton
    Left = 149
    Top = 188
    Width = 75
    Height = 25
    Cancel = True
    Caption = 'Cancel'
    ModalResult = 2
    TabOrder = 1
  end
  object SaveWindowBtn: TRadioButton
    Left = 24
    Top = 24
    Width = 249
    Height = 17
    Caption = 'Save the window contents as-is'
    Checked = True
    TabOrder = 2
    TabStop = True
  end
  object Render800Btn: TRadioButton
    Left = 24
    Top = 48
    Width = 249
    Height = 17
    Caption = 'Recalculate at 800x600'
    TabOrder = 3
  end
  object Render1024Btn: TRadioButton
    Left = 24
    Top = 72
    Width = 249
    Height = 17
    Caption = 'Recalculate at 1024x768'
    TabOrder = 4
  end
  object RenderScreenBtn: TMonRadioButton
    Left = 24
    Top = 120
    Width = 249
    Height = 17
    Caption = 'Recalculate at screen resolution'
    TabOrder = 5
    OnCheckedChange = RenderScreenBtnCheckedChange
  end
  object MakeDeskChk: TCheckBox
    Left = 40
    Top = 144
    Width = 193
    Height = 17
    Caption = 'Make the result my desktop'
    Enabled = False
    TabOrder = 6
  end
  object RenderCustomBtn: TRadioButton
    Left = 24
    Top = 96
    Width = 97
    Height = 17
    Caption = 'Recalculate at'
    TabOrder = 7
  end
  object Xres: TEdit
    Left = 120
    Top = 94
    Width = 49
    Height = 21
    TabOrder = 8
    Text = 'Xres'
  end
  object YRes: TEdit
    Left = 192
    Top = 94
    Width = 49
    Height = 21
    TabOrder = 9
    Text = 'YRes'
  end
end
