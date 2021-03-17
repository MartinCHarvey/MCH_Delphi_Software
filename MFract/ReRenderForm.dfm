object ReRenderFrm: TReRenderFrm
  Left = 190
  Top = 116
  BorderIcons = []
  BorderStyle = bsDialog
  Caption = 'Calculating'
  ClientHeight = 116
  ClientWidth = 336
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
  object RenderInfo: TLabel
    Left = 8
    Top = 8
    Width = 53
    Height = 13
    Caption = 'RenderInfo'
  end
  object DstFileInfo: TLabel
    Left = 8
    Top = 32
    Width = 50
    Height = 13
    Caption = 'DstFileInfo'
  end
  object ProgressBar: TProgressBar
    Left = 8
    Top = 56
    Width = 313
    Height = 17
    TabOrder = 0
  end
  object CancelBtn: TButton
    Left = 130
    Top = 80
    Width = 75
    Height = 25
    Caption = 'Cancel'
    TabOrder = 1
    OnClick = CancelBtnClick
  end
end
