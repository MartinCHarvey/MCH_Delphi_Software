object CoWTreeTestForm: TCoWTreeTestForm
  Left = 0
  Top = 0
  Caption = 'CoWTreeTestForm'
  ClientHeight = 242
  ClientWidth = 356
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
  object BasicInsertBtn: TButton
    Left = 95
    Top = 8
    Width = 75
    Height = 25
    Caption = 'AB Insert'
    TabOrder = 1
    OnClick = BasicInsertBtnClick
  end
  object BasicDeleteBtn: TButton
    Left = 176
    Top = 8
    Width = 75
    Height = 25
    Caption = 'AB Delete'
    TabOrder = 2
    OnClick = BasicDeleteBtnClick
  end
  object STTest: TButton
    Left = 14
    Top = 8
    Width = 75
    Height = 25
    Caption = 'Simple Tree'
    TabOrder = 0
    OnClick = STTestClick
  end
  object Performance: TButton
    Left = 264
    Top = 8
    Width = 75
    Height = 25
    Caption = 'Performance'
    TabOrder = 3
    OnClick = PerformanceClick
  end
  object Memo1: TMemo
    Left = 14
    Top = 39
    Width = 325
    Height = 194
    Lines.Strings = (
      'Memo1')
    TabOrder = 4
  end
end
