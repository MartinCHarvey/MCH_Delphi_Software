object WorkItemTest2Form: TWorkItemTest2Form
  Left = 0
  Top = 0
  Caption = 'WorkItemTest2Form'
  ClientHeight = 103
  ClientWidth = 482
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
  object LowEdit: TEdit
    Left = 16
    Top = 8
    Width = 121
    Height = 21
    TabOrder = 0
    Text = 'LowEdit'
  end
  object HighEdit: TEdit
    Left = 152
    Top = 8
    Width = 121
    Height = 21
    TabOrder = 1
    Text = 'HighEdit'
  end
  object CalcBtn: TButton
    Left = 288
    Top = 8
    Width = 75
    Height = 25
    Caption = 'CalcBtn'
    TabOrder = 2
    OnClick = CalcBtnClick
  end
  object PrimeEdit: TEdit
    Left = 16
    Top = 40
    Width = 121
    Height = 21
    TabOrder = 3
    Text = 'PrimeEdit'
  end
  object ResultEdit: TEdit
    Left = 152
    Top = 40
    Width = 121
    Height = 21
    TabOrder = 4
    Text = 'ResultEdit'
  end
  object CheckBtn: TButton
    Left = 288
    Top = 40
    Width = 75
    Height = 25
    Caption = 'CheckBtn'
    TabOrder = 5
    OnClick = CheckBtnClick
  end
  object StatusBar: TStatusBar
    Left = 0
    Top = 84
    Width = 482
    Height = 19
    Panels = <>
    SimplePanel = True
  end
  object ResetBtn: TButton
    Left = 384
    Top = 40
    Width = 75
    Height = 25
    Caption = 'ResetBtn'
    TabOrder = 7
    OnClick = ResetBtnClick
  end
end
