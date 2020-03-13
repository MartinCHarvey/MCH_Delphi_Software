object WorkItemTestForm: TWorkItemTestForm
  Left = 192
  Top = 114
  Caption = 'WorkItemTestForm'
  ClientHeight = 39
  ClientWidth = 279
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'MS Sans Serif'
  Font.Style = []
  OldCreateOrder = False
  PixelsPerInch = 96
  TextHeight = 13
  object CreateButton: TButton
    Left = 16
    Top = 8
    Width = 75
    Height = 25
    Caption = 'Creation'
    TabOrder = 0
    OnClick = CreateButtonClick
  end
  object NFlowBtn: TButton
    Left = 104
    Top = 8
    Width = 75
    Height = 25
    Caption = 'Normal Flow'
    TabOrder = 1
    OnClick = NFlowBtnClick
  end
  object CFlowBtn: TButton
    Left = 192
    Top = 8
    Width = 81
    Height = 25
    Caption = 'Start && Cancel'
    TabOrder = 2
    OnClick = CFlowBtnClick
  end
end
