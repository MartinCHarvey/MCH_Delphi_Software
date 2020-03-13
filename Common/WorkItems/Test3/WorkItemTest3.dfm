object Form1: TForm1
  Left = 0
  Top = 0
  Caption = 'Form1'
  ClientHeight = 242
  ClientWidth = 527
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  PixelsPerInch = 96
  TextHeight = 13
  object TestNumaBtn: TButton
    Left = 144
    Top = 48
    Width = 75
    Height = 25
    Caption = 'TestNumaBtn'
    TabOrder = 0
    OnClick = TestNumaBtnClick
  end
  object TestAllocBtn: TButton
    Left = 264
    Top = 48
    Width = 75
    Height = 25
    Caption = 'TestAllocBtn'
    TabOrder = 1
    OnClick = TestAllocBtnClick
  end
end
