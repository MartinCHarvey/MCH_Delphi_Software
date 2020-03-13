object Form1: TForm1
  Left = 0
  Top = 0
  Caption = 'Form1'
  ClientHeight = 501
  ClientWidth = 832
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  PixelsPerInch = 96
  TextHeight = 13
  object TestBtn: TButton
    Left = 32
    Top = 16
    Width = 75
    Height = 25
    Caption = 'Test'
    TabOrder = 0
    OnClick = TestBtnClick
  end
  object InputTextEdit: TEdit
    Left = 129
    Top = 18
    Width = 337
    Height = 21
    TabOrder = 1
    Text = 'This is some sample text'
  end
  object TokenLiteralsMemo: TMemo
    Left = 32
    Top = 64
    Width = 209
    Height = 153
    Lines.Strings = (
      'T'
      'Th'
      'i'
      's'
      'some'
      'ex')
    TabOrder = 2
  end
  object ResultsMemo: TMemo
    Left = 247
    Top = 64
    Width = 570
    Height = 425
    Lines.Strings = (
      'ResultsMemo')
    TabOrder = 3
  end
end
