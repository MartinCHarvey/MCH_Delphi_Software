object RWWLockTestForm: TRWWLockTestForm
  Left = 0
  Top = 0
  Caption = 'RWWLockTestForm'
  ClientHeight = 242
  ClientWidth = 527
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'Tahoma'
  Font.Style = []
  OldCreateOrder = False
  OnCreate = FormCreate
  PixelsPerInch = 96
  TextHeight = 13
  object SimpleBtn: TButton
    Left = 89
    Top = 8
    Width = 75
    Height = 25
    Caption = 'SimpleBtn'
    TabOrder = 0
    OnClick = SimpleBtnClick
  end
  object ContendBtn: TButton
    Left = 170
    Top = 8
    Width = 75
    Height = 25
    Caption = 'ContendBtn'
    TabOrder = 1
    OnClick = ContendBtnClick
  end
  object Memo1: TMemo
    Left = 0
    Top = 56
    Width = 527
    Height = 186
    Align = alBottom
    Lines.Strings = (
      'Memo1')
    TabOrder = 2
  end
  object TimingsBtn: TButton
    Left = 8
    Top = 8
    Width = 75
    Height = 25
    Caption = 'TimingsBtn'
    TabOrder = 3
    OnClick = TimingsBtnClick
  end
end
