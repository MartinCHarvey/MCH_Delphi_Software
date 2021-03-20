object FractAboutDlg: TFractAboutDlg
  Left = 245
  Top = 108
  BorderStyle = bsDialog
  Caption = 'MFract'
  ClientHeight = 87
  ClientWidth = 313
  Color = clBtnFace
  ParentFont = True
  OldCreateOrder = True
  Position = poScreenCenter
  OnShow = FormShow
  PixelsPerInch = 96
  TextHeight = 13
  object Bevel1: TBevel
    Left = 8
    Top = 8
    Width = 297
    Height = 73
    Shape = bsFrame
  end
  object Label1: TLabel
    Left = 48
    Top = 24
    Width = 74
    Height = 13
    Caption = 'MFract version '
  end
  object VerLbl: TLabel
    Left = 128
    Top = 24
    Width = 99
    Height = 13
    Caption = '0.0.0.0 (32 bit build)'
  end
  object Label2: TLabel
    Left = 48
    Top = 48
    Width = 222
    Height = 13
    Caption = 'Martin Harvey  martin_c_harvey@hotmail.com'
  end
end
