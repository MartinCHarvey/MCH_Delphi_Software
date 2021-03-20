object DragHelperForm: TDragHelperForm
  Left = 41
  Top = 703
  BorderIcons = []
  BorderStyle = bsToolWindow
  Caption = 'Move / Zoom / Rotate'
  ClientHeight = 107
  ClientWidth = 129
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -11
  Font.Name = 'MS Sans Serif'
  Font.Style = []
  OldCreateOrder = False
  OnShow = FormShow
  PixelsPerInch = 96
  TextHeight = 13
  object PB: TPaintBox
    Left = 0
    Top = 0
    Width = 129
    Height = 107
    Align = alClient
    OnPaint = PBPaint
  end
end
