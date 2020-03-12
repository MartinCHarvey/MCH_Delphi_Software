unit PrefsEditForm;
{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, FMX.Edit;

type
  TPrefsEditFrm = class(TForm)
    Label1: TLabel;
    Label2: TLabel;
    Label3: TLabel;
    ExpireMediaChk: TCheckBox;
    ExpireCommentsChk: TCheckBox;
    LoadEdit: TEdit;
    ExpireDataEdit: TEdit;
    Label4: TLabel;
    Label5: TLabel;
    ExpireImagesEdit: TEdit;
    Label6: TLabel;
    Label7: TLabel;
    DataDirEdit: TEdit;
    OkBtn: TButton;
    CancelBtn: TButton;
    procedure FormShow(Sender: TObject);
    procedure OkBtnClick(Sender: TObject);
    procedure CancelBtnClick(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  PrefsEditFrm: TPrefsEditFrm;

implementation

uses
  UserPrefs;

const
  S_WARNING = 'If you change the data root directory, you will need to' +
    ' manually copy files across, and restart the app';
  S_WARNING2 = 'Directory does not exist, are you sure?';
  S_VALUE_POSITIVE = 'Value must be >= 0';

{$R *.fmx}

procedure TPrefsEditFrm.CancelBtnClick(Sender: TObject);
begin
  Close;
end;

procedure TPrefsEditFrm.FormShow(Sender: TObject);
begin
  ExpireMediaChk.IsChecked := CurSessUserPrefs.ExpireMedia;
  ExpireCommentsChk.IsChecked := CurSessUserPrefs.ExpireComments;
  ExpireDataEdit.Text := IntToStr(CurSessUserPrefs.DataExpireDays);
  ExpireImagesEdit.Text := IntToStr(CurSessUserPrefs.ImageExpireDays);
  LoadEdit.Text := InttoStr(CurSessUserPrefs.LoadBackDays);
  DataDirEdit.Text := CurSessUserPrefs.DataRootDir;
end;

procedure TPrefsEditFrm.OkBtnClick(Sender: TObject);
var
  tmp: integer;
begin
  try
    if CurSessUserPrefs.DataRootDir <> DataDirEdit.Text then
    begin
      if MessageDlg(S_WARNING, TMSgDlgType.mtWarning, [TMsgDlgBtn.mbOK, TMsgDlgBtn.mbCancel], 0)
        <> mrOK then
        exit;
      if not DirectoryExists(DataDirEdit.Text) then
        if MessageDlg(S_WARNING2, TMSgDlgType.mtWarning, [TMsgDlgBtn.mbOK, TMsgDlgBtn.mbCancel], 0)
          <> mrOK then
          exit;
    end;
    CurSessUserPrefs.ExpireMedia := ExpireMediaChk.IsChecked;
    CurSessUserPrefs.ExpireComments := ExpireCommentsChk.IsChecked;

    tmp := StrToInt(ExpireDataEdit.Text);
    if tmp >= 0 then
      CurSessUserPrefs.DataExpireDays := tmp
    else
      raise EConvertError.Create(S_VALUE_POSITIVE);

    tmp := StrToInt(ExpireImagesEdit.Text);
    if tmp >= 0 then
      CurSessUserPrefs.ImageExpireDays := tmp
    else
      raise EConvertError.Create(S_VALUE_POSITIVE);

    tmp := StrToInt(LoadEdit.Text);
    if tmp >= 0 then
      CurSessUserPrefs.LoadBackDays := tmp
    else
      raise EConvertError.Create(S_VALUE_POSITIVE);

    CurSessUserPrefs.DataRootDir := DataDirEdit.Text;
  except
    on EConvertError do
    begin
      FormShow(self);
      raise;
    end;
  end;
  Close;
end;

end.
