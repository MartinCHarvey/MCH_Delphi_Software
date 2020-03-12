unit AddNewUserForm;
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
  TAddNewUserFrm = class(TForm)
    Label1: TLabel;
    UsernameEdit: TEdit;
    GroupBox1: TGroupBox;
    InstaRadio: TRadioButton;
    TwitterRadio: TRadioButton;
    ScanBtn: TButton;
    CloseBtn: TButton;
    procedure CloseBtnClick(Sender: TObject);
    procedure ScanBtnClick(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  AddNewUserFrm: TAddNewUserFrm;

implementation

{$R *.fmx}

uses
  DataObjects, DBContainer, UserPrefs;

const
  S_UNABLE_TO_SCAN = 'Unable to scan for new user, blank?';

procedure TAddNewUserFrm.CloseBtnClick(Sender: TObject);
begin
  Close;
end;

procedure TAddNewUserFrm.ScanBtnClick(Sender: TObject);
var
  SiteType: TKSiteType;
begin
  if InstaRadio.IsChecked then
    SiteType := tstInstagram
  else if TwitterRadio.IsChecked then
    SiteType := tstTwitter
  else
  begin
    Assert(false);
    exit;
  end;
  if not DBCont.BatchLoader.ScanForNewUser(UsernameEdit.Text,
                                    SiteType,
                                    CurSessUserPrefs.LoadSince) then
    MessageDlg(S_UNABLE_TO_SCAN,
               TMsgDlgType.mtError,
               [TMsgDlgBtn.mbOK], 0, TMsgDlgBtn.mbOK);
  Close;
end;

end.
