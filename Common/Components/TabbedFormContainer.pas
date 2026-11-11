unit TabbedFormContainer;
{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, Forms, Dialogs,
  ComCtrls;

type
  TTabbedFormContainer = class(TPageControl)
  private
    { Private declarations }
    FDesiredWidth, FDesiredHeight:integer;
    FFormList: TList;
  protected
    { Protected declarations }
    function GetForm(Idx:Integer):TForm;
    function GetFormCount: integer;
  public
    { Public declarations }
    function AddTabWithForm(FormClass: TFormClass; TabText: string):TForm;
    property DesiredWidth:integer read FDesiredWidth;
    property DesiredHeight:integer read FDesiredHeight;
    procedure ClearAllTabs;
    constructor Create(AOwner: TComponent);override;
    destructor Destroy; override;
    property Forms[Idx:integer]: TForm read GetForm;
    property FormCount:integer read GetFormCount;
  published
    { Published declarations }
  end;

procedure Register;

implementation

constructor TTabbedFormContainer.Create(AOwner: TComponent);
begin
  inherited;
  FFormList := TList.Create;
end;

destructor TTabbedFormContainer.Destroy;
begin
  FFormList.Free;
  inherited;
end;

function TTabbedFormContainer.GetForm(Idx:Integer): TForm;
begin
  result := TObject(FFormList.Items[Idx]) as TForm;
end;

function TTabbedFormContainer.GetFormCount: integer;
begin
  result := FFormList.Count;
end;

function TTabbedFormContainer.AddTabWithForm(FormClass: TFormClass; TabText:string):TForm;
var
  NewSheet:TTabSheet;
begin
  result := nil;
  NewSheet := nil;
  try
    NewSheet := TTabSheet.Create(self);
    with NewSheet do
    begin
      Parent := self;
      PageControl := self;
      Caption := TabText;
    end;
    result := FormClass.Create(NewSheet);
    if result.Width > DesiredWidth then
      FDesiredWidth := result.Width;
    if result.Height > DesiredHeight then
      FDesiredHeight := result.Height;
    with result do
    begin
      Parent := NewSheet;
      BorderIcons := [];
      BorderStyle := bsNone;
      Align := alClient;
      Show;
    end;
    FFormList.Add(result);
  except
    on Exception do
    begin
      NewSheet.Free;
      result.Free;
      raise;
    end;
  end;
end;

procedure TTabbedFormContainer.ClearAllTabs;
begin
  while PageCount > 0 do
    Pages[0].Free;
  FFormList.Clear;
end;

procedure Register;
begin
  RegisterComponents('MCH', [TTabbedFormContainer]);
end;

end.

