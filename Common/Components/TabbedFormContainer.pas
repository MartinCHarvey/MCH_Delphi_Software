unit TabbedFormContainer;

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

