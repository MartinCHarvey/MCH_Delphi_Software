unit PageMapperFrm;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.ComCtrls, Vcl.ExtCtrls, Vcl.StdCtrls, FetcherParser;

type
  TPageMapperForm = class(TForm)
    UrlEdit: TEdit;
    LoadBtn: TButton;
    ScriptCheck: TCheckBox;
    Panel1: TPanel;
    ErrMemo: TMemo;
    TreeView: TTreeView;
    ListBox: TListBox;
    Splitter1: TSplitter;
    Splitter2: TSplitter;
    procedure LoadBtnClick(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure ListBoxDblClick(Sender: TObject);
  private
    { Private declarations }
    FPResult: TFetchParseResult;
    procedure HandleFetchParsesAvailable(Sender: TObject);
    procedure PopulateErrMemo;
    procedure PopulateTreeView;
    procedure PopulateScriptsList;
  public
    { Public declarations }
  end;

var
  PageMapperForm: TPageMapperForm;

implementation

{$R *.dfm}

uses HTTPDocFetcher, HTMLParseEvents, CommonNodes, HTMLNodes,
  DLList;

procedure TPageMapperForm.FormCreate(Sender: TObject);
begin
  GFetcherParser.OnFetchParsesCompleted := HandleFetchParsesAvailable;
end;

procedure TPageMapperForm.ListBoxDblClick(Sender: TObject);
var
  i: integer;
  Node: TCommonNode;
  TreeItem: TTreeNode;
begin
//  if ListBox.SelCount = 1 then
//  begin
    for i := 0 to Pred(ListBox.Items.Count) do
    begin
      if ListBox.Selected[i] then
      begin
        Node := ListBox.Items.Objects[i] as TCommonNode;
        TreeItem := Node.Obj1 as TTreeNode;
        TreeItem.MakeVisible;
        TreeItem.Selected := true;
        TreeItem.Focused := true;
        TreeView.SetFocus;
        exit;
      end;
    end;
//  end;
end;

procedure TPageMapperForm.LoadBtnClick(Sender: TObject);
var
  FPM: TFetchParseMethod;
begin
  if ScriptCheck.Checked then
    FPM := fpmGetWithScripts
  else
    FPM := fpmGet;
  GFetcherParser.AddDocToFetchParse(UrlEdit.Text, nil, FPM, nil, nil, nil);
end;

procedure TPageMapperForm.PopulateErrMemo;
var
  i: integer;
  E: TParseEvent;
begin
  for i := 0 to Pred(FPResult.ParseEventList.Count) do
  begin
    E := TParseEvent(FPResult.ParseEventList.Items[i]);
    if E.EvType < pwtWarning then
      ErrMemo.Lines.Add(E.Language + ' Line ' + IntToStr(E.Line) +
                      ' Col ' + IntToStr(E.Col) +
                      ' (' + IntToStr(E.Code) +
                      ') ' + 'Info:' + E.StrData)
    else if E.EvType < petError then
      ErrMemo.Lines.Add(E.Language + ' Line ' + IntToStr(E.Line) +
                      ' Col ' + IntToStr(E.Col) +
                      ' (' + IntToStr(E.Code) +
                      ') ' + 'Warning: ' + E.StrData)
    else
      ErrMemo.Lines.Add(E.Language + ' Line ' + IntToStr(E.Line) +
                      ' Col ' + IntToStr(E.Col) +
                      ' (' + IntToStr(E.Code) +
                      ') ' + 'Error: ' + E.StrData);
  end;
end;

procedure PopAllNodesExec(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
var
  TreeView: TTreeView;
  Item: TTreeNode;
begin
  TreeView := TTreeView(Ref1);
  if Assigned(Node.ContainerNode) and Assigned(Node.ContainerNode.Obj1) then
    Item := TreeView.Items.AddChildObject(Node.ContainerNode.Obj1 as TTreeNode, Node.AsText.Trim, Node)
  else
    Item := TreeView.Items.AddObject(nil, Node.AsText.Trim, Node);
  Node.Obj1 := Item;
end;

function PopAllNodesDecision(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
var
  res2: boolean;
begin
  result := (Node.NodeType <> NodeTypeHTML) or
            (not (Node is THTMLBlock)) or
    Assigned((Node as THTMLBlock).Tag) or
    (not DlItemIsEmpty(@Node.ContainedListHead)) or
    (Length((Node as THTMLBlock).AsText.Trim) > 0);
end;

function PopAllNodesRecurse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
begin
  result := true;
end;

procedure PopScriptLinksExec(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
var
  ListBox: TListBox;
  ST: THTMLScriptTag;
begin
  ListBox := TListBox(Ref1);
  ST := (Node as THTMLBlock).Tag as THTMLScriptTag;
  ListBox.AddItem(ST.AsText, Node);
end;

function PopScriptLinksDecision(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
var
  res2: boolean;
begin
  result := (Node is THTMLBlock) and Assigned((Node as THTMLBlock).Tag)
    and ((Node as THTMLBlock).Tag is THTMLScriptTag);
end;

function PopScriptLinksRecurse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
begin
  result := Node.NodeType = NodeTypeHTML;
end;

procedure TPageMapperForm.PopulateTreeView;
begin
  if Assigned(FPResult.ParseResult) then
  begin
    FPResult.ParseResult.IterateNodes(PopAllNodesDecision, PopAllNodesExec, PopAllNodesRecurse, TreeView, nil, nil);
  end;
end;

procedure TPageMapperForm.PopulateScriptsList;
begin
  if Assigned(FPResult.ParseResult) then
  begin
    FPResult.ParseResult.IterateNodes(PopScriptLinksDecision, PopScriptLinksExec, PopScriptLinksRecurse, ListBox, nil, nil);
  end;
end;

procedure TPageMapperForm.FormDestroy(Sender: TObject);
begin
  FPResult.Free;
end;

procedure TPageMapperForm.HandleFetchParsesAvailable(Sender: TObject);
begin
  ErrMemo.Clear;
  ListBox.Clear;
  TreeView.Items.Clear;

  FPResult.Free;
  FPResult := GFetcherParser.GetCompletedFetchParse;
  PopulateErrMemo;
  PopulateTreeView;
  PopulateScriptsList;
end;

end.
