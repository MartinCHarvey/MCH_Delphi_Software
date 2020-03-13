unit MMapperFrm;

{ Copyright Martin Harvey December 2016. }

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, FMX.Edit, FMX.Layouts, FMX.Memo,
  DLThreadQueue, WorkItems, FMX.TabControl, FMX.TreeView, FetcherParser,
  CommonNodes, FMX.ListBox;

type
  TMapperForm = class(TForm)
    FetchBtn: TButton;
    UrlEdit: TEdit;
    Panel1: TPanel;
    TabControl1: TTabControl;
    TreeViewTab: TTabItem;
    MsgsTab: TTabItem;
    Memo1: TMemo;
    TreeView1: TTreeView;
    FindLbl: TLabel;
    SearchEdit: TEdit;
    FindBtn: TButton;
    JSFindCombo: TComboBox;
    StatementItem: TListBoxItem;
    ExpressionItem: TListBoxItem;
    ParseFileEdit: TEdit;
    BrowseBtn: TButton;
    ParseTypeCombo: TComboBox;
    ParseLbl: TLabel;
    ParseBtn: TButton;
    ListBoxItem1: TListBoxItem;
    ListBoxItem2: TListBoxItem;
    ListBoxItem3: TListBoxItem;
    ListBoxItem4: TListBoxItem;
    OpenDialog1: TOpenDialog;
    DumpBtn: TButton;
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure FetchBtnClick(Sender: TObject);
    procedure FindBtnClick(Sender: TObject);
    procedure BrowseBtnClick(Sender: TObject);
    procedure ParseBtnClick(Sender: TObject);
    procedure DumpBtnClick(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
  private
    { Private declarations }
    FNodeSearchFragment: TCommonNode;
    FParseTree: TCommonNode;
    FFoundFragment: TCommonNode;
    FSamplePostReferrer: string;
    FSampleOrigin: string;
    procedure PopulateGuiTree;
    procedure ClearGuiTree;

    procedure HandleFetchParsesAvailable(Sender: TObject);
    //Dump event list from HTML parses.
    procedure DumpEventList(Item: TFetchParseResult);
  public
    { Public declarations }
  end;

var
  MapperForm: TMapperForm;

implementation

{$R *.fmx}

uses JSNodes, HTMLNodes, HTTPDocFetcher, HTMLParseEvents,
     DLList, HTMLEscapeHelper, CommonPool, Trackables, GlobalLog, IOUtils;

type
  THTMLTreeViewItem = class(TTreeViewItem)
  private
    FHTMLNode: TCommonNode;
  public
    property HTMLNode: TCommonNode read FHTMLNode write FHTMLNode;
  end;

function SanitiseUrl(Url:string):string;
var
  idx: cardinal;
begin
  result := Url;
  for idx := 1 to Length(result) do
  begin
    if CharInSet(result[idx], ['\','/','"','''',':', '?']) then
      result[idx] := '_';
  end;
end;

procedure TMapperForm.FetchBtnClick(Sender: TObject);
begin
  GFetcherParser.AddDocToFetchParse(UrlEdit.Text, nil, fpmGet, nil, nil, nil);
end;


procedure TMapperForm.FindBtnClick(Sender: TObject);
var
  Ans: AnsiString;
  Stream: TMemoryStream;
begin
  if Length(SearchEdit.Text) > 0 then
  begin
    if Assigned(JSFindCombo.Selected) then
    begin
      Ans := SearchEdit.Text;
      Stream := TTrackedMemoryStream.Create;
      Stream.WriteData(@Ans[1], Length(Ans));
      if not GFetcherParser.AddStreamToParse(Stream, tdtJScript, TObject(@FNodeSearchFragment), nil) then
        Stream.Free;
    end;
  end
  else
  begin
    FNodeSearchFragment.Free;
    FNodeSearchFragment := nil;
    ClearGuiTree;
    PopulateGuiTree;
  end;
end;

function TreePopDecision(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
var
  res2: boolean;
begin
//  result := Node.NodeType = (Ref2 as TCommonNode).NodeType;
  result := true;
  res2 := (not (Node is THTMLBlock)) or
    Assigned((Node as THTMLBlock).Tag) or
    (not DlItemIsEmpty(@Node.ContainedListHead)) or
    (Length((Node as THTMLBlock).AsText.Trim) > 0);
  result := result and res2;
end;

procedure TreePopExec(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
var
  TreeView: TTreeView;
  Item: THTMLTreeViewItem;

begin
  TreeView := TTreeView(Ref1);
  Item := THTMLTreeViewItem.Create(TreeView);
  Item.HTMLNode := Node;
  Node.Obj1 := Item;
  Item.Text := Node.AsText.Trim;
  if Assigned(Node.ContainerNode) and Assigned(Node.ContainerNode.Obj1) then
    Item.Parent := Node.ContainerNode.Obj1 as THTMLTreeViewItem
end;

procedure TreePopClear(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
begin
  Node.Obj1.Free;
  Node.Obj1 := nil;
end;

function TreePopRecurse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
begin
  //result := Node.NodeType = (Ref2 as TCommonNode).NodeType;
  result := RecurseAlways(Node, Ref1, Ref2, Ref3);
end;

procedure TMapperForm.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  BeginUpdate;
end;

procedure TMapperForm.FormCreate(Sender: TObject);
begin
  GFetcherParser.OnFetchParsesCompleted := HandleFetchParsesAvailable;
  AppGlobalLog.OpenFileLog(TPath.GetTempPath() + 'MMapper.log');
  AppGlobalLog.Severities := AllSeverities;
end;

procedure TMapperForm.FormDestroy(Sender: TObject);
begin
  ClearGUITree;
  FParseTree.Free;
  FNodeSearchFragment.Free;
  AppGlobalLog.CloseFileLog;
end;

function BadRegexpDecision(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
begin
  result := (Node is TJSSimpleExpr)
    and ((Node as TJSSimpleExpr).SimpleExprType = setRegExp);
end;

procedure BadRegexpExec(Node: TCommonNode; Ref1, Ref2, Ref3: TObject);
var
  N: TJSString;
  Q, I: integer;

begin
  N := Node as TJSString;
  Q := 0;
  for I := 1 to Length(N.StrData) do
    if CharInSet(N.StrData[i], ['''', '"']) then
      Inc(Q);
  if (Q mod 2) <> 0 then
      (Ref1 as TMapperForm).Memo1.Lines.Add('Unmatched regexp, bad things will happen.');
end;

function BadRegexpRecurse(Node: TCommonNode; Ref1, Ref2, Ref3: TObject): boolean;
begin
  result := (Node.NodeType = NodeTypeHTML) or (Node.NodeType = NodeTypeJS);
end;

procedure TMapperForm.BrowseBtnClick(Sender: TObject);
begin
  if OpenDialog1.Execute then
    ParseFileEdit.Text := OpenDialog1.Files.Strings[0];
end;

procedure TMapperForm.ClearGuiTree;
begin
  Assert(Assigned(FFoundFragment) = (TreeView1.Count > 0) and Assigned(TreeView1.Items[0]));
  if Assigned(FFoundFragment) then
  begin
    BeginUpdate;
    try
      FFoundFragment.IterateNodes(TreePopDecision, TreePopClear, TreePopRecurse,
                                        TreeView1, FFoundFragment, nil, false);
    finally
      EndUpdate;
    end;
  end;
  FFoundFragment := nil;
end;

procedure TMapperForm.ParseBtnClick(Sender: TObject);
var
  FS: TFileStream;
begin
  if Assigned(ParseTypeCombo.Selected) and (Length(ParseFileEdit.Text) > 0) then
  begin
    FS := TTrackedFileStream.Create(ParseFileEdit.Text, fmOpenRead);
    GFetcherParser.AddStreamToParse(FS,
                                    THTMLDocType(ParseTypeCombo.Selected.Tag),
                                    nil, nil);
  end;
end;

procedure TMapperForm.PopulateGUITree;
var
  Bail: TNodeSearchBailout;
begin
  if Assigned(FParseTree) then
  begin
    if not Assigned(FNodeSearchFragment) then
      FFoundFragment := FParseTree
    else
    begin
      Bail := TNodeSearchBailout.Create;
      try
        BeginUpdate;
        FParseTree.IterateNodes(TreeMatchDecision, TreeMatchExecute, TreeMatchRecurse,
                                Bail, FNodeSearchFragment, nil);
        FFoundFragment := Bail.FoundNode;
      finally
        EndUpdate;
        Bail.Free;
      end;
    end;
  end
  else
    FFoundFragment := nil;

  if Assigned(FFoundFragment) then
  begin
    FFoundFragment.IterateNodes(TreePopDecision, TreePopExec, TreePopRecurse,
                                      TreeView1, FFoundFragment, nil);
    (FFoundFragment.Obj1 as THTMLTreeViewItem).Parent := TreeView1;
  end;
end;

procedure TMapperForm.HandleFetchParsesAvailable(Sender: TObject);
var
  FPResult: TFetchParseResult;
  TrimTo: TJSSearchType;
  Proto, Site, FullFile, Name, Ext: string;
begin
  FPResult := GFetcherParser.GetCompletedFetchParse;
  if not Assigned(FPResult) then
    exit;

  if not (FPResult.FPRef1 = TObject(@FNodeSearchFragment)) then
  begin
    //If we want to do subsequent post requests based on this.
    if not Assigned(FPResult.PassedInStream) then
    begin
      if ((FPResult.Method = fpmPost) or (FPResult.Method = fpmGet))
        and (Assigned(FPResult.FetcherOpts)) then
      begin
        FSamplePostReferrer := FPResult.FetcherOpts.Referer;
        FSampleOrigin := FPResult.FetcherOpts.RequestCustomOrigin;
      end
      else if ((FPResult.Method = fpmGet) or (FPResult.Method = fpmGetWithScripts))
        and (Length(FPResult.Url) > 0) then
      begin
        FSamplePostReferrer := FPResult.Url;
        ParseUrl(FPResult.Url, Proto, Site, FullFile, Name, Ext);
        FSampleOrigin := Proto + '://' + Site + '/';
      end
      else
        Assert(false);
    end;

    ClearGUITree;

    //Result is proper parse tree.
    FParseTree.Free;
    FParseTree := nil;

    Memo1.Lines.Clear;
    if FPResult.FetchResult <> difNone then
      Memo1.Lines.Add(FPResult.FetchString);

    if Assigned(FPResult.ParseResult) then
    begin
      FPResult.ParseResult.IterateNodes(BadRegexpDecision, BadRegexpExec, BadRegexpRecurse, self, nil, nil);
      FParseTree := FPResult.ParseResult;
      FPResult.ParseResult := nil;
    end
    else
      FParseTree := nil;
  end
  else
  begin
    //Result is tree fragment that we were supposed to be searching for.
    FNodeSearchFragment.Free;
    FNodeSearchFragment := nil;
    if Assigned(JSFindCombo.Selected) then
    begin
      if JSFindCombo.Selected =  StatementItem then
        TrimTo := jttStatement
      else if JSFindCombo.Selected = ExpressionItem then
        TrimTo := jttExpression
      else
      begin
        Assert(false);
        TrimTo := TJSSearchType(nil);
      end;
      if Assigned(FPResult.ParseResult) then
      begin
        FNodeSearchFragment := TrimSearchTree(FPResult.ParseResult as TJSNode, TrimTo);
        FPResult.ParseResult := nil;
      end
    end;
    ClearGUITree;
  end;
  PopulateGuiTree;
  DumpEventList(FPResult);
  FPResult.Free;
end;

procedure TMapperForm.DumpBtnClick(Sender: TObject);
var
  DumpString: string;
  DumpAnsi: AnsiString;
  TmpStream: TFileStream;
begin
  if Assigned(FParseTree) and (FParseTree is THTMLNode) then
    DumpString := (FParseTree as THTMLNode).ReconstructedHTML(trhNone);
  DumpAnsi := DumpString;
  TmpStream := TFileStream.Create(TPath.GetTempPath + 'MMapper_dump.txt', fmCreate);
  try
    TmpStream.Write(DumpAnsi[1], Length(DumpAnsi));
  finally
    TmpStream.Free;
  end;
end;

procedure TMapperForm.DumpEventList(Item: TFetchParseResult);
var
  i: integer;
  E: TParseEvent;
begin
  //TODO. Something useful with Item - read error list.
  for i := 0 to Pred(Item.ParseEventList.Count) do
  begin
    E := TParseEvent(Item.ParseEventList.Items[i]);
    if E.EvType < pwtWarning then
      Memo1.Lines.Add(E.Language + ' Line ' + IntToStr(E.Line) +
                      ' Col ' + IntToStr(E.Col) +
                      ' (' + IntToStr(E.Code) +
                      ') ' + 'Info:' + E.StrData)
    else if E.EvType < petError then
      Memo1.Lines.Add(E.Language + ' Line ' + IntToStr(E.Line) +
                      ' Col ' + IntToStr(E.Col) +
                      ' (' + IntToStr(E.Code) +
                      ') ' + 'Warning: ' + E.StrData)
    else
      Memo1.Lines.Add(E.Language + ' Line ' + IntToStr(E.Line) +
                      ' Col ' + IntToStr(E.Col) +
                      ' (' + IntToStr(E.Code) +
                      ') ' + 'Error: ' + E.StrData);
  end;
end;

end.
