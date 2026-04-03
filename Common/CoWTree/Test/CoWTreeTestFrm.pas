unit CoWTreeTestFrm;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls, IndexedStore, CoWABTree, CoWTree,
  CoWSimpleTree;

type
  //Cpl of hacky classes to get hold of index nodes and check balance.
  TIndexedStoreHack = class;

  TCoWTreeTestForm = class(TForm)
    BasicInsertBtn: TButton;
    BasicDeleteBtn: TButton;
    STTest: TButton;
    procedure BasicInsertBtnClick(Sender: TObject);
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
    procedure BasicDeleteBtnClick(Sender: TObject);
    procedure STTestClick(Sender: TObject);
  private
    { Private declarations }
    FStore: TIndexedStoreHack;
    FOriginalAdditionStore: TIndexedStoreHack;
    FCowTree: TCowABTree;
    procedure CheckIndexesSame(Sel: TCowSel; Store: TIndexedStoreHack);
    procedure Reset;
    procedure Report;
  public
    { Public declarations }
    OK: boolean;
  end;

  TIndexedStoreHack = class(TIndexedStoreO)
  public
    function IndexByTag(Tag: Pointer): TSIndex;
  end;

var
  CoWTreeTestForm: TCoWTreeTestForm;

implementation

{$R *.dfm}

// OK, general strategy here is that we have a perfectly good existing
// Indexed store which does AVL OK. Just perform the same manipulation
// on that, and on new CoWTree datastructures, and make sure we always get
// the right answer.
//
// Rely on debug tracking to check referencing etc.

const
  SMALL_ITEMCOUNT = 1000;

  //Couple of hacky helper classes to get at the index nodes, and check
  //balance factors as well.

type
  TItemRecHack = class(TItemRec)
  public
    function GetINodeByRoot(Root: TSIndex): TIndexNode;
  end;

  TIndexLinkHack = class(TIndexNodeLink)
  public
    function GetIndexNode: TIndexNode;
  end;

  TIndexNodeHack = class(TIndexNode)
  public
    function GetBal: integer;
  end;

  TTrivialDataItem = class
  public
    FMainIRec: TItemRecHack;
    FOriginalAddIRec: TItemRecHack;
    Key: integer;
    function Bal(IRec: TItemRecHack; Store: TIndexedStoreHack): integer;
  end;

  TTrivialIndexNode = class(TIndexNode)
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  end;

  TTrivialSearchVal = class(TTrivialIndexNode)
  public
    SearchKey: integer;
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  end;

  TCowTestItem = class(TCowTreeItem)
    //Really trivial, store the keys in the nodes at the moment.
  protected
    function Compare(Other: TCoWTreeItem;
                     AllowKeyDedupe: boolean): integer; override;
    //CopyFrom copies reffed children (providing the keys).
    procedure CopyFrom(Source: TCoWTreeItem); override;
  public
    Key: integer;
    function GetBal: integer;
  end;

{ Functions }

procedure Chk(Val: boolean);
begin
  if not Val then
  begin
    Assert(false);
    CoWTreeTestForm.OK := false;
  end;
end;


{ TIndexedStoreHack }

function TIndexedStoreHack.IndexByTag(Tag: Pointer): TSIndex;
begin
  result := self.GetIndexByTag(TTagType(Tag));
  Chk(Assigned(result));
end;

{ TItemRecHack }

function TItemRecHack.GetINodeByRoot(Root: TSIndex): TIndexNode;
var
  ILink: TIndexNodeLink;
  ILHack: TIndexLinkHack;
begin
  ILink := self.GetIndexLinkByRoot(Root, false); //No locking here.
  Chk(Assigned(ILink));
  ILHack := TIndexLinkHack(ILink);
  result := ILHack.GetIndexNode;
end;

{ TIndexLinkHack }

function TIndexLinkHack.GetIndexNode: TIndexNode;
begin
  result := FIndexNode;
end;

{ TIndexNodeHack }

function TIndexNodeHack.GetBal: integer;
begin
  result := Bal;
end;


{ TTrivialDataItem }

function TTrivialDataItem.Bal(IRec: TItemRecHack; Store: TIndexedStoreHack): integer;
var
  IRecHack: TItemRecHack;
  INode: TIndexNode;
  INodeHack: TIndexNodeHack;
  IdxRoot: TSIndex;
begin
  IRecHack := TItemRecHack(IRec);
  IdxRoot := Store.IndexByTag(Pointer(1));
  INode := IRecHack.GetINodeByRoot(IdxRoot);
  INodeHack := TIndexNodeHack(INode);
  result := INodeHack.bal;
end;

{ TTrivialIndexNode }

function TTrivialIndexNode.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  OwnData, OtherData: TTrivialDataItem;
begin
  OwnData := OwnItem as TTrivialDataItem;
  OtherData := OtherItem as TTrivialDataItem;
  if OtherData.Key > OwnData.Key then
    result := 1
  else if OtherData.Key < OwnData.Key then
    result := -1
  else
    result := 0;
end;

function TTrivialSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  OtherData: TTrivialDataItem;
begin
  OtherData := OtherItem as TTrivialDataItem;
  if OtherData.Key > SearchKey then
    result := 1
  else if OtherData.Key < SearchKey then
    result := -1
  else
    result := 0;
end;

{ TCowTestItem }

function TCowTestItem.GetBal: integer;
begin
  result := self.Bal;
end;

function TCowTestItem.Compare(Other: TCoWTreeItem;
                 AllowKeyDedupe: boolean): integer;
var
  OtherCow: TCowTestItem;
begin
  OtherCow := Other as TCowTestItem;
  if OtherCow.Key > Key then
    result := 1
  else if OtherCow.Key < Key then
    result := -1
  else
    result := 0;
end;

procedure TCowTestItem.CopyFrom(Source: TCoWTreeItem);
begin
  self.Key := (Source as TCowTestItem).Key;
  inherited;
end;

{ TCowTreeTestForm}

procedure TCowTreeTestForm.Report;
begin
  if OK then
    ShowMessage('All OK.')
  else
    ShowMessage('Something broken');
end;

procedure TCowTreeTestForm.Reset;
var
  IRec: TItemRec;
  Item: TObject;
begin
  IRec := FStore.GetAnItem;
  while Assigned(IRec) do
  begin
    FStore.RemoveItem(IRec);
    IRec := FStore.GetAnItem;
  end;
  //OriginalAdditionStore has all items allocated.
  IRec := FOriginalAdditionStore.GetAnItem;
  while Assigned(IRec) do
  begin
    Item := IRec.Item;
    FOriginalAdditionStore.RemoveItem(IRec);
    Item.Free;
    IRec := FOriginalAdditionStore.GetAnItem;
  end;
  FCowTree.Clear(abMain);
  FCowTree.Clear(abNext);
  OK := true;
end;

procedure TCoWTreeTestForm.STTestClick(Sender: TObject);
var
  i: integer;
  CTI: TCowTestItem;
  CowSimple: TCowSimpleTree;

begin
  OK := true;
  CowSimple := TCowSimpleTree.Create;
  try
    for i := 0 to Pred(SMALL_ITEMCOUNT) do
    begin
      CTI := TCowTestItem.Create;
      CTI.Key := i;
      Chk(CowSimple.Add(CTI));
    end;
    CTI := TCowTestItem.Create;
    for i := 0 to Pred(SMALL_ITEMCOUNT) do
    begin
      CTI.Key := i;
      Chk(CowSimple.Remove(CTI));
    end;
  finally
    CTI.Release;
    CowSimple.Release;
  end;
  Report;
end;

procedure TCoWTreeTestForm.CheckIndexesSame(Sel: TCowSel; Store: TIndexedStoreHack);
var
  IRec: TItemRec;
  TestItem: TCowTestItem;
  TTDat: TTrivialDataItem;
  a,b: integer;
begin
  Store.FirstByIndex(TObject(1), IRec);
  TestItem := FCowTree.First(Sel) as TCowTestItem;
  Chk(Assigned(IRec) = Assigned(TestItem));
  while Assigned(IRec) do
  begin
    TTDat := Irec.Item as TTrivialDataItem;
    Chk(Assigned(TTDat));
    //Check keys.
    Chk(TTDat.Key = TestItem.Key);
    //Check balance factors.
    a := TTDat.Bal(TItemRecHack(IRec), TIndexedStoreHack(Store));
    b := TestItem.bal;
    Chk(a = b);

    Store.NextByIndex(TObject(1), IRec);
    TestItem := FCowTree.NeighbourNode(TestItem, Sel, false) as TCowTestItem;
    Chk(Assigned(IRec) = Assigned(TestItem));
  end;
end;

procedure TCoWTreeTestForm.BasicDeleteBtnClick(Sender: TObject);
var
  ToDelete, i, DelIndex: integer;
  RV: TISRetVal;
  IRec: TItemRec;
  CowItem: TCowTestItem;
  RdSel: TCowSel;
  Key: integer;
  Broken: boolean;
begin
  //Populate all three lists so they are the same.
  BasicInsertBtnClick(sender);
  CheckIndexesSame(abMain, FStore);
  ShowMessage('Deletion of half via multiple traversals.');
  ToDelete := FStore.Count div 2;
  RdSel := abMain;
  while ToDelete > 0 do
  begin
    DelIndex := Random(FStore.Count);
    RV := FStore.FirstByIndex(Pointer(1), IRec);
    Chk(RV = RVOK);
    Chk(Assigned(IRec));
    CowItem := FCowTree.First(RdSel) as TCowTestItem;
    Chk(Assigned(CowItem));
    Chk(CowItem.Key = (Irec.Item as TTrivialDataItem).Key);
    for i := 0 to Pred(DelIndex) do
    begin
      RV := FStore.NextByIndex(Pointer(1), IRec);
      Chk(RV = rvOK);
      Chk(Assigned(IRec));
      CowItem := FCowTree.NeighbourNode(CowItem, RdSel, false) as TCowTestItem;
      Chk(Assigned(CowItem));
      Chk(CowItem.Key = (Irec.Item as TTrivialDataItem).Key);
    end;
    //Now delete.
    RV := FStore.RemoveItem(IRec);
    Chk(RV = RVOK);
    FCowTree.Remove(CowItem, RdSel, TCowSel.abNext);
    RdSel := abNext;
    CheckIndexesSame(abNext, FStore);
    Dec(ToDelete);
  end;
  CheckIndexessame(abMain, FOriginalAdditionStore);
  CheckIndexesSame(abNext, FStore);
  //OK, and now delete the remainder by key
  ShowMessage('Deletion of remainder by key.');
  CowItem := TCowTestItem.Create;
  try
    while (FStore.Count > 0) do
    begin
      IRec := FStore.GetAnItem;
      Chk(Assigned(IRec));
      CowItem.Key := (Irec.Item as TTrivialDataItem).Key;
      Chk(FStore.RemoveItem(IRec) = rvOK);
      Chk(FCowTree.Remove(CowItem, RdSel, abNext));
      CheckIndexesSame(abNext, FStore);
      Chk(not FCowTree.Remove(CowItem, RdSel, abNext));
      CheckIndexesSame(abNext, FStore);
    end;
    Chk(not Assigned(FCowTree.RootItem(TCowSel.abNext)));
  finally
    CowItem.Release;
  end;
  Report;
end;

procedure TCoWTreeTestForm.BasicInsertBtnClick(Sender: TObject);
var
  i: integer;
  NewKey: integer;
  NewItem: TTrivialDataItem;
  SV: TTrivialSearchVal;
  RV: TIsRetVal;
  IRec: TItemRec;
  CTI, SillyItem: TCowTestItem;
begin
  Reset;
  CheckIndexesSame(abMain, FOriginalAdditionStore);
  CheckIndexessame(abMain, FStore);
  SV := TTrivialSearchVal.Create;
  SillyItem := TCowTestItem.Create;
  try
    for i := 0 to Pred(SMALL_ITEMCOUNT) do
    begin
      NewKey := Random(High(NewKey));
      //Just check we're not inseting duplicates into the tree.
      //We'll worry about key/item de-dup in a bit.
      SV.SearchKey := NewKey;
      RV := FOriginalAdditionStore.FindByIndex(TObject(1), SV, IRec);
      Chk(RV in [rvOK, rvNotFound]);
      if RV = rvNotFound then
      begin
        NewItem := TTrivialDataItem.Create;
        NewItem.Key := NewKey;
        RV := FOriginalAdditionStore.AddItem(NewItem, IRec);
        Chk(RV = rvOK);
        RV := FStore.AddItem(NewItem, IRec);
        Chk(RV = rvOK);
        CTI := TCowTestItem.Create;
        CTI.Key := NewKey;
        SillyItem.Key := NewKey;
        Chk(FCowTree.Add(CTI, abMain, abMain));
        CheckIndexesSame(abMain, FStore);
        //Now try dup insert.
        Chk(not FCowTree.Add(SillyItem, abMain, abMain));
        CheckIndexesSame(abMain, FStore);
      end;
    end;
  finally
    SV.Free;
    SillyItem.Release;
  end;
  CheckIndexesSame(abMain, FOriginalAdditionStore);
  Report;
end;

procedure TCoWTreeTestForm.FormCreate(Sender: TObject);
begin
  FStore := TIndexedStoreHack.Create;
  FStore.AddIndex(TTrivialIndexNode, TObject(1), false);
  FOriginalAdditionStore := TIndexedStoreHack.Create;
  FOriginalAdditionStore.AddIndex(TTrivialIndexNode, TObject(1), false);
  FCowTree := TCowABTree.Create;
end;

procedure TCoWTreeTestForm.FormDestroy(Sender: TObject);
begin
  Reset;
  FOriginalAdditionStore.Free;
  FStore.Free;
  FCowTree.Release;
end;

initialization
  Randomize;
end.
