unit ContainerItems;

{
  Copyright (c) Martin Harvey 2003.

  This file defines some extensions to the standard list and tree views
  to allow them to propagate actions into contained classes.
}

interface

uses
  Windows, Messages, SysUtils, Classes, Graphics, Controls, Forms, Dialogs,
  ComCtrls, ActnList;

type
  TContainedItem = class;

  TContItemAction = class(TAction)
  private
    FDetails:TObject;
  protected
    function GetItemFromTarget(Target: TObject): TContainedItem;
  public
    function HandlesTarget(Target: TObject): boolean; override;
    procedure ExecuteTarget(Target: TObject); override;
    procedure UpdateTarget(Target: TObject); override;
    destructor Destroy;override;

    property Details:TObject read FDetails write FDetails;
  published
  end;

  TContainedItem = class(TPersistent)
  private
  protected
  public
    function HandlesAction(Action: TContItemAction): boolean; virtual;
    procedure UpdateAction(Action: TContItemAction); virtual;
    procedure ExecuteAction(Action: TContItemAction); virtual;
  published
  end;

  TItemActionEvent = procedure(Sender: TObject; Action: TBasicAction; Item:
    TContainedItem; var PassToItem: boolean) of object;

  TItemHandleQuery = function(Sender: TObject; Action: TBasicAction; Item:
    TContainedItem): boolean of object;

  TContainerTreeView = class(TTreeView)
  private
    FOnContainedUpdate: TItemActionEvent;
    FOnContainedExecute: TItemActionEvent;
    FOnHandleQuery: TItemHandleQuery;
    FNodeChangingTo: TTreeNode;
  protected
    procedure MouseDown(Button: TMouseButton; Shift: TShiftState;
      X, Y: Integer); override;
    function HandleQuery(Action: TBasicAction): boolean; dynamic;
    procedure ContainedUpdate(Action: TBasicAction; Item: TContainedItem; var
      PassToItem: boolean);
    procedure ContainedExecute(Action: TBasicAction; Item: TContainedItem; var
      PassToItem: boolean);
  public
    function GetSelectedItem: TContainedItem;
  published
    property OnContainedUpdate: TItemActionEvent
      read FOnContainedUpdate write FOnContainedUpdate;
    property OnContainedExecute: TItemActionEvent
      read FOnContainedExecute write FOnContainedExecute;
    property OnHandleQuery: TItemHandleQuery
      read FOnHandleQuery write FOnHandleQuery;
  end;

  TContainerListView = class(TListView)
  private
    FOnContainedUpdate: TItemActionEvent;
    FOnContainedExecute: TItemActionEvent;
    FOnHandleQuery: TItemHandleQuery;
  protected
    function HandleQuery(Action: TBasicAction): boolean; dynamic;
    procedure ContainedUpdate(Action: TBasicAction; Item: TContainedItem; var
      PassToItem: boolean);
    procedure ContainedExecute(Action: TBasicAction; Item: TContainedItem; var
      PassToItem: boolean);
  public
    function GetSelectedItem: TContainedItem;
  published
    property OnContainedUpdate: TItemActionEvent
      read FOnContainedUpdate write FOnContainedUpdate;
    property OnContainedExecute: TItemActionEvent
      read FOnContainedExecute write FOnContainedExecute;
    property OnHandleQuery: TItemHandleQuery
      read FOnHandleQuery write FOnHandleQuery;
  end;

procedure Register;

implementation

procedure Register;
begin
  RegisterComponents('MCH', [TContainerTreeView, TContainerListView]);
  RegisterComponents('MCH', [TContItemAction]);
end;

(************************************
 * TContItemAction                  *
 ************************************)

function TContItemAction.GetItemFromTarget(Target: TObject): TContainedItem;
begin
  if Target is TContainerListView then
    result := TContainerListView(Target).GetSelectedItem
  else if Target is TContainerTreeView then
    result := TContainerTreeView(Target).GetSelectedItem
  else
    result := nil;
end;

function TContItemAction.HandlesTarget(Target: TObject): boolean;
var
  Item: TContainedItem;
  Ctrl: TWinControl;
begin
  result := false;
  if Assigned(Target) and (Target is TWinControl) then
  begin
    Ctrl := TWinControl(Target);
    if Ctrl.Focused then
    begin
      { Handled either if control can handle it, or item itself
        can handle it.
        Ask control if it can handle it because its ability to handle
        via a call or callback might be dependent on the item itself,
        and also the state of the form. }
      if (Ctrl is TContainerListView) and
        TContainerListView(Ctrl).HandleQuery(Self) then
        result := true
      else if (Ctrl is TContainerTreeView) and
        TContainerTreeView(Ctrl).HandleQuery(Self) then
        result := true
      else
      begin
        Item := GetItemFromTarget(Ctrl);
        if Assigned(Item) then
          result := Item.HandlesAction(Self);
      end;
    end;
  end;
end;

procedure TContItemAction.ExecuteTarget(Target: TObject);
var
  PassToItem: boolean;
  Item: TContainedItem;
begin
  PassToItem := true;
  if (Target is TContainerListView) then
  begin
    with TContainerListView(Target) do
    begin
      Item := GetSelectedItem;
      ContainedExecute(Self, Item, PassToItem);
      if PassToItem then
        Item.ExecuteAction(Self);
    end;
  end else if (Target is TContainerTreeView) then
  begin
    with TContainerTreeView(Target) do
    begin
      Item := GetSelectedItem;
      ContainedExecute(Self, Item, PassToItem);
      if PassToItem then
        Item.ExecuteAction(Self);
    end;
  end;
end;

procedure TContItemAction.UpdateTarget(Target: TObject);
var
  PassToItem: boolean;
  Item: TContainedItem;
begin
  PassToItem := true;
  if (Target is TContainerListView) then
  begin
    with TContainerListView(Target) do
    begin
      Item := GetSelectedItem;
      ContainedUpdate(Self, Item, PassToItem);
      if PassToItem then
        Item.UpdateAction(Self);
    end;
  end else if (Target is TContainerTreeView) then
  begin
    with TContainerTreeView(Target) do
    begin
      Item := GetSelectedItem;
      ContainedUpdate(Self, Item, PassToItem);
      if PassToItem then
        Item.UpdateAction(Self);
    end;
  end;
end;

destructor TContItemAction.Destroy;
begin
  FDetails.Free;
  inherited;
end;

(************************************
 * TContainedItem                   *
 ************************************)

function TContainedItem.HandlesAction(Action: TContItemAction): boolean;
begin
  result := false;
end;

procedure TContainedItem.UpdateAction(Action: TContItemAction);
begin
 //Empty for the moment.
end;

procedure TContainedItem.ExecuteAction(Action: TContItemAction);
begin
 //Empty for the moment.
end;

(************************************
 * TContainerTreeView               *
 ************************************)

procedure TContainerTreeView.MouseDown(Button: TMouseButton; Shift: TShiftState; X, Y: Integer);
begin
  if (Button = mbRight) or (Button = mbLeft) then
    FNodeChangingTo := GetNodeAt(X, Y);
  inherited;
end;

function TContainerTreeView.GetSelectedItem: TContainedItem;
var
  SelNode: TTreeNode;
begin
  SelNode := FNodeChangingTo;
  if not Assigned(SelNode) then
    SelNode := Selected;
  if Assigned(SelNode) then
    result := TObject(SelNode.Data) as TContainedItem
  else
    result := nil;
end;

function TContainerTreeView.HandleQuery(Action: TBasicAction): boolean;
begin
  if Assigned(FOnHandleQuery) then
    result := FOnHandleQuery(Self, Action, GetSelectedItem)
  else
    result := false;
end;

procedure TContainerTreeView.ContainedUpdate(Action: TBasicAction; Item:
  TContainedItem; var PassToItem: boolean);
begin
  PassToItem := true;
  if Assigned(FOnContainedUpdate) then
    FOnContainedUpdate(self, Action, Item, PassToItem);
end;

procedure TContainerTreeView.ContainedExecute(Action: TBasicAction; Item:
  TContainedItem; var PassToItem: boolean);
begin
  PassToItem := true;
  if Assigned(FOnContainedExecute) then
    FOnContainedExecute(self, Action, Item, PassToItem);
end;

(************************************
 * TContainerListView               *
 ************************************)

function TContainerListView.GetSelectedItem: TContainedItem;
var
  SelItem: TListItem;
begin
  SelItem := Selected;
  if Assigned(SelItem) then
     result := TObject(SelItem.Data) as TContainedItem
  else
    result := nil;
end;

function TContainerListView.HandleQuery(Action: TBasicAction): boolean;
begin
  if Assigned(FOnHandleQuery) then
    result := FOnHandleQuery(Self, Action, GetSelectedItem)
  else
    result := false;
end;

procedure TContainerListView.ContainedUpdate(Action: TBasicAction; Item:
  TContainedItem; var PassToItem: boolean);
begin
  PassToItem := true;
  if Assigned(FOnContainedUpdate) then
    FOnContainedUpdate(self, Action, Item, PassToItem);
end;

procedure TContainerListView.ContainedExecute(Action: TBasicAction; Item:
  TContainedItem; var PassToItem: boolean);
begin
  PassToItem := true;
  if Assigned(FOnContainedExecute) then
    FOnContainedExecute(self, Action, Item, PassToItem);
end;

end.

