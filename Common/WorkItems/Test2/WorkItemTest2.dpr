program WorkItemTest2;

uses
  Vcl.Forms,
  WorkItemTest2Frm in 'WorkItemTest2Frm.pas' {WorkItemTest2Form},
  RecycleItems in 'RecycleItems.pas',
  WorkItems in '..\WorkItems.pas',
  DLList in '..\..\DLList\DLList.pas',
  IndexedStore in '..\..\Indexed Store\IndexedStore.pas',
  BinaryTree in '..\..\Balanced Tree\BinaryTree.pas',
  Vcl.Themes,
  Vcl.Styles,
  Trackables in '..\..\Tracking\Trackables.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TWorkItemTest2Form, WorkItemTest2Form);
  Application.Run;
end.
