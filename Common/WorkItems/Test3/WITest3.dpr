program WITest3;

uses
  Vcl.Forms,
  WorkItemTest3 in 'WorkItemTest3.pas' {Form1},
  WorkItemsNUMA in '..\WorkItemsNUMA.pas',
  WorkItems in '..\WorkItems.pas',
  DLList in '..\..\DLList\DLList.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
