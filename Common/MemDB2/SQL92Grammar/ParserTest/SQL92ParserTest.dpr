program SQL92ParserTest;

uses
  FMX.Forms,
  ParserTestFrm in 'ParserTestFrm.pas' {Form1},
  SQL92Grammar in '..\SQL92Grammar.PAS',
  SQL92Nodes in '..\SQL92Nodes.pas',
  CocoBase in '..\..\..\..\CoCoDM\Distributable\Frames\CocoBase.pas',
  Trackables in '..\..\..\Tracking\Trackables.pas',
  BinaryTree in '..\..\..\Balanced Tree\BinaryTree.pas',
  CommonNodes in '..\..\..\HTMLParser\CommonNodes.pas',
  DLList in '..\..\..\DLList\DLList.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
