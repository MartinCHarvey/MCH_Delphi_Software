program SQL92ParserTest;

uses
  FMX.Forms,
  ParserTestFrm in 'ParserTestFrm.pas' {Form1},
  SQL92Nodes in '..\SQL92Nodes.pas',
  Trackables in '..\..\..\Tracking\Trackables.pas',
  BinaryTree in '..\..\..\Balanced Tree\BinaryTree.pas',
  CommonNodes in '..\..\..\HTMLParser\CommonNodes.pas',
  DLList in '..\..\..\DLList\DLList.pas',
  SQL92Grammar_lexer in '..\SQL92Grammar_lexer.pas',
  lexlib in '..\..\..\tply_redist\lexlib.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
