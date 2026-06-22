program SQL92ParserTest;

uses
  FMX.Forms,
  ParserTestFrm in 'ParserTestFrm.pas' {Form1},
  SQL92Nodes in '..\SQL92Nodes.pas',
  Trackables in '..\..\..\Tracking\Trackables.pas',
  CommonNodes in '..\..\..\HTMLParser\CommonNodes.pas',
  SQL92Grammar_lexer in '..\SQL92Grammar_lexer.pas',
  lexlib in '..\..\..\tply_redist\lexlib.pas',
  DLList in '..\..\..\DLList\DLList.pas',
  BinaryTree in '..\..\..\Balanced Tree\BinaryTree.pas',
  SQL92Grammar_parser in '..\SQL92Grammar_parser.pas',
  yacclib_trkobj in '..\..\..\tply_redist\yacclib_trkobj.pas';

{$R *.res}

begin
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
