unit LexerTestForm;

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Variants, System.Classes, Vcl.Graphics,
  Vcl.Controls, Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls;

type
  TForm1 = class(TForm)
    TestBtn: TButton;
    InputTextEdit: TEdit;
    TokenLiteralsMemo: TMemo;
    ResultsMemo: TMemo;
    procedure TestBtnClick(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
  end;

var
  Form1: TForm1;

implementation

{$R *.dfm}

uses SoftLexer;


procedure TForm1.TestBtnClick(Sender: TObject);
var
  Lexer: TUnicodeSoftLexer;
  ErrMsg: string;
  idx, idx2: integer;
  Str: string;
  TokenResults: TList;
  WideRes: TWideTokenResult;
begin
  Lexer := TUnicodeSoftLexer.Create;
  try
    for idx := 0 to Pred(TokenLiteralsMemo.Lines.Count) do
    begin
      Str := TokenLiteralsMemo.Lines[idx];
      if Length(Str) > 0 then
        Lexer.AddWTokenString(Str, idx);
    end;
    ResultsMemo.Clear;
    if not Lexer.SetupLexer(ErrMsg) then
    begin
      ResultsMemo.Lines.Add('Error: ' + ErrMsg);
      exit;
    end;
    for idx := 1 to Length(InputTextEdit.Text) do
    begin
      TokenResults := nil;
      if Lexer.InputWChar(InputTextEdit.Text[idx], TokenResults) then
      begin
        if Assigned(TokenResults) then
        begin
          for idx2 := 0 to Pred(TokenResults.Count) do
          begin
            WideRes := TWideTokenResult(TokenResults[idx2]);
            if WideRes.TokenTag <> TOK_INVALID then
              ResultsMemo.Lines.Add(
                'Token ' + IntToStr(WideRes.TokenTag)
                + '(' + WideRes.TokenString + ')')
            else
              ResultsMemo.Lines.Add(
                'Invalid token (' + WideRes.TokenString + ')');
            WideRes.Free;
          end;
          TokenResults.Free;
        end;
      end;
    end;

  finally
    Lexer.Free;
  end;
end;

end.
