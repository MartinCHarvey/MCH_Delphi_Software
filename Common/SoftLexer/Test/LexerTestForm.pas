unit LexerTestForm;
{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

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
