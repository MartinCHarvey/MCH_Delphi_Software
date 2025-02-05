unit ForwardBugUnit;

interface

uses
  System.SysUtils, System.Types, System.UITypes, System.Rtti, System.Classes,
  System.Variants, FMX.Types, FMX.Controls, FMX.Forms, FMX.Dialogs,
  FMX.StdCtrls, IdHTTPServer, IdContext, IdCustomHTTPServer;

type
  TForm1 = class(TForm)
    procedure FormCreate(Sender: TObject);
    procedure FormDestroy(Sender: TObject);
  private
    { Private declarations }
  public
    { Public declarations }
    FIdHTTP: TIdHTTPServer;
    procedure HandleCommandGet(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);
  end;

var
  Form1: TForm1;

implementation

{$R *.fmx}

procedure TForm1.FormCreate(Sender: TObject);
begin
  FIdHTTP := TIdHTTPServer.Create;
  FIdHTTP.DefaultPort := 8080;
  FIdHTTP.OnCommandGet := HandleCommandGet;
  FIdHTTP.Active := true;
end;

procedure TForm1.FormDestroy(Sender: TObject);
begin
  FIdHTTP.Free;
end;

procedure TForm1.HandleCommandGet(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);

  procedure QueryParamURIHandling(OriginalURI: string;
                                  URIComponents: TStringList;
                                  var URIExtension: string;
                                  var ParamsOrBookmark: string);
  var
    SepPos, tmp: integer;
    done: boolean;
  begin
    //TODO - expect an uri, but will give up when we get ? or #
    //explicitly should not get // strings (empty URI components).
    if Pos('/', OriginalURI) = 1 then
      OriginalURI := OriginalURI.Substring(1);

    while Length(OriginalURI) > 0 do
    begin
      Done := false;

      SepPos := Succ(Length(OriginalURI));
      tmp := Pos('/', OriginalURI);
      if (tmp > 0) and (tmp <= SepPos) then
        SepPos := tmp;

      tmp := Pos('#', OriginalURI);
      if (tmp > 0) and (tmp <= SepPos) then
      begin
        SepPos := tmp;
        Done := true;
      end;

      tmp := Pos('?', OriginalURI);
      if (tmp > 0) and (tmp <= SepPos) then
      begin
        SepPos := tmp;
        Done := true;
      end;

      URIComponents.Add(OriginalURI.Substring(0, Pred(SepPos)));
      if Done then
      begin
        ParamsOrBookmark := OriginalURI.Substring(Pred(SepPos));
        OriginalURI := '';
      end
      else
        OriginalURI := OriginalURI.Substring(SepPos);
    end;
    if URIComponents.Count > 0 then
    begin
      URIExtension := URIComponents.Strings[Pred(URIComponents.Count)];
      SepPos := Pos('.', URIExtension);
      if SepPos > 0 then
        URIExtension := URIExtension.Substring(Pred(SepPos))
      else
        URIExtension := '';
    end;
  end;

  procedure QueryParamRawURI(var RawURI: string);
  var
    Tmp: string;
    SpacePos: integer;
  begin
    Tmp := ARequestInfo.RawHTTPCommand.Trim;
    SpacePos := Pos(' ', Tmp);
    if SpacePos > 0 then
    begin
      Tmp := Tmp.Substring(SpacePos).Trim;
      SpacePos := Pos(' ', Tmp);
      if SpacePos > 0 then
        Tmp := Tmp.Substring(0, Pred(SpacePos));
    end;
    RawURI := Tmp;
  end;

var
  Components:TStringList;

begin
  Components := TStringList.Create;
  try

  finally
    Components.Free;
  end;
//  ARequestInfo.
end;


end.
