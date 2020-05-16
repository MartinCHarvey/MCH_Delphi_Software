unit HTTPForwarder;

{

Copyright © 2020 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the “Software”), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

interface

{ Forwards all HTTP requests to HTTPS }

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  SysUtils, IdHTTPServer, HTTPMisc, IdContext, IdCustomHTTPServer;

type
{$IFDEF USE_TRACKABLES}
  THTTPForwarder = class(TTrackable)
{$ELSE}
  THTTPForwarder = class
{$ENDIF}
  private
    FIdHTTP: TIdHTTPServer;
    FOnEndpointRequest: TEndpointRequestEvent;
  protected
    function DoEndpointRequest: string;
    procedure HandleIdCommandGet(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);
  public
    constructor Create;
    destructor Destroy; override;
    property OnEndpointRequest: TEndpointRequestEvent read FOnEndpointRequest write FOnEndpointRequest;
  end;

var
  GHTTPForwarder: THTTPForwarder;

implementation

uses
  IdAssignedNumbers;

procedure THTTPForwarder.HandleIdCommandGet(AContext: TIdContext; ARequestInfo: TIdHTTPRequestInfo; AResponseInfo: TIdHTTPResponseInfo);
begin
  AResponseInfo.Redirect(S_HTTPS_PREFIX + DoEndpointRequest + ARequestInfo.URI);
end;

function THTTPForwarder.DoEndpointRequest: string;
begin
  if Assigned(FOnEndpointRequest) then
    result := FOnEndpointRequest(self)
  else
    result := S_LOCALHOST;
end;

constructor THTTPForwarder.Create;
begin
  inherited;
  FIdHTTP := TIdHTTPServer.Create(nil);
  FIdHTTP.OnCommandGet := HandleIdCommandGet;
  FIdHTTP.KeepAlive := true;
  //SSL changes.
  FIdHTTP.DefaultPort := IdPORT_http;
  FIdHTTP.Active := true;
end;

destructor THTTPForwarder.Destroy;
begin
  FIdHTTP.Free;
  inherited;
end;

initialization
  GHTTPForwarder := THTTPForwarder.Create;
finalization
  GHTTPForwarder.Free;
end.
