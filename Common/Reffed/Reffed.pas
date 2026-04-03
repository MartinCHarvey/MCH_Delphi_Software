unit Reffed;

{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the �Software�), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED �AS IS�, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

}

interface

{$IFDEF USE_TRACKABLES}
uses
  Trackables;
{$ENDIF}

type
{$IFDEF USE_TRACKABLES}
  TReffed = class(TTrackable)
{$ELSE}
  TReffed = class
{$ENDIF}
  private
    FRef: integer;
  public
    constructor Create;

{$IFOPT C+}
    function Unitary: boolean;
    function AddRef: TReffed;
    function Release: boolean;
{$ELSE}
    function Unitary: boolean; inline;
    function AddRef: TReffed; inline;
    function Release: boolean; inline;
{$ENDIF}

    procedure Free;
  end;

implementation

uses
  LockAbstractions;

constructor TReffed.Create;
begin
  inherited;
  FRef := 1;
end;

function TReffed.AddRef: TReffed;
begin
  if Assigned(Self) then
    InterlockedIncrement(FRef);
  result := Self;
end;

function TReffed.Release: boolean;
begin
  result := false;
  if Assigned(self) then
  begin
{$IFOPT C+}
    //Let trackables catch too many releases.
    if InterlockedDecrement(FRef) <= 0 then
{$ELSE}
    if InterlockedDecrement(FRef) = 0 then
{$ENDIF}
    begin
      result := true;
      inherited Free;
    end;
  end;
end;

function TReffed.Unitary: boolean;
begin
  result := FRef = 1;
end;

procedure TReffed.Free;
begin
  Assert(false);    //Use release instead.
  Assert(FRef = 1);
  Release;
end;

end.
