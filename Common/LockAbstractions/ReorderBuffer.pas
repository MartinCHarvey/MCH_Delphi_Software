unit ReorderBuffer;

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
  uses SyncObjs, DLList
{$IFDEF USE_TRACKABLES}
  ,Trackables
{$ENDIF}
  ;

type
{$IFDEF USE_TRACKABLES}
  TReservation = class(TTrackable)
{$ELSE}
  TReservation = record
{$ENDIF}
    FListItem: TDLEntry;
    FData: TObject;
  end;
{$IFDEF USE_TRACKABLES}
  PReservation = TReservation;
{$ELSE}
  PReservation = ^TReservation;
{$ENDIF}

{$IFDEF USE_TRACKABLES}
  TReorderBuffer = class(TTrackable)
{$ELSE}
  TReorderBuffer = class
{$ENDIF}
  private
    FLock: TCriticalSection;
    FQueue: TDLEntry;
  protected
  public
    constructor Create;
    destructor Destroy; override;

    function Reserve: PReservation;
    procedure Commit(var Reservation: PReservation; Data: TObject);
    procedure Abort(var Reservation: PReservation);
    function Drain: TObject;
  end;

implementation

uses
  Reffed;

function NewReservation: PReservation;
begin
{$IFDEF USE_TRACKABLES}
  result := TReservation.Create;
{$ELSE}
  New(result);
  FillChar(result^, sizeof(result^), 0);
{$ENDIF}
end;

procedure DisposeReservation(Reservation: PReservation);
begin
{$IFDEF USE_TRACKABLES}
  Reservation.Free;
{$ELSE}
  Dispose(Reservation);
{$ENDIF}
end;

constructor TReorderBuffer.Create;
begin
  inherited;
  FLock := TCriticalSection.Create;
  DLItemInitList(@FQueue);
end;

destructor TReorderBuffer.Destroy;
var
  Obj: TObject;
begin
  Assert(DlItemIsEmpty(@FQueue));
  Obj := Drain;
  while Assigned(Obj) do
  begin
    if Obj is TReffed then
      (Obj as TReffed).Release
    else
      Obj.Free;
    Obj := Drain;
  end;
  FLock.Free;
  inherited;
end;

function TReorderBuffer.Reserve: PReservation;
begin
  result := NewReservation;
  DLItemInitObj(TObject(result), @result.FListItem);
  FLock.Acquire;
  try
    DLListInsertTail(@FQueue, @result.FListItem);
  finally
    FLock.Release;
  end;
end;

procedure TReorderBuffer.Commit(var Reservation: PReservation; Data: TObject);
begin
  Assert(Assigned(Reservation));
  Assert(not Assigned(Reservation.FData));
  Assert(Assigned(Data));
  FLock.Acquire;
  try
    Reservation.FData := Data;
    Reservation := nil;
  finally
    FLock.Release;
  end;
end;

procedure TReorderBuffer.Abort(var Reservation: PReservation);
var
  Tmp: PReservation;
begin
  if Assigned(Reservation) then
  begin
    Assert(not Assigned(Reservation.FData));
    FLock.Acquire;
    try
      Tmp := Reservation;
      Reservation := nil;
      DLListRemoveObj(@Tmp.FListItem);
    finally
      FLock.Release;
    end;
    DisposeReservation(Tmp);
  end;
end;

function TReorderBuffer.Drain: TObject;
var
  Reservation: PReservation;
begin
  result := nil;
  Reservation := nil;
  FLock.Acquire;
  try
    if not DlItemIsEmpty(@FQueue) then
    begin
      Reservation := PReservation(FQueue.FLink.Owner);
      if Assigned(Reservation.FData) then
      begin
        DLListRemoveObj(@Reservation.FListItem);
        result := Reservation.FData;
      end
      else
        Reservation := nil;
    end;
  finally
    FLock.Release;
  end;
  if Assigned(Reservation) then
    DisposeReservation(Reservation);
end;


end.
