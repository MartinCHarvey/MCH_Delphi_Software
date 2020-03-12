unit OrdinalSets;
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

uses Classes
{$IFDEF USE_TRACKABLES}
    , Trackables
{$ENDIF}
    ;

type

{$IFDEF USE_TRACKABLES}
  TOrdinalRange = class(TTrackable)
{$ELSE}
  TOrdinalRange = class
{$ENDIF}
  public
    Min, Max: Word;
  end;

  TOrdinalSetOp = (opAdd, opSubtract, OpIntersect, OpExclusive);

{$IFDEF USE_TRACKABLES}

  TOrdinalRangeList = class(TTrackable)
{$ELSE}
  TOrdinalRangeList = class
{$ENDIF}
  protected
    FRanges: TList;
    function GetHasMembers: boolean;
    function GetRangeCount: integer;
    procedure StripZeroSizeRanges;
  public
{$IFDEF USE_TRACKABLES}
    function GetExtraInfoText: string; override;
{$ENDIF}
    constructor Create;
    destructor Destroy; override;
    // The "Mingle" operator has a range for every case where there is
    // a range in either of the two parent sets, and a new range when
    // either or both of the parent sets have a range start or end.
    // It is both commutative and associative, and applying this
    // repeatedly for many sets (A, B, C) results in a new range for every case
    // where the membership of (A, B, C) differs.
    procedure MingleRanges(Other: TOrdinalRangeList); virtual;
    function GetFirstRange: TOrdinalRange;
    function GetNextRange(Range: TOrdinalRange): TOrdinalRange;
    function SameRangeList(Other: TOrdinalRangeList): boolean;
    function LookupRangeNumber(Ordinal: integer; var RangeNo: integer): boolean;
    procedure Clear;
    property HasMembers: boolean read GetHasMembers;
    property RangeCount: integer read GetRangeCount;
  end;

  TOrdinalSet = class(TOrdinalRangeList)
  private
  protected
    procedure Merge(Other: TOrdinalSet; Op: TOrdinalSetOp);
    procedure MergeTouchingRanges;
    procedure Normalise;
  public
    procedure MingleRanges(Other: TOrdinalRangeList); override;
    procedure Incl(Ordinal: integer);
    procedure InclAChar(Char: AnsiChar);
    procedure InclWChar(Char: WideChar);
    procedure AddSet(Other: TOrdinalSet);
    procedure RemoveSet(Other: TOrdinalSet);
    procedure IntersectSet(Other: TOrdinalSet);
    procedure ExclusiveSet(Other: TOrdinalSet);
    function Contains(Ordinal: integer): boolean;
    function ContainsAChar(Char: AnsiChar): boolean;
    function ContainsWChar(Char: WideChar): boolean;
    function SetEquals(Other: TOrdinalSet): boolean;
    // TODO.
    // function FirstInSet: integer;
    // function NextInSet(Ordinal: integer): integer;
    property HasMembers: boolean read GetHasMembers;
    property Count: integer read GetRangeCount;
  end;

implementation

uses
  SysUtils;

const
  S_NOT_EXPECTED_FOR_SETS = 'Expect the mingle operator only ' + 'for range lists, not sets';

  { TOrdinalRangeList }

{$IFDEF USE_TRACKABLES}

function TOrdinalRangeList.GetExtraInfoText: string;
var
  Idx: integer;
  Range: TOrdinalRange;
begin
  result := '[';
  for Idx := 0 to Pred(FRanges.Count) do
  begin
    Range := TObject(FRanges[Idx]) as TOrdinalRange;
    result := result + '(' + IntToStr(Range.Min) + ',' + IntToStr(Range.Max) + ')';
  end;
  result := result + ']';
end;
{$ENDIF}

function TOrdinalRangeList.GetHasMembers: boolean;
begin
  result := FRanges.Count > 0;
end;

function TOrdinalRangeList.GetRangeCount: integer;
begin
  result := FRanges.Count;
end;

function TOrdinalRangeList.GetFirstRange: TOrdinalRange;
begin
  result := nil;
  if FRanges.Count > 0 then
    result := TObject(FRanges[0]) as TOrdinalRange;
end;

function TOrdinalRangeList.GetNextRange(Range: TOrdinalRange): TOrdinalRange;
var
  Idx: integer;
begin
  result := nil;
  for Idx := 0 to Pred(FRanges.Count) do
  begin
    if Range = FRanges[Idx] then
    begin
      if Idx <> Pred(FRanges.Count) then
      begin
        result := TObject(FRanges[Succ(Idx)]) as TOrdinalRange;
        exit;
      end;
    end;
  end;
end;

procedure TOrdinalRangeList.MingleRanges(Other: TOrdinalRangeList);
var
  OwnIdx, OtherIdx: integer;
  OwnRangeBegin, OtherRangeBegin: boolean;
  OwnRange, OtherRange: TOrdinalRange;
  NextTransOwn: boolean;
  InOwn, InOther: boolean;
  OwnVal, OtherVal: integer;
  ResRanges: TList;
  NewRange: TOrdinalRange;
begin
  OwnIdx := 0;
  OtherIdx := 0;
  OwnRangeBegin := true;
  OtherRangeBegin := true;
  OwnVal := Low(integer);
  OtherVal := Low(integer);
  NewRange := nil;
  ResRanges := TList.Create;
  InOwn := false;
  InOther := false;
  while (OwnIdx < FRanges.Count) or (OtherIdx < Other.FRanges.Count) do
  begin
    if (OwnIdx < FRanges.Count) then
      OwnRange := FRanges[OwnIdx]
    else
      OwnRange := nil;
    if (OtherIdx < Other.FRanges.Count) then
      OtherRange := Other.FRanges[OtherIdx]
    else
      OtherRange := nil;
    // Find values pointed to by indexes and booleans.
    if Assigned(OwnRange) then
    begin
      if OwnRangeBegin then
        OwnVal := TOrdinalRange(FRanges[OwnIdx]).Min
      else
        OwnVal := TOrdinalRange(FRanges[OwnIdx]).Max;
    end;
    if Assigned(OtherRange) then
    begin
      if OtherRangeBegin then
        OtherVal := TOrdinalRange(Other.FRanges[OtherIdx]).Min
      else
        OtherVal := TOrdinalRange(Other.FRanges[OtherIdx]).Max;
    end;
    // Find next transition point based on ranges and selected values.
    if Assigned(OwnRange) and Assigned(OtherRange) then
    begin
      // If ordinal difference, then easy to work out which
      // to process first. If not, then process "max" vals before
      // "min" vals, so in overlapping cases, we do things in the right
      // order, even if we have to do a later normalisation
      // (touching  or zero size ranges)
      if OwnVal < OtherVal then
        NextTransOwn := true
      else if OwnVal > OtherVal then
        NextTransOwn := false
      else
      begin
        if not OwnRangeBegin then
          NextTransOwn := true
        else if not OtherRangeBegin then
          NextTransOwn := false
        else
          NextTransOwn := false; // Just set it to one of them (doesn't matter).
      end;
    end
    else
      NextTransOwn := Assigned(OwnRange);
    // Calculate new set membership.
    if NextTransOwn then
      InOwn := Assigned(OwnRange) and OwnRangeBegin
    else
      InOther := Assigned(OtherRange) and OtherRangeBegin;

    // Always finish previous range
    if Assigned(NewRange) then
    begin
      if NextTransOwn then
        NewRange.Max := OwnVal
      else
        NewRange.Max := OtherVal;
      ResRanges.Add(NewRange);
      NewRange := nil;
    end;
    // Start a new range.
    if (InOwn or InOther) then
    begin
      NewRange := TOrdinalRange.Create;
      if NextTransOwn then
        NewRange.Min := OwnVal
      else
        NewRange.Min := OtherVal;
    end;
    // Increment indices to next transition.
    if NextTransOwn then
    begin
      if not OwnRangeBegin then
        Inc(OwnIdx);
      OwnRangeBegin := not OwnRangeBegin;
    end
    else
    begin
      if not OtherRangeBegin then
        Inc(OtherIdx);
      OtherRangeBegin := not OtherRangeBegin;
    end;
  end;
  Assert(not Assigned(NewRange));
  // Replace ranges in my object.
  for OwnIdx := 0 to Pred(FRanges.Count) do
    TObject(FRanges[OwnIdx]).Free;
  FRanges.Free;
  FRanges := ResRanges;
  StripZeroSizeRanges;
end;

procedure TOrdinalRangeList.StripZeroSizeRanges;
var
  Idx: integer;
  ThisRange: TOrdinalRange;
begin
  // Go through ranges, and delete empty ones.
  Idx := 0;
  while Idx < FRanges.Count do
  begin
    ThisRange := TObject(FRanges[Idx]) as TOrdinalRange;
    if ThisRange.Min = ThisRange.Max then
    begin
      ThisRange.Free;
      FRanges[Idx] := nil;
    end;
    Inc(Idx);
  end;
  FRanges.Pack;
end;

constructor TOrdinalRangeList.Create;
begin
  inherited;
  FRanges := TList.Create;
end;

destructor TOrdinalRangeList.Destroy;
begin
  Clear;
  FRanges.Free;
  inherited;
end;

procedure TOrdinalRangeList.Clear;
var
  Idx: integer;
begin
  for Idx := 0 to Pred(FRanges.Count) do
    TObject(FRanges[Idx]).Free;
  FRanges.Clear;
end;

function TOrdinalRangeList.SameRangeList(Other: TOrdinalRangeList): boolean;
var
  Idx: integer;
  R1, R2: TOrdinalRange;
begin
  result := false;
  if not(FRanges.Count = Other.FRanges.Count) then
    exit;
  for Idx := 0 to Pred(FRanges.Count) do
  begin
    R1 := TObject(FRanges[Idx]) as TOrdinalRange;
    R2 := TObject(Other.FRanges[Idx]) as TOrdinalRange;
    if not((R1.Min = R2.Min) and (R1.Max = R2.Max)) then
      exit;
  end;
  result := true;
end;

function TOrdinalRangeList.LookupRangeNumber(Ordinal: integer; var RangeNo: integer): boolean;
var
  Idx: integer;
  Range: TOrdinalRange;
begin
  result := false;
  for Idx := 0 to Pred(FRanges.Count) do
  begin
    Range := TObject(FRanges[Idx]) as TOrdinalRange;
    if (Ordinal >= Range.Min) and (Ordinal < Range.Max) then
    begin
      result := true;
      RangeNo := Idx;
      exit;
    end;
  end;
end;

{ TOrdinalSet }

function TOrdinalSet.SetEquals(Other: TOrdinalSet): boolean;
begin
  result := SameRangeList(Other);
end;

procedure TOrdinalSet.MingleRanges(Other: TOrdinalRangeList);
begin
  Assert(false, S_NOT_EXPECTED_FOR_SETS);
  inherited;
end;

procedure TOrdinalSet.Incl(Ordinal: integer);
var
  Idx: integer;
  Range, NewRange: TOrdinalRange;
  Added: boolean;
begin
  Added := false;
  Idx := 0;
  while (not Added) and (Idx < FRanges.Count) do
  begin
    Range := TObject(FRanges[Idx]) as TOrdinalRange;
    if Ordinal < Range.Min then
    begin
      NewRange := TOrdinalRange.Create;
      NewRange.Min := Ordinal;
      NewRange.Max := Succ(Ordinal);
      FRanges.Insert(Idx, NewRange);
      Added := true;
    end
    else if (Ordinal >= Range.Min) and (Ordinal < Range.Max) then
    begin
      // Nothing to do, already.
      Added := true;
    end;
    Inc(Idx);
  end;
  if not Added then
  begin
    // Add to the very end of the list of ranges.
    NewRange := TOrdinalRange.Create;
    NewRange.Min := Ordinal;
    NewRange.Max := Succ(Ordinal);
    FRanges.Add(NewRange);
  end;
  Normalise;
end;

procedure TOrdinalSet.InclAChar(Char: AnsiChar);
begin
  Incl(Ord(Char));
end;

procedure TOrdinalSet.InclWChar(Char: WideChar);
begin
  Incl(Ord(Char));
end;

procedure TOrdinalSet.MergeTouchingRanges;
var
  Idx: integer;
  ThisRange, NextRange: TOrdinalRange;
begin
  // Go through ranges and merge adjacent or intersecting ones
  // Assumption is that range maxima are ordered.
  Idx := 0;
  while Succ(Idx) < FRanges.Count do
  begin
    // Ranges should not be zero size.
    ThisRange := TObject(FRanges[Idx]) as TOrdinalRange;
    NextRange := TObject(FRanges[Succ(Idx)]) as TOrdinalRange;
    if ThisRange.Max >= NextRange.Min then
    begin
      Assert(NextRange.Max >= ThisRange.Max); // FUBAR, merge violates ordering.
      // Merge, no increment.
      NextRange.Min := ThisRange.Min;
      ThisRange.Free;
      FRanges.Delete(Idx);
    end
    else
      Inc(Idx);
  end;
end;

procedure TOrdinalSet.Normalise;
begin
  StripZeroSizeRanges;
  MergeTouchingRanges;
end;

procedure TOrdinalSet.Merge(Other: TOrdinalSet; Op: TOrdinalSetOp);
var
  OwnIdx, OtherIdx: integer;
  OwnRangeBegin, OtherRangeBegin: boolean;
  OwnRange, OtherRange: TOrdinalRange;
  InResult, PrevInResult: boolean;
  NextTransOwn: boolean;
  OwnVal, OtherVal: integer;
  ResRanges: TList;
  NewRange: TOrdinalRange;
  InOwn, InOther: boolean;
begin
  OwnIdx := 0;
  OtherIdx := 0;
  OwnRangeBegin := true;
  OtherRangeBegin := true;
  InResult := false;
  PrevInResult := false;
  OwnVal := Low(integer);
  OtherVal := Low(integer);
  NewRange := nil;
  ResRanges := TList.Create;
  InOwn := false;
  InOther := false;
  while (OwnIdx < FRanges.Count) or (OtherIdx < Other.FRanges.Count) do
  begin
    if (OwnIdx < FRanges.Count) then
      OwnRange := FRanges[OwnIdx]
    else
      OwnRange := nil;
    if (OtherIdx < Other.FRanges.Count) then
      OtherRange := Other.FRanges[OtherIdx]
    else
      OtherRange := nil;
    // Find values pointed to by indexes and booleans.
    if Assigned(OwnRange) then
    begin
      if OwnRangeBegin then
        OwnVal := TOrdinalRange(FRanges[OwnIdx]).Min
      else
        OwnVal := TOrdinalRange(FRanges[OwnIdx]).Max;
    end;
    if Assigned(OtherRange) then
    begin
      if OtherRangeBegin then
        OtherVal := TOrdinalRange(Other.FRanges[OtherIdx]).Min
      else
        OtherVal := TOrdinalRange(Other.FRanges[OtherIdx]).Max;
    end;
    // Find next transition point based on ranges and selected values.
    if Assigned(OwnRange) and Assigned(OtherRange) then
    begin
      // If ordinal difference, then easy to work out which
      // to process first. If not, then process "max" vals before
      // "min" vals, so in overlapping cases, we do things in the right
      // order, even if we have to do a later normalisation
      // (touching  or zero size ranges)
      if OwnVal < OtherVal then
        NextTransOwn := true
      else if OwnVal > OtherVal then
        NextTransOwn := false
      else
      begin
        if not OwnRangeBegin then
          NextTransOwn := true
        else if not OtherRangeBegin then
          NextTransOwn := false
        else
          NextTransOwn := false; // Just set it to one of them (doesn't matter).
      end;
    end
    else
      NextTransOwn := Assigned(OwnRange);
    // Calculate new set membership.
    if NextTransOwn then
      InOwn := Assigned(OwnRange) and OwnRangeBegin
    else
      InOther := Assigned(OtherRange) and OtherRangeBegin;
    case Op of
      opAdd:
        InResult := InOwn or InOther;
      opSubtract:
        InResult := InOwn and not InOther;
      OpIntersect:
        InResult := InOwn and InOther;
      OpExclusive:
        InResult := (InOwn or InOther) and not(InOwn and InOther);
    else
      Assert(false);
    end;
    // Store the result.
    if InResult <> PrevInResult then
    begin
      if InResult then
      begin
        NewRange := TOrdinalRange.Create;
        if NextTransOwn then
          NewRange.Min := OwnVal
        else
          NewRange.Min := OtherVal;
      end
      else
      begin
        // Finished item in result range.
        if NextTransOwn then
          NewRange.Max := OwnVal
        else
          NewRange.Max := OtherVal;
        ResRanges.Add(NewRange);
        NewRange := nil;
      end;
      PrevInResult := InResult;
    end;
    // Increment indices to next transition.
    if NextTransOwn then
    begin
      if not OwnRangeBegin then
        Inc(OwnIdx);
      OwnRangeBegin := not OwnRangeBegin;
    end
    else
    begin
      if not OtherRangeBegin then
        Inc(OtherIdx);
      OtherRangeBegin := not OtherRangeBegin;
    end;
  end;
  Assert(not InResult); // Check output range creation completed.
  // Replace ranges in my object.
  for OwnIdx := 0 to Pred(FRanges.Count) do
    TObject(FRanges[OwnIdx]).Free;
  FRanges.Free;
  FRanges := ResRanges;
  Normalise; // Result ranges may touch, or be empty.
end;

procedure TOrdinalSet.AddSet(Other: TOrdinalSet);
begin
  Merge(Other, opAdd);
end;

procedure TOrdinalSet.RemoveSet(Other: TOrdinalSet);
begin
  Merge(Other, opSubtract);
end;

procedure TOrdinalSet.IntersectSet(Other: TOrdinalSet);
begin
  Merge(Other, OpIntersect);
end;

procedure TOrdinalSet.ExclusiveSet(Other: TOrdinalSet);
begin
  Merge(Other, OpExclusive);
end;

function TOrdinalSet.Contains(Ordinal: integer): boolean;
var
  Idx: integer;
  Range: TOrdinalRange;
begin
  for Idx := 0 to Pred(FRanges.Count) do
  begin
    Range := TObject(FRanges[Idx]) as TOrdinalRange;
    if Ordinal >= Range.Min then
    begin
      // In this or subsequent ranges.
      if Ordinal < Range.Max then
      begin
        result := true;
        exit;
      end;
    end
    else
    begin
      result := false; // Nah, too small.
      exit;
    end;
  end;
  result := false; // No, I'm not going to go from the edges in ....
end;

function TOrdinalSet.ContainsAChar(Char: AnsiChar): boolean;
begin
  result := Contains(Ord(Char));
end;

function TOrdinalSet.ContainsWChar(Char: WideChar): boolean;
begin
  result := Contains(Ord(Char));
end;

end.
