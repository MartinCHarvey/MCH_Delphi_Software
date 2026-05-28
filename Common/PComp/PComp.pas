unit PComp;

{

Copyright © 2026 Martin Harvey <martin_c_harvey@hotmail.com>

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

function ComparePointers(Own, Other: Pointer): integer;

implementation

{$HINTS OFF}
function ComparePointers(Own, Other: Pointer): integer;
var
  OwnInt, OtherInt: Cardinal;
  Own64, Other64: UInt64;
begin
  if sizeof(Pointer) = sizeof(Cardinal) then
  begin
    OwnInt := Cardinal(Own);
    OtherInt := Cardinal(Other);
    if OtherInt > OwnInt then
      result := 1
    else if OtherInt < OwnInt then
      result := -1
    else
      result := 0;
  end
  else if sizeof(Pointer) = sizeof(UInt64) then
  begin
    Own64 := UInt64(Own);
    Other64 := UInt64(Other);
    if Other64 > Own64 then
      result := 1
    else if Other64 < Own64 then
      result := -1
    else
      result := 0;
  end
  else
    Assert(false);
end;
{$HINTS ON}

end.
