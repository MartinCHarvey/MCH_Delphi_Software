unit KKUtils;
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

function CompareDecimalKeyStr(Str1, Str2: string): integer;
function UnixDateTimeToDelphiDateTime(UnixDateTime: Int64): TDateTime;
function DelphiDateTimeToUnixDateTime(DelphiDateTime: TDateTime):Int64;

const
    S_INSTAGRAM_SITE_NAME = 'www.instagram.com';
    S_TWITTER_SITE_NAME = 'twitter.com'; //No www


implementation

uses
  SysUtils, StrUtils;

function CompareDecimalKeyStr(Str1, Str2: string):integer;
var
  Diff, L1, L2: integer;
begin
  L1 := Length(Str1);
  L2 := Length(Str2);
  Diff := L1 - L2;
  if Diff > 0 then
  begin
    Str2 := Str2.PadLeft(Diff + L2, '0');
  end
  else if Diff < 0 then
  begin
    Diff := -Diff;
    Str1 := Str1.PadLeft(Diff + L1, '0');
  end;
  result := CompareStr(Str1, Str2);
end;

function UnixDateTimeToDelphiDateTime(UnixDateTime: Int64): TDateTime;
begin
  Result := EncodeDate(1970, 1, 1) + (UnixDateTime / 86400); {86400=No. of secs. per day}
end;

function DelphiDateTimeToUnixDateTime(DelphiDateTime: TDateTime):Int64;
begin
  result := Trunc((DelphiDateTime - EncodeDate(1970, 1, 1)) * 86400);
end;

end.
