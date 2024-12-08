unit NullStream;

{

Copyright © 2024 Martin Harvey <martin_c_harvey@hotmail.com>

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

uses
  Classes, SysUtils;

{ NullStream does nothing. Does not even maintain any state about
  position, resources open (none) or anything else.
  Use it when you want absolutely all calls to go into the bitbucket. }

type
  TNullStream = class(TStream)
  private
  protected
    function GetSize: Int64; override;
    procedure SetSize(NewSize: Longint); overload; override;
    procedure SetSize(const NewSize: Int64); overload; override;
  public
    function Read(var Buffer; Count: Longint): Longint; overload; override;
    function Write(const Buffer; Count: Longint): Longint; overload; override;
    function Read(Buffer: TBytes; Offset, Count: Longint): Longint; overload; override;
    function Write(const Buffer: TBytes; Offset, Count: Longint): Longint; overload; override;
    function Seek(Offset: Longint; Origin: Word): Longint; overload; override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; overload; override;
  end;

implementation

function TNullStream.GetSize: Int64;
begin
  result := 0;
end;

procedure TNullStream.SetSize(NewSize: Longint);
begin
end;

procedure TNullStream.SetSize(const NewSize: Int64);
begin
end;

function TNullStream.Read(var Buffer; Count: Longint): Longint;
begin
  result := 0;
end;

function TNullStream.Write(const Buffer; Count: Longint): Longint;
begin
  result := 0;
end;

function TNullStream.Read(Buffer: TBytes; Offset, Count: Longint): Longint;
begin
  result := 0;
end;

function TNullStream.Write(const Buffer: TBytes; Offset, Count: Longint): Longint;
begin
  result := 0;
end;

function TNullStream.Seek(Offset: Longint; Origin: Word): Longint;
begin
  result := 0;
end;

function TNullStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  result := 0;
end;

end.
