unit MemDBIndexing;
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

{
  In memory database. Indexing and constraint code.
}

interface

uses
{$IFDEF USE_TRACKABLES}
  Trackables,
{$ENDIF}
  IndexedStore, MemDBMisc;

type
  TIndexNumber = type integer;

  TMemDBIndexNode = class(TDuplicateValIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  end;

  TMemDBIndexNodeSearchVal = class(TMemDbIndexNode)
  private
    FPointerSearchVal: pointer;
    FIdSearchVal: string;
    FFieldSearchVals: TMemDbFieldDataRecs;
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  public
    property PointerSearchVal: pointer read FPointerSearchVal write FPointerSearchVal;
    property IdSearchVal: string read FIdSearchVal write FIdSearchVal;
    property FieldSearchVals: TMemDbFieldDataRecs read FFieldSearchVals write FFieldSearchVals;
  end;

  TMemDBRowLookasideIndexNode = class(TDuplicateValIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  end;

  TMemDBRowLookasideSearchVal = class(TMemDBRowLookasideIndexNode)
  private
    FFieldSearchVals: TMemDbFieldDataRecs;
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer; override;
  public
    property FieldSearchVals: TMemDbFieldDataRecs read FFieldSearchVals write FFieldSearchVals;
  end;

{$IFOPT C+}
function CompareFields(const OwnRec: TMemDbFieldDataRec; const OtherRec: TMemDbFieldDataRec): integer;
{$ELSE}
function CompareFields(const OwnRec: TMemDbFieldDataRec; const OtherRec: TMemDbFieldDataRec): integer; inline;
{$ENDIF}

const
  S_INDEXING_INTERNAL_FIELD_TYPE_1 = 'Internal indexing error: Different field types.';
  S_INDEXING_INTERNAL_FIELD_TYPE_2 = 'Internal indexing error: Unsupported field type.';

var
  MDBInternalIndexRowId:TMemDBITagData;

implementation

uses
{$IFOPT C+}
{$ELSE}
  Classes,
{$ENDIF}
  MemDBBuffered, SysUtils, MemDbStreamable;

{ Misc functions }

function CompareFields(const OwnRec: TMemDbFieldDataRec; const OtherRec: TMemDbFieldDataRec): integer;
begin
  if OwnRec.FieldType <> OtherRec.FieldType then
    raise EMemDBInternalException.Create(S_INDEXING_INTERNAL_FIELD_TYPE_1);
  case OwnRec.FieldType of
    ftInteger:
      if OtherRec.i32Val > OwnRec.i32Val then
        result := 1
      else if OtherRec.i32Val < OwnRec.i32Val then
        result := -1
      else
        result := 0;
    ftCardinal:
      if OtherRec.u32Val > OwnRec.u32Val then
        result := 1
      else if OtherRec.u32Val < OwnRec.u32Val then
        result := -1
      else
        result := 0;
    ftInt64:
      if OtherRec.i64Val > OwnRec.i64Val then
        result := 1
      else if OtherRec.i64Val < OwnRec.i64Val then
        result := -1
      else
        result := 0;
    ftUint64:
      if OtherRec.u64Val > OwnRec.u64Val then
        result := 1
      else if OtherRec.u64Val < OwnRec.u64Val then
        result := -1
      else
        result := 0;
    ftUnicodeString: result := CompareStr(OtherRec.sVal, OwnRec.sVal);
    ftDouble:
      if OtherRec.dVal > OwnRec.dVal then
        result := 1
      else if OtherRec.dVal < OwnRec.dVal then
        result := -1
      else
        result := 0;
    ftGuid: result := CompareGuids(OtherRec.gVal, OwnRec.gVal);
  else
    raise EMemDBInternalException.Create(S_INDEXING_INTERNAL_FIELD_TYPE_2);
    result := 0;
  end;
end;

{ TMemDbIndexNode }

{ Two separate cases:
  1. Changing data:
    a) Current Copy should be either NIL or valid.
    b) Next copy is Valid or delete sentinel

  - CurrentIndex always deals with CurrentCopy,
    sufficient to put NIL's at one end (low / high) of the index.

  - NextData deals with most recent copy,
    - might have both NIL in current and delete sentinel if added and
      deleted new row.

  - Don't distinguish between NIL and delete sentinel once we've finished
    resolving the field list

    - For the moment, don't skip delete sentinels on fields,
      we'll assume the index tags are up to date and valid.

  2. Changing field lists.

   - Items gets removed and re-added.

     - We encode the current field, and the next field in the "extra field",
       current field is used on tree removal, and next field on re-insertion,
       in cases where metadata changed.

       Current and next are the same for existing indices when items removed
       from the tree for modification via metadata changes, so no index tag
       re-name required.

       However, upon re-insertion into the tree (with delete sentinels),
       the current and next are different, the next value being used
       just after commit time, when the delete sentinels have been removed.

}

//TODO - Consider some radical hack whereby we don't need to adjust the indexes
//to temporary values at all.

var
  GoodTagsRead: integer = 0;

function TMemDBIndexNode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  PTag: PITagStruct;
  TagData: TMemDBITagData;

  OwnRow, OtherRow: TMemDBRow;
  OwnFieldList, OtherFieldList: TMemStreamableList;
  OwnField, OtherField: TMemDBStreamable;
  OwnFieldOffsets, OtherFieldOffsets: TFieldOffsets;
  UsingNextCopy, OtherUsingNextCopy: boolean;
  ff:integer;
  LOfs: integer;
begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OwnItem is TMemDbRow);
  Assert(OtherItem is TMemDbRow);
{$IFOPT C+}
  OwnRow := OwnItem as TMemDbRow;
  OtherRow := OtherItem as TMemDBRow;
{$ELSE}
  OwnRow := TMemDBRow(OwnItem);
  OtherRow := TMemDBROw(OtherItem);
{$ENDIF}
  PTag := PItagStruct(IndexTag);
  TagData := PTag.TagData;
  Assert(TagData is TMemDBITagData);
  Assert(TagData.TagStructs[sicCurrent].TagData = TagData);
  Assert(TagData.TagStructs[sicLatest].TagData = TagData);
  Inc(GoodTagsRead);
  if TagData.MainIndexClass = micInternal then
  begin
    case TagData.InternalIndexClass of
    iicRowId:
      result:= CompareStr(OwnRow.RowId, OtherRow.RowId);
    else
      Assert(false);
      result :=0;
    end;
  end
  else
  begin
    OwnFieldList := nil;
    OtherFieldList := nil;

    if Assigned(OwnRow.ABData[abCurrent]) then
    begin
      Assert(not (OwnRow.ABData[abCurrent] is TMemDeleteSentinel));
{$IFOPT C+}
      OwnFieldList := OwnRow.ABData[abCurrent] as TMemStreamableList;
{$ELSE}
      OwnFieldList := TMemStreamableList(OwnRow.ABData[abCurrent]);
{$ENDIF}
    end;
    if Assigned(OtherRow.ABData[abCurrent]) then
    begin
      Assert(not (OtherRow.ABData[abCurrent] is TMemDeleteSentinel));
{$IFOPT C+}
      OtherFieldList := OtherRow.ABData[abCurrent] as TMemStreamableList;
{$ELSE}
      OtherFieldList := TMemStreamableList(OtherRow.ABData[abCurrent]);
{$ENDIF}
    end;
    //Main index class is either permanent or temporary.
    //Case 2)
    //If temporary, then index newly added, or changing field number,
    //hence latest ab buffer if table structure has changed, else current
    //(possibly to add indexes without changing table structure).
    //Case 1)
    //If permanent, then using next if subclass is most recent.

    if (TagData.MainIndexClass = micTemporary)
      or (PTag.SubIndexClass = sicLatest) then
    begin
      //Go for most recent A/B buffer if we can.
      UsingNextCopy := Assigned(OwnRow.ABData[abNext]);
      OtherUsingNextCopy := Assigned(OtherRow.ABData[abNext]);
      //Not necessarily!! When change table metadata, not all the rows
      //have been processed at once.
      //if IndexClass = icTemporary then
        //Assert(UsingNextCopy = OtherUsingNextCopy);
      if UsingNextCopy then
      begin
        if OwnRow.ABData[abNext] is TMemDeleteSentinel then
          OwnFieldList := nil
        else
        begin
{$IFOPT C+}
          OwnFieldList := OwnRow.ABData[abNext] as TMemStreamableList;
{$ELSE}
          OwnFieldList := TMemStreamableList(OwnRow.ABData[abNext]);
{$ENDIF}
        end;
      end;
      if OtherUsingNextCopy then
      begin
        if OtherRow.ABData[abNext] is TMemDeleteSentinel then
          OtherFieldList := nil
        else
        begin
{$IFOPT C+}
          OtherFieldList := OtherRow.ABData[abNext] as TMemStreamableList;
{$ELSE}
          OtherFieldList := TMemStreamableList(OtherRow.ABData[abNext]);
{$ENDIF}
        end;
      end;
    end
    else
    begin
      UsingNextCopy := false;
      OtherUsingNextCopy := false;
    end;
    if not (Assigned(OwnFieldList) or Assigned(OtherFieldList)) then
      result := 0
    else if not (Assigned(OwnFieldList) and Assigned(OtherFieldList)) then
    begin
      if Assigned(OwnFieldList) then
        result := 1
      else
        result := -1;
    end
    else //Both assigned.
    begin
      if (TagData.MainIndexClass = micTemporary) then
      begin
        //These field offsets look the wrong way around, but aren't.

        //ExtraFieldOffset is the "higher" number when removing fields from the
        //table. Not only does it correspond to the old index, (which you expect)

        //it *also* corresponds to the new index, when the delete sentinels
        //have not yet been removed.

        //Dynamic array assignment by reference - no dynamic array allocation
        //or copying here.
        if UsingNextCopy then
          OwnFieldOffsets := TagData.ExtraFieldOffsets
        else
          OwnFieldOffsets := TagData.DefaultFieldOffsets;
        if OtherUsingNextCopy then
          OtherFieldOffsets := TagData.ExtraFieldOffsets
        else
          OtherFieldOffsets := TagData.DefaultFieldOffsets;
      end
      else
      begin
        OwnFieldOffsets := TagData.DefaultFieldOffsets;
        OtherFieldOffsets := TagData.DefaultFieldOffsets;
      end;

      //Check field indexes in range.
      //However, do not need to have the same number of fields in each...
      LOfs := Length(OwnFieldOffsets);
      Assert(LOfs > 0);
      Assert(LOfs = Length(OtherFieldOffsets));
      result := 0;
      ff := 0;
      while (result = 0) and (ff < LOfs) do
      begin
        Assert(OwnFieldOffsets[ff] <= OwnFieldList.Count);
        Assert(OtherFieldOffsets[ff] <= OtherFieldList.Count);
        OwnField := OwnFieldList.Items[OwnFieldOffsets[ff]];
        OtherField := OtherFieldList.Items[OtherFieldOffsets[ff]];
        Assert(not (OwnField is TMemDeleteSentinel));
        Assert(not (OtherField is TMemDeleteSentinel));
{$IFOPT C+}
        result := CompareFields((OwnField as TMemFieldData).FDataRec,
                                (OtherField as TMemFieldData).FDataRec);
{$ELSE}
        result := CompareFields(TMemFieldData(OwnField).FDataRec,
                                TMemFieldData(OtherField).FDataRec);
{$ENDIF}
        Inc(ff);
      end;
    end;
  end;
end;

{ TMemDBIndexNodeSearchVal }

function TMemDBIndexNodeSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
//OwnItem ptr always NIL, use built in field. Own field always exists.
//Otherwise, as function above.
var
  PTag: PITagStruct;
  TagData: TMemDBITagData;

  OtherRow: TMemDBRow;
  OtherFieldList: TMemStreamableList;
  OtherField: TMemDBStreamable;
  OtherFieldOffsets: TFieldOffsets;
  OtherUsingNextCopy: boolean;
  ff:integer;
  LOfs: integer;

begin
  Assert(not Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OtherItem is TMemDbRow);
{$IFOPT C+}
  OtherRow := OtherItem as TMemDBRow;
{$ELSE}
  OtherRow := TMemDBRow(OtherItem);
{$ENDIF}
  PTag := PItagStruct(IndexTag);
  TagData := PTag.TagData;
  if TagData.MainIndexClass = micInternal then
  begin
    case TagData.InternalIndexClass of
    iicRowId:
      result:= CompareStr(FIdSearchVal, OtherRow.RowId);
    else
      Assert(false);
      result := 0;
    end;
  end
  else
  begin
    OtherFieldList := nil;
    if Assigned(OtherRow.ABData[abCurrent]) then
    begin
      Assert(not (OtherRow.ABData[abCurrent] is TMemDeleteSentinel));
{$IFOPT C+}
      OtherFieldList := OtherRow.ABData[abCurrent] as TMemStreamableList;
{$ELSE}
      OtherFieldList := TMemStreamableList(OtherRow.ABData[abCurrent]);
{$ENDIF}
    end;
    //Main index class is either permanent or temporary.
    //Case 2)
    //If temporary, then index newly added, or changing field number,
    //hence latest ab buffer if table structure has changed, else current
    //(possibly to add indexes without changing table structure).
    //Case 1)
    //If permanent, then using next if subclass is most recent.

    if (TagData.MainIndexClass = micTemporary)
      or (PTag.SubIndexClass = sicLatest) then
    begin
      //N.B yes, there is some cleverness on removing index nodes
      //for icCurrent, in that they're unchanged if a current index
      //is marked as temporary (layout change => no data value change).

      //Go for most recent A/B buffer.
      OtherUsingNextCopy := Assigned(OtherRow.ABData[abNext]);

      if OtherUsingNextCopy then
      begin
        if OtherRow.ABData[abNext] is TMemDeleteSentinel then
          OtherFieldList := nil
        else
        begin
{$IFOPT C+}
          OtherFieldList := OtherRow.ABData[abNext] as TMemStreamableList;
{$ELSE}
          OtherFieldList := TMemStreamableList(OtherRow.ABData[abNext]);
{$ENDIF}
        end;
      end;
    end
    else
      OtherUsingNextCopy := false;

    if not Assigned(OtherFieldList) then
      result := 1
    else //Both assigned.
    begin
      if (TagData.MainIndexClass = micTemporary) then
      begin
        //These field offsets look the wrong way around, but aren't.

        //ExtraFieldOffset is the "higher" number when removing fields from the
        //table. Not only does it correspond to the old index, (which you expect)

        //it *also* corresponds to the new index, when the delete sentinels
        //have not yet been removed.

        //Dynamic array assignment by reference - no dynamic array allocation
        //or copying here.
        if OtherUsingNextCopy then
          OtherFieldOffsets := TagData.ExtraFieldOffsets
        else
          OtherFieldOffsets := TagData.DefaultFieldOffsets;
      end
      else
        OtherFieldOffsets := TagData.DefaultFieldOffsets;

      //Check field indexes in range.
      //However, do not need to have the same number of fields in each...
      LOfs := Length(OtherFieldOffsets);
      Assert(LOfs > 0);
      Assert(LOfs = Length(FFieldSearchVals));
      result := 0;
      ff := 0;
      while (result = 0) and (ff < LOfs) do
      begin
        Assert(OtherFieldOffsets[ff] <= OtherFieldList.Count);

        OtherField := OtherFieldList.Items[OtherFieldOffsets[ff]];
        Assert(not (OtherField is TMemDeleteSentinel));
  {$IFOPT C+}
        result := CompareFields(FFieldSearchVals[ff],
                                (OtherField as TMemFieldData).FDataRec);
  {$ELSE}
        result := CompareFields(FFieldSearchVals[ff],
                                TMemFieldData(OtherField).FDataRec);
  {$ENDIF}
        Inc(ff);
      end;
    end;
  end;
end;

{ TMemDBRowLookasideIndexNode}

function TMemDBRowLookasideIndexNode.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  Tag: PMemDBMetaTags;
  OwnFieldList, OtherFieldList: TMemStreamableList;
  OwnRow, OtherRow: TMemDbRow;
  OwnField, OtherField: TMemDBStreamable;
  ff: integer;

begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OwnItem is TMemDBRow);
  Assert(OtherItem is TMemDBRow);
  Tag := PMemDBMetaTags(IndexTag);

  //First off, deal with NULL abData cases.
{$IFOPT C+}
  OwnRow := OwnItem as TMemDBRow;
  OtherRow := OtherItem as TMemDbRow;
{$ELSE}
  OwnRow := TMemDBRow(OwnItem);
  OtherRow := TMemDBRow(OtherItem);
{$ENDIF}

  if AssignedNotSentinel(OwnRow.ABData[Tag.abBuf]) then
  begin
{$IFOPT C+}
    OwnFieldList := OwnRow.ABData[Tag.abBuf] as TMemStreamableList;
{$ELSE}
    OwnFieldList := TMemStreamableList(OwnRow.ABData[Tag.abBuf]);
{$ENDIF}
  end
  else
    OwnFieldList := nil;

  if AssignedNotSentinel(OtherRow.ABData[Tag.abBuf]) then
  begin
{$IFOPT C+}
    OtherFieldList := OtherRow.ABData[Tag.abBuf] as TMemStreamableList;
{$ELSE}
    OtherFieldList := TMemStreamableList(OtherRow.ABData[Tag.abBuf]);
{$ENDIF}
  end
  else
    OtherFieldList := nil;

  if not (Assigned(OwnFieldList) or Assigned(OtherFieldList)) then
    result := 0
  else if not (Assigned(OwnFieldList) and Assigned(OtherFieldList)) then
  begin
    if Assigned(OwnFieldList) then
      result := 1
    else
      result := -1;
  end
  else //Both assigned.
  begin
      Assert(Length(Tag.FieldAbsIdxs) > 0);
      result := 0;
      ff := 0;
      while (result = 0) and (ff < Length(Tag.FieldAbsIdxs)) do
      begin
        Assert(Tag.FieldAbsIdxs[ff] <= OwnFieldList.Count);
        Assert(Tag.FieldAbsIdxs[ff] <= OtherFieldList.Count);
        OwnField := OwnFieldList.Items[Tag.FieldAbsIdxs[ff]];
        OtherField := OtherFieldList.Items[Tag.FieldAbsIdxs[ff]];
        Assert(not (OwnField is TMemDeleteSentinel));
        Assert(not (OtherField is TMemDeleteSentinel));
{$IFOPT C+}
        result := CompareFields((OwnField as TMemFieldData).FDataRec,
                                (OtherField as TMemFieldData).FDataRec);
{$ELSE}
        result := CompareFields(TMemFieldData(OwnField).FDataRec,
                                TMemFieldData(OtherField).FDataRec);
{$ENDIF}
        Inc(ff);
      end;
  end;
end;

{ TMemDBRowLookasideSearchVal }

function TMemDBRowLookasideSearchVal.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: TTagType; OtherNode: TIndexNode): integer;
var
  Tag: PMemDBMetaTags;
  OtherFieldList: TMemStreamableList;
  OtherRow: TMemDbRow;
  OtherField: TMemDBStreamable;
  ff: integer;

begin
  Assert(not Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OtherItem is TMemDBRow);
  Tag := PMemDBMetaTags(IndexTag);

  //First off, deal with NULL abData cases.
{$IFOPT C+}
  OtherRow := OtherItem as TMemDbRow;
{$ELSE}
  OtherRow := TMemDbRow(OtherItem);
{$ENDIF}

  if AssignedNotSentinel(OtherRow.ABData[Tag.abBuf]) then
  begin
{$IFOPT C+}
    OtherFieldList := OtherRow.ABData[Tag.abBuf] as TMemStreamableList
{$ELSE}
    OtherFieldList := TMemStreamableList(OtherRow.ABData[Tag.abBuf]);
{$ENDIF}
  end
  else
    OtherFieldList := nil;

  if not Assigned(OtherFieldList) then
      result := 1
  else //Both assigned.
  begin
      Assert(Length(Tag.FieldAbsIdxs) > 0);
      Assert(Length(Tag.FieldAbsIdxs) = Length(FFieldSearchVals));
      result := 0;
      ff := 0;
      while (result = 0) and (ff < Length(Tag.FieldAbsIdxs)) do
      begin
        Assert(Tag.FieldAbsIdxs[ff] <= OtherFieldList.Count);
        OtherField := OtherFieldList.Items[Tag.FieldAbsIdxs[ff]];
        Assert(not (OtherField is TMemDeleteSentinel));
{$IFOPT C+}
        result := CompareFields(FFieldSearchVals[ff],
                                (OtherField as TMemFieldData).FDataRec);
{$ELSE}
        result := CompareFields(FFieldSearchVals[ff],
                                TMemFieldData(OtherField).FDataRec);
{$ENDIF}
        Inc(ff);
      end;
  end;
end;


initialization
  MDBInternalIndexRowId := TMemDBITagData.Create;
  MDBInternalIndexRowId.InitInternal(iicRowId);
finalization
  MDBInternalIndexRowId.Free;
end.
