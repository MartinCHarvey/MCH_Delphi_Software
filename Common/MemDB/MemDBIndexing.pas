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
  IndexedStore, MemDBMisc;

{ Make indexing pretty strict with respect to field types / values,
  so that any errors in re-indexing are caught early }

{ NB. Encoding of index types into tags:

  63 - 40 : Reserved.
  39 - 32 : Field number deconfliction (multiple indices on same field).
  31, 30    : Index node class.
     29 - 0 : Class specific.
  +----------------------+
  |  |   |               |
  +----------------------+

  Class: 0: CurrentIndex.
         1: MostRecentIndex.
         2: TemporaryIndex (Field number or type changing) - todo disallow indexed field type changes?
         3: InternalIndex.

  Class 0, 1:
      29 - 15: Reserved.
      14 - 0: Field offset.
  +----------------------+
  |  |                   |
  +----------------------+

  Class 2:
     29, 28: Index class post commit. (Going from Temp to Current or Most Recent).
     25 - 15: Field offset post-commit. (Later "current" copy of data).
      14 - 0: Field offset pre-commit. (Most recent copy of data).
  +----------------------+
  |  |  |                 |
  +----------------------+

}

type
  TFieldOffset = 0.. $3FFF; //14 bits.
  TFieldDeconflict = 0..$FF; //8 Bits.
  TIndexNumber = type TFieldOffset;

const
  DeconflictShift = 32;
  DeconflictMask: UInt64 = UInt64(Ord(High(TFieldDeconflict))) shl DeconflictShift;
  ClassShift = 30;
  ClassMask: Uint64 = Uint64($3) shl ClassShift;
  EncapClassShift = 28;
  EncapClassMask:Uint64 = Uint64($3) shl EncapClassShift;
  LowFieldShift = 0;
  LowFieldMask:Uint64 = Ord(High(TFieldOffset)) shl LowFieldShift;
  HighFieldShift = 14;
  HighFieldMask:Uint64 = Ord(High(TFieldOffset)) shl HighFieldShift;

{$IFOPT C+}
  procedure DecodeIndexTag(Index: Int64;
                         var IndexClass, EncapIndexClass: TIndexClass;
                         var DefaultFieldOffset, ExtraFieldOffset: TFieldOffset;
                         var FieldDeconflict: TFieldDeconflict);

  procedure EncodeIndexTag(var Index: Int64;
                         IndexClass, EncapIndexClass: TIndexClass;
                         DefaultFieldOffset, ExtraFieldOffset: TFieldOffset;
                         FieldDeconflict: TFieldDeconflict);
{$ELSE}
  procedure DecodeIndexTag(Index: Int64;
                         var IndexClass, EncapIndexClass: TIndexClass;
                         var DefaultFieldOffset, ExtraFieldOffset: TFieldOffset;
                         var FieldDeconflict: TFieldDeconflict); inline;

  procedure EncodeIndexTag(var Index: Int64;
                         IndexClass, EncapIndexClass: TIndexClass;
                         DefaultFieldOffset, ExtraFieldOffset: TFieldOffset;
                         FieldDeconflict: TFieldDeconflict); inline;
{$ENDIF}

type
  TMemDBIndexNode = class(TDuplicateValIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  end;

  TMemDBIndexNodeSearchVal = class(TMemDbIndexNode)
  private
    FPointerSearchVal: pointer;
    FIdSearchVal: string;
    FFieldSearchVal: TMemDbFieldDataRec;
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  public
    property PointerSearchVal: pointer read FPointerSearchVal write FPointerSearchVal;
    property IdSearchVal: string read FIdSearchVal write FIdSearchVal;
    property FieldSearchVal: TMemDbFieldDataRec read FFieldSearchVal write FFieldSearchVal;
  end;

  TMemDBFieldLookasideIndexNode = class(TIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  end;

  TMemDBFieldLookasideSearchVal = class(TMemDBFieldLookasideIndexNode)
  private
    FFieldSearchVal: TMemDbFieldDataRec;
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer; override;
  public
    property FieldSearchVal: TMemDbFieldDataRec read FFieldSearchVal write FFieldSearchVal;
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
  MDBInternalIndexPtr,
  MDBInternalIndexRowId,
  MDBInternalIndexCurrCopy,
  MDBInternalIndexNextCopy:Int64;

implementation

uses
  MemDBBuffered, SysUtils, MemDbStreamable
{$IFOPT C+}
{$ELSE}
  , Classes
{$ENDIF}

  ;

{ Misc functions }
procedure DecodeIndexTag(Index: Int64;
                       var IndexClass, EncapIndexClass: TIndexClass;
                       var DefaultFieldOffset, ExtraFieldOffset: TFieldOffset;
                       var FieldDeconflict: TFieldDeconflict);
var
  UnsignedIndex: UInt64;
begin
  UnsignedIndex := UInt64(Index);
  FieldDeconflict := TFieldDeconflict((UnsignedIndex and DeconflictMask) shr DeconflictShift);
  IndexClass := TIndexClass((UnsignedIndex and ClassMask) shr ClassShift);
  if IndexClass = icTemporary then
  begin
    EncapIndexClass := TIndexClass((UnsignedIndex and EncapClassMask) shr EncapClassShift);
    DefaultFieldOffset := (UnsignedIndex and LowFieldMask) shr LowFieldShift;
    ExtraFieldOffset := (UnsignedIndex and HighFieldMask) shr HighFieldShift;
  end
  else
  begin
    EncapIndexClass := Low(EncapIndexClass);
    DefaultFieldOffset := (UnsignedIndex and LowFieldMask) shr LowFieldShift;
    ExtraFieldOffset := 0;
  end;
end;

procedure EncodeIndexTag(var Index: Int64;
                       IndexClass, EncapIndexClass: TIndexClass;
                       DefaultFieldOffset, ExtraFieldOffset: TFieldOffset;
                       FieldDeconflict: TFieldDeconflict);
var
  UnsignedIndex: UInt64;
{$IFOPT C+}
 DbgIndexClass, DbgEncapIndexClass: TIndexClass;
 DbgDefaultFieldOffset, DbgExtraFieldOffset: TFieldOffset;
 DbgFieldDeconflict: TFieldDeconflict;
{$ENDIF}
begin
  UnsignedIndex := (UInt64(Ord(IndexClass)) shl ClassShift)
                   or
                   (Uint64(Ord(FieldDeconflict)) shl DeconflictShift);
  if IndexClass = icTemporary then
  begin
    UnsignedIndex := UnsignedIndex or
      (UInt64(Ord(EncapIndexClass)) shl EncapClassShift) or
      (UInt64(Ord(DefaultFieldOffset)) shl LowFieldShift) or
      (UInt64(Ord(ExtraFieldOffset)) shl HighFieldShift);
  end
  else
  begin
    UnsignedIndex := UnsignedIndex or
      (UInt64(Ord(DefaultFieldOffset)) shl LowFieldShift)
  end;
  Index := Int64(UnsignedIndex);
{$IFOPT C+}
  DecodeIndexTag(Index, DbgIndexClass, DbgEncapIndexClass,
                 DbgDefaultFieldOffset, DbgExtraFieldOffset, DbgFieldDeconflict);
  Assert(DbgIndexClass = IndexClass);
  Assert(DbgDefaultFieldOffset = DefaultFieldOffset);
  Assert(DbgFieldDeconflict = FieldDeconflict);
  if IndexClass = icTemporary then
  begin
    Assert(DbgEncapIndexClass = EncapIndexClass);
    Assert(DbgExtraFieldOffset = ExtraFieldOffset);
  end;
{$ENDIF}
end;

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

function TMemDBIndexNode.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
var
  OwnRow, OtherRow: TMemDBRow;
  OwnFieldList, OtherFieldList: TMemStreamableList;
  OwnField, OtherField: TMemDBStreamable;
  IndexClass, EncapIndexClass: TIndexClass;
  DefaultFieldOffset, ExtraFieldOffset: TFieldOffset;
  OwnFieldOffset, OtherFieldOffset: TFieldOffset;
  UsingNextCopy, OtherUsingNextCopy: boolean;
  FieldDeconflict: TFieldDeconflict;

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
  DecodeIndexTag(IndexTag, IndexClass, EncapIndexClass, DefaultFieldOffset, ExtraFieldOffset, FieldDeconflict);
  if IndexClass = icInternal then
  begin
    if IndexTag = MDBInternalIndexPtr then
      result := ComparePointers(OwnRow, OtherRow)
    else if IndexTag = MDBInternalIndexRowId then
      result:= CompareStr(OwnRow.RowId, OtherRow.RowId)
    else if IndexTag = MDBInternalIndexCurrCopy then
      result := ComparePointers(OwnRow.ABData[abCurrent], OtherRow.ABData[abCurrent])
    else if IndexTag = MDBInternalIndexNextCopy then
      result := ComparePointers(OwnRow.ABData[abNext], OtherRow.ABData[abNext])
    else
    begin
      Assert(false);
      result := 0;
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
    //(icCurrent, icMostRecent, icTemporary, icInternal)

    if IndexClass in [icMostRecent, icTemporary] then
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
      if IndexClass = icTemporary then
      begin
        if UsingNextCopy then
          OwnFieldOffset := ExtraFieldOffset
        else
          OwnFieldOffset := DefaultFieldOffset;
        if OtherUsingNextCopy then
          OtherFieldOffset := ExtraFieldOffset
        else
          OtherFieldOffset := DefaultFieldOffset;
      end
      else
      begin
        OwnFieldOffset := DefaultFieldOffset;
        OtherFieldOffset := DefaultFieldOffset;
      end;

      //Check field indexes in range.
      //However, do not need to have the same number of fields in each...
      Assert(OwnFieldOffset <= OwnFieldList.Count);
      Assert(OtherFieldOffset <= OtherFieldList.Count);
      OwnField := OwnFieldList.Items[OwnFieldOffset];
      OtherField := OtherFieldList.Items[OtherFieldOffset];
      Assert(not (OwnField is TMemDeleteSentinel));
      Assert(not (OtherField is TMemDeleteSentinel));
{$IFOPT C+}
      result := CompareFields((OwnField as TMemFieldData).FDataRec,
                              (OtherField as TMemFieldData).FDataRec);
{$ELSE}
      result := CompareFields(TMemFieldData(OwnField).FDataRec,
                              TMemFieldData(OtherField).FDataRec);
{$ENDIF}
    end;
  end;
end;

{ TMemDBIndexNodeSearchVal }

function TMemDBIndexNodeSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
//OwnItem ptr always NIL, use built in field. Own field always exists.
//Otherwise, as function above.
var
  OtherRow: TMemDBRow;
  OtherFieldList: TMemStreamableList;
  OtherField: TMemDBStreamable;
  IndexClass, EncapIndexClass: TIndexClass;
  DefaultFieldOffset, ExtraFieldOffset: TFieldOffset;
  FieldOffset: TFieldOffset;
  OtherUsingNextCopy: boolean;
  FieldDeconflict: TFieldDeconflict;

begin
  Assert(not Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OtherItem is TMemDbRow);
{$IFOPT C+}
  OtherRow := OtherItem as TMemDBRow;
{$ELSE}
  OtherRow := TMemDBRow(OtherItem);
{$ENDIF}
  DecodeIndexTag(IndexTag, IndexClass, EncapIndexClass, DefaultFieldOffset, ExtraFieldOffset, FieldDeconflict);
  if IndexClass = icInternal then
  begin
    if IndexTag = MDBInternalIndexPtr then
      result := ComparePointers(FPointerSearchVal, OtherRow)
    else if IndexTag = MDBInternalIndexRowId then
      result:= CompareStr(FIdSearchVal, OtherRow.RowId)
    else if IndexTag = MDBInternalIndexCurrCopy then
      result := ComparePointers(FPointerSearchVal, OtherRow.ABData[abCurrent])
    else if IndexTag = MDBInternalIndexNextCopy then
      result := ComparePointers(FPointerSearchVal, OtherRow.ABData[abNext])
    else
    begin
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

    if IndexClass in [icMostRecent, icTemporary] then
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
      if IndexClass = icTemporary then
      begin
        if OtherUsingNextCopy then
          FieldOffset := ExtraFieldOffset
        else
          FieldOffset := DefaultFieldOffset;
      end
      else
        FieldOffset := DefaultFieldOffset;

      //Check field indexes in range.
      //However, do not need to have the same number of fields in each...
      Assert(FieldOffset <= OtherFieldList.Count);
      OtherField := OtherFieldList.Items[FieldOffset];
      Assert(not (OtherField is TMemDeleteSentinel));
{$IFOPT C+}
      result := CompareFields(FFieldSearchVal,
                              (OtherField as TMemFieldData).FDataRec);
{$ELSE}
      result := CompareFields(FFieldSearchVal,
                              TMemFieldData(OtherField).FDataRec);
{$ENDIF}
    end;
  end;
end;

{ TMemDBFieldLookasideIndexNode}

function TMemDBFieldLookasideIndexNode.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OwnItem is TMemFieldData);
  Assert(OtherItem is TMemFieldData);
{$IFOPT C+}
  result := CompareFields((OwnItem as TMemFieldData).FDataRec,
                          (OtherItem as TMemFieldData).FDataRec);
{$ELSE}
  result := CompareFields(TMemFieldData(OwnItem).FDataRec,
                          TMemFieldData(OtherItem).FDataRec);
{$ENDIF}
end;

{ TMemDBFieldLookasideSearchVal }

function TMemDBFieldLookasideSearchVal.CompareItems(OwnItem: TObject; OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
begin
  Assert(not Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  Assert(OtherItem is TMemFieldData);
{$IFOPT C+}
  result := CompareFields(FFieldSearchVal,
                          (OtherItem as TMemFieldData).FDataRec);
{$ELSE}
  result := CompareFields(FFieldSearchVal,
                          TMemFieldData(OtherItem).FDataRec);
{$ENDIF}
end;

initialization
  EncodeIndexTag(MDBInternalIndexPtr, icInternal, IcInternal, 0, 0, 0);
  EncodeIndexTag(MDBInternalIndexRowId, icInternal, IcInternal, 1, 0, 0);
  EncodeIndexTag(MDBInternalIndexCurrCopy, icInternal, IcInternal, 2, 0, 0);
  EncodeIndexTag(MDBInternalIndexNextCopy, icInternal, IcInternal, 3, 0, 0);
end.
