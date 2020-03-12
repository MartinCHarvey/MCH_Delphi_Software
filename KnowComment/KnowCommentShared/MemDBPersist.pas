unit MemDBPersist;
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

uses MemDB, MemDBApi, DBPersist, DataObjects, DBGeneric, MemDBInit, MemDbMisc;

//TODO - There's a whole load of string/guid fixing to do here.

type
  TMDBTransactionContext = record
    S: TMemDBSession;
    T: TMemDBTransaction;
    DBAPI: TMemAPIDatabase;
  end;

  TMDBItemPersister = class(TItemPersister)
  protected
    //Nest setup / commit requests.
    function TransactionSetup(var Ctx: TObject;
                              var TxCtxt: TMDBTransactionContext;
                              Mode: TMDBAccessMode;
                              Sync: TMDBSyncMode = amLazyWrite;
                              Iso: TMDBIsolationLevel= ilDirtyRead): TMemAPIDatabase;
    //Commit / rollback N.B.
    //Returns whether the commit/rollback has been deferred, because we are nested.
    //Might want/need to re-raise an exception?
    //Careful with subsequent use of DBAPI in exception cases.
    function TransactionCommit(var TxCtxt: TMDBTransactionContext): boolean;
    function TransactionRollback(var TxCtxt: TMDBTransactionContext): boolean;

    function AccessTblData(const TxCtxt: TMDBTransactionContext; Lvl: TKListLevel): TMemAPITableData;
    function GetOwnerIndexName(Lvl: TKListLevel): string;
    function GetOwnerFieldName(Lvl: TKListLevel): string;
    function GetParentIndexName(Lvl: TKListLevel): string;
    function GetParentFieldName(Lvl: TKListLevel): string;

    function GetParentDB: TMemKDB;

    function FindItemByAnyIDUser(Table: TMemAPITableData; Blocks: TVarBlockArray; const Guid: TGuid): boolean;
    function FindItemByAnyIDMedia(Table: TMemAPITableData; Blocks: TVarBlockArray; const Guid: TGuid): boolean;
    function FindItemByAnyIDComment(Table: TMemAPITableData; Blocks: TVarBlockArray; const Guid: TGuid): boolean;

    function FindItemByAnyIDLL(Level: TKListLevel; Table: TMemAPITableData; Blocks: TVarBlockArray; const Guid: TGuid): boolean;
    function FindItemByAnyID(Table: TMemAPITableData; Blocks: TVarBlockArray; const Guid: TGuid): boolean;

    function CreateItemFromTableCursor(Table: TMemAPITableData): TKKeyedObject; virtual; abstract;
    procedure UpdateTableCursorFromItem(Table: TMemAPITableData; Item: TKKeyedObject;
      const OwnerGUID: TGuid);
    procedure CreateTableRecordFromItem(Table: TMemAPITableData; Item: TKKeyedObject;
      const OwnerGUID: TGuid);
    procedure UpdateCommon(Table: TMemAPITableData; Item: TKKeyedObject; const OwnerGUID: TGuid);
      virtual; abstract;

    function GetSingleItemMTB(Ctx: TObject; Blocks: TVarBlockArray; var Item: TKKeyedObject): boolean; override;
    function GetSingleItemMTG(Ctx: TObject; const ItemKey: TGuid; var Item: TKKeyedObject): boolean; override;
    function GetItemListForParentMT(Ctx: TObject; const ParentKey: TGuid; var ItemList: TKItemList): boolean; override;

    function GetItemTreeMT(Ctx: TObject; Blocks: TVarBlockArray;
                           const ItemKey: TGuid;
                           UseBlocks: boolean;
                           var Item: TKKeyedObject): boolean;

    function GetItemTreeMTB(Ctx: TObject; Blocks: TVarBlockArray; var Item: TKKeyedObject): boolean; override;
    function GetItemTreeMTG(Ctx: TObject; const ItemKey: TGuid; var Item: TKKeyedObject): boolean; override;

    function CustomQueryMT(Ctx: TObject; const Key: TGuid; KeyMeaning: TCustomKeyMeaning;
      AdditionalFilter: TAdditionalFilter; var TopLevelList: TKItemList): boolean; override;

    function ChangeSingleItemMT(Ctx: TObject; const OwnerKey: TGuid; Item: TKKeyedObject; ActionSet: TPersistActionSet)
      : boolean; override;
    function ChangeItemListMT(Ctx: TObject; const ParentKey: TGuid; ItemList: TKItemList; ActionSet: TPersistActionSet)
      : boolean; override;
    function ChangeItemListForOtherLevelMT(Ctx: TObject; const ParentKey: TGuid; ParentLevel: TKListLevel;
      ItemList: TKItemList; ActionSet: TPersistActionSet): boolean; override;

    function ExpireItemListMT(Ctx: TObject; const ParentKey: TGuid; ExpireBefore: TDateTime;
      ExpiryType: TDBExpiryType): boolean; override;

    function PruneUnusedMT(Ctx: TObject): boolean; override;
  public
    property ParentDB: TMemKDB read GetParentDB;
  end;

  TMDBUserProfilePersister = class(TMDBItemPersister)
  protected
    function CreateItemFromTableCursor(Table: TMemAPITableData): TKKeyedObject; override;
    procedure UpdateCommon(Table: TMemAPITableData; Item: TKKeyedObject; const OwnerGUID: TGuid); override;
    function GetListLevel: TKListLevel; override;
  end;

  TMDBMediaItemPersister = class(TMDBItemPersister)
  protected
    function CreateItemFromTableCursor(Table: TMemAPITableData): TKKeyedObject; override;
    procedure UpdateCommon(Table: TMemAPITableData; Item: TKKeyedObject; const OwnerGUID: TGuid); override;
    function GetListLevel: TKListLevel; override;
  end;

  TMDBCommentItemPersister = class(TMDBItemPersister)
  protected
    function CreateItemFromTableCursor(Table: TMemAPITableData): TKKeyedObject; override;
    procedure UpdateCommon(Table: TMemAPITableData; Item: TKKeyedObject; const OwnerGUID: TGuid); override;
    function GetListLevel: TKListLevel; override;
  end;

implementation

uses DBSchemaStrs, SysUtils, GlobalLog;

const
  S_NO_KEY_SEARCH_INFO = 'No appropriate key information provided when doing a search.';
  S_FIELD_OUT_OF_RANGE = 'Read a field with value out of range for enum.';
  S_READ_RECORD_WITH_NULL_KEY = 'Read a record with NULL key';
  S_ITEM_KEYS_INCONSISTENT = 'GUID keys for item inconsistent';
  S_WRITING_NULL_KEY = 'Trying to write NULL GUID key to database.';
  S_NULL_OWNER_GUID = 'Supplied owner GUID NULL';
  S_CUSTOM_QUERY_BAD_PARAMS = 'Bad parameters supplied to custom query.';
  S_CUSTOM_QUERY_INTERNAL_ERROR = 'Custom query internal error';
  S_LEVEL_INTERNAL_ERROR = 'Internal error (list levels).';
  S_EXPIRY_INTERNAL = 'Internal error expiring out of date items.';

{ TMDBItemPersister }

function TMDBItemPersister.FindItemByAnyIDUser(Table: TMemAPITableData; Blocks: TVarBlockArray;
  const Guid: TGuid): boolean;
var
  UBType: TKSiteType;
  tried: boolean;
  SearchData: TMemDbFieldDataRec;
begin
  result := false;
  tried := false;
  // First try to search by GUID, then by uid, then username.
  // Criteria are that if an addressing or ID value is not null or empty,
  // then it should be a valid way of finding the user.


  if Guid <> TGuid.Empty then
  begin
    SearchData.FieldType := ftGuid;
    tried := true;
    SearchData.gVal := Guid;
    result := Table.FindByIndex(S_INDEX_USER_TABLE_PRIMARY, SearchData);
  end
  else
  begin
    SearchData.FieldType := ftUnicodeString;
    Assert(Blocks.llTag = klUserList);
    for UBType := Low(UBType) to High(UBType) do
    begin
      if Assigned(Blocks.UBlocks[UBType]) and Blocks.UBlocks[UBType].Valid then
      begin
        if Length(Blocks.UBlocks[UBType].UserId) > 0 then
        begin
          tried := true;
          SearchData.sVal := Blocks.UBlocks[UBType].UserId;
          result := Table.FindByIndex(UserBlockIndexNames[UBType, tuxUserId],
                                      SearchData);
          if result then
            break;
        end
        else if Length(Blocks.UBlocks[UBType].Username) > 0 then
        begin
          tried := true;
          SearchData.sVal := Blocks.UBlocks[UBType].Username;
          result := Table.FindByIndex(UserBlockIndexNames[UBType, tuxUserName],
                                      SearchData);
          if result then
            break;
        end;
      end;
    end;
  end;
  if not tried then
    raise EDBPErsistError.Create(S_NO_KEY_SEARCH_INFO);
end;

function TMDBItemPersister.FindItemByAnyIDMedia(Table: TMemAPITableData; Blocks: TVarBlockArray;
  const Guid: TGuid): boolean;
var
  MBType: TKSiteType;
  SearchField: string;
  tried: boolean;
  SearchData: TMemDbFieldDataRec;
begin
  result := false;
  tried := false;
  // First try to search by GUID, then by uid, then username.
  // Criteria are that if an addressing or ID value is not null or empty,
  // then it should be a valid way of finding the user.


  if Guid <> TGuid.Empty then
  begin
    SearchData.FieldType := ftGuid;
    tried := true;
    SearchData.gVal := Guid;
    result := Table.FindByIndex(S_INDEX_MEDIA_TABLE_PRIMARY, SearchData);
  end
  else
  begin
    SearchData.FieldType := ftUnicodeString;
    Assert(Blocks.llTag = klMediaList);
    for MBType := Low(MBType) to High(MBType) do
    begin
      if Assigned(Blocks.MBlocks[MBType]) and Blocks.MBlocks[MBType].Valid then
      begin
        if Length(Blocks.MBlocks[MBType].MediaID) > 0 then
        begin
          SearchField := MediaBlockIndexNames[MBType, tmxMediaId];
          tried := true;
          SearchData.sVal := Blocks.MBlocks[MBType].MediaID;
          result := Table.FindByIndex(MediaBlockIndexNames[MBType, tmxMediaId], SearchData);
          if result then
            break;
        end
        else if Length(Blocks.MBlocks[MBType].MediaCode) > 0 then
        begin
          SearchField := MediaBlockIndexNames[MBType, tmxMediaCode];
          tried := true;
          SearchData.sVal := Blocks.MBlocks[MBType].MediaID;
          result := Table.FindByIndex(MediaBlockIndexNames[MBType, tmxMediaCode], SearchData);
          if result then
            break;
        end;
      end;
    end;
  end;
  if not tried then
    raise EDBPErsistError.Create(S_NO_KEY_SEARCH_INFO);
end;

function TMDBItemPersister.FindItemByAnyIDComment(Table: TMemAPITableData; Blocks: TVarBlockArray;
  const Guid: TGuid): boolean;
var
  CBType: TKSiteType;
  tried: boolean;
  SearchData: TMemDbFieldDataRec;
begin
  result := false;
  tried := false;
  // First try to search by GUID, then by uid
  // Criteria are that if an addressing or ID value is not null or empty,
  // then it should be a valid way of finding the user.

  if Guid <> TGuid.Empty then
  begin
    SearchData.FieldType := ftGuid;
    tried := true;
    SearchData.gVal := Guid;
    result := Table.FindByIndex(S_INDEX_COMMENT_TABLE_PRIMARY, SearchData);
  end
  else
  begin
    SearchData.FieldType := ftUnicodeString;
    Assert(Blocks.llTag = klCommentList);
    for CBType := Low(CBType) to High(CBType) do
    begin
      if Assigned(Blocks.CBlocks[CBType]) and Blocks.CBlocks[CBType].Valid then
      begin
        if Length(Blocks.CBlocks[CBType].CommentId) > 0 then
        begin
          tried := true;
          SearchData.sVal := Blocks.CBlocks[CBType].CommentId;
          result := Table.FindByIndex(CommentBlockIndexNames
            [CBType, tcxCommentId], SearchData);
          if result then
            break;
        end
      end;
    end;
  end;
  if not tried then
    raise EDBPErsistError.Create(S_NO_KEY_SEARCH_INFO);
end;

function TMDBItemPersister.FindItemByAnyIDLL(Level: TKListLevel; Table: TMemAPITableData; Blocks: TVarBlockArray; const Guid: TGuid): boolean;
begin
  case Level of
    klUserList: result := FindItemByAnyIDUser(Table, Blocks, Guid);
    klMediaList: result := FindItemByAnyIDMedia(Table, Blocks, Guid);
    klCommentList: result := FindItemByAnyIDComment(Table, Blocks, Guid);
  else
    raise EDBPersistError.Create(S_LEVEL_INTERNAL_ERROR);
  end;
end;

function TMDBItemPersister.FindItemByAnyID(Table: TMemAPITableData; Blocks: TVarBlockArray; const Guid: TGuid): boolean;
begin
  result := FindItemByAnyIDLL(ListLevel, Table, Blocks, Guid);
end;

procedure TMDBItemPersister.UpdateTableCursorFromItem(Table: TMemAPITableData; Item: TKKeyedObject;
  const OwnerGUID: TGuid);
begin
  with Table do
  begin
    //No need, no separate browse mode, fields loaded auto.
    //Edit;
    try
      UpdateCommon(Table, Item, OwnerGUID);
    finally
      Post;
    end;
  end;
end;

procedure TMDBItemPersister.CreateTableRecordFromItem(Table: TMemAPITableData; Item: TKKeyedObject;
  const OwnerGUID: TGuid);
begin
  with Table do
  begin
    Append;
    try
      UpdateCommon(Table, Item, OwnerGUID);
    finally
      Post;
    end;
  end;
end;

//Nest setup / commit requests.
function TMDBItemPersister.TransactionSetup(var Ctx: TObject;
                                            var TxCtxt: TMDBTransactionContext;
                                            Mode: TMDBAccessMode;
                                            Sync: TMDBSyncMode;
                                            Iso: TMDBIsolationLevel): TMemAPIDatabase;
begin
  if Assigned(Ctx) then
  begin
    //Transaction context already exists, copy up call stack.
    with TxCtxt do
    begin
      S := nil;
      T := Ctx as TMemDBTransaction;
      DBAPI := T.GetAPI;
      Assert(Mode <= T.Mode);
      Assert(Sync <= T.Sync);
      if Mode > amRead then
        Assert(Iso = T.Isolation);
    end;
    result := TxCtxt.DBAPI;
  end
  else
  begin
    with TxCtxt do
    begin
      S := ParentDB.MemDB.StartSession;
      T := S.StartTransaction(Mode, Sync, Iso);
      DBAPI := T.GetAPI;
      result := DBAPI;
      Ctx := T;
    end;
  end;
end;

//Returns whether the commit has been deferred, because we are nested.
//Might want/need to re-raise an exception?
//Careful with subsequent use of DBAPI in exception cases.
function TMDBItemPersister.TransactionCommit(var TxCtxt: TMDBTransactionContext): boolean;
begin
  TxCtxt.DBAPI.Free;
  TxCtxt.DBAPI := nil;
  result := not Assigned(TxCtxt.S);
  if not result then
  begin
    TxCtxt.T.CommitAndFree;
    TxCtxt.T := nil;
    TxCtxt.S.Free;
    TxCtxt.S := nil;
  end;
end;

//Returns whether the rollback has been deferred, because we are nested.
//Might want/need to re-raise an exception?
//Careful with subsequent use of DBAPI in exception cases.
function TMDBItemPersister.TransactionRollback(var TxCtxt: TMDBTransactionContext): boolean;
begin
  TxCtxt.DBAPI.Free;
  TxCtxt.DBAPI := nil;
  result := not Assigned(TxCtxt.S);
  if not result then
  begin
    TxCtxt.T.RollbackAndFree;
    TxCtxt.T := nil;
    TxCtxt.S.Free;
    TxCtxt.S := nil;
  end;
end;

//TODO - Remove Lvl param is always called at current level.
function TMDBItemPersister.AccessTblData(const TxCtxt: TMDBTransactionContext; Lvl: TKListLevel): TMemAPITableData;
var
  TblName: string;
  TblHandle: TMemDBHandle;
begin
  case Lvl of
    klUserList: TblName := S_USER_TABLE;
    klMediaList: TblName := S_MEDIA_TABLE;
    klCommentList: TblName := S_COMMENT_TABLE;
  else
    raise EDBPersistError.Create(S_LEVEL_INTERNAL_ERROR);
  end;
  TblHandle := TxCtxt.DBAPI.OpenTableOrKey(TblName);
  result := TxCtxt.DBAPI.GetApiObjectFromHandle(TblHandle, APITableData) as TMemAPITableData;
end;

//TODO - Remove Lvl param if always called at current level.
function TMDBItemPersister.GetOwnerIndexName(Lvl: TKListLevel):string;
begin
  case Lvl of
    klUserList: result := '';
    klMediaList: result := S_INDEX_MEDIA_OWNER_KEY;
    klCommentList: result := S_INDEX_COMMENT_OWNER_KEY;
  else
   raise EDBPersistError.Create(S_LEVEL_INTERNAL_ERROR);
  end;
end;

//TODO - Remove Lvl param if always called at current level.
function TMDBItemPersister.GetOwnerFieldName(Lvl: TKListLevel):string;
begin
  case Lvl of
    klUserList: result := '';
    klMediaList: result := S_MEDIA_OWNER_KEY;
    klCommentList: result := S_COMMENT_OWNER_KEY;
  else
    raise EDBPersistError.Create(S_LEVEL_INTERNAL_ERROR);
  end;
end;

//TODO - Remove Lvl param if always called at current level.
function TMDBItemPersister.GetParentIndexName(Lvl: TKListLevel):string;
begin
  case Lvl of
    klUserList: result := '';
    klMediaList: result := S_INDEX_MEDIA_OWNER_KEY;
    klCommentList: result := S_INDEX_COMMENT_MEDIA_KEY;
  else
    raise EDBPersistError.Create(S_LEVEL_INTERNAL_ERROR);
  end;
end;

function TMDBItemPersister.GetParentFieldName(Lvl: TKListLevel):string;
begin
  case Lvl of
    klUserList: result := '';
    klMediaList: result := S_MEDIA_OWNER_KEY;
    klCommentList: result := S_COMMENT_MEDIA_KEY;
  else
    raise EDBPersistError.Create(S_LEVEL_INTERNAL_ERROR);
  end;
end;

function TMDBItemPersister.GetSingleItemMTB(Ctx: TObject; Blocks: TVarBlockArray;
  var Item: TKKeyedObject): boolean;
var
  TxCtxt: TMDBTransactionContext;
  Table: TMemAPITableData;
begin
  Item := nil;
  TransactionSetup(Ctx, TxCtxt, amRead);
  try
    Table := AccessTblData(TxCtxt, ListLevel);
    try
      result := FindItemByAnyID(Table, Blocks, TGuid.Empty);
    if result then
      Item := CreateItemFromTableCursor(Table);
    finally
      Table.Free;
    end;
    TransactionCommit(TxCtxt);
  except
    TransactionRollback(TxCtxt);
    raise;
  end;
end;

function TMDBItemPersister.GetSingleItemMTG(Ctx: TObject; const ItemKey: TGuid; var Item: TKKeyedObject): boolean;
var
  TxCtxt: TMDBTransactionContext;
  Table: TMemAPITableData;
  Blocks: TVarBlockArray;
begin
  result := false;
  Item := nil;
  if ItemKey = TGuid.Empty then
    exit;
  FillChar(Blocks, sizeof(Blocks), 0);
  Blocks.llTag := GetListLevel;
  TransactionSetup(Ctx, TxCtxt, amRead);
  try
    Table := AccessTblData(TxCtxt, ListLevel);
    try
      result := FindItemByAnyID(Table, Blocks, ItemKey);
      if result then
        Item := CreateItemFromTableCursor(Table);
    finally
      Table.Free;
    end;
    TransactionCommit(TxCtxt);
  except
    TransactionRollback(TxCtxt);
    raise;
  end;
end;

function TMDBItemPersister.GetItemTreeMT(Ctx: TObject; Blocks: TVarBlockArray;
                                                       const ItemKey: TGuid;
                                                       UseBlocks: boolean;
                                                       var Item: TKKeyedObject): boolean;
var
  TxCtxt: TMDBTransactionContext;
  ItemCalledList, ResItemList: TKItemList;
  ChildItem, ReadChildTree: TKKeyedObject;
  ChildPersister: TMDBItemPersister;
begin
  Item := nil;
  TransactionSetup(Ctx, TxCtxt, amRead);
  try
    if UseBlocks then
      result := GetSingleItemMTB(Ctx, Blocks, Item)
    else
      result := GetSingleItemMTG(Ctx, ItemKey, Item);
    if result then
    begin
      if ListLevel < High(TKListLevel) then
      begin
        ChildPersister := ParentDB.CreateItemPersister(Succ(ListLevel)) as TMDBItemPersister;
        try
          result := ChildPersister.GetItemListForParentMT(Ctx, Item.Key, ItemCalledList);
          if result then
          begin
            try
              case ListLevel of
                klUserList: ResItemList := (Item as TKUserProfile).Media;
                klMediaList: ResItemList := (Item as TKMediaItem).Comments;
              else
                raise EDBPersistError.Create(S_LEVEL_INTERNAL_ERROR);
              end;
              ResItemList.MergeWithNewer(ItemCalledList);
              if ListLevel < Pred(High(TKListLevel)) then
              begin
                  //Skip extra merge-step with single items already got...
                  ChildItem := ResItemList.AdjacentBySortVal(katFirst, ksvPointer, nil);
                  while Assigned(ChildItem) and result do
                  begin
                    result := ChildPersister.GetItemTreeMTG(Ctx, ChildItem.Key, ReadChildTree);
                    if result then
                    begin
                      try
                        ChildItem.MergeWithNewer(ReadChildTree);
                      finally
                        ReadChildTree.Free; //Merged across items not freed.
                      end;
                      ChildItem := ResItemList.AdjacentBySortVal(katNext, ksvPointer, ChildItem);
                    end;
                  end;
              end;
            finally
              ItemCalledList.Free;
            end;
          end;
        finally
          ChildPersister.Free;
        end;
      end;
    end;
    TransactionCommit(TxCtxt);
  except
    TransactionRollback(TxCtxt);
    raise;
  end;
end;

function TMDBItemPersister.GetItemTreeMTB(Ctx: TObject; Blocks: TVarBlockArray; var Item: TKKeyedObject): boolean;
begin
  result := GetItemTreeMT(Ctx, Blocks, TGuid.Empty, true, Item);
end;

function TMDBItemPersister.GetItemTreeMTG(Ctx: TObject; const ItemKey: TGuid; var Item: TKKeyedObject): boolean;
var
  Blocks: TVarBlockArray;
begin
  FillChar(Blocks, sizeof(Blocks), 0);
  Blocks.llTag := GetListLevel;
  result := GetItemTreeMT(Ctx, Blocks, ItemKey, false, Item);
end;

function TMDBItemPersister.GetItemListForParentMT(Ctx: TObject; const ParentKey: TGuid;
  var ItemList: TKItemList): boolean;
var
  TxCtxt: TMDBTransactionContext;
  Table: TMemAPITableData;
  Item: TKKeyedObject;
  OK: boolean;
  SearchData, Data: TMemDbFieldDataRec;
begin
  ItemList := GetListClass.Create as TKIdList;
  TransactionSetup(Ctx, TxCtxt, amRead);
  try
    Table := AccessTblData(TxCtxt, ListLevel);
    try
      Assert((ParentKey <> TGuid.Empty)
        = (Length(GetParentIndexName(ListLevel)) > 0));
      if Length(GetParentIndexName(ListLevel)) > 0 then
      begin
        SearchData.FieldType := ftGuid;
        SearchData.gVal := ParentKey;
        OK := Table.FindEdgeByIndex(ptFirst, GetParentIndexName(ListLevel), SearchData)
      end
      else
        OK := Table.Locate(ptFirst, GetParentIndexName(ListLevel));
      while OK do
      begin
        Item := CreateItemFromTableCursor(Table);
        OK := ItemList.Add(Item);
        Assert(OK);

        OK := Table.Locate(ptNext, GetParentIndexName(ListLevel));
        if Length(GetParentIndexName(ListLevel)) > 0 then
        begin
          if OK then
            Table.ReadField(GetParentFieldName(ListLevel), Data);
          OK := OK and DataRecsSame(Data, SearchData)
        end;
      end;
      result := true;
    finally
      Table.Free;
    end;
    TransactionCommit(TxCtxt);
  except
    TransactionRollback(TxCtxt);
    raise;
  end;
end;

function TMDBItemPersister.CustomQueryMT(Ctx: TObject; const Key: TGuid; KeyMeaning: TCustomKeyMeaning;
      AdditionalFilter: TAdditionalFilter; var TopLevelList: TKItemList): boolean;

  procedure AddToChildren(ParentItem, Item: TKKeyedObject);
  begin
    if ParentItem is TKUserProfile then
    begin
      if not (ParentItem as TKUserProfile).Media.Add(Item) then
        raise EDBPersistError.Create(S_CUSTOM_QUERY_INTERNAL_ERROR);
    end
    else if ParentItem is TKMediaItem then
    begin
      if not (ParentItem as TKMediaItem).Comments.Add(Item) then
        raise EDBPersistError.Create(S_CUSTOM_QUERY_INTERNAL_ERROR);
    end
    else
      raise EDBPersistError.Create(S_CUSTOM_QUERY_INTERNAL_ERROR);
  end;

var
  TxCtxt: TMDBTransactionContext;
  LowerList, HigherList: TKItemList;
  LowerListLevel, HigherListLevel: TKListLevel;
  LowerTable: TMemAPITableData;
  HigherPersister: TMDBItemPersister;
  IndexName, FieldName: string;
  ParentKey: TGuid;
  OK: boolean;
  SearchData, Data: TMemDbFieldDataRec;
  Item, NextItem, ParentItem: TKKeyedObject;
  NullBlocks: TVarBlockArray;
begin
  LowerList := GetListClass.Create as TKIdList;
  HigherList := nil;
  LowerListLevel := ListLevel;
  result := false;
  //Step one. Query DB for matching items at bottommost level.
  TransactionSetup(Ctx, TxCtxt, amRead);
  try
    LowerTable := AccessTblData(TxCtxt, LowerListLevel);
    try
      case KeyMeaning of
        ckmNone: IndexName := '';
        ckmItemParentKey:
        begin
          IndexName := GetParentIndexName(ListLevel);
          FieldName := GetParentFieldName(ListLevel)
        end;
        ckmCommentOwnerKey:
        begin
          if ListLevel = klCommentList then
          begin
            IndexName := S_INDEX_COMMENT_OWNER_KEY;
            FieldName := S_COMMENT_OWNER_KEY;
          end
          else
            raise EDBPersistError.Create(S_CUSTOM_QUERY_BAD_PARAMS);
        end;
      end;
      if (Length(IndexName) = 0) and (Key <> TGuid.Empty) then
        raise EDBPersistError.Create(S_CUSTOM_QUERY_BAD_PARAMS);

      //OK, set up index to initially find on.
      //TODO - At the moment, always searching on string fields.
      //may need more cleverness for general searches.

      if Key <> TGuid.Empty then
      begin
        SearchData.FieldType := ftGuid;
        SearchData.gVal := Key;
        OK := LowerTable.FindEdgeByIndex(ptFirst, IndexName, SearchData)
      end
      else
        OK := LowerTable.Locate(ptFirst, IndexName);
      while OK do
      begin
        Item := CreateItemFromTableCursor(LowerTable);
        OK := LowerList.Add(Item);
        Assert(OK);

        OK := LowerTable.Locate(ptNext, IndexName);
        if Key <> TGuid.Empty then
        begin
          if OK then
            LowerTable.ReadField(FieldName, Data);
          OK := OK and DataRecsSame(Data, SearchData)
        end;
      end;
      //Step 2. If additional filter, then apply that.
      if Assigned(AdditionalFilter) then
      begin
        Item := LowerList.AdjacentBySortVal(katFirst, ksvPointer, nil);
        while Assigned(Item) do
        begin
          NextItem := LowerList.AdjacentBySortVal(katNext, ksvPointer, nil);
          if not AdditionalFilter.FilterAllowKeyedObject(Item) then
          begin
            LowerList.Remove(Item);
            Item.Free;
          end;
          Item := NextItem;
        end;
      end;
      //Step 3. Go from a flat bottom level list and build the lists by owner,
      //from the bottom up until we have a tree.
      //Unfortunately, the TKDataObject items do not have the owner key information,
      //So we need to requery the DB at each level for that information.
      //We could cache it, but KISS.

      //We're in the same transaction, all DB queries should succeed.
      FillChar(NullBlocks, sizeof(NullBlocks), 0);
      while LowerListLevel > Low(LowerListLevel) do
      begin
        //Setup level above list.
        HigherListLevel := Pred(LowerListLevel);
        HigherPersister := ParentDB.CreateItemPersister(HigherListLevel) as TMDBItemPersister;
        HigherList := GetOtherListClass(HigherListLevel).Create as TKIdList;
        NullBlocks.llTag := LowerListLevel;

        FieldName := GetParentFieldName(LowerListLevel);
        Item := LowerList.AdjacentBySortVal(katFirst, ksvPointer, nil);
        while Assigned(Item) do
        begin
          NextItem := LowerList.AdjacentBySortVal(katNext, ksvPointer, Item);
          //Locate item in lower table.
          OK := FindItemByAnyIDLL(LowerListLevel, LowerTable, NullBlocks, Item.Key);
          if not OK then
            raise EDBPersistError.Create(S_CUSTOM_QUERY_INTERNAL_ERROR);
          //Find owner key.
          LowerTable.ReadField(FieldName, Data);
          Assert(Data.FieldType = ftGuid);
          ParentKey := Data.gVal;
          ParentItem := HigherList.SearchByInternalKeyOnly(ParentKey);
          if not Assigned(ParentItem) then
          begin
            if not HigherPersister.GetSingleItemMTG(Ctx, ParentKey, ParentItem) then
              raise EDBPersistError.Create(S_CUSTOM_QUERY_INTERNAL_ERROR);
            if not HigherList.Add(ParentItem) then
              raise EDBPersistError.Create(S_CUSTOM_QUERY_INTERNAL_ERROR);
          end;
          //Cool, remove lower item from lower list, re-parent to parent item.
          LowerList.Remove(Item);
          AddToChildren(ParentItem, Item);
          Item := NextItem;
        end;

        //Swap lists and levels from lower to higher.
        if LowerList.Count > 0 then
          raise EDBPersistError.Create(S_CUSTOM_QUERY_INTERNAL_ERROR);
        LowerList.Free;
        LowerList := HigherList;
        HigherList := nil;

        LowerTable.Free;
        LowerListLevel := HigherListLevel;
        LowerTable := AccessTblData(TxCtxt, LowerListLevel);

        HigherPersister.Free;
        HigherPersister := nil;
      end;
    finally
      LowerTable.Free;
      HigherPersister.Free;
    end;

    TransactionCommit(TxCtxt);
    TopLevelList := LowerList;
    LowerList := nil;
    Assert(HigherList = nil);
    result := true;
  except
    LowerList.Free;
    HigherList.Free;
    TransactionRollback(TxCtxt);
    raise;
  end;
end;


function TMDBItemPersister.ChangeSingleItemMT(Ctx: TObject; const OwnerKey: TGuid; Item: TKKeyedObject;
  ActionSet: TPersistActionSet): boolean;
var
  Action: TPersistAction;
  TxCtxt: TMDBTransactionContext;
  Table: TMemAPITableData;
  Blocks: TVarBlockArray;
  NewGUID: TGUID;
  ChildPersister: TMDBItemPersister;
  NextLevel: TKListLevel;
  ChildList: TKItemList;
begin
  result := false;
  if not Assigned(Item) then
    exit;
  TransactionSetup(Ctx, TxCtxt, amReadWrite);
  try
    Table := AccessTblData(TxCtxt, ListLevel);
    try
      PersisterBlocksFromKeyedObject(Item, Blocks);
      Action := Low(Action);
      repeat
        if Action in ActionSet then
        begin
          case Action of
            patAdd:
              begin
                if not FindItemByAnyID(Table, Blocks, Item.Key) then
                begin
                  if Item.Key = TGuid.Empty then
                  begin
                    CreateGUID(NewGUID);
                    Item.Key := NewGUID;
                  end;
                  CreateTableRecordFromItem(Table, Item, OwnerKey);
                  result := true;
                end;
              end;
            patDelete:
              begin
                if FindItemByAnyID(Table, Blocks, Item.Key) then
                begin
                  result := true;
                  // Recursively delete lower level items in the tree.
                  if ListLevel < High(TKListLevel) then
                  begin
                    NextLevel := Succ(ListLevel);
                    ChildPersister := ParentDB.CreateItemPersister(NextLevel) as TMDBItemPersister;
                    ChildList := ChildPersister.ListClass.Create as TKItemList;
                    try
                      result := result and ChildPersister.ChangeItemListMT(Ctx, Item.Key, ChildList,
                        [patDelete]);
                    finally
                      ChildList.Free;
                      ChildPersister.Free;
                    end;
                  end;
                  if ListLevel = klUserList then
                  begin
                    // Comments, feedback etc still owned by users, and need to be
                    // removed - currently comments only, might later be feedback too.
                    ChildPersister := ParentDB.CreateItemPersister(klCommentList) as TMDBItemPersister;
                    ChildList := ChildPersister.ListClass.Create as TKItemList;
                    try
                      result := result and ChildPersister.ChangeItemListForOtherLevelMT(Ctx, Item.Key,
                        ListLevel, ChildList, [patDelete]);
                    finally
                      ChildList.Free;
                      ChildPersister.Free;
                    end;
                  end;
                  if result then
                  begin
{$IFDEF DEBUG_DATABASE_DELETE}
                    GLogLog(SV_INFO,  ListLevelStrings[ListLevel] +
                      ' Single item DELETE, OwnerKey: ' + OwnerKey +
                      ' ItemKey: '+ Item.Key);
{$ENDIF}
                    Table.Delete;
                  end;
                end;
              end;
            patUpdate:
              begin
                if FindItemByAnyID(Table, Blocks, Item.Key) then
                begin
                  UpdateTableCursorFromItem(Table, Item, OwnerKey);
                  result := true;
                end;
              end
          else
            Assert(false);
          end;
        end;
        if Action < High(Action) then
          Inc(Action)
        else
          break;
      until result;
    finally
      Table.Free;
    end;
    TransactionCommit(TxCtxt);
  except
    TransactionRollback(TxCtxt);
    raise;
  end;
end;

//TODO - The delete case will not work for user lists,
//needs Table.Locate on ('') instead of Table.FindEdgeByIndex
//See GetItemList or ExpireItemList for an example.
function TMDBItemPersister.ChangeItemListMT(Ctx: TObject; const ParentKey: TGuid; ItemList: TKItemList;
  ActionSet: TPersistActionSet): boolean;
var
  TxCtxt: TMDBTransactionContext;
  Table: TMemAPITableData;
  FilteredDataset: TMemAPITableData;
{$IFDEF DEBUG_DATABASE_DELETE_2}
  FilteredDataset2: TMemAPITableData;
{$ENDIF}
  FilterSearchData: TMemDbFieldDataRec;
  FilterData: TMemDbFieldDataRec;
  ChildPersister: TMDBItemPersister;
  ChildList: TKItemList;
  NextLevel: TKListLevel;
  ActionGood: boolean;
  Action: TPersistAction;
  Item, SrcItem: TKKeyedObject;
  Blocks: TVarBlockArray;
  NewGUID: TGUID;
  SubRes: boolean;
  OK: boolean;
begin
  result := false;
  if not Assigned(ItemList) then
    exit;
  if (ParentKey = TGuid.Empty)  <> (ListLevel = klUserList) then
    exit;
  TransactionSetup(Ctx, TxCtxt, amReadWrite);
  try
    Table := AccessTblData(TxCtxt, ListLevel);
    try
      result := true;
      for Action := Low(Action) to High(Action) do
      begin
        if Action in ActionSet then
        begin
          case Action of
            patAdd:
              begin
                // Go thru provided list, and add those not in db.
                Item := ItemList.AdjacentBySortVal(katFirst, ksvPointer, nil);
                while Assigned(Item) do
                begin
                  PersisterBlocksFromKeyedObject(Item, Blocks);
                  if not FindItemByAnyID(Table, Blocks, Item.Key) then
                  begin
                    if Item.Key = TGuid.Empty then
                    begin
                      CreateGUID(NewGUID);
                      Item.Key := NewGUID;
                    end;
                    CreateTableRecordFromItem(Table, Item, ParentKey);
                  end;
                  Item := ItemList.AdjacentBySortVal(katNext, ksvPointer, Item);
                end;
                ActionGood := true;
              end;
            patDelete:
              begin
                ActionGood := true;
                //And with some understanding of transaction isolation level,
                //Can go thru dataset last comitted read, and delete those
                //without building a separate key list.
                FilteredDataSet := AccessTblData(TxCtxt, ListLevel);
                FilteredDataSet.Isolation := ilCommittedRead;
                //Committed read because auto-next on dataset delete does not
                //necessarily take us to the correct "next" filtered row.
{$IFDEF DEBUG_DATABASE_DELETE_2}
                FilteredDataSet2 := AccessTblData(TxCtxt, ListLevel);
                FilteredDataSet2.Isolation := ilCommittedRead;
                //Committed read because auto-next on dataset delete does not
                //necessarily take us to the correct "next" filtered row.
{$ENDIF}
                try
                  FilterSearchData.FieldType := ftGuid;
                  FilterSearchData.gVal := ParentKey;
{$IFDEF DEBUG_DATABASE_DELETE_2}
                  GLogLog(SV_INFO, 'Print list of items by index (debug check).');
                  OK := FilteredDataSet.FindEdgeByIndex(ptFirst, GetParentIndexName(ListLevel), FilterSearchData);
                  while OK do
                  begin
                    Item := CreateItemFromTableCursor(FilteredDataset);
                    Item.Free;
                    Item := nil;
                    OK := FilteredDataSet.Locate(ptNext, GetParentIndexName(ListLevel));
                  end;
                  GLogLog(SV_INFO, 'End Print list of items by index (debug check end).');

                  GLogLog(SV_INFO, 'Print list of items by ROWID (debug check).');
                  OK := FilteredDataSet.Locate(ptFirst, '');
                  while OK do
                  begin
                    Item := CreateItemFromTableCursor(FilteredDataset);
                    Item.Free;
                    Item := nil;
                    OK := FilteredDataSet.Locate(ptNext, '');
                  end;
                  GLogLog(SV_INFO, 'End Print list of items by ROWID (debug check end).');
{$ENDIF}
                  OK := FilteredDataSet.FindEdgeByIndex(ptFirst, GetParentIndexName(ListLevel), FilterSearchData);
{$IFDEF DEBUG_DATABASE_DELETE}
                  if OK then
                  begin
                    GLogLog(SV_INFO,  ListLevelStrings[ListLevel] +
                      ' Filtered list DELETE Find FIRST OK');
                  end;
{$ENDIF}
                  while OK do
                  begin
                    Item := CreateItemFromTableCursor(FilteredDataset);
{$IFDEF DEBUG_DATABASE_DELETE}
                  if OK then
                  begin
                    GLogLog(SV_INFO,  ListLevelStrings[ListLevel] +
                      ' Filtered list DELETE Find item OK ParentKey: ' + ParentKey + ' ItemKey: '+ Item.Key);
                  end;
{$ENDIF}
                    try
                      SrcItem := ItemList.SearchBySortVal(ksvKey, Item);
                      if not Assigned(SrcItem) then
                      begin
                        SubRes := true;
                        // Recursively delete lower level items in the tree.
                        if ListLevel < High(TKListLevel) then
                        begin
                          NextLevel := Succ(ListLevel);
                          ChildPersister := ParentDB.CreateItemPersister(NextLevel) as TMDBItemPersister;
                          ChildList := ChildPersister.ListClass.Create as TKItemList;
                          try
                            SubRes := SubRes and ChildPersister.ChangeItemListMT(Ctx, Item.Key, ChildList, [patDelete]);
                          finally
                            ChildList.Free;
                            ChildPersister.Free;
                          end;
                        end;
                        if ListLevel = klUserList then
                        begin
                          // Comments, feedback etc still owned by users, and need to be
                          // removed - currently comments only, might later be feedback too.
                          ChildPersister := ParentDB.CreateItemPersister(klCommentList) as TMDBItemPersister;
                          ChildList := ChildPersister.ListClass.Create as TKItemList;
                          try
                            SubRes := SubRes and ChildPersister.ChangeItemListForOtherLevelMT(Ctx, Item.Key, ListLevel, ChildList, [patDelete]);
                          finally
                            ChildList.Free;
                            ChildPersister.Free;
                          end;
                        end;
                        if SubRes then
                        begin
{$IFDEF DEBUG_DATABASE_DELETE}
                    GLogLog(SV_INFO,  ListLevelStrings[ListLevel] +
                      ' Filtered list DELETE, ParentKey: ' + ParentKey +
                      ' ItemKey: '+ Item.Key);
{$ENDIF}
                          FilteredDataSet.Delete;
                          //Delete only navigates to next isoDirtyRead,
                          //else stays on same row.
                        end
                        else
                        begin
{$IFDEF DEBUG_DATABASE_DELETE}
                    GLogLog(SV_INFO,  'WARNING: SubRes is false, it should be true!');
{$ENDIF}
                        end;
{$IFDEF DEBUG_DATABASE_DELETE_2}
                        GLogLog(SV_INFO, '');
                        GLogLog(SV_INFO, 'Print list of items by index (debug check).');
                        OK := FilteredDataSet2.FindEdgeByIndex(ptFirst, GetParentIndexName(ListLevel), FilterSearchData);
                        while OK do
                        begin
                          Item.Free;
                          Item := nil;
                          Item := CreateItemFromTableCursor(FilteredDataset2);
                          Item.Free;
                          Item := nil;
                          OK := FilteredDataSet2.Locate(ptNext, GetParentIndexName(ListLevel));
                        end;
                        GLogLog(SV_INFO, 'End Print list of items by index (debug check end).');
                        GLogLog(SV_INFO, '');
{$ENDIF}
                        ActionGood := ActionGood and SubRes;
                      end;
                    finally
                      Item.Free;
                    end;
                    OK := FilteredDataSet.Locate(ptNext, GetParentIndexName(ListLevel));
{$IFDEF DEBUG_DATABASE_DELETE}
                    if OK then
                    begin
                      GLogLog(SV_INFO,  ListLevelStrings[ListLevel] +
                        ' Filtered list DELETE LOCATE NEXT OK ParentKey: ' + ParentKey);
                    end
                    else
                    begin
                      GLogLog(SV_INFO,  ListLevelStrings[ListLevel] +
                        ' Filtered list DELETE LOCATE NEXT no more data, STOP.');
                    end;
{$ENDIF}
                    if OK then
                      FilteredDataSet.ReadField(GetParentFieldName(ListLevel), FilterData);
{$IFDEF DEBUG_DATABASE_DELETE}
                    if OK then
                    begin
                      if DataRecsSame(FilterData, FilterSearchData) then
                      begin
                        GLogLog(SV_INFO,  ListLevelStrings[ListLevel] +
                          ' Filtered list DELETE Recs same, keep going.');
                      end
                      else
                      begin
                        GLogLog(SV_INFO,  ListLevelStrings[ListLevel] +
                          ' Filtered list DELETE Recs differ, STOP.');
                        if FilterData.FieldType = ftUnicodeString then
                        begin
                          Assert(FilterData.FieldType = FilterSearchData.FieldType);
                          GLogLog(SV_INFO, ListLevelStrings[ListLevel] +
                            ' Filtered list DELETE, SearchRec: ' + FilterSearchData.sVal
                            + ' DataRec: ' + FilterData.sVal);
                        end;
                      end;
                    end;
{$ENDIF}
                    OK := OK and DataRecsSame(FilterData, FilterSearchData)
                  end;
                finally
                  FilteredDataset.Free;
{$IFDEF DEBUG_DATABASE_DELETE_2}
                  FilteredDataset2.Free;
{$ENDIF}
                end;
              end;
            patUpdate:
              begin
                // Go thru list and update those in DB table.
                Item := ItemList.AdjacentBySortVal(katFirst, ksvPointer, nil);
                while Assigned(Item) do
                begin
                  PersisterBlocksFromKeyedObject(Item, Blocks);
                  if FindItemByAnyID(Table, Blocks, Item.Key) then
                    UpdateTableCursorFromItem(Table, Item, ParentKey);
                  Item := ItemList.AdjacentBySortVal(katNext, ksvPointer, Item);
                end;
                ActionGood := true;
              end;
          else
            Assert(false);
            ActionGood := false;
          end;
          result := result and ActionGood;
        end;
      end;
    finally
      Table.Free;
    end;
    TransactionCommit(TxCtxt);
  except
    TransactionRollback(TxCtxt);
    raise;
  end;
end;

//TODO - I guess we could add more indices on last updated and/or date,
//but DB load takes long enough. For the moment, just brute force iterate
//thru entire table once.
function TMDBItemPersister.ExpireItemListMT(Ctx: TObject; const ParentKey: TGuid; ExpireBefore: TDateTime;
  ExpiryType: TDBExpiryType): boolean;

var
  TxCtxt: TMDBTransactionContext;
  Table:TMemAPITableData;
  Iter, DoDelete, SubRes: boolean;
  FilterSearchData, FieldData: TMemDbFieldDataRec;
  NextLevel: TKListLevel;
  ChildPersister: TMDBItemPErsister;
  ChildList: TKItemList;
  ItemKey: TGuid;

  function ItemExpired(Table:TMemAPITableData; ExpireBefore: TDateTime; ExpiryType: TDBExpiryType): boolean;
  var
    FieldName: string;
  begin
    case ExpiryType of
      dbxDate: begin
        case ListLevel of
          klUserList: raise EDBPersistError.Create(S_EXPIRY_INTERNAL);
          klMediaList: FieldName := S_MEDIA_DATE;
          klCommentList: FieldName := S_COMMENT_DATE;
        else
          raise EDBPersistError.Create(S_EXPIRY_INTERNAL);
        end;
      end;
      dbxLastUpdated: FieldName := S_LAST_UPDATED;
    else
      raise EDBPersistError.Create(S_EXPIRY_INTERNAL);
    end;
    Table.ReadField(FieldName, FieldData);
    Assert(FieldData.FieldType = ftDouble);
    result := FieldData.dVal < ExpireBefore;
  end;

begin
  result := false;
  if (ListLevel = klUserList) and (ExpiryType = dbxLastUpdated) then
    exit;
  if (ParentKey = TGuid.Empty)  <> (ListLevel = klUserList) then
    exit;

  TransactionSetup(Ctx, TxCtxt, amReadWrite);
  try
    //Committed read because auto-next on dataset delete does not
    //necessarily take us to the correct "next" filtered row.
    Table := AccessTblData(TxCtxt, ListLevel);
    Table.Isolation := ilCommittedRead;
    try
      if ListLevel <> Low(TKListLevel) then
      begin
        FilterSearchData.FieldType := ftGuid;
        FilterSearchData.gVal := ParentKey;
      end;

      if ListLevel = Low(TkListLevel) then
        Iter := Table.Locate(ptFirst, '')
      else
        Iter := Table.FindEdgeByIndex(ptFirst, GetParentIndexName(ListLevel), FilterSearchData);

      while Iter do
      begin
        DoDelete := ItemExpired(Table, ExpireBefore, ExpiryType);
        if DoDelete then
        begin
          //TODO Couldn't be bothered to read entire item ...
          //Arguable whether a good thing or not.
          case ListLevel of
            klUserList: Table.ReadField(S_USER_KEY, FieldData);
            klMediaList: Table.ReadField(S_MEDIA_KEY, FieldData);
            klCommentList: Table.ReadField(S_COMMENT_KEY, FieldData);
          else
            raise EDBPersistError.Create(S_EXPIRY_INTERNAL);
          end;
          Assert(FieldData.FieldType = ftGuid);
          ItemKey := FieldData.gVal;

          //Children deletion same as elsewhere
          //TODO Might yet make the code common ....

          SubRes := true;
          // Recursively delete lower level items in the tree.
          if ListLevel < High(TKListLevel) then
          begin
            NextLevel := Succ(ListLevel);
            ChildPersister := ParentDB.CreateItemPersister(NextLevel) as TMDBItemPersister;
            ChildList := ChildPersister.ListClass.Create as TKItemList;
            try
              SubRes := SubRes and ChildPersister.ChangeItemListMT(Ctx, ItemKey, ChildList, [patDelete]);
            finally
              ChildList.Free;
              ChildPersister.Free;
            end;
          end;
          if ListLevel = klUserList then
          begin
            // Comments, feedback etc still owned by users, and need to be
            // removed - currently comments only, might later be feedback too.
            ChildPersister := ParentDB.CreateItemPersister(klCommentList) as TMDBItemPersister;
            ChildList := ChildPersister.ListClass.Create as TKItemList;
            try
              SubRes := SubRes and ChildPersister.ChangeItemListForOtherLevelMT(Ctx, ItemKey, ListLevel, ChildList, [patDelete]);
            finally
              ChildList.Free;
              ChildPersister.Free;
            end;
          end;
          DoDelete := DoDelete and SubRes;
        end;
        if DoDelete then
          Table.Delete;
        if ListLevel = Low(TKListLevel) then
          Iter := Table.Locate(ptNext, '')
        else
        begin
          Iter := Table.Locate(ptNext, GetParentIndexName(ListLevel));
          if Iter then
          begin
            Table.ReadField(GetParentFieldName(ListLevel), FilterSearchData);
            Assert(FilterSearchData.FieldType = ftGuid);
            Iter := FilterSearchData.gVal = ParentKey;
          end;
        end;
      end;
    finally
      Table.Free;
    end;
    TransactionCommit(TxCtxt);
    result := true;
  except
    TransactionRollback(TxCtxt);
    raise;
  end;
end;

function TMDBItemPersister.PruneUnusedMT(Ctx: TObject): boolean;
var
  TxCtxt: TMDBTransactionContext;
  Table:TMemAPITableData;
  ChildTable: TMemAPITableData;
  CommentTable: TMemAPITableData;
  Iter, DoDelete: boolean;
  FieldData: TMemDbFieldDataRec;
  UP: TKUSerProfile;

begin
  result := false;
  //Let's assume we don't want to do this at the highest list level.
  //i.e. Auto delete all comments.
  if ListLevel = High(TKListLevel) then
    exit;
  TransactionSetup(Ctx, TxCtxt, amReadWrite);
  try
    //Not comitted read, because we are going through absolutely all items,
    //So "auto-next" behaviour is fine.
    Table := AccessTblData(TxCtxt, ListLevel);
    ChildTable := AccessTblData(TxCtxt, Succ(ListLevel));
    if ListLevel = klUserList then
      CommentTable := AccessTblData(TxCtxt, klCommentList)
    else
      CommentTable := nil;
    try
      //Use locate API's, we're not going to go thru subsets of stuff.
      Iter := Table.Locate(ptFirst, '');
      while Iter do
      begin
        case ListLevel of
          klUserList: Table.ReadField(S_USER_KEY, FieldData);
          klMediaList: Table.ReadField(S_MEDIA_KEY, FieldData);
          klCommentList: Table.ReadField(S_COMMENT_KEY, FieldData);
        else
          raise EDBPersistError.Create(S_EXPIRY_INTERNAL);
        end;
        Assert(FieldData.FieldType = ftGuid);
        //First, any child items?
        DoDelete := not ChildTable.FindByIndex(GetParentIndexName(Succ(ListLevel)), FieldData);
        if DoDelete then
        begin
          if ListLevel = klUserList then
          begin
            //Any comments owned by this user profile?
            DoDelete := not CommentTable.FindByIndex(GetOwnerIndexName(klCommentList), FieldData);
            //Finally, if nothing dependent on this profile or item...
            if DoDelete then
            begin
              UP := CreateItemFromTableCursor(Table) as TKUserProfile;
              try
                DoDelete := UP.InterestLevel = TKProfileInterestLevel.kpiFetchUserForRefs;
              finally
                UP.Free;
              end;
            end;
          end;
        end;
        //We do auto-increment in this table (iso level).
        if DoDelete then
        begin
          Table.Delete;
          Iter := Table.RowSelected;
        end
        else
          Iter := Table.Locate(ptNext, '');
      end;
    finally
      CommentTable.Free;
      ChildTable.Free;
      Table.Free;
    end;
    TransactionCommit(TxCtxt);
    result := true;
  except
    TransactionRollback(TxCtxt);
    raise;
  end;
end;

function TMDBItemPersister.ChangeItemListForOtherLevelMT(Ctx: TObject; const ParentKey: TGuid;
  ParentLevel: TKListLevel; ItemList: TKItemList; ActionSet: TPersistActionSet): boolean;
var
  TxCtxt: TMDBTransactionContext;
  Action: TPersistAction;
  ActionGood: boolean;
  FilteredDataset: TMemAPITableData;
  FilterSearchData, FilterData: TMemDbFieldDataRec;
  OK: boolean;
  Item, SrcItem: TKKeyedObject;
begin
  result := false;
  if not Assigned(ItemList) then
    exit;
  if ParentKey = TGuid.Empty then
    exit;
  TransactionSetup(Ctx, TxCtxt, amReadWrite);
  try
    result := true;
    for Action := Low(Action) to High(Action) do
    begin
      if Action in ActionSet then
      begin
        case Action of
          patAdd, patUpdate:
            begin
              ActionGood := false;
              // Not implemented, nor can we here - don't have the owning
              // media keys to hand, although in theory we could do a join.
            end;
          patDelete:
            begin
              if (ListLevel = klCommentList) and (ParentLevel = klUserList) then
              begin
                ActionGood := true;
                FilteredDataSet := AccessTblData(TxCtxt, ListLevel);
                FilteredDataSet.Isolation := ilCommittedRead;
                try
                  FilterSearchData.FieldType := ftGuid;
                  FilterSearchData.gVal := ParentKey;
                  OK := FilteredDataSet.FindEdgeByIndex(ptFirst, GetOwnerIndexName(ListLevel), FilterSearchData);
                  while OK do
                  begin
                    Item := CreateItemFromTableCursor(FilteredDataSet);
                    Assert(ListLevel = High(TKListLevel));
                    // So we don't need to delete child objects.
                    try
                      SrcItem := ItemList.SearchBySortVal(ksvKey, Item);
                      if not Assigned(SrcItem) then
                        FilteredDataSet.Delete;
                    finally
                      Item.Free;
                    end;
                    OK := FilteredDataSet.Locate(ptNext, GetOwnerIndexName(ListLevel));
                    if OK then
                      FilteredDataSet.ReadField(GetOwnerFieldName(ListLevel), FilterData);
                    OK := OK and DataRecsSame(FilterData, FilterSearchData)
                  end;
                finally
                  FilteredDataSet.Free;
                end;
              end
              else
                ActionGood := false;
            end;
        else
          Assert(false);
          ActionGood := false;
        end;
        result := result and ActionGood;
      end;
    end;
    TransactionCommit(TxCtxt);
  except
    TransactionRollback(TxCtxt);
    raise;
  end;
end;

function TMDBItemPersister.GetParentDB: TMemKDB;
begin
  result := TDBPersist(self).ParentDB as TMemKDB;
end;


{ TMDBUserProfilePersister }

function TMDBUserProfilePersister.CreateItemFromTableCursor(Table: TMemAPITableData): TKKeyedObject;
var
  UBType: TKSiteType;
  r: TKUserProfile;
  Data: TMemDBFieldDataRec;
begin
  result := TKUserProfile.Create;
  r := result as TKUserProfile;
  with Table do
  begin
    ReadField(S_USER_KEY, Data);
    Assert(Data.FieldType = ftGuid);
    result.Key := Data.gVal;
    for UBType := Low(UBType) to High(UBType) do
    begin
      ReadField(UserBlockFieldNames[UBType, tubUserName], Data);
      Assert(Data.FieldType = ftUnicodeString);
      r.SiteUserBlock[UBType].Username := Data.sVal;
      ReadField(UserBlockFieldNames[UBType, tubUserId], Data);
      Assert(Data.FieldType = ftUnicodeString);
      r.SiteUserBlock[UBType].UserId := Data.sVal;
      ReadField(UserBlockFieldNames[UBType, tubVerified], Data);
      Assert(Data.FieldType = ftInteger);
      r.SiteUserBlock[UBType].Verified := Data.i32Val <> 0;
      ReadField(UserBlockFieldNames[UBType, tubProfUrl], Data);
      Assert(Data.FieldType = ftUnicodeString);
      r.SiteUserBlock[UBType].ProfilePicUrl := Data.sVal;
      ReadField(UserBlockFieldNames[UBType, tubFullName], Data);
      Assert(Data.FieldType = ftUnicodeString);
      r.SiteUserBlock[UBType].FullName := Data.sVal;
      ReadField(UserBlockFieldNames[UBType, tubBio], Data);
      Assert(Data.FieldType = ftUnicodeString);
      r.SiteUserBlock[UBType].Bio := Data.sVal;
      ReadField(UserBlockFieldNames[UBType, tubFollowsCount], Data);
      Assert(Data.FieldType = ftInteger);
      r.SiteUserBlock[UBType].FollowsCount := Data.i32Val;
      ReadField(UserBlockFieldNames[UBType, tubFollowerCount], Data);
      Assert(Data.FieldType = ftInteger);
      r.SiteUserBlock[UBType].FollowerCount := Data.i32Val;

      if (Length(r.SiteUserBlock[UBType].Username) > 0) or
        (Length(r.SiteUserBlock[UBType].UserId) > 0) or
        (Length(r.SiteUserBlock[UBType].ProfilePicUrl) > 0) or
        (Length(r.SiteUserBlock[UBType].FullName) > 0) or (Length(r.SiteUserBlock[UBType].Bio) > 0)
        or (r.SiteUserBlock[UBType].FollowsCount <> 0) or
        (r.SiteUserBlock[UBType].FollowerCount <> 0) then
        r.SiteUserBlock[UBType].Valid := true;
    end;
    ReadField(S_USER_INTEREST, Data);
    Assert(Data.FieldType = ftInteger);
    if not ((Data.i32Val >= Ord(Low(r.InterestLevel)))
         and (Data.i32Val <= Ord(High(r.InterestLevel)))) then
      raise EDBPersistError.Create(S_FIELD_OUT_OF_RANGE);
    r.InterestLevel := TKProfileInterestLevel(Data.i32Val);
    ReadField(S_LAST_UPDATED, Data);
    Assert(Data.FieldType = ftDouble);
    r.LastUpdated := Data.dVal;
  end;
  if result.Key = TGuid.Empty then
    raise EDBPersistError.Create(S_READ_RECORD_WITH_NULL_KEY);
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'UTABLE read: ' + ' UKey: ' + r.Key + ' IUID: ' + r.SiteUserBlocks[tstInstagram]
    .UserId + ' IUName: ' + r.SiteUserBlocks[tstInstagram].Username + ' TUID: ' + r.SiteUserBlocks
    [tstTwitter].UserId + ' TUName: ' + r.SiteUserBlocks[tstTwitter].Username);
{$ENDIF}
end;

procedure TMDBUserProfilePersister.UpdateCommon(Table: TMemAPITableData; Item: TKKeyedObject;
  const OwnerGUID: TGuid);
var
  UBType: TKSiteType;
  ReadUserKey: TGuid;
  U: TKUserProfile;
  Data: TMemDBFieldDataRec;
begin
  U := Item as TKUserProfile;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'UTABLE update: ' + ' UKey: ' + U.Key + ' IUID: ' + U.SiteUserBlocks
    [tstInstagram].UserId + ' IUName: ' + U.SiteUserBlocks[tstInstagram].Username + ' TUID: ' +
    U.SiteUserBlocks[tstTwitter].UserId + ' TUName: ' + U.SiteUserBlocks[tstTwitter].Username);
{$ENDIF}
  with Table do
  begin
    ReadField(S_USER_KEY, Data);
    Assert(Data.FieldType = ftGuid);
    ReadUserKey := Data.gVal;
    if ReadUserKey <> TGuid.Empty then
    begin
      if U.Key <> TGuid.Empty then
        if U.Key <> ReadUserKey then
          raise EDBPersistError.Create(S_ITEM_KEYS_INCONSISTENT);
    end
    else
    begin
      if U.Key = TGuid.Empty then
        raise EDBPersistError.Create(S_WRITING_NULL_KEY);
      Assert(Data.FieldType = ftGuid);
      Data.gVal := U.Key;
      WriteField(S_USER_KEY, Data);
    end;

    for UBType := Low(UBType) to High(UBType) do
    begin
      if U.SiteUserBlock[UBType].Valid then
      begin
        Data.FieldType := ftUnicodeString;
        Data.sVal := U.SiteUserBlock[UBType].Username;
        WriteField(UserBlockFieldNames[UBType, tubUserName], Data);
        Assert(Data.FieldType = ftUnicodeString);
        Data.sVal := U.SiteUserBlock[UBType].UserId;
        WriteField(UserBlockFieldNames[UBType, tubUserId], Data);
        Data.FieldType := ftInteger;
        Data.i32Val := Ord(U.SiteUserBlock[UBType].Verified);
        WriteField(UserBlockFieldNames[UBType, tubVerified], Data);
        Data.FieldType := ftUnicodeString;
        Data.sVal := U.SiteUserBlock[UBType].ProfilePicUrl;
        WriteField(UserBlockFieldNames[UBType, tubProfUrl], Data);
        Assert(Data.FieldType = ftUnicodeString);
        Data.sVal := U.SiteUserBlock[UBType].FullName;
        WriteField(UserBlockFieldNames[UBType, tubFullName], Data);
        Assert(Data.FieldType = ftUnicodeString);
        Data.sVal := U.SiteUserBlock[UBType].Bio;
        WriteField(UserBlockFieldNames[UBType, tubBio], Data);
        Data.FieldType := ftInteger;
        Data.i32Val := U.SiteUserBlock[UBType].FollowsCount;
        WriteField(UserBlockFieldNames[UBType, tubFollowsCount], Data);
        Assert(Data.FieldType = ftInteger);
        Data.i32Val := U.SiteUserBlock[UBType].FollowerCount;
        WriteField(UserBlockFieldNames[UBType, tubFollowerCount], Data);
      end;
    end;
    Data.FieldType := ftInteger;
    Data.i32Val := Ord(U.InterestLevel);
    WriteField(S_USER_INTEREST, Data);
    Data.FieldType := ftDouble;
    Data.dVal := U.LastUpdated;
    WriteField(S_LAST_UPDATED, Data);
  end;
end;

function TMDBUserProfilePersister.GetListLevel: TKListLevel;
begin
  result := klUserList;
end;

{ TMDBMediaItemPersister }


function TMDBMediaItemPersister.CreateItemFromTableCursor(Table: TMemAPITableData): TKKeyedObject;
var
  MBType: TKSiteType;
  r: TKMediaItem;
  Data: TMemDbFieldDataRec;
begin
  result := TKMediaItem.Create;
  r := result as TKMediaItem;
  with Table do
  begin
    ReadField(S_MEDIA_KEY, Data);
    Assert(Data.FieldType = ftGuid);
    r.Key := Data.gVal;
    ReadField(S_MEDIA_DATE, Data);
    Assert(Data.FieldType = ftDouble);
    r.Date := Data.dVal;
    ReadField(S_MEDIA_DATA_STR, Data);
    Assert(Data.FieldType = ftUnicodeString);
    r.MediaData := Data.sVal;
    ReadField(S_MEDIA_TYPE, Data);
    Assert(Data.FieldType = ftInteger);
    if not ((Data.i32Val >= Ord(Low(r.MediaType)))
         and (Data.i32Val <= Ord(High(r.MediaType)))) then
      raise EDBPersistError.Create(S_FIELD_OUT_OF_RANGE);
    r.MediaType := TKMediaItemType(Data.i32Val);
    ReadField(S_MEDIA_RESOURCE_URL, Data);
    Assert(Data.FieldType = ftUnicodeString);
    r.ResourceURL := Data.sVal;
    ReadField(S_LAST_UPDATED, Data);
    Assert(Data.FieldType = ftDouble);
    r.LastUpdated := Data.dVal;

    for MBType := Low(MBType) to High(MBType) do
    begin
      ReadField(MediaBlockFieldNames[MBType, tmbMediaId], Data);
      Assert(Data.FieldType = ftUnicodeString);
      r.SiteMediaBlock[MBType].MediaID := Data.sVal;
      ReadField(MediaBlockFieldNames[MBType, tmbOwnerId], Data);
      Assert(Data.FieldType = ftUnicodeString);
      r.SiteMediaBlock[MBType].OwnerID := Data.sVal;
      ReadField(MediaBlockFieldNames[MBType, tmbMediaCode], Data);
      Assert(Data.FieldType = ftUnicodeString);
      r.SiteMediaBlock[MBType].MediaCode := Data.sVal;

      if (Length(r.SiteMediaBlock[MBType].MediaID) > 0) or
        (Length(r.SiteMediaBlock[MBType].OwnerID) > 0) or
        (Length(r.SiteMediaBlock[MBType].MediaCode) > 0) then
        r.SiteMediaBlock[MBType].Valid := true;
    end;
  end;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'MTABLE read: ' + ' MKey: ' + r.Key + ' IMID: ' + r.SiteMediaBlocks[tstInstagram]
    .MediaID + ' IMCode: ' + r.SiteMediaBlocks[tstInstagram].MediaCode + ' TMID: ' +
    r.SiteMediaBlocks[tstTwitter].MediaID + ' TMCode: ' + r.SiteMediaBlocks[tstTwitter].MediaCode);
{$ENDIF}
end;

procedure TMDBMediaItemPersister.UpdateCommon(Table: TMemAPITableData; Item: TKKeyedObject;
  const OwnerGUID: TGuid);
var
  MBType: TKSiteType;
  ReadMediaKey, ReadMediaOwnerKey: TGuid;
  M: TKMediaItem;
  Data: TMemDbFieldDataRec;
begin
  M := Item as TKMediaItem;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'MTABLE update: ' + ' MKey: ' + M.Key + ' UOwnerKey: ' + OwnerGUID + ' IMID: ' +
    M.SiteMediaBlocks[tstInstagram].MediaID + ' IMCode: ' + M.SiteMediaBlocks[tstInstagram]
    .MediaCode + ' TMID: ' + M.SiteMediaBlocks[tstTwitter].MediaID + ' TMCode: ' + M.SiteMediaBlocks
    [tstTwitter].MediaCode);
{$ENDIF}
  if OwnerGUID = TGuid.Empty then
    raise EDBPersistError.Create(S_NULL_OWNER_GUID);
  with Table do
  begin
    ReadField(S_MEDIA_KEY, Data);
    Assert(Data.FieldType = ftGuid);
    ReadMediaKey := Data.gVal;
    if Data.gVal <> TGuid.Empty then
    begin
      if M.Key <> TGuid.Empty then
        if M.Key <> ReadMediaKey then
          raise EDBPersistError.Create(S_ITEM_KEYS_INCONSISTENT);
    end
    else
    begin
      if M.Key = TGuid.Empty then
        raise EDBPersistError.Create(S_WRITING_NULL_KEY);
      Assert(Data.FieldType = ftGuid);
      Data.gVal := M.Key;
      WriteField(S_MEDIA_KEY, Data);
    end;

    ReadField(S_MEDIA_OWNER_KEY, Data);
    Assert(Data.FieldType = ftGuid);
    ReadMediaOwnerKey := Data.gVal;
    if Data.gVal <> TGuid.Empty then
    begin
      if OwnerGUID <> ReadMediaOwnerKey then
        raise EDBPersistError.Create(S_ITEM_KEYS_INCONSISTENT);
    end
    else
    begin
      Assert(Data.FieldType = ftGuid);
      Data.gVal := OwnerGUID;
      WriteField(S_MEDIA_OWNER_KEY, Data);
    end;

    Data.FieldType := ftDouble;
    Data.dVal := M.Date;
    WriteField(S_MEDIA_DATE, Data);
    Data.dVal := M.LastUpdated;
    WriteField(S_LAST_UPDATED, Data);

    Data.FieldType := ftUnicodeString;
    Data.sVal := M.MediaData;
    WriteField(S_MEDIA_DATA_STR, Data);
    Data.sVal := M.ResourceURL;
    WriteField(S_MEDIA_RESOURCE_URL, Data);

    Data.FieldType := ftInteger;
    Data.i32Val := Ord(M.MediaType);
    WriteField(S_MEDIA_TYPE, Data);

    for MBType := Low(MBType) to High(MBType) do
    begin
      if M.SiteMediaBlock[MBType].Valid then
      begin
        Data.FieldType := ftUnicodeString;
        Data.sVal := M.SiteMediaBlock[MBType].MediaID;
        WriteField(MediaBlockFieldNames[MBType, tmbMediaId], Data);
        Data.sVal := M.SiteMediaBlock[MBType].OwnerID;
        WriteField(MediaBlockFieldNames[MBType, tmbOwnerId], Data);
        Data.sVal := M.SiteMediaBlock[MBType].MediaCode;
        WriteField(MediaBlockFieldNames[MBType, tmbMediaCode], Data);
      end;
    end;
  end;
end;

function TMDBMediaItemPersister.GetListLevel: TKListLevel;
begin
  result := klMediaList;
end;

{ TMDBCommentItemPersister }

function TMDBCommentItemPersister.CreateItemFromTableCursor(Table: TMemAPITableData): TKKeyedObject;
var
  CBType: TKSiteType;
  C: TKCommentItem;
  Data: TMemDbFieldDataRec;
begin
  result := TKCommentItem.Create;
  C := result as TKCommentItem;
  with Table do
  begin
    ReadField(S_COMMENT_KEY, Data);
    Assert(Data.FieldType = ftGuid);
    C.Key := Data.gVal;

    ReadField(S_COMMENT_DATE, Data);
    Assert(Data.FieldType = ftDouble);
    C.Date := Data.dVal;

    ReadField(S_COMMENT_DATA_STR, Data);
    Assert(Data.FieldType = ftUnicodeString);
    C.CommentData := Data.sVal;

    ReadField(S_COMMENT_TYPE, Data);
    Assert(Data.FieldType = ftInteger);
    if not ((Data.i32Val >= Ord(Low(C.CommentType)))
         and (Data.i32Val <= Ord(High(C.CommentType)))) then
      raise EDBPersistError.Create(S_FIELD_OUT_OF_RANGE);
    C.CommentType := TKCommentItemType(Data.i32Val);

    ReadField(S_LAST_UPDATED, Data);
    Assert(Data.FieldType = ftDouble);
    C.LastUpdated := Data.dVal;

    for CBType := Low(CBType) to High(CBType) do
    begin
      ReadField(CommentBlockFieldNames[CBType, tcbCommentId], Data);
      Assert(Data.FieldType = ftUnicodeString);
      C.SiteCommentBlock[CBType].CommentId := Data.sVal;
      if Length(C.SiteCommentBlock[CBType].CommentId) > 0 then
        C.SiteCommentBlock[CBType].Valid := true;

      ReadField(CommentBlockFieldNames[CBType, tcbCommentOwnerId], Data);
      Assert(Data.FieldType = ftUnicodeString);
      C.SiteUserBlock[CBType].UserId := Data.sVal;
      if Length(C.SiteUserBlock[CBType].UserId) > 0 then
        C.SiteUserBlock[CBType].Valid := true;
    end;
    ReadField(S_COMMENT_OWNER_KEY, Data);
    Assert(Data.FieldType = ftGuid);
    C.OwnerKey := Data.gVal;
  end;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'CTABLE read: ' + ' CKey: ' + C.Key + ' CUserOwnerKey: ' + C.OwnerKey + ' ICID: '
    + C.SiteCommentBlocks[tstInstagram].CommentId + ' IUID: ' + C.SiteUserBlock[tstInstagram].UserId
    + ' TCID: ' + C.SiteCommentBlocks[tstTwitter].CommentId + ' TUID: ' + C.SiteUserBlock
    [tstTwitter].UserId);
{$ENDIF}
end;

procedure TMDBCommentItemPersister.UpdateCommon(Table: TMemAPITableData; Item: TKKeyedObject;
  const OwnerGUID: TGuid);
var
  CBType: TKSiteType;
  ReadCommentKey, ReadCommentMediaKey, ReadCommentOwnerKey: TGuid;
  C: TKCommentItem;
  Data: TMemDbFieldDataRec;
begin
  C := Item as TKCommentItem;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'CTABLE update: ' + ' CKey: ' + C.Key + ' CMediaOwnerKey: ' + OwnerGUID +
    ' CUserOwnerKey: ' + C.OwnerKey + ' ICID: ' + C.SiteCommentBlocks[tstInstagram].CommentId +
    ' IUID: ' + C.SiteUserBlock[tstInstagram].UserId + ' TCID: ' + C.SiteCommentBlocks[tstTwitter]
    .CommentId + ' TUID: ' + C.SiteUserBlock[tstTwitter].UserId);
{$ENDIF}
  if OwnerGUID = TGuid.Empty then
    raise EDBPersistError.Create(S_NULL_OWNER_GUID);
  with Table do
  begin
    ReadField(S_COMMENT_KEY, Data);
    Assert(Data.FieldType = ftGuid);
    ReadCommentKey := Data.gVal;
    if Data.gVal <> TGuid.Empty then
    begin
      if C.Key <> TGuid.Empty then
        if C.Key <> ReadCommentKey then
          raise EDBPersistError.Create(S_ITEM_KEYS_INCONSISTENT);
    end
    else
    begin
      if C.Key = TGuid.Empty then
        raise EDBPersistError.Create(S_WRITING_NULL_KEY);
      Assert(Data.FieldType = ftGuid);
      Data.gVal := C.Key;
      WriteField(S_COMMENT_KEY, Data);
    end;

    ReadField(S_COMMENT_MEDIA_KEY, Data);
    Assert(Data.FieldType = ftGuid);
    ReadCommentMediaKey := Data.gVal;
    if Data.gVal <> TGuid.Empty then
    begin
      if OwnerGUID <> ReadCommentMediaKey then
        raise EDBPersistError.Create(S_ITEM_KEYS_INCONSISTENT);
    end
    else
    begin
      Assert(Data.FieldType = ftGuid);
      Data.gVal := OwnerGUID;
      WriteField(S_COMMENT_MEDIA_KEY, Data);
    end;

    ReadField(S_COMMENT_OWNER_KEY, Data);
    Assert(Data.FieldType = ftGuid);
    ReadCommentOwnerKey := Data.gVal;
    if Data.gVal <> TGuid.Empty then
    begin
      if C.OwnerKey <> ReadCommentOwnerKey then
        raise EDBPersistError.Create(S_ITEM_KEYS_INCONSISTENT);
    end
    else
    begin
      if C.OwnerKey = TGuid.Empty then
        raise EDBPersistError.Create(S_WRITING_NULL_KEY);
      Assert(Data.FieldType = ftGuid);
      Data.gVal := C.OwnerKey;
      WriteField(S_COMMENT_OWNER_KEY, Data);
    end;

    Data.FieldType := ftUnicodeString;
    for CBType := Low(CBType) to High(CBType) do
    begin
      if C.SiteCommentBlock[CBType].Valid then
      begin
        Data.sVal := C.SiteCommentBlock[CBType].CommentId;
        WriteField(CommentBlockFieldNames[CBType, tcbCommentId], Data);
      end;
      if C.SiteUserBlock[CBType].Valid then
      begin
        Data.sVal := C.SiteUserBlock[CBType].UserId;
        WriteField(CommentBlockFieldNames[CBType, tcbCommentOwnerId], Data);
      end;
    end;

    Data.FieldType := ftDouble;
    Data.dVal := C.Date;
    WriteField(S_COMMENT_DATE, Data);
    Data.dVal := C.LastUpdated;
    WriteField(S_LAST_UPDATED, Data);

    Data.FieldType := ftUnicodeString;
    Data.sVal := C.CommentData;
    WriteField(S_COMMENT_DATA_STR, Data);

    Data.FieldType := ftInteger;
    Data.i32Val := Ord(C.CommentType);
    WriteField(S_COMMENT_TYPE, Data);
  end;
end;

function TMDBCommentItemPersister.GetListLevel: TKListLevel;
begin
  result := klCommentList;
end;


end.
