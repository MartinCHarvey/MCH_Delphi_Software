unit dbXpressPersist;

{ Copyright Martin Harvey December 2016. }

interface

uses DBPersist, DataObjects, Data.DB, SimpleDS, Classes, DBGeneric, DLList,
  DBXpressInit;

type

  TDBXItemPersister = class(TItemPersister)
  protected
    function GetParentDB: TDBXDB;

    function FindItemByAnyID(Table: TSimpleDataSet; Blocks: TVarBlockArray; const Guid: TGuid): boolean;
      virtual; abstract;
    function CreateItemFromTableCursor(Table: TDataset): TKKeyedObject; virtual; abstract;
    procedure UpdateTableCursorFromItem(Table: TSimpleDataSet; Item: TKKeyedObject;
      const OwnerGUID: TGuid);
    procedure CreateTableRecordFromItem(Table: TSimpleDataSet; Item: TKKeyedObject;
      const OwnerGUID: TGuid);
    procedure UpdateCommon(Table: TSimpleDataSet; Item: TKKeyedObject; const OwnerGUID: TGuid);
      virtual; abstract;

    // Level-wise helpers for misc stuff.
    function GetKeyedListDataset(const ItemKey: TGuid; AlternateKeyField: string = ''): TDataset;
    procedure PutKeyedListDataset(Dataset: TDataset);

    function GetSingleItemMTB(Ctx: TObject; Blocks: TVarBlockArray; var Item: TKKeyedObject): boolean; override;
    function GetSingleItemMTG(Ctx: TObject; const ItemKey: TGuid; var Item: TKKeyedObject): boolean; override;
    function GetItemListForParentMT(Ctx: TObject; const ParentKey: TGuid; var ItemList: TKItemList): boolean; override;

    function ChangeSingleItemMT(Ctx: TObject; const OwnerKey: TGuid; Item: TKKeyedObject; ActionSet: TPersistActionSet)
      : boolean; override;
    function ChangeItemListMT(Ctx: TObject; const ParentKey: TGuid; ItemList: TKItemList; ActionSet: TPersistActionSet)
      : boolean; override;
    function ChangeItemListForOtherLevelMT(Ctx: TObject; const ParentKey: TGuid; ParentLevel: TKListLevel;
      ItemList: TKItemList; ActionSet: TPersistActionSet): boolean; override;
  public
    property ParentDB: TDBXDB read GetParentDB;
  end;

  TDBXUserProfilePersister = class(TDBXItemPersister)
  protected
    function FindItemByAnyID(Table: TSimpleDataSet; Blocks: TVarBlockArray; const Guid: TGuid)
      : boolean; override;
    function CreateItemFromTableCursor(Table: TDataset): TKKeyedObject; override;
    procedure UpdateCommon(Table: TSimpleDataSet; Item: TKKeyedObject; const OwnerGUID: TGuid); override;
    function GetListLevel: TKListLevel; override;
  end;

  TDBXMediaItemPersister = class(TDBXItemPersister)
  protected
    function FindItemByAnyID(Table: TSimpleDataSet; Blocks: TVarBlockArray; const Guid: TGuid)
      : boolean; override;
    function CreateItemFromTableCursor(Table: TDataset): TKKeyedObject; override;
    procedure UpdateCommon(Table: TSimpleDataSet; Item: TKKeyedObject; const OwnerGUID: TGuid); override;
    function GetListLevel: TKListLevel; override;
  end;

  TDBXCommentItemPersister = class(TDBXItemPersister)
  protected
    function FindItemByAnyID(Table: TSimpleDataSet; Blocks: TVarBlockArray; const Guid: TGuid)
      : boolean; override;
    function CreateItemFromTableCursor(Table: TDataset): TKKeyedObject; override;
    procedure UpdateCommon(Table: TSimpleDataSet; Item: TKKeyedObject; const OwnerGUID: TGuid); override;
    function GetListLevel: TKListLevel; override;
  end;

implementation

uses
  SysUtils, IndexedStore, DBXCommon, SQLExpr,
  Data.SqlTimSt, HTMLEscapeHelper, GlobalLog, DBSchemaStrs;

const
  S_UNKNOWN_TRANSACTION_ACTION = 'Unknown default action when finalising transaction.';
  S_UNKNOWN_SITE_TYPE = 'Unknown site type searching for record on non-primary key.';
  S_NO_KEY_SEARCH_INFO = 'No appropriate key information provided when doing a search.';

  { Helper variables and functions }

function GetStringFieldValue(SF: TField): string;
begin
  result := CheckedUTF8ToUnicode((SF as TStringField).Value);
end;

function GetMemoFieldValue(MF: TField): string;
begin
  result := CheckedUTF8ToUnicode((MF as TMemoField).Value);
end;

function GetIntFieldValue(MF: TField): integer;
begin
  result := (MF as TIntegerField).Value;
end;

procedure SetStringFieldValue(SF: TField; Val: string);
begin
  (SF as TStringField).Value := CheckedUnicodeToUTF8(Val);
end;

procedure SetMemoFieldValue(MF: TField; Val: string);
begin
  (MF as TMemoField).Value := CheckedUnicodeToUTF8(Val);
end;

procedure SetIntFieldValue(MF: TField; Val: integer);
begin
  (MF as TIntegerField).Value := Val;
end;

{ TDBXItemPersister }

function TDBXItemPersister.GetParentDB: TDBXDB;
begin
  result := TDBPersist(self).ParentDB as TDBXDB;
end;

function TDBXItemPersister.GetKeyedListDataset(const ItemKey: TGuid; AlternateKeyField: string)
  : TDataset;
var
  SQL, TableName, OwnerField: string;
begin
  case ListLevel of
    klUserList:
      begin
        Assert(ItemKey = TGuid.Empty);
        result := ParentDB.GetDataset(ListLevel);
      end;
    klMediaList, klCommentList:
      begin
        case ListLevel of
          klMediaList:
            begin
              TableName := S_MEDIA_TABLE;
              OwnerField := S_MEDIA_OWNER_KEY;
            end;
          klCommentList:
            begin
              TableName := S_COMMENT_TABLE;
              if Length(AlternateKeyField) = 0 then
                OwnerField := S_COMMENT_MEDIA_KEY
              else
                OwnerField := AlternateKeyField;
            end;
        end;
        SQL := 'SELECT * FROM ' + ParentDB.FieldQuoteChar + TableName + ParentDB.FieldQuoteChar;
        SQL := SQL + ' ' + 'WHERE ' + ParentDB.FieldQuoteChar + OwnerField + ParentDB.FieldQuoteChar
          + ' = ''' + GUIDToString(ItemKey) + '''';
        result := ParentDB.GetSQLQuery(SQL, ListLevel) as TSQLQuery;
      end;
  else
    Assert(false);
    result := nil;
  end;
end;


procedure TDBXItemPersister.PutKeyedListDataset(Dataset: TDataset);
begin
  case ListLevel of
    klUserList:
      ParentDB.PutDataset(Dataset);
    klMediaList, klCommentList:
      ParentDB.PutSQLQuery(Dataset);
  else
    Assert(false);
  end;
end;

procedure TDBXItemPersister.UpdateTableCursorFromItem(Table: TSimpleDataSet; Item: TKKeyedObject;
  const OwnerGUID: TGuid);
begin
  with Table do
  begin
    Edit;
    try
      UpdateCommon(Table, Item, OwnerGUID);
    finally
      Post;
    end;
  end;
end;

procedure TDBXItemPersister.CreateTableRecordFromItem(Table: TSimpleDataSet; Item: TKKeyedObject;
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

function TDBXItemPersister.GetSingleItemMTB(Ctx: TObject; Blocks: TVarBlockArray;
  var Item: TKKeyedObject): boolean;
var
  Table: TSimpleDataSet;
begin
  Item := nil;
  Table := ParentDB.GetDataset(ListLevel) as TSimpleDataSet;
  try
    result := FindItemByAnyID(Table, Blocks, TGuid.Empty);
    if result then
      Item := CreateItemFromTableCursor(Table);
  finally
    ParentDB.PutDataset(Table);
  end;
end;

function TDBXItemPersister.GetSingleItemMTG(Ctx: TObject; const ItemKey: TGuid; var Item: TKKeyedObject): boolean;
var
  Table: TSimpleDataSet;
  Blocks: TVarBlockArray;
begin
  result := false;
  Item := nil;
  if ItemKey = TGuid.Empty then
    exit;
  FillChar(Blocks, sizeof(Blocks), 0);
  Blocks.llTag := GetListLevel;
  Table := ParentDB.GetDataset(GetListLevel) as TSimpleDataSet;
  try
    result := FindItemByAnyID(Table, Blocks, ItemKey);
    if result then
      Item := CreateItemFromTableCursor(Table);
  finally
    ParentDB.PutDataset(Table);
  end;
end;

function TDBXItemPersister.GetItemListForParentMT(Ctx: TObject; const ParentKey: TGuid;
  var ItemList: TKItemList): boolean;
var
  Dataset: TDataset;
  Item: TKKeyedObject;
  OK: boolean;
begin
  ItemList := GetListClass.Create as TKIdList;
  Dataset := GetKeyedListDataset(ParentKey);
  try
    Dataset.First;
    while not Dataset.Eof do
    begin
      Item := CreateItemFromTableCursor(Dataset);
      OK := ItemList.Add(Item);
      Assert(OK);
      Dataset.Next;
    end;
    result := true;
  finally
    PutKeyedListDataset(Dataset);
  end;
end;

function TDBXItemPersister.ChangeSingleItemMT(Ctx: TObject; const OwnerKey: TGuid; Item: TKKeyedObject;
  ActionSet: TPersistActionSet): boolean;
var
  Action: TPersistAction;
  Table: TSimpleDataSet;
  Blocks: TVarBlockArray;
  NewGUID: TGUID;
  ChildPersister: TDBXItemPersister;
  NextLevel: TKListLevel;
  ChildList: TKItemList;
begin
  result := false;
  if not Assigned(Item) then
    exit;
  Table := ParentDB.GetDataset(ListLevel) as TSimpleDataSet;
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
                  ChildPersister := ParentDB.CreateItemPersister(NextLevel) as TDBXItemPersister;
                  ChildList := ChildPersister.ListClass.Create as TKItemList;
                  try
                    result := result and ChildPersister.ChangeItemListMT(nil, Item.Key, ChildList,
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
                  ChildPersister := ParentDB.CreateItemPersister(klCommentList) as TDBXItemPersister;
                  ChildList := ChildPersister.ListClass.Create as TKItemList;
                  try
                    result := result and ChildPersister.ChangeItemListForOtherLevelMT(nil, Item.Key,
                      ListLevel, ChildList, [patDelete]);
                  finally
                    ChildList.Free;
                    ChildPersister.Free;
                  end;
                end;
                if result then
                  Table.Delete;
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
    ParentDB.PutDataset(Table);
  end;
end;

function TDBXItemPersister.ChangeItemListMT(Ctx: TObject; const ParentKey: TGuid; ItemList: TKItemList;
  ActionSet: TPersistActionSet): boolean;
var
  Table: TSimpleDataSet;
  FilteredDataset: TDataset;
  ChildPersister: TDBXItemPersister;
  ChildList: TKItemList;
  NextLevel: TKListLevel;
  ActionGood: boolean;
  Action: TPersistAction;
  DeleteKeys: TStringList;
  Item, SrcItem: TKKeyedObject;
  Blocks: TVarBlockArray;
  NewGUID: TGUID;
  SubRes: boolean;
  DelKeyIdx: integer;
begin
  result := false;
  if not Assigned(ItemList) then
    exit;
  if (ParentKey = TGuid.Empty)  <> (ListLevel = klUserList) then
    exit;

  Table := ParentDB.GetDataset(ListLevel) as TSimpleDataSet;
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
              DeleteKeys := TStringList.Create;
              try
                // Work with the filtered dataset of the list of items
                // which are children of this parent key.
                FilteredDataset := GetKeyedListDataset(ParentKey);
                try
                  FilteredDataset.First;
                  while not FilteredDataset.Eof do
                  begin
                    Item := CreateItemFromTableCursor(FilteredDataset);
                    try
                      SrcItem := ItemList.SearchBySortVal(ksvKey, Item);
                      if not Assigned(SrcItem) then
                      begin
                        SubRes := true;
                        // Recursively delete lower level items in the tree.
                        if ListLevel < High(TKListLevel) then
                        begin
                          NextLevel := Succ(ListLevel);
                          ChildPersister := ParentDB.CreateItemPersister(NextLevel)
                            as TDBXItemPersister;
                          ChildList := ChildPersister.ListClass.Create as TKItemList;
                          try
                            SubRes := SubRes and ChildPersister.ChangeItemListMT(nil, Item.Key,
                              ChildList, [patDelete]);
                          finally
                            ChildList.Free;
                            ChildPersister.Free;
                          end;
                        end;
                        if ListLevel = klUserList then
                        begin
                          // Comments, feedback etc still owned by users, and need to be
                          // removed - currently comments only, might later be feedback too.
                          ChildPersister := ParentDB.CreateItemPersister(klCommentList)
                            as TDBXItemPersister;
                          ChildList := ChildPersister.ListClass.Create as TKItemList;
                          try
                            SubRes := SubRes and ChildPersister.ChangeItemListForOtherLevelMT
                              (nil, Item.Key, ListLevel, ChildList, [patDelete]);
                          finally
                            ChildList.Free;
                            ChildPersister.Free;
                          end;
                        end;
                        if SubRes then
                          DeleteKeys.Add(GuidToString(Item.Key));
                          //Shouldn't really use strings here, but it's simpler,
                          //and this code is old and legacy.
                        ActionGood := ActionGood and SubRes;
                      end;
                    finally
                      Item.Free;
                    end;
                    FilteredDataset.Next;
                  end;
                finally
                  PutKeyedListDataset(FilteredDataset);
                end;
                DelKeyIdx := 0;
                FillChar(Blocks, sizeof(Blocks), 0);
                Blocks.llTag := GetListLevel;
                while (DelKeyIdx < DeleteKeys.Count) and ActionGood do
                begin
                  if FindItemByAnyID(Table, Blocks, StringToGuid(DeleteKeys[DelKeyIdx])) then
                    Table.Delete
                  else
                    ActionGood := false;
                  Inc(DelKeyIdx);
                end;
              finally
                DeleteKeys.Free;
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
    ParentDB.PutDataset(Table);
  end;
end;

function TDBXItemPersister.ChangeItemListForOtherLevelMT(Ctx: TObject; const ParentKey: TGuid;
  ParentLevel: TKListLevel; ItemList: TKItemList; ActionSet: TPersistActionSet): boolean;
var
  Action: TPersistAction;
  ActionGood: boolean;
  DeleteKeys: TStringList;
  Table: TSimpleDataSet;
  FilteredTable: TDataset;
  Item, SrcItem: TKKeyedObject;
  DelKeyIdx: integer;
  Blocks: TVarBlockArray;
begin
  result := false;
  if not Assigned(ItemList) then
    exit;
  if ParentKey = TGuid.Empty then
    exit;
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
              DeleteKeys := TStringList.Create;
              Table := ParentDB.GetDataset(klCommentList) as TSimpleDataSet;
              try
                FilteredTable := GetKeyedListDataset(ParentKey, S_COMMENT_OWNER_KEY);
                try
                  FilteredTable.First;
                  while not FilteredTable.Eof do
                  begin
                    Item := CreateItemFromTableCursor(FilteredTable);
                    Assert(ListLevel = High(TKListLevel));
                    // So we don't need to delete child objects.
                    try
                      SrcItem := ItemList.SearchBySortVal(ksvKey, Item);
                      if not Assigned(SrcItem) then
                        DeleteKeys.Add(GuidToString(Item.Key));
                      FilteredTable.Next;
                    finally
                      Item.Free;
                    end;
                  end;
                finally
                  PutKeyedListDataset(FilteredTable);
                end;
                DelKeyIdx := 0;
                FillChar(Blocks, sizeof(Blocks), 0);
                Blocks.llTag := GetListLevel;
                while (DelKeyIdx < DeleteKeys.Count) and ActionGood do
                begin
                  if FindItemByAnyID(Table, Blocks, StringtoGuid(DeleteKeys[DelKeyIdx])) then
                    Table.Delete
                  else
                    ActionGood := false;
                  Inc(DelKeyIdx);
                end;
              finally
                DeleteKeys.Free;
                ParentDB.PutDataset(Table);
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
end;

{ TDBXUserProfilePersister }

function TDBXUserProfilePersister.FindItemByAnyID(Table: TSimpleDataSet; Blocks: TVarBlockArray;
  const Guid: TGuid): boolean;
var
  UBType: TKSiteType;
  SearchField: string;
  tried: boolean;
begin
  result := false;
  tried := false;
  // First try to search by GUID, then by uid, then username.
  // Criteria are that if an addressing or ID value is not null or empty,
  // then it should be a valid way of finding the user.

  if Guid <> TGuid.Empty then
  begin
    tried := true;
    Table.IndexName := S_INDEX_USER_TABLE_PRIMARY;
    result := Table.FindKey([GuidToString(Guid)])
  end
  else
  begin
    Assert(Blocks.llTag = klUserList);
    for UBType := Low(UBType) to High(UBType) do
    begin
      if Assigned(Blocks.UBlocks[UBType]) and Blocks.UBlocks[UBType].Valid then
      begin
        if Length(Blocks.UBlocks[UBType].UserId) > 0 then
        begin
          SearchField := UserBlockIndexNames[UBType, tuxUserId];
          tried := true;
          Table.IndexName := SearchField;
          result := Table.FindKey([Blocks.UBlocks[UBType].UserId]);
        end
        else if Length(Blocks.UBlocks[UBType].Username) > 0 then
        begin
          SearchField := UserBlockIndexNames[UBType, tuxUserName];
          tried := true;
          Table.IndexName := SearchField;
          result := Table.FindKey([Blocks.UBlocks[UBType].Username]);
        end;
      end;
    end;
  end;
  if not tried then
    raise EDBPErsistError.Create(S_NO_KEY_SEARCH_INFO);
end;

function TDBXUserProfilePersister.CreateItemFromTableCursor(Table: TDataset): TKKeyedObject;
var
  UBType: TKSiteType;
  r: TKUserProfile;
begin
  result := TKUserProfile.Create;
  r := result as TKUserProfile;
  with Table do
  begin
    result.Key := StringToGuid((FieldByName(S_USER_KEY) as TStringField).Value);
    for UBType := Low(UBType) to High(UBType) do
    begin
      r.SiteUserBlock[UBType].Username :=
        GetStringFieldValue(FieldByName(UserBlockFieldNames[UBType, tubUserName]));
      r.SiteUserBlock[UBType].UserId :=
        GetStringFieldValue(FieldByName(UserBlockFieldNames[UBType, tubUserId]));
      r.SiteUserBlock[UBType].Verified := (FieldByName(UserBlockFieldNames[UBType, tubVerified])
        as TSmallIntField).Value <> 0;
      r.SiteUserBlock[UBType].ProfilePicUrl :=
        GetMemoFieldValue(FieldByName(UserBlockFieldNames[UBType, tubProfUrl]));
      r.SiteUserBlock[UBType].FullName :=
        GetMemoFieldValue(FieldByName(UserBlockFieldNames[UBType, tubFullName]));
      r.SiteUserBlock[UBType].Bio :=
        GetMemoFieldValue(FieldByName(UserBlockFieldNames[UBType, tubBio]));
      r.SiteUserBlock[UBType].FollowsCount :=
        (FieldByName(UserBlockFieldNames[UBType, tubFollowsCount]) as TLargeintField).Value;
      r.SiteUserBlock[UBType].FollowerCount :=
        (FieldByName(UserBlockFieldNames[UBType, tubFollowerCount]) as TLargeintField).Value;

      if (Length(r.SiteUserBlock[UBType].Username) > 0) or
        (Length(r.SiteUserBlock[UBType].UserId) > 0) or
        (Length(r.SiteUserBlock[UBType].ProfilePicUrl) > 0) or
        (Length(r.SiteUserBlock[UBType].FullName) > 0) or (Length(r.SiteUserBlock[UBType].Bio) > 0)
        or (r.SiteUserBlock[UBType].FollowsCount <> 0) or
        (r.SiteUserBlock[UBType].FollowerCount <> 0) then
        r.SiteUserBlock[UBType].Valid := true;
    end;
    r.InterestLevel := TKProfileInterestLevel
      ((FieldByName(S_USER_INTEREST) as TSmallIntField).Value);
    if FieldByName(S_LAST_UPDATED) is TSQLTimeStampField then
      r.LastUpdated := SQLTimeStampToDateTime
        ((FieldByName(S_LAST_UPDATED) as TSQLTimeStampField).Value)
    else if FieldByName(S_LAST_UPDATED) is TFloatField then
      r.LastUpdated := (FieldByName(S_LAST_UPDATED) as TFloatField).Value
    else
      raise EDatabaseError.Create('Unknown field type');
  end;
  if result.Key = TGuid.Empty then
    raise EDatabaseError.Create('Read a user profile with a NULL GUID key ');
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'UTABLE read: ' + ' UKey: ' + r.Key + ' IUID: ' + r.SiteUserBlocks[tstInstagram]
    .UserId + ' IUName: ' + r.SiteUserBlocks[tstInstagram].Username + ' TUID: ' + r.SiteUserBlocks
    [tstTwitter].UserId + ' TUName: ' + r.SiteUserBlocks[tstTwitter].Username);
{$ENDIF}
end;

procedure TDBXUserProfilePersister.UpdateCommon(Table: TSimpleDataSet; Item: TKKeyedObject;
  const OwnerGUID: TGuid);
var
  UBType: TKSiteType;
  ReadUserKey: string;
  U: TKUserProfile;
begin
  U := Item as TKUserProfile;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'UTABLE update: ' + ' UKey: ' + U.Key + ' IUID: ' + U.SiteUserBlocks
    [tstInstagram].UserId + ' IUName: ' + U.SiteUserBlocks[tstInstagram].Username + ' TUID: ' +
    U.SiteUserBlocks[tstTwitter].UserId + ' TUName: ' + U.SiteUserBlocks[tstTwitter].Username);
{$ENDIF}
  with Table do
  begin
    ReadUserKey := (FieldByName(S_USER_KEY) as TStringField).Value;
    if (Length(ReadUserKey) > 0) then
    begin
      if U.Key <> TGuid.Empty then
        if U.Key <> StringtoGuid(ReadUserKey) then
          raise EDatabaseError.Create('User profile key appears to have changed.');
    end
    else
    begin
      if U.Key = TGuid.Empty then
        raise EDatabaseError.Create('Attempt to write empty user profile key into DB');
      SetStringFieldValue(FieldByName(S_USER_KEY), GuidToString(U.Key));
    end;

    for UBType := Low(UBType) to High(UBType) do
    begin
      if U.SiteUserBlock[UBType].Valid then
      begin
        SetStringFieldValue(FieldByName(UserBlockFieldNames[UBType, tubUserName]),
          U.SiteUserBlock[UBType].Username);
        SetStringFieldValue(FieldByName(UserBlockFieldNames[UBType, tubUserId]),
          U.SiteUserBlock[UBType].UserId);
        if U.SiteUserBlock[UBType].Verified then
          (FieldByName(UserBlockFieldNames[UBType, tubVerified]) as TSmallIntField).Value := 1
        else
          (FieldByName(UserBlockFieldNames[UBType, tubVerified]) as TSmallIntField).Value := 0;
        SetMemoFieldValue(FieldByName(UserBlockFieldNames[UBType, tubProfUrl]),
          U.SiteUserBlock[UBType].ProfilePicUrl);
        SetMemoFieldValue(FieldByName(UserBlockFieldNames[UBType, tubFullName]),
          U.SiteUserBlock[UBType].FullName);
        SetMemoFieldValue(FieldByName(UserBlockFieldNames[UBType, tubBio]),
          U.SiteUserBlock[UBType].Bio);
        (FieldByName(UserBlockFieldNames[UBType, tubFollowsCount]) as TLargeintField).Value :=
          U.SiteUserBlock[UBType].FollowsCount;
        (FieldByName(UserBlockFieldNames[UBType, tubFollowerCount]) as TLargeintField).Value :=
          U.SiteUserBlock[UBType].FollowerCount;
      end;
    end;
    (FieldByName(S_USER_INTEREST) as TSmallIntField).Value := Ord(U.InterestLevel);
    if FieldByName(S_LAST_UPDATED) is TSQLTimeStampField then
      (FieldByName(S_LAST_UPDATED) as TSQLTimeStampField).Value :=
        DateTimeToSQLTimeStamp(U.LastUpdated)
    else if FieldByName(S_LAST_UPDATED) is TFloatField then
      (FieldByName(S_LAST_UPDATED) as TFloatField).Value := U.LastUpdated
    else
      raise EDatabaseError.Create('Unknown field type');
  end;
end;

function TDBXUserProfilePersister.GetListLevel: TKListLevel;
begin
  result := klUserList;
end;

{ TDBXMediaItemPersister }

function TDBXMediaItemPersister.FindItemByAnyID(Table: TSimpleDataSet; Blocks: TVarBlockArray;
  const Guid: TGuid): boolean;
var
  MBType: TKSiteType;
  SearchField: string;
  tried: boolean;
begin
  result := false;
  tried := false;
  // First try to search by GUID, then by uid, then username.
  // Criteria are that if an addressing or ID value is not null or empty,
  // then it should be a valid way of finding the user.

  if Guid <> TGuid.Empty then
  begin
    tried := true;
    Table.IndexName := S_INDEX_MEDIA_TABLE_PRIMARY;
    result := Table.FindKey([GuidToString(Guid)]);
  end
  else
  begin
    Assert(Blocks.llTag = klMediaList);
    for MBType := Low(MBType) to High(MBType) do
    begin
      if Assigned(Blocks.MBlocks[MBType]) and Blocks.MBlocks[MBType].Valid then
      begin
        if Length(Blocks.MBlocks[MBType].MediaID) > 0 then
        begin
          SearchField := MediaBlockIndexNames[MBType, tmxMediaId];
          tried := true;
          Table.IndexName := SearchField;
          result := Table.FindKey([Blocks.MBlocks[MBType].MediaID])
        end
        else if Length(Blocks.MBlocks[MBType].MediaCode) > 0 then
        begin
          SearchField := MediaBlockIndexNames[MBType, tmxMediaCode];
          tried := true;
          Table.IndexName := SearchField;
          result := Table.FindKey([Blocks.MBlocks[MBType].MediaID]);
        end;
      end;
    end;
  end;
  if not tried then
    raise EDBPErsistError.Create(S_NO_KEY_SEARCH_INFO);
end;

function TDBXMediaItemPersister.CreateItemFromTableCursor(Table: TDataset): TKKeyedObject;
var
  MBType: TKSiteType;
  r: TKMediaItem;
begin
  result := TKMediaItem.Create;
  r := result as TKMediaItem;
  with Table do
  begin
    r.Key := StringToGuid(GetStringFieldValue(FieldByName(S_MEDIA_KEY)));
    if FieldByName(S_MEDIA_DATE) is TSQLTimeStampField then
      r.Date := SQLTimeStampToDateTime((FieldByName(S_MEDIA_DATE) as TSQLTimeStampField).Value)
    else if FieldByName(S_MEDIA_DATE) is TFloatField then
      r.Date := (FieldByName(S_MEDIA_DATE) as TFloatField).Value
    else
      raise EDatabaseError.Create('Unknown field type');
    r.MediaData := GetMemoFieldValue(FieldByName(S_MEDIA_DATA_STR));
    r.MediaType := TKMediaItemType(GetIntFieldValue(FieldByName(S_MEDIA_TYPE)));
    r.ResourceURL := GetMemoFieldValue(FieldByName(S_MEDIA_RESOURCE_URL));
    if FieldByName(S_LAST_UPDATED) is TSQLTimeStampField then
      r.LastUpdated := SQLTimeStampToDateTime
        ((FieldByName(S_LAST_UPDATED) as TSQLTimeStampField).Value)
    else if FieldByName(S_LAST_UPDATED) is TFloatField then
      r.LastUpdated := (FieldByName(S_LAST_UPDATED) as TFloatField).Value
    else
      raise EDatabaseError.Create('Unknown field type');
    for MBType := Low(MBType) to High(MBType) do
    begin
      r.SiteMediaBlock[MBType].MediaID :=
        GetStringFieldValue(FieldByName(MediaBlockFieldNames[MBType, tmbMediaId]));
      r.SiteMediaBlock[MBType].OwnerID :=
        GetStringFieldValue(FieldByName(MediaBlockFieldNames[MBType, tmbOwnerId]));
      r.SiteMediaBlock[MBType].MediaCode :=
        GetStringFieldValue(FieldByName(MediaBlockFieldNames[MBType, tmbMediaCode]));

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

procedure TDBXMediaItemPersister.UpdateCommon(Table: TSimpleDataSet; Item: TKKeyedObject;
  const OwnerGUID: TGuid);
var
  MBType: TKSiteType;
  ReadMediaKey, ReadMediaOwnerKey: string;
  M: TKMediaItem;
begin
  M := Item as TKMediaItem;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'MTABLE update: ' + ' MKey: ' + M.Key + ' UOwnerKey: ' + OwnerGUID + ' IMID: ' +
    M.SiteMediaBlocks[tstInstagram].MediaID + ' IMCode: ' + M.SiteMediaBlocks[tstInstagram]
    .MediaCode + ' TMID: ' + M.SiteMediaBlocks[tstTwitter].MediaID + ' TMCode: ' + M.SiteMediaBlocks
    [tstTwitter].MediaCode);
{$ENDIF}
  if OwnerGUID = TGuid.Empty then
    raise EDatabaseError.Create('Supplied owner GUID empty');
  with Table do
  begin
    ReadMediaKey := GetStringFieldValue(FieldByName(S_MEDIA_KEY));
    if Length(ReadMediaKey) > 0 then
    begin
      if M.Key <> TGuid.Empty then
        if M.Key <> StringToGuid(ReadMediaKey) then
          raise EDatabaseError.Create('Media item GUID key appears to have changed.');
    end
    else
    begin
      if M.Key = TGuid.Empty then
        raise EDatabaseError.Create('Attempt to write empty media item key into DB');
      SetStringFieldValue(FieldByName(S_MEDIA_KEY), GuidToString(M.Key));
    end;

    ReadMediaOwnerKey := GetStringFieldValue(FieldByName(S_MEDIA_OWNER_KEY));
    if Length(ReadMediaOwnerKey) > 0 then
    begin
      if OwnerGUID <> StringtoGuid(ReadMediaOwnerKey) then
        raise EDatabaseError.Create('Media item owner GUID key appears to have changed.');
    end
    else
      SetStringFieldValue(FieldByName(S_MEDIA_OWNER_KEY), GuidToString(OwnerGUID));

    if FieldByName(S_MEDIA_DATE) is TSQLTimeStampField then
      (FieldByName(S_MEDIA_DATE) as TSQLTimeStampField).Value := DateTimeToSQLTimeStamp(M.Date)
    else if FieldByName(S_MEDIA_DATE) is TFloatField then
      (FieldByName(S_MEDIA_DATE) as TFloatField).Value := M.Date
    else
      raise EDatabaseError.Create('Unknown field type');
    SetMemoFieldValue(FieldByName(S_MEDIA_DATA_STR), M.MediaData);
    SetIntFieldValue(FieldByName(S_MEDIA_TYPE), integer(M.MediaType));
    SetMemoFieldValue(FieldByName(S_MEDIA_RESOURCE_URL), M.ResourceURL);
    if FieldByName(S_LAST_UPDATED) is TSQLTimeStampField then
      (FieldByName(S_LAST_UPDATED) as TSQLTimeStampField).Value :=
        DateTimeToSQLTimeStamp(M.LastUpdated)
    else if FieldByName(S_LAST_UPDATED) is TFloatField then
      (FieldByName(S_LAST_UPDATED) as TFloatField).Value := M.LastUpdated
    else
      raise EDatabaseError.Create('Unknown field type');
    for MBType := Low(MBType) to High(MBType) do
    begin
      if M.SiteMediaBlock[MBType].Valid then
      begin
        SetStringFieldValue(FieldByName(MediaBlockFieldNames[MBType, tmbMediaId]),
          M.SiteMediaBlock[MBType].MediaID);
        SetStringFieldValue(FieldByName(MediaBlockFieldNames[MBType, tmbOwnerId]),
          M.SiteMediaBlock[MBType].OwnerID);
        SetStringFieldValue(FieldByName(MediaBlockFieldNames[MBType, tmbMediaCode]),
          M.SiteMediaBlock[MBType].MediaCode);
      end;
    end;
  end;
end;

function TDBXMediaItemPersister.GetListLevel: TKListLevel;
begin
  result := klMediaList;
end;

{ TDBXCommentItemPersister }

function TDBXCommentItemPersister.FindItemByAnyID(Table: TSimpleDataSet; Blocks: TVarBlockArray;
  const Guid: TGuid): boolean;
var
  CBType: TKSiteType;
  SearchField: string;
  tried: boolean;
begin
  result := false;
  tried := false;
  // First try to search by GUID, then by uid
  // Criteria are that if an addressing or ID value is not null or empty,
  // then it should be a valid way of finding the user.

  if Guid <> TGuid.Empty then
  begin
    tried := true;
    Table.IndexName := S_INDEX_COMMENT_TABLE_PRIMARY;
    result := Table.FindKey([GuidToString(Guid)]);
  end
  else
  begin
    Assert(Blocks.llTag = klCommentList);
    for CBType := Low(CBType) to High(CBType) do
    begin
      if Assigned(Blocks.CBlocks[CBType]) and Blocks.CBlocks[CBType].Valid then
      begin
        if Length(Blocks.CBlocks[CBType].CommentId) > 0 then
        begin
          SearchField := CommentBlockIndexNames[CBType, tcxCommentId];
          tried := true;
          Table.IndexName := SearchField;
          result := Table.FindKey([Blocks.CBlocks[CBType].CommentId]);
        end
      end;
    end;
  end;
  if not tried then
    raise EDBPErsistError.Create(S_NO_KEY_SEARCH_INFO);
end;

function TDBXCommentItemPersister.CreateItemFromTableCursor(Table: TDataset): TKKeyedObject;
var
  CBType: TKSiteType;
  C: TKCommentItem;
begin
  result := TKCommentItem.Create;
  C := result as TKCommentItem;
  with Table do
  begin
    C.Key := StringtoGuid(GetStringFieldValue(FieldByName(S_COMMENT_KEY)));
    if FieldByName(S_COMMENT_DATE) is TSQLTimeStampField then
      C.Date := SQLTimeStampToDateTime((FieldByName(S_COMMENT_DATE) as TSQLTimeStampField).Value)
    else if FieldByName(S_COMMENT_DATE) is TFloatField then
      C.Date := (FieldByName(S_COMMENT_DATE) as TFloatField).Value
    else
      raise EDatabaseError.Create('Unknown field type');
    C.CommentData := GetMemoFieldValue(FieldByName(S_COMMENT_DATA_STR));
    C.CommentType := TKCommentItemType(GetIntFieldValue(FieldByName(S_COMMENT_TYPE)));
    if FieldByName(S_LAST_UPDATED) is TSQLTimeStampField then
      C.LastUpdated := SQLTimeStampToDateTime
        ((FieldByName(S_LAST_UPDATED) as TSQLTimeStampField).Value)
    else if FieldByName(S_LAST_UPDATED) is TFloatField then
      C.LastUpdated := (FieldByName(S_LAST_UPDATED) as TFloatField).Value
    else
      raise EDatabaseError.Create('Unknown field type');
    for CBType := Low(CBType) to High(CBType) do
    begin
      C.SiteCommentBlock[CBType].CommentId :=
        GetStringFieldValue(FieldByName(CommentBlockFieldNames[CBType, tcbCommentId]));
      if Length(C.SiteCommentBlock[CBType].CommentId) > 0 then
        C.SiteCommentBlock[CBType].Valid := true;

      C.SiteUserBlock[CBType].UserId :=
        GetStringFieldValue(FieldByName(CommentBlockFieldNames[CBType, tcbCommentOwnerId]));
      if Length(C.SiteUserBlock[CBType].UserId) > 0 then
        C.SiteUserBlock[CBType].Valid := true;
    end;
    C.OwnerKey := StringtoGuid(GetStringFieldValue(FieldByName(S_COMMENT_OWNER_KEY)));
  end;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'CTABLE read: ' + ' CKey: ' + C.Key + ' CUserOwnerKey: ' + C.OwnerKey + ' ICID: '
    + C.SiteCommentBlocks[tstInstagram].CommentId + ' IUID: ' + C.SiteUserBlock[tstInstagram].UserId
    + ' TCID: ' + C.SiteCommentBlocks[tstTwitter].CommentId + ' TUID: ' + C.SiteUserBlock
    [tstTwitter].UserId);
{$ENDIF}
end;

procedure TDBXCommentItemPersister.UpdateCommon(Table: TSimpleDataSet; Item: TKKeyedObject;
  const OwnerGUID: TGuid);
var
  CBType: TKSiteType;
  ReadCommentKey, ReadCommentMediaKey, ReadCommentOwnerKey: string;
  C: TKCommentItem;
begin
  C := Item as TKCommentItem;
{$IFDEF DEBUG_DATABASE}
  GLogLog(SV_INFO, 'CTABLE update: ' + ' CKey: ' + C.Key + ' CMediaOwnerKey: ' + OwnerGUID +
    ' CUserOwnerKey: ' + C.OwnerKey + ' ICID: ' + C.SiteCommentBlocks[tstInstagram].CommentId +
    ' IUID: ' + C.SiteUserBlock[tstInstagram].UserId + ' TCID: ' + C.SiteCommentBlocks[tstTwitter]
    .CommentId + ' TUID: ' + C.SiteUserBlock[tstTwitter].UserId);
{$ENDIF}
  if OwnerGUID = TGuid.Empty then raise EDatabaseError.Create('Supplied owner GUID empty');
  with Table do
  begin
    ReadCommentKey := (FieldByName(S_COMMENT_KEY) as TStringField).Value;
    if Length(ReadCommentKey) > 0 then
    begin
      if C.Key <> TGuid.Empty then
        if C.Key <> StringToGuid(ReadCommentKey) then
          raise EDatabaseError.Create('Comment item GUID key appears to have changed.');
    end
    else
    begin
      if C.Key = TGuid.Empty then
        raise EDatabaseError.Create('Attempt to write empty comment item key into DB');
      SetStringFieldValue(FieldByName(S_COMMENT_KEY), GuidToString(C.Key));
    end;

    ReadCommentMediaKey := (FieldByName(S_COMMENT_MEDIA_KEY) as TStringField).Value;
    if Length(ReadCommentMediaKey) > 0 then
    begin
      if OwnerGUID <> StringToGuid(ReadCommentMediaKey) then
        raise EDatabaseError.Create('Comment item owning media GUID key appears to have changed.');
    end
    else
      SetStringFieldValue(FieldByName(S_COMMENT_MEDIA_KEY), GuidToString(OwnerGUID));

    ReadCommentOwnerKey := (FieldByName(S_COMMENT_OWNER_KEY) as TStringField).Value;
    if Length(ReadCommentOwnerKey) > 0 then
    begin
      if C.OwnerKey <> StringToGuid(ReadCommentOwnerKey) then
        raise EDatabaseError.Create('Comment item owning user GUID key appears to have changed.');
    end
    else
    begin
      if C.OwnerKey  = TGuid.Empty then
        raise EDatabaseError.Create('Comment item supplied owner key is empty.');
      SetStringFieldValue(FieldByName(S_COMMENT_OWNER_KEY), GuidToString(C.OwnerKey));
    end;

    for CBType := Low(CBType) to High(CBType) do
    begin
      if C.SiteCommentBlock[CBType].Valid then
        SetStringFieldValue(FieldByName(CommentBlockFieldNames[CBType, tcbCommentId]),
          C.SiteCommentBlock[CBType].CommentId);
      if C.SiteUserBlock[CBType].Valid then
        SetStringFieldValue(FieldByName(CommentBlockFieldNames[CBType, tcbCommentOwnerId]),
          C.SiteUserBlock[CBType].UserId);
    end;
    if FieldByName(S_COMMENT_DATE) is TSQLTimeStampField then
      (FieldByName(S_COMMENT_DATE) as TSQLTimeStampField).Value := DateTimeToSQLTimeStamp(C.Date)
    else if FieldByName(S_COMMENT_DATE) is TFloatField then
      (FieldByName(S_COMMENT_DATE) as TFloatField).Value := C.Date
    else
      raise EDatabaseError.Create('Unknown field type');
    SetMemoFieldValue(FieldByName(S_COMMENT_DATA_STR), C.CommentData);
    SetIntFieldValue(FieldByName(S_COMMENT_TYPE), integer(C.CommentType));
    if FieldByName(S_LAST_UPDATED) is TSQLTimeStampField then
      (FieldByName(S_LAST_UPDATED) as TSQLTimeStampField).Value :=
        DateTimeToSQLTimeStamp(C.LastUpdated)
    else if FieldByName(S_LAST_UPDATED) is TFloatField then
      (FieldByName(S_LAST_UPDATED) as TFloatField).Value := C.LastUpdated
    else
      raise EDatabaseError.Create('Unknown field type');
  end;
end;

function TDBXCommentItemPersister.GetListLevel: TKListLevel;
begin
  result := klCommentList;
end;

end.
