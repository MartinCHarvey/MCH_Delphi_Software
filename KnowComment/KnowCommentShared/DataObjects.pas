unit DataObjects;

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

// TODO - MetaLink fields need extra DB entry to link to original items.
// TODO - SiteUser / SiteMedia / SiteComment blocks to be abstracted
//        into keyed object IDBlocks.
// TODO - User, Media, Comment lists to be abstracted into Generic child lists.

uses CommonNodes, IndexedStore, Trackables;

type
  TKMergeHint = (tmhDontChangeInterest, tmhDontChangeMediaLastUpdated);
  TKMergeHintSet = set of TKMergeHint;

  TKSiteType = (tstInstagram, tstTwitter);
  TKSiteTypeStrings = array [TKSiteType] of string;

  TDBExpiryType =(dbxDate, dbxLastUpdated);

  TKList = class;
  TKUserList = class;
  TKMediaList = class;
  TKCommentList = class;

  // Thought about using TPersistent assign, but copying operations here
  // not quite the same as basic assignment.
{$IFDEF USE_TRACKABLES}

  TKDataObject = class(TTrackable)
{$ELSE}
  TKDataObject = class
{$ENDIF}
  public
    constructor Create; virtual;
    procedure Assign(Source: TKDataObject); virtual;
    class function Clone(Source: TKDataObject): TKDataObject;
    class function DeepClone(Source: TKDataObject): TKDataObject;
    procedure DeepAssign(Source: TKDataObject); virtual;
    function FixupFromDBPersisted(Persisted: TKDataObject): boolean; virtual;

    // Index nodes for insta etc currently guaranteed to be unique, but will not
    // always, so merge will need to consider whether certain key values NULL or
    // not. This will already have to happen for DB's.
    function MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean; virtual;

    // Sanity check checks only properties expected from importers, for a particular
    // site type. It does not check DB key consistency or other list etc referential integrity.
    function SanityCheck(SiteType: TKSiteType): boolean; virtual;
  end;

  TKDataClass = class of TKDataObject;

  // Common ----------------------------------------------

  TKKeyedObject = class(TKDataObject)
  private
    FSKey: TGuid;
    FLastUpdated: TDateTime;
    FRef: TObject;
  protected
    procedure SetKey(NewKey: TGuid);
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
    procedure SetRefsForListRecursive(List: TKList; Ref: TObject);
{$ENDIF}
  public
    destructor Destroy; override;
    procedure Assign(Source: TKDataObject); override;
    function MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean; override;
    function CloneSelfAndChildKeyed: TKKeyedObject; virtual;
    function FixupFromDBPersisted(Persisted: TKDataObject): boolean; override;
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
    procedure SetRefsRecursive(Ref: TObject); virtual;
{$ENDIF}
    property Key: TGuid read FSKey write SetKey;
    property LastUpdated: TDateTime read FLastUpdated write FLastUpdated;
    property Ref: TObject read FRef write FRef;
  end;

  // Users ----------------------------------------------

  TKSiteBlock = class(TKDataObject)
  protected
    FValid: boolean;
  public
    procedure Assign(Source: TKDataObject); override;
    function MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean; override;
    function FixupFromDBPersisted(Persisted: TKDataObject): boolean; override;
    property Valid: boolean read FValid write FValid;
    function SanityCheck(SiteType: TKSiteType): boolean; override;
  end;

  TKSiteUserBlock = class(TKSiteBlock)
  private
    FSUsername: string;
    FSUserId: string;
    FVerified: boolean;
    FSProfilePicUrl: string;
    FSFullName: string;
    FSBio: string;
    FFollowsCount: cardinal;
    FFollowerCount: cardinal;

    // Generic fields to handle loading, merging etc.

    // TODO - might not need this field.
    FOriginal: boolean;
    // Block is original attached to user profile or partial attached to comment.
  protected
  public
    constructor Create; override;
    procedure Assign(Source: TKDataObject); override;

    property Verified: boolean read FVerified write FVerified;
    property FollowerCount: cardinal read FFollowerCount write FFollowerCount;
    property FollowsCount: cardinal read FFollowsCount write FFollowsCount;
    property Username: string read FSUsername write FSUsername;
    property UserId: string read FSUserId write FSUserId;
    property ProfilePicUrl: string read FSProfilePicUrl write FSProfilePicUrl;
    property FullName: string read FSFullName write FSFullName;
    property Bio: string read FSBio write FSBio;
    function SanityCheck(SiteType: TKSiteType): boolean; override;
  end;

  TKUserBlockArray = array [TKSiteType] of TKSiteUserBlock;
  PKUserBlockArray = ^TKUSerBlockArray;

  TKProfileInterestLevel = (kpiFetchUserForRefs,
    // Expect to store user profile for ref integrity / 3rd party comments only.
    kpiTreeForInfo); // Expect to store entire tree of media and comments for user.

  // Any other config that's per-user to be stored as part of GUI / stream system,
  // and human readable.

  TKUserProfile = class(TKKeyedObject)
  private
    FSiteUserBlock: TKUserBlockArray;
    FMedia: TKMediaList;
    FInterestLevel: TKProfileInterestLevel;
  protected
    function GetSiteUserBlock(Idx: TKSiteType): TKSiteUserBlock;
    procedure SetSiteUserBlock(Idx: TKSiteType; NewBlock: TKSiteUserBlock);
  public
    constructor Create; override;
    destructor Destroy; override;
    procedure DeepAssign(Source: TKDataObject); override;
    procedure Assign(Source: TKDataObject); override;
    function MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean; override;
    function CloneSelfAndChildKeyed: TKKeyedObject; override;
    function FixupFromDBPersisted(Persisted: TKDataObject): boolean; override;
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
    procedure SetRefsRecursive(Ref: TObject); override;
{$ENDIF}

    property SiteUserBlocks: TKUserBlockArray read FSiteUserBlock;
    property SiteUserBlock[Idx: TKSiteType]: TKSiteUserBlock read GetSiteUserBlock
      write SetSiteUserBlock;
    property Media: TKMediaList read FMedia;
    property InterestLevel: TKProfileInterestLevel read FInterestLevel write FInterestLevel;
    function SanityCheck(SiteType: TKSiteType): boolean; override;
  end;

  // Media and comment lists ----------------------------------------------

  TKMediaItem = class;
  TKCommentItem = class;

  //TODO - Clean up TKextents / TKItemExtents,
  //and also TKItemList / TKIdList
  //See if smarter way of making extents optional on lists (only needed,
  //in the import merge process.

  TKExtents = class(TKDataObject)
  end;

  TKItemExtents = class(TKExtents)
  private
    FSFirstId: string;
    FSLastId: string;
    FHasEarlier: boolean;
    FHasLater: boolean;
    FSLinkId: string; // If we expect a later query to join, this is the
    // Id value for (before / after) we passed in to the query.
  public
    procedure Assign(Source: TKDataObject); override;
    function MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean; override;

    property HasEarlier: boolean read FHasEarlier write FHasEarlier;
    property HasLater: boolean read FHasLater write FHasLater;
    property FirstId: string read FSFirstId write FSFirstId;
    property LastId: string read FSLastId write FSLastId;
    property LinkId: string read FSLinkId write FSLinkId;
    function SanityCheck(SiteType: TKSiteType): boolean; override;
  end;

  //N.B If you change presentation order, be aware that some
  //parts of the UI logic expect interested and/or verified users
  //to be at the start of the list in this order.
  TKKeySortVal = (ksvPointer, ksvKey, ksvPresentationOrder, ksvIdentifierOrder);
  TKAdjacencyType = (katFirst, katLast, katNext, katPrevious);

  TKListLevel = (klUserList, klMediaList, klCommentList);
  TKListLevelStrings = array [TKListLevel] of string;
  TKListLevelSet = set of TKListLevel;

  TKList = class(TKDataObject)
  private
    FList: TIndexedStore;
    FParent: TKKeyedObject;
  protected
    property List: TIndexedStore read FList;
    procedure AssignHelper(Source: TKDataObject; Deep: boolean);
    function MergeHelper(MergeInCandidate: TKDataObject): TKDataObject; virtual;
    function IndexCheckGeneric(IndexTag: Int64): TISRetVal;
    function SearchGenericIRec(IndexTag: Int64; SearchVal: TIndexNode; var IRec: TItemRec)
      : TISRetVal;
    function SearchNearGenericIRec(IndexTag: Int64; SearchVal: TIndexNode; var IRec: TItemRec)
      : TISRetVal;
    function AdjacentGenericIRec(AdjType: TKAdjacencyType; IndexTag: Int64; var IRec: TItemRec)
      : TISRetVal;

    function SearchBySortValIRec(SortVal: TKKeySortVal; Item: TKDataObject; var IRec: TItemRec)
      : TISRetVal;
    function SearchNearBySortValIRec(SortVal: TKKeySortVal; SearchStr: string; var IRec: TItemRec)
      : TISRetVal;
    function GetCount: integer;
  public
    function GetListLevel: TKListLevel; virtual; abstract;
    class function CloneListAndItemsOnly(Source: TKDataObject): TKDataObject;
    procedure AssignTopItems(Source: TKDataObject); virtual;
    procedure DeepAssign(Source: TKDataObject); override;
    function FixupFromDBPersisted(Persisted: TKDataObject): boolean; override;

    constructor Create; override;
    destructor Destroy; override;

    function SearchBySortVal(SortVal: TKKeySortVal; Item: TKKeyedObject): TKKeyedObject;
    function SearchNearBySortVal(SortVal: TKKeySortVal; SearchStr: string): TKKeyedObject;
    function SearchByInternalKeyOnly(Key: TGuid): TKKeyedObject;
    function MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean; override;

    function AdjacentBySortVal(AdjType: TKAdjacencyType; SortVal: TKKeySortVal;
      Current: TKKeyedObject): TKKeyedObject;
    procedure Remove(Obj: TKKeyedObject);
    function Add(Obj: TKKeyedObject): boolean;
    procedure DeleteChildren;
    function SanityCheck(SiteType: TKSiteType): boolean; override;
    property Count: integer read GetCount;
    property Parent: TKKeyedObject read FParent;
  end;


  // TODO - Search Helpers etc etc for this and other classes.
  TKItemList = class(TKList)
  private
    FExtents: TKExtents;
  protected
    function MergeHelper(MergeInCandidate: TKDataObject): TKDataObject; override;
  public
    procedure DeepAssign(Source: TKDataObject); override;
    function SanityCheck(SiteType: TKSiteType): boolean; override;

    constructor Create; override;
    destructor Destroy; override;
    function MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean; override;
    property Extents: TKExtents read FExtents;
  end;

  TKIDTimeSortVal = (tsvLastModified, tsvDate);

  TKIdList = class(TKItemList)
  protected
    function SearchByDataBlockIRec(ST: TKSiteType; Item: TKKeyedObject; var IRec: TItemRec)
      : TISRetVal;
  public
    function SearchByDataBlock(ST: TKSiteType; ItemWithId: TKKeyedObject): TKKeyedObject;
    function MergeHelper(MergeInCandidate: TKDataObject): TKDataObject; override;
    function MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean; override;

    function AdjacentByTimeIndex(AdjType: TKAdjacencyType; SortVal: TKIDTimeSortVal;
      CurrentObject: TKKeyedObject): TKKeyedObject;

    procedure PrepareImport(ST: TKSiteType);
  end;

  TKMediaList = class(TKIdList)
  public
    function GetListLevel: TKListLevel; override;
  end;

  TKCommentList = class(TKIdList)
  public
    function GetListLevel: TKListLevel; override;

    function SearchByCommentOwnerKeyIRec(User: TKUserProfile; var IRec: TItemRec): TISRetVal;
    function SearchByCommentOwner(User: TKUserProfile): TKCommentItem;
  end;

  TKUserList = class(TKIdList)
  protected
    function SearchByUserNameIRec(ST: TKSiteType; Item: TKKeyedObject; var IRec: TItemRec)
      : TISRetVal;
  public
    function GetListLevel: TKListLevel; override;
    function SearchByUserName(ST: TKSiteType; Item: TKKeyedObject): TKUserProfile;
  end;

  // Media ----------------------------------------------

  TKSiteMediaBlock = class(TKSiteBlock)
  private
    FSMediaID: string;
    FSMediaCode: string;
    FSOwnerID: string;
  public
    procedure Assign(Source: TKDataObject); override;
    property MediaID: string read FSMediaID write FSMediaID;
    property MediaCode: string read FSMediaCode write FSMediaCode;
    property OwnerID: string read FSOwnerID write FSOwnerID;
    function SanityCheck(SiteType: TKSiteType): boolean; override;
  end;

  TKMediaBlockArray = array [TKSiteType] of TKSiteMediaBlock;

  //Plain text - instagram.
  //HTML - twitter tweet
  //mitMetaLink - twitter retweet
  //mitHTMLWithQuote - quoted tweet (TODO).
  TKMediaItemType = (mitPlainText, mitHTML, mitMetaLink, mitHTMLWithQuote);

  TKMediaItem = class(TKKeyedObject)
  private
    FSiteMediaBlock: TKMediaBlockArray;
    FDate: TDateTime;
    FMediaType: TKMediaItemType;
    FSMediaData: string;
    FSResourceURL: string;
    FComments: TKCommentList;
  protected
    function GetSiteMediaBlock(Idx: TKSiteType): TKSiteMediaBlock;
  public
    constructor Create; override;
    destructor Destroy; override;
    procedure Assign(Source: TKDataObject); override;
    procedure DeepAssign(Source: TKDataObject); override;
    function MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean; override;
    function CloneSelfAndChildKeyed: TKKeyedObject; override;
    function FixupFromDBPersisted(Persisted: TKDataObject): boolean; override;
    function SanityCheck(SiteType: TKSiteType): boolean; override;
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
    procedure SetRefsRecursive(Ref: TObject); override;
{$ENDIF}

    property SiteMediaBlocks: TKMediaBlockArray read FSiteMediaBlock;
    property SiteMediaBlock[Idx: TKSiteType]: TKSiteMediaBlock read GetSiteMediaBlock;
    property Date: TDateTime read FDate write FDate;
    property MediaType: TKMediaItemType read FMediaType write FMediaType;
    property Comments: TKCommentList read FComments;
    property ResourceURL: string read FSResourceURL write FSResourceURL;
    property MediaData: string read FSMediaData write FSMediaData;
  end;

  // Comments ----------------------------------------------

  TKSiteCommentBlock = class(TKSiteBlock)
  private
    FSCommentId: string;
  protected
  public
    procedure Assign(Source: TKDataObject); override;
    property CommentId: string read FSCommentId write FSCommentId;
    function SanityCheck(SiteType: TKSiteType): boolean; override;
  end;

  TKCommentBlockArray = array [TKSiteType] of TKSiteCommentBlock;

  TKCommentItemType = (citPlainText, citMetaLink);

  TKCommentItem = class(TKKeyedObject)
  private
    FDate: TDateTime;
    FSiteUserBlock: TKUserBlockArray;
    FSiteCommentBlock: TKCommentBlockArray;
    FCommentType: TKCommentItemType;
    FSOwnerKey: TGuid;
    FSCommentStrData: string;
  protected
    function GetSiteUserBlock(Idx: TKSiteType): TKSiteUserBlock;
    function GetSiteCommentBlock(Idx: TKSiteType): TKSiteCommentBlock;
  public
    constructor Create; override;
    destructor Destroy; override;
    procedure Assign(Source: TKDataObject); override;
    procedure DeepAssign(Source: TKDataObject); override;
    function MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean; override;
    function FixupFromDBPersisted(Persisted: TKDataObject): boolean; override;
    function SanityCheck(SiteType: TKSiteType): boolean; override;

    property Date: TDateTime read FDate write FDate;
    property SiteUserBlocks: TKUserBlockArray read FSiteUserBlock;
    property SiteUserBlock[Idx: TKSiteType]: TKSiteUserBlock read GetSiteUserBlock;
    property SiteCommentBlocks: TKCommentBlockArray read FSiteCommentBlock;
    property SiteCommentBlock[Idx: TKSiteType]: TKSiteCommentBlock read GetSiteCommentBlock;
    property CommentType: TKCommentItemType read FCommentType write FCommentType;
    property CommentData: string read FSCommentStrData write FSCommentStrData;
    property OwnerKey: TGuid read FSOwnerKey write FSOwnerKey;
  end;

  // Indices and other supplementary classes ---------------------

  TKIndexTag = (tkPointer, tkInternalKey, tkInstaUserId, tkInstaMediaId, tkInstaCommentId,
    tkInstaUserName, tkTwitterUserId, tkTwitterMediaId, tkTwitterCommentId, tkTwitterUserName,
    tkUserLastUpdated, tkMediaDate, tkMediaLastUpdated, tkCommentDate, tkCommentLastUpdated,
    tkCommentOwnerKey,
    tkUserPresOrder, tkMediaPresOrder, tkCommentPresOrder,
    tkUserIdentifierOrder, tkMediaIdentifierOrder, tkCommentIdentifierOrder);

  TKStringIndex = class(TDuplicateValIndexNode)
  protected
    function GetSearchString: string; virtual; abstract;
    procedure SetSearchString(S: string); virtual; abstract;
  public
    property SearchString: string read GetSearchString write SetSearchString;
  end;

  TKKeyedIndex = class(TDuplicateValIndexNode)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
  end;

  TKKeyedIndexSearchVal = class(TKKeyedIndex)
  private
    FKeyedObject: TKKeyedObject;
  protected
    function GetSearchKey: TGuid;
    procedure SetSearchKey(NewKey: TGuid);
  public
    constructor Create; override;
    destructor Destroy; override;
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
    property SearchKey: TGuid read GetSearchKey write SetSearchKey;
  end;

  TKBySiteIndex = class(TKStringIndex)
  protected
    function GetSiteType(out ST: TKSiteType): boolean;
    procedure SetSearchSiteType(ST: TKSiteType); virtual; abstract;
    function GetSearchSiteType: TKSiteType; virtual; abstract;
  public
    // To be set only when passing search vals in.
    property SearchSiteType: TKSiteType read GetSearchSiteType write SetSearchSiteType;
  end;

  TKIdIndex = class(TKBySiteIndex)
  protected
    procedure SetSearchListLevel(LL: TKListLevel); virtual; abstract;
    function GetSearchListLevel: TKListLevel; virtual; abstract;
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
    // As determined from index tags.
    function GetListLevel(out TL: TKListLevel): boolean;
  public
    property SearchListLevel: TKListLevel read GetSearchListLevel write SetSearchListLevel;
  end;

  TKIdIndexSearchVal = class(TKIdIndex)
  private
    FSearchUser: TKUserProfile;
    FSearchMedia: TKMediaItem;
    FSearchComment: TKCommentItem;

    FSearchSiteType: TKSiteType;
    FSearchSiteTypeSet: boolean;
    FSearchListLevel: TKListLevel;
    FSearchListLevelSet: boolean;
  protected
    procedure SetSearchSiteType(ST: TKSiteType); override;
    function GetSearchSiteType: TKSiteType; override;
    procedure SetSearchListLevel(LL: TKListLevel); override;
    function GetSearchListLevel: TKListLevel; override;
    function GetSearchString: string; override;
    procedure SetSearchString(S: string); override;
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
  public
    destructor Destroy; override;
  end;

  TKUserNameIndex = class(TKBySiteIndex)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
  end;

  TKUserNameSearchVal = class(TKUserNameIndex)
  private
    FSearchUserProf: TKUserProfile;
    FSearchSiteType: TKSiteType;
    FSearchSiteTypeSet: boolean;
  protected
    procedure SetSearchSiteType(ST: TKSiteType); override;
    function GetSearchSiteType: TKSiteType; override;
    function GetSearchString: string; override;
    procedure SetSearchString(S: string); override;
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
  public
    constructor Create; override;
    destructor Destroy; override;
  end;

  TKDateSearchField = (dsfDate, dsfLastUpdated);

  TKGeneralListIndex = class(TDuplicateValIndexNode)
  protected
    function GetListLevel(out TL: TKListLevel): boolean;
  end;

  TKDateIndex = class(TKGeneralListIndex)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
    // As determined from index tags.
    function GetDateSearchField(out DSF: TKDateSearchField): boolean;
  end;

  TKDateIndexSearchVal = class(TKDateIndex)
  private
    FSearchUser: TKUserProfile;
    FSearchMedia: TKMediaItem;
    FSearchComment: TKCommentItem;

    FSearchListLevel: TKListLevel;
    FSearchListLevelSet: boolean;
    FSearchDateField: TKDateSearchField;
    FSearchDateFieldSet: boolean;
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
    procedure SetSearchListLevel(LL: TKListLevel);
    function GetSearchListLevel: TKListLevel;
    procedure SetSearchDateField(DSF: TKDateSearchField);
    function GetSearchDateField: TKDateSearchField;
    function GetSearchDate: TDateTime;
    procedure SetSearchDate(NewDate: TDateTime);
  public
    constructor Create; override;
    destructor Destroy; override;
    property SearchListLevel: TKListLevel read GetSearchListLevel write SetSearchListLevel;
    property SearchDateField: TKDateSearchField read GetSearchDateField write SetSearchDateField;
    property SearchDate: TDateTime read GetSearchDate write SetSearchDate;
  end;

  //N.B If you change presentation order, be aware that some
  //parts of the UI logic expects interested and/or verified users
  //to be at the start of the list in this order.
  TKPresentationOrderIndex = class(TKGeneralListIndex)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
  end;

  TKIdentifierOrderIndex = class(TKGeneralListIndex)
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
  end;

  TKIdentifierOrderSearchVal = class(TKIdentifierOrderIndex)
  private
    FSearchString: string;
  protected
    function CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
      : integer; override;
  public
    property SearchString: string read FSearchString write FSearchString;
  end;

  //Datatypes for custom key queries.
  //Initial filtering by some sort of key,
  //later filtering according to some custom filter.

  //If none, supplied key should be blank, else should be a valid key.
  TCustomKeyMeaning = (ckmNone, ckmItemParentKey, ckmCommentOwnerKey);

{$IFDEF USE_TRACKABLES}
  TAdditionalFilter = class(TTrackable)
{$ELSE}
  TAdditionalFilter = class
{$ENDIF}
  public
    function FilterAllowKeyedObject(Obj: TKKeyedObject):boolean; virtual; abstract;
  end;

  //TODO - Some duplication in index node classes of GetListLevel function.
  //Might re-org class heirarchy?

procedure CloneUserBlockArray(const InBlocks: TKUserBlockArray; var OutBlocks: TKUserBlockArray);

procedure CloneMediaBlockArray(const InBlocks: TKMediaBlockArray; var OutBlocks: TKMediaBlockArray);

procedure CloneCommentBlockArray(const InBlocks: TKCommentBlockArray;
  var OutBlocks: TKCommentBlockArray);

function IndexTagToSiteType(Tag: TKIndexTag): TKSiteType;
function IndexTagToListLevel(Tag: TKIndexTag): TKListLevel;

procedure SiteTypeAndLevelToIdIndexTag(ST: TKSiteType; LV: TKListLevel; out Tag: TKIndexTag);

procedure SiteTypeToUsernameIndexTag(ST: TKSiteType; out Tag: TKIndexTag);

procedure LevelAndSearchFieldToDateTag(LV: TKListLevel; SF: TKDateSearchField; out Tag: TKIndexTag);

procedure IndexTagToDateSearchField(Tag: TKIndexTag; out SF: TKDateSearchField);

procedure LevelToPresentationOrderTag(LV: TKListLevel; out Tag: TKIndexTag);

procedure LevelToIdentifierOrderTag(LV: TKListLevel; out Tag: TKIndexTag);

function GenMetaLink(DestST: TKSiteType; DestLV: TKListLevel; ItemId: string;
  OwningUID: string): string;

function ParseMetaLink(MetaString: string; var DestST: TKSiteType; var DestLV: TKListLevel;
  var ItemId: string; var OwningUID: string): boolean;

function GetItemClass(ListLevel: TKListLevel): TKDataClass;
function GetListClass(ListLevel: TKListLevel): TKDataClass;

//Profile verified or interested.
function ProfileV(SrcUP: TKUserProfile): boolean;
function ProfileVorI(SrcUP: TKUserProfile): boolean;

//Create an identifier for quick list searches.
function IdentifierStringFromItem(Item: TKKeyedObject; LL: TKListLevel): string;

const
  SiteTypeStrings: TKSiteTypeStrings = ('tstInstagram', 'tstTwitter');
  ListLevelStrings: TKListLevelStrings = ('klUserList', 'klMediaList', 'klCommentList');

implementation

uses
  SysUtils, KKUtils, GlobalLog, MemDBMisc;

const
  S_ADJACENT_PASSED_PREVIOUS = 'First/Next function passed previous value when not needed, check.';

  // TODO - Virtual constructors for TKDataClasses.
function GetItemClass(ListLevel: TKListLevel): TKDataClass;
begin
  case ListLevel of
    klUserList:
      result := TKUserProfile;
    klMediaList:
      result := TKMediaItem;
    klCommentList:
      result := TKCommentItem;
  else
    Assert(false);
    result := nil;
  end;
end;

function GetListClass(ListLevel: TKListLevel): TKDataClass;
begin
  case ListLevel of
    klUserList:
      result := TKUserList;
    klMediaList:
      result := TKMediaList;
    klCommentList:
      result := TKCommentList;
  else
    Assert(false);
    result := nil;
  end;
end;

function IndexTagToSiteType(Tag: TKIndexTag): TKSiteType;
begin
  case Tag of
    tkInstaUserId, tkInstaMediaId, tkInstaCommentId, tkInstaUserName:
      result := tstInstagram;
    tkTwitterUserId, tkTwitterMediaId, tkTwitterCommentId, tkTwitterUserName:
      result := tstTwitter;
  else
    Assert(false);
    result := tstInstagram;
  end;
end;

function IndexTagToListLevel(Tag: TKIndexTag): TKListLevel;
begin
  case Tag of
    tkInstaUserId, tkTwitterUserId, tkUserLastUpdated, tkUserPresOrder, tkUserIdentifierOrder:
      result := klUserList;
    tkInstaMediaId, tkTwitterMediaId, tkMediaDate, tkMediaLastUpdated, tkMediaPresOrder, tkMediaIdentifierOrder:
      result := klMediaList;
    tkInstaCommentId, tkTwitterCommentId, tkCommentDate, tkCommentLastUpdated, tkCommentPresOrder, tkCommentIdentifierOrder:
      result := klCommentList;
  else
    Assert(false);
    result := klUserList;
  end;
end;

procedure SiteTypeAndLevelToIdIndexTag(ST: TKSiteType; LV: TKListLevel; out Tag: TKIndexTag);
begin
  case LV of
    klUserList:
      begin
        case ST of
          tstInstagram:
            Tag := tkInstaUserId;
          tstTwitter:
            Tag := tkTwitterUserId;
        else
          Assert(false);
        end;
      end;
    klMediaList:
      begin
        case ST of
          tstInstagram:
            Tag := tkInstaMediaId;
          tstTwitter:
            Tag := tkTwitterMediaId;
        else
          Assert(false);
        end;
      end;
    klCommentList:
      begin
        case ST of
          tstInstagram:
            Tag := tkInstaCommentId;
          tstTwitter:
            Tag := tkTwitterCommentId;
        else
          Assert(false);
        end;
      end;
  else
    Assert(false);
  end;
end;

procedure SiteTypeToUsernameIndexTag(ST: TKSiteType; out Tag: TKIndexTag);
begin
  case ST of
    tstInstagram:
      Tag := tkInstaUserName;
    tstTwitter:
      Tag := tkTwitterUserName;
  else
    Assert(false);
  end;
end;

procedure LevelAndSearchFieldToDateTag(LV: TKListLevel; SF: TKDateSearchField; out Tag: TKIndexTag);
begin
  case LV of
    klUserList:
      begin
        case SF of
          dsfLastUpdated:
            Tag := tkUserLastUpdated;
        else
          Assert(false);
        end;
      end;
    klMediaList:
      begin
        case SF of
          dsfDate:
            Tag := tkMediaDate;
          dsfLastUpdated:
            Tag := tkMediaLastUpdated;
        else
          Assert(false);
        end;
      end;
    klCommentList:
      begin
        case SF of
          dsfDate:
            Tag := tkCommentDate;
          dsfLastUpdated:
            Tag := tkCommentLastUpdated;
        end;
      end;
  else
    Assert(false);
  end;
end;

procedure LevelToPresentationOrderTag(LV: TKListLevel; out Tag: TKIndexTag);
begin
  case LV of
    klUserList:  Tag := tkUserPresOrder;
    klMediaList: Tag := tkMediaPresOrder;
    klCommentList: Tag := tkCommentPresOrder;
  else
    Assert(false);
  end;
end;

procedure LevelToIdentifierOrderTag(LV: TKListLevel; out Tag: TKIndexTag);
begin
  case LV of
    klUserList: Tag := tkUserIdentifierOrder;
    klMediaList: Tag := tkMediaIdentifierOrder;
    klCommentList: Tag := tkCommentIdentifierOrder;
  else
    Assert(false);
  end;
end;

procedure IndexTagToDateSearchField(Tag: TKIndexTag; out SF: TKDateSearchField);
begin
  case Tag of
    tkMediaDate, tkCommentDate:
      SF := dsfDate;
    tkUserLastUpdated, tkMediaLastUpdated, tkCommentLastUpdated:
      SF := dsfLastUpdated;
  else
    Assert(false);
  end;
end;

{ Block array cloning functions }

procedure CloneUserBlockArray(const InBlocks: TKUserBlockArray; var OutBlocks: TKUserBlockArray);
var
  ST: TKSiteType;
begin
  for ST := Low(ST) to High(ST) do
  begin
    if Assigned(OutBlocks[ST]) then
      OutBlocks[ST].Free;
    OutBlocks[ST] := TKDataObject.DeepClone(InBlocks[ST]) as TKSiteUserBlock;
  end;
end;

procedure CloneMediaBlockArray(const InBlocks: TKMediaBlockArray; var OutBlocks: TKMediaBlockArray);
var
  ST: TKSiteType;
begin
  for ST := Low(ST) to High(ST) do
  begin
    if Assigned(OutBlocks[ST]) then
      OutBlocks[ST].Free;
    OutBlocks[ST] := TKDataObject.DeepClone(InBlocks[ST]) as TKSiteMediaBlock;
  end;
end;

procedure CloneCommentBlockArray(const InBlocks: TKCommentBlockArray;
  var OutBlocks: TKCommentBlockArray);
var
  ST: TKSiteType;
begin
  for ST := Low(ST) to High(ST) do
  begin
    if Assigned(OutBlocks[ST]) then
      OutBlocks[ST].Free;
    OutBlocks[ST] := TKDataObject.DeepClone(InBlocks[ST]) as TKSiteCommentBlock;
  end;
end;

const
  S_META_INSTA = 'meta-insta';
  S_META_TWITTER = 'meta-twitter';
  S_META_USERLIST = 'userlist-id';
  S_META_MEDIALIST = 'medialist-id';
  S_META_COMMENTLIST = 'commentlist-id';
  S_OWNING_UID = 'owning-uid';

function GenMetaLink(DestST: TKSiteType; DestLV: TKListLevel; ItemId: string;
  OwningUID: string): string;
begin
  case DestST of
    tstInstagram:
      result := S_META_INSTA;
    tstTwitter:
      result := S_META_TWITTER;
  else
    Assert(false);
  end;
  result := result + ':';
  case DestLV of
    klUserList:
      result := result + S_META_USERLIST;
    klMediaList:
      result := result + S_META_MEDIALIST;
    klCommentList:
      result := result + S_META_COMMENTLIST;
  else
    Assert(false);
  end;
  result := result + ':';
  result := result + ItemId;
  result := result + ':';
  result := result + S_OWNING_UID;
  result := result + ':';
  result := result + OwningUID;
end;

function ParseMetaLink(MetaString: string; var DestST: TKSiteType; var DestLV: TKListLevel;
  var ItemId: string; var OwningUID: string): boolean;
var
  ColonPos: integer;
  Val: string;
  i: integer;
begin
  result := true;
  for i := 0 to 4 do
  begin
    if i < 4 then
    begin
      ColonPos := MetaString.IndexOf(':');
      if ColonPos > 0 then
      begin
        Val := MetaString.Substring(0, ColonPos);
        MetaString := MetaString.Substring(ColonPos + 1);
      end;
    end
    else
      Val := MetaString;
    case i of
      0:
        begin
          if Val = S_META_INSTA then
            DestST := tstInstagram
          else if Val = S_META_TWITTER then
            DestST := tstTwitter
          else
          begin
            result := false;
            exit;
          end;
        end;
      1:
        begin
          if Val = S_META_USERLIST then
            DestLV := klUserList
          else if Val = S_META_MEDIALIST then
            DestLV := klMediaList
          else if Val = S_META_COMMENTLIST then
            DestLV := klCommentList
          else
          begin
            result := false;
            exit;
          end;
        end;
      2:
        ItemId := Val;
      3:
        if Val <> S_OWNING_UID then
        begin
          result := false;
          exit;
        end;
      4:
        OwningUID := Val;
    else
      Assert(false);
    end;
  end;
end;

function IdentifierStringFromItem(Item: TKKeyedObject; LL: TKListLevel): string;
var
  UP: TKUSerProfile;
  MI: TKMediaItem;
  CI: TKCommentItem;
  ST: TKSiteType;
  BaseString: string;
  i,x: integer;
begin
  case LL of
    klUserList:
    begin
      UP:= Item as TKUserProfile;
      for ST := Low(ST) to High(ST) do
      begin
        if UP.SiteUserBlock[ST].Valid and (Length(UP.SiteUserBlock[ST].Username) > 0) then
        begin
          result := UP.SiteUserBlock[ST].Username;
          exit; //No need for any further string processing.
        end;
      end;
    end;
    klMediaList:
    begin
      MI := Item as TKMediaItem;
      BaseString := MI.MediaData;
      Assert(MI.MediaType = mitPlainText); //TODO - Convert HTML back to text, metalinks.
    end;
    klCommentList:
    begin
      CI := Item as TKCommentItem;
      BaseString := CI.CommentData;
      Assert(CI.CommentType = citPlainText); //TODO - metalink handling.
    end
  else
    Assert(false);
    result := '';
  end;
  Trim(BaseString);
  x := Length(BaseString);
  i := Pos(' ', BaseString);
  if (i > 0) and (i < x) then x := i;
  i := Pos(#9, BaseString);
  if (i > 0) and (i < x) then x := i;
  i := Pos(#13, BaseString);
  if (i > 0) and (i < x) then x := i;
  i := Pos(#10, BaseString);
  if (i > 0) and (i < x) then x := i;
  result := BaseString.Substring(0, Pred(i));
end;

{ TKDataObject }

function TKDataObject.SanityCheck(SiteType: TKSiteType): boolean;
begin
  result := true;
end;

constructor TKDataObject.Create;
begin
  inherited; // Empty constructor necessary for TKDataClass magic.
end;

procedure TKDataObject.Assign(Source: TKDataObject);
begin
  // Derived classes should copy local fields only.
end;

class function TKDataObject.Clone(Source: TKDataObject): TKDataObject;
var
  SrcClass: TKDataClass;
begin
  if not Assigned(Source) then
    result := nil
  else
  begin
    Assert(Source.ClassType.InheritsFrom(TKDataObject));
    SrcClass := TKDataClass(Source.ClassType);
    result := SrcClass.Create;
    result.Assign(Source);
  end;
end;

class function TKDataObject.DeepClone(Source: TKDataObject): TKDataObject;
var
  SrcClass: TKDataClass;
begin
  if not Assigned(Source) then
    result := nil
  else
  begin
    Assert(Source.ClassType.InheritsFrom(TKDataObject));
    SrcClass := TKDataClass(Source.ClassType);
    result := SrcClass.Create;
    result.DeepAssign(Source);
  end;
end;

procedure TKDataObject.DeepAssign(Source: TKDataObject);
begin
  Assign(Source);
  // Derived classes should call inherited first, and then clone.
end;

function TKDataObject.MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet): boolean;
begin
  result := true;
end;

function TKDataObject.FixupFromDBPersisted(Persisted: TKDataObject): boolean;
begin
  result := Assigned(Persisted);
end;

{ TKKeyedObject }

{$IFDEF DEBUG_OBJECT_OWNERSHIP}
procedure TKKeyedObject.SetRefsForListRecursive(List: TKList; Ref: TObject);
var
  IRec: TItemRec;
  RV: TISRetVal;
begin
  IRec := List.FList.GetAnItem;
  while Assigned(IRec) do
  begin
    (IRec.Item as TKKeyedObject).SetRefsRecursive(Ref);
    List.FList.GetAnotherItem(IRec);
  end;
end;

procedure TKKeyedObject.SetRefsRecursive(Ref: TObject);
begin
  self.Ref := Ref;
end;
{$ENDIF}

destructor TKKeyedObject.Destroy;
begin
{$IFDEF DEBUG_OBJECT_OWNERSHIP}
  Assert(not Assigned(Ref));
{$ENDIF}
  inherited;
end;

procedure TKKeyedObject.SetKey(NewKey: TGuid);
begin
  Assert((not (NewKey = TGuid.Empty)) or (FSKey = TGuid.Empty));
  FSKey := NewKey;
end;

procedure TKKeyedObject.Assign(Source: TKDataObject);
var
  Src: TKKeyedObject;
begin
  inherited;
  Src := Source as TKKeyedObject;
  Key := Src.Key;
  FLastUpdated := Src.FLastUpdated;
end;

function TKKeyedObject.MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet): boolean;
var
  N: TKKeyedObject;
begin
  result := inherited;
  if not result then
    exit;

  N := Newer as TKKeyedObject;
  if Key = TGuid.Empty then
  begin
    if N.Key <>  TGuid.Empty then
      Key := N.Key;
  end
  else if N.Key <>  TGuid.Empty then
  begin
    if Key <> N.Key then
    begin
      Assert(false);
      result := false;
      exit;
    end;
  end;
  if (N.FLastUpdated <> 0) then
  begin
    if N.LastUpdated < LastUpdated then
    begin
      result := false;
      exit;
    end;
    if not((tmhDontChangeMediaLastUpdated in HintSet) and (Self is TKMediaItem)) then
    begin
      if (N.FLastUpdated > LastUpdated) then
        LastUpdated := N.LastUpdated;
    end;
  end;
end;


function TKKeyedObject.FixupFromDBPersisted(Persisted: TKDataObject): boolean;
var
  P: TKKeyedObject;
begin
  result := inherited;
  if not result then exit;

  P := Persisted as TKKeyedObject;
  if Key = TGuid.Empty then
  begin
    if P.Key <> TGuid.Empty then
      Key := P.Key;
  end
  else if P.Key <> TGuid.Empty then
  begin
    if Key <> P.Key then
    begin
      Assert(false);
      result := false;
      exit;
    end;
  end;
  // Don't change. FLastUpdated: TDateTime;
  // Don't change FRef: TObject;
end;

function TKKeyedObject.CloneSelfAndChildKeyed: TKKeyedObject;
begin
  result := TKDataObject.Clone(self) as TKKeyedObject;
end;

{ TKUserProfile }

{$IFDEF DEBUG_OBJECT_OWNERSHIP}
procedure TKUserProfile.SetRefsRecursive(Ref: TObject);
begin
  inherited;
  SetRefsForListRecursive(FMedia, Ref);
end;
{$ENDIF}

function TKUserProfile.SanityCheck(SiteType: TKSiteType): boolean;
begin
  result := inherited and Assigned(FSiteUserBlock[SiteType]) and FSiteUserBlock[SiteType]
    .SanityCheck(SiteType) and FMedia.SanityCheck(SiteType);
end;

procedure TKUserProfile.DeepAssign(Source: TKDataObject);
var
  Src: TKUserProfile;
begin
  inherited;
  Src := Source as TKUserProfile;
  CloneUserBlockArray(Src.FSiteUserBlock, FSiteUserBlock);
  FMedia.Free;
  FMedia := DeepClone(Src.FMedia) as TKMediaList;
  FMedia.FParent := self;
end;

procedure TKUserProfile.Assign(Source: TKDataObject);
var
  S: TKSiteType;
  Src: TKUserProfile;
begin
  inherited;
  Src := Source as TKUserProfile;
  for S := Low(S) to High(S) do
  begin
    if Assigned(FSiteUserBlock[S]) and Assigned(Src.FSiteUserBlock[S]) then
      FSiteUserBlock[S].Assign(Src.FSiteUserBlock[S]);
  end;
  FInterestLevel := Src.FInterestLevel;
end;

function TKUserProfile.MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet): boolean;
var
  S: TKSiteType;
  N: TKUserProfile;
begin
  result := inherited;
  if not result then
    exit;
  N := Newer as TKUserProfile;
  for S := Low(S) to High(S) do
  begin
    if Assigned(N.FSiteUserBlock[S]) then
    begin
      if Assigned(FSiteUserBlock[S]) then
        FSiteUserBlock[S].MergeWithNewer(N.FSiteUserBlock[S], HintSet)
      else
        FSiteUserBlock[S] := TKDataObject.DeepClone(N.FSiteUserBlock[S]) as TKSiteUserBlock;
    end;
  end;
  FMedia.MergeWithNewer(N.FMedia, HintSet);
  if not(tmhDontChangeInterest in HintSet) then
    FInterestLevel := N.FInterestLevel;
end;


function TKUserProfile.FixupFromDBPersisted(Persisted: TKDataObject): boolean;
var
  P: TKUserProfile;
  S: TKSiteType;
begin
  result := inherited;
  if not result then exit;
  P := Persisted as TKUserProfile;
  for S := Low(S) to High(S) do
  begin
    if Assigned(P.FSiteUserBlock[S]) then
    begin
      if Assigned(FSiteUserBlock[S]) then
        FSiteUserBlock[S].FixupFromDBPersisted(P.FSiteUserBlock[S])
      else
        FSiteUserBlock[S] := TKDataObject.DeepClone(P.FSiteUserBlock[S]) as TKSiteUserBlock;
    end;
  end;
  FMedia.FixupFromDBPersisted(P.FMedia);
  //Always copy interest level across from DB copy.
  FInterestLevel := P.FInterestLevel;
end;


function TKUserProfile.GetSiteUserBlock(Idx: TKSiteType): TKSiteUserBlock;
begin
  result := FSiteUserBlock[Idx];
end;

procedure TKUserProfile.SetSiteUserBlock(Idx: TKSiteType; NewBlock: TKSiteUserBlock);
begin
  FSiteUserBlock[Idx] := NewBlock;
end;

constructor TKUserProfile.Create;
var
  S: TKSiteType;
begin
  inherited;
  for S := Low(S) to High(S) do
    FSiteUserBlock[S] := TKSiteUserBlock.Create;
  FMedia := TKMediaList.Create;
  FMedia.FParent := self;
end;

destructor TKUserProfile.Destroy;
var
  S: TKSiteType;
begin
  for S := Low(S) to High(S) do
    FSiteUserBlock[S].Free;
  FMedia.Free;
  inherited;
end;

function TKUserProfile.CloneSelfAndChildKeyed: TKKeyedObject;
begin
  result := inherited;
  (result as TKUSerProfile).Media.Free;
  (result as TKUSerProfile).FMedia :=
    TKItemList.CloneListAndItemsOnly(Media) as TKMediaList;
  (result as TKUSerProfile).FMedia.FParent := result;
end;

{ TKMediaItem }

{$IFDEF DEBUG_OBJECT_OWNERSHIP}
procedure TKMediaItem.SetRefsRecursive(Ref: TObject);
begin
  inherited;
  SetRefsForListRecursive(FComments, Ref);
end;
{$ENDIF}

function TKMediaItem.SanityCheck(SiteType: TKSiteType): boolean;
begin
  result := inherited and Assigned(SiteMediaBlock[SiteType]) and SiteMediaBlock[SiteType]
    .SanityCheck(SiteType) and (Date <> 0) and
  // TODO - Check media data and resource url?
    FComments.SanityCheck(SiteType);
end;

function TKMediaItem.GetSiteMediaBlock(Idx: TKSiteType): TKSiteMediaBlock;
begin
  result := FSiteMediaBlock[Idx];
end;

constructor TKMediaItem.Create;
var
  S: TKSiteType;
begin
  inherited;
  for S := Low(S) to High(S) do
    FSiteMediaBlock[S] := TKSiteMediaBlock.Create;
  FComments := TKCommentList.Create;
  FComments.FParent := self;
end;

destructor TKMediaItem.Destroy;
var
  S: TKSiteType;
begin
  for S := Low(S) to High(S) do
    FSiteMediaBlock[S].Free;
  FComments.Free;
  inherited;
end;

procedure TKMediaItem.Assign(Source: TKDataObject);
var
  Src: TKMediaItem;
  S: TKSiteType;
begin
  inherited;
  Src := Source as TKMediaItem;
  Date := Src.Date;
  MediaData := Src.MediaData;
  MediaType := Src.MediaType;
  ResourceURL := Src.ResourceURL;
  for S := Low(S) to High(S) do
  begin
    if Assigned(SiteMediaBlock[S]) and Assigned(Src.SiteMediaBlock[S]) then
      SiteMediaBlock[S].Assign(Src.SiteMediaBlock[S]);
  end;
end;

procedure TKMediaItem.DeepAssign(Source: TKDataObject);
var
  Src: TKMediaItem;
begin
  inherited;
  Src := Source as TKMediaItem;
  CloneMediaBlockArray(FSiteMediaBlock, Src.FSiteMediaBlock);
  FComments.Free;
  FComments := DeepClone(Src.FComments) as TKCommentList;
  FComments.FParent := self;
end;

function TKMediaItem.MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet): boolean;
var
  N: TKMediaItem;
  S: TKSiteType;
begin
  result := inherited;
  if not result then
    exit;
  N := Newer as TKMediaItem;
  Date := N.FDate;
  MediaType := N.MediaType;
  MediaData := N.MediaData;
  ResourceURL := N.ResourceURL;
  for S := Low(TKSiteType) to High(TKSiteType) do
  begin
    if Assigned(N.SiteMediaBlock[S]) then
    begin
      if Assigned(FSiteMediaBlock[S]) then
        SiteMediaBlock[S].MergeWithNewer(N.SiteMediaBlock[S], HintSet)
      else
        FSiteMediaBlock[S] := TKDataObject.DeepClone(N.SiteMediaBlock[S]) as TKSiteMediaBlock;
    end;
  end;
  Comments.MergeWithNewer(N.Comments, HintSet);
end;

function TKMediaItem.FixupFromDBPersisted(Persisted: TKDataObject): boolean;
var
  P: TKMediaItem;
  S:TKSiteType;
begin
  result := inherited;
  if not result then exit;
  P := Persisted as TKMediaItem;
  for S := Low(TKSiteType) to High(TKSiteType) do
  begin
    if Assigned(P.SiteMediaBlock[S]) then
    begin
      if Assigned(FSiteMediaBlock[S]) then
        SiteMediaBlock[S].FixupFromDBPersisted(P.SiteMediaBlock[S])
      else
        FSiteMediaBlock[S] := TKDataObject.DeepClone(P.SiteMediaBlock[S]) as TKSiteMediaBlock;
    end;
  end;
{ Do not change these, imported version most recent
    FDate: TDateTime;
    FMediaType: TKMediaItemType;
    FSMediaData: string;
    FSResourceURL: string;
}
  Comments.FixupFromDBPersisted(P.Comments);
end;

function TKMediaItem.CloneSelfAndChildKeyed: TKKeyedObject;
begin
  result := inherited;
  (result as TKMediaItem).Comments.Free;
  (result as TKMediaItem).FComments :=
    TKItemList.CloneListAndItemsOnly(Comments) as TKCommentList;
  (result as TKMediaItem).FComments.FParent := result;
end;

{ TKCommentItem }

function TKCommentItem.SanityCheck(SiteType: TKSiteType): boolean;
begin
  result := inherited and (Date <> 0) and (Length(CommentData) > 0) and
    (Assigned(SiteCommentBlock[SiteType])) and (SiteCommentBlock[SiteType].SanityCheck(SiteType))
    and (Assigned(SiteUserBlock[SiteType])) and (SiteUserBlock[SiteType].SanityCheck(SiteType));
end;

procedure TKCommentItem.Assign(Source: TKDataObject);
var
  Src: TKCommentItem;
  S: TKSiteType;
begin
  inherited;
  Src := Source as TKCommentItem;
  Date := Src.FDate;
  CommentData := Src.CommentData;
  CommentType := Src.CommentType;
  OwnerKey := Src.OwnerKey;
  for S := Low(TKSiteType) to High(TKSiteType) do
  begin
    if Assigned(SiteUserBlock[S]) and Assigned(Src.SiteUserBlock[S]) then
      SiteUserBlock[S].Assign(Src.SiteUserBlock[S]);
    if Assigned(SiteCommentBlock[S]) and Assigned(Src.SiteCommentBlock[S]) then
      SiteCommentBlock[S].Assign(Src.SiteCommentBlock[S]);
  end;
end;

procedure TKCommentItem.DeepAssign(Source: TKDataObject);
var
  Src: TKCommentItem;
begin
  inherited;
  Src := Source as TKCommentItem;
  CloneCommentBlockArray(Src.FSiteCommentBlock, FSiteCommentBlock);
  CloneUserBlockArray(Src.FSiteUserBlock, FSiteUserBlock);
end;

function TKCommentItem.MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet): boolean;
var
  S: TKSiteType;
  N: TKCommentItem;
begin
  result := inherited;
  if not result then
    exit;
  N := Newer as TKCommentItem;
  for S := Low(TKSiteType) to High(TKSiteType) do
  begin
    if Assigned(N.FSiteUserBlock[S]) then
    begin
      if Assigned(FSiteUserBlock[S]) then
        FSiteUserBlock[S].MergeWithNewer(N.FSiteUserBlock[S], HintSet)
      else
        FSiteUserBlock[S] := TKDataObject.DeepClone(N.FSiteUserBlock[S]) as TKSiteUserBlock;
    end;
    if Assigned(N.FSiteCommentBlock[S]) then
    begin
      if Assigned(FSiteCommentBlock[S]) then
        FSiteCommentBlock[S].MergeWithNewer(N.FSiteCommentBlock[S], HintSet)
      else
        FSiteCommentBlock[S] := TKDataObject.DeepClone(N.FSiteCommentBlock[S])
          as TKSiteCommentBlock;
    end;
  end;
  Date := N.Date;
  CommentType := N.CommentType;
  CommentData := N.CommentData;
end;

function TKCommentItem.FixupFromDBPersisted(Persisted: TKDataObject): boolean;
var
  P: TKCommentItem;
  S: TKSiteType;
begin
  result := inherited;
  if not result then exit;
  P := Persisted as TKCommentItem;
//    FSOwnerKey: string;
  if FSOwnerKey = TGuid.Empty then
  begin
    if P.FSOwnerKey <> TGuid.Empty then
      FSOwnerKey := P.FSOwnerKey;
  end
  else if P.FSOwnerKey <> TGuid.Empty then
  begin
    if FSOwnerKey <> P.FSOwnerKey then
    begin
      Assert(false);
      result := false;
      exit;
    end;
  end;
{ These will be newer in imported item, so leave.
    FDate: TDateTime;
    FCommentType: TKCommentItemType;
    FSCommentStrData: string;
}
  for S := Low(TKSiteType) to High(TKSiteType) do
  begin
    if Assigned(P.FSiteUserBlock[S]) then
    begin
      if Assigned(FSiteUserBlock[S]) then
        FSiteUserBlock[S].FixupFromDBPersisted(P.FSiteUserBlock[S])
      else
        FSiteUserBlock[S] := TKDataObject.DeepClone(P.FSiteUserBlock[S]) as TKSiteUserBlock;
    end;
    if Assigned(P.FSiteCommentBlock[S]) then
    begin
      if Assigned(FSiteCommentBlock[S]) then
        FSiteCommentBlock[S].FixupFromDBPersisted(P.FSiteCommentBlock[S])
      else
        FSiteCommentBlock[S] := TKDataObject.DeepClone(P.FSiteCommentBlock[S])
          as TKSiteCommentBlock;
    end;
  end;
end;

function TKCommentItem.GetSiteUserBlock(Idx: TKSiteType): TKSiteUserBlock;
begin
  result := FSiteUserBlock[Idx];
end;

function TKCommentItem.GetSiteCommentBlock(Idx: TKSiteType): TKSiteCommentBlock;
begin
  result := FSiteCommentBlock[Idx];
end;

constructor TKCommentItem.Create;
var
  S: TKSiteType;
begin
  inherited;
  for S := Low(S) to High(S) do
  begin
    FSiteUserBlock[S] := TKSiteUserBlock.Create;
    FSiteCommentBlock[S] := TKSiteCommentBlock.Create;
  end;
end;

destructor TKCommentItem.Destroy;
var
  S: TKSiteType;
begin
  for S := Low(S) to High(S) do
  begin
    FSiteUserBlock[S].Free;
    FSiteCommentBlock[S].Free;
  end;
  inherited;
end;

{ TKItemExtents }

function TKItemExtents.SanityCheck(SiteType: TKSiteType): boolean;
begin
  result := inherited;
  if HasLater then
    result := result and (Length(LastId) > 0);
  if HasEarlier then
    result := result and (Length(FirstId) > 0);
  if Length(LinkId) > 0 then
  begin
    if CompareDecimalKeyStr(FirstId, LastId) <> 0 then
    begin
      if CompareDecimalKeyStr(LastId, LinkId) = 0 then
        result := result and HasLater;
      if CompareDecimalKeyStr(FirstId, LinkId) = 0 then
        result := result and HasEarlier;
    end
    else
      result := result and (HasEarlier or HasLater); // All the same number.
  end;
end;

procedure TKItemExtents.Assign(Source: TKDataObject);
var
  Src: TKItemExtents;
begin
  inherited;
  Src := Source as TKItemExtents;
  FirstId := Src.FirstId;
  LastId := Src.LastId;
  HasEarlier := Src.HasEarlier;
  HasLater := Src.HasLater;
  LinkId := Src.LinkId
end;

function TKItemExtents.MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet): boolean;
var
  SrcExtents: TKItemExtents;
begin
  result := true;
  SrcExtents := Newer as TKItemExtents;
  if not(Length(LinkId) > 0) then
  begin
    // TODO - could merge based on ID's and which is lower
    // for this set of extents, but for the moment, won't bother.
    FirstId := '';
    LastId := '';
    HasEarlier := false;
    HasLater := false;
  end
  else
  begin
    Assert((CompareDecimalKeyStr(FirstId, LinkId) = 0) or
      (CompareDecimalKeyStr(LastId, LinkId) = 0));
    if CompareDecimalKeyStr(LinkId, LastId) = 0 then
    begin
      LastId := SrcExtents.LastId;
      HasLater := SrcExtents.HasLater;
    end
    else if CompareDecimalKeyStr(LinkId, FirstId) = 0 then
    begin
      FirstId := SrcExtents.FirstId;
      HasEarlier := SrcExtents.HasEarlier;
    end;
    LinkId := '';
  end;
end;

{ TKList }

function TKList.GetCount: integer;
begin
  result := FList.Count;
end;

function TKList.SanityCheck(SiteType: TKSiteType): boolean;
var
  IRec: TItemRec;
begin
  result := inherited;
  IRec := FList.GetAnItem;
  while Assigned(IRec) and result do
  begin
    result := result and (IRec.Item as TKDataObject).SanityCheck(SiteType);
    FList.GetAnotherItem(IRec);
  end;
end;

constructor TKList.Create;
begin
  inherited;
  FList := TIndexedStore.Create;
  FList.AddIndex(TPointerINode, Ord(tkPointer));
end;

destructor TKList.Destroy;
var
  Tag: Int64;
  NodeClass: TIndexNodeClass;
  RV: TIsRetVal;
begin
  //Clear all the indexes before removing items (for speed).
  while FList.IndexCount > 0 do
  begin
    RV := FList.IndexInfoByOrdinal(0, Tag, NodeClass);
    Assert(RV = rvOK);
    RV := FList.DeleteIndex(Tag);
    Assert(RV = rvOK);
  end;
  //Now remove items and free.
  List.DeleteChildren;
  FList.Free;
  inherited;
end;

procedure TKList.DeleteChildren;
begin
  List.DeleteChildren;
end;

function TKList.IndexCheckGeneric(IndexTag: Int64): TISRetVal;
begin
  if FList.HasIndex(Ord(IndexTag)) then
    result := rvOK
  else
  begin
    // TODO - Could split this into virtual functions, but it's such a pain!
    // Better to keep it all in once place.
    case IndexTag of
      Ord(tkPointer):
        result := FList.AddIndex(TPointerINode, IndexTag);
      Ord(tkInternalKey):
        result := FList.AddIndex(TKKeyedIndex, IndexTag);
      Ord(tkInstaUserId), Ord(tkTwitterUserId), Ord(tkInstaMediaId), Ord(tkTwitterMediaId),
        Ord(tkInstaCommentId), Ord(tkTwitterCommentId):
        result := FList.AddIndex(TKIdIndex, IndexTag);
      Ord(tkUserLastUpdated), Ord(tkMediaDate), Ord(tkMediaLastUpdated), Ord(tkCommentDate),
        Ord(tkCommentLastUpdated):
        result := FList.AddIndex(TKDateIndex, IndexTag);
      Ord(tkCommentOwnerKey):
        result := FList.AddIndex(TKKeyedIndex, IndexTag);
      Ord(tkInstaUserName), Ord(tkTwitterUserName):
        result := FList.AddIndex(TKUserNameIndex, IndexTag);
      Ord(tkUserPresOrder), Ord(tkMediaPresOrder), Ord(tkCommentPresOrder):
        result := FList.AddIndex(TKPresentationOrderIndex, IndexTag);
      Ord(tkUserIdentifierOrder), Ord(tkMediaIdentifierOrder), Ord(tkCommentIdentifierOrder):
        result := FList.AddIndex(TKIdentifierOrderIndex, IndexTag);
    else
      result := rvInternalError;
    end;
  end;
  // Provided the data isn't bad, we should *always* be able to create indices.
  Assert(result = rvOK);
end;

function TKList.SearchGenericIRec(IndexTag: Int64; SearchVal: TIndexNode; var IRec: TItemRec)
  : TISRetVal;
begin
  Assert(Assigned(SearchVal));
  IRec := nil;
  result := IndexCheckGeneric(IndexTag);
  if result = rvOK then
    result := FList.FindByIndex(IndexTag, SearchVal, IRec);
  Assert((result = rvOK) or (result = rvNotFound));
end;

function TKList.SearchNearGenericIRec(IndexTag: Int64; SearchVal: TIndexNode; var IRec: TItemRec)
  : TISRetVal;
begin
  Assert(Assigned(SearchVal));
  IRec := nil;
  result := IndexCheckGeneric(IndexTag);
  if result = rvOK then
    result := FList.FindNearByIndex(IndexTag, SearchVal, IRec);
  Assert((result = rvOK) or (result = rvNotFound));
end;

function TKList.AdjacentGenericIRec(AdjType: TKAdjacencyType; IndexTag: Int64; var IRec: TItemRec)
  : TISRetVal;
begin
  if (AdjType = katFirst) or (AdjType = katLast) then
    IRec := nil;
  result := IndexCheckGeneric(IndexTag);
  if result = rvOK then
  begin
    case AdjType of
      katFirst:
        result := FList.FirstByIndex(IndexTag, IRec);
      katLast:
        result := FList.LastByIndex(IndexTag, IRec);
      katNext:
        result := FList.NextByIndex(IndexTag, IRec);
      katPrevious:
        result := FList.PreviousByIndex(IndexTag, IRec);
    else
      result := rvInternalError;
    end;
  end;
  if result <> rvOK then
    IRec := nil;
  Assert((result = rvOK) or (result = rvNotFound));
end;

//TODO - Could expand search / search near to allow more types of sort val,
//not necessary at the moment.
function TKList.SearchBySortValIRec(SortVal: TKKeySortVal; Item: TKDataObject; var IRec: TItemRec)
  : TISRetVal;
var
  SearchVal: TIndexNode;
  IndexTag: TKIndexTag;
begin
  Assert(Assigned(Item));
  IRec := nil;
  case SortVal of
    ksvPointer:
      begin
        SearchVal := TSearchPointerINode.Create;
        IndexTag := tkPointer;
      end;
    ksvKey:
      begin
        SearchVal := TKKeyedIndexSearchVal.Create;
        IndexTag := tkInternalKey;
      end
  else
    result := rvInternalError;
    exit;
  end;
  try
    case SortVal of
      ksvPointer:
        (SearchVal as TSearchPointerINode).SearchVal := Item;
      ksvKey:
        (SearchVal as TKKeyedIndexSearchVal).SearchKey := (Item as TKKeyedObject).Key;
    else
      result := rvInternalError;
      exit;
    end;
    result := SearchGenericIRec(Ord(IndexTag), SearchVal, IRec);
  finally
    SearchVal.Free;
  end;
  Assert((result = rvOK) or (result = rvNotFound));
end;

//TODO - Could expand search / search near to allow more types of sort val,
//not necessary at the moment.
function TKList.SearchNearBySortValIRec(SortVal: TKKeySortVal; SearchStr: string; var IRec: TItemRec)
  : TISRetVal;
var
  SearchVal: TIndexNode;
  IndexTag: TKIndexTag;
begin
  IRec := nil;
  case SortVal of
    ksvIdentifierOrder:
    begin
      SearchVal := TKIdentifierOrderSearchVal.Create;
      (SearchVal as TKIdentifierOrderSearchVal).SearchString := SearchStr;
      LevelToIdentifierOrderTag(GetListLevel, IndexTag);
    end;
  else
    result := rvInternalError;
    exit;
  end;
  try
    result := SearchNearGenericIRec(Ord(IndexTag), SearchVal, IRec);
  finally
    SearchVal.Free;
  end;
  Assert((result = rvOK) or (result = rvNotFound));
end;

function TKList.SearchBySortVal(SortVal: TKKeySortVal; Item: TKKeyedObject): TKKeyedObject;
var
  IRec: TItemRec;
begin
  result := nil;
  if SearchBySortValIRec(SortVal, Item, IRec) = rvOK then
    result := IRec.Item as TKKeyedObject;
end;

function TKList.SearchNearBySortVal(SortVal: TKKeySortVal; SearchStr: string): TKKeyedObject;
var
  IRec: TItemRec;
begin
  result := nil;
  if SearchNearBySortValIRec(SortVal, SearchStr, IRec) = rvOK then
    result := IRec.Item as TKKeyedObject;
end;

function TKList.SearchByInternalKeyOnly(Key: TGuid): TKKeyedObject;
var
  TmpItem: TKKeyedObject;
begin
  TmpItem := TKKeyedObject.Create;
  try
    TmpItem.Key := Key;
    result := SearchBySortVal(ksvKey, TmpItem);
  finally
    TmpItem.Free;
  end;
end;

function TKList.MergeHelper(MergeInCandidate: TKDataObject): TKDataObject;
begin
  result := nil;
end;

function TKList.MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet): boolean;
var
  SrcItem, NextSrcItem, OwnItem: TItemRec;
  Src: TKList;
  IR: TISRetVal;
  MH: TKDataObject;
begin
  result := inherited;
  Assert(Assigned(Newer));
  Src := Newer as TKList;
  SrcItem := Src.List.GetAnItem;
  while result and Assigned(SrcItem) do
  begin
    NextSrcItem := SrcItem;
    Src.List.GetAnotherItem(NextSrcItem);
    MH := MergeHelper(SrcItem.Item as TKDataObject);
    if not Assigned(MH) then
    begin
      IR := List.AddItem(SrcItem.Item, OwnItem);
      Assert(IR = rvOK);
      IR := Src.List.RemoveItem(SrcItem);
      Assert(IR = rvOK);
    end
    else
    begin
      // Need to remove and re-insert to preserve indices if various
      // vals change.
      IR := SearchBySortValIRec(ksvPointer, MH, OwnItem);
      Assert(IR = rvOK);
      IR := List.RemoveItem(OwnItem);
      Assert(IR = rvOK);
      MH.MergeWithNewer(SrcItem.Item as TKDataObject, HintSet);
      IR := List.AddItem(MH, OwnItem);
      Assert(IR = rvOK);
    end;
    if result then
      SrcItem := NextSrcItem;
  end;
end;

function TKList.FixupFromDBPersisted(Persisted: TKDataObject): boolean;
var
  P: TKList;
  OwnItem: TItemRec;
  PersistChild: TKDataObject;
begin
  result := inherited;
  if not result then exit;
  P := Persisted as TKList;
  //Using the merge helper the other way round from the merge,
  //we expect the persisted list to be very much larger.
  OwnItem := List.GetAnItem;
  while Assigned(OwnItem) do
  begin
    PersistChild := P.MergeHelper(OwnItem.Item as TKDataObject);
    if Assigned(PersistChild) then
      (OwnItem.Item as TKDataObject).FixupFromDBPersisted(PersistChild);
    List.GetAnotherItem(OwnItem)
  end;
end;


function TKList.AdjacentBySortVal(AdjType: TKAdjacencyType; SortVal: TKKeySortVal;
  Current: TKKeyedObject): TKKeyedObject;
var
  Tag: TKIndexTag;
  IRec: TItemRec;
  Ret: TISRetVal;
begin
  result := nil;
  case SortVal of
    ksvPointer:
      Tag := tkPointer;
    ksvKey:
      Tag := tkInternalKey;
    ksvPresentationOrder:
      LevelToPresentationOrderTag(GetListLevel, Tag);
    ksvIdentifierOrder:
      LevelToIdentifierOrderTag(GetListLevel, Tag);
  else
    Assert(false);
    exit;
  end;
  if ((AdjType = katFirst) or (AdjType = katLast)) then
  begin
    if Assigned(Current) then
      GLogLog(SV_WARN, S_ADJACENT_PASSED_PREVIOUS);
    IRec := nil;
    Ret := rvOK;
  end
  else
  begin
    // Find the original object by pointer.
    Ret := SearchBySortValIRec(ksvPointer, Current, IRec);
    Assert(Ret = rvOK);
  end;
  if Ret = rvOK then
    Ret := AdjacentGenericIRec(AdjType, Ord(Tag), IRec);
  if Ret = rvOK then
    result := IRec.Item as TKKeyedObject
end;

procedure TKList.Remove(Obj: TKKeyedObject);
var
  RetVal: TISRetVal;
  IRec: TItemRec;
begin
  RetVal := SearchBySortValIRec(ksvPointer, Obj, IRec);
  if RetVal = rvOK then
    RetVal := FList.RemoveItem(IRec);
  Assert(RetVal = rvOK);
end;

function TKList.Add(Obj: TKKeyedObject): boolean;
var
  IRec: TItemRec;
begin
  result := FList.AddItem(Obj, IRec) = rvOK;
end;

class function TKList.CloneListAndItemsOnly(Source: TKDataObject): TKDataObject;
var
  SrcClass: TKDataClass;
begin
  if not Assigned(Source) then
    result := nil
  else
  begin
    Assert(Source.ClassType.InheritsFrom(TKDataObject));
    Assert(Source.ClassType.InheritsFrom(TKList));
    SrcClass := TKDataClass(Source.ClassType);
    result := SrcClass.Create;
    (result as TKList).AssignTopItems(Source);
  end;
end;

procedure TKList.AssignHelper(Source: TKDataObject; Deep: boolean);
var
  Src: TKList;
  ItemRec, TmpRec: TItemRec;
  RV: TISRetVal;
  IndexTag: Int64;
  IndexClass: TIndexNodeClass;
  i: integer;
begin
  inherited;
  Src := Source as TKList;
  // Delete all existing items
  List.DeleteChildren;
  // Delete all existing indices
  while List.IndexCount > 0 do
  begin
    RV := List.IndexInfoByOrdinal(0, IndexTag, IndexClass);
    Assert(RV = rvOK);
    List.DeleteIndex(IndexTag);
  end;
  // Copy indices across from src.
  for i := 0 to Pred(Src.List.IndexCount) do
  begin
    RV := Src.List.IndexInfoByOrdinal(i, IndexTag, IndexClass);
    Assert(RV = rvOK);
    RV := List.AddIndex(IndexClass, IndexTag);
    Assert(RV = rvOK);
  end;
  // And clone items from src.
  ItemRec := Src.List.GetAnItem;
  while Assigned(ItemRec) do
  begin
    if Deep then
      RV := List.AddItem(DeepClone(ItemRec.Item as TKDataObject), TmpRec)
    else
      RV := List.AddItem(Clone(ItemRec.Item as TKDataObject), TmpRec);
    Assert(RV = rvOK);
    Src.List.GetAnotherItem(ItemRec);
  end;
end;

procedure TKList.AssignTopItems(Source: TKDataObject);
begin
  AssignHelper(Source, false);
end;

procedure TKList.DeepAssign(Source: TKDataObject);
begin
  AssignHelper(Source, true);
end;

{ TKItemList }

function TKItemList.SanityCheck(SiteType: TKSiteType): boolean;
begin
  result := inherited and FExtents.SanityCheck(SiteType);
end;

function TKItemList.MergeHelper(MergeInCandidate: TKDataObject): TKDataObject;
var
  MC: TKKeyedObject;
begin
  result := inherited;
  if Assigned(result) then
    exit;

  MC := MergeInCandidate as TKKeyedObject;
  if MC.Key <> TGuid.Empty then
    result := SearchBySortVal(ksvKey, MC);
end;

procedure TKItemList.DeepAssign(Source: TKDataObject);
var
  Src: TKItemList;
begin
  inherited;
  Src := Source as TKItemList;
  FExtents.Free;
  FExtents := DeepClone(Src.FExtents) as TKExtents
end;

constructor TKItemList.Create;
begin
  inherited;
  FExtents := TKItemExtents.Create;
end;

destructor TKItemList.Destroy;
begin
  FExtents.Free;
  inherited;
end;

function TKItemList.MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet): boolean;
var
  Src: TKItemList;
begin
  Assert(Assigned(Newer));
  Assert(Newer is TKItemList);
  Src := Newer as TKItemList;
  result := Extents.MergeWithNewer(Src.Extents, HintSet) and
    inherited MergeWithNewer(Newer, HintSet);
end;

{ TKIdList }

function TKIdList.SearchByDataBlockIRec(ST: TKSiteType; Item: TKKeyedObject; var IRec: TItemRec)
  : TISRetVal;
var
  IndexTag: TKIndexTag;
  SearchVal: TKIdIndexSearchVal;
  IdBlockValid: boolean;
begin
  IRec := nil;
  SiteTypeAndLevelToIdIndexTag(ST, GetListLevel, IndexTag);
  case GetListLevel of
    klUserList:
      IdBlockValid := (Item as TKUserProfile).FSiteUserBlock[ST].Valid;
    klMediaList:
      IdBlockValid := (Item as TKMediaItem).FSiteMediaBlock[ST].Valid;
    klCommentList:
      IdBlockValid := (Item as TKCommentItem).FSiteCommentBlock[ST].Valid;
  else
    Assert(false);
    result := rvInternalError;
    exit;
  end;
  if not IdBlockValid then
  begin
    result := rvInvalidSearchVal;
    exit;
  end;
  SearchVal := TKIdIndexSearchVal.Create;
  try
    SearchVal.SearchSiteType := ST;
    SearchVal.SearchListLevel := GetListLevel;
    case GetListLevel of
      klUserList:
        SearchVal.SearchString := (Item as TKUserProfile).FSiteUserBlock[ST].UserId;
      klMediaList:
        SearchVal.SearchString := (Item as TKMediaItem).FSiteMediaBlock[ST].MediaID;
      klCommentList:
        SearchVal.SearchString := (Item as TKCommentItem).FSiteCommentBlock[ST].CommentId;
    else
      Assert(false);
      result := rvInternalError;
      exit;
    end;
    result := SearchGenericIRec(Ord(IndexTag), SearchVal, IRec);
  finally
    SearchVal.Free;
  end;
end;

function TKIdList.SearchByDataBlock(ST: TKSiteType; ItemWithId: TKKeyedObject): TKKeyedObject;
var
  IRec: TItemRec;
begin
  result := nil;
  if SearchByDataBlockIRec(ST, ItemWithId, IRec) = rvOK then
    result := IRec.Item as TKKeyedObject;
end;

function TKIdList.MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet = []): boolean;
var
  Src: TKIdList;
begin
  Assert(Assigned(Newer));
  Assert(Newer is TKIdList);
  Src := Newer as TKIdList;
  Assert(Src.GetListLevel = GetListLevel);
  result := inherited;
end;

function TKIdList.MergeHelper(MergeInCandidate: TKDataObject): TKDataObject;
var
  MC: TKKeyedObject;
  ST: TKSiteType;
begin
  result := inherited;
  if Assigned(result) then
    exit;
  MC := MergeInCandidate as TKKeyedObject;
  for ST := Low(ST) to High(ST) do
  begin
    result := SearchByDataBlock(ST, MC);
    if Assigned(result) then
      exit;
  end;
end;

function TKIdList.AdjacentByTimeIndex(AdjType: TKAdjacencyType; SortVal: TKIDTimeSortVal;
  CurrentObject: TKKeyedObject): TKKeyedObject;
var
  Tag: TKIndexTag;
  IRec: TItemRec;
  Ret: TISRetVal;
begin
  result := nil;
  case SortVal of
    tsvLastModified:
      LevelAndSearchFieldToDateTag(GetListLevel, dsfLastUpdated, Tag);
    tsvDate:
      LevelAndSearchFieldToDateTag(GetListLevel, dsfDate, Tag);
  else
    Assert(false);
    exit;
  end;
  if ((AdjType = katFirst) or (AdjType = katLast)) then
  begin
    IRec := nil;
    Ret := rvOK;
  end
  else
  begin
    // Find the original object by pointer.
    Ret := SearchBySortValIRec(ksvPointer, CurrentObject, IRec);
    Assert(Ret = rvOK);
  end;
  if Ret = rvOK then
    Ret := AdjacentGenericIRec(AdjType, Ord(Tag), IRec);
  if Ret = rvOK then
    result := IRec.Item as TKKeyedObject
end;

procedure TKIdList.PrepareImport(ST: TKSiteType);
var
  IndexTag: TKIndexTag;
  RV: TISRetVal;
begin
  SiteTypeAndLevelToIdIndexTag(ST, GetListLevel, IndexTag);
  RV := IndexCheckGeneric(Ord(IndexTag));
  Assert(RV = rvOK);
end;

{ TKMediaList }

function TKMediaList.GetListLevel: TKListLevel;
begin
  result := klMediaList;
end;

{ TKCommentList }

function TKCommentList.GetListLevel: TKListLevel;
begin
  result := klCommentList;
end;

function TKCommentList.SearchByCommentOwnerKeyIRec(User: TKUserProfile; var IRec: TItemRec)
  : TISRetVal;
var
  SearchVal: TKKeyedIndexSearchVal;
begin
  Assert(Assigned(User));
  IRec := nil;
  SearchVal := TKKeyedIndexSearchVal.Create;
  try
    SearchVal.SearchKey := User.Key;
    result := SearchGenericIRec(Ord(tkCommentOwnerKey), SearchVal, IRec);
  finally
    SearchVal.Free;
  end;
end;

function TKCommentList.SearchByCommentOwner(User: TKUserProfile): TKCommentItem;
var
  IRec: TItemRec;
begin
  result := nil;
  if SearchByCommentOwnerKeyIRec(User, IRec) = rvOK then
    result := IRec.Item as TKCommentItem;
end;

{ TKUserList }

function TKUserList.GetListLevel: TKListLevel;
begin
  result := klUserList;
end;

function TKUserList.SearchByUserNameIRec(ST: TKSiteType; Item: TKKeyedObject; var IRec: TItemRec)
  : TISRetVal;
var
  IndexTag: TKIndexTag;
  SearchVal: TKUserNameSearchVal;
  IdBlockValid: boolean;
begin
  IRec := nil;
  SiteTypeToUsernameIndexTag(ST, IndexTag);
  Assert(GetListLevel = klUserList);
  IdBlockValid := (Item as TKUserProfile).FSiteUserBlock[ST].Valid;
  if not IdBlockValid then
  begin
    result := rvInvalidSearchVal;
    exit;
  end;
  SearchVal := TKUserNameSearchVal.Create;
  try
    SearchVal.SearchSiteType := ST;
    SearchVal.SearchString := (Item as TKUserProfile).FSiteUserBlock[ST].Username;
    result := SearchGenericIRec(Ord(IndexTag), SearchVal, IRec);
  finally
    SearchVal.Free;
  end;
end;

function TKUserList.SearchByUserName(ST: TKSiteType; Item: TKKeyedObject): TKUserProfile;
var
  IRec: TItemRec;
begin
  result := nil;
  if SearchByUserNameIRec(ST, Item, IRec) = rvOK then
    result := IRec.Item as TKUserProfile;
end;

{ TKSiteBlock }

function TKSiteBlock.SanityCheck(SiteType: TKSiteType): boolean;
begin
  result := inherited and FValid;
end;

procedure TKSiteBlock.Assign(Source: TKDataObject);
begin
  inherited;
  FValid := (Source as TKSiteBlock).FValid;
end;

function TKSiteBlock.MergeWithNewer(Newer: TKDataObject; HintSet: TKMergeHintSet): boolean;
begin
  result := inherited;
  if not result then
    exit;
  if not(Newer as TKSiteBlock).FValid then
    exit;
  Assign(Newer);
end;

function TKSiteBlock.FixupFromDBPersisted(Persisted: TKDataObject): boolean;
var
  P: TKSiteBlock;
begin
  result := inherited;
  if not result then exit;

  P := Persisted as TKSiteBlock;

  //Only copy site type blocks across if DB blocks valid and ours aren't
  //(cross site merge in DB).
  result := (not FValid) and P.Valid;
  if result then
    Assign(P);
end;

{ TKSiteUserBlock }

function TKSiteUserBlock.SanityCheck(SiteType: TKSiteType): boolean;
begin
  result := inherited and (Length(FSUsername) > 0) and (Length(FSUserId) > 0);
end;

constructor TKSiteUserBlock.Create;
begin
  inherited;
  FOriginal := true;
end;

procedure TKSiteUserBlock.Assign(Source: TKDataObject);
var
  Src: TKSiteUserBlock;
begin
  inherited;
  Src := Source as TKSiteUserBlock;
  Username := Src.Username;
  UserId := Src.UserId;
  Verified := Src.Verified;
  ProfilePicUrl := Src.ProfilePicUrl;
  FullName := Src.FullName;
  Bio := Src.Bio;
  FollowsCount := Src.FollowsCount;
  FollowerCount := Src.FollowerCount;
end;

{ TKSiteMediaBlock }

function TKSiteMediaBlock.SanityCheck(SiteType: TKSiteType): boolean;
begin
  result := inherited and (Length(FSMediaID) > 0) and (Length(FSOwnerID) > 0);
  if SiteType = tstInstagram then
    result := result and (Length(FSMediaCode) > 0);
end;

procedure TKSiteMediaBlock.Assign(Source: TKDataObject);
var
  Src: TKSiteMediaBlock;
begin
  inherited;
  Src := Source as TKSiteMediaBlock;
  MediaID := Src.MediaID;
  MediaCode := Src.MediaCode;
  OwnerID := Src.OwnerID;
end;

{ TKSiteCommentBlock }

function TKSiteCommentBlock.SanityCheck(SiteType: TKSiteType): boolean;
begin
  result := inherited and (Length(FSCommentId) > 0);
end;

procedure TKSiteCommentBlock.Assign(Source: TKDataObject);
var
  Src: TKSiteCommentBlock;
begin
  inherited;
  Src := Source as TKSiteCommentBlock;
  CommentId := Src.CommentId;
end;

{ TKKeyedIndex }

function TKKeyedIndex.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64;
  OtherNode: TIndexNode): integer;
var
  Tag: TKIndexTag;
  OwnKey, OtherKey: TGuid;
begin
  // Conservative coding here - can't remember the details!
  // Find the index tag, what a pain!
  Tag := TKIndexTag(IndexTag);
  Assert(Tag >= Low(Tag));
  Assert(Tag <= High(Tag));
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  if Tag = tkCommentOwnerKey then
  begin
    // One of these may be a TKKeyedObject, not a TKCommentItem
    // if a search val.
    if (OwnItem is TKCommentItem) then
      OwnKey := (OwnItem as TKCommentItem).OwnerKey
    else
    begin
      Assert(OwnItem is TKKeyedObject);
      OwnKey := (OwnItem as TKKeyedObject).Key;
    end;
    if (OtherItem is TKCommentItem) then
      OtherKey := (OtherItem as TKCommentItem).OwnerKey
    else
    begin
      Assert(OtherItem is TKKeyedObject);
      OtherKey := (OtherItem as TKKeyedObject).Key;
    end;
    result := MemDBMisc.CompareGuids(OtherKey, OwnKey);
  end
  else
  begin
    Assert(Tag = tkInternalKey);
    result := CompareGuids((OtherItem as TKKeyedObject).Key, (OwnItem as TKKeyedObject).Key);
  end;
end;

{ TKKeyedIndexSearchVal }

constructor TKKeyedIndexSearchVal.Create;
begin
  inherited;
  FKeyedObject := TKKeyedObject.Create;
end;

destructor TKKeyedIndexSearchVal.Destroy;
begin
  FKeyedObject.Free;
  inherited;
end;

function TKKeyedIndexSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64;
  OtherNode: TIndexNode): integer;
begin
  Assert(not Assigned(OwnItem));
  result := inherited CompareItems(FKeyedObject, OtherItem, IndexTag, OtherNode);
end;

function TKKeyedIndexSearchVal.GetSearchKey: TGuid;
begin
  result := FKeyedObject.Key;
end;

procedure TKKeyedIndexSearchVal.SetSearchKey(NewKey: TGuid);
begin
  FKeyedObject.Key := NewKey;
end;

{ TKBySiteIndex }

function TKBySiteIndex.GetSiteType(out ST: TKSiteType): boolean;
var
  Index: integer;
  IndexTag: TKIndexTag;
begin
  result := Assigned(IndexLink);
  if result then
  begin
    Index := IndexLink.RootIndex.Tag;
    Assert(Index >= Ord(Low(TKIndexTag)));
    Assert(Index <= Ord(High(TKIndexTag)));
    IndexTag := TKIndexTag(Index);
    ST := IndexTagToSiteType(IndexTag);
  end;
end;

{ TKIdIndex }

function TKIdIndex.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64;
  OtherNode: TIndexNode): integer;
var
  CmpIndex: TKIdIndex;
  OtherProf, OwnProf: TKUserProfile;
  OtherM, OwnM: TKMediaItem;
  OtherC, OwnC: TKCommentItem;
  ST: TKSiteType;
  KL: TKListLevel;
begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  if not GetSiteType(ST) then
  begin
    CmpIndex := OtherNode as TKIdIndex;
    if not CmpIndex.GetSiteType(ST) then
      Assert(false);
  end;
  if not GetListLevel(KL) then
  begin
    CmpIndex := OtherNode as TKIdIndex;
    if not CmpIndex.GetListLevel(KL) then
      Assert(false);
  end;
  case KL of
    klUserList:
      begin
        OtherProf := OtherItem as TKUserProfile;
        OwnProf := OwnItem as TKUserProfile;
        result := CompareDecimalKeyStr(OtherProf.FSiteUserBlock[ST].UserId,
          OwnProf.FSiteUserBlock[ST].UserId);
      end;
    klMediaList:
      begin
        OwnM := OwnItem as TKMediaItem;
        OtherM := OtherItem as TKMediaItem;
        result := CompareDecimalKeyStr(OtherM.FSiteMediaBlock[ST].MediaID,
          OwnM.FSiteMediaBlock[ST].MediaID);
      end;
    klCommentList:
      begin
        OwnC := OwnItem as TKCommentItem;
        OtherC := OtherItem as TKCommentItem;
        result := CompareDecimalKeyStr(OtherC.FSiteCommentBlock[ST].CommentId,
          OwnC.FSiteCommentBlock[ST].CommentId);
      end;
  else
    begin
      Assert(false);
      result := -1;
    end;
  end;
end;

function TKIdIndex.GetListLevel(out TL: TKListLevel): boolean;
var
  Index: integer;
  IndexTag: TKIndexTag;
begin
  result := Assigned(IndexLink);
  if result then
  begin
    Index := IndexLink.RootIndex.Tag;
    Assert(Index >= Ord(Low(TKIndexTag)));
    Assert(Index <= Ord(High(TKIndexTag)));
    IndexTag := TKIndexTag(Index);
    TL := IndexTagToListLevel(IndexTag);
  end;
end;

{ TKIdIndexSearchVal }

procedure TKIdIndexSearchVal.SetSearchSiteType(ST: TKSiteType);
begin
  Assert(not FSearchSiteTypeSet);
  FSearchSiteType := ST;
  FSearchSiteTypeSet := true;
end;

function TKIdIndexSearchVal.GetSearchSiteType: TKSiteType;
begin
  Assert(FSearchSiteTypeSet);
  result := FSearchSiteType;
end;

procedure TKIdIndexSearchVal.SetSearchListLevel(LL: TKListLevel);
begin
  Assert(not FSearchListLevelSet);
  FSearchListLevel := LL;
  FSearchListLevelSet := true;

  case FSearchListLevel of
    klUserList: FSearchUser := TKUserProfile.Create;
    klMediaList:
      FSearchMedia := TKMediaItem.Create;
    klCommentList:
      FSearchComment := TKCommentItem.Create;
  else
    Assert(false);
  end;
end;

function TKIdIndexSearchVal.GetSearchListLevel: TKListLevel;
begin
  Assert(FSearchListLevelSet);
  result := FSearchListLevel;
end;

function TKIdIndexSearchVal.GetSearchString: string;
begin
  case SearchListLevel of
    klUserList:
      result := FSearchUser.SiteUserBlock[SearchSiteType].UserId;
    klMediaList:
      result := FSearchMedia.SiteMediaBlock[SearchSiteType].MediaID;
    klCommentList:
      result := FSearchComment.SiteCommentBlock[SearchSiteType].CommentId;
  else
    Assert(false);
  end;
end;

procedure TKIdIndexSearchVal.SetSearchString(S: string);
begin
  case SearchListLevel of
    klUserList:
      FSearchUser.SiteUserBlock[SearchSiteType].UserId := S;
    klMediaList:
      FSearchMedia.SiteMediaBlock[SearchSiteType].MediaID := S;
    klCommentList:
      FSearchComment.SiteCommentBlock[SearchSiteType].CommentId := S;
  else
    Assert(false);
  end;
end;

function TKIdIndexSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64;
  OtherNode: TIndexNode): integer;
var
  FSearchItem: TObject;
begin
  Assert(FSearchListLevelSet);
  case SearchListLevel of
    klUserList:
      FSearchItem := FSearchUser;
    klMediaList:
      FSearchItem := FSearchMedia;
    klCommentList:
      FSearchItem := FSearchComment;
  else
    FSearchItem := nil;
    Assert(false);
  end;
  result := inherited CompareItems(FSearchItem, OtherItem, IndexTag, OtherNode);
end;

destructor TKIdIndexSearchVal.Destroy;
begin
  FSearchUser.Free;
  FSearchMedia.Free;
  FSearchComment.Free;
  inherited;
end;

{ TKUserNameIndex }

function TKUserNameIndex.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64;
  OtherNode: TIndexNode): integer;
var
  CmpIndex: TKUserNameIndex;
  OtherProf, OwnProf: TKUserProfile;
  ST: TKSiteType;
begin
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  if not GetSiteType(ST) then
  begin
    CmpIndex := OtherNode as TKUserNameIndex;
    if not CmpIndex.GetSiteType(ST) then
      Assert(false);
  end;
  OtherProf := OtherItem as TKUserProfile;
  OwnProf := OwnItem as TKUserProfile;
  result := CompareDecimalKeyStr(OtherProf.FSiteUserBlock[ST].Username,
    OwnProf.FSiteUserBlock[ST].Username);
end;

{ TKUserNameSearchVal }

procedure TKUserNameSearchVal.SetSearchSiteType(ST: TKSiteType);
begin
  Assert(not FSearchSiteTypeSet);
  FSearchSiteType := ST;
  FSearchSiteTypeSet := true;
end;

function TKUserNameSearchVal.GetSearchSiteType: TKSiteType;
begin
  Assert(FSearchSiteTypeSet);
  result := FSearchSiteType;
end;

function TKUserNameSearchVal.GetSearchString: string;
begin
  result := FSearchUserProf.FSiteUserBlock[SearchSiteType].Username;
end;

procedure TKUserNameSearchVal.SetSearchString(S: string);
begin
  FSearchUserProf.FSiteUserBlock[SearchSiteType].Username := S;
end;

constructor TKUserNameSearchVal.Create;
begin
  inherited;
  FSearchUserProf := TKUserProfile.Create;
end;

destructor TKUserNameSearchVal.Destroy;
begin
  FSearchUserProf.Free;
  inherited;
end;

function TKUserNameSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64;
  OtherNode: TIndexNode): integer;
begin
  result := inherited CompareItems(FSearchUserProf, OtherItem, IndexTag, OtherNode);
end;

{ TKGeneralListIndex }

function TKGeneralListIndex.GetListLevel(out TL: TKListLevel): boolean;
var
  Index: integer;
  IndexTag: TKIndexTag;
begin
  result := Assigned(IndexLink);
  if result then
  begin
    Index := IndexLink.RootIndex.Tag;
    Assert(Index >= Ord(Low(TKIndexTag)));
    Assert(Index <= Ord(High(TKIndexTag)));
    IndexTag := TKIndexTag(Index);
    TL := IndexTagToListLevel(IndexTag);
  end;
end;

{ TKDateIndex }

function TKDateIndex.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64;
  OtherNode: TIndexNode): integer;
var
  CmpIndex: TKDateIndex;
  OtherProf, OwnProf: TKUserProfile;
  OtherM, OwnM: TKMediaItem;
  OtherC, OwnC: TKCommentItem;
  KL: TKListLevel;
  DSF: TKDateSearchField;
begin
  result := 0;
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  if not GetDateSearchField(DSF) then
  begin
    CmpIndex := OtherNode as TKDateIndex;
    if not CmpIndex.GetDateSearchField(DSF) then
      Assert(false);
  end;
  if not GetListLevel(KL) then
  begin
    CmpIndex := OtherNode as TKDateIndex;
    if not CmpIndex.GetListLevel(KL) then
      Assert(false);
  end;
  case KL of
    klUserList:
      begin
        OtherProf := OtherItem as TKUserProfile;
        OwnProf := OwnItem as TKUserProfile;
        case DSF of
          dsfLastUpdated:
            begin
              if OtherProf.LastUpdated > OwnProf.LastUpdated then
                result := 1
              else if OtherProf.LastUpdated < OwnProf.LastUpdated then
                result := -1
              else
                result := 0;
            end
        else
          Assert(false);
        end;
      end;
    klMediaList:
      begin
        OwnM := OwnItem as TKMediaItem;
        OtherM := OtherItem as TKMediaItem;
        case DSF of
          dsfDate:
            begin
              if OtherM.Date > OwnM.Date then
                result := 1
              else if OtherM.Date < OwnM.Date then
                result := -1
              else
                result := 0;
            end;
          dsfLastUpdated:
            begin
              if OtherM.LastUpdated > OwnM.LastUpdated then
                result := 1
              else if OtherM.LastUpdated < OwnM.LastUpdated then
                result := -1
              else
                result := 0;
            end
        else
          Assert(false);
        end;
      end;
    klCommentList:
      begin
        OwnC := OwnItem as TKCommentItem;
        OtherC := OtherItem as TKCommentItem;
        case DSF of
          dsfDate:
            begin
              if OtherC.Date > OwnC.Date then
                result := 1
              else if OtherC.Date < OwnC.Date then
                result := -1
              else
                result := 0;
            end;
          dsfLastUpdated:
            begin
              if OtherC.LastUpdated > OwnC.LastUpdated then
                result := 1
              else if OtherC.LastUpdated < OwnC.LastUpdated then
                result := -1
              else
                result := 0;
            end
        else
          Assert(false);
        end;
      end;
  else
    begin
      Assert(false);
    end;
  end;
end;

function TKDateIndex.GetDateSearchField(out DSF: TKDateSearchField): boolean;
var
  Index: integer;
  IndexTag: TKIndexTag;
begin
  result := Assigned(IndexLink);
  if result then
  begin
    Index := IndexLink.RootIndex.Tag;
    Assert(Index >= Ord(Low(TKIndexTag)));
    Assert(Index <= Ord(High(TKIndexTag)));
    IndexTag := TKIndexTag(Index);
    IndexTagToDateSearchField(IndexTag, DSF);
  end;
end;

{ TKDateIndexSearchVal }

procedure TKDateIndexSearchVal.SetSearchListLevel(LL: TKListLevel);
begin
  Assert(not FSearchListLevelSet);
  FSearchListLevel := LL;
  FSearchListLevelSet := true;
end;

function TKDateIndexSearchVal.GetSearchListLevel: TKListLevel;
begin
  Assert(FSearchListLevelSet);
  result := FSearchListLevel;
end;

procedure TKDateIndexSearchVal.SetSearchDateField(DSF: TKDateSearchField);
begin
  Assert(not FSearchDateFieldSet);
  FSearchDateField := DSF;
  FSearchDateFieldSet := true;
end;

function TKDateIndexSearchVal.GetSearchDateField: TKDateSearchField;
begin
  Assert(FSearchDateFieldSet);
  result := FSearchDateField;
end;

function TKDateIndexSearchVal.GetSearchDate: TDateTime;
begin
  result := 0;
  Assert(FSearchDateFieldSet);
  Assert(FSearchListLevelSet);
  case FSearchListLevel of
    klUserList:
      begin
        case FSearchDateField of
          dsfLastUpdated:
            result := FSearchUser.LastUpdated;
        else
          Assert(false);
        end;
      end;
    klMediaList:
      begin
        case FSearchDateField of
          dsfDate:
            result := FSearchMedia.Date;
          dsfLastUpdated:
            result := FSearchMedia.LastUpdated;
        else
          Assert(false);
        end;
      end;
    klCommentList:
      begin
        case FSearchDateField of
          dsfDate:
            result := FSearchComment.Date;
          dsfLastUpdated:
            result := FSearchComment.LastUpdated;
        else
          Assert(false);
        end;
      end;
  else
    Assert(false);
  end;
end;

procedure TKDateIndexSearchVal.SetSearchDate(NewDate: TDateTime);
begin
  Assert(FSearchDateFieldSet);
  Assert(FSearchListLevelSet);
  case FSearchListLevel of
    klUserList:
      begin
        case FSearchDateField of
          dsfLastUpdated:
            FSearchUser.LastUpdated := NewDate;
        else
          Assert(false);
        end;
      end;
    klMediaList:
      begin
        case FSearchDateField of
          dsfDate:
            FSearchMedia.Date := NewDate;
          dsfLastUpdated:
            FSearchMedia.LastUpdated := NewDate;
        else
          Assert(false);
        end;
      end;
    klCommentList:
      begin
        case FSearchDateField of
          dsfDate:
            FSearchComment.Date := NewDate;
          dsfLastUpdated:
            FSearchComment.LastUpdated := NewDate;
        else
          Assert(false);
        end;
      end;
  else
    Assert(false);
  end;
end;

constructor TKDateIndexSearchVal.Create;
begin
  inherited;
  FSearchUser := TKUserProfile.Create;
  FSearchMedia := TKMediaItem.Create;
  FSearchComment := TKCommentItem.Create;
end;

destructor TKDateIndexSearchVal.Destroy;
begin
  FSearchUser.Free;
  FSearchMedia.Free;
  FSearchComment.Free;
  inherited;
end;

function TKDateIndexSearchVal.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64;
  OtherNode: TIndexNode): integer;
var
  FSearchItem: TObject;
begin
  Assert(FSearchListLevelSet);
  case SearchListLevel of
    klUserList:
      FSearchItem := FSearchUser;
    klMediaList:
      FSearchItem := FSearchMedia;
    klCommentList:
      FSearchItem := FSearchComment;
  else
    FSearchItem := nil;
    Assert(false);
  end;
  result := inherited CompareItems(FSearchItem, OtherItem, IndexTag, OtherNode);
end;

{ TKPresentationOrderIndex }

  //N.B If you change presentation order, be aware that some
  //parts of the UI logic expect interested and/or verified users
  //to be at the start of the list in this order.
function TKPresentationOrderIndex.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode): integer;
var
  KL: TKListLevel;
  CmpIndex: TKPresentationOrderIndex;
  OwnU, OtherU: TKUserProfile;
  OwnM, OtherM: TKMediaItem;
  OwnC, OtherC: TKCommentItem;
  OwnUname, OtherUname: string;
  OwnBS, OtherBS: cardinal;
  OwnVerified, OtherVerified: boolean;
  ST: TKSiteType;
begin
  result := 0;
  Assert(Assigned(OwnItem));
  Assert(Assigned(OtherItem));
  if not GetListLevel(KL) then
  begin
    CmpIndex := OtherNode as TKPresentationOrderIndex;
    if not CmpIndex.GetListLevel(KL) then
      Assert(false);
  end;
  case KL of
    klUserList:
    begin
      OwnU := OwnItem as TKUserProfile;
      OtherU := OtherItem as TKUserProfile;
      //Sort by interest level,
      result := Ord(OwnU.InterestLevel) - Ord(OtherU.InterestLevel);
      if result = 0 then
      begin
        //then by verified flag
        OwnVerified := false;
        OtherVerified := false;
        for ST := Low(ST) to High(ST) do
        begin
          if Assigned(OwnU.SiteUserBlocks[ST]) and OwnU.SiteUserBlocks[ST].Valid then
          begin
            if OwnU.SiteUserBlocks[ST].Verified then
            begin
              OwnVerified := true;
              break;
            end;
          end;
        end;
        for ST := Low(ST) to High(ST) do
        begin
          if Assigned(OtherU.SiteUserBlocks[ST]) and OtherU.SiteUserBlocks[ST].Valid then
          begin
            if OtherU.SiteUserBlocks[ST].Verified then
            begin
              OtherVerified := true;
              break;
            end;
          end;
        end;
        result := Ord(OwnVerified) - Ord(OtherVerified);
      end;
      if result = 0 then
      begin
        //then usernames (first site type found) case insensitive compare text.
        for ST := Low(ST) to High(ST) do
        begin
          if Assigned(OwnU.SiteUserBlocks[ST]) and OwnU.SiteUserBlocks[ST].Valid then
          begin
            OwnUname := OwnU.SiteUserBlocks[ST].Username;
            break;
          end;
        end;
        for ST := Low(ST) to High(ST) do
        begin
          if Assigned(OtherU.SiteUserBlocks[ST]) and OtherU.SiteUserBlocks[ST].Valid then
          begin
            OtherUname := OtherU.SiteUserBlocks[ST].Username;
            break;
          end;
        end;
        result := CompareText(OtherUname, OwnUName);
      end;
      //then bitmap of site types valid.
      if result = 0 then
      begin
        Assert(Ord(High(ST)) < ((8 * sizeof(cardinal)) -1));
        OwnBS := 0;
        OtherBS := 0;
        for ST := Low(ST) to High(ST) do
        begin
          if Assigned(OwnU.SiteUserBlocks[ST]) and OwnU.SiteUserBlocks[ST].Valid then
            OwnBS := OwnBS or (1 shl Ord(ST));
          if Assigned(OtherU.SiteUserBlocks[ST]) and OtherU.SiteUserBlocks[ST].Valid then
            OtherBS := OtherBS or (1 shl Ord(ST));
        end;
        result := Integer(OtherBS) - Integer(OwnBS);
      end;
      //let the system disambiguate by pointer.
    end;
    klMediaList:
    begin
      OwnM := OwnItem as TKMediaItem;
      OtherM := OtherItem as TKMediaItem;
      //Sort by date, most recent at the top.
      if OtherM.Date < OwnM.Date then
        result := 1
      else if OtherM.Date > OwnM.Date then
        result := -1
      else
        result := 0;
    end;
    klCommentList:
    begin
      OwnC := OwnItem as TKCommentItem;
      OtherC := OtherItem as TKCommentItem;
      //Sort by date, most recent at the top.
      if OtherC.Date < OwnC.Date then
        result := 1
      else if OtherC.Date > OwnC.Date then
        result := -1
      else
        result := 0;
    end;
  else
    Assert(false);
  end;
end;

function ProfileV(SrcUP: TKUserProfile): boolean;
var
  ST: TKSiteType;
begin
  result := Assigned(SrcUP);
  if result then
  begin
    result := false;
    for ST := Low(ST) to High(ST) do
    begin
      if SrcUP.SiteUserBlock[ST].Valid and SrcUP.SiteUserBlock[ST].Verified then
      begin
        result := true;
        break;
      end;
    end;
  end;
end;

function ProfileVorI(SrcUP: TKUserProfile): boolean;
begin
  result := Assigned(SrcUP);
  if result then
  begin
    result := SrcUP.FInterestLevel > kpiFetchUserForRefs;
    if not result then
      result := ProfileV(SrcUp);
  end;
end;

{ TKIdentifierOrderIndex }

function TKIdentifierOrderIndex.CompareItems(OwnItem, OtherItem: TObject; IndexTag: Int64; OtherNode: TIndexNode)
  : integer;
var
  OwnS, OtherS: string;
  LL: TKListLevel;
  CmpIndex: TKIdentifierOrderIndex;
begin
  if not GetListLevel(LL) then
  begin
    CmpIndex := OtherNode as TKIdentifierOrderIndex;
    if not CmpIndex.GetListLevel(LL) then
      Assert(false);
  end;
  OwnS := IdentifierStringFromItem(OwnItem as TKKeyedObject, LL);
  OtherS := IdentifierStringFromItem(OtherItem  as TKKeyedObject, LL);
  result := CompareStr(OwnS, OtherS);
end;

{ TKIdentifierOrderIndexSearchVal }

function TKIdentifierOrderSearchVal.CompareItems(OwnItem, OtherItem: TObject;
  IndexTag: Int64; OtherNode: TIndexNode): integer;
var
  LL: TKListLevel;
  CmpIndex: TKIdentifierOrderIndex;
  OtherS: string;
begin
  if not GetListLevel(LL) then
  begin
    CmpIndex := OtherNode as TKIdentifierOrderIndex;
    if not CmpIndex.GetListLevel(LL) then
      Assert(false);
  end;
  OtherS := IdentifierStringFromItem(OtherItem as TKKeyedObject, LL);
  result := CompareStr(FSearchString, OtherS);
end;

end.
