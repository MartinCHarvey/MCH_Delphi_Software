unit InstagramImporters;
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

uses
  Importers, FetcherParser, DataObjects, JSONNodes, JSNodes, HTMLNodes,
  CommonNodes, SyncObjs;

type
  TInstaMediaImporter = class(TMediaImporter)
  private
    FMediaItem: TKMediaItem;
    FCommentBailoutTime: TDateTime;
    FInitialFetchOnly: boolean;
    FBailout: boolean;
    FRhxGis: string;
    FNav: TJSNavHelper;
    FJNav: TJSONNavHelper;
  protected
    function SupplementaryQueriesFinished(FPResult: TFetchParseResult): boolean;

    procedure UpdateCommentItemsFromJSON(CX: TKCommentList; Obj: TKMediaItem;
      Nodes: TJSONContainer);
    procedure UpdateCommentsAndExtentsFromJSON(Obj: TKMediaItem; Nodes: TJSONDocument);
    procedure PopulateCommentItemsFromJS(Obj: TKMediaItem; NodeArray: TJSExprList);
    procedure PopulateCommentsAndExtentsFromJS(Obj: TKMediaItem; CommentFields: TJSExprList);
    procedure PopulateMediaObjFromJS(Obj: TKMediaItem; MediaFields: TJSExprList);

    function MakeMediaItemFromMediaPage(Doc: THTMLNode): TKMediaItem;
    function ProcessParseTree(WorkItem: TImporterWorkItem; var ErrMsg: string): boolean; override;
  public
    constructor Create;
    destructor Destroy; override;
    function RequestMedia(Block: TKSiteMediaBlock; Options: TImportOptions): boolean;
    function RetrieveResult(var Media: TKMediaItem; var ErrInfo: TImportErrInfo): boolean; override;
  end;

  TInstaUserProfileImporter = class(TUserProfileImporter)
  private
    FUserProfile: TKUserProfile;
    FMediaBailoutTime: TDateTime;
    FInitialFetchOnly: boolean;
    FBailout: boolean;
    FRhxGis: string;
    FNav: TJSNavHelper;
    FJNav: TJSONNavHelper;
  protected
    function SupplementaryQueriesFinished(FPResult: TFetchParseResult): boolean;

    procedure UpdateMediaItemsFromJSON(MX: TKMediaList; Obj: TKUserProfile; Nodes: TJSONContainer);
    procedure UpdateMediaAndExtentsFromJSON(Obj: TKUserProfile; Nodes: TJSONDocument);
    procedure PopulateMediaItemsFromJS(Obj: TKUserProfile; NodeArray: TJSExprList);
    procedure PopulateMediaAndExtentsFromJS(Obj: TKUserProfile; MediaFields: TJSExprList);
    procedure PopulateUserObjFromJS(Obj: TKUserProfile; UserFields: TJSExprList);

    // Consumes doc.
    function MakeUserObjFromProfilePage(Doc: THTMLNode): TKUserProfile;
    function ProcessParseTree(WorkItem: TImporterWorkItem; var ErrMsg: string): boolean; override;
  public
    constructor Create;
    destructor Destroy; override;
    function RequestUserProfile(Block: TKSiteUserBlock; Options: TImportOptions): boolean;
    function RetrieveResult(var UserProfile: TKUserProfile; var ErrInfo: TImportErrInfo)
      : boolean; override;
  end;

const
  S_IFIELD_PROFILE_PAGE: string = 'ProfilePage';
  S_IFIELD_POST_PAGE: string = 'PostPage';
  S_IFIELD_USER: string = 'user';
  S_IFIELD_GRAPHQL: string = 'graphql';
  S_IFIELD_SHORTCODE_MEDIA: string = 'shortcode_media';
  S_IFIELD_RHX_GIS: string = 'rhx_gis';

  S_REQUEST_FAILED = 'Request failed, possibly out of memory?';


implementation

uses
  SysUtils, DLList, IndexedStore, Classes, HTTPDocFetcher, HTMLEscapeHelper,
  Trackables, KKUtils,
{$IFDEF INCLUDE_AUTH}
  CredentialManager,
{$ENDIF}
  IdHashMessageDigest, IdGlobal,
  GlobalLog
{$IFOPT C+}
    , Windows
{$ENDIF}
    ;

const
  S_INSTA_MEDIA_URL_PREFIX: string = 'p/';
  S_INSTAGRAM_QUERY_URI: string = 'graphql/query/';
  S_SLASH: string = '/';

  // Instagram profile page parsing, user fields.
  S_IFIELD_USERNAME: string = 'username';
  S_IFIELD_FOLLOWS_COUNT: string = 'edge_follow';
  S_IFIELD_FOLLOWER_COUNT: string = 'edge_followed_by';
  S_IFIELD_PROF_PIC_URL: string = 'profile_pic_url';
  S_IFIELD_ID: string = 'id';
  S_IFIELD_USERBIO: string = 'biography';
  S_IFIELD_FULLNAME: string = 'full_name';
  S_IFIELD_VERIFIED: string = 'is_verified';
  S_IFIELD_COUNT: string = 'count';

  S_IFIELD_MEDIALIST: string = 'media';
  S_IFIELD_PAGE_INFO: string = 'page_info';
  S_IFIELD_EDGE_OWNER_TO_TIMELINE_MEDIA: string = 'edge_owner_to_timeline_media';
  S_IFIELD_NODES: string = 'nodes';
  S_IFIELD_NODE: string = 'node';
  S_IFIELD_EDGES: string = 'edges';
  S_IFIELD_HAS_PREVIOUS: string = 'has_previous_page';
  S_IFIELD_HAS_NEXT: string = 'has_next_page';
  S_IFIELD_START_CURSOR: string = 'start_cursor';
  S_IFIELD_END_CURSOR: string = 'end_cursor';

  S_IFIELD_CODE: string = 'code';
  S_IFIELD_SHORTCODE: string = 'shortcode';
  S_IFIELD_DATE: string = 'date';
  S_IFIELD_TAKEN_AT_TIMESTAMP: string = 'taken_at_timestamp';
  S_IFIELD_DISPLAY_SRC: string = 'display_src';
  S_IFIELD_THUMBNAIL_SRC: string = 'thumbnail_src';
  S_IFIELD_DISPLAY_URL: string = 'display_url';
  S_IFIELD_CAPTION: string = 'caption';
  S_IFIELD_EDGE_MEDIA_TO_CAPTION: string = 'edge_media_to_caption';
  S_IFIELD_EDGE_MEDIA_TO_PARENT_COMMENT: string = 'edge_media_to_parent_comment';
  S_IFIELD_EDGE_MEDIA_TO_COMMENT: string = 'edge_media_to_comment';
  S_IFIELD_OWNER: string = 'owner';

  S_INSTA_ERR_JSON_COMMENTS = 'Error reading comments from JSON encoding.';
  S_INSTA_ERR_JSON_COMMENTS_2 = 'Error reading comments from JSON encoding. (case 2)';
  S_INSTA_ERR_COMMENT_ITEM_SANITY_CHECK = 'Decoded comment fails sanity check.';
  S_INSTA_ERR_COMMENT_ITEM_SANITY_CHECK_2 = 'Decoded comment fails sanity check. (case 2)';
  S_INSTA_ERR_COMMENT_ITEM_INDEXING = 'Error adding comment to list, probably indexing.';
  S_INSTA_ERR_COMMENT_ITEM_INDEXING_2 = 'Error adding comment to list, probably indexing. (case 2)';
  S_INSTA_ERR_COMMENTS_EXTENTS_JSON = 'Error reading comments and extents from JSON.';
  S_INSTA_ERR_COMMENTS_EXTENTS_JSON_2 = 'Error reading comments and extents from JSON. (case 2)';
  S_INSTA_ERR_COMMENTS_EXTENTS_JSON_3 = 'Error reading comments and extents from JSON. (case 3)';
  S_INSTA_ERR_MEDIA_SUPP_QUERIES = 'Error generating supplementary queries after media item.';
  S_INSTA_ERR_MEDIA_SUPP_QUERIES_2 = 'Error generating supplementary queries after media item. (case 2)';
  S_INSTA_ERR_MEDIA_SUPP_QUERIES_3 = 'Error generating supplementary queries after media item. (case 3)';
  S_INSTA_ERR_COMMENTS_FROM_JS_1 = 'Error reading comments from Javascript.';
  S_INSTA_ERR_COMMENTS_FROM_JS_2 = 'Error reading comments from Javascript. (case 2)';
  S_INSTA_ERR_COMMENTS_FROM_JS_3 = 'Error reading comments from Javascript. (case 3)';
  S_INSTA_ERR_COMMENTS_FROM_JS_4 = 'Error reading comments from Javascript. (case 4)';

  S_INSTA_ERR_MEDIA_FROM_JS = 'Error reading media item from Javascript.';
  S_INSTA_ERR_MEDIA_PAGE_NO_POSTPAGE = 'Reading media page: No PostPage in JS.';
  S_INSTA_ERR_MEDIA_PAGE_NO_GRAPHQL = 'Reading media page: No GraphQL in JS.';
  S_INSTA_ERR_MEDIA_ITEM_SANITY_CHECK = 'Media item fails sanity check.';
  S_INSTA_ERR_UPDATE_MEDIA_FROM_JSON = 'Error reading media item from JSON.';
  S_INSTA_ERR_UPDATE_MEDIA_FROM_JSON_2 = 'Error reading media item from JSON. (case 2)';
  S_INSTA_ERR_MEDIA_ITEM_SANITY_CHECK_2 = 'Media item fails sanity check. (case 2)';
  S_INSTA_ERR_MEDIA_AND_EXTENTS_FROM_JSON = 'Error updating media and extents from JSON.';
  S_INSTA_ERR_MEDIA_AND_EXTENTS_FROM_JSON_2 = 'Error updating media and extents from JSON. (case 2)';
  S_INSTA_ERR_MEDIA_AND_EXTENTS_FROM_JSON_3 = 'Error updating media and extents from JSON. (case 3)';
  S_INSTA_ERR_MEDIA_ITEM_INDEXING = 'Error adding media item (indexing?).';
  S_INSTA_ERR_MEDIA_ITEM_INDEXING_2 = 'Error adding media item (indexing?). (case 2)';
  S_INSTA_ERR_MEDIA_ITEM_INDEXING_3 = 'Error adding media item (indexing?). (case 3)';
  S_INSTA_ERR_MEDIA_ITEMS_FROM_JS = 'Error reading media items from Javascript.';
  S_INSTA_ERR_MEDIA_ITEMS_FROM_JS_2 = 'Error reading media items from Javascript. (case 2)';
  S_INSTA_ERR_MEDIA_ITEMS_FROM_JS_3 = 'Error reading media items from Javascript. (case 3)';
  S_INSTA_ERR_MEDIA_ITEMS_FROM_JS_4 = 'Error reading media items from Javascript. (case 4)';
  S_INSTA_ERR_MEDIA_ITEM_SANITY_CHECK_3 = 'Media item fails sanity check. (case 3)';
  S_INSTA_ERR_POPULATE_USER_OBJ_FROM_JS = 'Error reading user object from Javascript.';
  S_INSTA_ERR_POPULATE_USER_OBJ_FROM_JS_2 = 'Error reading user object from Javascript. (case 2)';
  S_INSTA_ERR_POPULATE_USER_OBJ_FROM_JS_3 = 'Error reading user object from Javascript. (case 3)';
  S_INSTA_ERR_POPULATE_USER_OBJ_FROM_JS_4 = 'Error reading user object from Javascript. (case 4)';
  S_INSTA_ERR_USER_OBJ_NO_PROFILE_PAGE = 'Reading user object: No ProfilePage in JS';
  S_INSTA_ERR_USER_OBJ_NO_GRAPHQL = 'Reading user object: No GraphQL in JS';
  S_INSTA_ERR_USER_OBJ_FAILS_SANITY_CHECK = 'User obj read from JS fails sanity check';
  S_INST_ERR_USERPROF_FAIL_SUPP_QUERIES = 'Supplementary queries on user profile failed.';
  S_INST_ERR_USERPROF_FAIL_SUPP_QUERIES_2 = 'Supplementary queries on user profile failed. (case 2)';
  S_INST_ERR_USERPROF_FAIL_SUPP_QUERIES_3 = 'Supplementary queries on user profile failed. (case 3)';


const
  FetchItemCount = 200;

  // Instagram supplementary query constants - common.
  S_INSTA_Q_QUERY_HASH: string = 'query_hash';
  S_INSTA_Q_COMMON_VARS: string = 'variables';

  // Instagram supplementary query constants - media.
  // TODO - Obtain this from the initial page fetch??
  S_INSTA_Q_QUERY_HASH_MEDIA: string = 'f2405b236d85e8296cf30347c9f08c2a';

  S_INSTA_Q_MEDIA_ID: string = '{"id":"';
  // Plus user id.
  S_INSTA_Q_MEDIA_FIRST: string = '","first":';
  // Plus count.
  S_INSTA_Q_MEDIA_AFTER: string = ',"after":"';
  S_INSTA_Q_MEDIA_BEFORE: string = ',"before":"';
  // Plus magic mungeification of cursor.
  S_INSTA_Q_MEDIA_END: string = '"}';

  // Instagram supplementary query constants - comments.
  // TODO - Obtain this from the initial page fetch??
  S_INSTA_Q_QUERY_HASH_COMMENTS: string = 'f0986789a5c5d17c2400faebf16efd0d';

  // Query for later comments.
  S_INSTA_Q_COMMENTS_SHORTCODE: string = '{"shortcode":"';
  // Plus shortcode.
  S_INSTA_Q_COMMENTS_FIRST: string = '","first":';
  // = S_INSTA_Q_MEDIA_FIRST;
  // Plus count of items.
  S_INSTA_Q_COMMENTS_AFTER: string = ',"after":"';
  // = S_INSTA_Q_MEDIA_AFTER;
  S_INSTA_Q_COMMENTS_BEFORE: string = ',"before":"';
  // = S_INSTA_Q_MEDIA_BEFORE;
  // Plus magic mungeification of cursor.
  S_INSTA_Q_COMMENTS_END: string = '"}';
  // = S_INSTA_Q_MEDIA_END;

  S_INSTA_APP_ENCODING: string = 'application/x-www-form-urlencoded';

  S_JSON_STATUS: string = 'status';
  S_JSON_DATA: string = 'data';
  S_JSON_OK: string = 'ok';

  // Instagram parsing, media fields.
  S_IFIELD_COMMENTS: string = 'comments';
  S_IFIELD_TEXT: string = 'text';
  S_IFIELD_CREATED_AT: string = 'created_at';

  S_IHDR_X_INSTAGRAM_GIS = 'X-Instagram-GIS';
  S_IHDR_X_IG_APP_ID = 'x-ig-app-id';
  // TODO - Obtain this from page fetch or elsewhere?
  S_IHDR_X_IG_APP_ID_VAL = '936619743392459';

  { Misc helper functions }

type
  TJSONQuoteType = (jqtRaw, jqtQuoteDouble);

procedure SetXInstagramGISHdr(Opts: TFetcherOpts; RhxGis: string; Variables: string);
var
  HdrIdx: integer;
  Found: boolean;
  HashString: string;
  Md5String: string;
  MD5: TIdHashMessageDigest5;
  TokenBytes, MD5Bytes: TIdBytes;
  OK: boolean;
begin
  Found := false;
  for HdrIdx := 0 to Pred(Opts.SupplementaryHeaderNames.Count) do
  begin
    if Opts.SupplementaryHeaderNames[HdrIdx] = S_IHDR_X_INSTAGRAM_GIS then
    begin
      Found := true;
      break;
    end;
  end;
  if not Found then
  begin
    HdrIdx := Opts.SupplementaryHeaderNames.Count;
    Opts.SupplementaryHeaderNames.Add(S_IHDR_X_INSTAGRAM_GIS);
    Opts.SupplementaryHeaderVals.Add(''); // Fill the string in later.
  end;
  HashString := Variables;
  HashString := RhxGis + ':' + HashString;
  MD5 := TIdHashMessageDigest5.Create;
  try
    OK := UTF8StringToIdBytes(HashString, TokenBytes);
    Assert(OK);
    // Md5Bytes := MD5.HashValue(TokenBytes);
    MD5Bytes := MD5.HashBytes(TokenBytes);
    Md5String := IdBytesToHexString(MD5Bytes);
  finally
    MD5.Free;
  end;
  Opts.SupplementaryHeaderVals[HdrIdx] := Md5String;
  // And the app hdr.
  for HdrIdx := 0 to Pred(Opts.SupplementaryHeaderNames.Count) do
  begin
    if Opts.SupplementaryHeaderNames[HdrIdx] = S_IHDR_X_IG_APP_ID then
    begin
      Found := true;
      break;
    end;
  end;
  if not Found then
  begin
    Opts.SupplementaryHeaderNames.Add(S_IHDR_X_IG_APP_ID);
    Opts.SupplementaryHeaderVals.Add(S_IHDR_X_IG_APP_ID_VAL);
    // Fill the string in later.
  end;
end;

function GetRhxGis(Nav: TJSNavHelper; Node: TJSNode): string;
// Node should be "window.shared_data"...
begin
  result := '';
  if Nav.NavAssignmentToRHS(Node) then
  begin
    if Nav.NavToFieldList(Node) then
    begin
      if Nav.NavChildToFieldValue(Node, 'rhx_gis', setStringLiteral) then
      begin
        result := (Node as TJSString).StrData;
        Nav.Pop(Node);
      end;
      Nav.Pop(Node);
    end;
    Nav.Pop(Node);
  end;
end;

function GenFirstOpts(ThrottleDelay: integer): TFetcherOpts;
begin
  result := TFetcherOpts.Create;
  result.SendCSRFToken := true;
  result.ThrottleDelay := ThrottleDelay;
  result.RequireAuthBeforeFetch := false;
  result.DesireAuth := false;
  result.OAuth2SignRequest := signAddAndSign;
  result.CustomUserAgent := S_USER_AGENT;
end;

function GenNextOpts(PrevOpts: TFetcherOpts; ThrottleDelay: integer; PrevUrl: string): TFetcherOpts;
var
  Proto, Site, Fullfile, Name, Ext: string;
begin
  result := TFetcherOpts.Create;
  result.SendCSRFToken := true;
  result.RequestContentType := S_INSTA_APP_ENCODING;
  result.CustomUserAgent := S_USER_AGENT;
  if Assigned(PrevOpts) then
  begin
    result.RequireAuthBeforeFetch := PrevOpts.RequireAuthBeforeFetch;
    result.DesireAuth := PrevOpts.DesireAuth;
    result.OAuth2SignRequest := signNone;
    result.SupplementaryHeaderNames.Assign(PrevOpts.SupplementaryHeaderNames);
    result.SupplementaryHeaderVals.Assign(PrevOpts.SupplementaryHeaderVals);
    // result.OAuth2SignRequest := PrevOpts.OAuth2SignRequest;
  end
  else
  begin
    result.RequireAuthBeforeFetch := false;
    result.DesireAuth := false;
    result.OAuth2SignRequest := signNone;
    // result.OAuth2SignRequest := signAddAndSign;
  end;
  if not(Assigned(PrevOpts) and (Length(PrevOpts.Referer) > 0)) then
    result.Referer := PrevUrl
  else
    result.Referer := PrevOpts.Referer;

  if Length(result.Referer) > 0 then
  begin
    ParseURL(result.Referer, Proto, Site, Fullfile, Name, Ext);
    result.RequestCustomOrigin := BuildURL(Proto, Site, '', false, nil, nil);
  end;
  result.ThrottleDelay := ThrottleDelay;
end;

function FindLabelledPage(Node: TJSNode; Nav: TJSNavHelper; var RhxGis: string;
  PageTitle: string): TJSNode;
begin
  result := nil;
  if Nav.PushChildRec(Node, 'window._sharedData = {}', jttStatement) then
  begin
    RhxGis := GetRhxGis(Nav, Node);
    if Nav.NavAssignmentToRHS(Node, false) and Nav.NavToFieldList(Node, false) and
      Nav.NavChildToChildFieldList(Node, 'entry_data', false) and
      Nav.NavChildToFieldValue(Node, PageTitle, setInvalid, false) and
      Nav.NavToArrayElems(Node, false) and not DlItemIsEmpty(@Node.ContainedListHead) then
    begin
      Node := Node.ContainedListHead.FLink.Owner as TJSNode;
      if Nav.NavToFieldList(Node, false) then
        result := Node;
    end;
    Nav.Pop(Node);
  end;
end;

{ TInstaMediaImporter }

constructor TInstaMediaImporter.Create;
begin
  inherited;
  FNav := TJSNavHelper.Create;
  FJNav := TJSONNavHelper.Create;
end;

destructor TInstaMediaImporter.Destroy;
begin
  FMediaItem.Free;
  FMediaItem := nil;
  FNav.Free;
  FJNav.Free;
  inherited;
end;

procedure TInstaMediaImporter.UpdateCommentItemsFromJSON(CX: TKCommentList; Obj: TKMediaItem;
  Nodes: TJSONContainer);
var
  Node: TJSONNode;
  CommentItem: TKCommentItem;
begin
  Assert(Assigned(Nodes));
  Node := (Nodes.ContainedListHead.FLink.Owner) as TJSONNode;
  while Assigned(Node) do
  begin
    if ((Node as TJSONContainer).ContainerType <> jctObject) then
      raise EImportError.Create(S_INSTA_ERR_JSON_COMMENTS);
    CommentItem := TKCommentItem.Create;
    try
      CommentItem.SiteUserBlock[tstInstagram].Valid := true;
      CommentItem.SiteCommentBlock[tstInstagram].Valid := true;
      CommentItem.LastUpdated := Now;

      if FJNav.NavToMemberValue(Node, S_IFIELD_NODE, sftObject) then
      begin
        if FJNav.NavToMemberValue(Node, S_IFIELD_TEXT, sftString) then
        begin
          CommentItem.CommentData := (Node as TJSONSimpleValue).StrData;
          CommentItem.CommentType := citPlainText;
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_CREATED_AT, sftNumber) then
        begin
          try
            CommentItem.Date := UnixDateTimeToDelphiDateTime
              (Trunc(StrToFloat((Node as TJSONSimpleValue).StrData)));
          except
            on EConvertError do
              raise EImportError.Create(S_INSTA_ERR_JSON_COMMENTS_2);
          end;
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_ID, sftString) then
        begin
          CommentItem.SiteCommentBlock[tstInstagram].CommentId :=
            (Node as TJSONSimpleValue).StrData;
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_OWNER, sftObject, false) then
        begin
          if FJNav.NavToMemberValue(Node, S_IFIELD_ID, sftString) then
          begin
            CommentItem.SiteUserBlock[tstInstagram].UserId := (Node as TJSONSimpleValue).StrData;
            FJNav.Pop(Node);
          end;
          if FJNav.NavToMemberValue(Node, S_IFIELD_PROF_PIC_URL, sftString) then
          begin
            CommentItem.SiteUserBlock[tstInstagram].ProfilePicUrl :=
              (Node as TJSONSimpleValue).StrData;
            FJNav.Pop(Node);
          end;
          if FJNav.NavToMemberValue(Node, S_IFIELD_USERNAME, sftString) then
          begin
            CommentItem.SiteUserBlock[tstInstagram].Username := (Node as TJSONSimpleValue).StrData;
            FJNav.Pop(Node);
          end;
        end;
        FJNav.Pop(Node);
      end;

      // If comments older than bailout ID, then'd don't retrieve any more.
      if (FCommentBailoutTime <> 0) and (CommentItem.Date < FCommentBailoutTime) then
      begin
        FBailout := true;
      end;

{$IFOPT C+}
      if not CommentItem.SanityCheck(tstInstagram) then
      begin
        DebugBreak();
      end;
{$ENDIF}
      if not CommentItem.SanityCheck(tstInstagram) then
        raise EImportError.Create(S_INSTA_ERR_COMMENT_ITEM_SANITY_CHECK);

      if CX.Add(CommentItem) then
        CommentItem := nil
      else
        raise EImportError.Create(S_INSTA_ERR_COMMENT_ITEM_INDEXING);
    finally
      CommentItem.Free;
    end;
    Node := (Node.SiblingListEntry.FLink.Owner) as TJSONContainer;
  end;
end;

procedure TInstaMediaImporter.UpdateCommentsAndExtentsFromJSON(Obj: TKMediaItem;
  Nodes: TJSONDocument);
var
  NewComments, CX: TKCommentList;
  Node: TJSONNode;
begin
  CX := Obj.Comments;
  Assert(Assigned(CX));
  CX.PrepareImport(tstInstagram);
  // Nodes is docUment.
  NewComments := TKCommentList.Create;
  NewComments.PrepareImport(tstInstagram);
  try
    Node := CheckAssigned(Nodes.ContainedListHead.FLink.Owner) as TJSONContainer;
    if (Node as TJSONContainer).ContainerType <> jctObject then
      raise EImportError.Create(S_INSTA_ERR_COMMENTS_EXTENTS_JSON);

    // Node is outer container obj.
    if FJNav.NavToMemberValue(Node, S_JSON_STATUS, sftString) then
    begin
      if (Node as TJSONSimpleValue).StrData <> S_JSON_OK then
        raise EImportError.Create(S_INSTA_ERR_COMMENTS_EXTENTS_JSON_2);
      FJNav.Pop(Node);
    end;
    if FJNav.NavToMemberValue(Node, S_JSON_DATA, sftObject, false) and
      FJNav.NavToMemberValue(Node, S_IFIELD_SHORTCODE_MEDIA, sftObject, false) and
      FJNav.NavToMemberValue(Node, S_IFIELD_EDGE_MEDIA_TO_COMMENT, sftObject, false) then
    begin
      if FJNav.NavToMemberValue(Node, S_IFIELD_PAGE_INFO, sftObject) then
      begin
        if FJNav.NavToMemberValue(Node, S_IFIELD_HAS_PREVIOUS, sftBoolean) then
        begin
          (NewComments.Extents as TKItemExtents).HasLater :=
            ((Node as TJSONSimpleValue).ValType = svtTrue);
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_HAS_NEXT, sftBoolean) then
        begin
          (NewComments.Extents as TKItemExtents).HasEarlier :=
            ((Node as TJSONSimpleValue).ValType = svtTrue);
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_START_CURSOR, sftString) then
        begin
          (NewComments.Extents as TKItemExtents).LastId := (Node as TJSONSimpleValue).StrData;
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_END_CURSOR, sftString) then
        begin
          (NewComments.Extents as TKItemExtents).FirstId := (Node as TJSONSimpleValue).StrData;
          FJNav.Pop(Node);
        end;
        FJNav.Pop(Node);
      end;
      if FJNav.NavToMemberValue(Node, S_IFIELD_EDGES, sftArray) then
      begin
        UpdateCommentItemsFromJSON(NewComments, Obj, Node as TJSONContainer);
        FJNav.Pop(Node);
      end;
    end;
    if not CX.MergeWithNewer(NewComments) then
      raise EImportError.Create(S_INSTA_ERR_COMMENTS_EXTENTS_JSON_3);
  finally
    NewComments.Free;
  end;
end;

function TInstaMediaImporter.SupplementaryQueriesFinished(FPResult: TFetchParseResult): boolean;
var
  OldestId: string;
  NewestId: string;
  QueryParams: TStringList;
  QueryValues: TStringList;
  QueryString, VariableString: string;
  QueryOpts: TFetcherOpts;
  ItemExtents: TKItemExtents;

begin
  result := true;
  if not Assigned(FMediaItem) then
    raise EImportError.Create(S_INSTA_ERR_MEDIA_SUPP_QUERIES);

  if FBailout or FInitialFetchOnly then
    exit;

  ItemExtents := (FMediaItem.Comments.Extents as TKItemExtents);
  // Check media list is accurate reflection on what's actually there.
  OldestId := ItemExtents.FirstId;
  NewestId := ItemExtents.LastId;

  if ItemExtents.HasEarlier or ItemExtents.HasLater then
  begin
    QueryParams := TStringList.Create;
    QueryValues := TStringList.Create;
    try
      QueryParams.Add(S_INSTA_Q_QUERY_HASH);
      QueryValues.Add(S_INSTA_Q_QUERY_HASH_COMMENTS);

      QueryParams.Add(S_INSTA_Q_COMMON_VARS);

      QueryString := S_INSTA_Q_COMMENTS_SHORTCODE + FMediaItem.SiteMediaBlock[tstInstagram]
        .MediaCode + S_INSTA_Q_COMMENTS_FIRST + IntToStr(FetchItemCount);

      if ItemExtents.HasEarlier then
        QueryString := QueryString + S_INSTA_Q_COMMENTS_AFTER
      else
        QueryString := QueryString + S_INSTA_Q_COMMENTS_BEFORE;

      if ItemExtents.HasEarlier then
      begin
        if not(Length(OldestId) > 0) then
          raise EImportError.Create(S_INSTA_ERR_MEDIA_SUPP_QUERIES_2);
        QueryString := QueryString + OldestId;
        ItemExtents.LinkId := OldestId;
      end
      else
      begin
        if not(Length(NewestId) > 0) then
          raise EImportError.Create(S_INSTA_ERR_MEDIA_SUPP_QUERIES_3);
        QueryString := QueryString + NewestId;
        ItemExtents.LinkId := NewestId;
      end;

      QueryString := QueryString + S_INSTA_Q_COMMENTS_END;
      QueryValues.Add(QueryString);
      VariableString := QueryString;

      QueryString := BuildURL(S_SSL_PROTOCOL, S_INSTAGRAM_SITE_NAME, S_INSTAGRAM_QUERY_URI, true,
        QueryParams, QueryValues);

      QueryOpts := GenNextOpts(FPResult.FetcherOpts, ThrottleDelay, FPResult.Url);
      SetXInstagramGISHdr(QueryOpts, FRhxGis, VariableString);
      if AttemptNextFetchParseRequest then
      begin
        result := false;
        if not GFetcherParser.AddDocToFetchParse(QueryString, nil, fpmGet, QueryOpts, self, nil)
        then
        begin
          FinishedFetchParseRequest;
          QueryOpts.Free;
          FErrInfo.FetchOK := false;
          result := true;
        end
      end
      else
      begin
        QueryOpts.Free;
        FErrInfo.FetchOK := false;
      end;
    finally
      QueryParams.Free;
      QueryValues.Free;
    end;
  end;
end;

procedure TInstaMediaImporter.PopulateCommentItemsFromJS(Obj: TKMediaItem; NodeArray: TJSExprList);
var
  CX: TKCommentList;
  CommentItem: TKCommentItem;
  Node: TJSNode;
begin
  Assert(Assigned(Obj));
  CX := Obj.Comments;
  Assert(Assigned(CX));
  CX.PrepareImport(tstInstagram);
  Assert(Assigned(NodeArray));

  Node := (NodeArray.ContainedListHead.FLink.Owner) as TJSSimpleExpr;
  while Assigned(Node) do
  begin
    if (Node as TJSSimpleExpr).SimpleExprType <> setObjLit then
      raise EImportError.Create(S_INSTA_ERR_COMMENTS_FROM_JS_1);

    CommentItem := TKCommentItem.Create;
    try
      CommentItem.SiteUserBlock[tstInstagram].Valid := true;
      CommentItem.SiteCommentBlock[tstInstagram].Valid := true;
      CommentItem.LastUpdated := Now;

      if FNav.NavToFieldList(Node) then
      begin
        if FNav.NavChildToChildFieldList(Node, S_IFIELD_NODE, false) then
        begin
          if FNav.NavChildToFieldValue(Node, S_IFIELD_TEXT, setStringLiteral) then
          begin
            CommentItem.CommentData := (Node as TJSString).StrData;
            CommentItem.CommentType := citPlainText;
            FNav.Pop(Node);
          end;
          if FNav.NavChildToFieldValue(Node, S_IFIELD_CREATED_AT, setNumber) then
          begin
            if ((Node as TJSNumber).Int64Valid) then
              CommentItem.Date := UnixDateTimeToDelphiDateTime((Node as TJSNumber).Int64Value)
            else if (Node as TJSNumber).FloatValid then
              CommentItem.Date := UnixDateTimeToDelphiDateTime
                (Trunc((Node as TJSNumber).FloatValue))
            else
              raise EImportError.Create(S_INSTA_ERR_COMMENTS_FROM_JS_2);
            FNav.Pop(Node);
          end;
          if FNav.NavChildToFieldValue(Node, S_IFIELD_ID, setStringLiteral) then
          begin
            CommentItem.SiteCommentBlock[tstInstagram].CommentId := (Node as TJSString).StrData;
            FNav.Pop(Node);
          end;
          if FNav.NavChildToChildFieldList(Node, S_IFIELD_OWNER) then
          begin
            if FNav.NavChildToFieldValue(Node, S_IFIELD_ID, setStringLiteral) then
            begin
              CommentItem.SiteUserBlock[tstInstagram].UserId := (Node as TJSString).StrData;
              FNav.Pop(Node);
            end;
            if FNav.NavChildToFieldValue(Node, S_IFIELD_PROF_PIC_URL) then
            begin
              CommentItem.SiteUserBlock[tstInstagram].ProfilePicUrl := (Node as TJSString).StrData;
              FNav.Pop(Node);
            end;
            if FNav.NavChildToFieldValue(Node, S_IFIELD_USERNAME) then
            begin
              CommentItem.SiteUserBlock[tstInstagram].Username := (Node as TJSString).StrData;
              FNav.Pop(Node);
            end;
            FNav.Pop(Node);
          end;
        end
        else
          raise EImportError.Create(S_INSTA_ERR_COMMENTS_FROM_JS_3);
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(S_INSTA_ERR_COMMENTS_FROM_JS_4);

      // If comments older than bailout ID, then'd don't retrieve any more.
      if (FCommentBailoutTime <> 0) and (CommentItem.Date < FCommentBailoutTime) then
      begin
        FBailout := true;
      end;

{$IFOPT C+}
      if not CommentItem.SanityCheck(tstInstagram) then
      begin
        DebugBreak();
      end;
{$ENDIF}
      if not CommentItem.SanityCheck(tstInstagram) then
        raise EImportError.Create(S_INSTA_ERR_COMMENT_ITEM_SANITY_CHECK_2);

      if CX.Add(CommentItem) then
        CommentItem := nil
      else
        raise EImportError.Create(S_INSTA_ERR_COMMENT_ITEM_INDEXING_2);
    finally
      CommentItem.Free;
    end;
    Node := (Node.SiblingListEntry.FLink.Owner) as TJSSimpleExpr;
  end;
end;

procedure TInstaMediaImporter.PopulateCommentsAndExtentsFromJS(Obj: TKMediaItem;
  CommentFields: TJSExprList);
var
  CX: TKCommentList;
  CF: TJSNode;
begin
  Assert(Assigned(Obj));
  Assert(Assigned(CommentFields));
  CX := Obj.Comments;
  CX.PrepareImport(tstInstagram);
  CF := CommentFields;

  if FNav.NavChildToChildFieldList(CF, S_IFIELD_PAGE_INFO) then
  begin
    if FNav.NavChildToFieldValue(CF, S_IFIELD_HAS_PREVIOUS, setTrue) then
    begin
      (CX.Extents as TKItemExtents).HasLater := ((CF as TJSSimpleExpr).SimpleExprType = setTrue);
      FNav.Pop(CF);
    end;
    if FNav.NavChildToFieldValue(CF, S_IFIELD_HAS_NEXT, setTrue) then
    begin
      (CX.Extents as TKItemExtents).HasEarlier := ((CF as TJSSimpleExpr).SimpleExprType = setTrue);
      FNav.Pop(CF);
    end;
    if FNav.NavChildToFieldValue(CF, S_IFIELD_START_CURSOR, setStringLiteral) then
    begin
      (CX.Extents as TKItemExtents).LastId := ((CF as TJSString) as TJSString).StrData;
      FNav.Pop(CF);
    end;
    if FNav.NavChildToFieldValue(CF, S_IFIELD_END_CURSOR, setStringLiteral) then
    begin
      (CX.Extents as TKItemExtents).FirstId := ((CF as TJSString) as TJSString).StrData;
      FNav.Pop(CF);
    end;
    FNav.Pop(CF);
  end;
  if FNav.NavChildToFieldValue(CF, S_IFIELD_EDGES, setArrayLit) then
  begin
    if FNav.NavToArrayElems(CF, false) then
      PopulateCommentItemsFromJS(Obj, CF as TJSExprList);
    FNav.Pop(CF);
  end;
end;

procedure TInstaMediaImporter.PopulateMediaObjFromJS(Obj: TKMediaItem; MediaFields: TJSExprList);
var
  MF: TJSNode;
begin
  Assert(MediaFields.ListType = eltObjectFields);
  MF := MediaFields;

  if FNav.NavChildToFieldValue(MF, S_IFIELD_SHORTCODE, setStringLiteral) then
  begin
    Obj.SiteMediaBlock[tstInstagram].MediaCode := (MF as TJSString).StrData;
    FNav.Pop(MF);
  end;
  if FNav.NavChildToFieldValue(MF, S_IFIELD_TAKEN_AT_TIMESTAMP, setNumber) then
  begin
    if not((MF as TJSNumber).Int64Valid) then
      raise EImportError.Create(S_INSTA_ERR_MEDIA_FROM_JS);
    Obj.Date := UnixDateTimeToDelphiDateTime((MF as TJSNumber).Int64Value);
    FNav.Pop(MF);
  end;
  if FNav.NavChildToFieldValue(MF, S_IFIELD_ID, setStringLiteral) then
  begin
    Obj.SiteMediaBlock[tstInstagram].MediaID := (MF as TJSString).StrData;
    FNav.Pop(MF);
  end;
  if FNav.NavChildToFieldValue(MF, S_IFIELD_DISPLAY_URL, setStringLiteral) then
  begin
    Obj.ResourceURL := (MF as TJSString).StrData;
    FNav.Pop(MF);
  end;
  if FNav.NavChildToChildFieldList(MF, S_IFIELD_EDGE_MEDIA_TO_CAPTION) then
  begin
    if FNav.NavChildToFieldValue(MF, S_IFIELD_EDGES, setArrayLit, false) and
      FNav.NavToArrayElems(MF, false) then
    begin
      if not DlItemIsEmpty(@MF.ContainedListHead) then
      begin
        // Take first item.
        MF := MF.ContainedListHead.FLink.Owner as TJSNode;
        if FNav.NavToFieldList(MF, false) and FNav.NavChildToChildFieldList(MF, S_IFIELD_NODE,
          false) and FNav.NavChildToFieldValue(MF, S_IFIELD_TEXT, setStringLiteral, false) then
        begin
          Obj.MediaData := (MF as TJSString).StrData;
          Obj.MediaType := mitPlainText;
        end;
      end;
    end;
    FNav.Pop(MF);
  end;
  if FNav.NavChildToChildFieldList(MF, S_IFIELD_OWNER) then
  begin
    if FNav.NavChildToFieldValue(MF, S_IFIELD_ID, setStringLiteral, false) then
      Obj.SiteMediaBlock[tstInstagram].OwnerID := (MF as TJSString).StrData;
    FNav.Pop(MF);
  end;
  if FNav.NavChildToChildFieldList(MF, S_IFIELD_EDGE_MEDIA_TO_PARENT_COMMENT) then
  begin
    PopulateCommentsAndExtentsFromJS(Obj, MF as TJSExprList);
    FNav.Pop(MF);
  end;
end;

function TInstaMediaImporter.MakeMediaItemFromMediaPage(Doc: THTMLNode): TKMediaItem;
var
  DataNode: TJSNode;
  MI: TKMediaItem;
begin
  Assert(Assigned(Doc));
  result := nil;
  DataNode := FindLabelledPage(TJSNode(Doc), FNav, FRhxGis, 'PostPage');
  if not Assigned(DataNode) then
    raise EImportError.Create(S_INSTA_ERR_MEDIA_PAGE_NO_POSTPAGE);

  if FNav.NavChildToChildFieldList(DataNode, 'graphql', false) then
  begin
    if not FNav.NavChildToChildFieldList(DataNode, 'shortcode_media', false) then
      DataNode := nil;
  end
  else
    DataNode := nil;

  if not Assigned(DataNode) then
    raise EImportError.Create(S_INSTA_ERR_MEDIA_PAGE_NO_GRAPHQL);

  // OK, we now have the list of various fields in the object.
  MI := TKMediaItem.Create;
  try
    MI.LastUpdated := Now;
    MI.SiteMediaBlock[tstInstagram].Valid := true;
    PopulateMediaObjFromJS(MI, DataNode as TJSExprList);
{$IFOPT C+}
    if not MI.SanityCheck(tstInstagram) then
    begin
      DebugBreak(); // TODO - Very rarely, docs do not have an owner ID.
    end;
{$ENDIF}
    if not MI.SanityCheck(tstInstagram) then
      raise EImportError.Create(S_INSTA_ERR_MEDIA_ITEM_SANITY_CHECK);
    // TODO fix up any additional info from HTML meta tags.
    result := MI;
    MI := nil;
  finally
    MI.Free;
  end;
end;

// This function assumes that exceptions are raised before we get to the
// point where we would start additional fetch parses.
function TInstaMediaImporter.ProcessParseTree(WorkItem: TImporterWorkItem;
  var ErrMsg: string): boolean;
var
  FPResult: TFetchParseResult;
begin
  result := false;
  try
    FPResult := WorkItem.FPResult;
    Assert(Assigned(FPResult));
    result := true;
    try
      if (FPResult.ParseResult).NodeType = NodeTypeHTML then
      begin
        FMediaItem.Free;
        FMediaItem := MakeMediaItemFromMediaPage(FPResult.ParseResult as THTMLDocument);
        if SupplementaryQueriesFinished(FPResult) then
          FinishedProcessing(WorkItem);
      end
      else if (FPResult.ParseResult.NodeType) = NodeTypeJSON then
      begin
        UpdateCommentsAndExtentsFromJSON(FMediaItem, (FPResult.ParseResult as TJSONDocument));
        if SupplementaryQueriesFinished(FPResult) then
          FinishedProcessing(WorkItem);
      end
      else
        Assert(false);
    except
      on E: Exception do
      begin
        result := false;
        ErrMsg := E.ClassName + ':' + E.Message;
      end;
    end;
  finally
    try
      FNav.StackEmpty(result);
    except
      on EAssertionFailed do;
    end;
  end;
end;

function TInstaMediaImporter.RequestMedia(Block: TKSiteMediaBlock; Options: TImportOptions)
  : boolean;
var
  FetchUrl: string;
  QueryOpts: TFetcherOpts;
begin
  result := false;
  Assert(Block.Valid);
  if AttemptStartProcessing then
  begin
    FErrInfo.Reset;
    if AttemptNextFetchParseRequest then
    begin
      FMediaItem.Free;
      FMediaItem := nil;
      FBailout := false;
      if Assigned(Options) then
      begin
        FCommentBailoutTime := Options.BailoutTime;
        FInitialFetchOnly := Options.InitialFetchOnly;
      end
      else
      begin
        FCommentBailoutTime := 0;
        FInitialFetchOnly := false;
      end;

      FetchUrl := BuildURL(S_SSL_PROTOCOL, S_INSTAGRAM_SITE_NAME,
        S_INSTA_MEDIA_URL_PREFIX + Block.MediaCode + S_SLASH, false, nil, nil);
      QueryOpts := GenFirstOpts(ThrottleDelay);
      result := GFetcherParser.AddDocToFetchParse(FetchUrl, nil, fpmGet, QueryOpts, self, nil);
      if not result then
      begin
        QueryOpts.Free;
        FinishedFetchParseRequest;
      end;
    end;
    if not result then
    begin
      FErrInfo.FetchOK := false;
      FErrInfo.ParseOK := false;
      FErrInfo.ErrMsg := S_REQUEST_FAILED;
      FinishedProcessing(nil);
    end;
  end;
end;

function TInstaMediaImporter.RetrieveResult(var Media: TKMediaItem;
  var ErrInfo: TImportErrInfo): boolean;
begin
  Media := nil;
  ErrInfo := nil;
  result := LockCheckIdle;
  try
    if result then
    begin
      ErrInfo := TImportErrInfo.Create;
      ErrInfo.Assign(FErrInfo);
      Media := FMediaItem;
      FErrInfo.Reset;
      FMediaItem := nil;
    end;
  finally
    UnlockFromIdle;
  end;
end;

{ TInstaUserProfileImporter }

constructor TInstaUserProfileImporter.Create;
begin
  inherited;
  FNav := TJSNavHelper.Create;
  FJNav := TJSONNavHelper.Create;
end;

destructor TInstaUserProfileImporter.Destroy;
begin
  FUserProfile.Free;
  FUserProfile := nil;
  FNav.Free;
  FJNav.Free;
  inherited;
end;

procedure TInstaUserProfileImporter.UpdateMediaItemsFromJSON(MX: TKMediaList; Obj: TKUserProfile;
  Nodes: TJSONContainer);
var
  Node: TJSONNode;
  MediaItem: TKMediaItem;
begin
  // Nodes is outermost array.
  Assert(Assigned(Nodes));
  Node := (Nodes.ContainedListHead.FLink.Owner) as TJSONNode;
  while Assigned(Node) do
  begin
    // Node is array item.
    if ((Node as TJSONContainer).ContainerType <> jctObject) then
      raise EImportError.Create(S_INSTA_ERR_UPDATE_MEDIA_FROM_JSON);
    MediaItem := TKMediaItem.Create;
    try
      MediaItem.LastUpdated := Now;
      MediaItem.SiteMediaBlock[tstInstagram].Valid := true;

      if FJNav.NavToMemberValue(Node, S_IFIELD_NODE, sftObject) then
      begin
        if FJNav.NavToMemberValue(Node, S_IFIELD_SHORTCODE, sftString) then
        begin
          MediaItem.SiteMediaBlock[tstInstagram].MediaCode := (Node as TJSONSimpleValue).StrData;
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_TAKEN_AT_TIMESTAMP, sftNumber) then
        begin
          MediaItem.Date := UnixDateTimeToDelphiDateTime
            (StrToInt64((Node as TJSONSimpleValue).StrData));
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_ID, sftString) then
        begin
          MediaItem.SiteMediaBlock[tstInstagram].MediaID := (Node as TJSONSimpleValue).StrData;
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_THUMBNAIL_SRC, sftString) then
        begin
          MediaItem.ResourceURL := (Node as TJSONSimpleValue).StrData;
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_EDGE_MEDIA_TO_CAPTION, sftObject) then
        begin
          if FJNav.NavToMemberValue(Node, S_IFIELD_EDGES, sftArray) then
          begin
            // Take the first item in the array.
            if not DlItemIsEmpty(@Node.ContainedListHead) then
            begin
              Node := Node.ContainedListHead.FLink.Owner as TJSONContainer;
              if FJNav.NavToMemberValue(Node, S_IFIELD_NODE, sftObject, false) and
                FJNav.NavToMemberValue(Node, S_IFIELD_TEXT, sftString, false) then
              begin
                MediaItem.MediaData := (Node as TJSONSimpleValue).StrData;
                MediaItem.MediaType := mitPlainText;
              end;
            end;
            FJNav.Pop(Node);
          end;
          FJNav.Pop(Node);
        end;
        if FJNav.NavToMemberValue(Node, S_IFIELD_OWNER, sftObject, false) then
        begin
          if FJNav.NavToMemberValue(Node, S_IFIELD_ID, sftString) then
          begin
            MediaItem.SiteMediaBlock[tstInstagram].OwnerID := (Node as TJSONSimpleValue).StrData;
            FJNav.Pop(Node);
          end;
        end;
        FJNav.Pop(Node);
      end
      else
        raise EImportError.Create(S_INSTA_ERR_UPDATE_MEDIA_FROM_JSON_2);

      // If media older than bailout ID, then'd don't retrieve any more.
      if (FMediaBailoutTime <> 0) and (MediaItem.Date < FMediaBailoutTime) then
      begin
        FBailout := true;
      end;

      // TODO - Check this is consistent with workaround below.
      // requires further investigation.
{$IFOPT C+}
      if not MediaItem.SanityCheck(tstInstagram) then
      begin
        DebugBreak();
      end;
{$ENDIF}
      if not MediaItem.SanityCheck(tstInstagram) then
        raise EImportError.Create(S_INSTA_ERR_MEDIA_ITEM_SANITY_CHECK_2);

      // Silently bin items where the owner field is not right.
      if not((Length(MediaItem.SiteMediaBlock[tstInstagram].OwnerID) > 0) and
        (CompareDecimalKeyStr(MediaItem.SiteMediaBlock[tstInstagram].OwnerID,
        Obj.SiteUserBlock[tstInstagram].UserId) <> 0)) then
      begin
        if MX.Add(MediaItem) then
          MediaItem := nil
        else
          raise EImportError.Create(S_INSTA_ERR_MEDIA_ITEM_INDEXING);
      end;
      // Else just skip the item.
    finally
      MediaItem.Free;
    end;
    Node := (Node.SiblingListEntry.FLink.Owner) as TJSONContainer;
  end;
end;

procedure TInstaUserProfileImporter.UpdateMediaAndExtentsFromJSON(Obj: TKUserProfile;
  Nodes: TJSONDocument);
var
  NewMedia, MX: TKMediaList;
  JSONNode: TJSONNode;
  DBGJSONVal: TJSONSimpleValue;
begin
  MX := Obj.Media;
  Assert(Assigned(MX));
  MX.PrepareImport(tstInstagram);

  NewMedia := TKMediaList.Create;
  NewMedia.PrepareImport(tstInstagram);
  try
    // Document to Outermost JSON object.
    JSONNode := CheckAssigned(Nodes.ContainedListHead.FLink.Owner) as TJSONContainer;
    if (JSONNode as TJSONContainer).ContainerType <> jctObject then
      raise EImportError.Create(S_INSTA_ERR_MEDIA_AND_EXTENTS_FROM_JSON);

    if FJNav.NavToMemberValue(JSONNode, S_JSON_STATUS, sftString) then
    begin
      DBGJSONVal := (JSONNode as TJSONSimpleValue);
      if DBGJSONVal.StrData <> S_JSON_OK then
        raise EImportError.Create(S_INSTA_ERR_MEDIA_AND_EXTENTS_FROM_JSON_2);
      FJNav.Pop(JSONNode);
    end;
    if FJNav.NavToMemberValue(JSONNode, S_JSON_DATA, sftObject, false) and
      FJNav.NavToMemberValue(JSONNode, S_IFIELD_USER, sftObject, false) and
      FJNav.NavToMemberValue(JSONNode, S_IFIELD_EDGE_OWNER_TO_TIMELINE_MEDIA, sftObject, false) then
    begin
      if FJNav.NavToMemberValue(JSONNode, S_IFIELD_PAGE_INFO, sftObject) then
      begin
        if FJNav.NavToMemberValue(JSONNode, S_IFIELD_HAS_PREVIOUS, sftBoolean) then
        begin
          (NewMedia.Extents as TKItemExtents).HasLater :=
            ((JSONNode as TJSONSimpleValue).ValType = svtTrue);
          FJNav.Pop(JSONNode);
        end;
        if FJNav.NavToMemberValue(JSONNode, S_IFIELD_HAS_NEXT, sftBoolean) then
        begin
          (NewMedia.Extents as TKItemExtents).HasEarlier :=
            ((JSONNode as TJSONSimpleValue).ValType = svtTrue);
          FJNav.Pop(JSONNode);
        end;
        if FJNav.NavToMemberValue(JSONNode, S_IFIELD_START_CURSOR, sftString) then
        begin
          (NewMedia.Extents as TKItemExtents).LastId := (JSONNode as TJSONSimpleValue).StrData;
          FJNav.Pop(JSONNode);
        end;
        if FJNav.NavToMemberValue(JSONNode, S_IFIELD_END_CURSOR, sftString) then
        begin
          (NewMedia.Extents as TKItemExtents).FirstId := (JSONNode as TJSONSimpleValue).StrData;
          FJNav.Pop(JSONNode);
        end;
        FJNav.Pop(JSONNode);
      end;
      if FJNav.NavToMemberValue(JSONNode, S_IFIELD_EDGES, sftArray) then
      begin
        UpdateMediaItemsFromJSON(NewMedia, Obj, JSONNode as TJSONContainer);
        FJNav.Pop(JSONNode);
      end;
    end
    else
      raise EImportError.Create(S_INSTA_ERR_MEDIA_AND_EXTENTS_FROM_JSON_3);

    if not MX.MergeWithNewer(NewMedia) then
      raise EImportError.Create(S_INSTA_ERR_MEDIA_ITEM_INDEXING_2);
  finally
    NewMedia.Free;
  end;
end;

procedure TInstaUserProfileImporter.PopulateMediaItemsFromJS(Obj: TKUserProfile;
  NodeArray: TJSExprList);
var
  MX: TKMediaList;
  MediaItem: TKMediaItem;
  Node: TJSNode;
begin
  Assert(Assigned(Obj));
  MX := Obj.Media;
  Assert(Assigned(MX));
  MX.PrepareImport(tstInstagram);
  Assert(Assigned(NodeArray));
  Node := (NodeArray.ContainedListHead.FLink.Owner as TJSNode);
  while Assigned(Node) do
  begin
    if (Node as TJSSimpleExpr).SimpleExprType <> setObjLit then
      raise EImportError.Create(S_INSTA_ERR_MEDIA_ITEMS_FROM_JS);

    MediaItem := TKMediaItem.Create;
    try
      MediaItem.LastUpdated := Now;
      MediaItem.SiteMediaBlock[tstInstagram].Valid := true;

      if FNav.NavToFieldList(Node) then
      begin
        if FNav.NavChildToChildFieldList(Node, S_IFIELD_NODE, false) then
        begin
          if FNav.NavChildToFieldValue(Node, S_IFIELD_SHORTCODE, setStringLiteral) then
          begin
            MediaItem.SiteMediaBlock[tstInstagram].MediaCode := (Node as TJSString).StrData;
            FNav.Pop(Node);
          end;
          if FNav.NavChildToFieldValue(Node, S_IFIELD_TAKEN_AT_TIMESTAMP, setNumber) then
          begin
            // List of comments not available in top level profile view.
            if not((Node as TJSNumber).Int64Valid) then
              raise EImportError.Create(S_INSTA_ERR_MEDIA_ITEMS_FROM_JS_2);
            MediaItem.Date := UnixDateTimeToDelphiDateTime((Node as TJSNumber).Int64Value);
            FNav.Pop(Node);
          end;
          if FNav.NavChildToFieldValue(Node, S_IFIELD_ID, setStringLiteral) then
          begin
            MediaItem.SiteMediaBlock[tstInstagram].MediaID := (Node as TJSString).StrData;
            FNav.Pop(Node);
          end;
          if FNav.NavChildToFieldValue(Node, S_IFIELD_THUMBNAIL_SRC, setStringLiteral) then
          begin
            MediaItem.ResourceURL := (Node as TJSString).StrData;
            FNav.Pop(Node);
          end;
          if FNav.NavChildToChildFieldList(Node, S_IFIELD_EDGE_MEDIA_TO_CAPTION) then
          begin
            if FNav.NavChildToFieldValue(Node, S_IFIELD_EDGES, setArrayLit, false) and
              FNav.NavToArrayElems(Node, false) then
            begin
              // Take the first one.
              if not DlItemIsEmpty(@Node.ContainedListHead) then
              begin
                Node := Node.ContainedListHead.FLink.Owner as TJSNode;
                // Should be an object literal.
                if FNav.NavToFieldList(Node, false) and FNav.NavChildToChildFieldList(Node,
                  S_IFIELD_NODE, false) and FNav.NavChildToFieldValue(Node, S_IFIELD_TEXT,
                  setStringLiteral, false) then
                begin
                  MediaItem.MediaData := (Node as TJSString).StrData;
                  MediaItem.MediaType := mitPlainText;
                end;
              end;
            end;
            FNav.Pop(Node);
          end;
          if FNav.NavChildToChildFieldList(Node, S_IFIELD_OWNER) then
          begin
            if FNav.NavChildToFieldValue(Node, S_IFIELD_ID, setStringLiteral, false) then
              MediaItem.SiteMediaBlock[tstInstagram].OwnerID := (Node as TJSString).StrData;
            FNav.Pop(Node);
          end;
        end
        else
          raise EImportError.Create(S_INSTA_ERR_MEDIA_ITEMS_FROM_JS_3);
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(S_INSTA_ERR_MEDIA_ITEMS_FROM_JS_4);

      // If media older than bailout ID, then'd don't retrieve any more.
      if (FMediaBailoutTime <> 0) and (MediaItem.Date < FMediaBailoutTime) then
      begin
        FBailout := true;
      end;
{$IFOPT C+}
      if not MediaItem.SanityCheck(tstInstagram) then
      begin
        DebugBreak();
      end;
{$ENDIF}
      if not MediaItem.SanityCheck(tstInstagram) then
        raise EImportError.Create(S_INSTA_ERR_MEDIA_ITEM_SANITY_CHECK_3);

      // Silently bin items where the owner field is not right.
      if not((Length(MediaItem.SiteMediaBlock[tstInstagram].OwnerID) > 0) and
        (CompareDecimalKeyStr(MediaItem.SiteMediaBlock[tstInstagram].OwnerID,
        Obj.SiteUserBlock[tstInstagram].UserId) <> 0)) then
      begin
        if MX.Add(MediaItem) then
          MediaItem := nil
        else
          raise EImportError.Create(S_INSTA_ERR_MEDIA_ITEM_INDEXING_3);
      end;
    finally
      MediaItem.Free;
    end;
    Node := (Node.SiblingListEntry.FLink.Owner) as TJSSimpleExpr;
  end;
end;

procedure TInstaUserProfileImporter.PopulateMediaAndExtentsFromJS(Obj: TKUserProfile;
  MediaFields: TJSExprList);
var
  MX: TKMediaList;
  MF: TJSNode;
begin
  Assert(Assigned(Obj));
  Assert(Assigned(MediaFields));
  MX := Obj.Media;
  MX.PrepareImport(tstInstagram);
  MF := MediaFields;
  if FNav.NavChildToChildFieldList(MF, S_IFIELD_PAGE_INFO) then
  begin
    if FNav.NavChildToFieldValue(MF, S_IFIELD_HAS_PREVIOUS, setTrue) then
    begin
      (MX.Extents as TKItemExtents).HasLater := (MF as TJSSimpleExpr).SimpleExprType = setTrue;
      FNav.Pop(MF);
    end;
    if FNav.NavChildToFieldValue(MF, S_IFIELD_HAS_NEXT, setTrue) then
    begin
      (MX.Extents as TKItemExtents).HasEarlier := (MF as TJSSimpleExpr).SimpleExprType = setTrue;
      FNav.Pop(MF);
    end;
    if FNav.NavChildToFieldValue(MF, S_IFIELD_START_CURSOR, setStringLiteral) then
    begin
      (MX.Extents as TKItemExtents).LastId := (MF as TJSString).StrData;
      FNav.Pop(MF);
    end;
    if FNav.NavChildToFieldValue(MF, S_IFIELD_END_CURSOR, setStringLiteral) then
    begin
      (MX.Extents as TKItemExtents).FirstId := (MF as TJSString).StrData;
      FNav.Pop(MF);
    end;
    FNav.Pop(MF);
  end;
  if FNav.NavChildToFieldValue(MF, S_IFIELD_EDGES, setArrayLit) then
  begin
    if FNav.NavToArrayElems(MF, false) then
      PopulateMediaItemsFromJS(Obj, MF as TJSExprList);
    FNav.Pop(MF);
  end;
end;

procedure TInstaUserProfileImporter.PopulateUserObjFromJS(Obj: TKUserProfile;
  UserFields: TJSExprList);
var
  UserBlock: TKSiteUserBlock;
  FollowCountSet, FollowerCountSet: boolean;
  UF: TJSNode;
begin
  UF := UserFields;
  UserBlock := Obj.SiteUserBlock[tstInstagram];
  if FNav.NavChildToFieldValue(UF, S_IFIELD_USERNAME, setStringLiteral) then
  begin
    UserBlock.Username := (UF as TJSString).StrData;
    FNav.Pop(UF);
  end;
  FollowCountSet := false;
  FollowerCountSet := false;
  if FNav.NavChildToChildFieldList(UF, S_IFIELD_FOLLOWS_COUNT) then
  begin
    if FNav.NavChildToFieldValue(UF, S_IFIELD_COUNT, setNumber, false) then
    begin
      UserBlock.FollowsCount := (UF as TJSNumber).IntValue;
      FollowCountSet := true;
    end;
    FNav.Pop(UF);
  end;
  if FNav.NavChildToChildFieldList(UF, S_IFIELD_FOLLOWER_COUNT) then
  begin
    if FNav.NavChildToFieldValue(UF, S_IFIELD_COUNT, setNumber, false) then
    begin
      UserBlock.FollowerCount := (UF as TJSNumber).IntValue;
      FollowerCountSet := true;
    end;
    FNav.Pop(UF);
  end;
  if not(FollowCountSet and FollowerCountSet) then
    raise EImportError.Create(S_INSTA_ERR_POPULATE_USER_OBJ_FROM_JS);

  if FNav.NavChildToFieldValue(UF, S_IFIELD_PROF_PIC_URL, setStringLiteral) then
  begin
    UserBlock.ProfilePicUrl := (UF as TJSString).StrData;
    FNav.Pop(UF);
  end
  else if FNav.NavChildToFieldValue(UF, S_IFIELD_PROF_PIC_URL, setNull) then
  begin
    UserBlock.ProfilePicUrl := '';
    FNav.Pop(UF);
  end
  else
    raise EImportError.Create(S_INSTA_ERR_POPULATE_USER_OBJ_FROM_JS_2);

  if FNav.NavChildToFieldValue(UF, S_IFIELD_ID, setStringLiteral) then
  begin
    UserBlock.UserId := (UF as TJSString).StrData;
    FNav.Pop(UF);
  end;

  if FNav.NavChildToFieldValue(UF, S_IFIELD_USERBIO, setStringLiteral) then
  begin
    UserBlock.Bio := (UF as TJSString).StrData;
    FNav.Pop(UF);
  end
  else if FNav.NavChildToFieldValue(UF, S_IFIELD_USERBIO, setNull) then
  begin
    UserBlock.Bio := '';
    FNav.Pop(UF);
  end
  else
    raise EImportError.Create(S_INSTA_ERR_POPULATE_USER_OBJ_FROM_JS_3);

  if FNav.NavChildToFieldValue(UF, S_IFIELD_FULLNAME, setStringLiteral) then
  begin
    UserBlock.FullName := (UF as TJSString).StrData;
    FNav.Pop(UF);
  end
  else if FNav.NavChildToFieldValue(UF, S_IFIELD_FULLNAME, setNull) then
  begin
    UserBlock.FullName := '';
    FNav.Pop(UF);
  end
  else
    raise EImportError.Create(S_INSTA_ERR_POPULATE_USER_OBJ_FROM_JS_4);

  if FNav.NavChildToChildFieldList(UF, S_IFIELD_EDGE_OWNER_TO_TIMELINE_MEDIA) then
  begin
    PopulateMediaAndExtentsFromJS(Obj, UF as TJSExprList);
    FNav.Pop(UF);
  end;

  if FNav.NavChildToFieldValue(UF, S_IFIELD_VERIFIED, setTrue) then
  begin
    UserBlock.Verified := ((UF as TJSSimpleExpr).SimpleExprType = setTrue);
    FNav.Pop(UF);
  end;
end;

function TInstaUserProfileImporter.MakeUserObjFromProfilePage(Doc: THTMLNode): TKUserProfile;
var
  DataNode: TJSNode;
  UP: TKUserProfile;

begin
  Assert(Assigned(Doc));
  result := nil;
  DataNode := FindLabelledPage(TJSNode(Doc), FNav, FRhxGis, 'ProfilePage');
  if not Assigned(DataNode) then
    raise EImportError.Create(S_INSTA_ERR_USER_OBJ_NO_PROFILE_PAGE);

  if FNav.NavChildToChildFieldList(DataNode, 'graphql', false) then
  begin
    if not FNav.NavChildToChildFieldList(DataNode, 'user', false) then
      DataNode := nil;
  end
  else
    DataNode := nil;

  if not Assigned(DataNode) then
    raise EImportError.Create(S_INSTA_ERR_USER_OBJ_NO_GRAPHQL);

  // OK, we now have the list of various fields in the object.
  UP := TKUserProfile.Create;
  try
    UP.LastUpdated := Now;
    UP.SiteUserBlock[tstInstagram].Valid := true;
    PopulateUserObjFromJS(UP, DataNode as TJSExprList);
{$IFOPT C+}
    if not UP.SanityCheck(tstInstagram) then
    begin
      DebugBreak();
    end;
{$ENDIF}
    if not UP.SanityCheck(tstInstagram) then
      raise EImportError.Create(S_INSTA_ERR_USER_OBJ_FAILS_SANITY_CHECK);
    // TODO.
    // Fix up any additional info from HTML meta tags.
    // External URL not linkshimmed is the obvious one.
    result := UP;
    UP := nil;
  finally
    UP.Free;
  end;
end;

function TInstaUserProfileImporter.SupplementaryQueriesFinished
  (FPResult: TFetchParseResult): boolean;
var
  OldestId: string;
  NewestId: string;
  QueryParams: TStringList;
  QueryValues: TStringList;
  QueryString, VariableString: string;
  QueryOpts: TFetcherOpts;

begin
  result := true;
  if not Assigned(FUserProfile) then
    raise EImportError.Create(S_INST_ERR_USERPROF_FAIL_SUPP_QUERIES);

  if FBailout or FInitialFetchOnly then
    exit;

  // Check media list is accurate reflection on what's actually there.
  OldestId := (FUserProfile.Media.Extents as TKItemExtents).FirstId;
  NewestId := (FUserProfile.Media.Extents as TKItemExtents).LastId;

  if (FUserProfile.Media.Extents as TKItemExtents).HasEarlier or
    (FUserProfile.Media.Extents as TKItemExtents).HasLater then
  begin
    QueryParams := TStringList.Create;
    QueryValues := TStringList.Create;
    try
      QueryParams.Add(S_INSTA_Q_QUERY_HASH);
      QueryValues.Add(S_INSTA_Q_QUERY_HASH_MEDIA);

      QueryParams.Add(S_INSTA_Q_COMMON_VARS);

      QueryString := S_INSTA_Q_MEDIA_ID + FUserProfile.SiteUserBlock[tstInstagram].UserId +
        S_INSTA_Q_MEDIA_FIRST + IntToStr(FetchItemCount);

      if (FUserProfile.Media.Extents as TKItemExtents).HasEarlier then
      begin
        QueryString := QueryString + S_INSTA_Q_MEDIA_AFTER;

        if not(Length(OldestId) > 0) then
          raise EImportError.Create(S_INST_ERR_USERPROF_FAIL_SUPP_QUERIES_2);
        QueryString := QueryString + OldestId;
        (FUserProfile.Media.Extents as TKItemExtents).LinkId := OldestId;
      end
      else
      begin
        QueryString := QueryString + S_INSTA_Q_MEDIA_BEFORE;

        if not(Length(NewestId) > 0) then
          raise EImportError.Create(S_INST_ERR_USERPROF_FAIL_SUPP_QUERIES_3);
        QueryString := QueryString + NewestId;
        (FUserProfile.Media.Extents as TKItemExtents).LinkId := NewestId;
      end;
      QueryString := QueryString + S_INSTA_Q_MEDIA_END;
      QueryValues.Add(QueryString);
      VariableString := QueryString;

      QueryString := BuildURL(S_SSL_PROTOCOL, S_INSTAGRAM_SITE_NAME, S_INSTAGRAM_QUERY_URI, true,
        QueryParams, QueryValues);

      QueryOpts := GenNextOpts(FPResult.FetcherOpts, ThrottleDelay, FPResult.Url);
      SetXInstagramGISHdr(QueryOpts, FRhxGis, VariableString);

      if AttemptNextFetchParseRequest then
      begin
        result := false;
        if not GFetcherParser.AddDocToFetchParse(QueryString, nil, fpmGet, QueryOpts, self, nil)
        then
        begin
          FinishedFetchParseRequest;
          QueryOpts.Free;
          FErrInfo.FetchOK := false;
          result := true;
        end
      end
      else
      begin
        QueryOpts.Free;
        FErrInfo.FetchOK := false;
      end;
    finally
      QueryParams.Free;
      QueryValues.Free;
    end;
  end
end;

// This function assumes that exceptions are raised before we get to the
// point where we would start additional fetch parses.
function TInstaUserProfileImporter.ProcessParseTree(WorkItem: TImporterWorkItem;
  var ErrMsg: string): boolean;
var
  FPResult: TFetchParseResult;
begin
  result := false;
  try
    FPResult := WorkItem.FPResult;
    Assert(Assigned(FPResult));
    result := true;
    try
      if (FPResult.ParseResult).NodeType = NodeTypeHTML then
      begin
        FUserProfile.Free;
        FUserProfile := MakeUserObjFromProfilePage(FPResult.ParseResult as THTMLDocument);
        if SupplementaryQueriesFinished(FPResult) then
          FinishedProcessing(WorkItem);
      end
      else if (FPResult.ParseResult.NodeType) = NodeTypeJSON then
      begin
        UpdateMediaAndExtentsFromJSON(FUserProfile, (FPResult.ParseResult as TJSONDocument));
        if SupplementaryQueriesFinished(FPResult) then
          FinishedProcessing(WorkItem);
      end
      else
        Assert(false);
    except
      on E: Exception do
      begin
        result := false;
        ErrMsg := E.ClassName + ':' + E.Message;
      end;
    end;
  finally
    try
      FNav.StackEmpty(result);
    except
      on EAssertionFailed do;
    end;
  end;
end;

function TInstaUserProfileImporter.RequestUserProfile(Block: TKSiteUserBlock;
  Options: TImportOptions): boolean;
var
  FetchUrl: string;
  QueryOpts: TFetcherOpts;
begin
  result := false;
  Assert(Block.Valid);
  if AttemptStartProcessing then
  begin
    FErrInfo.Reset;
    if AttemptNextFetchParseRequest then
    begin
      FUserProfile.Free;
      FUserProfile := nil;
      FBailout := false;
      if Assigned(Options) then
      begin
        FMediaBailoutTime := Options.BailoutTime;
        FInitialFetchOnly := Options.InitialFetchOnly;
      end
      else
      begin
        FMediaBailoutTime := 0;
        FInitialFetchOnly := false;
      end;

      FetchUrl := BuildURL(S_SSL_PROTOCOL, S_INSTAGRAM_SITE_NAME, Block.Username + S_SLASH, false,
        nil, nil);
      QueryOpts := GenFirstOpts(ThrottleDelay);
      result := GFetcherParser.AddDocToFetchParse(FetchUrl, nil, fpmGet, QueryOpts, self, nil);
      if not result then
      begin
        QueryOpts.Free;
        FinishedFetchParseRequest;
      end;
    end;
    if not result then
    begin
      FErrInfo.FetchOK := false;
      FErrInfo.ParseOK := false;
      FErrInfo.ErrMsg := S_REQUEST_FAILED;
      FinishedProcessing(nil);
    end;
  end;
end;

function TInstaUserProfileImporter.RetrieveResult(var UserProfile: TKUserProfile;
  var ErrInfo: TImportErrInfo): boolean;
begin
  UserProfile := nil;
  ErrInfo := nil;
  result := LockCheckIdle;
  try
    if result then
    begin
      ErrInfo := TImportErrInfo.Create;
      ErrInfo.Assign(FErrInfo);
      UserProfile := FUserProfile;
      FErrInfo.Reset;
      FUserProfile := nil;
    end;
  finally
    UnlockFromIdle;
  end;
end;

end.
