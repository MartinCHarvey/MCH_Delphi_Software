unit TwitterImporters;
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

// Twitter importers a bit different from instagram, basically,
// user and media items only, buuuut,
// For cases where tweets reference other tweets, we need a way
// of putting a link field in which references the original tweet.

type
  TTwitterMediaImporter = class(TMediaImporter)
  private
    FNav: THTMLNavHelper;
    FMediaItem: TKMediaItem;
    FParentUserBlock: TKSiteUserBlock;
    FCommentBailoutTime: TDateTime;
    FInitialFetchOnly: boolean;
    FBailout: boolean;

    // Two stage process for updating via JSON, as it has some
    // encapsulated HTML that needs re-parsing.
    FPendingCommentList: TKCommentList;
    FPendingJSONRoot: TJSONNode;
    FPendingEncapsulatedHtml: TJSONSimpleValue;
  protected
    procedure ClearPendingState;
    procedure UpdateRepliesAndExtentsFromEncapHTML(Doc: THTMLNode);
    function UpdateRepliesAndExtentsFromJSON(FPResult: TFetchParseResult): boolean;
    procedure PopulateTweetReplyFromMediaHTML(CommentList: TKCommentList; BodyNode: THTMLBlock);
    procedure PopulateTweetReplyListFromMediaPage(Media: TKMediaItem; Block: THTMLBlock);
    procedure PopulateTweetDetailsFromMediaPage(Media: TKMediaItem; Block: THTMLBlock);
    function MakeMediaItemFromMediaPage(Doc: THTMLNode): TKMediaItem;
    function SupplementaryQueriesFinished(FPResult: TFetchParseResult): boolean;
    // Returns whether generated supplementary query, in which case,
    // Doc node cached.
    function ProcessParseTree(WorkItem: TImporterWorkItem; var ErrMsg: string): boolean; override;
  public
    constructor Create;
    destructor Destroy; override;
    function RequestMedia(ParentUserBlock: TKSiteUserBlock; MediaBlock: TKSiteMediaBlock;
      Options: TImportOptions): boolean;
    function RetrieveResult(var Media: TKMediaItem; var ErrInfo: TImportErrInfo): boolean; override;
  end;

  TTwitterUserProfileImporter = class(TUserProfileImporter)
  private
    FNav: THTMLNavHelper;
    FUserProfile: TKUserProfile;
    FMediaBailoutTime: TDateTime;
    FInitialFetchOnly: boolean;
    FBailout: boolean;

    // Two stage process for updating via JSON, as it has some
    // encapsulated HTML that needs re-parsing.
    FPendingMediaList: TKMediaList;
    FPendingJSONRoot: TJSONNode;
    FPendingEncapsulatedHtml: TJSONSimpleValue;
  protected
    procedure ClearPendingState;
    function SupplementaryQueriesFinished(FPResult: TFetchParseResult): boolean;

    procedure PopulateUserObjFromHTML(Obj: TKUserProfile; Block: THTMLBlock);

    procedure PopulateUserTweetFromProfileHTML(MediaList: TKMediaList; ParentUID: string;
      Block: THTMLBlock);
    procedure PopulateUserTweetListFromHTML(Obj: TKUserProfile; Block: THTMLBlock);
    function MakeUserObjFromProfilePage(Doc: THTMLNode): TKUserProfile;

    procedure UpdateMediaAndExtentsFromEncapHTML(Doc: THTMLNode);
    function UpdateMediaAndExtentsFromJSON(FPResult: TFetchParseResult): boolean;
    // Returns whether generated supplementary query, in which case,
    // Doc node cached.
    function ProcessParseTree(WorkItem: TImporterWorkItem; var ErrMsg: string): boolean; override;
    function CheckAssigned(Obj: TObject; Msg: string = ''): TObject; override;
  public
    constructor Create;
    destructor Destroy; override;
    function RequestUserProfile(Block: TKSiteUserBlock; Options: TImportOptions): boolean;
    function RetrieveResult(var UserProfile: TKUserProfile; var ErrInfo: TImportErrInfo)
      : boolean; override;
  end;

implementation

uses
  HTTPDocFetcher, SysUtils, Trackables, HTMLEscapeHelper, DLList, IndexedStore,
  Classes,
{$IFDEF INCLUDE_AUTH}
  CredentialManager,
{$ENDIF}
  KKUtils
{$IFOPT C+}
    , Windows
{$ENDIF}
    ;

const
  S_SLASH: string = '/';

  S_TWITTER_DATA_UID: string = 'data-user-id';
  S_TWITTER_DATA_UNAME: string = 'data-screen-name';
  S_TWITTER_DATA_FULLNAME: string = 'data-name';

  S_TWITTER_DATA_COUNT: string = 'data-count';
  S_TWITTER_HREF: string = 'href';
  S_TWITTER_SRC: string = 'src';
  S_TWITTER_DATA_MAX_POSITION: string = 'data-max-position';
  S_TWITTER_DATA_MIN_POSITION: string = 'data-min-position';

  S_TWITTER_DATA_ITEM_ID: string = 'data-item-id';
  S_TWITTER_DATA_TWEET_ID: string = 'data-tweet-id';
  S_TWITTER_DATA_TIME: string = 'data-time';

  S_TWITTER_Q_INC_AVAIL_FEATURES: string = 'include_available_features';
  S_TWITTER_Q_INC_ENTITIES: string = 'include_entities';
  S_TWITTER_Q_MAX_POSITION: string = 'max_position';
  S_TWITTER_Q_RESET_ERROR_STATE: string = 'reset_error_state';
  S_TWITTER_QUERY_URI_1: string = 'i/profiles/show/';
  S_TWITTER_QUERY_URI_2: string = '/timeline/tweets';

  S_TWITTER_JSON_MIN_POSITION: string = 'min_position';
  S_TWITTER_JSON_HAS_MORE_ITEMS: string = 'has_more_items';
  S_TWITTER_JSON_ITEMS_HTML: string = 'items_html';

  // Media import
  S_TWITTER_RETWEET_ID = 'data-retweet-id';
  S_TWITTER_MISTAKEN_RETWEET =
    'Media importer followed retweet redirection - original tweets only please.';

  S_TWITTER_MQUERY_URI_1: string = 'i/';
  S_TWITTER_MQUERY_URI_2: string = '/conversation/';

  S_REQUEST_FAILED = 'Request failed, possibly out of memory?';

function GenFirstOpts(ThrottleDelay: integer): TFetcherOpts;
begin
  result := TFetcherOpts.Create;
  result.CustomUserAgent := S_USER_AGENT;
  result.RequireAuthBeforeFetch := false;
  result.DesireAuth := false;
  result.OAuth2SignRequest := signAddAndSign;
  result.ThrottleDelay := ThrottleDelay;
end;

function GenNextOpts(PrevOpts: TFetcherOpts; ThrottleDelay: integer; PrevUrl: string): TFetcherOpts;
var
  Proto, Site, Fullfile, Name, Ext: string;
begin
  result := TFetcherOpts.Create;
  result.CustomUserAgent := S_USER_AGENT;
  if Assigned(PrevOpts) then
  begin
    result.RequireAuthBeforeFetch := PrevOpts.RequireAuthBeforeFetch;
    result.DesireAuth := PrevOpts.DesireAuth;
    result.OAuth2SignRequest := PrevOpts.OAuth2SignRequest;
    result.SupplementaryHeaderNames.Assign(PrevOpts.SupplementaryHeaderNames);
    result.SupplementaryHeaderVals.Assign(PrevOpts.SupplementaryHeaderVals);
  end
  else
  begin
    result.RequireAuthBeforeFetch := false;
    result.DesireAuth := false;
    result.OAuth2SignRequest := signAddAndSign;
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

{ TTwitterMediaImporter }

procedure TTwitterMediaImporter.ClearPendingState;
begin
  FPendingCommentList.Free;
  FPendingJSONRoot.Free;
  FPendingCommentList := nil;
  FPendingJSONRoot := nil;
  FPendingEncapsulatedHtml := nil;
end;

// NB. This is pretty similar to the innards of PopulateTweetReplyListFromMediaPage.
procedure TTwitterMediaImporter.UpdateRepliesAndExtentsFromEncapHTML(Doc: THTMLNode);
var
  Node: THTMLNode;
begin
  try
    Node := Doc;
    if not FNav.FindChild(Node, '<li class=""/>') then
      Node := nil;
    while Assigned(Node) do
    begin
      if FNav.PushChild(Node, '<ol class="stream-items"/>') then
      begin
        // Regardless of whether just one tweet or multiple, we just want the first.
        // If we ever want to follow up on replies to replies, then we'll let the batch loader
        // follow the recursion.
        if FNav.PushChildRec(Node, '<li class="js-stream-item stream-item stream-item"/>') then
        begin
          PopulateTweetReplyFromMediaHTML(FPendingCommentList, Node as THTMLBlock);
          FNav.Pop(Node);
        end
        else if FNav.PushChildRec(Node,
          '<li class="stream-tombstone-item stream-item js-stream-item"/>') then
        begin
          // Tweet or user deleted, ignore.
          FNav.Pop(Node);
        end
        else
          raise EImportError.Create(FNav.LastSearch);
        FNav.Pop(Node);
      end;
      if not FNav.FindNext(Node, '<li class=""/>') then
        Node := nil;
    end;
    if not FMediaItem.Comments.MergeWithNewer(FPendingCommentList) then
      raise EImportError.Create('');
  finally
    ClearPendingState;
  end;
end;

function TTwitterMediaImporter.UpdateRepliesAndExtentsFromJSON(FPResult: TFetchParseResult)
  : boolean;
var
  Doc: TJSONNode;

  NewCommentList: TKCommentList;
  NewEncapHTML: TJSONSimpleValue;

  Cont: TJSONContainer;
  Mem: TJSONMember;
  Val: TJSONValue;
  SVal: TJSONSimpleValue;

  GotMin, GotMoreFlag, GotItems: boolean;
  EncapHTMLAnsi: AnsiString;
  EncapHTMLStream: TStream;

begin
  result := true;
  Doc := FPResult.ParseResult as TJSONNode;
  Assert(Assigned(Doc));
  // Some extent information is held in JSON, but most of it is in encapsulated HTML.
  Assert(not Assigned(FPendingCommentList));
  Assert(not Assigned(FPendingJSONRoot));
  Assert(not Assigned(FPendingEncapsulatedHtml));
  GotMin := false;
  GotMoreFlag := false;
  GotItems := false;
  NewCommentList := nil;
  NewEncapHTML := nil;

  try
    NewCommentList := TKCommentList.Create;

    CheckAssigned(Doc.ContainedListHead.FLink.Owner);
    if not(Doc.ContainedListHead.FLink.Owner is TJSONContainer) then
      raise EImportError.Create('');
    Cont := Doc.ContainedListHead.FLink.Owner as TJSONContainer;
    if not(Cont.ContainerType = jctObject) then
      raise EImportError.Create('');
    Mem := Cont.ContainedListHead.FLink.Owner as TJSONMember;
    while Assigned(Mem) do
    begin
      if Mem.Name = S_TWITTER_JSON_MIN_POSITION then
      begin
        Val := CheckAssigned(Mem.ContainedListHead.FLink.Owner) as TJSONValue;
        if not(Val is TJSONSimpleValue) then
          raise EImportError.Create('');
        SVal := Val as TJSONSimpleValue;
        if SVal.ValType = svtString then
        begin
          (NewCommentList.Extents as TKItemExtents).FirstId := SVal.StrData;
          (NewCommentList.Extents as TKItemExtents).LinkId := SVal.StrData;
        end
        else if SVal.ValType = svtNull then
        begin
          (NewCommentList.Extents as TKItemExtents).FirstId := '';
          (NewCommentList.Extents as TKItemExtents).LinkId := '';
        end
        else
          raise EImportError.Create('');
        GotMin := true;
      end
      else if Mem.Name = S_TWITTER_JSON_HAS_MORE_ITEMS then
      begin
        Val := CheckAssigned(Mem.ContainedListHead.FLink.Owner) as TJSONValue;
        if not(Val is TJSONSimpleValue) then
          raise EImportError.Create('');
        SVal := Val as TJSONSimpleValue;
        if not((SVal.ValType = svtFalse) or (SVal.ValType = svtTrue)) then
          raise EImportError.Create('');
        (NewCommentList.Extents as TKItemExtents).HasEarlier := SVal.ValType = svtTrue;
        GotMoreFlag := true;
      end
      else if Mem.Name = S_TWITTER_JSON_ITEMS_HTML then
      begin
        Val := CheckAssigned(Mem.ContainedListHead.FLink.Owner) as TJSONValue;
        if not(Val is TJSONSimpleValue) then
          raise EImportError.Create('');
        SVal := Val as TJSONSimpleValue;
        if SVal.ValType <> svtString then
          raise EImportError.Create('');

        NewEncapHTML := SVal;
        GotItems := true;
      end;
      Mem := Mem.SiblingListEntry.FLink.Owner as TJSONMember;
    end;
    if not(GotMin and GotMoreFlag and GotItems) then
      raise EImportError.Create('');

    // Encapsulated HTML should definitely be UTF-8 encoded and all niceified.
    // Need ANSIString encoding to write to temp stream and feed to fetcher-parser.
    if AttemptNextFetchParseRequest then
    begin
      result := false;
      EncapHTMLAnsi := CheckedUnicodeToUTF8(NewEncapHTML.StrData);
      EncapHTMLStream := TTrackedMemoryStream.Create;
      EncapHTMLStream.WriteBuffer(EncapHTMLAnsi[1], Length(EncapHTMLAnsi));
      if not GFetcherParser.AddStreamToParse(EncapHTMLStream, tdtHTML, self, nil) then
      begin
        result := true;
        FErrInfo.FetchOK := false;
        EncapHTMLStream.Free;
      end;
    end
    else
      FErrInfo.FetchOK := false;
    // All OK, set temp vars, and note of where we are, remove JSON tree and cache on the side.
    if not result then
    begin
      FPendingCommentList := NewCommentList;
      NewCommentList := nil;
      FPendingJSONRoot := Doc;
      FPendingEncapsulatedHtml := NewEncapHTML;
      Assert(FPendingJSONRoot = FPResult.ParseResult);
      FPResult.ParseResult := nil;
    end;
  finally
    NewCommentList.Free;
  end;
end;

// Block is: <li class="js-stream-item stream-item stream-item" data-item-id="936253419781844993" id="stream-item-tweet-936253419781844993" data-item-type="tweet"/>
procedure TTwitterMediaImporter.PopulateTweetReplyFromMediaHTML(CommentList: TKCommentList;
  BodyNode: THTMLBlock);
var
  CommentItem: TKCommentItem;
  Node: THTMLNode;
  Tag: THTMLTag;
  ValPair: THTMLValuePair;
  TweetId: string;
  OwnerId: string;
  OwnerName: string;
  ProfilePicURL: string;
begin
  Node := BodyNode;
  CommentItem := TKCommentItem.Create;
  try
    Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
    ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_ITEM_ID)) as THTMLValuePair;
    TweetId := ValPair.ValData;
    if not(Length(TweetId) > 0) then
      raise EImportError.Create('');

    // Currently similar to part of PopulateUserTweetFromProfileHTML, but might change.
    if FNav.PushChild(Node, '<div class="tweet js-stream-tweet"/>') then
    begin
      if FNav.PushChild(Node, '<div class="content"/>') then
      begin
        if FNav.PushChild(Node, '<div class="stream-item-header"/>') then
        begin
          if FNav.PushChild(Node, '<a class="js-user-profile-link"/>') then
          begin
            Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
            ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_UID)) as THTMLValuePair;
            OwnerId := ValPair.ValData;
            ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_HREF)) as THTMLValuePair;
            OwnerName := ValPair.ValData;
            if OwnerName[1] <> '/' then
              raise EImportError.Create('');
            OwnerName := OwnerName.Substring(1);
            //Optionally get an avatar as well, not vital, but nice.
            if FNav.PushChild(Node, '<img class="avatar">') then
            begin
              Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
              ValPair := Tag.FindValuePairByName(S_TWITTER_SRC) as THTMLValuePair;
              if Assigned(ValPair) and (Length(ValPair.ValData) > 0) then
                ProfilePicURL := ValPair.ValData;
              FNav.Pop(Node);
            end;
            FNav.Pop(Node);
          end
          else
            raise EImportError.Create(FNav.LastSearch);
          if FNav.PushChild(Node, '<small class="time"/>') then
          begin
            if FNav.PushChildRec(Node, '<span class="_timestamp js-short-timestamp"/>') then
            begin
              Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
              ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_TIME))
                as THTMLValuePair;
              CommentItem.Date := UnixDateTimeToDelphiDateTime(Trunc(StrToFloat(ValPair.ValData)));
              FNav.Pop(Node);
            end
            else
              raise EImportError.Create(FNav.LastSearch);
            FNav.Pop(Node);
          end
          else
            raise EImportError.Create(FNav.LastSearch);
          FNav.Pop(Node);
        end
        else
          raise EImportError.Create(FNav.LastSearch);
        // Don't need original tweet text.
        FNav.Pop(Node);
      end
      else if FNav.FindChild(Node, '<div class="StreamItemContent--withheld"/>') then
      begin
        FNav.Pop(Node);
        exit;
      end
      else
        raise EImportError.Create(FNav.LastSearch);
      FNav.Pop(Node);
    end;

    CommentItem.CommentData := GenMetaLink(tstTwitter, klMediaList, TweetId, OwnerId);
    CommentItem.CommentType := citMetaLink;
    CommentItem.SiteUserBlock[tstTwitter].Username := OwnerName;
    CommentItem.SiteUserBlock[tstTwitter].UserId := OwnerId;
    CommentItem.SiteUserBlock[tstTwitter].ProfilePicUrl := ProfilePicUrl;
    CommentItem.SiteUserBlock[tstTwitter].Valid := true;
    // Assigning comment ID as original tweet ID a bit hacky, but is a valid
    // ref, and ensures uniqueness.
    CommentItem.SiteCommentBlock[tstTwitter].CommentId := TweetId;
    CommentItem.SiteCommentBlock[tstTwitter].Valid := true;
    CommentItem.LastUpdated := Now;

    // If comments older than bailout ID, then'd don't retrieve any more.
    if (FCommentBailoutTime <> 0) and (CommentItem.Date < FCommentBailoutTime) then
    begin
      FBailout := true;
    end;

{$IFOPT C+}
    if not CommentItem.SanityCheck(tstTwitter) then
    begin
      DebugBreak();
    end;
{$ENDIF}
    if not CommentItem.SanityCheck(tstTwitter) then
      raise EImportError.Create('');

    if CommentList.Add(CommentItem) then
      CommentItem := nil
    else
      raise EImportError.Create('');

  finally
    CommentItem.Free;
  end;
end;

// Block is: <div class="stream-container  " data-max-position="" data-min-position=""/>
procedure TTwitterMediaImporter.PopulateTweetReplyListFromMediaPage(Media: TKMediaItem;
  Block: THTMLBlock);
var
  Node: THTMLNode;
  Tag: THTMLTag;
  ValPair: THTMLValuePair;
  Ext: TKItemExtents;
begin
  Node := Block;
  Ext := (Media.Comments.Extents as TKItemExtents);
  Tag := CheckAssigned(Block.Tag) as THTMLTag;
  ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_MIN_POSITION)) as THTMLValuePair;
  Ext.FirstId := ValPair.ValData;
  ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_MAX_POSITION)) as THTMLValuePair;
  Ext.LastId := ValPair.ValData;
  Ext.HasLater := false;
  Ext.HasEarlier := Length(Ext.FirstId) > 0;

  if FNav.PushChild(Node, '<div class="stream"/>') then
  begin
    if FNav.PushChild(Node, '<ol class="stream-items" id="stream-items-id"/>') then
    begin
      // NB. This is basically the same as UpdateFromEncapHTML.
      if not FNav.FindChild(Node, '<li class=""/>') then
        Node := nil;
      while Assigned(Node) do
      begin
        if FNav.PushChild(Node, '<ol class="stream-items"/>') then
        begin
          // Regardless of whether just one tweet or multiple, we just want the first.
          // If we ever want to follow up on replies to replies, then we'll let the batch loader
          // follow the recursion.
          if FNav.PushChildRec(Node, '<li class="js-stream-item stream-item stream-item"/>') then
          begin
            PopulateTweetReplyFromMediaHTML(Media.Comments, Node as THTMLBlock);
            FNav.Pop(Node);
          end
          else if FNav.PushChildRec(Node,
            '<li class="stream-tombstone-item stream-item js-stream-item"/>') then
          begin
            // Tweet or user deleted, ignore.
            FNav.Pop(Node);
          end
          else
            raise EImportError.Create(FNav.LastSearch);
          FNav.Pop(Node);
        end;
        if not FNav.FindNext(Node, '<li class=""/>') then
          Node := nil;
      end;
      FNav.Pop(Node);
    end;
    FNav.Pop(Node);
  end
  else
    raise EImportError.Create(FNav.LastSearch);
end;

// Block is: <div class="tweet permalink-tweet"/>
// No, we can't be a retweet.
procedure TTwitterMediaImporter.PopulateTweetDetailsFromMediaPage(Media: TKMediaItem;
  Block: THTMLBlock);
var
  Node: THTMLNode;
  Tag: THTMLTag;
  ValPair: THTMLValuePair;
begin
  Node := Block;
  Tag := Block.Tag;
  ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_TWEET_ID)) as THTMLValuePair;
  Media.SiteMediaBlock[tstTwitter].MediaID := ValPair.ValData;
  if FNav.PushChild(Node, '<div class="content clearfix"/>') then
  begin
    if FNav.PushChild(Node, '<div class="permalink-header"/>') then
    begin
      if FNav.PushChild(Node, '<a class="js-user-profile-link js-nav"/>') then
      begin
        // Check UID ok.
        Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
        ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_UID)) as THTMLValuePair;
        Media.SiteMediaBlock[tstTwitter].OwnerId := ValPair.ValData;
        if CompareText(Media.SiteMediaBlock[tstTwitter].OwnerId, FParentUserBlock.UserId) <> 0 then
          raise EImportError.Create(S_TWITTER_MISTAKEN_RETWEET);
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(FNav.LastSearch);

      if FNav.PushChild(Node, '<small class="time"/>') then
      begin
        if FNav.PushChildRec(Node, '<span class="_timestamp js-short-timestamp"/>') then
        begin
          Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
          ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_TIME)) as THTMLValuePair;
          Media.Date := UnixDateTimeToDelphiDateTime(Trunc(StrToFloat(ValPair.ValData)));
          FNav.Pop(Node);
        end
        else
          raise EImportError.Create(FNav.LastSearch);
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(FNav.LastSearch);
      FNav.Pop(Node);
    end
    else
      raise EImportError.Create(FNav.LastSearch);
    FNav.Pop(Node);
  end
  else
    raise EImportError.Create(FNav.LastSearch);
  if FNav.PushChild(Node, '<div class="js-tweet-text-container"/>') then
  begin
    Media.MediaData := Node.ReconstructedHTML(trhOmitTopTag);
    Media.MediaType := mitHTML;
    FNav.Pop(Node);
  end
  else
    raise EImportError.Create(FNav.LastSearch);

  Media.SiteMediaBlock[tstTwitter].Valid := true;
  Media.LastUpdated := Now;
end;

function TTwitterMediaImporter.MakeMediaItemFromMediaPage(Doc: THTMLNode): TKMediaItem;
var
  Node: THTMLNode;
  MediaItem: TKMediaItem;
begin
  Assert(Assigned(Doc));
  result := nil;
  MediaItem := nil;
  Node := Doc;
  try
    if FNav.FindChild(Node, '<html/>') and FNav.FindChild(Node, '<body/>') then
    begin
      if FNav.PushChild(Node, '<div class="PermalinkOverlay" id="permalink-overlay"/>') then
      begin
        if FNav.PushChildRec(Node, '<div class="PermalinkOverlay-content"/>') then
        begin
          if FNav.PushChildRec(Node, '<div role="main" class="permalink original-permalink-page"/>')
          then
          begin
            MediaItem := TKMediaItem.Create;
            if FNav.PushChild(Node, '<div class="permalink-inner permalink-tweet-container"/>') then
            begin
              if FNav.PushChild(Node, '<div class="tweet permalink-tweet"/>') then
              begin
                PopulateTweetDetailsFromMediaPage(MediaItem, Node as THTMLBlock);
                FNav.Pop(Node);
              end
              else
                EImportError.Create(FNav.LastSearch);
              FNav.Pop(Node);
            end
            else
              raise EImportError.Create(FNav.LastSearch);
            if FNav.PushChild(Node, '<div class="replies-to permalink-replies"/>') then
            begin
              if FNav.PushChildRec(Node, '<div class="stream-container"/>') then
              begin
                PopulateTweetReplyListFromMediaPage(MediaItem, Node as THTMLBlock);
                FNav.Pop(Node);
              end
              else
                raise EImportError.Create(FNav.LastSearch);
              FNav.Pop(Node);
            end
            else
              raise EImportError.Create(FNav.LastSearch);
            FNav.Pop(Node);
          end
          else
            raise EImportError.Create(FNav.LastSearch);
          FNav.Pop(Node);
        end
        else
          raise EImportError.Create(FNav.LastSearch);
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(FNav.LastSearch);
    end
    else
      raise EImportError.Create(FNav.LastSearch);

{$IFOPT C+}
    if not MediaItem.SanityCheck(tstTwitter) then
    begin
      DebugBreak();
    end;
{$ENDIF}
    if not MediaItem.SanityCheck(tstTwitter) then
      raise EImportError.Create('');

    result := MediaItem;
    MediaItem := nil;
  finally
    MediaItem.Free;
  end;
end;

function TTwitterMediaImporter.ProcessParseTree(WorkItem: TImporterWorkItem;
  var ErrMsg: string): boolean;
var
  FPResult: TFetchParseResult;
begin
  FPResult := WorkItem.FPResult;
  Assert(Assigned(FPResult));
  result := true;
  try
    try
      if (FPResult.ParseResult).NodeType = NodeTypeHTML then
      begin
        Assert((Assigned(FPendingCommentList) = Assigned(FPendingJSONRoot)) and
          (Assigned(FPendingCommentList) = Assigned(FPendingEncapsulatedHtml)));
        if not Assigned(FPendingCommentList) then
        begin
          // First page query.
          FMediaItem.Free;
          FMediaItem := MakeMediaItemFromMediaPage(FPResult.ParseResult as THTMLDocument);
          if SupplementaryQueriesFinished(FPResult) then
            FinishedProcessing(WorkItem);
        end
        else
        begin
          UpdateRepliesAndExtentsFromEncapHTML(FPResult.ParseResult as THTMLDocument);
          if SupplementaryQueriesFinished(FPResult) then
            FinishedProcessing(WorkItem);
        end;
      end
      else if (FPResult.ParseResult.NodeType) = NodeTypeJSON then
      begin
        if UpdateRepliesAndExtentsFromJSON(FPResult) then
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

function TTwitterMediaImporter.RequestMedia(ParentUserBlock: TKSiteUserBlock;
  MediaBlock: TKSiteMediaBlock; Options: TImportOptions): boolean;
var
  FetchUrl: string;
  QueryOpts: TFetcherOpts;
begin
  result := false;
  Assert(MediaBlock.Valid);
  Assert(ParentUserBlock.Valid);
  if AttemptStartProcessing then
  begin
    FParentUserBlock.Free;
    FParentUserBlock := nil;
    FErrInfo.Reset;
    if AttemptNextFetchParseRequest then
    begin
      FMediaItem.Free;
      FMediaItem := nil;
      ClearPendingState;

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

      FParentUserBlock := TKSiteUserBlock.Clone(ParentUserBlock) as TKSiteUserBlock;
      // https://twitter.com/MartinCHarvey/status/937771144920346624
      FetchUrl := BuildURL(S_SSL_PROTOCOL, S_TWITTER_SITE_NAME, ParentUserBlock.Username +
        '/status/' + MediaBlock.MediaID, false, nil, nil);
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

function TTwitterMediaImporter.RetrieveResult(var Media: TKMediaItem;
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
      FParentUserBlock.Free;
      FParentUserBlock := nil;
      FMediaItem := nil;
      ClearPendingState;
    end;
  finally
    UnlockFromIdle;
  end;
end;

constructor TTwitterMediaImporter.Create;
begin
  inherited;
  FNav := THTMLNavHelper.Create;
  FNav.ValPairHandling := vphTokenize;
end;

destructor TTwitterMediaImporter.Destroy;
begin
  FNav.Free;
  FMediaItem.Free;
  FParentUserBlock.Free;
  ClearPendingState;
  inherited;
end;

function TTwitterMediaImporter.SupplementaryQueriesFinished(FPResult: TFetchParseResult): boolean;
var
  OldestId: string;
  NewestId: string;
  QueryParams: TStringList;
  QueryValues: TStringList;
  QueryString: string;
  QueryOpts: TFetcherOpts;

begin
  result := true;
  if not Assigned(FMediaItem) then
    raise EImportError.Create('');

  if FBailout or FInitialFetchOnly then
    exit;

  // Check media list is accurate reflection on what's actually there.
  OldestId := (FMediaItem.Comments.Extents as TKItemExtents).FirstId;
  NewestId := (FMediaItem.Comments.Extents as TKItemExtents).LastId;

  if (FMediaItem.Comments.Extents as TKItemExtents).HasEarlier then
  // or (FUserProfile.Media.Extents as TKItemExtents).HasLater then
  begin
    QueryParams := TStringList.Create;
    QueryValues := TStringList.Create;
    try
      // Max position is the same as min position of previous HTML page.

      QueryParams.Add(S_TWITTER_Q_INC_AVAIL_FEATURES);
      QueryValues.Add('1');
      QueryParams.Add(S_TWITTER_Q_INC_ENTITIES);
      QueryValues.Add('1');
      QueryParams.Add(S_TWITTER_Q_MAX_POSITION);
      QueryValues.Add(OldestId);
      QueryParams.Add(S_TWITTER_Q_RESET_ERROR_STATE);
      QueryValues.Add('false');

      (FMediaItem.Comments.Extents as TKItemExtents).LinkId := OldestId;
      {
        Query is of the form:
        https://twitter.com/i/<Username>/conversation/<MediaID>?
      }
      QueryString := BuildURL(S_SSL_PROTOCOL, S_TWITTER_SITE_NAME,
        S_TWITTER_MQUERY_URI_1 + FParentUserBlock.Username + S_TWITTER_MQUERY_URI_2 +
        FMediaItem.SiteMediaBlock[tstTwitter].MediaID, true, QueryParams, QueryValues);

      QueryOpts := GenNextOpts(FPResult.FetcherOpts, ThrottleDelay, FPResult.Url);
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

{ TTwitterUserProfileImporter }

procedure TTwitterUserProfileImporter.ClearPendingState;
begin
  FPendingMediaList.Free;
  FPendingJSONRoot.Free;
  FPendingMediaList := nil;
  FPendingJSONRoot := nil;
  FPendingEncapsulatedHtml := nil;
end;

procedure TTwitterUserProfileImporter.UpdateMediaAndExtentsFromEncapHTML(Doc: THTMLNode);
var
  Node: THTMLNode;
begin
  try
    Node := Doc;
    if not FNav.FindChild(Node, '<li class="js-stream-item stream-item" data-item-type="tweet"/>')
    then
      Node := nil;
    while Assigned(Node) do
    begin
      PopulateUserTweetFromProfileHTML(FPendingMediaList, FUserProfile.SiteUserBlock[tstTwitter]
        .UserId, Node as THTMLBlock);
      if not FNav.FindNext(Node, '<li class="js-stream-item stream-item" data-item-type="tweet"/>')
      then
        Node := nil;
    end;

    if not FUserProfile.Media.MergeWithNewer(FPendingMediaList) then
      raise EImportError.Create('');
  finally
    ClearPendingState;
  end;
end;

function TTwitterUserProfileImporter.UpdateMediaAndExtentsFromJSON
  (FPResult: TFetchParseResult): boolean;
var
  Doc: TJSONNode;

  NewMediaList: TKMediaList;
  NewEncapHTML: TJSONSimpleValue;

  Cont: TJSONContainer;
  Mem: TJSONMember;
  Val: TJSONValue;
  SVal: TJSONSimpleValue;

  GotMin, GotMoreFlag, GotItems: boolean;
  EncapHTMLAnsi: AnsiString;
  EncapHTMLStream: TStream;

begin
  result := true;
  Doc := FPResult.ParseResult as TJSONNode;
  Assert(Assigned(Doc));
  // Some extent information is held in JSON, but most of it is in encapsulated HTML.
  Assert(not Assigned(FPendingMediaList));
  Assert(not Assigned(FPendingJSONRoot));
  Assert(not Assigned(FPendingEncapsulatedHtml));
  GotMin := false;
  GotMoreFlag := false;
  GotItems := false;
  NewMediaList := nil;
  NewEncapHTML := nil;

  try
    NewMediaList := TKMediaList.Create;

    CheckAssigned(Doc.ContainedListHead.FLink.Owner);
    if not(Doc.ContainedListHead.FLink.Owner is TJSONContainer) then
      raise EImportError.Create('');
    Cont := Doc.ContainedListHead.FLink.Owner as TJSONContainer;
    if not(Cont.ContainerType = jctObject) then
      raise EImportError.Create('');
    Mem := Cont.ContainedListHead.FLink.Owner as TJSONMember;
    while Assigned(Mem) do
    begin
      if Mem.Name = S_TWITTER_JSON_MIN_POSITION then
      begin
        Val := CheckAssigned(Mem.ContainedListHead.FLink.Owner) as TJSONValue;
        if not(Val is TJSONSimpleValue) then
          raise EImportError.Create('');
        SVal := Val as TJSONSimpleValue;
        if SVal.ValType = svtString then
        begin
          (NewMediaList.Extents as TKItemExtents).FirstId := SVal.StrData;
          (NewMediaList.Extents as TKItemExtents).LinkId := SVal.StrData;
        end
        else if SVal.ValType = svtNull then
        begin
          (NewMediaList.Extents as TKItemExtents).FirstId := '';
          (NewMediaList.Extents as TKItemExtents).LinkId := '';
        end
        else
          raise EImportError.Create('');
        GotMin := true;
      end
      else if Mem.Name = S_TWITTER_JSON_HAS_MORE_ITEMS then
      begin
        Val := CheckAssigned(Mem.ContainedListHead.FLink.Owner) as TJSONValue;
        if not(Val is TJSONSimpleValue) then
          raise EImportError.Create('');
        SVal := Val as TJSONSimpleValue;
        if not((SVal.ValType = svtFalse) or (SVal.ValType = svtTrue)) then
          raise EImportError.Create('');
        (NewMediaList.Extents as TKItemExtents).HasEarlier := SVal.ValType = svtTrue;
        GotMoreFlag := true;
      end
      else if Mem.Name = S_TWITTER_JSON_ITEMS_HTML then
      begin
        Val := CheckAssigned(Mem.ContainedListHead.FLink.Owner) as TJSONValue;
        if not(Val is TJSONSimpleValue) then
          raise EImportError.Create('');
        SVal := Val as TJSONSimpleValue;
        if SVal.ValType <> svtString then
          raise EImportError.Create('');

        NewEncapHTML := SVal;
        GotItems := true;
      end;
      Mem := Mem.SiblingListEntry.FLink.Owner as TJSONMember;
    end;
    if not(GotMin and GotMoreFlag and GotItems) then
      raise EImportError.Create('');

    // Encapsulated HTML should definitely be UTF-8 encoded and all niceified.
    // Need ANSIString encoding to write to temp stream and feed to fetcher-parser.
    if AttemptNextFetchParseRequest then
    begin
      result := false;
      EncapHTMLAnsi := CheckedUnicodeToUTF8(NewEncapHTML.StrData);
      EncapHTMLStream := TTrackedMemoryStream.Create;
      EncapHTMLStream.WriteBuffer(EncapHTMLAnsi[1], Length(EncapHTMLAnsi));
      if not GFetcherParser.AddStreamToParse(EncapHTMLStream, tdtHTML, self, nil) then
      begin
        result := true;
        FErrInfo.FetchOK := false;
        EncapHTMLStream.Free;
      end;
    end
    else
      FErrInfo.FetchOK := false;
    // All OK, set temp vars, and note of where we are, remove JSON tree and cache on the side.
    if not result then
    begin
      FPendingMediaList := NewMediaList;
      NewMediaList := nil;
      FPendingJSONRoot := Doc;
      FPendingEncapsulatedHtml := NewEncapHTML;
      Assert(FPendingJSONRoot = FPResult.ParseResult);
      FPResult.ParseResult := nil;
    end;
  finally
    NewMediaList.Free;
  end;
end;

function TTwitterUserProfileImporter.SupplementaryQueriesFinished
  (FPResult: TFetchParseResult): boolean;
var
  OldestId: string;
  NewestId: string;
  QueryParams: TStringList;
  QueryValues: TStringList;
  QueryString: string;
  QueryOpts: TFetcherOpts;

begin
  result := true;
  if not Assigned(FUserProfile) then
    raise EImportError.Create('');

  if FBailout or FInitialFetchOnly then
    exit;

  // Check media list is accurate reflection on what's actually there.
  OldestId := (FUserProfile.Media.Extents as TKItemExtents).FirstId;
  NewestId := (FUserProfile.Media.Extents as TKItemExtents).LastId;

  if (FUserProfile.Media.Extents as TKItemExtents).HasEarlier then
  // or (FUserProfile.Media.Extents as TKItemExtents).HasLater then
  begin
    QueryParams := TStringList.Create;
    QueryValues := TStringList.Create;
    try
      // Max position is the same as min position of previous HTML page.

      QueryParams.Add(S_TWITTER_Q_INC_AVAIL_FEATURES);
      QueryValues.Add('1');
      QueryParams.Add(S_TWITTER_Q_INC_ENTITIES);
      QueryValues.Add('1');
      QueryParams.Add(S_TWITTER_Q_MAX_POSITION);
      QueryValues.Add(OldestId);
      QueryParams.Add(S_TWITTER_Q_RESET_ERROR_STATE);
      QueryValues.Add('false');

      (FUserProfile.Media.Extents as TKItemExtents).LinkId := OldestId;

      QueryString := BuildURL(S_SSL_PROTOCOL, S_TWITTER_SITE_NAME,
        S_TWITTER_QUERY_URI_1 + FUserProfile.SiteUserBlock[tstTwitter].Username +
        S_TWITTER_QUERY_URI_2, true, QueryParams, QueryValues);

      QueryOpts := GenNextOpts(FPResult.FetcherOpts, ThrottleDelay, FPResult.Url);
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

// Block is '<div id="page-container" class="AppContent"/>'
procedure TTwitterUserProfileImporter.PopulateUserObjFromHTML(Obj: TKUserProfile;
  Block: THTMLBlock);
var
  Node: THTMLNode;
  Tag: THTMLTag;
  ValPair: THTMLValuePair;
begin
  Node := Block;
  if FNav.PushChild(Node, '<div class="ProfileCanopy ProfileCanopy--withNav"/>') then
  begin
    if FNav.PushChildRec(Node, '<div class="ProfileCanopy-nav"/>') then
    begin
      if FNav.PushChild(Node, '<div class="ProfileNav" role="navigation"/>') then
      begin
        Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
        ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_UID)) as THTMLValuePair;
        Obj.SiteUserBlock[tstTwitter].UserId := ValPair.ValData;
        if FNav.PushChild(Node, '<ul class="ProfileNav-list"/>') then
        begin
          if FNav.PushChild(Node, '<li class="ProfileNav-item ProfileNav-item--userActions"/>') then
          begin
            if FNav.PushChildRec(Node, '<div class="user-actions btn-group"/>') then
            begin
              Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
              ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_UNAME))
                as THTMLValuePair;
              Obj.SiteUserBlock[tstTwitter].Username := ValPair.ValData;
              ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_FULLNAME))
                as THTMLValuePair;
              Obj.SiteUserBlock[tstTwitter].FullName := ValPair.ValData;
              FNav.Pop(Node);
            end
            else
              raise EImportError.Create(FNav.LastSearch);
            FNav.Pop(Node);
          end
          else
            raise EImportError.Create(FNav.LastSearch);
          if FNav.PushChild(Node, '<li class="ProfileNav-item ProfileNav-item--following"/>') then
          begin
            if FNav.PushChildRec(Node, '<span class="ProfileNav-value"/>') then
            begin
              Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
              ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_COUNT))
                as THTMLValuePair;
              Obj.SiteUserBlock[tstTwitter].FollowsCount := StrToInt(ValPair.ValData);
              FNav.Pop(Node);
            end
            else
              raise EImportError.Create(FNav.LastSearch);
            FNav.Pop(Node);
          end;
          // Else leave counts unchanged, prob zero
          if FNav.PushChild(Node, '<li class="ProfileNav-item ProfileNav-item--followers"/>') then
          begin
            if FNav.PushChildRec(Node, '<span class="ProfileNav-value"/>') then
            begin
              Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
              ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_COUNT))
                as THTMLValuePair;
              Obj.SiteUserBlock[tstTwitter].FollowerCount := StrToInt(ValPair.ValData);
              FNav.Pop(Node);
            end
            else
              raise EImportError.Create(FNav.LastSearch);
            FNav.Pop(Node);
          end;
          // Else leave counts unchanged, prob zero
          FNav.Pop(Node);
        end
        else
          raise EImportError.Create(FNav.LastSearch);
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(FNav.LastSearch);
      FNav.Pop(Node);
    end
    else
      raise EImportError.Create(FNav.LastSearch);

    if FNav.PushChildRec(Node, '<div class="ProfileCanopy-card"/>') then
    begin
      if FNav.PushChildRec(Node, '<a class="ProfileCardMini-avatar profile-picture"/>"') then
      begin
        Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
        ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_HREF)) as THTMLValuePair;
        Obj.SiteUserBlock[tstTwitter].ProfilePicUrl := ValPair.ValData;
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(FNav.LastSearch);
      FNav.Pop(Node);
    end
    else
      raise EImportError.Create(FNav.LastSearch);
    FNav.Pop(Node)
  end
  else
    raise EImportError.Create(FNav.LastSearch);

  if FNav.PushChild(Node, '<div class="AppContainer"/>') then
  begin
    if FNav.PushChildRec(Node, '<div class="ProfileHeaderCard"/>') then
    begin
      if FNav.PushChild(Node, '<h1 class="ProfileHeaderCard-name"/>') then
      begin
        if FNav.FindChild(Node, '<span class="ProfileHeaderCard-badges"/>') then
          Obj.SiteUserBlock[tstTwitter].Verified := true;
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(FNav.LastSearch);
      if FNav.PushChild(Node, '<p class="ProfileHeaderCard-bio"/>') then
      begin
        Obj.SiteUserBlock[tstTwitter].Bio := '';
        Node := Node.ContainedListHead.FLink.Owner as THTMLNode;
        while Assigned(Node) do
        begin
          Obj.SiteUserBlock[tstTwitter].Bio := Obj.SiteUserBlock[tstTwitter].Bio +
            (Node as THTMLBlock).Text;
          Node := Node.SiblingListEntry.FLink.Owner as THTMLNode;
        end;
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(FNav.LastSearch);
      FNav.Pop(Node);
    end
    else
      raise EImportError.Create(FNav.LastSearch);
    FNav.Pop(Node)
  end
  else
    raise EImportError.Create(FNav.LastSearch);
end;

// Timeline tweets, all we need to do here is to detect retweets,
// We don't need to worry abour replies at the moment
// (they get handled in the thread of the tweet being replied to)
// ...at least for the moment.
procedure TTwitterUserProfileImporter.PopulateUserTweetFromProfileHTML(MediaList: TKMediaList;
  ParentUID: string; Block: THTMLBlock);
var
  Node: THTMLNode;
  Tag: THTMLTag;
  ValPair: THTMLValuePair;
  MediaItem: TKMediaItem;
  IsRetweet: boolean;
  TweetId: string;
  RetweetId: string;
  RetweetedOwner: string;
  TweetData: string;

begin
  MediaItem := TKMediaItem.Create;
  try
    Node := Block;
    IsRetweet := false;
    // Block is: <li class="js-stream-item stream-item" data-item-type="tweet"/>
    if FNav.PushChild(Node, '<div class="tweet js-stream-tweet"/>') then
    begin
      Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
      ValPair := Tag.FindValuePairByName(S_TWITTER_RETWEET_ID) as THTMLValuePair;
      if Assigned(ValPair) then
        RetweetId := ValPair.ValData
      else
        RetweetId := '';
      ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_ITEM_ID)) as THTMLValuePair;
      TweetId := ValPair.ValData;

      if FNav.PushChild(Node, '<div class="context"/>') then
      begin
        // Detect retweets
        if FNav.PushChildRec(Node, '<span class="js-retweet-text"/>') then
        begin
          IsRetweet := true;
          FNav.Pop(Node);
        end;
        FNav.Pop(Node);
      end
      else if FNav.FindChild(Node, '<div class="StreamItemContent--withheld"/>') then
      begin
        FNav.Pop(Node);
        exit;
      end
      else
        raise EImportError.Create(FNav.LastSearch);

      if not(IsRetweet = (Length(RetweetId) > 0)) then
        raise EImportError.Create('Retweet indications do not match');

      // Currently same as PopulateTweetReplyFromMediaHTML, but might change...
      if FNav.PushChild(Node, '<div class="content"/>') then
      begin
        if FNav.PushChild(Node, '<div class="stream-item-header"/>') then
        begin
          if IsRetweet then
          begin
            if FNav.PushChild(Node, '<a class="js-user-profile-link"/>') then
            begin
              Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
              ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_UID))
                as THTMLValuePair;
              RetweetedOwner := ValPair.ValData;
              FNav.Pop(Node);
            end
            else
              raise EImportError.Create(FNav.LastSearch);
          end;
          if FNav.PushChild(Node, '<small class="time"/>') then
          begin
            if FNav.PushChildRec(Node, '<span class="_timestamp js-short-timestamp"/>') then
            begin
              Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
              ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_TIME))
                as THTMLValuePair;
              MediaItem.Date := UnixDateTimeToDelphiDateTime(Trunc(StrToFloat(ValPair.ValData)));
              FNav.Pop(Node);
            end
            else
              raise EImportError.Create(FNav.LastSearch);
            FNav.Pop(Node);
          end
          else
            raise EImportError.Create(FNav.LastSearch);
          FNav.Pop(Node);
        end
        else
          raise EImportError.Create(FNav.LastSearch);
        if FNav.PushChild(Node, '<div class="js-tweet-text-container"/>') then
        begin
          TweetData := Node.ReconstructedHTML(trhOmitTopTag);
          FNav.Pop(Node);
        end
        else
          raise EImportError.Create(FNav.LastSearch);
        FNav.Pop(Node);
      end
      else if FNav.FindChild(Node, '<div class="StreamItemContent--withheld"/>') then
      begin
        FNav.Pop(Node);
        exit;
      end
      else
        raise EImportError.Create(FNav.LastSearch);
      FNav.Pop(Node);

      if not(IsRetweet = (Length(RetweetId) > 0)) then
        raise EImportError.Create(FNav.LastSearch);

      if IsRetweet then
        MediaItem.SiteMediaBlock[tstTwitter].MediaID := RetweetId
      else
        MediaItem.SiteMediaBlock[tstTwitter].MediaID := TweetId;

      MediaItem.SiteMediaBlock[tstTwitter].OwnerId := ParentUID;
      MediaItem.SiteMediaBlock[tstTwitter].Valid := true;
      MediaItem.LastUpdated := Now;
    end
    else
      raise EImportError.Create(FNav.LastSearch);

    if IsRetweet then
    begin
      MediaItem.MediaData := GenMetaLink(tstTwitter, klMediaList, TweetId, RetweetedOwner);
      MediaItem.MediaType := mitMetaLink;
    end
    else
    begin
      MediaItem.MediaData := TweetData;
      MediaItem.MediaType := mitHTML;
    end;

    if not(IsRetweet = (MediaItem.MediaType = mitMetaLink)) then
      raise EImportError.Create('Retweet and media type do not match.');

    // If media older than bailout ID, then'd don't retrieve any more.
    // Don't count retweets, because the date on the item is that of the
    // original tweet, not the retweet time.
    if not IsRetweet then
    begin
      if (FMediaBailoutTime <> 0) and (MediaItem.Date < FMediaBailoutTime) then
      begin
        FBailout := true;
      end;
    end;

{$IFOPT C+}
    if not MediaItem.SanityCheck(tstTwitter) then
    begin
      DebugBreak();
    end;
{$ENDIF}
    if not MediaItem.SanityCheck(tstTwitter) then
      raise EImportError.Create('');

    if MediaList.Add(MediaItem) then
      MediaItem := nil
    else
      raise EImportError.Create('');
  finally
    MediaItem.Free;
  end;
end;

// Block is '<div id="page-container" class="AppContent"/>'
procedure TTwitterUserProfileImporter.PopulateUserTweetListFromHTML(Obj: TKUserProfile;
  Block: THTMLBlock);
var
  Node: THTMLNode;
  Tag: THTMLTag;
  ValPair: THTMLValuePair;
  MExtents: TKItemExtents;
begin
  Node := Block;
  if FNav.PushChild(Node, '<div class="AppContainer"/>') then
  begin
    // If no profile timeline, then private profile.
    if FNav.PushChildRec(Node, '<div id="timeline" class="ProfileTimeline "/>') then
    begin
      if FNav.PushChild(Node, '<div class="stream-container"/>') then
      begin
        // Get data-max-position and data-min-position out of this block.
        Tag := CheckAssigned((Node as THTMLBlock).Tag) as THTMLTag;
        MExtents := Obj.Media.Extents as TKItemExtents;
        ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_MAX_POSITION))
          as THTMLValuePair;
        MExtents.LastId := ValPair.ValData;
        ValPair := CheckAssigned(Tag.FindValuePairByName(S_TWITTER_DATA_MIN_POSITION))
          as THTMLValuePair;
        MExtents.FirstId := ValPair.ValData;
        MExtents.HasEarlier := Length(MExtents.FirstId) > 0;
        MExtents.HasLater := false;
        MExtents.LinkId := MExtents.FirstId;
        if FNav.PushChild(Node, '<div class="stream"/>') then
        begin
          if FNav.PushChild(Node,
            '<ol class="stream-items js-navigable-stream" id="stream-items-id"/>') then
          begin
            if not FNav.FindChild(Node,
              '<li class="js-stream-item stream-item" data-item-type="tweet"/>') then
              Node := nil;
            while Assigned(Node) do
            begin
              PopulateUserTweetFromProfileHTML(Obj.Media, Obj.SiteUserBlock[tstTwitter].UserId,
                Node as THTMLBlock);
              if not FNav.FindNext(Node,
                '<li class="js-stream-item stream-item" data-item-type="tweet"/>') then
                Node := nil;
            end;
            FNav.Pop(Node);
          end
          else
            raise EImportError.Create(FNav.LastSearch);
          FNav.Pop(Node);
        end
        else
          raise EImportError.Create(FNav.LastSearch);
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(FNav.LastSearch);
      FNav.Pop(Node);
    end;
    FNav.Pop(Node);
  end
  else
    raise EImportError.Create(FNav.LastSearch);
end;

function TTwitterUserProfileImporter.MakeUserObjFromProfilePage(Doc: THTMLNode): TKUserProfile;
var
  Node: THTMLNode;
  UP: TKUserProfile;
begin
  Assert(Assigned(Doc));
  result := nil;
  Node := Doc;
  if FNav.FindChild(Node, '<html/>') and FNav.FindChild(Node, '<body/>') then
  begin
    if FNav.PushChild(Node, '<div id="doc"/>') then
    begin
      if FNav.PushChildRec(Node, '<div id="page-container" class="AppContent"/>') then
      begin
        // OK, probably able to have a fair shot at creating user profile.
        UP := TKUserProfile.Create;
        try
          UP.LastUpdated := Now;
          UP.SiteUserBlock[tstTwitter].Valid := true;
          PopulateUserObjFromHTML(UP, Node as THTMLBlock);
          PopulateUserTweetListFromHTML(UP, Node as THTMLBlock);

{$IFOPT C+}
          if not UP.SanityCheck(tstTwitter) then
          begin
            DebugBreak();
          end;
{$ENDIF}
          if not UP.SanityCheck(tstTwitter) then
            raise EImportError.Create('');

          result := UP;
          UP := nil;
        finally
          UP.Free;
        end;
        FNav.Pop(Node);
      end
      else
        raise EImportError.Create(FNav.LastSearch);
      FNav.Pop(Node);
    end
    else
      raise EImportError.Create(FNav.LastSearch);
  end
  else
    raise EImportError.Create(FNav.LastSearch);
end;

// This function assumes that exceptions are raised before we get to the
// point where we would start additional fetch parses.
function TTwitterUserProfileImporter.ProcessParseTree(WorkItem: TImporterWorkItem;
  var ErrMsg: string): boolean;
var
  FPResult: TFetchParseResult;
begin
  FPResult := WorkItem.FPResult;
  Assert(Assigned(FPResult));
  result := true;
  try
    try
      if (FPResult.ParseResult).NodeType = NodeTypeHTML then
      begin
        Assert((Assigned(FPendingMediaList) = Assigned(FPendingJSONRoot)) and
          (Assigned(FPendingMediaList) = Assigned(FPendingEncapsulatedHtml)));
        if not Assigned(FPendingMediaList) then
        begin
          // First page query.
          FUserProfile.Free;
          FUserProfile := MakeUserObjFromProfilePage(FPResult.ParseResult as THTMLDocument);
          if SupplementaryQueriesFinished(FPResult) then
            FinishedProcessing(WorkItem);
        end
        else
        begin
          UpdateMediaAndExtentsFromEncapHTML(FPResult.ParseResult as THTMLDocument);
          if SupplementaryQueriesFinished(FPResult) then
            FinishedProcessing(WorkItem);
        end;
      end
      else if (FPResult.ParseResult.NodeType) = NodeTypeJSON then
      begin
        if UpdateMediaAndExtentsFromJSON(FPResult) then
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

function TTwitterUserProfileImporter.CheckAssigned(Obj: TObject; Msg: string): TObject;
begin
  if Length(Msg) = 0 then
    Msg := FNav.LastSearch;
  result := inherited CheckAssigned(Obj, Msg);
end;

function TTwitterUserProfileImporter.RequestUserProfile(Block: TKSiteUserBlock;
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
      ClearPendingState;

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

      FetchUrl := BuildURL(S_SSL_PROTOCOL, S_TWITTER_SITE_NAME, Block.Username + S_SLASH, false,
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

function TTwitterUserProfileImporter.RetrieveResult(var UserProfile: TKUserProfile;
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
      ClearPendingState;
    end;
  finally
    UnlockFromIdle;
  end;
end;

constructor TTwitterUserProfileImporter.Create;
begin
  inherited;
  FNav := THTMLNavHelper.Create;
  FNav.ValPairHandling := vphTokenize;
end;

destructor TTwitterUserProfileImporter.Destroy;
begin
  FNav.Free;
  FUserProfile.Free;
  FUserProfile := nil;
  ClearPendingState;
  inherited;
end;

end.
