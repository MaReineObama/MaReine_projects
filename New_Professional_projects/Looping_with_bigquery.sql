-- In this use case, we create a table that will contains data coming from different other table


CREATE OR REPLACE TABLE `<project_id>.<destination_dataset>.<destination_table>` (
    date DATE,
    account_id INT64,
    campaign_id INT64,
    adset_id INT64,
    ad_id INT64,
    platform STRING,
    objective STRING,
    currency STRING,
    cid STRING,
    Impressions FLOAT64,
    Clicks FLOAT64,
    Spends_local FLOAT64,
    Link_clicks FLOAT64,
    Post_engagements FLOAT64,
    Add_to_wishlist FLOAT64,
    Leads FLOAT64,
    Offsite_customed_leads FLOAT64,
    Post_Saves FLOAT64,
    Registration_completed FLOAT64,
    Post_shares FLOAT64,
    Post_comments FLOAT64,
    Location_searches FLOAT64,
    Website_location_searches FLOAT64,
    Event_responses FLOAT64,
    Pages_like FLOAT64,
    Messaging_conversations_started FLOAT64,
    Page_engagement FLOAT64,
    Post_reactions FLOAT64,
    Content_views FLOAT64,
    Landing_page_views FLOAT64,
    Videos_view_3s FLOAT64,
    Page_photo_views FLOAT64,
    Blocked_messaging_conversations FLOAT64,
    Message_to_buy FLOAT64,
    Video_plays_50 FLOAT64,
    Video_plays_15 FLOAT64,
    Video_plays_100 FLOAT64,
    Video_plays_25 FLOAT64,
    Video_plays_95 FLOAT64,
    Video_plays_75 FLOAT64,
    Video_plays FLOAT64,
    last_modified TIMESTAMP)
 

DECLARE Market ARRAY<STRING>;
DECLARE n INT64 DEFAULT 0;

SET Market = ['nksa', 'nmef'];


LOOP
SET n = n+1;
IF n > ARRAY_LENGTH(Market) THEN LEAVE;
END IF;

EXECUTE IMMEDIATE '''
INSERT INTO `<project_id>.<destination_dataset>.<destination_table>` 
(date, account_id, campaign_id, adset_id, ad_id, platform, objective, currency, cid, Impressions, Clicks, Spends_local, Link_clicks, Post_engagements, Add_to_wishlist, Leads, Offsite_customed_leads, Post_Saves, Registration_completed, Post_shares, Post_comments, Location_searches, Website_location_searches, Event_responses, Pages_like, Messaging_conversations_started, Page_engagement, Post_reactions, Content_views, Landing_page_views, Videos_view_3s, Page_photo_views, Blocked_messaging_conversations, Message_to_buy, Video_plays_50, Video_plays_15, Video_plays_100, Video_plays_25, Video_plays_95, Video_plays_75, Video_plays, last_modified)

  WITH
    mainkpi_adinsights AS (
    SELECT
      DateStart AS date,
      SAFE_CAST(AdAccountId AS int) AS account_id,
      SAFE_CAST(CampaignId AS int) AS campaign_id,
      SAFE_CAST(AdSetId AS int) AS adset_id,
      SAFE_CAST(AdId AS int) AS ad_id,
      PublisherPlatform AS platform,
      Objective AS objective,
      CASE
        WHEN LTRIM(AdAccountId) = "1051855731553497" THEN "SAR"
        WHEN LTRIM(AdAccountId) = "1042983862440684" THEN "AED"
      ELSE
     NULL
    END
      AS currency,
      SUM(Impressions) AS impressions,
      SUM(Clicks) AS clicks,
      SUM(Spend) AS spends_local,
      SUM(LinkClicks) AS link_clicks,
      SUM(PostEngagements) AS post_engagements,
    max(IF
      (gravity_updated IS NULL
        OR gravity_inserted > gravity_updated,
        gravity_inserted,
        gravity_updated )) last_modified_ts,
    FROM
      `<project_id>.<destination_dataset>.fb_amio_''' || Market[ORDINAL(n)] || '''_adinsights`
    GROUP BY
      date,
      account_id,
      campaign_id,
      adset_id,
      ad_id,
      platform,
      objective,
      currency ),

    secondarykpi_adinsightactions AS (
    SELECT
      DateStart AS date,
      SAFE_CAST(AdAccountId AS int) AS account_id,
      SAFE_CAST(CampaignId AS int) AS campaign_id,
      SAFE_CAST(AdSetId AS int) AS adset_id,
      SAFE_CAST(AdId AS int) AS ad_id,
      -- PublisherPlatform AS platform,
      SUM(
      IF
        (ActionCollection LIKE "%add_to_wishlist%",
          SAFE_CAST(ActionValue AS int),
          0)) AS add_to_wishlist,
       SUM(
      IF
        (actiontype LIKE "%lead%",
          SAFE_CAST(ActionValue AS int),
          0)) AS Leads,
      SUM(
      IF
        (actiontype LIKE "offsite_conversion.custom%" or actiontype LIKE "%offsite_conversion.fb_pixel_custom%",
          SAFE_CAST(ActionValue AS int),
          0)) AS Offsite_customed_leads,
      SUM(
      IF
        (actiontype LIKE "%post_save%",
          SAFE_CAST(ActionValue AS int),
          0)) AS Post_Saves,
      
      SUM(
      IF
        (actiontype LIKE "%complete_registration%",
          SAFE_CAST(ActionValue AS int),
          0)) AS Registration_completed,
     
      SUM(
      IF
        (actiontype = "post",
          SAFE_CAST(ActionValue AS int),
          0)) AS Post_shares,
      SUM(
      IF
        (actiontype = "comment",
          SAFE_CAST(ActionValue AS int),
          0)) AS Post_comments,
      SUM(
      IF
        (actiontype = "find_location_total",
          SAFE_CAST(ActionValue AS int),
          0)) AS Location_searches,
      SUM(
      IF
        (actiontype = "find_location_website",
          SAFE_CAST(ActionValue AS int),
          0)) AS Website_location_searches,
      SUM(
      IF
        (actiontype = "rsvp",
          SAFE_CAST(ActionValue AS int),
          0)) AS Event_responses,
      SUM(
      IF
        (actiontype = "like",
          SAFE_CAST(ActionValue AS int),
          0)) AS Pages_like,
      SUM(
      IF
        (actiontype = "onsite_conversion.messaging_conversation_started_7d" OR actiontype = "onsite_conversion.messaging_first_reply",
          SAFE_CAST(ActionValue AS int),
          0)) AS Messaging_conversations_started,
      SUM(
      IF
        (actiontype = "page_engagement",
          SAFE_CAST(ActionValue AS int),
          0)) AS Page_engagement,
      SUM(
      IF
        (actiontype = "post_reaction",
          SAFE_CAST(ActionValue AS int),
          0)) AS Post_reactions,
      SUM(
      IF
        (actiontype = "%view_content%",
          SAFE_CAST(ActionValue AS int),
          0)) AS Content_views,
      SUM(
      IF
        (actiontype = "landing_page_view",
          SAFE_CAST(ActionValue AS int),
          0)) AS Landing_page_views,
      SUM(
      IF
        (actiontype = "video_view",
          SAFE_CAST(ActionValue AS int),
          0)) AS Videos_view_3s,
      SUM(
      IF
        (actiontype = "photo_view",
          SAFE_CAST(ActionValue AS int),
          0)) AS Page_photo_views,
      SUM(
      IF
        (actiontype = "onsite_conversion.messaging_block",
          SAFE_CAST(ActionValue AS int),
          0)) AS Blocked_messaging_conversations,
      SUM(
      IF
        (actiontype = "onsite_conversion.message_to_buy",
          SAFE_CAST(ActionValue AS int),
          0)) AS Message_to_buy,
      SUM(
      IF
        (ActionCollection = "VideoP50WatchedActions",
          SAFE_CAST(ActionValue AS int),
          0)) AS Video_plays_50,
      SUM(
      IF
        (ActionCollection = "Video15_secWatchedActions",
          SAFE_CAST(ActionValue AS int),
          0)) AS Video_plays_15,
      SUM(
      IF
        (ActionCollection = "VideoP100_watchedActions",
          SAFE_CAST(ActionValue AS int),
          0)) AS Video_plays_100,
      SUM(
      IF
        (ActionCollection = "VideoP25WatchedActions",
          SAFE_CAST(ActionValue AS int),
          0)) AS Video_plays_25,
      SUM(
      IF
        (ActionCollection = "VideoP95WatchedActions",
          SAFE_CAST(ActionValue AS int),
          0)) AS Video_plays_95,
      SUM(
      IF
        (ActionCollection = "VideoP75WatchedActions",
          SAFE_CAST(ActionValue AS int),
          0)) AS Video_plays_75,
      SUM(
      IF
        (ActionCollection = "VideoPlayActions",
          SAFE_CAST(ActionValue AS int),
          0)) AS Video_plays,
      
   MAX(IF
      (gravity_updated IS NULL
        OR gravity_inserted > gravity_updated,
        gravity_inserted,
        gravity_updated )) last_modified_ts,
    FROM
      `<project_id>.<destination_dataset>.fb_amio_''' || Market[ORDINAL(n)] || '''_adinsightsactions`
    GROUP BY
      date,
      account_id,
      campaign_id,
      adset_id,
      ad_id
      -- platform
    )

  SELECT
    a.date,
    a.account_id,
    a.campaign_id,
    a.adset_id,
    a.ad_id,
    a.platform,
    a.objective,
    a.currency,
    cid,
  SAFE_DIVIDE(SUM(impressions),
     COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Impressions,

  SAFE_DIVIDE(SUM(clicks),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Clicks,

  SAFE_DIVIDE(SUM(spends_local),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Spends_local,

  SAFE_DIVIDE(SUM(link_clicks),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Link_clicks,

  SAFE_DIVIDE(SUM(post_engagements),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Post_engagements,

  SAFE_DIVIDE(SUM(add_to_wishlist),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Add_to_wishlist,

  SAFE_DIVIDE(SUM(Leads),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Leads,

  SAFE_DIVIDE(SUM(Offsite_customed_leads),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Offsite_customed_leads,

  SAFE_DIVIDE(SUM(Post_Saves),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Post_Saves,

  SAFE_DIVIDE(SUM(Registration_completed),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Registration_completed,

  SAFE_DIVIDE(SUM(Post_shares),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Post_shares,

  SAFE_DIVIDE(SUM(Post_comments),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Post_comments,

  SAFE_DIVIDE(SUM(Location_searches),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Location_searches,

  SAFE_DIVIDE(SUM(Website_location_searches),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Website_location_searches,

  SAFE_DIVIDE(SUM(Event_responses),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Event_responses,

  SAFE_DIVIDE(SUM(Pages_like),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Pages_like,

  SAFE_DIVIDE(SUM(Messaging_conversations_started),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Messaging_conversations_started,

  SAFE_DIVIDE(SUM(Page_engagement),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Page_engagement,

  SAFE_DIVIDE(SUM(Post_reactions),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Post_reactions,

  SAFE_DIVIDE(SUM(Content_views),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Content_views,

  SAFE_DIVIDE(SUM(Landing_page_views),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Landing_page_views,

  SAFE_DIVIDE(SUM(Videos_view_3s),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Videos_view_3s,

  SAFE_DIVIDE(SUM(Page_photo_views),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Page_photo_views,

  SAFE_DIVIDE(SUM(Blocked_messaging_conversations),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Blocked_messaging_conversations,

  SAFE_DIVIDE(SUM(Message_to_buy),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Message_to_buy,

  SAFE_DIVIDE(SUM(Video_plays_50),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Video_plays_50,

  SAFE_DIVIDE(SUM(Video_plays_15),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Video_plays_15,

  SAFE_DIVIDE(SUM(Video_plays_100),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Video_plays_100,

  SAFE_DIVIDE(SUM(Video_plays_25),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Video_plays_25,

  SAFE_DIVIDE(SUM(Video_plays_95),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Video_plays_95,

  SAFE_DIVIDE(SUM(Video_plays_75),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Video_plays_75,

  SAFE_DIVIDE(SUM(Video_plays),
    COUNT(IF(cid IS NULL, 0, 1)) OVER (PARTITION BY date, ad_id, platform, objective)) AS Video_plays,

   IF
    (a.last_modified_ts >= b.last_modified_ts OR b.last_modified_ts IS NULL,
      a.last_modified_ts,
      b.last_modified_ts) AS last_modified
  FROM
    mainkpi_adinsights a
  LEFT JOIN
    secondarykpi_adinsightactions b
  USING
    ( date,
      account_id,
      campaign_id,
      adset_id,
      ad_id
      -- platform
      )
  LEFT JOIN
  (SELECT *
  FROM
    `<project_id>.<destination_dataset>.staging_facebook_amio_3d0_cidcontrol1_cidlooker`
    WHERE cid IS NOT NULL
    ) c
  USING
    (ad_id)
    WHERE IMPRESSIONS > 0 
  GROUP BY
    a.date,
    a.account_id,
    a.campaign_id,
    a.adset_id,
    a.ad_id,
    a.platform,
    a.objective,
    a.currency,
    cid,
    last_modified
    
''';

END LOOP;
