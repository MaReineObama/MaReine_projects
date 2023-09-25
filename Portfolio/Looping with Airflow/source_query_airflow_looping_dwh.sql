DELETE FROM `nissan-react.DWH_React.{{ params.table_name }}`
WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND CURRENT_DATE();


INSERT INTO `nissan-react.DWH_React.{{ params.table_name }}`(
    date,
    country,
    trackingcode,
    uniquevisitors,
    bounces,
    pageviews,
    visits,
    Qualified_Visits,
    KBA,
    Lead,
    K_Configurator_Engagement,
    K_Vehicle_Brochure_Download,
    K_Offer_Engagement,
    K_Compare_tool_Interactions,
    K_Dealer_Search,
    Configurator_Completion,
    K_Dealer_Contact,
    K_Click_to_Dealer_Website,
    L_Contact_Dealer,
    L_Test_Drive_Request,
    L_Book_a_Service,
    L_Reserve_Online,
    L_Request_a_Brochure,
    L_Request_a_Quote,
    L_Keep_Me_Informed,
    L_Request_a_Call_Back,
    L_Offer_Details_Request,
    L_Video_Call_Request,
    channel_type
  )
  SELECT
    date,
    UPPER(country),
    trackingcode,
    SUM(uniquevisitors) AS  uniquevisitors,
    SUM(bounces) AS bounces,
    SUM(pageviews) AS pageviews,
    SUM(visits) AS visits,
    SUM(cm1006_1),
    SUM(cm1006_2),
    SUM(cm1006_3),
    SUM(cm1006_4),
    SUM(cm1006_5),
    SUM(cm1006_6),
    SUM(cm1006_7),
    SUM(cm1006_8),
    SUM(cm1006_9),
    SUM(cm1006_10),
    SUM(cm1006_11),
    SUM(cm1006_12),
    SUM(cm1006_13),
    SUM(cm1006_14),
    SUM(cm1006_15),
    SUM(cm1006_16),
    SUM(cm1006_17),
    SUM(cm1006_18),
    SUM(cm1006_19),
    SUM(cm1006_20),
    CASE WHEN STARTS_WITH(trackingcode,'ban_') THEN 'display'
        WHEN STARTS_WITH(trackingcode,'SM_') OR STARTS_WITH(trackingcode,'sm_') THEN 'social'
        when STARTS_WITH(trackingcode,'PS_') OR STARTS_WITH(trackingcode,'ps_') THEN 'marin'
        ELSE 'other'
    END
FROM `nissan-react.Staging_React.{{ params.staging_table }}`
WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND CURRENT_DATE()
GROUP BY date, country, trackingcode;

