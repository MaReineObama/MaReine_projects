DELETE FROM `nissan-react.Staging_React.{{ params.into_table_name }}`
WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND CURRENT_DATE() AND country='{{ params.iso_code }}';
    
INSERT INTO `nissan-react.Staging_React.{{ params.into_table_name }}` (
    date,
    country,
    trackingcode,
    bounces,
    pageviews,
    uniquevisitors,
    visits,
    cm1006_1,
    cm1006_2,
    cm1006_3,
    cm1006_4,
    cm1006_5,
    cm1006_6,
    cm1006_7,
    cm1006_8,
    cm1006_9,
    cm1006_10,
    cm1006_11,
    cm1006_12,
    cm1006_13,
    cm1006_14,
    cm1006_15,
    cm1006_16,
    cm1006_17,
    cm1006_18,
    cm1006_19,
    cm1006_20

    
    
    SELECT DISTINCT
          date,
          '{{ params.iso_code }}' AS country,
          trackingcode,
          bounces,
          pageviews,
          uniquevisitors,
          visits,
          cm1006_1,
          cm1006_2,
          cm1006_3,
          cm1006_4,
          cm1006_5,
          cm1006_6,
          cm1006_7,
          cm1006_8,
          cm1006_9,
          cm1006_10,
          cm1006_11,
          cm1006_12,
          cm1006_13,
          cm1006_14,
          cm1006_15,
          cm1006_16,
          cm1006_17,
          cm1006_18,
          cm1006_19,
          cm1006_20
    
    FROM `{{ params.from_table_name }}`
    WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AND CURRENT_DATE();
