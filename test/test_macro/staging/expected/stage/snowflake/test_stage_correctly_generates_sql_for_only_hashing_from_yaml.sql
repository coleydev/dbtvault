WITH source_data AS (

    SELECT

    "BOOKING_FK",
    "ORDER_FK",
    "CUSTOMER_PK",
    "CUSTOMER_ID",
    "LOAD_DATE",
    "RECORD_SOURCE",
    "CUSTOMER_DOB",
    "CUSTOMER_NAME",
    "NATIONALITY",
    "PHONE",
    "TEST_COLUMN_2",
    "TEST_COLUMN_3",
    "TEST_COLUMN_4",
    "TEST_COLUMN_5",
    "TEST_COLUMN_6",
    "TEST_COLUMN_7",
    "TEST_COLUMN_8",
    "TEST_COLUMN_9",
    "BOOKING_DATE"

    FROM [DATABASE_NAME].[SCHEMA_NAME].raw_source
),

hashed_columns AS (

    SELECT

    "BOOKING_FK",
    "ORDER_FK",
    "CUSTOMER_ID",
    "LOAD_DATE",
    "RECORD_SOURCE",
    "CUSTOMER_DOB",
    "CUSTOMER_NAME",
    "NATIONALITY",
    "PHONE",
    "TEST_COLUMN_2",
    "TEST_COLUMN_3",
    "TEST_COLUMN_4",
    "TEST_COLUMN_5",
    "TEST_COLUMN_6",
    "TEST_COLUMN_7",
    "TEST_COLUMN_8",
    "TEST_COLUMN_9",
    "BOOKING_DATE",

    CAST((MD5_BINARY(NULLIF(UPPER(TRIM(CAST("CUSTOMER_ID" AS VARCHAR))), ''))) AS BINARY(16)) AS "CUSTOMER_PK",

    CAST(MD5_BINARY(
         CONCAT(
             IFNULL(NULLIF(UPPER(TRIM(CAST("CUSTOMER_DOB" AS VARCHAR))), ''), '^^'), '||',
             IFNULL(NULLIF(UPPER(TRIM(CAST("CUSTOMER_ID" AS VARCHAR))), ''), '^^'), '||',
             IFNULL(NULLIF(UPPER(TRIM(CAST("CUSTOMER_NAME" AS VARCHAR))), ''), '^^')
         )) AS BINARY(16)
    ) AS "CUST_CUSTOMER_HASHDIFF",

    CAST(MD5_BINARY(
         CONCAT(
             IFNULL(NULLIF(UPPER(TRIM(CAST("CUSTOMER_ID" AS VARCHAR))), ''), '^^'), '||',
             IFNULL(NULLIF(UPPER(TRIM(CAST("NATIONALITY" AS VARCHAR))), ''), '^^'), '||',
             IFNULL(NULLIF(UPPER(TRIM(CAST("PHONE" AS VARCHAR))), ''), '^^')
         )) AS BINARY(16)
    ) AS "CUSTOMER_HASHDIFF"

    FROM source_data
),

columns_to_select AS (

    SELECT

    "CUSTOMER_PK",
    "CUST_CUSTOMER_HASHDIFF",
    "CUSTOMER_HASHDIFF"

    FROM hashed_columns
)

SELECT * FROM columns_to_select