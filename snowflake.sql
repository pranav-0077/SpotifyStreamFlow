CREATE WAREHOUSE glue_ware;
USE WAREHOUSE glue_ware;

CREATE DATABASE glue_base;
USE DATABASE glue_base;

CREATE SCHEMA glue_sch;
USE SCHEMA glue_sch;

CREATE STORAGE INTEGRATION s3_spotifyglue
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/fake-placeholder-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://itsmyec2bucket/processed_data/spotify_clean_data/');

DESC INTEGRATION s3_spotifyglue;

CREATE OR REPLACE STAGE glue_stage
  URL = 's3://itsmyec2bucket/processed_data/spotify_clean_data/'
  STORAGE_INTEGRATION = s3_spotifyglue
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

CREATE OR REPLACE TABLE spotify_data (
  Source STRING,
  Index_col STRING,
  ID STRING,
  Name STRING,
  URL STRING
);

CREATE OR REPLACE FILE FORMAT spotify_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
FIELD_DELIMITER = ',';

COPY INTO spotify_data
FROM @glue_stage
ON_ERROR = 'CONTINUE';

ALTER STORAGE INTEGRATION s3_spotifyglue
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::310022569683:role/Glue_Snowflake';

SELECT * FROM spotify_data;
