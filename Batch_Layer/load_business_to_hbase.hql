-- Data schema
-- json_business_row = FOREACH json_business_raw_row_3 GENERATE 
--     REGEX_EXTRACT(str, '\\"business_id\\"\\:\\s\\"(.*?)\\"', 1) AS business_id, 
--     REGEX_EXTRACT(str, '\\"name\\"\\:\\s\\"(.*?)\\"', 1) AS name, 
--     categories, 
--     REGEX_EXTRACT(str, '\\"review_count\\"\\:\\s(.*?),', 1) AS review_count, 
--     REGEX_EXTRACT(str, '\\"stars\\"\\:\\s(.*?),', 1) AS stars,
--     REGEX_EXTRACT(str, '\\"open\\"\\:\\s(.*?),', 1) AS open,
--     REPLACE(REPLACE(REGEX_EXTRACT(str, '\\"full_address\\"\\:\\s\\"(.*?)\\"', 1),'\\\\n','*'),'\\\\r','*') AS full_address,
--     REGEX_EXTRACT(str, '\\"city\\"\\:\\s\\"(.*?)\\"', 1) AS city,
--     REGEX_EXTRACT(str, '\\"state\\"\\:\\s\\"(.*?)\\"', 1) AS state,
--     REGEX_EXTRACT(str, '\\"longitude\\"\\:\\s(.*?),', 1) AS longitude,
--     REGEX_EXTRACT(str, '\\"latitude\\"\\:\\s(.*?),', 1) AS latitude;


CREATE EXTERNAL TABLE yuan_yelp_business_not_orc(
    business_id String,
    name String,
    categories String,
    review_count int, 
    stars String, 
    open String,
    full_address String,
    city String,
    state String,
    longitude String,
    latitude String
    )
COMMENT 'intermediate non orc table, DATA ABOUT businesss on yelp'
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'hdfs:///user/xiaomaogy/output/json_business_table' OVERWRITE INTO TABLE yuan_yelp_business_not_orc;

CREATE TABLE yuan_yelp_business_hbase_sync(
    business_id String,
    name String,
    categories String,
    review_count int, 
    stars String, 
    open String,
    full_address String,
    city String,
    state String,
    longitude String,
    latitude String
    )
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with SERDEPROPERTIES ("hbase.columns.mapping" = ":key, business:name, business:categories, business:review_count, 
    business:stars, business:open, business:full_address, business:city, business:state, business:longitude, business:latitude")
TBLPROPERTIES ("hbase.table.name" = "yuan_yelp_business_hive_sync");

INSERT OVERWRITE TABLE yuan_yelp_business_hbase_sync SELECT * FROM yuan_yelp_business_not_orc;

