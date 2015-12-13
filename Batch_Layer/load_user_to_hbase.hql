CREATE EXTERNAL TABLE yuan_yelp_user_not_orc(
    user_id String,
    name String,
    average_stars String,
    review_count int, 
    type String, 
    votes_funny int,
    votes_useful int,
    votes_cool int
    )
COMMENT 'intermediate non orc table, DATA ABOUT users on yelp'
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n';

LOAD DATA INPATH 'hdfs:///user/xiaomaogy/output/json_user_table' OVERWRITE INTO TABLE yuan_yelp_user_not_orc;

CREATE TABLE yuan_yelp_user_hbase_sync(
    user_id String,
    name String,
    average_stars String,
    review_count int, 
    type String, 
    votes_funny int,
    votes_useful int,
    votes_cool int
    )
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with SERDEPROPERTIES ("hbase.columns.mapping" = ":key, user:name, user:average_stars, user:review_count, user:type, user:votes_funny, user:votes_useful, user:votes_cool")
TBLPROPERTIES ("hbase.table.name" = "yuan_yelp_user_hive_sync");

INSERT OVERWRITE TABLE yuan_yelp_user_hbase_sync SELECT * FROM yuan_yelp_user_not_orc;

