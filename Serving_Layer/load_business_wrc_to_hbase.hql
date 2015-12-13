CREATE EXTERNAL TABLE yuan_yelp_business_wrc_not_orc_3(
    business_id String,
    name String,
    categories String,
    review_count_recorded int , 
    stars String, 
    open String,
    full_address String,
    city String,
    state String,
    longitude String,
    latitude String
    )
COMMENT 'intermediate non orc table, DATA ABOUT businesss on yelp, added with calculated review counts by all users and active users(> 10 reviews)'
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n';

-- LOAD DATA INPATH 'hdfs:///user/xiaomaogy/output/business_w_active_review_count' OVERWRITE INTO TABLE yuan_yelp_business_wrc_not_orc_2;
LOAD DATA INPATH 'hdfs:///apps/hive/warehouse/yuan_yelp_business_wrc_not_orc' OVERWRITE INTO TABLE yuan_yelp_business_wrc_not_orc_2;

CREATE TABLE yuan_yelp_business_wrc_hbase_sync_2(
    business_id String,
    name String,
    categories String,
    review_count_recorded BIGINT, 
    stars String, 
    open String,
    full_address String,
    city String,
    state String,
    longitude String,
    latitude String,
    review_count_active_user BIGINT,
    review_count_all_user BIGINT
    )
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with SERDEPROPERTIES ("hbase.columns.mapping" = ":key, business:name, business:categories, business:review_count, 
    business:stars, business:open, business:full_address, business:city, business:state, business:longitude, business:latitude, business:review_count_active_user, business:review_count_all_user")
TBLPROPERTIES ("hbase.table.name" = "yuan_yelp_business_wrc_hbase_sync_2");

INSERT OVERWRITE TABLE yuan_yelp_business_wrc_hbase_sync_2 SELECT * FROM yuan_yelp_business_wrc_not_orc_2;

