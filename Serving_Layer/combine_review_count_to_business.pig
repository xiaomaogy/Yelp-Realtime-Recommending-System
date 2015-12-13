--load all the business data

business_row = LOAD 'hdfs:///apps/hive/warehouse/yuan_yelp_business_not_orc/part-m-00000' USING PigStorage('\u0001')
as (business_id:chararray, name:chararray, categories:chararray, review_count_recorded:int, stars:double, open:boolean, 
    full_address:chararray, city:chararray, state:chararray,
    longitude:chararray, latitude:chararray);

business_review_count_w_active_user = LOAD 'hdfs:///user/xiaomaogy/output/temp1' USING PigStorage('\u0001') as (business_id:chararray, review_count_active_user:int, active_user_count:int);

business_review_count = LOAD 'hdfs:///user/xiaomaogy/output/temp2' USING PigStorage('\u0001') as (business_id:chararray, review_count_all_user:int, all_user_count:int);

--combine the review counts into business table

business_w_active_user_raw_row = JOIN business_row BY business_id, 
    business_review_count_w_active_user BY business_id;

business_w_active_user_row = FOREACH business_w_active_user_raw_row 
GENERATE business_row::business_id AS business_id, 
business_row::name AS name, 
business_row::categories as categories, 
business_row::review_count_recorded AS review_count_recorded, 
business_row::stars as stars, 
business_row::open AS open, 
business_row::full_address AS full_address,
business_row::city AS city,
business_row::state AS state,
business_row::longitude AS longitude,
business_row::latitude AS latitude,
business_review_count_w_active_user::review_count_active_user AS review_count_active_user;

business_w_both_user_raw_row = JOIN business_w_active_user_row BY business_id, 
    business_review_count BY business_id;

business_w_both_user_row = FOREACH business_w_both_user_raw_row 
GENERATE 
business_w_active_user_row::business_id AS business_id, 
business_w_active_user_row::name AS name, 
business_w_active_user_row::categories as categories, 
business_w_active_user_row::review_count_recorded AS review_count_recorded, 
business_w_active_user_row::stars as stars, 
business_w_active_user_row::open AS open, 
business_w_active_user_row::full_address AS full_address,
business_w_active_user_row::city AS city,
business_w_active_user_row::state AS state,
business_w_active_user_row::longitude AS longitude,
business_w_active_user_row::latitude AS latitude,
business_w_active_user_row::review_count_active_user AS review_count_active_user,
business_review_count::review_count_all_user AS review_count_all_user;

STORE business_w_both_user_row INTO 'hdfs:///user/xiaomaogy/output/business_w_active_review_count' USING PigStorage('\u0001');
