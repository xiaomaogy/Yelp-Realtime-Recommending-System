json_review_raw_row = LOAD 'hdfs:///user/xiaomaogy/input/yelp_training_set_review.json' USING JsonLoader('votes:(funny:int, useful:int, cool:int), user_id:chararray, review_id:chararray, stars:int, date:chararray, text:chararray, type:chararray, business_id:chararray');

json_review_row = FOREACH json_review_raw_row GENERATE FLATTEN(votes),user_id, review_id, stars AS review_stars, date AS review_date, REPLACE(REPLACE(text,'\n','*'),'\r','*') AS review_text, type, business_id;

json_review_row_2 = FOREACH json_review_row GENERATE review_id, business_id, user_id,review_stars, review_date, review_text, type,$0 AS votes_funny, $1 AS votes_useful, $2 AS votes_cool;

STORE json_review_row_2 INTO 'hdfs:///user/xiaomaogy/output/json_review_table' USING PigStorage('\u0001');

-- STORE json_review_row_2 INTO 'hbase://yuan_yelp_review'
-- USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('review:business_id, review:user_id,review:review_stars, review:review_date, review:review_text, review:type,review:votes_funny, review:votes_useful, review:votes_cool');

-- INSERT OVERWRITE TABLE yuan_yelp_review_hbase_sync SELECT * FROM yuan_yelp_review_not_orc;
