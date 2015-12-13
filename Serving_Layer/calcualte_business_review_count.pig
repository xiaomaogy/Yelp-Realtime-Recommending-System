--load all the data
review_row = LOAD 'hdfs:///apps/hive/warehouse/yuan_yelp_review_not_orc/part-m-00000' USING PigStorage('\u0001')
as (review_id:chararray, business_id:chararray, user_id:chararray, review_stars:double, review_date:datetime, 
    review_text:chararray, type:chararray, votes_funny:int, votes_useful:int, votes_cool:int);

user_row = LOAD 'hdfs:///apps/hive/warehouse/yuan_yelp_user_not_orc/part-m-00000' USING PigStorage('\u0001')
as (user_id:chararray, name:chararray, average_stars:double, review_count:int, type:chararray, votes_funny:int, votes_useful:int, votes_cool:int);

--calculate the review count made by active user
active_user_row = FILTER user_row BY (review_count >= 10);

review_w_active_user_row_raw = JOIN review_row BY user_id, active_user_row BY user_id USING 'replicated';

review_w_active_user_row = FOREACH review_w_active_user_row_raw GENERATE review_row::business_id AS business_id, review_row::review_id AS review_id, review_row::user_id AS user_id;

review_w_active_user_row_by_business = GROUP review_w_active_user_row BY business_id;

business_review_count_w_active_user = FOREACH review_w_active_user_row_by_business GENERATE group AS business_id, COUNT(review_w_active_user_row.review_id) as cal_review_count, COUNT(review_w_active_user_row.user_id) as cal_user_count;

STORE business_review_count_w_active_user INTO 'hdfs:///user/xiaomaogy/output/temp1' USING PigStorage('\u0001');

--calculate the review count by all users
review_row_short = FOREACH review_row GENERATE review_id, business_id,user_id;

review_row_by_business = GROUP review_row_short BY business_id;

business_review_count = FOREACH review_row_by_business GENERATE group AS business_id, COUNT(review_row_short.review_id) as cal_review_count, COUNT(review_row_short.user_id) as cal_user_count;

STORE business_review_count INTO 'hdfs:///user/xiaomaogy/output/business_w_active_review_count' USING PigStorage('\u0001');

