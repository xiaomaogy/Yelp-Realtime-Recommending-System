json_user_raw_row = LOAD 'hdfs:///user/xiaomaogy/input/yelp_training_set_user.json' 
USING JsonLoader('votes:(funny:int, useful:int, cool:int), user_id:chararray, 
    name:chararray, average_stars:float, review_count:int, type:chararray');

json_user_row = FOREACH json_user_raw_row GENERATE FLATTEN(votes),user_id, name, average_stars, review_count, type;

json_user_row_2 = FOREACH json_user_row GENERATE user_id, name, average_stars,review_count, type, $0 AS votes_funny, $1 AS votes_useful, $2 AS votes_cool;

STORE json_user_row_2 INTO 'hdfs:///user/xiaomaogy/output/json_user_table' USING PigStorage('\u0001');
