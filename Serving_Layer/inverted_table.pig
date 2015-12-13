business_row = LOAD 'hdfs:///apps/hive/warehouse/yuan_yelp_business_not_orc/part-m-00000' USING PigStorage('\u0001')
as (business_id:chararray, name:chararray, categories:chararray, review_count_recorded:int, stars:double, open:boolean, 
    full_address:chararray, city:chararray, state:chararray,
    longitude:chararray, latitude:chararray);

business_row_category_keywords_raw = FOREACH business_row GENERATE FLATTEN(TOKENIZE(categories,' #&()')) AS keyword, business_id;

business_row_category_keywords = FILTER business_row_category_keywords_raw BY (int)SIZE(keyword) > 3;

business_row_name_keywords_raw = FOREACH business_row GENERATE FLATTEN(TOKENIZE(name,' ')) AS keyword, business_id;

business_row_name_keywords = FILTER business_row_name_keywords_raw BY (int)SIZE(keyword) > 3;

business_row_keywords_raw = UNION business_row_category_keywords, business_row_name_keywords;

business_row_keywords = FOREACH business_row_keywords_raw GENERATE LOWER(keyword) AS keyword, business_id;

keywords_business_id_group_raw = GROUP business_row_keywords BY keyword;

keywords_business_id_group = FOREACH keywords_business_id_group_raw GENERATE group AS keyword, BagToTuple(business_row_keywords.business_id);

STORE keywords_business_id_group INTO 'hdfs:///user/xiaomaogy/output/inverted_table_keyword_to_business_id' USING PigStorage('\u0001');