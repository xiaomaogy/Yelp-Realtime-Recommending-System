review_raw = LOAD 'hdfs:///apps/hive/warehouse/yuan_yelp_review_not_orc' USING PigStorage('\u0001') 
    AS (review_id:chararray, 
        business_id:chararray, 
        user_id:chararray,
        review_stars:int, 
        review_date:chararray, 
        review_text:chararray,
        type:chararray, 
        votes_funny:int, 
        votes_useful:int,
        votes_cool:int); 

--Here the Pig's Split function doesn't work, so I had to use this really inefficient way to split data

review_before_2010 = FILTER review_raw BY review_date MATCHES '200.*';
review_2010 = FILTER review_raw BY review_date MATCHES '2010.*';
review_2011 = FILTER review_raw BY review_date MATCHES '2011.*';
review_2012 = FILTER review_raw BY review_date MATCHES '2012.*';
review_2013 = FILTER review_raw BY review_date MATCHES '2013.*';
review_2010_order = ORDER review_2010 BY review_date ASC;
review_2011_order = ORDER review_2011 BY review_date ASC;
review_2012_order = ORDER review_2012 BY review_date ASC;
review_2013_order = ORDER review_2013 BY review_date ASC;

STORE review_before_2010 INTO 'hdfs:///user/xiaomaogy/output/review_before_2010' USING PigStorage('\u0001');
STORE review_2010_order INTO 'hdfs:///user/xiaomaogy/output/review_2010_order' USING PigStorage('\u0001');
STORE review_2011_order INTO 'hdfs:///user/xiaomaogy/output/review_2011_order' USING PigStorage('\u0001');
STORE review_2012_order INTO 'hdfs:///user/xiaomaogy/output/review_2012_order' USING PigStorage('\u0001');
STORE review_2013_order INTO 'hdfs:///user/xiaomaogy/output/review_2013_order' USING PigStorage('\u0001');
