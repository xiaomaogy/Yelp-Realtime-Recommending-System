json_business_raw_row = LOAD 'hdfs:///user/xiaomaogy/input/yelp_training_set_business.json' as (str:chararray);

json_business_raw_row_2 = FOREACH json_business_raw_row GENERATE REPLACE(str, '\\"neighborhoods\\"\\:\\s\\[\\],\\s','') AS str;

json_business_raw_row_3 = FOREACH json_business_raw_row_2 GENERATE  REPLACE(REPLACE(REGEX_EXTRACT(str, '\\"categories\\"\\:\\s\\[(.*?)\\]', 1), '\\"', ''),',','#') AS categories , REPLACE(str, '\\"categories\\"\\:\\s\\[(.*?)\\],', '') AS str;

json_business_row = FOREACH json_business_raw_row_3 GENERATE 
    REGEX_EXTRACT(str, '\\"business_id\\"\\:\\s\\"(.*?)\\"', 1) AS business_id, 
    REGEX_EXTRACT(str, '\\"name\\"\\:\\s\\"(.*?)\\"', 1) AS name, 
    categories, 
    REGEX_EXTRACT(str, '\\"review_count\\"\\:\\s(.*?),', 1) AS review_count, 
    REGEX_EXTRACT(str, '\\"stars\\"\\:\\s(.*?),', 1) AS stars,
    REGEX_EXTRACT(str, '\\"open\\"\\:\\s(.*?),', 1) AS open,
    REPLACE(REPLACE(REGEX_EXTRACT(str, '\\"full_address\\"\\:\\s\\"(.*?)\\"', 1),'\\\\n','*'),'\\\\r','*') AS full_address,
    REGEX_EXTRACT(str, '\\"city\\"\\:\\s\\"(.*?)\\"', 1) AS city,
    REGEX_EXTRACT(str, '\\"state\\"\\:\\s\\"(.*?)\\"', 1) AS state,
    REGEX_EXTRACT(str, '\\"longitude\\"\\:\\s(.*?),', 1) AS longitude,
    REGEX_EXTRACT(str, '\\"latitude\\"\\:\\s(.*?),', 1) AS latitude;

STORE json_business_row INTO 'hdfs:///user/xiaomaogy/output/json_business_table' USING PigStorage('\u0001');

-- Sample data
-- {"business_id": "rncjoVoEFUJGCUoC1JgnUA", "full_address": "8466 W Peoria Ave\nSte 6\nPeoria, AZ 85345", "open": true, 
-- "categories": ["Accountants", "Professional Services", "Tax Services", "Financial Services"], "city": "Peoria", "review_count": 3, 
-- "name": "Peoria Income Tax Service", "neighborhoods": [], "longitude": -112.241596, "state": "AZ", "stars": 5.0, "latitude": 33.581867000000003, 
-- "type": "business"}
