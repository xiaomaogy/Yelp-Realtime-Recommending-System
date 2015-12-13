The Address to view the web interface:  http://104.197.20.219/cgi-bin/yg79/yelp/yelp.pl

--------------------------
Update Since Last Submit
--------------------------

[Batch Layer Modification for faster search]

My previous version was retrieving all of the records from Hbase, then filter out the records using the entered keyword. This approach is a very inefficient way.After reading a paper about the design of google's search engine database, I realize that I could do the same thing: create a inverted table.The table uses keyword as key and its value is an array of business_ids correlated with that keyword. 

I used inverted_table.pig to create this table. In this pig script I first extracted every word from the business records' name and category fields. Then I group by the words I extracted and associate with those records the business_id.

Initially I got an confusing bug using this approach, then I found that there are some keywords related with huge amount of business_id (LIKE 'I','and','me'). So I filtered away all the keyword shorter than 3 character.

After I get these records with keyword related with bunch of business_ids, I stored it into hdfs:///user/xiaomaogy/output/inverted_table_keyword_to_business_id, then as usual I loaded it into Hbase through Hive.

[Perl Script utilizing the inverted table]

In perl the search is now different. I first get the search keyword string from user, then split the keyword string by space and query every keyword
against the inverted_table, and get a list of business_ids, then I query the business table to get the business record related with each business_id.Then I filter all these business record based on their name and category, so that when I search for 'Japanese Restaurant', it would only show 'Japanese restaurant' related stuff instead of all the restaurants and all the japanese stuff. 

Also I make the web interface look better with the help of bootstrap template.

[Speed Layer]

1. Load data to Kafka.

I researched the Yelp's Api, but its rest Api was not able to return a lot of reviews based on a certain business name or id. Also I looked at its RSS feed, but it only returns 20 new reviews every 2 hour, and most of the records doesn't match the business name in my own business table(because my data was only a fraction of the business in Arizona)
So I had to create the streaming data by myself. I split the original review table into 5 parts using split_review.pig. Then I load all the records before 2010 as what I did before. 

YelpLoadToKafka is a maven project to load 2010-2013 data into kafka, you could maven install the project then run it using:
java -cp YelpLoadToKafka-0.0.1-SNAPSHOT.jar com.yangmao.LoadYelpToKafka

The default is to load the '/mnt/scratch/yg79/simulatedDataSource/review_2010_order/part-r-00000' file and put it into kafka, you could change the fileDir to load 2011, 2012, 2013 files by changing review_201*_order in the directory. I've only put the 2010 data in the kafka topic 'yuan_yelp_reviews'

2. Process the Data from Kafka and update the Hbase's business table's review count and Hbase's review table.

YelpKafkaHBase is a maven project to process the records in Kafka using Storm, it contains a KafkaSpout, a SplitFieldsBolt and a UpdateReviewToHbaseBolt. 

KafkaSpout simply retrieve the records from kafka.

SplitFieldsBolt split the records into different fields, like review_id, review_text, etc.

UpdateReviewToHbaseBolt does two things, it first add the new review records into the review table if that review_id doesn't exist before, then if it puts in a new review, it will increment the number of review_count_new corresponding to that review's business_id. Here since all the fields in review table is String, it will have a ERROR: org.apache.hadoop.hbase.DoNotRetryIOException: Attempted to increment field that isn't 64 bits wide.

I tried to solve this problem, first by changing the data type of review_count from int to BIGINT, but this didn't work.

So I made storm to increment a different field, the review_count_new field, so that Hbase could create this field within it, and within the Hbase it has the data type long. 

However I am having another problem here, the Hbase stores the long datatype as \x00\x00\x00 etc format, and I was not able to convert this back to a long number, so I just let the perl script print out the long bytes as it is, and you will see as the storm stream the data, the review_count_new will change to different symbols.

In this step, to run the code, simply type this after maven install:

storm jar uber-* com.yangmao.YelpTopology

And you will see the symbols change if you refresh the web interface in http://104.197.20.219/cgi-bin/yg79/yelp/yelp.pl, remember to load some new review data in the previous step, otherwise the Hbase's review_count wouldn't change.



----------------------------------
Update Since Last Submit Finished
----------------------------------

[Batch Layer]

---------------
Cleaning data:
---------------

I downloaded the yelp training data from https://www.kaggle.com/c/yelp-recruiting/data, which includes all of the user, business and review data from yelp in Arizona State.

Since the raw data's format was json, I used Pig's JsonLoader to parse all of the fields into pig, this approach worked with review and user data. For review it has a lot of '\n' and '\r' in its review_text field. Since JsonLoader use '\n' and '\r' to recognize each separate json record, these symbols in review_text would cause error. So I replaced all of these symbols with '*'.

However when it comes to business data, JsonLoader approach didn't quite work because business data have nested Json structure, for business data's categories field, it has an Array of categories under that field, and JsonLoader couldn't read that no matter what I tried. 

So in the end I chose to read the business data in Pig as a single string for each line, replacing all of the square brackets for all those array, then use Pig's REGEX_EXTRACT to parse each json field. 

After I parsed all of these data, I stored them using PigStorage format onto HDFS, here I used '\u0001' as the delimiter. Initially I used '|' as delimiter and turns out that in many of the review_text people also use '|' to talk about some restaurant, so some debugging happend here.

## In step I generated these data:

hdfs:///user/xiaomaogy/output/json_business_table
hdfs:///user/xiaomaogy/output/json_review_table
hdfs:///user/xiaomaogy/output/json_user_table

In the next step they will be moved to
hdfs:///apps/hive/warehouse/

# I put the source data in

hdfs:///user/xiaomaogy/input

# The Pig scripts I used in this step

parse_user_json.pig
parse_review_json.pig
parse_business_json.pig

-----------------------
Loading data to Hbase:
-----------------------

Initially I was planning to use Pig to directly store the data into Hbase as illustrated in the class. But since orc format in hive is said to be faster, I decided to give it a try. 

orc table in Hive cannot be loaded with data directly using LOAD DATA, so I had to create three non_orc hive tables:

yuan_yelp_business_not_orc
yuan_yelp_review_not_orc
yuan_yelp_user_not_orc

Delimiter field is set to '\001', note here that '\u0001' doesn't seem to work. 

Then use Insert Into to load the data into three orc hive tables;

yuan_yelp_review
yuan_yelp_business
yuan_yelp_user


Later I found that there is a way to create a table that can be shared between hive and Hbase, using org.apache.hadoop.hive.hbase.HBaseStorageHandler, so I deleted all the orc tables and created three synchronized hive tables:

yuan_yelp_business_hbase_sync
yuan_yelp_review_hbase_sync
yuan_yelp_user_hbase_sync

[Serving Layer]

---------------------
Precalculating table
---------------------

Usually people who makes more reviews are considered more reliable. So I think of counting the review made only by these active users, and compare it to the total review count made by all users.

#Here I used review count >= 10 to distinguish active user. 

In calculate_business_review_count.pig, I calculated the review_count for each business (active-user vs. all-user), here they are still in their own rows, I stored them in temp1 and temp2 tables. 

In combine_review_count_to_business.pig, I loaded the temp1 and temp2 tables, then join all there tables together, so that for each business_id, we have three review counts, the review_count given by the source data(review_count_recorded), the review_count calculated based on active_users(review_count_active_user) , and the review_count calculated based on all_users(review_count_all_user). 

It turns out that the review_count given by the data source doesn't match the actual calculated data. 

After this I loaded the new business table into 

yuan_yelp_business_wrc_hbase_sync


----------
Perl CGI
----------

#see yelp.pl for source code of the script

Here the interface first detect if a keyword parameter is present, if not then it will wait until you enters one. When a keyword is present, it will first pull all of the data in the hbase table yuan_yelp_business_wrc_hbase_sync, then it will filter all the records by matching the keyword with all the records' name and category field. 

I know that getting all the records is not very good here(there are 10k records in that table). However in the $hbase->get method there is only a where => keystartswith method, which is not sufficient here. I am not sure what is the best solution here. 

Anyway this is the project on Thursday, Dec 3rd, and I will add the speed layer in the next few days. 



