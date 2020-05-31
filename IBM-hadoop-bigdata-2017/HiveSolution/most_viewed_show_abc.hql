-- this script has been written by @Joel BASSA , learner on Intellipaat Hadoop course 1st july 2017 batch 
-- Problem statement: what is the most viewed show on ABC channel? 

-- Instructions to run the script
--  put join2_genchan*.txt files in own directory hdfs ie. /ibm/data/genchan 
--  put join2_gennum*.txt files in own directory hdfs ie. /ibm/data/gennum 
-- run command hive -f <filename>.hql 

-- ************************start script****************************************************
-- create table with all genchan*.txt files as those files have same format
CREATE EXTERNAL TABLE IF NOT EXISTS genchan(show string, channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/ibm/data/genchan';

-- create table with all gennum*.txt files as those files have same format
CREATE EXTERNAL TABLE IF NOT EXISTS gennum(show string, count int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/ibm/data/gennum';

-- query
SELECT c.channel,c.show
FROM (SELECT a.channel, a.show,  sum(b.count) viewer_num 
FROM genchan a JOIN gennum b ON (a.show = b.show)
WHERE a.channel = 'ABC'
GROUP BY a.show, a.channel
ORDER BY viewer_num DESC
LIMIT 1) c;

-- ************************end script****************************************************

