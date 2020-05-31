-- this script has been written by @Joel BASSA , learner on Intellipaat Hadoop course 1st july 2017 batch 
-- Problem statement: what are the aired shows on ZOO,NOX,ABC channels? 

-- Instructions to run the script
--  put join2_genchan*.txt files in own directory hdfs ie. /ibm/data/genchan 
--  no need to have gennum files
-- run command hive -f <filename>.hql 

-- ************************start script****************************************************
-- create table with all genchan*.txt files as those files have same format
CREATE EXTERNAL TABLE IF NOT EXISTS genchan(show string, channel string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/ibm/data/genchan';

-- query
SELECT DISTINCT a.channel,a.show
FROM genchan a 
WHERE a.channel IN ('ZOO','NOX','ABC');

-- ************************end script****************************************************

