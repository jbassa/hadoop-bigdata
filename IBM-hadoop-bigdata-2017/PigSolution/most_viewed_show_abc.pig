-- this script has been written by @Joel BASSA , learner on Intellipaat Hadoop course 1st july 2017 batch 
-- Problem statement: what is the  most viewed show on ABC? 

-- Instructions to run the script
--  put join2_genchan*.txt files in own directory hdfs ie. /ibm/data/genchan 
--  put join2_gennum*.txt files in own directory hdfs ie. /ibm/data/gennum 
-- run command pig -x local <filename>.pig or pig <filename>.pig

-- ************************start script****************************************************

-- load all join2_genchan*.txt at once
genchan = LOAD '/home/cloudera/join/genchan/' USING PigStorage(',') AS (show:chararray, channel:chararray);

--load all join2_gennum*.txt at once
gennum = LOAD '/home/cloudera/join/gennum/' USING PigStorage(',') AS (show:chararray, count:int);

-- filter on ABC channel
onlyABC = FILTER genchan BY channel == 'ABC';

-- joining
onlyABCJoinNum = JOIN onlyABC BY show, gennum BY show;

-- group by channel and show
groupByShow = GROUP onlyABCJoinNum by (onlyABC::channel,onlyABC::show);

-- aggregation
totalViewersByShow = FOREACH groupByShow GENERATE group, onlyABCJoinNum.onlyABC::channel ,SUM(onlyABCJoinNum.gennum::count) as sum;

-- order by desc
totalOrderByDesc = ORDER totalViewersByShow BY sum DESC;

-- take first item
totalFirstItem = LIMIT totalOrderByDesc 1;

-- take only channel & show
result = FOREACH totalFirstItem GENERATE group.channel,group.show;

STORE result INTO '/home/cloudera/output3/' USING PigStorage(' ');

-- ************************end script****************************************************
