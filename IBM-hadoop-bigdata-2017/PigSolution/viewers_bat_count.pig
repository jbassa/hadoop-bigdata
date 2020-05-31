-- this script has been written by @Joel BASSA , learner on Intellipaat Hadoop course 1st july 2017 batch 
-- Problem statement: what is the  number of viewers for the BAT channel? 

-- Instructions to run the script
--  put join2_genchan*.txt files in own directory hdfs ie. /ibm/data/genchan 
--  put join2_gennum*.txt files in own directory hdfs ie. /ibm/data/gennum 
-- run command pig -x local <filename>.pig or pig <filename>.pig

-- ************************start script****************************************************

-- load all join2_genchan*.txt at once
genchan = LOAD '/home/cloudera/join/genchan/' USING PigStorage(',') AS (show:chararray, channel:chararray);

--load all join2_gennum*.txt at once
gennum = LOAD '/home/cloudera/join/gennum/' USING PigStorage(',') AS (show:chararray, count:int);

-- filter  on BAT channel
onlyBAT = FILTER genchan BY channel == 'BAT';

-- joining
onlyBATJoinNum = JOIN onlyBAT BY show, gennum BY show;

-- group by channel
groupByBAT = GROUP onlyBATJoinNum by onlyBAT::channel;

-- aggregation
totalViewers = FOREACH groupByBAT GENERATE group,  SUM(onlyBATJoinNum.gennum::count);


STORE totalViewers INTO '/home/cloudera/output2/' USING PigStorage(' ');

-- ************************end script****************************************************
