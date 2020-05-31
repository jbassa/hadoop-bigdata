-- this script has been written by @Joel BASSA , learner on Intellipaat Hadoop course 1st july 2017 batch 
-- Problem statement: what are the aired shows on ZOO, NOX, ABC channels? 

-- Instructions to run the script
--  put join2_genchan*.txt files in own directory hdfs ie. /ibm/data/genchan 
--  no need to have gennum files
-- run command pig -x local <filename>.pig or pig <filename>.pig

-- ************************start script****************************************************

-- load all join2_genchan*.txt at once
genchan = LOAD '/home/cloudera/join/genchan/' USING PigStorage(',') AS (show:chararray, channel:chararray);

-- filter on ZOO , NOX and ABC
onlyABC = FILTER genchan BY channel == 'ZOO' OR channel =='NOX' OR channel =='ABC';

-- take distinct values
onlyABCDistinct = DISTINCT onlyABC; 

-- order by channel
orderByChannel = ORDER onlyABCDistinct BY channel;

-- channel , show in that sequence
result = FOREACH orderByChannel GENERATE channel,show;

STORE result INTO '/home/cloudera/output4/' USING PigStorage(' ');

-- ************************end script****************************************************
