
DROP TABLE rats;

CREATE EXTERNAL TABLE rats(frequency INT, time INT, convolution INT)
PARTITIONED BY(rat STRING, dt STRING, channel STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS SEQUENCEFILE LOCATION '/home/ashish/workspace/NeuroHadoop/build/output';

ALTER TABLE rats ADD PARTITION(rat='R048',dt='2004-07-15',channel='11d');
ALTER TABLE rats ADD PARTITION(rat='R187',dt='2009-11-08',channel='6a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='1a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='2a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='3a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='4a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='5a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='6a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='7a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='8a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='9a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='10a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='11a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='12a');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='r1');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='r1r2');
ALTER TABLE rats ADD PARTITION(rat='R192',dt='2009-11-19',channel='r2');

DROP TABLE ratsaverage;

CREATE TABLE ratsaverage(frequency INT, time INT, convolution INT)
PARTITIONED BY(rat STRING, dt STRING, channel STRING)
LOCATION '/home/ashish/workspace/NeuroHadoop/hive'
;

ALTER TABLE ratsaverage ADD PARTITION(rat='R192',dt='2009-11-19',channel='avg');
ALTER TABLE ratsaverage ADD PARTITION(rat='R192',dt='2009-11-19',channel='r1');
ALTER TABLE ratsaverage ADD PARTITION(rat='R192',dt='2009-11-19',channel='r1r2');
ALTER TABLE ratsaverage ADD PARTITION(rat='R192',dt='2009-11-19',channel='r2');

INSERT OVERWRITE TABLE ratsaverage PARTITION (rat='R192', dt='2009-11-19', channel='avg')
SELECT frequency, time, AVG(convolution)
FROM rats 
WHERE rat='R192' 
AND dt='2009-11-19'
AND NOT(channel LIKE '%r%')
GROUP BY time, frequency
;

INSERT OVERWRITE TABLE ratsaverage PARTITION (rat='R192', dt='2009-11-19', channel='r1')
SELECT frequency, time, convolution
FROM rats 
WHERE rat='R192' 
AND dt='2009-11-19'
AND channel='r1'
;

INSERT OVERWRITE TABLE ratsaverage PARTITION (rat='R192', dt='2009-11-19', channel='r1r2')
SELECT frequency, time, convolution
FROM rats 
WHERE rat='R192' 
AND dt='2009-11-19'
AND channel='r1r2'
;

INSERT OVERWRITE TABLE ratsaverage PARTITION (rat='R192', dt='2009-11-19', channel='r2')
SELECT frequency, time, convolution
FROM rats 
WHERE rat='R192' 
AND dt='2009-11-19'
AND channel='r2'
;
