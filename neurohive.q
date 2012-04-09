DROP TABLE rats;
DROP TABLE ratsaverage;

CREATE EXTERNAL TABLE rats(time INT, frequency INT, convolution INT)
PARTITIONED BY(rat STRING, dt STRING, channel STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS SEQUENCEFILE LOCATION '/neuro/output';

CREATE TABLE ratsaverage(time INT, frequency INT, convolution INT)
PARTITIONED BY(rat STRING, dt STRING, channel STRING)
LOCATION '/neuro/hive';

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

ALTER TABLE ratsaverage ADD PARTITION(rat='R192',dt='2009-11-19',channel='avg');
ALTER TABLE ratsaverage ADD PARTITION(rat='R192',dt='2009-11-19',channel='r1');
ALTER TABLE ratsaverage ADD PARTITION(rat='R192',dt='2009-11-19',channel='r1r2');
ALTER TABLE ratsaverage ADD PARTITION(rat='R192',dt='2009-11-19',channel='r2');

INSERT OVERWRITE TABLE ratsaverage PARTITION (rat='R192', dt='2009-11-19', channel='avg')
SELECT time, frequency, AVG(convolution)
FROM rats
WHERE rat='R192'
AND dt='2009-11-19'
AND NOT(channel LIKE '%r%')
GROUP BY time, frequency
;

INSERT OVERWRITE TABLE ratsaverage PARTITION (rat='R192', dt='2009-11-19', channel='r1')
SELECT time, frequency, convolution
FROM rats
WHERE rat='R192'
AND dt='2009-11-19'
AND channel='r1'
;

INSERT OVERWRITE TABLE ratsaverage PARTITION (rat='R192', dt='2009-11-19', channel='r1r2')
SELECT time, frequency, convolution
FROM rats
WHERE rat='R192'
AND dt='2009-11-19'
AND channel='r1r2'
;

INSERT OVERWRITE TABLE ratsaverage PARTITION (rat='R192', dt='2009-11-19', channel='r2')
SELECT time, frequency, convolution
FROM rats
WHERE rat='R192'
AND dt='2009-11-19'
AND channel='r2'
;
