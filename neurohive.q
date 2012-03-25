
CREATE EXTERNAL TABLE rats(time INT, frequency INT, convolution INT)
PARTITIONED BY(rat STRING, dt STRING, channel STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS SEQUENCEFILE LOCATION '/home/ashish/NeuroHadoop/build/output';

ALTER TABLE rats ADD PARTITION(rat='R048',dt='2004-07-15',channel='11d');
ALTER TABLE rats ADD PARTITION(rat='R187',dt='2009-11-08',channel='6a');
