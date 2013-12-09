REGISTER /usr/lib/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.jar
REGISTER /usr/lib/hbase/hbase-0.94.13/hbase-0.94.13.jar

set hbase.zookeeper.quorum 'ec2-54-220-125-34.eu-west-1.compute.amazonaws.com';

data = LOAD 'hbase://ratings' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf1:*', '-loadKey true -limit 5') as (id:CHARARRAY, rates:MAP[]);
 
counts = FOREACH data GENERATE id, rates;

DUMP counts;
