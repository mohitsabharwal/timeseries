# Timeseries POC using Kafka - Spark Streaming - Parquet

# Conditions/Constraints
* Spark version: 2.1
* The PoC should handle the timeseries ingested via Kafka using Spark Streaming API. 
* Spark Streaming application listen to a Kafka topics, parses/validate timeseries input from
topic, serializes the micro-batch results into Parquet file(s).

# Configuration
The configuration file format is pretty self-explanatory. It allows user to specify all needed Spark, 
Kafka and Zookeeper properties. 

For example:
```
application.name=timeseries
application.batch.milliseconds=20000
application.executors=2
application.executor.cores=2
application.executor.memory=2g
kafka.group.id=group1
kafka.topics=[stocks,bonds] // multiple topics consumed by same dstream
kafka.brokers="REPLACE1:9092,REPLACE1:9092,REPLACE1:9092"
kafka.encoding=string
zk.connection="REPLACE1:2181,REPLACE1:2181,REPLACE1:2181"
zk.field.names=[group_id,topic,partition,offset] 
zk.field.types=[string,string,int,long]
zk.key.field.names=[group_id,topic,partition]
zk.znode.prefix="/timeseries"
zk.session.timeout.millis=1000
output.file="/tmp/foo"   // output directory to store parquet files generated by micro-batches
```

# Usage
The usage is the standard spark-submit command line:
`spark2-submit <jar> <config file>`

For example: If you run this application in YARN cluster mode and specify a 
config file location as `/tmp/ts.conf` and custom log4j.properties file, 
the command line will look something like:

```
SPARK_KAFKA_VERSION=0.10  spark2-submit --master yarn --deploy-mode cluster \
  --files /tmp/log4j.properties#log4j.properties,/tmp/ts.conf#ts.conf \
  --conf spark.executor.extraJavaOptions='-Dlog4j.configuration=file:log4j.properties' \
  --conf spark.driver.extraJavaOptions='-Dlog4j.configuration=file:log4j.properties' \
  --conf spark.dynamicAllocation.enabled=false \
  --class com.cloudera.mohit.timeseries.Timeseries timeseries-1.0-SNAPSHOT.jar /tmp/ts.conf
```