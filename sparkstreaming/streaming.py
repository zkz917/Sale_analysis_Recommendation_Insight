import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils

# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(rdd):
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        df = spark.read.json(rdd)

        # Creates a temporary view using the DataFrame
        df.createOrReplaceTempView("insight")

        age_sqlDF = spark.sql("SELECT age, count(*) as count, max(timestamp) as timestamp FROM insight group by age")
        
        state_sqlDF = spark.sql("SELECT state, count(*) as count, max(timestamp) as timestamp FROM insight group by state")
        
        age_sqlDF.write \
             .format("org.apache.spark.sql.cassandra") \
             .mode('append') \
             .options(table="realtimeage", keyspace="insight") \
             .save()

        state_sqlDF.write \
             .format("org.apache.spark.sql.cassandra") \
             .mode('append') \
             .options(table="realtimestate", keyspace="insight") \
             .save()
        
    except:
        pass

def main():
    if len(sys.argv) != 2:
        print("Usage: streaming <bootstrap.servers>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="insight")
    # process the data every 3 seconds with microbatch
    ssc = StreamingContext(sc, 3)
    # get stream data from kafka
    kafkaStream = KafkaUtils.createDirectStream(ssc, 
                                                ["salestream"], 
                                                {"bootstrap.servers": sys.argv[1]})
    
    lines = kafkaStream.map(lambda x: x[1])
    
    lines.foreachRDD(process)
    
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()

    
