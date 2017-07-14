# Sale_analysis_Recommendation_Insight
A dashboard app to show the sale analysis and recommendation results.  
[Demo Slide](http://goo.gl/nmcbR4)  
[Demo Site](http://saleinsight.site) 
[Demo Video](https://youtu.be/z0aIcaDKFn0)



## RealTime analysis
### 1. Kafka
### 2. Spark Streaming
### 3. Cassandra

## Batch Processing 
### 1. HDFS
### 2. Hadoop MapReduce

## Data Pipeline
Reference-style: 
![alt text][logo]

[logo]: https://github.com/zkz917/Sale_analysis_Recommendation_Insight/blob/master/image/data.png

### MapReduce Job
cd src

hdfs dfs -rm -r /dataDividedByUser

hdfs dfs -rm -r /coOccurrenceMatrix

hdfs dfs -rm -r /Normalize

hdfs dfs -rm -r /Multiplication

hdfs dfs -rm -r /Sum

cd src/main/java/

hadoop com.sun.tools.javac.Main *.java

jar cf recommender.jar *.class

hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /Normalize /Multiplication /Sum

hdfs dfs -cat /Sum/*

#args0: original dataset

#args1: output directory for DividerByUser job

#args2: output directory for coOccurrenceMatrixBuilder job

#args3: output directory for Normalize job

#args4: output directory for Multiplication job

#args5: output directory for Sum job
