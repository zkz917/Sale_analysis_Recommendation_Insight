from __future__ import print_function
import sys
import time
from kafka import KafkaConsumer
import os

class Consumer(object):
    def __init__(self, bootstrap_servers, topic):
        #Initialize Consumer with kafka broker IP and topic.
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.hdfs_path = '/user'
        self.consumer = KafkaConsumer(bootstrap_servers = bootstrap_servers)
        self.consumer.subscribe([topic])
        self.block_cnt = 0
        
    def consume_topic(self, output_dir):
        # set up the consumertopic 
        timestamp = time.strftime('%Y%m%d%H%M%S')
        # open file for writing
        self.temp_file_path = "%s/kafka_%s_%s.dat" % (output_dir,
                                                         self.topic,
                                                         timestamp)
        self.temp_file = open(self.temp_file_path,"w")
        
        messageCount = 0
        for message in self.consumer:
            messageCount += 1
            self.temp_file.write(message.value + "\n")
            if messageCount % 1000 == 0:
                # when the file size is 20M then flush the file to HDFS
                if self.temp_file.tell() > 20000000:
                    self.flush_to_hdfs(output_dir)

    def flush_to_hdfs(self, output_dir):
        # store the file in hdfs
        self.temp_file.close()

        timestamp = time.strftime('%Y%m%d%H%M%S')

        hadoop_fullpath = "%s/%s_%s.dat" % (self.hdfs_path, 
                                               self.topic, 
                                               timestamp)

        self.block_cnt += 1

        # place blocked messages into history and cached folders on hdfs
        os.system("hdfs dfs -put %s %s" % (self.temp_file_path,
                                                hadoop_fullpath))
        
        os.remove(self.temp_file_path)

        timestamp = time.strftime('%Y%m%d%H%M%S')

        self.temp_file_path = "%s/kafka_%s_%s.dat" % (output_dir,
                                                         self.topic,
                                                         timestamp)
        self.temp_file = open(self.temp_file_path, "w")

        
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: consumer-hdfs <bootstrap_servers>", file=sys.stderr)
        exit(-1)
    print "\nConsuming messages..."
    cons = Consumer(bootstrap_servers=sys.argv[1], 
                    topic="salestream")
    cons.consume_topic("temp")