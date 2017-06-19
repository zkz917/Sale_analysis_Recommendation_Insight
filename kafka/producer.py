from kafka import KafkaProducer
import sys
import time
import json
from datetime import datetime
from random import randint


class Producer(object):
    def run(self, bootstrap_servers):
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        for msg in range(100000000):
            message = {
                        'user_id': str(randint(1,10000000)),
                        'product_id': str(randint(1,100000)),
                        'action': randint(0,3),
                        'gender': randint(0,2),
                        'state': randint(0,50),
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')      
                      }

            message = json.dumps(message, encoding='utf-8') 
            producer.send('salestream', message)
            time.sleep(0.01)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: producer <bootstrap_servers>")
        exit(-1)
    prod = Producer()
    prod.run(sys.argv[0], sys.argv[1])

