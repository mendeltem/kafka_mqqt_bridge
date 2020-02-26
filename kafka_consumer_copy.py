from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
import json
import os
import time
from pymongo import MongoClient
import paho.mqtt.client as mqtt
from joblib import Parallel, delayed
import numpy as np
import pprint
import heapq

import flask



#mongo_client = MongoClient(port=27017)
#
#import pymongo
#from pymongo import MongoClient, ReturnDocument
#dbclient = MongoClient('mongodb://localhost:27017/')

#db=mongo_client.database
# To consume latest messages and auto-commit offsets
#kafka_topic = "sensor_illuminance_bright1_DEVICE1-8A12-4F4F-8F69-6B8F3C2E78DD"

config_data = []
save_db = []
#kafka_topic2 = "sensor_brightness"
class streamMedian:
    def __init__(self):
        self.minHeap, self.maxHeap = [], []
        self.cumulative_sum = 0.0             # new instance variable
        self.N=0


    def insert(self, num):
        if self.N%2==0:
            heapq.heappush(self.maxHeap, -1*num)
            self.N+=1
            if len(self.minHeap)==0:
                return
            if -1*self.maxHeap[0]>self.minHeap[0]:
                toMin=-1*heapq.heappop(self.maxHeap)
                toMax=heapq.heappop(self.minHeap)
                heapq.heappush(self.maxHeap, -1*toMax)
                heapq.heappush(self.minHeap, toMin)
        else:
            toMin=-1*heapq.heappushpop(self.maxHeap, -1*num)
            heapq.heappush(self.minHeap, toMin)
            self.N+=1

    # median code...
    def getMean(self):
        total = 0
        for num in self.maxHeap:
            total -= num
        for num in self.minHeap:
            total += num 
        return total/self.N
    

def getAvg(prev_avg, new_value, n): 
    """streaming average"""
    return ((prev_avg * n + new_value) / (n + 1)); 


def change(f , char1, char2):
    """replace a character with another if containes
    
    return character
    """
    return char2 if f == char1 else f


def on_connect_config(client, userdata, flags, rc):
    
    if rc==0:
        print("Connected with result code " + str(rc))
        # von config
        client.subscribe("config/#")
    else:
        print("Bad connection Returned code=",rc)
#consumer.seek_to_end()
#posts = db.test_collection
def on_message_config(client, userdata, msg):
    global topics, config_data  
    m_decode=str(msg.payload.decode("utf-8","ignore"))
#    print("data Received type",type(m_decode))
#    print("data Received",m_decode)
#    print("Converting from Json to Object")
    config_data=json.loads(m_decode) #decode json data        
      
    

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])      
config_client = mqtt.Client()
mqtt_broker_ip = 'localhost'
mqtt_broker_port = 1883


config_client.on_connect = on_connect_config
config_client.on_message = on_message_config


config_client.connect(mqtt_broker_ip, mqtt_broker_port,  60)
config_client.loop_start()
time.sleep(10)
config_client.loop_stop()


#Parallel(n_jobs=-1, verbose=10)(delayed(print_message)(
#                    sensor
#                    ) for sensor in config_data["sensors"])   
#    

kafka_topics = []        
kafka_units  = []


conf = {}


# get the information from config         
for sensor in config_data["sensors"]:
    
    
    kafka_topic = ''.join([change(c, "/", "_") for c in sensor["mqtt-topic"]])
    kafka_unit = ''.join([change(c, "/", "_") for c in sensor["units"]])
    
    kafka_topics.append(kafka_topic)
    conf[kafka_topic] = kafka_unit

#for location in config_data["location"]:
#    print(location)
#    
    
#toupe ** fuer dynamic   
#consumer = KafkaConsumer(kafka_topics[0],kafka_topics[1],kafka_topics[2],
#                         group_id=None,
#                         bootstrap_servers=['localhost:9092'])    


 
consumer.subscribe(kafka_topics)

values_counter = {}
avg_values     = {}
median_object  = {}
media_values   = {}
min_values     = {}
max_values     = {}

all_info       = {}

values         = {}

for i,message in enumerate(consumer):
    
    os.system('clear')
    
    print("last incoming message")
  
    print ("topic: ", message.topic)     
    record = json.loads(message.value)
    
    time_of_record = time.asctime(time.localtime(message.timestamp/1000.))
       
    print("timestamp:", time_of_record)
    
    value = record["value"]
    
    print("value", value)
    
    values.setdefault(message.topic, 0)
    values[message.topic] = round(float(value), 2)
    
    values_counter.setdefault(message.topic, 0)
    values_counter[message.topic] += 1    
    
    median_object.setdefault(message.topic, streamMedian())
    median_object[message.topic].insert(values[message.topic])
    
    media_values.setdefault(message.topic, 0)
    media_values[message.topic] = round(median_object[message.topic].getMean(),
                                        2)
    
    min_values.setdefault(message.topic, 0)
    
    #firt message 
    if values_counter[message.topic] == 1:
        min_values[message.topic] = values[message.topic]
        
    if min_values[message.topic] > values[message.topic]: 
        min_values[message.topic] = values[message.topic]
    
    max_values.setdefault(message.topic, 0)
    if max_values[message.topic] < values[message.topic]: 
        max_values[message.topic] = values[message.topic]   
    

    avg_values.setdefault(message.topic, 0)
    avg = getAvg(avg_values[message.topic], 
                 values[message.topic], 
                 values_counter[message.topic])
    
    avg_values[message.topic] = round(avg, 2)
    
    
    all_info.setdefault(message.topic, {})
    all_info[message.topic] = {"average value:"  : avg_values[message.topic] , 
                               "median value:"   : media_values[message.topic], 
                               "unit:"           : conf[message.topic],
                               "timestamp:"      : time_of_record,
                               "min:"            : min_values[message.topic],
                               "max:"            : max_values[message.topic],
                               "location:"       : config_data["location"]
                              }
    
    pp = pprint.PrettyPrinter(indent=1)
    
    print("saved mean from stream")
    pp.pprint(all_info)

    #if eventhandler find new config message break the loop 
    
    
    
    
    
    
    

