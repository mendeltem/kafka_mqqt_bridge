from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
import json
import os
import time
from pymongo import MongoClient
import paho.mqtt.client as mqtt
from joblib import Parallel, delayed
import numpy as np

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
        
config_client = mqtt.Client()
mqtt_broker_ip = 'localhost'
mqtt_broker_port = 1883


config_client.on_connect = on_connect_config
config_client.on_message = on_message_config


config_client.connect(mqtt_broker_ip, mqtt_broker_port,  60)
config_client.loop_start()
time.sleep(10)
config_client.loop_stop()

config_data["sensors"]



#Parallel(n_jobs=-1, verbose=10)(delayed(print_message)(
#                    sensor
#                    ) for sensor in config_data["sensors"])   
#    

kafka_topics = []        
        
for sensor in config_data["sensors"]:
    
    
    kafka_topic = ''.join([change(c, "/", "_") for c in sensor["mqtt-topic"]])
    
    kafka_topics.append(kafka_topic)
    
    
consumer = KafkaConsumer(kafka_topics[0],kafka_topics[1],kafka_topics[2],
                         group_id=None,
                         bootstrap_servers=['localhost:9092'])    

for message in consumer:
  
    print ("topic: ", message.topic)     
    record = json.loads(message.value)
       
    print("timestamp:", time.asctime(time.localtime(message.timestamp/1000.)))
    
    print("message", record)

    save = {
        'record'     : record,
        'Time'       : time.asctime(time.localtime(message.timestamp/1000.)) 
    }

    save_db.append((dict(save)))
    
    print("len of db: ",len(save_db))
    

#
#    inserted_id=posts.insert_one(save).inserted_id
#    print(inserted_id)


#db.test_collection.find_one({})

