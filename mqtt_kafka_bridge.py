#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan  6 11:56:37 2020

@author: panda
"""
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError
import msgpack
import json
import os

kafka_broker_ip = 'localhost'
kafka_broker_port = 9092
mqtt_broker_ip = 'localhost'
mqtt_broker_port = 1883


def change(f , char1, char2):
    """replace a character with another if containes
    
    return character
    """
    return char2 if f == char1 else f


def connect_kafka_producer(ip ='localhost:9092',  type_of_message = 1):
    _producer = None
    try:
        if type_of_message == 1:
            _producer = KafkaProducer(bootstrap_servers=[ip],
                                value_serializer=msgpack.dumps)
        if type_of_message == 2:
            _producer = KafkaProducer(bootstrap_servers=[ip],
              value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            
        
        connect_kafka_producer
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def send_message_to_kafka(mqtttopic, message):
    """
    Send message to kafka consumer
    """
    os.system('clear')
    print("Mqtt Topic: " , mqtttopic)
    
    kafka_topic = ''.join([change(c, "/", "_") for c in mqtttopic])
    
    print("Kafka Topic", kafka_topic)
    
    print("Message: " , message.decode("utf-8") )
    

    kafka_message = message.decode("utf-8")
    
    
    if "temperatur" in kafka_topic:
    
        producer.send(kafka_topic, {'Temperatur': kafka_message,
                                    'Standort':  "Berlin"
                                    })

    
    if "brightness" in kafka_topic:
    
        producer.send(kafka_topic, {'Helligkeit': kafka_message,
                                    'Standort':  "Berlin"
                                    })
        
        
    if "news" in kafka_topic:
    
        producer.send(kafka_topic, {'Messages': kafka_message,
                                    #config 
                                    'Standort':  "Berlin"
                                    })    
    
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # von config
    client.subscribe("#")
                                        
def on_message(client, userdata, msg):
    
    send_message_to_kafka(msg.topic, msg.payload)
    
if __name__ == "__main__":

    producer = connect_kafka_producer(kafka_broker_ip + ":" + \
                                      str(kafka_broker_port), 2)
    
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message    
    #zertifikat
    client.connect(mqtt_broker_ip, 1, 60)
    
    client.loop_forever()
    
    
    
#mqtttopic = topic
#    
#kafka_topic = ''.join([change(c, "/", "_") for c in mqtttopic])
#    
#producer.send(kafka_topic, {'key': "mendel"})    
