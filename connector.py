#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 06:11:28 2020

@author: panda
"""

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError
import msgpack
import json
import os
import time



kafka_broker_ip = 'localhost'
kafka_broker_port = 9092
mqtt_broker_ip = 'localhost'
mqtt_broker_port = 1883

config_data = []
topics = []

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
    
    print("Kafka-MQTT Connector")
    
    print("Mqtt Topic: " , mqtttopic)
    
    kafka_topic = ''.join([change(c, "/", "_") for c in mqtttopic])
    
    print("Kafka Topic", kafka_topic)
    
    kafka_message = message.decode("utf-8")
    
    print("Value:",kafka_message)
    
    for sensor in config_data["sensors"]:
              
        kafka_topic = ''.join([change(c, "/", "_") for c in sensor["mqtt-topic"]])
    
        producer.send(kafka_topic, {sensor["units"]: kafka_message,
                                'Standort':  config_data["location"]
                                })
    
#def on_message_msgs(mosq, obj, msg):
#    # This callback will only be called for messages with topics that match
#    # $SYS/broker/messages/#
#    print("MESSAGES: " + msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
#    

def on_connect_config(client, userdata, flags, rc):
    
    if rc==0:
        print("Connected with config result code " + str(rc))
        # von config
        client.subscribe("kafka/config")
    else:
        print("Bad connection Returned code=",rc)

def on_connect(client, userdata, flags, rc):
    
    if rc==0:
        print("Connected with message result code " + str(rc))
        # von config
        message_client.subscribe(topics) 
    else:
        print("Bad connection Returned code=",rc)

def on_message(client, userdata, msg):
    time.sleep(1)
#    os.system('clear')
    topic=msg.topic
#    print("Sensor Mqtt Topics",topic)
    
    #m_decode=str(msg.payload.decode("utf-8","ignore"))
#    print('message',m_decode)
    send_message_to_kafka(topic, msg.payload)
        
    
def on_message_config(client, userdata, msg):
    global topics, config_data  
#    os.system('clear')
    m_decode=str(msg.payload.decode("utf-8","ignore"))
#    print("data Received type",type(m_decode))
#    print("data Received",m_decode)
#    print("Converting from Json to Object")
    config_data=json.loads(m_decode) #decode json data
#    print(type(m_in))
#    print("Config Topic", msg.topic)
#    
#    print("device-uuid = ",m_in["device-uuid"])
#    print("location = ",m_in["location"])
#    print("sensors = ",m_in["sensors"])

     
def connect_mqtt_borker_config(mqtt_broker_ip,mqtt_broker_port, delay = 60):
    
    client = mqtt.Client()

    try:
        
        client.on_connect = on_connect_config
        client.on_message = on_message_config    
        client.connect(mqtt_broker_ip, mqtt_broker_port, delay)
        
    except Exception as ex:
        print('couldnt connect to mqtt broker. Maybe wrong port?')
        print(str(ex))
    return client     
 

if __name__ == "__main__":
    
    
    producer = connect_kafka_producer(kafka_broker_ip + ":" + \
                                      str(kafka_broker_port), 2)
    
    config_client = mqtt.Client()
    message_client = mqtt.Client()
    
    config_client.on_connect = on_connect_config
    config_client.on_message = on_message_config
    
    message_client.on_connect = on_connect
    message_client.on_message = on_message
    
    
    try:
        config_client.connect(mqtt_broker_ip, mqtt_broker_port,  60)
        message_client.connect(mqtt_broker_ip, mqtt_broker_port, 60)
    
    except ValueError:
        pass

    
    #config_client.subscribe("config/#")
                            
    config_client.loop_start()
    time.sleep(15)
    config_client.loop_stop()
#    
    while True:
        try:
            sensors = config_data["sensors"]   
            topics = []
            
            for sensor in sensors:       
                topics.append((sensor['mqtt-topic'],0))
        
            time.sleep(3)
            message_client.loop_start()
    
        except ValueError:
            pass
#

#message_client.loop_stop()
    
#    
#cart = mqtt.Client()
#mobile = mqtt.Client()
#elevator = mqtt.Client()
#
#cart.on_connect = on_connect
#cart.on_message = on_message
#
#cart.connect("test.mosquitto.org", 1883, 60)
#mobile.connect("test.mosquitto.org", 1883, 60)
#elevator.connect("test.mosquitto.org", 1883, 60)
#
####(1)Call dolly
#cart.subscribe("cart/status")
#mobile.publish("cart/status", "ON")
#time.sleep(1)
#
####(2)Get starting room number
#cart.subscribe("cart/room/starting_room_number")
#mobile.publish("cart/room/starting_room_number", "331")
#time.sleep(1)
#
####(3)Call elevator
#elevator.subscribe("elevator/status")
#cart.publish("elevator/status", "ON")
#    
#    
#    
    