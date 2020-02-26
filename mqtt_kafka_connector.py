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

def connect_mqtt_borker(mqtt_broker_ip,mqtt_broker_port, delay = 60):
    
    client = mqtt.Client()

    try:
        
        client.on_connect = on_connect
        client.on_message = on_message   
        client.connect(mqtt_broker_ip, mqtt_broker_port, delay)
        
    except Exception as ex:
        print('couldnt connect to mqtt broker. Maybe wrong port?')
        print(str(ex))
    return client  



def send_message_to_kafka(mqtttopic, message):
    """
    Send message to kafka consumer
    """
    os.system('clear')
    print("Mqtt Topic: " , mqtttopic)
    
    kafka_topic = ''.join([change(c, "/", "_") for c in mqtttopic])
    
    print("Kafka Topic", kafka_topic)
    
    d_message = dict(message)
    
    print("Message: " , d_message )
    

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
    
    producer.flush()
    
    
def on_connect_config(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # von config
    client.subscribe("config/#")


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # von config
    #client.subscribe("sensor/temperature/#")
                     
                     
def on_message(client, userdata, msg):
    os.system('clear')
    topic=msg.topic
    print("topic",topic)
    
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    print(m_decode)
    
    
def on_message_config(client, userdata, msg):
    os.system('clear')
    topic=msg.topic
    m_decode=str(msg.payload.decode("utf-8","ignore"))
#    print("data Received type",type(m_decode))
#    print("data Received",m_decode)
#    print("Converting from Json to Object")
    m_in=json.loads(m_decode) #decode json data
#    print(type(m_in))
    print("topic", topic)
    
    print("device-uuid = ",m_in["device-uuid"])
    print("location = ",m_in["location"])
    print("sensors = ",m_in["sensors"])
    
    sensors = m_in["sensors"]
    
    
    topics = []
    
    for sensor in sensors:       
        topics.append((sensor['mqtt-topic'],0))
        
    message_client = mqtt.Client()
    
    message_client.on_connect = on_connect
    message_client.on_message = on_message   
    message_client.connect(mqtt_broker_ip, mqtt_broker_port, 10)    
    
    message_client.loop_start()  
    message_client.subscribe(topics)
    
#        client.message_callback_add('config/topic', sensor['mqtt-topic'])

              
    #send_message_to_kafka(msg.topic, msg.payload)
    
if __name__ == "__main__":

    producer = connect_kafka_producer(kafka_broker_ip + ":" + \
                                      str(kafka_broker_port), 2)
    

    
    config_client = connect_mqtt_borker_config(mqtt_broker_ip,
                                               mqtt_broker_port)
    
    
    
    config_client.loop_forever()
    config_client.disconnect()
    
    
    
    
#mqtttopic = topic
#    
#kafka_topic = ''.join([change(c, "/", "_") for c in mqtttopic])
#    
#producer.send(kafka_topic, {'key': "mendel"})    
