#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 13:58:14 2020

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

connector = mqtt_kafka_connector(
            kafka_broker_ip = 'localhost',
            kafka_broker_port = 9092,
            mqtt_broker_ip = 'localhost',
            mqtt_broker_port = 1883)


connector.connect()


class mqtt_kafka_connector:
    def __init__(self, kafka_broker_ip,kafka_broker_port, mqtt_broker_ip,mqtt_broker_port):
        
        self.kafka_broker_ip = 'localhost'
        self.kafka_broker_port = 9092
        self.mqtt_broker_ip = 'localhost'
        self.mqtt_broker_port = 1883

    def connect_kafka_producer(self,ip ='localhost:9092',  type_of_message = 1):
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
#
#
    def connect_mqtt_borker(self,mqtt_broker_ip,mqtt_broker_port, delay = 60):
    
        try:
            client.connect(mqtt_broker_ip, mqtt_broker_port, delay)
        except Exception as ex:
            print('couldnt connect to mqtt broker. Maybe wrong port?')
            print(str(ex))
        return client   

    def on_connect(client, userdata, flags, rc):
        print("Connected with result code " + str(rc))
        # von config
        client.subscribe("#")
                                            
                         
    def on_message(client, userdata, msg):
        print("msg")
        
        send_message_to_kafka(msg.topic, msg.payload)


    def connect(self):
        
        producer = connect_kafka_producer(self.kafka_broker_ip + ":" + \
                                          str(self.kafka_broker_port), 2)
        
        
        client = connect_mqtt_borker(self.mqtt_broker_ip,self.mqtt_broker_port)
    
    
    
    
    def loop(self):
        client.loop_forever()