from kafka import KafkaConsumer
import json
import msgpack
import os

# To consume latest messages and auto-commit offsets
kafka_topic = "sensor_temperatur_SIMULATOR1-8A12-4F4F-8F69-7B8F4C2A78FF"
#kafka_topic2 = "sensor_brightness"

consumer = KafkaConsumer(kafka_topic,
                         group_id=None,
                         bootstrap_servers=['localhost:9092'])
for message in consumer:
    os.system('clear')
  
    print ("Konsument 3 Temperatur ")
    print ("topic: ", message.topic)
    #print ("partition: ", message.partition)
    #print ("offset: ", message.offset)
    
    #print ("key: ", message.key)
    
    message_dictionary = message.value.decode("utf-8")
    
    print ("value: ", message_dictionary)
    
