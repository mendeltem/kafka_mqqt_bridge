import time
import random
import threading
import paho.mqtt.client as mqtt
import json

threadLock = threading.Lock()

BROKER_ADDR = "localhost"
BROKER_PORT = 1883

TOPIC_1 = 'config' 

def mqtt_connect(mqtt_client, userdata, flags, rc):
    
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    #mqtt_client.subscribe("led/" + DEVICE_UUID_1)
    pass

def mqtt_message(mqtt_client, userdata, msg):
    print("MESSAGE topic: " + msg.topic + "MESSAGE payload: " + msg.payload.decode())
    

def sensor_loop():
    
    message_dict = {
      'device-uuid' : 'DEVICE1-8A12-4F4F-8F69-6B8F3C2E78FF',
      'location' : 'building1/room3',
      'sensors' : [
        {'mqtt-topic' : 'sensor/illuminance/bright1/DEVICE1-8A12-4F4F-8F69-6B8F3C2E78FF',
         'units' : 'CelsiusScale'}
      ]
    }
          
    data_out=json.dumps(message_dict) # encode object to JSON  
      
    while True:
        mqtt_client.publish(TOPIC_1, data_out)
        time.sleep(10)
    

if __name__ == "__main__":
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = mqtt_connect
    mqtt_client.on_message = mqtt_message

    mqtt_client.connect(BROKER_ADDR, BROKER_PORT, 60)

    sensor_handler = threading.Thread(target=sensor_loop)
    sensor_handler.start()

    mqtt_client.loop_start()
    mqtt_client.disconnect()
    
    sensor_handler.join()
