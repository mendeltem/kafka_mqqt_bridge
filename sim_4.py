import time
import random
import threading
import paho.mqtt.client as mqtt

threadLock = threading.Lock()
DEVICE_UUID_1 = "SIMULATOR1-8A12-4F4F-8F69-7B8F4C2A78FF"

BROKER_ADDR = "localhost"
BROKER_PORT = 1883

SENSOR_TOPIC_1 = 'sensor/news/' + DEVICE_UUID_1

def mqtt_connect(mqtt_client, userdata, flags, rc):
    mqtt_client.subscribe("led/" + DEVICE_UUID_1)

def mqtt_message(mqtt_client, userdata, msg):
#    print("MESSAGE topic: " + msg.topic + "MESSAGE payload: " + msg.payload.decode())
    pass

def sensor_loop():
    while True:
        mqtt_client.publish(SENSOR_TOPIC_1, "Hallo")
        mqtt_client.publish(SENSOR_TOPIC_1, "World")
        time.sleep(5)

if __name__ == "__main__":
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = mqtt_connect
    mqtt_client.on_message = mqtt_message

    mqtt_client.connect(BROKER_ADDR, BROKER_PORT, 60)

    sensor_handler = threading.Thread(target=sensor_loop)
    sensor_handler.start()

    mqtt_client.loop_start()
    sensor_handler.join()

