
#start and stop mqtt
sudo service mosquitto start 
sudo service  mosquitto stop


#start zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties 


#start kafka broker
bin/kafka-server-start.sh config/server.properties



#start kafka consumer manuelly
bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic sensor_brightness_SIMULATOR1-8A12-4F4F-8F69-7B8F4C2A78DD --property print.key=true --from-beginning


