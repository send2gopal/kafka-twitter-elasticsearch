# kafka-twitter-elasticsearch

## Start Zookeeper
    zookeeper-server-start config/zookeeper.properties

## Start Broker
    kafka-server-start config/server.properties

## Topic
###### Create
    kafka-topics --zookeeper 127.0.0.1:2181 --topic twitter_topic --create --partitions 3 --replication-factor 1
###### List
    kafka-topics --zookeeper 127.0.0.1:2181 --list

## Start Console Consumer
    kafka-console-consumer --bootstrap-server 127.0.0.1:9092  --topic topic_name
