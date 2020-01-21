#! /bin/sh
#wget http://mirrors.ibiblio.org/apache/kafka/2.3.0/kafka_2.12-2.3.0.tgz
wget http://mirror.metrocast.net/apache/kafka/2.4.0/kafka_2.11-2.4.0.tgz
tar -zxf kafka_2.11-2.4.0.tgz
mv kafka* /usr/local
ln -s /usr/local/kafka_2.11-2.4.0 /usr/local/kafka
#wget https://github.com/Landoop/stream-reactor/releases/download/1.2.2/kafka-connect-cassandra-1.2.2-2.1.0-all.tar.gz
#tar -zxf kafka-connect-cassandra-1.2.2-2.1.0-all.tar.gz
#mv kafka-connect-cassandra-*.jar /usr/local/kafka/libs
#/usr/local/kafka/bin/kafka-server-start.sh /usr/local/kafka/config/server.properties
#/usr/local/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic classroom

mkdir /home/student/ROI/SparkProgram/Day5/stream

