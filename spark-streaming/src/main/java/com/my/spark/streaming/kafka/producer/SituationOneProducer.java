package com.my.spark.streaming.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 创建topic
 *bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 3 --topic test-group
 * 一个Producer，一个topic，三个partition，一个Consumer Group，一个Consumer
 */
public class SituationOneProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //连接到kafka服务器
        properties.put("bootstrap.servers", "master:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //批处理的数量
        properties.put("batch.size", "10");
        //创建kafka生产者
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test-group",String.valueOf(i), String.valueOf(i));
            //发送record到kafaka
            producer.send(record);
        }
    }
}
