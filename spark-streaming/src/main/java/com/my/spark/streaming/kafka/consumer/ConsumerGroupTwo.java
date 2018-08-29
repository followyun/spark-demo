package com.my.spark.streaming.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 一个Producer，一个topic，三个partition，一个Consumer Group，一个Consumer
 */
public class ConsumerGroupTwo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //连接到kafka服务器
        properties.put("bootstrap.servers", "master:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "group2");
        //创建kafka生产者
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test-group"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record :
                    records) {
                System.out.printf("offset = %d, key = %s, value = %s, topic = %s, partition = %d",
                        record.offset(), record.key(), record.value(), record.topic(), record.partition());
                System.out.println();
            }
        }
    }
}
