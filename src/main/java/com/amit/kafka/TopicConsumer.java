package com.amit.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TopicConsumer {

    public static void main(String[] args) {
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092, localhost:9093");
        p.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("group.id", "my-goup");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        List<String> topics = Arrays.asList("rep_topic-1");
        consumer.subscribe(topics);
        while(true){
            try {
            final ConsumerRecords<String, String> records = consumer.poll(10);
            records.forEach(r -> {
                System.out.println(r.partition() + " -- " + r.key() + " -- " + r.value());
            });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
