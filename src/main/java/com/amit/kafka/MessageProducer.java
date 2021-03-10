package com.amit.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MessageProducer {

    public static void main(String[] args) {
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092, localhost:9093");
        p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(p);

        try {
            for (int i = 0; i < 20; i++) {
                ProducerRecord<String, String> record1 = new ProducerRecord<>("rep_topic-1", Integer.toString(i), "Hi - " + Integer.toString(i));
                ProducerRecord<String, String> record2 = new ProducerRecord<>("rep_topic-2", Integer.toString(i), "Hi - " + Integer.toString(i));
                producer.send(record1);
                producer.send(record2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }

}
