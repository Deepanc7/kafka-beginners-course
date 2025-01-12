package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a kafka producer");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.23.131.70:9092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("log.level", "DEBUG");
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello world!");

        // send data -- asynchronous
        producer.send(producerRecord);

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();

    }
}
