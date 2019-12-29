/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.hints;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;
import org.apache.log4j.Logger;

public class SimpleConsumer {
    
    static private final Logger logger = Logger.getLogger(SimpleProducer.class);

    public static void main(String[] args) {

        String bootstrapServers = args[0];
        String topic = args[1];
        
        String consumerGroupId = "avro-schema-kafka";


        // Set up Java properties
        Properties props = new Properties();
        
        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // groupId is mandatory for consumers, starting with Kafka 0.9.0.x.
        // You can refer to this JIRA: https://issues.apache.org/jira/browse/KAFKA-2648
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

        // Required properties to process records
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());


        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // List of topics to subscribe to
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, partition = %d, key = %s, value = %s%n", 
                            record.offset(), record.partition(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            logger.error("Caught exception while processing records", e);
        }
    }
}
