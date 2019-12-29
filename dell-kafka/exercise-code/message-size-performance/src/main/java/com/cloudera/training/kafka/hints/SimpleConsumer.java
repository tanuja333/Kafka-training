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

/**
 * This class is a Kafka consumer that subscribes to a specified topic and 
 * prints information to standard output about messages received on that topic.
 */
public class SimpleConsumer {
    
    private final Logger logger = Logger.getLogger(SimpleConsumer.class);
    
    private final Properties props;
    private final String topic;
    
    public SimpleConsumer(String bootstrapServers, String topic) {
        props = new Properties();
        setupProperties(props, bootstrapServers);

        this.topic = topic;
    }
    
    public void startConsuming() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // List of topics to subscribe to
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", 
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            logger.error("Exception in consumer", e);
        }
    }

    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String topic = args[1];

        SimpleConsumer consumer = new SimpleConsumer(bootstrapServers, topic);
        consumer.startConsuming();
    }

    private static void setupProperties(Properties props, String bootstrapServers) {
        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // groupId is mandatory for consumers, starting with Kafka 0.9.0.x.
        // You can refer to this JIRA: https://issues.apache.org/jira/browse/KAFKA-2648
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "message-size-performance");

        // Required properties to process records
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
    }
}
