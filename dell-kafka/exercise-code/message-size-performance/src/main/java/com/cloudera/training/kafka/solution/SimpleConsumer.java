/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.solution;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

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
        try (KafkaConsumer<String, Measurement> consumer = new KafkaConsumer<>(props)) {
            // List of topics to subscribe to
            consumer.subscribe(Arrays.asList(topic));

            int count = 0;
            while (true) {
                ConsumerRecords<String, Measurement> records = consumer.poll(100);
                for (ConsumerRecord<String, Measurement> record : records) {
                    if (count % 1000 == 0) {
                        int keySize = record.serializedKeySize();
                        int valueSize = record.serializedValueSize();
                        int totalSize = keySize + valueSize;
                        
                        // Create a string representation of the value, just to ensure that
                        // everything came through OK
                        StringBuilder sb = new StringBuilder();
                        sb.append(new Date(record.value().getTimestamp()));
                        sb.append("-");
                        sb.append(record.value().getVoltage());
                        String valueString = sb.toString();

                        System.out.printf("key size = %d, value size = %d, total size = %s, value=%s%n", 
                                keySize, valueSize, totalSize, valueString);                        
                    }
                    
                    count++;
                }
            }
        } catch (Exception e) {
            logger.error("Caught exception while consuming records", e);
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

        
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // NOTE: Changed to use our custom deserializer class
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                MeasurementDeserializer.class.getName());
    }
}
