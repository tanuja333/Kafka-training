/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleConsumer {

    public static void main(String[] args) {

        String bootstrapServers = args[0];
        String topic = args[1];

        // Set up Java properties
        Properties props = new Properties();
        setupProperties(props, bootstrapServers);

        Boolean firstMessage = true;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // List of topics to subscribe to
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d, offset = %d, value = %s", record.partition(), record.offset(),
                            record.value());

                    if (firstMessage) {
                        System.out.println("\nThis is the first message I read: ^^^");
                        Thread.sleep(5 * 1000);
                        firstMessage = false;
                    }

                    String[] fields = record.value().split(",");
                    String customerId = fields[0];
                    String firstName = fields[1];

                    System.out.printf("Successfully processed custid = %s, firstname = %s%n", customerId, firstName);
                }
                // Commit the offsets, now that we're sure we've looped through our records successfully
                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupProperties(Properties props, String bootstrapServers) {

        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // groupId is mandatory for consumers, starting with Kafka 0.9.0.x.
        // You can refer to this JIRA: https://issues.apache.org/jira/browse/KAFKA-2648
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "handling-invalid-records");

        // Do not auto-commit offsets because we want this consumer to
        // retry at the point it left off
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Required properties to process records
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Required properties for Kerberos
        // props.setProperty("security.protocol", "SASL_PLAINTEXT");
        // props.setProperty("sasl.kerberos.service.name", "kafka");
    }

}
