/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.solution;

import com.cloudera.training.kafka.datagen.CustomerSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Fire and Forget Producer Example.
 */
public class SimpleProducer {

    public static void main(String[] args) {

        String bootstrapServers = args[0];
        String topic = args[1];
        int messageCount = Integer.parseInt(args[2]);

        // instantiate the data generator for customer records
        CustomerSource customers = new CustomerSource();

        // Set up Java properties
        Properties props = new Properties();
        
        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure. On a cluster,
        // this should look something like: host1:9092,host2:9092,host3:9092.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Required properties to process records
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Now that the consumer is configured, let's use it
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < messageCount; i++) {
                String customerData = customers.getNewCustomerInfo();

                ProducerRecord<String, String> data = new ProducerRecord<>(topic, null, customerData);
                producer.send(data);

                System.out.printf("Sent message %d, with value '%s'", i, data.toString());

                // wait a short time (up to two seconds) before sending the next one, 
                // to better simulate customer data coming in from some external source
                long wait = Math.round(Math.random() * 2000);
                Thread.sleep(wait);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
