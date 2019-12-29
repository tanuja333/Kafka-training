/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.hints;

import com.cloudera.training.kafka.datagen.CustomerSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleSyncProducer {

    public static void main(String[] args) {

        String bootstrapServers = args[0];
        String topic = args[1];
        int messageCount = Integer.parseInt(args[2]);
        
        // instantiate the data generator for customer records
        CustomerSource customers = new CustomerSource();

        // Set up Java properties
        Properties props = new Properties();

        setupProperties(props, bootstrapServers);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < messageCount; i++) {
                String customerData = customers.getNewCustomerInfo();

                ProducerRecord<String, String> data = new ProducerRecord<>(topic, null, customerData);

                // Use Future.get() to wait for a reply from Kafka.
                // This method will throw an exception if the record is not sent successfully to Kafka.
                // If there were no errors, we will get a RecordMetadata object that we can use to retrieve
                // the offset the message was written to
				
				// TODO: get the Future object returned as a result of sending
				// the message, and then get the RecordMetadata object it contains.
                producer.send(data);

				// TODO: Update this line to include the offset from the RecordMetadata object you retrieved
                System.out.println(i + "|" + data.toString());

                // pause for a short random period to simulate a non-constant stream of incoming data
                long wait = Math.round(Math.random() * 5);
                Thread.sleep(wait);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupProperties(Properties props, String bootstrapServers) {
        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more bootstrapServers will
        // help in case of host failure or broker failure.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // format:   "hostname1:port1,hostname2:port2,hostname3:port3");

        // Required properties to process records
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
    }
}
