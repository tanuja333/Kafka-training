/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.solution;

import com.cloudera.training.kafka.datagen.CustomerSource;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleAsyncProducer {

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

                // We call the send() method with a callback function
                // which gets triggered when it receives a response from the Kafka broker
                producer.send(data, new DemoCallback());

                // As is async we don't know the offset until DemoCallback.onCompletion() is invoked
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
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // format:   "hostname1:port1,hostname2:port2,hostname3:port3");


        // Required properties to process records
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
    }

    private static class DemoCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
            } else {
                System.out.println("Offset:  " + recordMetadata.offset());
            }
        }
    }

}
