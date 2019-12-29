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

public class ProducerRetries {

    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String topic = args[1];
        int messageCount = Integer.parseInt(args[2]);
        
        // instantiate the data generator for customer records
        CustomerSource customers = new CustomerSource();

        // Set up Java properties
        Properties props = new Properties();

        // These properties should be set for every producer
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
		
		
		// These properties can be set to control delivery. The settings 
		// will vary according to the details of your use case.

		// The producer will wait for acknowledgement from one broker (the leader)
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
		
		// Control how many open requests we can have per Kafka broker connection (defaults to 5)
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // Retry sending the message up to 25 times (defaults to zero)
        props.put(ProducerConfig.RETRIES_CONFIG, 25);

		// Wait one second (1,000 milliseconds) before retrying
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1_000);

		// Wait up to 15 seconds (15,000 milliseconds) for the broker to
		// respond to a request
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);


        try ( 
                KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < messageCount; i++) {
                String customerData = customers.getNewCustomerInfo();
                
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, customerData);
                producer.send(record).get();

                // pause for a short random period to simulate a non-constant stream of incoming data
                long wait = Math.round(Math.random() * 25);
                if (i % 100 == 0 ) {
                    System.out.printf("Sent message #%d%n:", i);
                }
                Thread.sleep(wait);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
