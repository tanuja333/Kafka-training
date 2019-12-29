/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

import com.cloudera.training.kafka.datagen.CustomerSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class RetryingProducer {
    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String topic = args[1];
        
        // instantiate the data generator for customer records
        CustomerSource customers = new CustomerSource();

        // Set up Java properties
        Properties props = new Properties();

        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        //    "hostname1:port1,hostname2:port2,hostname3:port3");
        // Enable a few useful properties for this example. Use of these
        // settings will depend on your particular use case.
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        // Required properties to process records
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        
        // Props required for Kerberos
        // props.setProperty("security.protocol","SASL_PLAINTEXT");
        // props.setProperty("sasl.kerberos.service.name","kafka");

        setupRetriesInFlightTimeout(props);

        try ( 
                KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int count = 0;
            while (true) {
                String customerData = customers.getNewCustomerInfo();
                
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, customerData);
                producer.send(record).get();

                // pause for a short random period (up to one-half second) to simulate a 
                // stream of data coming in from an external source
                long wait = Math.round(Math.random() * 500);
                if (count % 100 == 0 ) {
                    System.out.printf("Sent message #%d%n:", count);
                }
                Thread.sleep(wait);
                
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupRetriesInFlightTimeout(Properties props) {
        //Only one in-flight messages per Kafka broker connection
        // - max.in.flight.requests.per.connection (default 5)
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        //Set the number of retries - retries
        props.put(ProducerConfig.RETRIES_CONFIG, 30);

        //Request timeout - request.timeout.ms
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);

        //Only retry after one second.
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1_000);
    }
}
