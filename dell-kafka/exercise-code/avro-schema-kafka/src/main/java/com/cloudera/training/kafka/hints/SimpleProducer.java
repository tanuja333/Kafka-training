/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.hints;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.cloudera.training.kafka.datagen.CustomerSource;
import org.apache.log4j.Logger;

public class SimpleProducer {

    private static final Logger logger = Logger.getLogger(SimpleProducer.class);
    
    public static void main(String[] args) {

        String bootstrapServers = args[0];
        String topic = args[1];
        int messageCount = Integer.parseInt(args[2]);

        // instantiate the data generator for customer records
        CustomerSource customers = new CustomerSource();
        

        // Set up Java properties
        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Required properties to process records
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());


        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < messageCount; i++) {
                String customerData = customers.getNewCustomerInfo();
        
                
// After you have generated the Customer class, uncomment the following code and
// then change the ProducerRecord to send the customer object
//                String[] data = customerData.split(",");
//                int customerId = Integer.parseInt(data[0]);
//                String firstName = data[1];
//                String lastName = data[2];
//                String phoneNumber = data[3];
//                
//                Customer customer = new Customer();
//                customer.setCustomerId(customerId);
//                customer.setFirstName(firstName);
//                customer.setLastName(lastName);
//                customer.setPhoneNumber(phoneNumber);

                
                ProducerRecord<String, String> value = new ProducerRecord<>(topic, null, customerData);
                RecordMetadata metadata = producer.send(value).get();

                System.out.printf("Offset: %d Partition: %d; Customer: %s%n", 
                        metadata.offset(), metadata.partition(), customerData);
                
                Thread.sleep(1000);
            }
                    
        } catch (Exception e) {
            logger.error("Caught exception while sending records", e);
        }
    }
}
