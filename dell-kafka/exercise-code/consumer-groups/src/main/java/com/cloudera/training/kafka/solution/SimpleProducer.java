/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.solution;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.training.kafka.datagen.CustomerSource;

public class SimpleProducer {

    static private final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    
    public static void main(String[] args) {

        String bootstrapServers = args[0];
        String topic = args[1];
        int messageCount = Integer.parseInt(args[2]);

        // instantiate the data generator for customer records
        CustomerSource customers = new CustomerSource();

        // Set up Java properties
        Properties props = new Properties();

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        // 262144 = 256 K, which trial and error shows improves throughput (as compared to the default 16 K)
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(262144));
        // props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5");


        // Required properties to process records
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        
        // Required properties for Kerberos
        // props.setProperty("security.protocol", "SASL_PLAINTEXT");
        // props.setProperty("sasl.kerberos.service.name", "kafka");

        Date startDate = new Date();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < messageCount; i++) {
                String customerData = customers.getNewCustomerInfo();
                ProducerRecord<String, String> data = new ProducerRecord<>(topic, null, customerData);
                RecordMetadata metadata = producer.send(data).get();

                System.out.printf("Offset: %d Partition: %d; Customer: %s%n", 
                        metadata.offset(), metadata.partition(), customerData);
                
                Thread.sleep(2000);
            }
                    
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        Date endDate = new Date();
        float diffInMillis = endDate.getTime() - startDate.getTime();
        System.out.printf("Sent %d messages in %,3f seconds\n", messageCount, diffInMillis/1000);
        System.out.printf("Messages per second: %f\n", messageCount / (diffInMillis/1000));
        System.out.printf("Seconds per message: %f\n", (diffInMillis/(1000 * messageCount)));
    }
}
