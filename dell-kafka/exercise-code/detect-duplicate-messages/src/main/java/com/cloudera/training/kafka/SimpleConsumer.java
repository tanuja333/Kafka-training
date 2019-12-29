/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class SimpleConsumer {
    
    private Set<String> processedCustomerIds;

    private final File serializedHashFile;    
    private final Properties props;
    private final String topic;
    
    public SimpleConsumer(String bootstrapServers, String topic) {
        props = new Properties();
        setupProperties(props, bootstrapServers);

        this.topic = topic;
        serializedHashFile = new File(System.getProperty("user.dir"), "OurSerializedHashFile.dat");
        
        if (serializedHashFile.exists()) {
            System.out.println("Reading data from OurSerializedHashFile.dat");
            readData(serializedHashFile);
        } else {
            // This must be our first time starting up
            System.out.println("Creating new OurSerializedHashFile.dat");
            processedCustomerIds = new HashSet<>();
        }
    }
    
    public void startConsuming() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // List of topics to subscribe to
            consumer.subscribe(Arrays.asList(topic));

            int i = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    i++;
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    
                    // extract the customer ID from the comma-delimited value
                    String customerId = record.value().split(",")[0];
                    if (processedCustomerIds.contains(customerId)) {
                        System.out.printf("*** Ignored previously seen customer ID %s ***\n", customerId);
                    } else {
                        processedCustomerIds.add(customerId);
                        
                        if (i % 10 == 0) {
                            // TODO: How often should we save this?
                            saveData(serializedHashFile);
                            // TODO: How often / where should we commit offsets?
                            // consumer.commitSync();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private void saveData(File hashFileLocation) {
        // Philosophical question: how can we know when to call this?
        FileOutputStream fos = null;
        ObjectOutputStream out = null;
        try {
            fos = new FileOutputStream(hashFileLocation);
            out = new ObjectOutputStream(fos);
            out.writeObject(processedCustomerIds);

            out.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
    
    private void readData(File hashFileLocation) {
        FileInputStream fis = null;
        ObjectInputStream in = null;
        try {
            fis = new FileInputStream(hashFileLocation);
            in = new ObjectInputStream(fis);
            processedCustomerIds = (Set<String>) in.readObject();
            in.close();
        } catch (IOException | ClassNotFoundException ex) {
            ex.printStackTrace();
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "detect-duplicate-messages");

        // Required properties to process records
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        
        // TODO: Un-comment this to manually commit offsets
        // props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // Required properties for Kerberos
        // props.setProperty("security.protocol", "SASL_PLAINTEXT");
        // props.setProperty("sasl.kerberos.service.name", "kafka");
    }
}
