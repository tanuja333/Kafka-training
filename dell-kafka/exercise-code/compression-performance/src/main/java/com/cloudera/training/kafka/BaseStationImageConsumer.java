/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

public class BaseStationImageConsumer {
    
    private static final Logger logger = Logger.getLogger(BaseStationImageConsumer.class);
    private MetricsLogger metricsLogger = new MetricsLogger("consumerMetricsLogger", 1000);
    
    private final Properties props;
    private final String topic;
    private int maxRecords = Integer.MAX_VALUE;
    
    public BaseStationImageConsumer(String bootstrapServers, String topic, int maxRecords) {
        props = new Properties();

        setupProperties(props, bootstrapServers);

        this.topic = topic;
        System.out.println("topic" + topic);
        this.maxRecords = maxRecords;
    }
    
    public void startConsuming() {
        try (KafkaConsumer<String, SurveillanceImage> consumer = new KafkaConsumer<>(props)) {
            // List of topics to subscribe to
            consumer.subscribe(Arrays.asList(topic));

            int count = 0;
            int emptyPolls = 0;
            int maxEmptyPolls = 10;
            while (count <= maxRecords && emptyPolls < maxEmptyPolls) {
                ConsumerRecords<String, SurveillanceImage> records = consumer.poll(500);
                if (records.count() == 0) {
                    System.out.println("Empty poll at " + count);
                    emptyPolls++;
                }
                else {
                    System.out.printf("Records fetched: %d at message #%d" , records.count(), count);
                    emptyPolls = 0;
                }
                for (ConsumerRecord<String, SurveillanceImage> record : records) {
                    if (count % 100 == 0) {

                        System.out.printf("Received record #%d with offset = %d\n",
                                count,
                                record.offset());
                        int keySize = record.serializedKeySize();
                        int valueSize = record.serializedValueSize();
                        int totalSize = keySize + valueSize;
                        
                        // Create a string representation of the value, just to ensure that
                        // everything came through OK
                        StringBuilder sb = new StringBuilder();
                        sb.append(new Date(record.value().getTimestamp()));
                        sb.append(" - ");
                        
                        byte[] image = record.value().getImage();
                        sb.append(String.format("Photo (%d bytes)", image.length));
                        String valueString = sb.toString();
                        

                        System.out.printf("key size = %d, value size = %d, total size = %s, value=%s%n", 
                                keySize, valueSize, totalSize, valueString);                        
                        
                    }
                    count++;
                }
            }
            if (emptyPolls >= maxEmptyPolls) {
                System.err.printf("Quit due to no more records.\n");
                
            }
            System.out.printf("Finished.  Records read: %d\n", count);
            metricsLogger.printMetrics(consumer.metrics());
        } catch (Exception e) {
            logger.error("Caught exception while consuming records", e);
        }
    }

    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String topic = args[1];
        
        int maxRecords = Integer.MAX_VALUE;

        if (args.length == 3) {
            maxRecords = Integer.parseInt(args[2]);
            
        }
        BaseStationImageConsumer consumer = new BaseStationImageConsumer(bootstrapServers, topic, maxRecords);
        consumer.startConsuming();
    }

    private static void setupProperties(Properties props, String bootstrapServers) {

        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // groupId is mandatory for consumers, starting with Kafka 0.9.0.x.
        // You can refer to this JIRA: https://issues.apache.org/jira/browse/KAFKA-2648
        // Assign a random group, to read all messages each run.
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "image-consumer" + UUID.randomUUID().toString());
        // Start reading from earliest offset
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        
        // NOTE: Changed to use our custom deserializer class
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                SurveillanceImageDeserializer.class.getName());
        
        // Required properties for Kerberos
        // props.setProperty("security.protocol", "SASL_PLAINTEXT");
        // props.setProperty("sasl.kerberos.service.name", "kafka");
    }
}
