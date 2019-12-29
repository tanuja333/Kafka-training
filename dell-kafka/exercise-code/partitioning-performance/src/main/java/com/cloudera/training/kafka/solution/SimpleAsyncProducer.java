/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.solution;

import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.TreeSet;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.training.kafka.datagen.CustomerSource;

public class SimpleAsyncProducer {

    static private final Logger logger = LoggerFactory.getLogger(SimpleAsyncProducer.class);
    
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

                // We call the send() method with a callback function
                // which gets triggered when it receives a response from the Kafka broker
                producer.send(data, new DemoCallback());

                if (i % 50000 == 1) {
                    printMetrics(producer);
                }
            }
            printMetrics(producer);
                    
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        Date endDate = new Date();
        float diffInMillis = endDate.getTime() - startDate.getTime();
        System.out.printf("Sent %d messages in %,3f seconds\n", messageCount, diffInMillis/1000);
        System.out.printf("Messages per second: %f\n", messageCount / (diffInMillis/1000));
        System.out.printf("Seconds per message: %f\n", (diffInMillis/(1000 * messageCount)));
    }

    private static class DemoCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
            }
            else {
                if(recordMetadata.offset() % 10000 == 0) {
                    System.out.printf("Offset: %d Partition: %d\n" , recordMetadata.offset(), recordMetadata.partition());
                }
            }
        }
    }

    public static void printMetrics(KafkaProducer<String,String> producer) {
        // (Optional) set a filter to print metrics that are most valuable (for
        // easier parsing)
        TreeSet<String> desiredMetrics = new TreeSet<>(
        Arrays.asList(
        "record-queue-time-avg", "record-send-rate", "records-per-request-avg",
        "request-size-max", "network-io-rate", "record-queue-time-avg",
        "incoming-byte-rate", "batch-size-avg", "response-rate", "requests-in-flight"));
        for (MetricName mn : producer.metrics().keySet()) {
            Metric m = producer.metrics().get(mn);
            if (desiredMetrics.isEmpty() || desiredMetrics.contains(m.metricName().name())) {
                String metricOutput = String.format(Locale.US, 
                        "%-25s\t%-20s\t%-10.2f\t%s",
                        mn.name(),
                        mn.tags().getOrDefault("node-id", ""),
                        m.metricValue(),
                        mn.description());
                logger.debug(metricOutput);
            }
        }
    }
}
