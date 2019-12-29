/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.cloudera.training.kafka.datagen.RandomStringGenerator;

public class SimpleAsyncProducer {

    public static void main(String[] args) {
        MetricsLogger metricsLogger = new MetricsLogger("producerMetricsLogger", 1000);

        String bootstrapServers = args[0];
        String topic = args[1];
        int sleepTime = 0;

        String compression;
        int maxRecords = Integer.MAX_VALUE;

        String usage = "SimpleAsyncProducer"
                + " <bootstrap servers> <topic> <compression=lz4|snappy|gzip|none>"
                + " [maxRecords]";

        if (args.length < 3) {
            System.err.println(usage);
            System.exit(1);
        }
        compression = args[2].toLowerCase();
        metricsLogger.setMetricsBatchName("SimpleRecords\t" + compression);

        if (args.length > 3) {
            maxRecords = Integer.parseInt(args[3]);
        }

        // Set up Java properties
        Properties props = new Properties();

        setupProperties(props, bootstrapServers, compression);

        RandomStringGenerator rsg = new RandomStringGenerator(50, 100);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < maxRecords; i++) {

                ProducerRecord<String, String> data = 
                    new ProducerRecord<>(topic, null, rsg.generateRandomString());
                // We call the send() method with a callback function
                // which gets triggered when it receives a response from the Kafka broker
                producer.send(data, new SendCallback(i, 10000));

                // As is async we don't know the offset until DemoCallback.onCompletion() is invoked
                if (i % 100000 == 0 ) {
                    System.out.println(i + "|" + data.toString());
                }

                if (sleepTime > 0) {
                    Thread.sleep(sleepTime);
                }
            }
            metricsLogger.printMetrics(producer.metrics());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void setupProperties(Properties props, String bootstrapServers, String compression) {
        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //    "hostname1:port1,hostname2:port2,hostname3:port3");


        // Required properties to process records
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Enable a few useful properties for this example. Use of these
        // settings will depend on your particular use case.
        props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        // Don't expire batches
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));

        // Required properties for Kerberos
        // props.setProperty("security.protocol", "SASL_PLAINTEXT");
        // props.setProperty("sasl.kerberos.service.name", "kafka");
        
        // TODO: uncomment this and experiment to compare the performance of 
        // using the three supported compression types (gzip, snappy, and lz4).
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32768 * 16));


    }

    private static class SendCallback implements Callback {
        // Used to intermittently print callback info
        private int counter;
        private int interval;

        public SendCallback(int counter, int interval) {
            this.counter = counter;
            this.interval = interval;
        }
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
            } else {
                if (counter % interval == 0) {
                    System.out.println("Received callback for Offset:  " + recordMetadata.offset());
                }
            }
        }
    }
}
