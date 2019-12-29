package com.cloudera.training.kafka;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.cloudera.training.kafka.datagen.RandomStringGenerator;

public class StrongBad {

    private DrEvil drEvil = new DrEvil();
    private Properties props;
    private Random rand = new Random();

    public void causeTrouble(String bootstrapServers, String topicName, boolean dataOnly) {
        drEvil.configure(bootstrapServers);
        short replicas = 3;
        int partitions = 3;
        if (dataOnly == false) {
            try {
                drEvil.createBadTopic(topicName, partitions, replicas);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                return;
            }
        }

        props = new Properties();
        setupProperties(props, bootstrapServers);

        RandomStringGenerator rsg = new RandomStringGenerator(50, 100);

        System.out.println("Writing data . . . . ");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int part = rand.nextInt(partitions);
            for (int i = 0; i < 10_000_000; i++) {

                ProducerRecord<String, String> data = new ProducerRecord<String, String>(topicName,  part, null, rsg.generateRandomString());
                producer.send(data);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setupProperties(Properties props, String bootstrapServers) {
        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // "hostname1:port1,hostname2:port2,hostname3:port3");

        // Required properties to process records
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Enable a few useful properties for this example. Use of these
        // settings will depend on your particular use case.
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "2");
        // Don't expire batches
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(15000L));

        // Required properties for Kerberos
        // props.setProperty("security.protocol", "SASL_PLAINTEXT");
        // props.setProperty("sasl.kerberos.service.name", "kafka");

        // TODO: uncomment this and experiment to compare the performance of
        // using the three supported compression types (gzip, snappy, and lz4).
        // props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32768 * 16));

    }

}
