package com.cloudera.training.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import com.cloudera.training.kafka.datagen.RandomStringGenerator;

public class AnotherBad {

    static private final Logger logger = Logger.getLogger(AnotherBad.class);
    private DrEvil drEvil = new DrEvil();
    private Properties props;
    private Random rand = new Random();
    private int partitionCount = 6;

    public void causeTrouble(String bootstrapServers, String topicName, boolean dataOnly) {
        drEvil.configure(bootstrapServers);
        if (!dataOnly) {
            try {
                drEvil.createTopic(topicName, partitionCount);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                return;
            }
        }

        props = new Properties();
        setupProperties(props, bootstrapServers);

        RandomStringGenerator rsg = new RandomStringGenerator(50, 100);

        // Produce consecutive IDs per partition
        Map<Integer, AtomicInteger> m = new HashMap<Integer, AtomicInteger>();
        for (int i = 0; i < partitionCount; i++) {
            m.put(i, new AtomicInteger(0));
        }

        System.out.println("Writing data. . . . . ");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10_000_000; i++) {
                int partition = rand.nextInt(partitionCount);

                String rec = rsg.generateRandomString();
                int partOffset = m.get(partition).incrementAndGet();

                ProducerRecord<String, String> data = new ProducerRecord<String, String>(topicName, partition, null,
                        partition + ": " + partOffset + ":" + rec);
                producer.send(data, new SendCallback(partition, partOffset, 1000));
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
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        // Don't expire batches
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(15000L));
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000");

        // Required properties for Kerberos
        // props.setProperty("security.protocol", "SASL_PLAINTEXT");
        // props.setProperty("sasl.kerberos.service.name", "kafka");

        // TODO: uncomment this and experiment to compare the performance of
        // using the three supported compression types (gzip, snappy, and lz4).
        // props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32768 * 16));

    }
    
    private static class SendCallback implements Callback {
        // Used to intermittently print callback info
        private int part;
        private int id;
        private int interval;

        public SendCallback(int part, int id, int interval) {
            this.part = part;
            this.id = id;
            this.interval = interval;
        }
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
                logger.debug(String.format("Failed callback for: %d:%d\n", this.part, this.id));
            } else {
                if (id % interval == 0) {
                    System.out.printf("Received callback for Part: %d, ID: %d, Offset: %d\n", 
                            recordMetadata.partition(),
                            id,
                            recordMetadata.offset());
                }
            }
        }
    }

}
