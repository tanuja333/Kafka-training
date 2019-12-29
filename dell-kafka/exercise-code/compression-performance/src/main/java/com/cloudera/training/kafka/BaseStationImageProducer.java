/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import com.cloudera.training.kafka.datagen.BaseStationSurveillanceServer;
import com.cloudera.training.kafka.datagen.ImageEvent;
import java.io.IOException;

/**
 * This producer listens for incoming surveillance photos from the base station
 * server and then sends a record with the image to the specified topic.
 */
public class BaseStationImageProducer  {
    
    private static final Logger logger = Logger.getLogger(BaseStationImageProducer.class);
    private MetricsLogger metricsLogger = new MetricsLogger("producerMetricsLogger", 1000);

    private final Properties props;
    private final String topic;
    private int maxRecords = Integer.MAX_VALUE;
    private final BaseStationSurveillanceServer cameraServer;

    public BaseStationImageProducer(String bootstrapServers, String topic, String compression, int maxRecords) {
        this.topic = topic;
        this.props = new Properties();
        this.maxRecords = maxRecords;

        setupProperties(bootstrapServers, compression);

        this.cameraServer = BaseStationSurveillanceServer.getService();
        this.metricsLogger.setMetricsBatchName("Images\t" + compression);
    }

    public static void main(String[] args) throws IOException {
        String usage = "BaseStationImageProducer"
                + " <bootstrap servers> <topic> <compression=lz4|snappy|gzip|none>"
                + " [maxRecords]";
        String bootstrapServers = args[0];
        String topic = args[1];
        String compression;
        int maxRecords = Integer.MAX_VALUE;

        if (args.length < 2) {
            System.err.println(usage);
            System.exit(1);
        }
        compression = args[2].toLowerCase();

        if (args.length > 3) {
            maxRecords = Integer.parseInt(args[3]);
        }

        BaseStationImageProducer bsip = new
        BaseStationImageProducer(bootstrapServers, topic, compression, 
            maxRecords); 
        bsip.startProducing();        
    }
    
    private void startProducing() throws IOException {
        int count = 0;

        try(KafkaProducer<String, SurveillanceImage> producer = 
                new KafkaProducer<>(this.props)) {
            while (count++ < maxRecords) {
                ImageEvent imageEvent = cameraServer.getImageEvent();            
                String baseStationId = imageEvent.getBaseStationId();

                long timestamp = imageEvent.getTimestamp().getTime();
                byte[] image = imageEvent.getImage();
                
                SurveillanceImage si = new SurveillanceImage(timestamp, image);
                ProducerRecord<String, SurveillanceImage> record = new ProducerRecord<>(topic, baseStationId, si);
                try {
                    if (count % 10 == 0) {
                        logger.debug(String.format("Sending surveillance image (size=%d) for station %s", image.length, baseStationId));
                        System.out.println("SENT RECORD " + count + " at " + new Date());
                    }
                    producer.send(record, new SendCallback(count, 100));
                } catch (Exception e) {
                    logger.error("Caught exception while sending record", e);
                }
            }
            metricsLogger.printMetrics(producer.metrics());
        }
    }

    private void setupProperties(String bootstrapServers, String compression) {
        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        //    "hostname1:port1,hostname2:port2,hostname3:port3");
        // Enable a few useful properties for this example. Use of these
        // settings will depend on your particular use case.
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        // Don't expire batches
        props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.toString(Long.MAX_VALUE));

        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        // NOTE: Changed to use our custom serializer class
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                SurveillanceImageSerializer.class.getName());
        
        // TODO: uncomment this and experiment to compare the performance of 
        // using the three supported compression types (gzip, snappy, and lz4).
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32768 * 16));
        
        // Props required for Kerberos
        // props.setProperty("security.protocol","SASL_PLAINTEXT");
        // props.setProperty("sasl.kerberos.service.name","kafka");
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
                    System.out.println("Offset:  " + recordMetadata.offset());
                }
            }
        }
    }
    
}
