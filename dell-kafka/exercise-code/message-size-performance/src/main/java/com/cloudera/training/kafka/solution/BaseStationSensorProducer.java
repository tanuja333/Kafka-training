/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.solution;

import com.cloudera.training.kafka.datagen.BaseStationSensorServer;
import com.cloudera.training.kafka.datagen.VoltageSensorReading;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import com.cloudera.training.kafka.datagen.VoltageSensorListener;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

/**
 * This is an updated version of the producer, which, unlike the original, 
 * sends a Java object representing the reading timestamp and voltage as the
 * value (instead of sending it as a String).
 */
public class BaseStationSensorProducer implements VoltageSensorListener {
    
    private final Logger logger = Logger.getLogger(BaseStationSensorProducer.class);
    
    private final KafkaProducer<String, Measurement> producer;
    private final Properties props;
    private final String topic;
    
    public BaseStationSensorProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        this.props = new Properties();
        
        setupProperties(bootstrapServers);
        
        this.producer = new KafkaProducer<>(props);
    }
    
    public static void main(String[] args) {
        String bootstrapServers = args[0];
        String topic = args[1];

        // create a new instance of this class and subscribe to notifications about
        // sensor readings, which are handled via the receivedVoltageReading method.
        BaseStationSensorProducer bssp = new BaseStationSensorProducer(bootstrapServers, topic);
        BaseStationSensorServer.getService().addVoltageSensorListener(bssp);
    }
    
    @Override
    public void receivedNewReading(VoltageSensorReading reading) {
        String baseStationId = reading.getBaseStationId();
        
        long timestamp = reading.getTimestamp().getTime();
        float voltage = reading.getVoltage();
        Measurement measurement = new Measurement(timestamp, voltage);

        // Create the Kafka record and send it to the broker
        ProducerRecord<String, Measurement> record = new ProducerRecord<>(topic, baseStationId, measurement);
        try {
            logger.debug(String.format("Sending measurement %s for station %s", measurement, baseStationId));
            producer.send(record);
        } catch (Exception e) {
            logger.error("Caught exception while sending record", e);
        }
    }

    private void setupProperties(String bootstrapServers) {
        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Enable a few useful properties for this example. Use of these
        // settings will depend on your particular use case.
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        // NOTE: Changed to use our custom serializer class
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                MeasurementSerializer.class.getName());
    }
}
