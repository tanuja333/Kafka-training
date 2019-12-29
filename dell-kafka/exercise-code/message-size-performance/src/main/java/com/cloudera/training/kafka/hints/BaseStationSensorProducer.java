/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.hints;

import com.cloudera.training.kafka.datagen.BaseStationSensorServer;
import com.cloudera.training.kafka.datagen.VoltageSensorReading;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import com.cloudera.training.kafka.datagen.VoltageSensorListener;
import org.apache.log4j.Logger;

/**
 * This class is a Kafka producer that listens to voltage sensors in Loudacre's
 * base stations and sends messages to a specified topic whenever new readings
 * are received.
 */
public class BaseStationSensorProducer implements VoltageSensorListener {
    
    private final Logger logger = Logger.getLogger(BaseStationSensorProducer.class);
    
    private final KafkaProducer<String, String> producer;
    private final Properties props;
    private final String topic;
    private final DateFormat timestampFormatter;
    
    public BaseStationSensorProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        this.props = new Properties();
        this.timestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z");
        
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
        // The base station ID will be sent as the key
        String baseStationId = reading.getBaseStationId();

        // Create a comma-delimited string with the sensor data to send as the value
        //   Field 1: Timestamp
        //   Field 2: Voltage
        StringBuilder sb = new StringBuilder();
        sb.append(timestampFormatter.format(reading.getTimestamp()));
        sb.append(",");
        sb.append(reading.getVoltage());
        String sensorData = sb.toString();

        // Create the Kafka record and send it to the broker
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, baseStationId, sensorData);
        
        try {
            logger.debug(String.format("Sending value %s for station %s", sensorData, baseStationId));
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

        // Required properties to process records
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        
    }
}
