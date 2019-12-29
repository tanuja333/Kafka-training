/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

import com.cloudera.training.kafka.datagen.BaseStationSensorServer;
import com.cloudera.training.kafka.datagen.SensorReadingListener;
import com.cloudera.training.kafka.datagen.VoltageReading;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class BaseStationSensorProducer implements SensorReadingListener {
    
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
        BaseStationSensorServer.getService().addSensorReadingListener(bssp);
    }
    
    @Override
    public void receivedVoltageReading(VoltageReading reading) {
        // Create a comma-delimited string with the sensor data:
        //   Field 1: Base Station ID
        //   Field 2: Timestamp
        //   Field 3: Voltage
        StringBuilder sb = new StringBuilder();
        sb.append(reading.getBaseStationId());
        sb.append(",");
        sb.append(timestampFormatter.format(reading.getTimestamp()));
        sb.append(",");
        sb.append(reading.getVoltage());
        
        String sensorData = sb.toString();
        
        // Create the Kafka record and send it to the broker
        // TODO Assign a partition to the record
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, sensorData);
        producer.send(record);
        System.out.printf("Sent data: %s%n", sensorData);
    }

    private void setupProperties(String bootstrapServers) {
        // This should point to at least one broker. Some communication
        // will occur to find the controller. Adding more brokers will
        // help in case of host failure or broker failure.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        //    "hostname1:port1,hostname2:port2,hostname3:port3");
        // Enable a few useful properties for this example. Use of these
        // settings will depend on your particular use case.
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        // Required properties to process records
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        
        // Props required for Kerberos
        // props.setProperty("security.protocol","SASL_PLAINTEXT");
        // props.setProperty("sasl.kerberos.service.name","kafka");
    }


}
