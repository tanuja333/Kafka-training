/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Implementation of Kafka's Serializer interface that can handle an Avro specific
 * object (an instance of a class generated from an Avro schema) by converting it
 * into a byte array.
 * 
 * @param <T> the type generated from the Avro schema
 */
public class SimpleAvroSerializer<T extends SpecificRecord> implements Serializer<T> {
    
    private static final Logger logger = Logger.getLogger(SimpleAvroSerializer.class);
    
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public byte[] serialize(String topic, T avroObject) {        
        if (avroObject == null) {
            return null;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        Schema schema = avroObject.getSchema();
        DatumWriter<T> outputDatumWriter = new SpecificDatumWriter<>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            
        try {    
            outputDatumWriter.write(avroObject, encoder);
            encoder.flush();
        } catch (IOException ex) {
            logger.error("Caught IOException while serializing record", ex);
        }
        
        return baos.toByteArray();
    }

    @Override
    public void close() {
        // no-op
    }
}
