/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.solution;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Implementation of Kafka's Deserializer interface for the Measurement class. 
 * It converts a byte array, which was created with the MeasurementSerializer 
 * class, back into a Measurement instance that is equivalent to the original.
 */
public class MeasurementDeserializer implements Deserializer<Measurement> {
    
    // The array being deserialized should always be of this length, which 
    // is 12 bytes: 8 for the timestamp (long) and 4 for the voltage (float)
    private static final int EXPECTED_LENGTH = Long.BYTES + Float.BYTES;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public Measurement deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        if (bytes.length != EXPECTED_LENGTH) {
            // Warn about data corruption
            String msg = String.format("Expected %d bytes, but received %d bytes", EXPECTED_LENGTH, bytes.length);
            throw new SerializationException(msg);
        }
        
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        
        long timestamp = bb.getLong();
        float voltage = bb.getFloat();
        
        return new Measurement(timestamp, voltage);
    }

    @Override
    public void close() {
        // no-op
    }
}
