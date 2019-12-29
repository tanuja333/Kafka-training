/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.solution;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Implementation of Kafka's Serializer interface for the Measurement class. It 
 * converts an instance of a measurement into a byte array.
 */
public class MeasurementSerializer implements Serializer<Measurement> {
    
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public byte[] serialize(String topic, Measurement measurement) {
        if (measurement == null) {
            return null;
        }

        // 8 bytes for timestamp (long) + 4 bytes for voltage (float)
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Float.BYTES);
        buffer.putLong(measurement.getTimestamp());
        buffer.putFloat(measurement.getVoltage());

        return buffer.array();
    }

    @Override
    public void close() {
        // no-op
    }
}
