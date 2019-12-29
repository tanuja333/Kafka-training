/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.Logger;

/**
 * Implementation of Kafka's Deserializer interface for the SurveillanceImage 
 * class. It converts a byte array created by the SurveillanceImageSerializer
 * class, back into a SurveillanceImage instance equivalent to the original.
 */
public class SurveillanceImageDeserializer implements Deserializer<SurveillanceImage> {
    
    private final Logger logger = Logger.getLogger(SurveillanceImageDeserializer.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public SurveillanceImage deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        int imageSize = bb.getInt();

        // The array being deserialized should always be of this length, which 
        // is: 4 for the integer that says how large the image is, 8 for the 
        // timestamp (long), plus the size of the image (which varies).
        int EXPECTED_LENGTH = Integer.BYTES + Long.BYTES + imageSize;
        if (bytes.length != EXPECTED_LENGTH) {
            // Warn about data corruption
            String msg = String.format("Expected %d bytes, but received %d bytes", EXPECTED_LENGTH, bytes.length);
            logger.error(msg);
            throw new SerializationException(msg);
        }
        
        long timestamp = bb.getLong();
        
        byte[] image = new byte[imageSize];
        bb.get(image);
        
        return new SurveillanceImage(timestamp, image);
    }

    @Override
    public void close() {
        // no-op
    }
}
