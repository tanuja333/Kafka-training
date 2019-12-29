/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

/**
 * Implementation of Kafka's Serializer interface for the SurveillanceImage 
 * class. It converts an instance of a SurveillanceImage into a byte array.
 */
public class SurveillanceImageSerializer implements Serializer<SurveillanceImage> {
    
    private final Logger logger = Logger.getLogger(SurveillanceImageSerializer.class);

    
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // no-op
    }

    @Override
    public byte[] serialize(String topic, SurveillanceImage si) {
        if (si == null) {
            return null;
        }
        
        byte[] image = si.getImage();
        int imageSize = 0;
        if (image != null)  {
            imageSize = image.length;
        }
        
        // the first field (integer) is the size of the array used to store the 
        // image (which is sent as the last field). The second is 8 bytes for 
        // the timestamp (long), and the third is a variable length for the image.
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Long.BYTES + imageSize);
        buffer.putInt(imageSize);
        buffer.putLong(si.getTimestamp());
        if (imageSize != 0) {
            buffer.put(image);
        }

        return buffer.array();
    }

    @Override
    public void close() {
        // no-op
    }
}
