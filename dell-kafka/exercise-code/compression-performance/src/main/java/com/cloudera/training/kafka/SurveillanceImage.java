/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

/**
 * Plain old Java object representing a surveillance image take at a specific 
 * time. It is similar to the ImageEvent class, except that it does not store
 * information about the base station.
 */
public class SurveillanceImage  {
    
    private final long timestamp;
    private final byte[] image;

    public SurveillanceImage(long timestamp, byte[] image) {
        this.timestamp = timestamp;
        this.image = image;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public byte[] getImage() {
        return image;
    }

    @Override
    public String toString() {
        int imageSize = 0;
        if (image != null) {
            imageSize = image.length;
        }
        
        return "SurveillanceImage [" + "timestamp=" + timestamp + ", imageSize=" + imageSize +"]";
    }
}
