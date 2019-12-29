/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.datagen;

import java.util.Date;

/**
 * This class is a plain old Java object that provides access to the image read
 * at a specific time from a surveillance camera in a specific base station.
 */
public class ImageEvent {
    
    private final String baseStationId;
    private final Date timestamp;
    private final byte[] image;
    
    ImageEvent(String baseStationId, byte[] image) {
        this.baseStationId = baseStationId;
        this.image = image;
        
        this.timestamp = new Date();
    }

    public String getBaseStationId() {
        return baseStationId;
    }

    public Date getTimestamp() {
        return timestamp;
    }
    
    public byte[] getImage() {
        return image;
    }
}
