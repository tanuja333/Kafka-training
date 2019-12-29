/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.datagen;

import java.util.Date;

/**
 * This class provides access to the data from a voltage reading gathered
 * at a specific time from a specific base station.
 */
public class VoltageReading {
    
    private final String baseStationId;
    private final float voltage;
    private final Date timestamp;
    
    VoltageReading(String baseStationId, float voltage) {
        this.baseStationId = baseStationId;
        this.voltage = voltage;
        
        this.timestamp = new Date();
    }

    public String getBaseStationId() {
        return baseStationId;
    }

    public float getVoltage() {
        return voltage;
    }

    public Date getTimestamp() {
        return timestamp;
    }
}
