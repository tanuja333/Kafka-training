/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.datagen;

import java.util.Date;

/**
 * This class is a plain old Java object that provides access to the data from 
 * voltage reading gathered at a specific time from a sensor in a specific base
 * station.
 */
public class VoltageSensorReading {
    
    private final String baseStationId;
    private final float voltage;
    private final Date timestamp;
    
    VoltageSensorReading(String baseStationId, float voltage) {
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
