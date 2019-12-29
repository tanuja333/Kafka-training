/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.solution;

/**
 * Plain old Java object representing a voltage reading made at a specific time.
 * It is similar to the VoltageSensorReading class, except that it does not 
 * store information about the base station.
 */
public class Measurement  {
    
    private final long timestamp;
    private final float voltage;

    public Measurement(long timestamp, float voltage) {
        this.timestamp = timestamp;
        this.voltage = voltage;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public float getVoltage() {
        return voltage;
    }

    @Override
    public String toString() {
        return "Measurement [" + "timestamp=" + timestamp + ", voltage=" + voltage + "]";
    }
}
