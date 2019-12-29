/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.datagen;

/**
 * Implement this interface and provide that instance to the 
 * BaseStationSensorServer#addVoltageSensorListener method to 
 * be notified about new sensor readings, which are provided
 * through the parameter in the receivedVoltageReading method.
 */
public interface VoltageSensorListener {
    
    public void receivedNewReading(VoltageSensorReading reading);
    
}
