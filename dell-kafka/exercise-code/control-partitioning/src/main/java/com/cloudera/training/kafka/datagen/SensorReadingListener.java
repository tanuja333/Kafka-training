/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.datagen;

/**
 * Implement this interface and provide that instance to the 
 * BaseStationSensorServer#addSensorReadingListener method to 
 * be notified about new sensor readings, which are provided
 * through the parameter in the receivedVoltageReading method.
 */
public interface SensorReadingListener {
    
    public void receivedVoltageReading(VoltageReading reading);
    
}
