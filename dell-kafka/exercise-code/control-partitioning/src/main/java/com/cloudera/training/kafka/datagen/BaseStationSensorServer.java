/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.datagen;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.math3.random.RandomDataGenerator;

/**
 * This class simulates a server that periodically (and continually) gathers 
 * data from base station voltage sensors and notifies interested parties 
 * about their values.
 */
public class BaseStationSensorServer {
    
    private final CopyOnWriteArrayList<SensorReadingListener> listeners;
    private final ScheduledExecutorService executor;
    private final RandomDataGenerator generator;
    
    private final String[] stationIds;

    private enum BaseStationSensorSingleton {
        
        INSTANCE();
        
        private final BaseStationSensorServer service;
        
        private BaseStationSensorSingleton() {
            RandomDataGenerator generator = new RandomDataGenerator();
            generator.reSeed(5150); // get the same ones every time
            service = new BaseStationSensorServer(generator, 5);
        }
        
        BaseStationSensorServer getService() {
            return service;
        }
    }
 
    private BaseStationSensorServer(RandomDataGenerator generator, int numBaseStations) {
        this.listeners = new CopyOnWriteArrayList<>();
        this.executor = Executors.newScheduledThreadPool(1);
        
        this.generator = generator;

        // populate potential list of base station IDs from a list of city names
        // in a text file and prepending a sequence number to each one
        stationIds = new String[numBaseStations];

        try {
            List<String> allStationIds = populateCitiesList("/cities.txt");
            
            // get a unique list of base station IDs from that list of cities
            Set<String> cityList = new HashSet<>();
            while (cityList.size() < numBaseStations) {
                int index = generator.nextInt(0, allStationIds.size() - 1);
                cityList.add(allStationIds.get(index));
            }

            cityList.toArray(stationIds);
        } catch (Exception e) { 
            e.printStackTrace();
        }
  
        executor.scheduleWithFixedDelay(new DataGenerationTask(), 0, 10, TimeUnit.SECONDS);
    }

    private List<String> populateCitiesList(String filename) throws IOException  {
        List<String> allCities = new ArrayList<>();

        try (
            InputStream is = getClass().getResourceAsStream(filename);
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);) {

            String line;
            int sequenceId = 10000;
            while ((line = br.readLine()) != null) {
                StringBuilder sb = new StringBuilder();
                
                sb.append(sequenceId);
                sb.append("-");
                sb.append(line);
                
                allCities.add(sb.toString());
                sequenceId++;
            }
        }
        
        return allCities;
    }

    public static BaseStationSensorServer getService() {
        return BaseStationSensorSingleton.INSTANCE.getService();
    }
    
    public void addSensorReadingListener(SensorReadingListener listener) {
        listeners.add(listener);
    }

    private class DataGenerationTask implements Runnable {

        @Override
        public void run() {
            for (String stationId : stationIds) {
                float voltage = (float) generator.nextUniform(108, 124, true);
                String baseStationId = stationId;
                VoltageReading reading = new VoltageReading(baseStationId, voltage);
                listeners.forEach((listener) -> {
                    listener.receivedVoltageReading(reading);
                });
            }
        }
    } 
}
