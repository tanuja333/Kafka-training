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
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math3.exception.NumberIsTooLargeException;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;

/**
 * This class simulates a server that periodically (and continually) gathers 
 * data from base station voltage sensors and notifies interested parties 
 * about their values.
 * 
 * From the student's point of view, this should be seen as a black box, 
 * since it serves only to generate random (but realistic) data and has
 * nothing to do with Kafka specifically.
 */
public class BaseStationSensorServer {
    
    private final Logger logger = Logger.getLogger(BaseStationSensorServer.class);
    
    private final CopyOnWriteArraySet<VoltageSensorListener> listeners;
    private final ScheduledExecutorService executor;
    private final RandomDataGenerator generator;
    
    private final String[] stationIds;

    private enum BaseStationSensorSingleton {
        
        INSTANCE();
        
        private final BaseStationSensorServer service;
        
        private BaseStationSensorSingleton() {
            RandomDataGenerator generator = new RandomDataGenerator();
            service = new BaseStationSensorServer(generator, 5000);
        }
        
        BaseStationSensorServer getService() {
            return service;
        }
    }
 
    private BaseStationSensorServer(RandomDataGenerator generator, int numBaseStations) {
        this.listeners = new CopyOnWriteArraySet<>();
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
        } catch (IOException | NumberIsTooLargeException e) { 
            logger.error("Caught exception while populating list of base stations", e);
        }
  
        executor.scheduleWithFixedDelay(new DataGenerationTask(), 0, 1, TimeUnit.SECONDS);
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
    
    public void addVoltageSensorListener(VoltageSensorListener listener) {
        if (listener == null) {
            return;
        }

        listeners.add(listener);
    }
    
    public void removeVoltageSensorListener(VoltageSensorListener listener) {
        if (listener == null) {
            return;
        }

        listeners.remove(listener);
    }
    
    private void notifyListeners(final VoltageSensorReading reading) {
        listeners.forEach((listener) -> {
            listener.receivedNewReading(reading);
        });
    }

    private class DataGenerationTask implements Runnable {

        @Override
        public void run() {
            for (String stationId : stationIds) {
                float voltage = (float) generator.nextUniform(108, 124, true);
                String baseStationId = stationId;
                VoltageSensorReading reading = new VoltageSensorReading(baseStationId, voltage);

                notifyListeners(reading);
            }
        }
    } 
}
