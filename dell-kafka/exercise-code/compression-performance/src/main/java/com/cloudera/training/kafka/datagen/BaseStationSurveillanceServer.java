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
import org.apache.commons.math3.exception.NumberIsTooLargeException;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Logger;

/**
 * This class simulates a server that periodically (and continually) gathers 
 * data from base stations (in this case, images from a surveillance camera) 
 * and notifies interested parties about their values.
 * 
 * From the student's point of view, this should be seen as a black box, 
 * since it serves only to generate random (but realistic) data and has
 * nothing to do with Kafka specifically.
 */
public class BaseStationSurveillanceServer {
    
    private final Logger logger = Logger.getLogger(BaseStationSurveillanceServer.class);
    
    private final String[] stationIds;    
    private final RandomDataGenerator random;
    private final ImageGenerator imageGenerator;

    private enum BaseStationSensorSingleton {
        
        INSTANCE();
        
        private final BaseStationSurveillanceServer service;
        
        private BaseStationSensorSingleton() {
            service = new BaseStationSurveillanceServer(5000);
        }
        
        BaseStationSurveillanceServer getService() {
            return service;
        }
    }

    public static BaseStationSurveillanceServer getService() {
        return BaseStationSensorSingleton.INSTANCE.getService();
    }

    public ImageEvent getImageEvent() throws IOException {
        int index = random.nextInt(0, stationIds.length - 1);
        String stationId = stationIds[index];
        
        byte[] image = imageGenerator.getJpegAsByteArray();
        
        return new ImageEvent(stationId, image);
    }
 
    private BaseStationSurveillanceServer(int numBaseStations) {
        this.random = new RandomDataGenerator();
        this.imageGenerator = new ImageGenerator(1024, 768, 1.0f);

        // populate potential list of base station IDs from a list of city names
        // in a text file and prepending a sequence number to each one
        stationIds = new String[numBaseStations];

        try {
            List<String> allStationIds = populateCitiesList("/cities.txt");
            
            // get a unique list of base station IDs from that list of cities
            Set<String> cityList = new HashSet<>();
            while (cityList.size() < numBaseStations) {
                int index = random.nextInt(0, allStationIds.size() - 1);
                cityList.add(allStationIds.get(index));
            }

            cityList.toArray(stationIds);
        } catch (IOException | NumberIsTooLargeException e) { 
            logger.error("Caught exception while populating list of base stations", e);
        }
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
}
