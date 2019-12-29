/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka;

//import com.cloudera.training.kafka.data.Activation;
//import java.io.File;
//import java.io.IOException;
//import org.apache.avro.Schema;
//import org.apache.avro.file.DataFileWriter;
//import org.apache.avro.io.DatumWriter;
//import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;


public class ActivationCreator {

    private static final Logger logger = Logger.getLogger(ActivationCreator.class);

    public static void main(String[] args) {
//        // Example of how to create an Activation object by calling the default 
//        // (zero argument) constructor and then setting each field individually
//        Activation one = new Activation();
//        one.setCustomerId(200000);
//        one.setDeviceId("AF24D037");
//        one.setModel("iPhone XR");
//        one.setPhoneNumber("213-555-7523");
//        one.setTimestamp(System.currentTimeMillis());
//        
//        // Example of how to define all the fields and then pass them to the 
//        // constructor in order to create a new Activation object
//        long timestamp = System.currentTimeMillis();
//        int customerId = 200001;
//        String deviceId = "BC7F05D2";
//        String model = "Samsung Galaxy S9";
//        String phoneNumber = "415-555-6841";
//        Activation two = new Activation(timestamp, customerId, deviceId, phoneNumber, model);
//
//        // Get a reference to the schema, which will be embedded into the file
//        Schema activationSchema = Activation.getClassSchema();
//        
//        // Get a writer that is capable of serializing Avro records as binary data
//        DatumWriter<Activation> datumWriter = new SpecificDatumWriter<>(activationSchema);
//        
//        // Use that to get a writer that can write those records to a file
//        DataFileWriter<Activation> dataFileWriter = new DataFileWriter<>(datumWriter);
//        
//        try {
//            // Create a new file at the path below
//            File file = new File("/home/training/Desktop/activations.avro");
//            if (file.exists()) {
//                file.delete();
//            }
//            dataFileWriter.create(activationSchema, file);
//
//            dataFileWriter.append(one);
//            dataFileWriter.append(two);
//
//            dataFileWriter.close();
//        } catch (IOException e) {
//            logger.error(e);
//        }
    }
}
