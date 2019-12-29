/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.serialization;

import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.log4j.Logger;

/**
 * Implementation of Kafka's Deserializer interface that can convert a byte array
 * (serialized with the corresponding SimpleAvroSerializer class) back into an
 * object equivalent to the original.
 * 
 * @param <T> the type generated from the Avro schema
 */
public class SimpleAvroDeserializer<T extends SpecificRecord> implements Deserializer<T> {
    
    private static final Logger logger = Logger.getLogger(SimpleAvroDeserializer.class);
    
    private SpecificDatumReader<T> reader;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Hack to work around Java's use of type erasure 
        // (http://download.oracle.com/javase/tutorial/java/generics/erasure.html)
        // and account for the fact that we can't directly reference the Customer
        // class because it hasn't been generated yet at the start of the exercise.
        String expectedClassName = "com.cloudera.training.kafka.data.Customer";
        Schema schema = null;
        try {
            Class<?> klass = Class.forName(expectedClassName);
            SpecificRecord instance = (SpecificRecord)klass.newInstance();
            schema = instance.getSchema();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            logger.error("Encountered problem in deserializer", ex);
        }

        reader = new SpecificDatumReader<>(schema);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {        
        T original = null;
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            original = reader.read(null, decoder);
        } catch (IOException ex) {
            logger.error("Encountered problem in deserializer", ex);
        }
        
        return original;
    }

    @Override
    public void close() {
        reader = null;
    }
}
