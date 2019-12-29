/**
 * Copyright (C) Cloudera, Inc. 2019
 */
package com.cloudera.training.kafka.datagen;

import java.util.Random;

/**
 * This class simply generates random strings, of a specified length, which are 
 * composed of alphanumeric characters.
 */
public class RandomStringGenerator {

    private static final String ALLOWED_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    private final Random random;
    private final int minStringLength;
    private final int outerBounds;
    private final StringBuilder builder;

    public RandomStringGenerator(int minStringLength, int maxStringLength)  {
        random = new Random();

        if (minStringLength < 1 || maxStringLength < minStringLength) {
            throw new IllegalArgumentException("Invalid length specified");
        }

        this.minStringLength = minStringLength;
        this.outerBounds = maxStringLength - minStringLength + 1;
        this.builder = new StringBuilder();
    }
    
    public String generateRandomString() {
        builder.delete(0, builder.length());

        int stringLength = random.nextInt(outerBounds) + minStringLength;
        int index = random.nextInt(ALLOWED_CHARACTERS.length());
        String repeatedChars = new String(new char[stringLength]).replace("\0", 
                ALLOWED_CHARACTERS.substring(index, index+1));

        return repeatedChars;
    }
}
