package com.cloudera.training.kafka;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TrogdorJunior {


    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Grrrr");
        }
        String bootstrapServers = args[0];
        int troubleNumber = Integer.parseInt(args[1]);
        String topicName = args[2];
        boolean dataOnly = false;

        if(args.length == 4) {
            dataOnly = true;
        }
        
        if (dataOnly) {
            DrEvil dre = new DrEvil();
            dre.configure(bootstrapServers);
            try {
            if (!dre.topicExists(topicName)) {
                String msg = "Topic " + topicName + " doesn't exist. \n" + 
                             "Use data_only flag only if the topic exists.\n" +
                             "Nothing was done.  Exiting";
                System.err.println(msg);
                System.exit(1);
                
            }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
            
        if (dataOnly) {
            System.out.printf("This program will write data to topic \"%s\".  Enter \"y\" to continue.", topicName);
        } else {
            System.out.printf("This program will create a topic \"%s\" and write data to it.  Enter \"y\" to continue.", topicName);
        }
            
       
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in)); 
        try{
            String rad = br.readLine();
            if (!rad.equals("y")) {
                System.err.println("Nothing was done.  Exiting");
                System.exit(1);
            }
        }
        catch(Exception e){
            System.err.println(e.getMessage());
            System.out.println("Nothing was done.  Exiting");
            System.exit(1);
        }            

        if (troubleNumber == 1) {
            StrongBad sb = new StrongBad();
            sb.causeTrouble(bootstrapServers, topicName, dataOnly);
        } else if (troubleNumber == 2) {
            AnotherBad ab = new AnotherBad();
            ab.causeTrouble(bootstrapServers, topicName, dataOnly);
        }
        else {
            System.out.println("Unknown trouble number");
        }
        System.exit(0);
    }
}
