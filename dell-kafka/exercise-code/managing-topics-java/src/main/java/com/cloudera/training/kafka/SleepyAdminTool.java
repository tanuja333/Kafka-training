package com.cloudera.training.kafka;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;


/**
 * Utility class to open an admin connection and sleep.
 * run this program more than once to see multiple instances.
 */
public class SleepyAdminTool implements AutoCloseable {

    private AdminClient adminClient;

    public SleepyAdminTool(Properties myProps)  {

        Properties properties = new Properties();
        properties.putAll(myProps);
        adminClient = AdminClient.create(properties);

    }

    public static void main(String args[]) {

        if (args.length < 2) {
            System.out.println("Usage: SleepyAdminTool <brokerlist> <sleep_seconds>");
            System.exit(0);

        }

        // List of Message Brokers
        String bootstrap = args[0];
        
        String sleepSecondArg = args[1];
        int sleepSeconds = Integer.parseInt(sleepSecondArg);

        Properties properties = new Properties();
        properties.setProperty("metadata.max.age.ms", "3000");
        properties.setProperty("bootstrap.servers", bootstrap);

        try(SleepyAdminTool tool = new SleepyAdminTool(properties)) {
            System.out.printf("Sleeping for %d\n", sleepSeconds);
            for (int i = 0; i < sleepSeconds; i++) {
                Thread.sleep(1000);
                System.out.printf("%d of %d\n", i, sleepSeconds);
            }
            System.out.println("Exiting");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        adminClient.close();
    }

}
