package com.cloudera.training.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;

public class DeleteTopicTool {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Usage: DeleteTopicTool <Bootstrap Servers> <Topic Name>");
            System.exit(0);

        }

        // List of Message Brokers
        String bootstrap = args[0];

        // The topic name
        String topicName = args[1];

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        
        try(AdminClient adminClient = AdminClient.create(properties)) {
            final DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topicName));
            // Since the call is Async, Lets wait for it to complete.
            result.values().get(topicName).get();
            System.out.printf("Successfully deleted topic %s\n", topicName);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }
}
