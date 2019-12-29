package com.cloudera.training.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;

public class ListTopicsTool implements AutoCloseable {

    private AdminClient adminClient;
    private int initTimeOut = 1000 * 100;

    public ListTopicsTool(Properties myProps)  {

        Properties properties = new Properties();
        properties.putAll(myProps);
        adminClient = AdminClient.create(properties);

    }

    public static void main(String args[]) {

        if (args.length < 1) {
            System.out.println("Usage: ListTopicTool <Bootstrap Servers>");
            System.exit(0);

        }

        // List of Message Brokers
        String bootstrap = args[0];

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        

        // List all topics
        try(ListTopicsTool tool = new ListTopicsTool(properties)) {
            Collection<String> topics = tool.listAllTopics();
            topics.forEach((t) -> {
                try {
                    String desc = tool.getTopicDescription(t).toString();
                    System.out.println(desc.replace(",", ",\n\t"));
                } catch (Exception e) {
                    e.printStackTrace();
                }

            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Collection<String> listAllTopics() throws Exception {
        Collection<String> allTopics = null;
        try {
            allTopics = new TreeSet<>(adminClient.listTopics().names().get(initTimeOut, TimeUnit.MILLISECONDS));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new Exception(e);
        }
        return allTopics;
    }

    private TopicDescription getTopicDescription(String topicName) throws Exception {
        try {
            return adminClient.describeTopics(Collections.unmodifiableList(Arrays.asList(topicName))).values()
                    .get(topicName).get(initTimeOut, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new Exception(e);
        }
    }

    @Override
    public void close() throws Exception {
        adminClient.close();
    }

}
