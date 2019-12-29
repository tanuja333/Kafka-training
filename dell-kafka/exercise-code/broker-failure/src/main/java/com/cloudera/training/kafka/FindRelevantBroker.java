package com.cloudera.training.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

public class FindRelevantBroker implements AutoCloseable {

    private AdminClient adminClient;
    private int initTimeOut = 1000 * 100;

    public FindRelevantBroker(Properties myProps)  {
        Properties properties = new Properties();
        properties.putAll(myProps);
        adminClient = AdminClient.create(properties);
    }

    public static void main(String args[]) {
        if (args.length < 2) {
            System.out.println("Usage: FindRelevantBroker <BootstrapServers> <TopicName>");
            System.exit(0);
        }

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", args[0]);
        
        String topicName = args[1];

        // List all topics
        try(FindRelevantBroker tool = new FindRelevantBroker(properties)) {
            TopicDescription desc = tool.getTopicDescription(topicName);
            List<TopicPartitionInfo> partitions = desc.partitions();
            partitions.forEach((partition) -> {
                Node leader = partition.leader();
                System.out.printf("Partition #%d of topic '%s' has leader on host '%s'%n", 
                        partition.partition(), topicName, leader.host());
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
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
