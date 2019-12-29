package com.cloudera.training.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

public class CreateTopicTool implements AutoCloseable {

    private AdminClient adminClient;

    public CreateTopicTool(Properties myProps) {

        Properties properties = new Properties();
        properties.putAll(myProps);
        adminClient = AdminClient.create(properties);

    }

    public static void main(String args[]) {

        if (args.length < 2) {
            System.out.println("Usage: CreateTopicTool <Bootstrap Servers> <Topic Name> [num replicas]");
            System.exit(0);

        }

        // List of Message Brokers
        String bootstrap = args[0];

        // The topic name
        String topicName = args[1];

        // Number of replicas
        String numReplicasArg = "3";
        if (args.length == 3) {
            numReplicasArg = args[2];
        }
        short numReplicas = Short.parseShort(numReplicasArg);

        // This can be args although here are just hardcoded
        int default_NumberPartitions = 3;

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        // client.id (should) uniquely identify this client/group of clients.
        // Used for logging, tracing of activity.  Should typically be a logical
        // application name, with a possible unique identifier
        properties.setProperty("client.id", "CreateTopicTool");

        try(CreateTopicTool tool = new CreateTopicTool(properties)) {
            // Create a Topic
            tool.createTopic(topicName, default_NumberPartitions, numReplicas);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void createTopic(String topicName, int partitions, short replicaFactor)
            throws ExecutionException, InterruptedException {
        try {
            final NewTopic newTopic = new NewTopic(topicName, partitions, replicaFactor);
            final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));

            // Since the call is Async, Lets wait for it to complete.
            createTopicsResult.values().get(topicName).get();
            System.out.printf("Topic %s created successfully", topicName);
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.out.printf("Topic %s already exists",  topicName);
            }
            else {
                throw new RuntimeException(e.getMessage(), e);
            }

        }
    }

    @Override
    public void close() throws Exception {
        adminClient.close();
    }

}
