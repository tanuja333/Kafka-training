package com.cloudera.training.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TopicExistsException;

public class DrEvil {

    private AdminClient adminClient;
    private Properties properties = new Properties();
    private CreateTopicsOptions cto;

    public void configure(String bootstrapServers) {

        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("client.id", "DrEvil");

    }
    
    public boolean topicExists(String topicName) throws Exception {
        adminClient = AdminClient.create(properties);
        Collection<String> allTopics = null;

        long initTimeOut = 5000;
        allTopics = new TreeSet<>(adminClient.listTopics().names().get(initTimeOut, TimeUnit.MILLISECONDS));
        
        for (String t : allTopics) {
            if (t.equals(topicName)) {
                return true;
            }
        }

        return false;
    }

    public void createTopic(String topicName, int partitionCount) throws ExecutionException, InterruptedException {

        adminClient = AdminClient.create(properties);

        // Get brokers' IDs
        System.out.println("Getting brokers");
        
        DescribeClusterResult dcr = adminClient.describeCluster();
        Collection<Node> nodes = dcr.nodes().get();
        Node[] nodesArray = nodes.toArray(new Node[0]);
        
        List<Integer> replicas = new ArrayList<Integer>();
        
        // Create list of brokers, minus one - we will use this
        // to create an unbalanced topic.  The *order* of the nodes
        // in this list determines the leader, too.  Thus, the created
        // topic will have the same leader for each partition :-)
        for(int i = 1; i < nodesArray.length; i++) {
            replicas.add(nodesArray[i].id());
        }
        
        cto = new CreateTopicsOptions();

        HashMap<Integer, List<Integer>> m = new HashMap<Integer, List<Integer>>();
        for (int i = 0; i < partitionCount; i++) {
            m.put(i, new ArrayList<Integer>(replicas));
        }
        try {
            final NewTopic newTopic = new NewTopic(topicName, m);

            final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));

            // Since the call is Async, Lets wait for it to complete.
            createTopicsResult.values().get(topicName).get();
            System.out.printf("Topic %s created successfully", topicName);
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.out.printf("Topic %s already exists.  Use data_only flag%n", topicName);
                throw new RuntimeException(e.getMessage(), e);

            } else {
                throw new RuntimeException(e.getMessage(), e);
            }

        }
        adminClient.close();

    }

    public void createBadTopic(String topicName, int partitions, short replicaFactor)
            throws ExecutionException, InterruptedException {
        adminClient = AdminClient.create(properties);
        try {
            final NewTopic newTopic = new NewTopic(topicName, partitions, replicaFactor);
            final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));

            // Since the call is Async, Lets wait for it to complete.
            createTopicsResult.values().get(topicName).get();
            System.out.printf("Topic %s created successfully", topicName);
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.out.printf("Topic %s already exists.  Use data_only flag to write data only.", topicName);
                throw new RuntimeException(e.getMessage(), e);
            } else {
                throw new RuntimeException(e.getMessage(), e);
            }

        }

        adminClient.close();
    }
}
