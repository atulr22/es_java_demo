package com.esapidemo.service;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;

/**
 * @author aadiyogi
 */
public class ClusterHealthService {
    private RestHighLevelClient restHighLevelClient;

    public  ClusterHealthService(RestHighLevelClient restHighLevelClient){
        this.restHighLevelClient = restHighLevelClient;
    }

    public void clusterHealth() throws IOException {

        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse response = restHighLevelClient.cluster().health(request, RequestOptions.DEFAULT);

        String clusterName = response.getClusterName();
        ClusterHealthStatus status = response.getStatus();
        boolean timedOut = response.isTimedOut();
        RestStatus restStatus = response.status();
        int numberOfNodes = response.getNumberOfNodes();
        int numberOfDataNodes = response.getNumberOfDataNodes();
        Map<String, ClusterIndexHealth> indices = response.getIndices();

        System.out.println("Cluster Response is timedout : " + timedOut);
        System.out.println("CResponse Status: " + restStatus);

        System.out.println("Cluster Name is : " + clusterName);
        System.out.println("Cluster " + clusterName + " status is : " + status);
        System.out.println("Number of Nodes in " + clusterName + " are : " + numberOfNodes);
        System.out.println("Number of Data Nodes in " + clusterName + " are : " + numberOfDataNodes);



        System.out.println("Indices Size : " + indices.size());




        GetIndexRequest request1 = new GetIndexRequest("*");
        GetIndexResponse response1 = restHighLevelClient.indices().get(request1, RequestOptions.DEFAULT);
        String[] indices1 = response1.getIndices();

        for(String str : indices1){
            System.out.println("Indicies : " + str);
        }

        //     System.out.println("Indicies size  : " + indices.size());
        //     System.out.println("Indicies  is Empty : " + indices.isEmpty());
//        System.out.println("Indices Information: ");
//        indices.forEach((k,v) -> System.out.println("Key = "
//                + k + ", Value = " + v));



        //  ClusterIndexHealth clusterIndexHealth = indices.get("orders");
//        ClusterHealthStatus indexStatus = index.getStatus();
//        int numberOfShards = clusterIndexHealth.getNumberOfShards();
//        int numberOfReplicas = clusterIndexHealth.getNumberOfReplicas();
//        int activeShards = clusterIndexHealth.getActiveShards();
//        int activePrimaryShards = clusterIndexHealth.getActivePrimaryShards();
//        int initializingShards = clusterIndexHealth.getInitializingShards();
//        int relocatingShards = clusterIndexHealth.getRelocatingShards();
//        int unassignedShards = clusterIndexHealth.getUnassignedShards();



//        Map<Integer, ClusterShardHealth> shards = clusterIndexHealth.getShards();
//        ClusterShardHealth shardHealth = shards.get(0);
//        int shardId = shardHealth.getShardId();
//        ClusterHealthStatus shardStatus = shardHealth.getStatus();
//        int active = shardHealth.getActiveShards();
//        int initializing = shardHealth.getInitializingShards();
//        int unassigned = shardHealth.getUnassignedShards();
//        int relocating = shardHealth.getRelocatingShards();
//        boolean primaryActive = shardHealth.isPrimaryActive();

    }


}
