package com.esapidemo.config;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;

import java.io.IOException;

/**
 * @author aadiyogi
 */
public class indexConfiguration {


    private  RestHighLevelClient restHighLevelClient = null;

       public indexConfiguration (RestHighLevelClient restHighLevelClient)
        {
            this.restHighLevelClient = restHighLevelClient;
        }

    public  void createIndex(String index) throws IOException {



        CreateIndexRequest createIndexrequest = new CreateIndexRequest(index);
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexrequest, RequestOptions.DEFAULT);

        boolean acknowledged = createIndexResponse.isAcknowledged();
      //  INDEX = createIndexResponse.index();

        if(acknowledged)
            System.out.println("New Index  " + createIndexResponse.index() + " created");
        else
            System.out.println("New Index  not created");
        //return  acknowledged;

        //return INDEX;

    }
}
