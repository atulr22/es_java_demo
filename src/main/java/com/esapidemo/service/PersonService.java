package com.esapidemo.service;

import com.esapidemo.model.Person;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author aadiyogi
 */
public class PersonService {

    private  String INDEX;
    private  RestHighLevelClient restHighLevelClient;
    private static ObjectMapper objectMapper = new ObjectMapper();

   public  PersonService(String index , RestHighLevelClient restHighLevelClient){
        this.INDEX = index;
        this.restHighLevelClient = restHighLevelClient;
    }


    public Person insertPerson(Person person){
        person.setPersonId(UUID.randomUUID().toString());
        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("personId", person.getPersonId());
        dataMap.put("name", person.getName());



//        Below API has been Depricated
//        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, person.getPersonId())
//                .source(dataMap);

        IndexRequest indexRequest = new IndexRequest(INDEX)
                .id(person.getPersonId()).source(dataMap);
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest , RequestOptions.DEFAULT);
        } catch(ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (java.io.IOException ex){
            ex.getLocalizedMessage();
        }
        return person;
    }

    public Person getPersonById(String id){


        // Below API has been Depricated
        // GetRequest getPersonRequest = new GetRequest(INDEX, TYPE, id);

        GetRequest getPersonRequest = new GetRequest(INDEX, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(getPersonRequest , RequestOptions.DEFAULT);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        return getResponse != null ?
                objectMapper.convertValue(getResponse.getSourceAsMap(), Person.class) : null;
    }

    public  Person updatePersonById(String id, Person person){


        // Below API has been Depricated
//        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, id)
//                .fetchSource(true);    // Fetch Object after its update

        UpdateRequest updateRequest = new UpdateRequest(INDEX , id).fetchSource(true);
        try {
            String personJson = objectMapper.writeValueAsString(person);
            updateRequest.doc(personJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest , RequestOptions.DEFAULT);
            return objectMapper.convertValue(updateResponse.getGetResult().sourceAsMap(), Person.class);
        }catch (JsonProcessingException e){
            e.getMessage();
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        System.out.println("Unable to update person");
        return null;
    }

    public void deletePersonById(String id) {

        // Below API has been Depricated
//        DeleteRequest deleteRequest = new DeleteRequest(INDEX, TYPE, id);


        DeleteRequest deleteRequest = new DeleteRequest(INDEX , id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest , RequestOptions.DEFAULT);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
    }
}
