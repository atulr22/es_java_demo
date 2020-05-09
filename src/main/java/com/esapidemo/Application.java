package com.esapidemo;

import com.esapidemo.config.ClientConfiguration;
import com.esapidemo.config.indexConfiguration;
import com.esapidemo.model.Person;
import com.esapidemo.service.ClusterHealthService;
import com.esapidemo.service.PersonService;

import org.elasticsearch.client.RestHighLevelClient;


import java.io.IOException;


/**
 * @author aadiyogi
 */

public class Application {

    public static void main(String[] args) throws IOException {

        System.out.println("Start Connection with ES and create REST Client");

        RestHighLevelClient restHighLevelClient = ClientConfiguration.getRestHighLevelClient();

        System.out.println("Connection with ES established and Rest Client Created");

//        ClusterHealthService clusterService = new ClusterHealthService(restHighLevelClient);
//        clusterService.clusterHealth();

        String index = "person";
        System.out.println("Creating Index " + index);

        indexConfiguration createIndexConfiguration = new indexConfiguration(restHighLevelClient);
        createIndexConfiguration.createIndex(index);

        System.out.println("Inserting a new Person with name XYZ...");

        PersonService personService = new PersonService(index, restHighLevelClient);
        Person person = new Person();
        person.setName("XYZ");

        person = personService.insertPerson(person);
        System.out.println("Person inserted --> " + person);

        System.out.println("Changing name to `XYZ xyz`...");

        person.setName("XYZ xyz");
        personService.updatePersonById(person.getPersonId(), person);
        System.out.println("Person updated  --> " + person);

        System.out.println("Getting XYZ...");
        Person personFromDB = personService.getPersonById(person.getPersonId());
        System.out.println("Person from DB  --> " + personFromDB);

//        System.out.println("Deleting XYZ...");
//        personService.deletePersonById(personFromDB.getPersonId());
//        System.out.println("Person Deleted");

        ClientConfiguration.closeConnection();
    }
}