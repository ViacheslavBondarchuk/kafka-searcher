package io.github.viacheslavbondarchuk.kafkasearcher.mongo.management;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:02 PM
 **/

@Component
public class MongoCollectionManagementService {
    private final MongoTemplate mongoTemplate;


    public MongoCollectionManagementService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public void dropCollection(String collectionName) {
        mongoTemplate.dropCollection(collectionName);
    }
}
