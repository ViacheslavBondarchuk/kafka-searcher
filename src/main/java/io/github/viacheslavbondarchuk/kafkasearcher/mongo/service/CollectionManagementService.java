package io.github.viacheslavbondarchuk.kafkasearcher.mongo.service;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;

import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.System.FIELDS;
import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.System.TOPICS;

/**
 * author: vbondarchuk
 * date: 5/16/2024
 * time: 1:31 PM
 **/

@Service
public class CollectionManagementService {
    private static final Set<String> systemCollections = Set.of(TOPICS, FIELDS);

    private final MongoTemplate mongoTemplate;

    public CollectionManagementService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public void dropCollection(String collectionName) {
        mongoTemplate.dropCollection(collectionName);
    }

    public void createCollection(String collectionName) {
        mongoTemplate.createCollection(collectionName);
    }

    public void recreateCollection(String collectionName) {
        dropCollection(collectionName);
        createCollection(collectionName);
    }

    public void removeAllNonSystemCollections() {
        for (String collectionName : mongoTemplate.getCollectionNames()) {
            if (!systemCollections.contains(collectionName)) {
                dropCollection(collectionName);
            }
        }
    }


}
