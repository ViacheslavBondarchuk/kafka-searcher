package io.github.viacheslavbondarchuk.kafkasearcher.mongo.service;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.registry.KafkaConsumerRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.SearchResult;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.QueryUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.web.domain.SearchRequest;
import io.github.viacheslavbondarchuk.kafkasearcher.web.domain.SearchType;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

import static io.github.viacheslavbondarchuk.kafkasearcher.web.domain.SearchType.ACTUAL;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 9:35 PM
 **/

@Service
public class SearchService {
    private final MongoTemplate mongoTemplate;
    private final KafkaConsumerRegistry<String, String> kafkaConsumerRegistry;

    public SearchService(MongoTemplate mongoTemplate, KafkaConsumerRegistry<String, String> kafkaConsumerRegistry) {
        this.mongoTemplate = mongoTemplate;
        this.kafkaConsumerRegistry = kafkaConsumerRegistry;
    }

    private String getCollectionName(SearchType searchType, String topic) {
        return searchType == ACTUAL ? topic : MongoCollections.makeUpdatesCollectionName(topic);
    }

    public SearchResult<List<Document>> search(SearchRequest request) {
        String collectionName = getCollectionName(request.searchType(), request.topic());
        return new SearchResult<>(
                mongoTemplate.count(QueryUtils.countQuery(request), Document.class, collectionName),
                mongoTemplate.find(QueryUtils.searchQuery(request), Document.class, collectionName)
        );
    }
}
