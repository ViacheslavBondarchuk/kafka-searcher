package io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.IndexDescription;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.IndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.Field.ENTITY_ID;
import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.Field.SYSTEM_LAST_UPDATE;
import static org.springframework.data.domain.Sort.Direction.ASC;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 3:07 PM
 **/

@Service
public final class IndexServiceImpl implements IndexService {
    private static final Logger logger = LoggerFactory.getLogger(IndexServiceImpl.class);

    private final MongoTemplate mongoTemplate;

    public IndexServiceImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    private Index toIndexDefinition(IndexDescription indexDescription) {
        Index index = new Index(indexDescription.field(), ASC);
        Optional.ofNullable(indexDescription.expire()).ifPresent(index::expire);
        Optional.ofNullable(indexDescription.name()).ifPresent(index::named);
        return index;
    }

    @Override
    public void createIndex(String collection, IndexDescription indexDescription) {
        mongoTemplate.indexOps(collection)
                .ensureIndex(toIndexDefinition(indexDescription));
        mongoTemplate.indexOps(MongoCollections.makeUpdatesCollectionName(collection))
                .ensureIndex(toIndexDefinition(indexDescription));
    }

    @Override
    public void dropIndex(String collection, String field) {
        try {
            mongoTemplate.indexOps(collection)
                    .dropIndex(field);
            mongoTemplate.indexOps(MongoCollections.makeUpdatesCollectionName(collection))
                    .dropIndex(field);
        } catch (Exception ex) {
            logger.error("Could not drop index {} for collection {}. Error message {}", field, collection, ex.getMessage());
        }
    }

    @Override
    public void createRetentionDocumentsIndex(Set<String> collections, Duration duration) {
        IndexDescription indexDescription = new IndexDescription(ENTITY_ID, ENTITY_ID, duration);
        collections.forEach(collection -> createIndex(collection, indexDescription));
    }

    @Override
    public void recreateRetentionDocumentsIndex(Set<String> collections, Duration duration) {
        IndexDescription indexDescription = new IndexDescription(SYSTEM_LAST_UPDATE, SYSTEM_LAST_UPDATE, duration);
        collections.forEach(collection -> {
            dropIndex(collection, SYSTEM_LAST_UPDATE);
            createIndex(collection, indexDescription);
        });
    }


}
