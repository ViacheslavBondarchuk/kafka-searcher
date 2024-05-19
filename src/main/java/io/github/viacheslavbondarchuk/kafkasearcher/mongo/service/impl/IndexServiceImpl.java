package io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.IndexDescription;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.IndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.springframework.data.domain.Sort.Direction.ASC;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 3:07 PM
 **/

@Service
public final class IndexServiceImpl implements IndexService {
    private static final Logger logger = LoggerFactory.getLogger(IndexServiceImpl.class);

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

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
        executorService.submit(() -> {
            try {
                mongoTemplate.indexOps(collection)
                        .ensureIndex(toIndexDefinition(indexDescription));
            } catch (Exception ex) {
                logger.error("Could not create index {} for collection {}. Error message {}", indexDescription, collection, ex.getMessage());
            }
        });
    }

    @Override
    public void dropIndex(String collection, String field) {
        executorService.submit(() -> {
            try {
                mongoTemplate.indexOps(collection)
                        .dropIndex(field);
            } catch (Exception ex) {
                logger.error("Could not drop index {} for collection {}. Error message {}", field, collection, ex.getMessage());
            }
        });

    }

}
