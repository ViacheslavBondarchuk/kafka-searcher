package io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.IndexDescription;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.IndexService;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.stereotype.Service;

import java.util.Optional;

import static org.springframework.data.domain.Sort.Direction.ASC;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 3:07 PM
 **/

@Service
public final class IndexServiceImpl implements IndexService {
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
    }

    @Override
    public void dropIndex(String collection, String field) {
        mongoTemplate.indexOps(collection)
                .dropIndex(field);
    }

}
