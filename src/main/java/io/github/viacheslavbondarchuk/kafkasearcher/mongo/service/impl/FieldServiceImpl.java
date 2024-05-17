package io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.FieldDescription;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.IndexDescription;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.FieldService;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Optional;
import java.util.function.Function;

import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.Field.FIELD;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 4:56 PM
 **/

@Service
public final class FieldServiceImpl implements FieldService {
    private static final Function<String, Query> queryByFieldNameFactory = fieldName -> Query.query(Criteria.where(FIELD).is(fieldName));

    private final MongoTemplate mongoTemplate;

    public FieldServiceImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    private Optional<FieldDescription> findFieldDescription(String field) {
        return Optional.ofNullable(mongoTemplate.findOne(
                queryByFieldNameFactory.apply(field), FieldDescription.class));
    }

    private FieldDescription updateFieldDescription(FieldDescription source, FieldDescription target) {
        if (target.getIndexDescription() == null) {
            target.setIndexDescription(source.getIndexDescription());
        }
        return target;
    }

    @Override
    public void save(FieldDescription fieldDescription) {
        mongoTemplate.save(findFieldDescription(fieldDescription.getName())
                .map(fd -> updateFieldDescription(fd, fieldDescription))
                .orElse(fieldDescription));
    }

    @Override
    public boolean exists(String field) {
        return mongoTemplate.exists(queryByFieldNameFactory.apply(field), FieldDescription.class);
    }
}
