package io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage;

import io.github.viacheslavbondarchuk.kafkasearcher.utils.DocumentUtils;
import org.bson.Document;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.Field.MONGO_ID;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 11:19 AM
 **/

@Component
public class DocumentStorage {
    private final MongoTemplate mongoTemplate;

    public DocumentStorage(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    private Update createUpsertUpdate(Document document) {
        Update update = new Update();
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            update.set(entry.getKey(), entry.getValue());
        }
        return update;
    }

    public void saveAll(String topic, Collection<Document> documents) {
        mongoTemplate.insert(documents, topic);
    }

    public void upsertAll(String topic, Collection<Document> documents) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, topic);
        List<Pair<Query, Update>> updates = new ArrayList<>();
        for (Document document : documents) {
            updates.add(Pair.of(
                    Query.query(Criteria.where(MONGO_ID).is(DocumentUtils.id(document))),
                    createUpsertUpdate(document)
            ));
        }
        bulkOperations.upsert(updates);
        bulkOperations.execute();
    }

    public void removeAll(String topic, Set<String> ids) {
        mongoTemplate.remove(Query.query(Criteria.where(MONGO_ID).in(ids)), topic);
    }

}
