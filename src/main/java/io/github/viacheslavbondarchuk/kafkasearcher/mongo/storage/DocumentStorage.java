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
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 11:19 AM
 **/

@Component
public class DocumentStorage {
    public static final String MONGO_ID_KEY = "_id";
    public static final String ENTITY_ID_KEY = "id";

    private static final Query REMOVE_ALL_QUERY = Query.query(Criteria.where(MONGO_ID_KEY));

    private final MongoTemplate mongoTemplate;
    private final TransactionTemplate transactionTemplate;

    public DocumentStorage(MongoTemplate mongoTemplate, TransactionTemplate transactionTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    private Update createUpsertUpdate(Document document) {
        Update update = new Update();
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            update.set(entry.getKey(), entry.getValue());
        }
        return update;
    }

    public void executeInTransaction(Consumer<TransactionStatus> consumer) {
        transactionTemplate.execute(status -> {
            consumer.accept(status);
            return null;
        });
    }

    public void save(String topic, Document document) {
        mongoTemplate.save(document, topic);
    }

    public void saveAll(String topic, Collection<Document> documents) {
        mongoTemplate.insert(documents, topic);
    }

    public void upsertAll(String topic, Collection<Document> documents) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, topic);
        List<Pair<Query, Update>> updates = new ArrayList<>();
        for (Document document : documents) {
            updates.add(Pair.of(
                    Query.query(Criteria.where(MONGO_ID_KEY).is(DocumentUtils.id(document))),
                    createUpsertUpdate(document)
            ));
        }
        bulkOperations.upsert(updates);
        bulkOperations.execute();
    }

    public void remove(String topic, String id) {
        mongoTemplate.remove(Query.query(Criteria.where(MONGO_ID_KEY).is(id)), topic);
    }

    public void removeAll(String topic, Set<String> ids) {
        mongoTemplate.remove(Query.query(Criteria.where(MONGO_ID_KEY).in(ids)), topic);
    }

    public void removeAll(String topic) {
        mongoTemplate.remove(REMOVE_ALL_QUERY, topic);
    }

}
