package io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaMessageListener;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.IndexDescription;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.IndexService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage.DocumentStorage;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.CollectionUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.DocumentUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.Field.SYSTEM_LAST_UPDATE;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 1:38 PM
 **/

public final class StoreToMongoMessageListener implements KafkaMessageListener<String, String> {
    private static final Logger logger = LoggerFactory.getLogger(StoreToMongoMessageListener.class);

    private final DocumentStorage documentStorage;
    private final IndexService indexService;
    private final Duration documentsRetention;
    private final String topic;
    private final String collectionName;
    private final String updatesCollectionName;

    public StoreToMongoMessageListener(DocumentStorage documentStorage, IndexService indexService, Duration documentsRetention, String topic) {
        this.documentStorage = documentStorage;
        this.indexService = indexService;
        this.documentsRetention = documentsRetention;
        this.topic = topic;
        this.collectionName = topic;
        this.updatesCollectionName = MongoCollections.makeUpdatesCollectionName(topic);
        this.init();
    }

    private void init() {
        indexService.dropIndex(collectionName, SYSTEM_LAST_UPDATE);
        indexService.dropIndex(updatesCollectionName, SYSTEM_LAST_UPDATE);

        IndexDescription indexDescription = new IndexDescription(SYSTEM_LAST_UPDATE, SYSTEM_LAST_UPDATE, documentsRetention);
        indexService.createIndex(collectionName, indexDescription);
        indexService.createIndex(updatesCollectionName, indexDescription);
    }

    private void storeToUpdates(List<ConsumerRecord<String, String>> messages) {
        List<Document> documents = DocumentUtils.fromRecords(KafkaUtils::uniqueId, messages);
        documentStorage.upsertAll(MongoCollections.makeUpdatesCollectionName(topic), documents);
    }

    private void storeToLatest(Collection<ConsumerRecord<String, String>> records) {
        Collection<ConsumerRecord<String, String>> compacted = CollectionUtils.compact(records, ConsumerRecord::key, KafkaUtils::keepLatest);
        Map<Boolean, List<ConsumerRecord<String, String>>> partitioned = compacted.stream()
                .collect(Collectors.partitioningBy(record -> record.value() != null));

        List<ConsumerRecord<String, String>> recordsToSave = partitioned.get(true);
        if (recordsToSave != null && !recordsToSave.isEmpty()) {
            documentStorage.upsertAll(topic, DocumentUtils.fromRecords(KafkaUtils::getKey, recordsToSave));
        }

        List<ConsumerRecord<String, String>> recordsToRemove = partitioned.get(false);
        if (recordsToRemove != null && !recordsToRemove.isEmpty()) {
            documentStorage.removeAll(topic, KafkaUtils.ids(recordsToRemove));
        }
    }

    @Override
    public void onMessages(List<ConsumerRecord<String, String>> messages, String topic) {
        logger.info("Processing {} messages, topic {}", messages.size(), topic);
        storeToUpdates(messages);
        storeToLatest(messages);
        logger.info("Processed {} messages, topic {}", messages.size(), topic);
    }

}
