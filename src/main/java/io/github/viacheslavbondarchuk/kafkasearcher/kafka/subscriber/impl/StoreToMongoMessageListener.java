package io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaMessageListener;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage.DocumentStorage;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.CollectionUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.DocumentUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 1:38 PM
 **/

public final class StoreToMongoMessageListener implements KafkaMessageListener<String, String> {
    private static final Logger logger = LoggerFactory.getLogger(StoreToMongoMessageListener.class);
    private final DocumentStorage documentStorage;

    public StoreToMongoMessageListener(DocumentStorage documentStorage) {
        this.documentStorage = documentStorage;
    }

    private void storeToUpdates(List<ConsumerRecord<String, String>> messages, String topic) {
        List<Document> documents = DocumentUtils.fromRecords(messages);
        documentStorage.saveAll(MongoCollections.makeUpdatesCollectionName(topic), documents);
    }

    private void storeToLatest(Collection<ConsumerRecord<String, String>> records, String topic) {
        Collection<ConsumerRecord<String, String>> compacted = CollectionUtils.compact(records, ConsumerRecord::key, KafkaUtils::keepLatest);
        Map<Boolean, List<ConsumerRecord<String, String>>> partitioned = compacted.stream()
                .collect(Collectors.partitioningBy(record -> record.value() != null));

        List<ConsumerRecord<String, String>> recordsToSave = partitioned.get(true);
        if (recordsToSave != null && !recordsToSave.isEmpty()) {
            documentStorage.upsertAll(topic, DocumentUtils.fromRecords(recordsToSave));
        }

        List<ConsumerRecord<String, String>> recordsToRemove = partitioned.get(false);
        if (recordsToRemove != null && !recordsToRemove.isEmpty()) {
            documentStorage.removeAll(topic, KafkaUtils.ids(recordsToRemove));
        }
    }

    @Override
    public void onMessages(List<ConsumerRecord<String, String>> messages, String topic) {
        logger.info("Processing {} messages, topic {}", messages.size(), topic);
        storeToUpdates(messages, topic);
        storeToLatest(messages, topic);
        logger.info("Processed {} messages, topic {}", messages.size(), topic);
    }

}
