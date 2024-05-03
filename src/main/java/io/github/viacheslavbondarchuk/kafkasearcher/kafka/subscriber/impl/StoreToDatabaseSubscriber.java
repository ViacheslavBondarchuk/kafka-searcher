package io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement.Acknowledgement;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.RecordsBatch;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaConsumerSubscriber;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.management.MongoCollectionManagementService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage.DocumentStorage;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.AsyncUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.CollectionUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.DocumentUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.UPDATES_PREFIX;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 11:10 AM
 **/

public final class StoreToDatabaseSubscriber implements KafkaConsumerSubscriber<RecordsBatch<String, String>> {
    private static final Duration DURATION = Duration.ofMillis(5);

    private final MongoCollectionManagementService collectionManagementService;
    private final DocumentStorage documentStorage;
    private final String collectionName;
    private final String updateCollectionName;
    private final ExecutorService executorService;

    public StoreToDatabaseSubscriber(MongoCollectionManagementService collectionManagementService,
                                     DocumentStorage documentStorage,
                                     String collectionName,
                                     ExecutorService executorService) {
        this.collectionManagementService = collectionManagementService;
        this.documentStorage = documentStorage;
        this.collectionName = collectionName;
        this.updateCollectionName = collectionName.concat(UPDATES_PREFIX);
        this.executorService = executorService;
    }

    @Override
    public void onSubscribe() {
        collectionManagementService.dropCollection(collectionName);
        collectionManagementService.dropCollection(updateCollectionName);
    }

    @Override
    public void onUnsubscribe() {
        collectionManagementService.dropCollection(collectionName);
        collectionManagementService.dropCollection(updateCollectionName);
    }

    private ConsumerRecord<String, String> keepLatest(ConsumerRecord<String, String> r1, ConsumerRecord<String, String> r2) {
        return r1.offset() > r2.offset() ? r1 : r2;
    }

    private void storeLatest(RecordsBatch<String, String> value) {
        Collection<ConsumerRecord<String, String>> compacted = CollectionUtils.compact(value, ConsumerRecord::key, this::keepLatest);
        Map<Boolean, List<ConsumerRecord<String, String>>> partitioned = compacted.stream()
                .collect(Collectors.partitioningBy(record -> record.value() != null));

        List<ConsumerRecord<String, String>> documentsToSave = partitioned.get(true);
        if (documentsToSave != null && !documentsToSave.isEmpty()) {
            documentStorage.upsertAll(collectionName, DocumentUtils.fromRecordsWithoutEntityId(ConsumerRecord::key, documentsToSave));
        }

        List<ConsumerRecord<String, String>> documentsToRemove = partitioned.get(false);
        if (documentsToRemove != null && !documentsToRemove.isEmpty()) {
            documentStorage.removeAll(collectionName, KafkaUtils.ids(documentsToRemove));
        }
    }

    private void storeUpdates(RecordsBatch<String, String> value) {
        documentStorage.upsertAll(updateCollectionName, DocumentUtils.fromRecords(KafkaUtils::uniqueId, value));
    }

    @Override
    public void onNotify(RecordsBatch<String, String> value, Acknowledgement ack) {
        AsyncUtils.awaitAllOf(DURATION,
                executorService.submit(() -> storeUpdates(value)),
                executorService.submit(() -> storeLatest(value)));
        ack.acknowledge();
    }


}
