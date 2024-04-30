package io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement.Acknowledgement;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.RecordsBatch;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaConsumerSubscriber;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.management.MongoCollectionManagementService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage.DocumentStorage;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.CollectionUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.DocumentUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 11:10 AM
 **/

public final class StoreLatestUpdatesSubscriber implements KafkaConsumerSubscriber<RecordsBatch<String, String>> {
    private final MongoCollectionManagementService collectionManagementService;
    private final DocumentStorage documentStorage;
    private final String topic;

    public StoreLatestUpdatesSubscriber(MongoCollectionManagementService collectionManagementService,
                                        DocumentStorage documentStorage,
                                        String topic) {
        this.collectionManagementService = collectionManagementService;
        this.documentStorage = documentStorage;
        this.topic = topic;
    }

    @Override
    public void onSubscribe() {
        collectionManagementService.dropCollection(topic);
    }

    @Override
    public void onUnsubscribe() {
        collectionManagementService.dropCollection(topic);
    }

    private ConsumerRecord<String, String> keepLatest(ConsumerRecord<String, String> r1, ConsumerRecord<String, String> r2) {
        return r1.offset() > r2.offset() ? r1 : r2;
    }

    @Override
    public void onNotify(RecordsBatch<String, String> value, Acknowledgement ack) {
        Collection<ConsumerRecord<String, String>> compacted = CollectionUtils.compact(value, ConsumerRecord::value, this::keepLatest);
        Map<Boolean, List<ConsumerRecord<String, String>>> partitioned = compacted.stream()
                .collect(Collectors.partitioningBy(record -> record.value() != null));

        List<ConsumerRecord<String, String>> documentsToSave = partitioned.get(true);
        if (documentsToSave != null && !documentsToSave.isEmpty()) {
            documentStorage.upsertAll(topic, DocumentUtils.fromRecordsWithoutEntityId(ConsumerRecord::key, documentsToSave));
        }

        List<ConsumerRecord<String, String>> documentsToRemove = partitioned.get(false);
        if (documentsToRemove != null && !documentsToRemove.isEmpty()) {
            documentStorage.removeAll(topic, KafkaUtils.ids(documentsToRemove));
        }
        ack.acknowledge();
    }


}
