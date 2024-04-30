package io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement.Acknowledgement;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.RecordsBatch;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaConsumerSubscriber;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.management.MongoCollectionManagementService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage.DocumentStorage;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.DocumentUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.KafkaUtils;

import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.UPDATES_PREFIX;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 11:10 AM
 **/

public final class StoreFullUpdatesSubscriber implements KafkaConsumerSubscriber<RecordsBatch<String, String>> {
    private final MongoCollectionManagementService collectionManagementService;
    private final DocumentStorage documentStorage;
    private final String topic;

    public StoreFullUpdatesSubscriber(MongoCollectionManagementService collectionManagementService,
                                      DocumentStorage documentStorage,
                                      String topic) {
        this.collectionManagementService = collectionManagementService;
        this.documentStorage = documentStorage;
        this.topic = topic.concat(UPDATES_PREFIX);
    }

    @Override
    public void onSubscribe() {
        collectionManagementService.dropCollection(topic);
    }

    @Override
    public void onUnsubscribe() {
        collectionManagementService.dropCollection(topic);
    }

    @Override
    public void onNotify(RecordsBatch<String, String> value, Acknowledgement ack) {
        documentStorage.saveAll(topic, DocumentUtils.fromRecords(KafkaUtils::uniqueId, value));
        ack.acknowledge();
    }


}
