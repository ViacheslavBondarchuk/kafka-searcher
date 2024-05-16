package io.github.viacheslavbondarchuk.kafkasearcher.kafka.processor.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.async.config.SchedulerConfig;
import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.async.policy.RejectingPolicy;
import io.github.viacheslavbondarchuk.kafkasearcher.async.scheduler.Scheduler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.processor.KafkaMessageProcessor;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.registry.KafkaConsumerRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaTopicMetadataService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage.DocumentStorage;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.CollectionUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.DocumentUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.eclipse.collections.api.block.HashingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * author: vbondarchuk
 * date: 5/15/2024
 * time: 3:29 PM
 **/

@Order(1)
@Component
public final class KafkaMessageStoringProcessorImpl implements KafkaMessageProcessor {
    private static final Logger log = LoggerFactory.getLogger(KafkaMessageStoringProcessorImpl.class);
    public static final HashingStrategy<ConsumerRecord<String, String>> BY_KEY_HASHING_STRATEGY = new HashingStrategy<>() {
        @Override
        public int computeHashCode(ConsumerRecord<String, String> record) {
            return Objects.hashCode(record.key());
        }

        @Override
        public boolean equals(ConsumerRecord<String, String> r1, ConsumerRecord<String, String> r2) {
            return Objects.equals(r1.key(), r2.key());
        }
    };

    private final KafkaConsumerRegistry<String, String> kafkaConsumerRegistry;
    private final Scheduler scheduler;
    private final ExecutorService executorService;
    private final ErrorHandler errorHandler;
    private final DocumentStorage storage;
    private final KafkaTopicMetadataService topicMetadataService;

    public KafkaMessageStoringProcessorImpl(KafkaConsumerRegistry<String, String> kafkaConsumerRegistry, ErrorHandler errorHandler,
                                            DocumentStorage storage,
                                            KafkaTopicMetadataService topicMetadataService) {
        this.kafkaConsumerRegistry = kafkaConsumerRegistry;
        this.errorHandler = errorHandler;
        this.storage = storage;
        this.topicMetadataService = topicMetadataService;
        this.scheduler = Scheduler.newScheduler(new SchedulerConfig("kafka-message-processor", 1, new RejectingPolicy(), false));
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public void init() {
        scheduler.scheduleAtFixedRate("handle-kafka-messages", this::handleKafkaMessages, 0, 500, MILLISECONDS, errorHandler);
    }

    private void storeToUpdates(Collection<ConsumerRecord<String, String>> records, String collectionName) {
        try {
            storage.upsertAll(MongoCollections.makeUpdatesCollectionName(collectionName), DocumentUtils.fromRecords(KafkaUtils::uniqueId, records));
        } catch (Exception ex) {
            log.error("Can not store updates to {}, error: ", collectionName, ex);
        }
    }

    private void storeToLatest(Collection<ConsumerRecord<String, String>> records, String collectionName) {
        try {
            Collection<ConsumerRecord<String, String>> compacted = CollectionUtils.compact(records, BY_KEY_HASHING_STRATEGY);
            Map<Boolean, List<ConsumerRecord<String, String>>> partitioned = compacted.stream()
                    .collect(Collectors.partitioningBy(record -> record.value() != null));

            List<ConsumerRecord<String, String>> documentsToSave = partitioned.get(true);
            if (documentsToSave != null && !documentsToSave.isEmpty()) {
                storage.upsertAll(collectionName, DocumentUtils.fromRecordsWithoutEntityId(ConsumerRecord::key, documentsToSave));
            }

            List<ConsumerRecord<String, String>> documentsToRemove = partitioned.get(false);
            if (documentsToRemove != null && !documentsToRemove.isEmpty()) {
                storage.removeAll(collectionName, KafkaUtils.ids(documentsToRemove));
            }
        } catch (Exception ex) {
            log.error("Can not store latest to {}, error: ", collectionName, ex);
        }
    }

    private void updateTopicMetadata(Collection<ConsumerRecord<String, String>> records, String topic) {
        records.forEach(record -> topicMetadataService.updateCurrentOffsets(topic, new TopicPartition(topic, record.partition()), record.offset()));
    }

    private void handleKafkaMessages(Queue<ConsumerRecord<String, String>> messageQueue, String topic) {
        Collection<ConsumerRecord<String, String>> records = CollectionUtils.drainTo(messageQueue, LinkedList::new, 20_000);
        if (!records.isEmpty()) {
            log.info("Drained: {}, topic: {}", records.size(), topic);
            storeToUpdates(records, topic);
            storeToLatest(records, topic);
            updateTopicMetadata(records, topic);
        }
    }

    private void handleKafkaMessages() {
        CompletableFuture.allOf(kafkaConsumerRegistry.consumers()
                .stream()
                .map(consumer -> CompletableFuture.runAsync(() -> handleKafkaMessages(consumer.getMessageQueue(), consumer.getTopic()),
                        executorService))
                .toArray(CompletableFuture[]::new));
    }
}
