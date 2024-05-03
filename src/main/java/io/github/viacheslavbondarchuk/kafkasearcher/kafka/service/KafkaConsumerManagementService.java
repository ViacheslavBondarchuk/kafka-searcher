package io.github.viacheslavbondarchuk.kafkasearcher.kafka.service;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.async.policy.BlockingPolicy;
import io.github.viacheslavbondarchuk.kafkasearcher.async.scheduler.Scheduler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.ObservableKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.RecordsBatch;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.factory.BatchableObservableConsumerFactory;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties.KafkaSchedulerProperties;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties.KafkaSubscriberProperties;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.registry.KafkaConsumerRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaConsumerSubscriber;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.impl.StoreToDatabaseSubscriber;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.management.MongoCollectionManagementService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.registry.KafkaTopicRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage.DocumentStorage;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.ThreadUtils;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 10:06 PM
 **/

@Service
@SuppressWarnings({"rawtypes"})
public class KafkaConsumerManagementService {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerManagementService.class);
    private final Scheduler scheduler;
    private final KafkaConsumerRegistry registry;
    private final BatchableObservableConsumerFactory factory;
    private final KafkaSchedulerProperties kafkaSchedulerProperties;
    private final KafkaTopicRegistry topicRegistry;
    private final DocumentStorage documentStorage;
    private final Map<String, KafkaConsumerSubscriber> subscriberMap;
    private final MongoCollectionManagementService collectionManagementService;
    private final ExecutorService executorService;

    private final ErrorHandler errorHandler;

    public KafkaConsumerManagementService(Scheduler scheduler,
                                          KafkaConsumerRegistry registry,
                                          BatchableObservableConsumerFactory factory,
                                          KafkaSchedulerProperties kafkaSchedulerProperties,
                                          KafkaSubscriberProperties subscriberProperties,
                                          KafkaTopicRegistry topicRegistry,
                                          DocumentStorage documentStorage,
                                          MongoCollectionManagementService collectionManagementService,
                                          ErrorHandler errorHandler) {
        this.scheduler = scheduler;
        this.registry = registry;
        this.factory = factory;
        this.kafkaSchedulerProperties = kafkaSchedulerProperties;
        this.executorService = new ThreadPoolExecutor(subscriberProperties.parallelism(), subscriberProperties.parallelism(),
                30, SECONDS, new LinkedBlockingQueue<>(), new BlockingPolicy(15, MINUTES));
        this.topicRegistry = topicRegistry;
        this.documentStorage = documentStorage;
        this.collectionManagementService = collectionManagementService;
        this.errorHandler = errorHandler;
        this.subscriberMap = new ConcurrentHashMap<>();
    }

    @PostConstruct
    private void init() {
        topicRegistry.topics()
                .forEach(this::register);
    }

    private void addSubscriberToMap(String topic, KafkaConsumerSubscriber subscriber) {
        subscriberMap.computeIfAbsent(topic, ignored -> subscriber);
    }

    public void register(String topic) {
        log.info("Registering topic: {}", topic);
        try {
            ObservableKafkaConsumer<RecordsBatch<String, String>> consumer = factory.newConsumer(topic, errorHandler);
            StoreToDatabaseSubscriber subscriber = new StoreToDatabaseSubscriber(
                    collectionManagementService, documentStorage, topic, executorService);
            consumer.subscribe(subscriber);
            addSubscriberToMap(topic, subscriber);
            registry.register(topic, consumer);
            scheduler.scheduleAtFixedRate(topic, consumer::poll, 0L, kafkaSchedulerProperties.period(), MILLISECONDS, errorHandler);
        } catch (Exception ex) {
            errorHandler.onError(ex);
        } finally {
            log.info("Registered topic: {}", topic);
        }
    }

    public void unregister(String topic) {
        log.info("Unregistering topic: {}", topic);
        try {
            ObservableKafkaConsumer consumer = registry.unregister(topic);
            if (consumer != null) {
                scheduler.cancel(topic, false);
                consumer.unsubscribe(subscriberMap.remove(topic));
                ThreadUtils.sleep(Duration.of(1, ChronoUnit.SECONDS));
                consumer.close();
            }
        } catch (Exception ex) {
            errorHandler.onError(ex);
        } finally {
            log.info("Unregistered topic: {}", topic);
        }
    }
}
