package io.github.viacheslavbondarchuk.kafkasearcher.kafka.service;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.ListenableKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.factory.ListenableKafkaConsumerFactory;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.registry.KafkaConsumerRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.impl.StoreToMongoMessageListener;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.IndexService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage.DocumentStorage;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 10:06 PM
 **/

@Service
@SuppressWarnings({"rawtypes"})
public class KafkaConsumerManagementService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerManagementService.class);

    private final KafkaConsumerRegistry<String, String> registry;
    private final ListenableKafkaConsumerFactory factory;
    private final ErrorHandler errorHandler;
    private final DocumentStorage documentStorage;
    private final IndexService indexService;
    private final Duration documentsRetention;

    public KafkaConsumerManagementService(KafkaConsumerRegistry<String, String> registry,
                                          ListenableKafkaConsumerFactory factory,
                                          ErrorHandler errorHandler,
                                          DocumentStorage documentStorage,
                                          IndexService indexService,
                                          @Value("${io.offer-searcher.documents.retention}") Duration documentsRetention) {
        this.registry = registry;
        this.factory = factory;
        this.errorHandler = errorHandler;
        this.documentStorage = documentStorage;
        this.indexService = indexService;
        this.documentsRetention = documentsRetention;
    }

    public void register(String topic) {
        logger.info("Registering topic: {}", topic);
        try {
            ListenableKafkaConsumer<String, String> listenableKafkaConsumer = factory.newConsumer(topic,
                    new StoreToMongoMessageListener(documentStorage, indexService, documentsRetention, topic));
            registry.register(topic, listenableKafkaConsumer);
        } catch (Exception ex) {
            errorHandler.onError(ex);
        } finally {
            logger.info("Registered topic: {}", topic);
        }
    }

    public void unregister(String topic) {
        logger.info("Unregistering topic: {}", topic);
        try {
            ListenableKafkaConsumer<String, String> consumer = registry.unregister(topic);
            ThreadUtils.sleep(Duration.of(1, ChronoUnit.SECONDS));
            consumer.close();
        } catch (Exception ex) {
            errorHandler.onError(ex);
        } finally {
            logger.info("Unregistered topic: {}", topic);
        }
    }
}
