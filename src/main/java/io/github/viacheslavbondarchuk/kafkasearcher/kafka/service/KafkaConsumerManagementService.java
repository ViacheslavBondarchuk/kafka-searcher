package io.github.viacheslavbondarchuk.kafkasearcher.kafka.service;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.QueuedKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.factory.QueuedKafkaConsumerFactory;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.registry.KafkaConsumerRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final QueuedKafkaConsumerFactory factory;
    private final ErrorHandler errorHandler;
    private final KafkaTopicMetadataService topicMetadataService;

    public KafkaConsumerManagementService(KafkaConsumerRegistry<String, String> registry,
                                          QueuedKafkaConsumerFactory factory,
                                          ErrorHandler errorHandler,
                                          KafkaTopicMetadataService topicMetadataService) {
        this.registry = registry;
        this.factory = factory;
        this.errorHandler = errorHandler;
        this.topicMetadataService = topicMetadataService;
    }


    public void register(String topic) {
        logger.info("Registering topic: {}", topic);
        try {
            QueuedKafkaConsumer<String, String> queuedKafkaConsumer = factory.newConsumer(topic);
            topicMetadataService.updateMaxOffsets(topic, queuedKafkaConsumer.getMaxOffsets());
            registry.register(topic, queuedKafkaConsumer);
        } catch (Exception ex) {
            errorHandler.onError(ex);
        } finally {
            logger.info("Registered topic: {}", topic);
        }
    }

    public void unregister(String topic) {
        logger.info("Unregistering topic: {}", topic);
        try {
            QueuedKafkaConsumer<String, String> consumer = registry.unregister(topic);
            topicMetadataService.remove(topic);
            ThreadUtils.sleep(Duration.of(1, ChronoUnit.SECONDS));
            consumer.close();
        } catch (Exception ex) {
            errorHandler.onError(ex);
        } finally {
            logger.info("Unregistered topic: {}", topic);
        }
    }
}
