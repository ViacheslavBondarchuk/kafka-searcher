package io.github.viacheslavbondarchuk.kafkasearcher.listener;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.processor.KafkaMessageProcessor;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaConsumerManagementService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.registry.KafkaTopicRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.CollectionManagementService;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * author: vbondarchuk
 * date: 5/16/2024
 * time: 11:39 AM
 **/

@Component
public class StartupListener implements ApplicationListener<ApplicationStartedEvent> {
    private final KafkaTopicRegistry kafkaTopicRegistry;
    private final KafkaConsumerManagementService kafkaConsumerManagementService;
    private final List<KafkaMessageProcessor> kafkaMessageProcessors;
    private final CollectionManagementService collectionManagementService;

    public StartupListener(KafkaTopicRegistry kafkaTopicRegistry,
                           KafkaConsumerManagementService kafkaConsumerManagementService,
                           List<KafkaMessageProcessor> kafkaMessageProcessors,
                           CollectionManagementService collectionManagementService) {
        this.kafkaTopicRegistry = kafkaTopicRegistry;
        this.kafkaConsumerManagementService = kafkaConsumerManagementService;
        this.kafkaMessageProcessors = kafkaMessageProcessors;
        this.collectionManagementService = collectionManagementService;
    }

    @Override
    public void onApplicationEvent(ApplicationStartedEvent event) {
        collectionManagementService.removeAllNonSystemCollections();
        kafkaTopicRegistry.forEach(kafkaConsumerManagementService::register);
        kafkaMessageProcessors.forEach(KafkaMessageProcessor::init);
    }
}
