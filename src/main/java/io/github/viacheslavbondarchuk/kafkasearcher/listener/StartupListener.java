package io.github.viacheslavbondarchuk.kafkasearcher.listener;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.processor.KafkaMessageProcessor;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaConsumerManagementService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.registry.KafkaTopicRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.IndexService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
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
    private final IndexService indexService;

    private final Duration documentsRetention;

    public StartupListener(KafkaTopicRegistry kafkaTopicRegistry,
                           KafkaConsumerManagementService kafkaConsumerManagementService,
                           List<KafkaMessageProcessor> kafkaMessageProcessors,
                           IndexService indexService,
                           @Value("${io.offer-searcher.documents.retention}") Duration documentsRetention) {
        this.kafkaTopicRegistry = kafkaTopicRegistry;
        this.kafkaConsumerManagementService = kafkaConsumerManagementService;
        this.kafkaMessageProcessors = kafkaMessageProcessors;
        this.indexService = indexService;
        this.documentsRetention = documentsRetention;
    }

    @Override
    public void onApplicationEvent(ApplicationStartedEvent event) {
        indexService.recreateRetentionDocumentsIndex(kafkaTopicRegistry.topics(), documentsRetention);
        kafkaTopicRegistry.forEach(kafkaConsumerManagementService::register);
        kafkaMessageProcessors.forEach(KafkaMessageProcessor::init);
    }
}
