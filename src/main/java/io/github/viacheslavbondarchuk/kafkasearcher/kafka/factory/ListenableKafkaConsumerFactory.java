package io.github.viacheslavbondarchuk.kafkasearcher.kafka.factory;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.ListenableKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.impl.ListenableKafkaConsumerImpl;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties.KafkaProperties;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaTopicMetadataService;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaMessageListener;
import io.github.viacheslavbondarchuk.kafkasearcher.security.service.JKSFileService;
import org.springframework.stereotype.Component;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 10:29 PM
 **/

@Component
public final class ListenableKafkaConsumerFactory {
    private final KafkaProperties kafkaProperties;
    private final JKSFileService jksFileService;
    private final KafkaTopicMetadataService topicMetadataService;
    private final ErrorHandler errorHandler;

    public ListenableKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                          JKSFileService jksFileService,
                                          KafkaTopicMetadataService topicMetadataService,
                                          ErrorHandler errorHandler) {
        this.kafkaProperties = kafkaProperties;
        this.jksFileService = jksFileService;
        this.topicMetadataService = topicMetadataService;
        this.errorHandler = errorHandler;
    }

    public <K, V> ListenableKafkaConsumer<K, V> newConsumer(String topic, KafkaMessageListener<K, V> listener) {
        return new ListenableKafkaConsumerImpl<>(topicMetadataService, topic, kafkaProperties.load(jksFileService, topic), listener, errorHandler);
    }
}
