package io.github.viacheslavbondarchuk.kafkasearcher.kafka.factory;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.ObservableKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.impl.BatchedObservableKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.RecordsBatch;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties.KafkaProperties;
import io.github.viacheslavbondarchuk.kafkasearcher.security.service.JKSFileService;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 10:29 PM
 **/

@Component
public final class BatchableObservableConsumerFactory {
    private final KafkaProperties kafkaProperties;
    private final JKSFileService jksFileService;

    public BatchableObservableConsumerFactory(KafkaProperties kafkaProperties, JKSFileService jksFileService) {
        this.kafkaProperties = kafkaProperties;
        this.jksFileService = jksFileService;
    }


    public <K, V> ObservableKafkaConsumer<RecordsBatch<K, V>> newConsumer(String topic, Duration pollTimeout, ErrorHandler errorHandler) {
        return new BatchedObservableKafkaConsumer<>(topic, kafkaProperties.load(jksFileService, topic), pollTimeout, errorHandler);
    }
}
