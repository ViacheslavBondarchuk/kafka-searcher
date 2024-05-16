package io.github.viacheslavbondarchuk.kafkasearcher.kafka.factory;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.QueuedKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.impl.QueuedKafkaConsumerImpl;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties.KafkaProperties;
import io.github.viacheslavbondarchuk.kafkasearcher.security.service.JKSFileService;
import org.springframework.stereotype.Component;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 10:29 PM
 **/

@Component
public final class QueuedKafkaConsumerFactory {
    private final KafkaProperties kafkaProperties;
    private final JKSFileService jksFileService;

    public QueuedKafkaConsumerFactory(KafkaProperties kafkaProperties, JKSFileService jksFileService) {
        this.kafkaProperties = kafkaProperties;
        this.jksFileService = jksFileService;
    }

    public <K, V> QueuedKafkaConsumer<K, V> newConsumer(String topic) {
        return new QueuedKafkaConsumerImpl<>(topic, kafkaProperties.load(jksFileService, topic));
    }
}
