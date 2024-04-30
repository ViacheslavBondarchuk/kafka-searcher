package io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.AbstractObservableKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.RecordsBatch;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Map;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 9:00 PM
 **/

public final class BatchedObservableKafkaConsumer<K, V> extends AbstractObservableKafkaConsumer<K, V, RecordsBatch<K, V>> {
    public BatchedObservableKafkaConsumer(String topic, Map<String, Object> config, Duration pollTimeout, ErrorHandler errorHandler) {
        super(topic, config, pollTimeout, errorHandler);
    }

    @Override
    protected RecordsBatch<K, V> transform(ConsumerRecords<K, V> records) {
        return new RecordsBatch<>(topic, records);
    }
}
