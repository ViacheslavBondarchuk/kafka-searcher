package io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.ListenableKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaTopicMetadataService;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaMessageListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:51 PM
 **/

public class ListenableKafkaConsumerImpl<K, V> implements ListenableKafkaConsumer<K, V> {
    private final KafkaTopicMetadataService topicMetadataService;
    private final KafkaConsumer<K, V> consumer;
    private final Duration pollTimeout;
    private final String topic;
    private final KafkaMessageListener<K, V> listener;
    private final ErrorHandler errorHandler;

    private volatile boolean isReadyToPoll = true;

    public ListenableKafkaConsumerImpl(KafkaTopicMetadataService topicMetadataService,
                                       String topic,
                                       Map<String, Object> config,
                                       KafkaMessageListener<K, V> listener,
                                       ErrorHandler errorHandler) {
        this.topicMetadataService = topicMetadataService;
        this.consumer = new KafkaConsumer<>(config);
        this.pollTimeout = Duration.ofMillis((Integer) config.getOrDefault(REQUEST_TIMEOUT_MS_CONFIG, 100L));
        this.topic = topic;
        this.listener = listener;
        this.errorHandler = errorHandler;
        this.init();
    }

    private void init() {
        consumer.subscribe(Collections.singletonList(topic));
        topicMetadataService.updateMaxOffsets(topic, consumer.endOffsets(consumer.partitionsFor(topic)
                .stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .toList()));
    }

    private TopicPartition createTopicPartition(ConsumerRecord<K, V> record) {
        return new TopicPartition(topic, record.partition());
    }

    @Override
    public void poll() {
        try {
            isReadyToPoll = false;
            ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
            if (!records.isEmpty()) {
                listener.onMessages(StreamSupport.stream(records.spliterator(), false)
                        .peek(record -> topicMetadataService.updateCurrentOffsets(topic, createTopicPartition(record), record.offset()))
                        .toList(), topic);
                consumer.commitSync();
            }
        } catch (Exception ex) {
            errorHandler.onError(ex);
        } finally {
            isReadyToPoll = true;
        }
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public boolean isReadyToPoll() {
        return isReadyToPoll;
    }

    @Override
    public void close() throws IOException {
        consumer.unsubscribe();
        consumer.close();
        topicMetadataService.remove(topic);
    }
}
