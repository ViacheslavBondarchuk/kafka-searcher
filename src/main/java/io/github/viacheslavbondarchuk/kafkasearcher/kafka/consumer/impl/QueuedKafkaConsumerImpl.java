package io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.QueuedKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:51 PM
 **/

public class QueuedKafkaConsumerImpl<K, V> implements QueuedKafkaConsumer<K, V> {
    private final KafkaConsumer<K, V> consumer;
    private final Duration pollTimeout;
    private final String topic;
    private final Queue<ConsumerRecord<K, V>> messageQueue;

    public QueuedKafkaConsumerImpl(String topic, Map<String, Object> config) {
        this.consumer = new KafkaConsumer<>(config);
        this.pollTimeout = Duration.ofMillis((Integer) config.getOrDefault(REQUEST_TIMEOUT_MS_CONFIG, 100L));
        this.topic = topic;
        this.messageQueue = new ConcurrentLinkedQueue<>();
        this.init();
    }

    private void init() {
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void poll() {
        ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
        records.forEach(messageQueue::offer);
        consumer.commitSync();
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public Map<TopicPartition, Long> getMaxOffsets() {
        List<TopicPartition> partitions = consumer.partitionsFor(topic)
                .stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .toList();
        return consumer.endOffsets(partitions);
    }

    @Override
    public Queue<ConsumerRecord<K, V>> getMessageQueue() {
        return messageQueue;
    }

    @Override
    public void close() throws IOException {
        consumer.unsubscribe();
        consumer.close();
    }
}
