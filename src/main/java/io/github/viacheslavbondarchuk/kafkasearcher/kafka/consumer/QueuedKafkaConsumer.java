package io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.util.Map;
import java.util.Queue;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:47 PM
 **/

public interface QueuedKafkaConsumer<K, V> extends Closeable, AutoCloseable {

    void poll();

    String getTopic();

    Map<TopicPartition, Long> getMaxOffsets();

    Queue<ConsumerRecord<K, V>> getMessageQueue();
}
