package io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 1:12 PM
 **/

public interface KafkaMessageListener<K, V> {
    void onMessages(List<ConsumerRecord<K, V>> messages, String topic);
}
