package io.github.viacheslavbondarchuk.kafkasearcher.kafka.registry;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.QueuedKafkaConsumer;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 10:05 PM
 **/

@Component
@SuppressWarnings({"rawtypes, unchecked"})
public class KafkaConsumerRegistry<K, V> {
    private final Map<String, QueuedKafkaConsumer<K, V>> consumerMap;
    private final Collection<QueuedKafkaConsumer<K, V>> consumers;

    public KafkaConsumerRegistry() {
        this.consumerMap = new ConcurrentHashMap<>();
        this.consumers = consumerMap.values();
    }

    public QueuedKafkaConsumer<K, V> get(String topic) {
        return consumerMap.get(topic);
    }

    public void register(String topic, QueuedKafkaConsumer<K, V> consumer) {
        consumerMap.put(topic, consumer);
    }

    public QueuedKafkaConsumer<K, V> unregister(String topic) {
        return consumerMap.remove(topic);
    }

    public Set<String> topics() {
        return consumerMap.keySet();
    }

    public Collection<QueuedKafkaConsumer<K, V>> consumers() {
        return consumers;
    }
}
