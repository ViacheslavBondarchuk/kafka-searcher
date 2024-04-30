package io.github.viacheslavbondarchuk.kafkasearcher.kafka.registry;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.ObservableKafkaConsumer;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 10:05 PM
 **/

@Component
@SuppressWarnings({"rawtypes, unchecked"})
public class KafkaConsumerRegistry {
    private final Map<String, ObservableKafkaConsumer> consumerMap;

    public KafkaConsumerRegistry() {
        this.consumerMap = new ConcurrentHashMap<>();
    }

    public ObservableKafkaConsumer get(String topic) {
        return consumerMap.get(topic);
    }

    public void register(String topic, ObservableKafkaConsumer consumer) {
        consumerMap.put(topic, consumer);
    }

    public ObservableKafkaConsumer unregister(String topic) {
        return consumerMap.remove(topic);
    }

    public Set<String> topics() {
        return consumerMap.keySet();
    }

    public Collection<ObservableKafkaConsumer> consumers() {
        return consumerMap.values();
    }

    public boolean isReady(String topic) {
        return Optional.of(consumerMap.get(topic))
                .map(ObservableKafkaConsumer::isReady)
                .orElse(false);
    }
}
