package io.github.viacheslavbondarchuk.kafkasearcher.mongo.registry;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.KafkaTopic;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.repository.KafkaTopicRepository;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 10:47 AM
 **/

@Component
public class KafkaTopicRegistry {
    private final Set<String> topics = ConcurrentHashMap.newKeySet();

    public KafkaTopicRegistry(KafkaTopicRepository repository) {
        repository.findAll()
                .stream()
                .map(KafkaTopic::name)
                .forEach(topics::add);
    }

    public void exists(String topic) {
        if (!contains(topic)) {
            throw new RuntimeException("Topic " + topic + " does not exist");
        }
    }

    public boolean contains(String topic) {
        return topics.contains(topic);
    }

    public Set<String> topics() {
        return Collections.unmodifiableSet(topics);
    }

    public void forEach(Consumer<String> consumer) {
        topics.forEach(consumer);
    }

    public void add(String topic) {
        topics.add(topic);
    }

    public void remove(String topic) {
        topics.remove(topic);
    }

    public Stream<String> stream() {
        return topics.stream();
    }
}
