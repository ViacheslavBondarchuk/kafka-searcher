package io.github.viacheslavbondarchuk.kafkasearcher.mongo.registry;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.KafkaTopic;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.repository.KafkaTopicRepository;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 10:47 AM
 **/

@Component
public class KafkaTopicRegistry {
    private final Set<String> topics;

    public KafkaTopicRegistry(KafkaTopicRepository repository) {
        this.topics = repository.findAll()
                .stream()
                .map(KafkaTopic::name)
                .collect(Collectors.toSet());
    }

    public boolean contains(String topic) {
        return topics.contains(topic);
    }

    public Set<String> topics() {
        return Collections.unmodifiableSet(topics);
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
