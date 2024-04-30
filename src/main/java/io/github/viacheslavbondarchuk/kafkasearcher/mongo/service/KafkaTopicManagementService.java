package io.github.viacheslavbondarchuk.kafkasearcher.mongo.service;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.KafkaTopic;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.registry.KafkaTopicRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.repository.KafkaTopicRepository;
import org.springframework.stereotype.Service;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 10:54 AM
 **/

@Service
public class KafkaTopicManagementService {
    private final KafkaTopicRegistry registry;
    private final KafkaTopicRepository topicRepository;

    public KafkaTopicManagementService(KafkaTopicRegistry registry, KafkaTopicRepository topicRepository) {
        this.registry = registry;
        this.topicRepository = topicRepository;
    }

    private void checkUnique(String topic) {
        if (registry.contains(topic)) {
            throw new RuntimeException("Duplicate topic: " + topic);
        }
    }

    public void register(String topic) {
        checkUnique(topic);
        topicRepository.save(new KafkaTopic(null, topic));
        registry.add(topic);
    }

    public void unregister(String topic) {
        topicRepository.deleteByName(topic);
        registry.remove(topic);
    }
}
