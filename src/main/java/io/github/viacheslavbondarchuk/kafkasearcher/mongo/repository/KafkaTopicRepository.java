package io.github.viacheslavbondarchuk.kafkasearcher.mongo.repository;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.KafkaTopic;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 10:34 AM
 **/

public interface KafkaTopicRepository extends MongoRepository<KafkaTopic, String> {
    void deleteByName(String topicName);
}
