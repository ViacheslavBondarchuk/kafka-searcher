package io.github.viacheslavbondarchuk.kafkasearcher.kafka.service;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.KafkaTopicMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * author: vbondarchuk
 * date: 5/15/2024
 * time: 5:24 PM
 **/

public interface KafkaTopicMetadataService {
    void updateMaxOffsets(String topic, Map<TopicPartition, Long> map);

    void updateCurrentOffsets(String topic, Map<TopicPartition, Long> map);

    void updateCurrentOffsets(String topic, TopicPartition partition, Long offset);

    void remove(String topic);

    KafkaTopicMetadata getTopicMetadata(String topic);
}
