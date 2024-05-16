package io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.KafkaTopicMetadata;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaTopicMetadataService;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * author: vbondarchuk
 * date: 5/15/2024
 * time: 5:24 PM
 **/

@Service
public class KafkaTopicMetadataServiceImpl implements KafkaTopicMetadataService {
    private final Map<String, KafkaTopicMetadata> metadataMap;

    public KafkaTopicMetadataServiceImpl() {
        this.metadataMap = new ConcurrentHashMap<>();
    }

    @Override
    public void updateMaxOffsets(String topic, Map<TopicPartition, Long> maxOffsets) {
        metadataMap.computeIfAbsent(topic, ignored -> new KafkaTopicMetadata())
                .updateMaxOffsets(maxOffsets);
    }

    @Override
    public void updateCurrentOffsets(String topic, Map<TopicPartition, Long> currentOffsets) {
        metadataMap.computeIfAbsent(topic, ignored -> new KafkaTopicMetadata())
                .updateCurrentOffsets(currentOffsets);
    }

    @Override
    public void updateCurrentOffsets(String topic, TopicPartition partition, Long offset) {
        metadataMap.computeIfAbsent(topic, ignored -> new KafkaTopicMetadata())
                .updateCurrentOffsets(partition, offset);
    }

    @Override
    public void remove(String topic) {
        metadataMap.remove(topic);
    }

    @Override
    public KafkaTopicMetadata getTopicMetadata(String topic) {
        return metadataMap.get(topic);
    }
}
