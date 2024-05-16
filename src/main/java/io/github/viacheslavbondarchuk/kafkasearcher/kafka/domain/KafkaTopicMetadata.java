package io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain;

import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * author: vbondarchuk
 * date: 5/15/2024
 * time: 5:02 PM
 **/

public class KafkaTopicMetadata {
    private final Map<TopicPartition, Long> maxOffsets;
    private final Map<TopicPartition, Long> currentOffsets;

    public Map<TopicPartition, Long> getMaxOffsets() {
        return Collections.unmodifiableMap(maxOffsets);
    }

    public Map<TopicPartition, Long> getCurrentOffsets() {
        return Collections.unmodifiableMap(currentOffsets);
    }

    public KafkaTopicMetadata() {
        this.maxOffsets = new ConcurrentHashMap<>();
        this.currentOffsets = new ConcurrentHashMap<>();
    }

    public void updateMaxOffsets(Map<TopicPartition, Long> maxOffsets) {
        this.maxOffsets.putAll(maxOffsets);
    }

    public void updateCurrentOffsets(Map<TopicPartition, Long> currentOffsets) {
        this.currentOffsets.putAll(currentOffsets);
    }

    public void updateCurrentOffsets(TopicPartition topicPartition, Long offset) {
        this.currentOffsets.put(topicPartition, offset);
    }
}
