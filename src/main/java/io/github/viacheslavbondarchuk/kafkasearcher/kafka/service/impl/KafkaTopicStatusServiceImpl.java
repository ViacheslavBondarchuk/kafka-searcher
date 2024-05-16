package io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.KafkaTopicMetadata;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.TopicStatus;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaTopicMetadataService;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaTopicStatusService;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;

/**
 * author: vbondarchuk
 * date: 5/15/2024
 * time: 5:15 PM
 **/

@Service
public class KafkaTopicStatusServiceImpl implements KafkaTopicStatusService {
    private final KafkaTopicMetadataService kafkaTopicMetadataService;

    public KafkaTopicStatusServiceImpl(KafkaTopicMetadataService kafkaTopicMetadataService) {
        this.kafkaTopicMetadataService = kafkaTopicMetadataService;
    }

    private Long getCurrentOffsetByPartition(Map<TopicPartition, Long> currentOffsets, TopicPartition topicPartition) {
        return Optional.ofNullable(currentOffsets.get(topicPartition)).orElse(0L);
    }

    private long calculateRemaining(Map<TopicPartition, Long> currentOffsets, TopicPartition topicPartition, long maxOffset) {
        return (maxOffset - getCurrentOffsetByPartition(currentOffsets, topicPartition)) - 1L;
    }

    private long getRemaining(Map<TopicPartition, Long> maxOffsets, Map<TopicPartition, Long> currentOffsets) {
        long remaining = 0L;
        for (Map.Entry<TopicPartition, Long> entry : maxOffsets.entrySet()) {
            remaining += Long.max(0L, calculateRemaining(currentOffsets, entry.getKey(), entry.getValue()));
        }
        return remaining < 1000 ? 0L : remaining;
    }

    @Override
    public TopicStatus getTopicStatus(String topic) {
        KafkaTopicMetadata metadata = kafkaTopicMetadataService.getTopicMetadata(topic);
        long remaining = getRemaining(metadata.getMaxOffsets(), metadata.getCurrentOffsets());
        return new TopicStatus(topic, remaining == 0, remaining);
    }

    @Override
    public boolean isReady(String topic) {
        return getTopicStatus(topic).ready();
    }
}
