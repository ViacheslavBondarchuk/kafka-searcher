package io.github.viacheslavbondarchuk.kafkasearcher.kafka.service;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.TopicStatus;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * author: vbondarchuk
 * date: 5/15/2024
 * time: 5:01 PM
 **/

public interface KafkaTopicStatusService {

    TopicStatus getTopicStatus(String topic);

    boolean isReady(String topic);
}
