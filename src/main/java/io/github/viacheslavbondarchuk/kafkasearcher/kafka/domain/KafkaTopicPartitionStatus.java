package io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 7:34 PM
 **/

public record KafkaTopicPartitionStatus(long maxOffset, long currentOffset, long remaining) {

}
