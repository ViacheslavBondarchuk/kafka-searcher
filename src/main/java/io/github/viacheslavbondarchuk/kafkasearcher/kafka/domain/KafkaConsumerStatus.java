package io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:11 AM
 **/

public record KafkaConsumerStatus(String topic, boolean ready, long remaining) {

}
