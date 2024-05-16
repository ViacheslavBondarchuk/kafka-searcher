package io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:11 AM
 **/

public record TopicStatus(String topic, boolean ready, long remaining) {

}
