package io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * author: vbondarchuk
 * date: 5/3/2024
 * time: 1:30 PM
 **/

@ConfigurationProperties("io.offer-searcher.transport.kafka.subscriber")
public record KafkaSubscriberProperties(int parallelism) {
}
