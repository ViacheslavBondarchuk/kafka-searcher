package io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 11:48 PM
 **/

@ConfigurationProperties("io.offer-searcher.transport.kafka.scheduler")
public record KafkaSchedulerProperties(long period, int parallelism) {

}
