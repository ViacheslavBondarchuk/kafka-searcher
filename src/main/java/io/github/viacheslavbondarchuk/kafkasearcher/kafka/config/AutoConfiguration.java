package io.github.viacheslavbondarchuk.kafkasearcher.kafka.config;

import io.github.viacheslavbondarchuk.kafkasearcher.async.config.SchedulerConfig;
import io.github.viacheslavbondarchuk.kafkasearcher.async.policy.BlockingPolicy;
import io.github.viacheslavbondarchuk.kafkasearcher.async.scheduler.Scheduler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties.KafkaSchedulerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 10:20 PM
 **/

@Configuration
public class AutoConfiguration {
    private final KafkaSchedulerProperties properties;

    public AutoConfiguration(KafkaSchedulerProperties properties) {
        this.properties = properties;
    }

    @Bean
    public Scheduler kafkaConsumerManagementScheduler() {
        SchedulerConfig config = new SchedulerConfig("kafka-consumer-management",
                properties.parallelism(), new BlockingPolicy(15, TimeUnit.MINUTES), false);
        return Scheduler.newScheduler(config);
    }
}
