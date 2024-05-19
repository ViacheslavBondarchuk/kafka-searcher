package io.github.viacheslavbondarchuk.kafkasearcher.kafka.processor.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.async.config.SchedulerConfig;
import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.async.policy.BlockingPolicy;
import io.github.viacheslavbondarchuk.kafkasearcher.async.policy.RejectingPolicy;
import io.github.viacheslavbondarchuk.kafkasearcher.async.scheduler.Scheduler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.processor.KafkaMessageProcessor;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties.KafkaSchedulerProperties;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.registry.KafkaConsumerRegistry;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * author: vbondarchuk
 * date: 5/15/2024
 * time: 2:55 PM
 **/

@Order(0)
@Component
public final class KafkaMessagePollingProcessorImpl implements KafkaMessageProcessor {
    private final KafkaSchedulerProperties kafkaSchedulerProperties;
    private final KafkaConsumerRegistry<String, String> kafkaConsumerRegistry;
    private final ExecutorService executorServices;
    private final ErrorHandler errorHandler;
    private final Scheduler scheduler;

    public KafkaMessagePollingProcessorImpl(KafkaConsumerRegistry<String, String> kafkaConsumerRegistry,
                                            KafkaSchedulerProperties kafkaSchedulerProperties,
                                            ErrorHandler errorHandler) {
        this.kafkaSchedulerProperties = kafkaSchedulerProperties;
        this.executorServices = new ThreadPoolExecutor(kafkaSchedulerProperties.parallelism(), kafkaSchedulerProperties.parallelism(),
                0L, MILLISECONDS, new LinkedBlockingQueue<>(kafkaSchedulerProperties.parallelism()), new BlockingPolicy(15, MINUTES));
        this.kafkaConsumerRegistry = kafkaConsumerRegistry;
        this.errorHandler = errorHandler;
        this.scheduler = Scheduler.newScheduler(new SchedulerConfig("kafka-message-polling-processor", 1, new RejectingPolicy(), false));
    }

    @Override
    public void init() {
        scheduler.scheduleAtFixedRate("poll-messages", this::pollMessages,
                0, kafkaSchedulerProperties.period(), MILLISECONDS, errorHandler);
    }

    private void pollMessages() {
        kafkaConsumerRegistry.consumers()
                .forEach(listenableKafkaConsumer -> executorServices.submit(listenableKafkaConsumer::poll));
    }

}
