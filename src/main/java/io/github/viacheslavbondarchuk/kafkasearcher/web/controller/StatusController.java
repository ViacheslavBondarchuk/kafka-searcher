package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.ObservableKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.KafkaConsumerStatus;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.registry.KafkaConsumerRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:15 AM
 **/

@RestController
@RequestMapping("status")
public class StatusController implements Endpoint {
    private final KafkaConsumerRegistry registry;

    public StatusController(KafkaConsumerRegistry registry) {
        this.registry = registry;
    }

    @GetMapping
    public Collection<KafkaConsumerStatus> status() {
        return registry.consumers()
                .stream()
                .map(ObservableKafkaConsumer::getStatus)
                .toList();
    }


}
