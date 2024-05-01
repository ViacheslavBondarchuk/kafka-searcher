package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer.ObservableKafkaConsumer;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.KafkaConsumerStatus;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.registry.KafkaConsumerRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.web.service.AuthorizationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.Comparator;

import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.Headers.SECRET_KEY;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:15 AM
 **/

@RestController
@RequestMapping("status")
public class StatusController implements Endpoint {
    private static final Comparator<KafkaConsumerStatus> comparator = Comparator.comparing(KafkaConsumerStatus::topic);

    private final AuthorizationService authorizationService;
    private final KafkaConsumerRegistry registry;

    public StatusController(AuthorizationService authorizationService, KafkaConsumerRegistry registry) {
        this.authorizationService = authorizationService;
        this.registry = registry;
    }

    @GetMapping
    public Collection<KafkaConsumerStatus> status(@RequestHeader(SECRET_KEY) char[] secretKey) {
        authorizationService.check(secretKey);
        return registry.consumers()
                .stream()
                .map(ObservableKafkaConsumer::getStatus)
                .sorted(comparator)
                .toList();
    }


}
