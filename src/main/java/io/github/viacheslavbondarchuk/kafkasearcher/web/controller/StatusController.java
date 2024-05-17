package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.TopicStatus;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaTopicStatusService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.registry.KafkaTopicRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.web.service.AuthorizationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
    private static final Comparator<TopicStatus> comparator = Comparator.comparing(TopicStatus::topic);

    private final AuthorizationService authorizationService;
    private final KafkaTopicStatusService statusService;
    private final KafkaTopicRegistry topicRegistry;

    public StatusController(AuthorizationService authorizationService, KafkaTopicStatusService statusService, KafkaTopicRegistry topicRegistry) {
        this.authorizationService = authorizationService;
        this.statusService = statusService;
        this.topicRegistry = topicRegistry;
    }

    @GetMapping
    public Collection<TopicStatus> status(@RequestHeader(SECRET_KEY) char[] secretKey) {
        authorizationService.check(secretKey);
        return topicRegistry.topics()
                .stream()
                .map(statusService::getTopicStatus)
                .sorted(comparator)
                .toList();
    }

    @GetMapping(path = "{topic}")
    public ResponseEntity<TopicStatus> status(@RequestHeader(SECRET_KEY) char[] secretKey, @PathVariable String topic) {
        authorizationService.check(secretKey);
        return ResponseEntity.ok(statusService.getTopicStatus(topic));
    }


}
