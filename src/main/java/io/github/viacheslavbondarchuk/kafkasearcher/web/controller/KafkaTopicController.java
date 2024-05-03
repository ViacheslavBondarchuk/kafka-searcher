package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaConsumerManagementService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.registry.KafkaTopicRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.KafkaTopicManagementService;
import io.github.viacheslavbondarchuk.kafkasearcher.web.service.AuthorizationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Comparator;
import java.util.List;

import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.Headers.SECRET_KEY;
import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.RequestParams.TOPIC;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 10:05 PM
 **/

@RestController
@RequestMapping("topics")
public class KafkaTopicController implements Endpoint {
    private final KafkaConsumerManagementService kafkaConsumerManagementService;
    private final KafkaTopicManagementService kafkaTopicManagementService;
    private final KafkaTopicRegistry topicRegistry;
    private final AuthorizationService authorizationService;

    public KafkaTopicController(KafkaConsumerManagementService kafkaConsumerManagementService,
                                KafkaTopicManagementService kafkaTopicManagementService,
                                KafkaTopicRegistry topicRegistry, AuthorizationService authorizationService) {
        this.kafkaConsumerManagementService = kafkaConsumerManagementService;
        this.kafkaTopicManagementService = kafkaTopicManagementService;
        this.topicRegistry = topicRegistry;
        this.authorizationService = authorizationService;
    }

    @GetMapping
    public List<String> topics(@RequestHeader(SECRET_KEY) char[] secretKey) {
        authorizationService.check(secretKey);
        return topicRegistry.topics()
                .stream()
                .sorted(Comparator.naturalOrder())
                .toList();
    }

    @PostMapping("/register")
    public ResponseEntity<?> register(@RequestParam(TOPIC) String topic, @RequestHeader(SECRET_KEY) char[] secretKey) {
        authorizationService.check(secretKey);
        kafkaTopicManagementService.register(topic);
        kafkaConsumerManagementService.register(topic);
        return emptyOkResponse();
    }

    @PostMapping("/unregister")
    public ResponseEntity<?> unregister(@RequestParam(TOPIC) String topic, @RequestHeader(SECRET_KEY) char[] secretKey) {
        authorizationService.check(secretKey);
        kafkaTopicManagementService.unregister(topic);
        kafkaConsumerManagementService.unregister(topic);
        return emptyOkResponse();
    }
}
