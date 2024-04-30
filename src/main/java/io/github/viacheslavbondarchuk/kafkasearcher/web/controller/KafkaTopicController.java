package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.service.KafkaConsumerManagementService;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.registry.KafkaTopicRegistry;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.KafkaTopicManagementService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

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

    public KafkaTopicController(KafkaConsumerManagementService kafkaConsumerManagementService,
                                KafkaTopicManagementService kafkaTopicManagementService,
                                KafkaTopicRegistry topicRegistry) {
        this.kafkaConsumerManagementService = kafkaConsumerManagementService;
        this.kafkaTopicManagementService = kafkaTopicManagementService;
        this.topicRegistry = topicRegistry;
    }

    @GetMapping
    public Set<String> topics() {
        return topicRegistry.topics();
    }

    @PostMapping("/register")
    public void register(@RequestParam("topic") String topic) {
        kafkaTopicManagementService.register(topic);
        kafkaConsumerManagementService.register(topic);
    }

    @PostMapping("/unregister")
    public void unregister(@RequestParam("topic") String topic) {
        kafkaTopicManagementService.unregister(topic);
        kafkaConsumerManagementService.unregister(topic);
    }
}
