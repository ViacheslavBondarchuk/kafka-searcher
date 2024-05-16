package io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.System.TOPICS;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 10:33 AM
 **/

@Document(collection = TOPICS)
public record KafkaTopic(@Id String id, String name) {
}
