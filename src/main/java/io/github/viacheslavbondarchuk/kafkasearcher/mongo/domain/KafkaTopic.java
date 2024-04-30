package io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain;

import org.springframework.data.annotation.Id;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 10:33 AM
 **/

public record KafkaTopic(@Id String id, String name) {
}
