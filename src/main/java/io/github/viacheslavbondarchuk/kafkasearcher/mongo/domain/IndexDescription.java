package io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain;

import java.time.Duration;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 3:30 PM
 **/

public record IndexDescription(String name, String field, Duration expire) {
}
