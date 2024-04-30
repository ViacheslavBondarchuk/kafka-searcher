package io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 9:45 PM
 **/

public record SearchResult<T>(long hits, T result) {

}
