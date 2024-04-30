package io.github.viacheslavbondarchuk.kafkasearcher.mongo.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 10:20 AM
 **/

@ConfigurationProperties("io.offer-searcher.mongodb")
public record MongoProperties(String host, String database, String username, char[] password) {

}
