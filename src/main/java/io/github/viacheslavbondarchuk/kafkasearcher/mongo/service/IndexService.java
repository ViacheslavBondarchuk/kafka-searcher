package io.github.viacheslavbondarchuk.kafkasearcher.mongo.service;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.IndexDescription;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 3:07 PM
 **/

public interface IndexService {
    void createIndex(String collection, IndexDescription indexDescription);

    void dropIndex(String collection, String field);
}
