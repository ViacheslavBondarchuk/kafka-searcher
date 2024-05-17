package io.github.viacheslavbondarchuk.kafkasearcher.mongo.service;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.FieldDescription;

/**
 * author: vbondarchuk
 * date: 5/17/2024
 * time: 4:49 PM
 **/

public interface FieldService {
    void save(FieldDescription fieldDescription);

    boolean exists(String field);
}
