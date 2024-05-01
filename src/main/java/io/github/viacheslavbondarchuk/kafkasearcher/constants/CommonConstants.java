package io.github.viacheslavbondarchuk.kafkasearcher.constants;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 10:24 PM
 **/

public interface CommonConstants {
    String UPDATES_PREFIX = "-updates";

    interface ValidationMessages {
        String TOPIC_VALIDATION_MESSAGE = "Topic is blank";
        String SEARCH_TYPE_VALIDATION_MESSAGE = "Search type is null";
        String SKIP_VALIDATION_MESSAGE = "Skip parameter should be positive or zero";
        String LIMIT_VALIDATION_MESSAGE = "Limit parameter should be positive";
    }

    interface Headers {
        String SECRET_KEY = "Secret-Key";
    }

    interface RequestParams {
        String TOPIC = "topic";
    }
}
