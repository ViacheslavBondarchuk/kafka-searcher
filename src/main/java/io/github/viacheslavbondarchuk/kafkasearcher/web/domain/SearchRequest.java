package io.github.viacheslavbondarchuk.kafkasearcher.web.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.ValidationMessages.LIMIT_VALIDATION_MESSAGE;
import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.ValidationMessages.SEARCH_TYPE_VALIDATION_MESSAGE;
import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.ValidationMessages.SKIP_VALIDATION_MESSAGE;
import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.ValidationMessages.TOPIC_VALIDATION_MESSAGE;
import static io.github.viacheslavbondarchuk.kafkasearcher.web.domain.SearchType.ACTUAL;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 9:37 PM
 **/

public record SearchRequest(@NotBlank(message = TOPIC_VALIDATION_MESSAGE) String topic,
                            Map<String, Object> query,
                            Map<String, Integer> sort,
                            Map<String, Integer> fields,
                            @NotNull(message = SEARCH_TYPE_VALIDATION_MESSAGE) SearchType searchType,
                            @PositiveOrZero(message = SKIP_VALIDATION_MESSAGE) int skip,
                            @Positive(message = LIMIT_VALIDATION_MESSAGE) int limit) {

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SearchRequest(String topic,
                         Map<String, Object> query,
                         Map<String, Integer> sort,
                         Map<String, Integer> fields,
                         SearchType searchType,
                         int skip,
                         int limit) {
        this.topic = topic;
        this.query = Optional.ofNullable(query).orElse(Collections.emptyMap());
        this.sort = Optional.ofNullable(sort).orElse(Collections.emptyMap());
        this.fields = Optional.ofNullable(fields).orElse(Collections.emptyMap());
        this.searchType = Optional.of(searchType).orElse(ACTUAL);
        this.skip = skip;
        this.limit = limit;
    }
}
