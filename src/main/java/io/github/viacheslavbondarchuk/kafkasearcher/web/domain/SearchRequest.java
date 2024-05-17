package io.github.viacheslavbondarchuk.kafkasearcher.web.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.ValidationMessages.LIMIT_VALIDATION_MESSAGE;
import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.ValidationMessages.SEARCH_TYPE_VALIDATION_MESSAGE;
import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.ValidationMessages.SKIP_VALIDATION_MESSAGE;
import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.ValidationMessages.TOPIC_VALIDATION_MESSAGE;
import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.Field.MONGO_ID;
import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants.MongoCollections.Field.SYSTEM_LAST_UPDATE;
import static io.github.viacheslavbondarchuk.kafkasearcher.web.domain.SearchType.ACTUAL;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 9:37 PM
 **/

public final class SearchRequest {

    @NotBlank(message = TOPIC_VALIDATION_MESSAGE)
    private final String topic;

    private final Map<String, Object> query;

    private final Map<String, Integer> sort;

    private final Map<String, Integer> fields;

    @NotNull(message = SEARCH_TYPE_VALIDATION_MESSAGE)
    private final SearchType searchType;

    @PositiveOrZero(message = SKIP_VALIDATION_MESSAGE)
    private final int skip;

    @Max(100)
    @Positive(message = LIMIT_VALIDATION_MESSAGE)
    private final int limit;


    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public SearchRequest(@JsonProperty("topic") String topic,
                         @JsonProperty("query") Map<String, Object> query,
                         @JsonProperty("sort") Map<String, Integer> sort,
                         @JsonProperty("fields") Map<String, Integer> fields,
                         @JsonProperty("searchType") SearchType searchType,
                         @JsonProperty("skip") int skip,
                         @JsonProperty("limit") int limit) {
        this.topic = topic;
        this.query = Optional.ofNullable(query).orElse(Collections.emptyMap());
        this.sort = modifySort(Optional.ofNullable(sort).orElse(new HashMap<>()));
        this.fields = hideUnnecessaryFields(Optional.ofNullable(fields).orElse(new HashMap<>()));
        this.fields.put(MONGO_ID, 0);
        this.searchType = Optional.of(searchType).orElse(ACTUAL);
        this.skip = skip;
        this.limit = limit;
    }

    private Map<String, Integer> hideUnnecessaryFields(Map<String, Integer> fields) {
        fields.put(MONGO_ID, 0);
        return fields;
    }

    private Map<String, Integer> modifySort(Map<String, Integer> sort) {
        sort.compute(MongoCollections.Field.SYSTEM_TIMESTAMP, (key, integer) -> integer == null ? -1 : integer);
        return sort;
    }

    public String topic() {return topic;}

    public Map<String, Object> query() {return query;}

    public Map<String, Integer> sort() {return sort;}

    public Map<String, Integer> fields() {return fields;}

    public SearchType searchType() {return searchType;}

    public int skip() {return skip;}

    public int limit() {return limit;}

    @Override
    public String toString() {
        return "SearchRequest[" +
               "topic=" + topic + ", " +
               "query=" + query + ", " +
               "sort=" + sort + ", " +
               "fields=" + fields + ", " +
               "searchType=" + searchType + ", " +
               "skip=" + skip + ", " +
               "limit=" + limit + ']';
    }

}
