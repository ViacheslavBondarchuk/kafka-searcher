package io.github.viacheslavbondarchuk.kafkasearcher.web.domain;

import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 9:49 PM
 **/

@JsonInclude(value = NON_NULL)
public record SearchResponse<T>(T data, Long hits, SearchType searchType, Integer skip, Integer limit) {

}
