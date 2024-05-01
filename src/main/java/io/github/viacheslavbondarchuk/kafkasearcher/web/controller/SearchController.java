package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.SearchResult;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.SearchService;
import io.github.viacheslavbondarchuk.kafkasearcher.web.domain.SearchRequest;
import io.github.viacheslavbondarchuk.kafkasearcher.web.domain.SearchResponse;
import io.github.viacheslavbondarchuk.kafkasearcher.web.service.AuthorizationService;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static io.github.viacheslavbondarchuk.kafkasearcher.constants.CommonConstants.Headers.SECRET_KEY;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 9:51 PM
 **/

@RestController
@RequestMapping("search")
public class SearchController implements Endpoint {
    private static final Logger log = LoggerFactory.getLogger(SearchController.class);

    private final SearchService searchService;
    private final AuthorizationService authorizationService;

    public SearchController(SearchService searchService, AuthorizationService authorizationService) {
        this.searchService = searchService;
        this.authorizationService = authorizationService;
    }

    @PostMapping
    public SearchResponse<List<Document>> search(@RequestBody @Validated SearchRequest searchRequest, @RequestHeader(SECRET_KEY) char[] secretKey) {
        authorizationService.check(secretKey);
        log.info("Searching in topic: {}, request: {}", searchRequest.topic(), searchRequest);
        SearchResult<List<Document>> searchResult = searchService.search(searchRequest);
        log.info("Hits: {}, for request: {}", searchResult.hits(), searchRequest);
        return new SearchResponse<>(searchResult.result(), searchResult.hits(),
                searchRequest.searchType(), searchRequest.skip(), searchRequest.limit());
    }
}
