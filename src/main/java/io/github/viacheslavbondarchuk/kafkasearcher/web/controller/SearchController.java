package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import io.github.viacheslavbondarchuk.kafkasearcher.mongo.domain.SearchResult;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.service.SearchService;
import io.github.viacheslavbondarchuk.kafkasearcher.web.domain.SearchRequest;
import io.github.viacheslavbondarchuk.kafkasearcher.web.domain.SearchResponse;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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

    public SearchController(SearchService searchService) {this.searchService = searchService;}

    @PostMapping
    public SearchResponse<List<Document>> search(@RequestBody SearchRequest searchRequest) {
        log.info("Searching in topic: {}, request: {}", searchRequest.topic(), searchRequest);
        SearchResult<List<Document>> searchResult = searchService.search(searchRequest);
        log.info("Hits: {}, for request: {}", searchResult.hits(), searchRequest);
        return new SearchResponse<>(searchResult.result(), searchResult.hits(),
                searchRequest.searchType(), searchRequest.skip(), searchRequest.limit());
    }
}
