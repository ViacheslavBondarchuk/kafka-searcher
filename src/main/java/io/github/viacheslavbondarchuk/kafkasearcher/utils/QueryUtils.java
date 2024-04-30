package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import io.github.viacheslavbondarchuk.kafkasearcher.web.domain.SearchRequest;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Query;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 9:41 PM
 **/

public final class QueryUtils {
    private QueryUtils() {}

    public static Query searchQuery(SearchRequest request) {
        BasicQuery query = new BasicQuery(new Document(request.query()));
        query.skip(request.skip());
        query.limit(request.limit());
        query.setSortObject(new Document(request.sort()));
        query.setFieldsObject(new Document(request.fields()));
        return query;
    }

    public static Query countQuery(SearchRequest request) {
        return new BasicQuery(new Document(request.query()));
    }

}
