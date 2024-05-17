package io.github.viacheslavbondarchuk.kafkasearcher.mongo.constants;

/**
 * author: vbondarchuk
 * date: 5/16/2024
 * time: 1:27 PM
 **/

public interface MongoCollections {
    interface System {
        String TOPICS = "topics";
        String FIELDS = "fields";
    }

    interface Prefix {
        String UPDATES_PREFIX = "-updates";
    }

    interface Field {
        String FIELD = "field";
    }

    static String makeUpdatesCollectionName(String collectionName) {
        return collectionName.concat(Prefix.UPDATES_PREFIX);
    }
}
