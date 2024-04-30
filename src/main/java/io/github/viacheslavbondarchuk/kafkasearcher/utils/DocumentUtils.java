package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage.DocumentStorage.ENTITY_ID_KEY;
import static io.github.viacheslavbondarchuk.kafkasearcher.mongo.storage.DocumentStorage.MONGO_ID_KEY;
import static io.github.viacheslavbondarchuk.kafkasearcher.utils.DateTimeUtils.ISO_DATE_TIME;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:13 PM
 **/

public final class DocumentUtils {
    public static final String HEADERS_KEY = "headers";
    public static final String TIMESTAMP_KEY = "timestamp";
    public static final String TIMESTAMP_TYPE_KEY = "timestampType";
    public static final String DATE_KEY = "date";
    public static final String PARTITION_KEY = "partition";
    public static final String SYSTEM_KEY = "system";
    public static final String OFFSET_KEY = "offset";

    private DocumentUtils() {}

    public static String id(Document document) {
        return String.valueOf(document.get(MONGO_ID_KEY));
    }

    public static Set<String> ids(Collection<Document> documents) {
        return documents.stream()
                .map(DocumentUtils::id)
                .collect(Collectors.toSet());
    }

    public static List<Document> fromRecordsWithoutEntityId(Function<ConsumerRecord<String, String>, String> idExtractor,
                                                            Collection<ConsumerRecord<String, String>> records) {
        return records.stream()
                .map(record -> DocumentUtils.fromRecordWithoutEntityId(idExtractor.apply(record), record))
                .toList();
    }

    public static Document fromRecordWithoutEntityId(String id, ConsumerRecord<String, String> record) {
        Document document = fromRecord(id, record);
        document.remove(ENTITY_ID_KEY);
        return document;
    }

    public static Document fromRecord(String id, ConsumerRecord<String, String> record) {
        Document document = record.value() == null ? new Document() : Document.parse(record.value());
        document.append(HEADERS_KEY, KafkaUtils.headersToMap(record.headers()));
        document.append(SYSTEM_KEY, Map.of(
                TIMESTAMP_KEY, record.timestamp(),
                TIMESTAMP_TYPE_KEY, record.timestampType(),
                DATE_KEY, DateTimeUtils.format(record.timestamp(), ISO_DATE_TIME),
                OFFSET_KEY, record.offset(),
                PARTITION_KEY, record.partition()
        ));
        if (id != null) {
            document.append(MONGO_ID_KEY, id);
        }
        return document;
    }

    public static List<Document> fromRecords(Function<ConsumerRecord<String, String>, String> idExtractor,
                                             Iterable<ConsumerRecord<String, String>> records) {
        return StreamSupport.stream(records.spliterator(), false)
                .map(record -> DocumentUtils.fromRecord(idExtractor.apply(record), record))
                .toList();
    }

}
