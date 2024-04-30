package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:16 PM
 **/

public final class KafkaUtils {
    private KafkaUtils() {}

    public static String uniqueId(ConsumerRecord<?, ?> record) {
        return MessageFormat.format("{0}-{1}", String.valueOf(record.offset()), String.valueOf(record.partition()));
    }

    public static Set<String> ids(Collection<ConsumerRecord<String, String>> records) {
        return records.stream()
                .map(ConsumerRecord::key)
                .collect(Collectors.toUnmodifiableSet());
    }

    public static Map<String, String> headersToMap(Headers headers) {
        Map<String, String> map = new HashMap<>();
        for (Header header : headers) {
            if (header.value() != null) {
                map.put(header.key(), new String(header.value()));
            }
        }
        return map;
    }

    public static Headers mapToHeaders(Map<String, String> map, Header... additional) {
        RecordHeaders headers = new RecordHeaders();
        map.forEach((key, value) -> headers.add(key, value.getBytes()));
        Optional.ofNullable(additional)
                .ifPresent(additionalHeaders -> Arrays.stream(additionalHeaders)
                        .forEach(headers::add));
        return headers;
    }
}
