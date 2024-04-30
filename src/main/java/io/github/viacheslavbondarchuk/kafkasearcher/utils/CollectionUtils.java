package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:31 PM
 **/

public final class CollectionUtils {
    private CollectionUtils() {}

    public static <K, V> Collection<V> compact(Iterable<V> iterable, Function<V, K> keyExtractor, BinaryOperator<V> merger) {
        LinkedHashMap<K, V> map = new LinkedHashMap<>();
        for (V value : iterable) {
            map.merge(keyExtractor.apply(value), value, merger);
        }
        return map.values();
    }
}
