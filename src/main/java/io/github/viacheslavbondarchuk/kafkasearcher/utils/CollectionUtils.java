package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Queue;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:31 PM
 **/

public final class CollectionUtils {
    private CollectionUtils() {}

    public static <K, V> Collection<V> compact(Iterable<V> iterable, Function<V, K> keyFunction, BinaryOperator<V> merger) {
        HashMap<K, V> hashMap = new HashMap<>();
        for (V element : iterable) {
            hashMap.merge(keyFunction.apply(element), element, merger);
        }
        return hashMap.values();
    }

    public static <V> Collection<V> drainTo(Queue<V> source, Collection<V> target, int limit) {
        V value;
        while (limit > 0 && (value = source.poll()) != null) {
            target.add(value);
            limit--;
        }
        return target;
    }

    public static <V> Collection<V> drainTo(Queue<V> source, Supplier<Collection<V>> factory, int limit) {
        return drainTo(source, factory.get(), limit);
    }

}
