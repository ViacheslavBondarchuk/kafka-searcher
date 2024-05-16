package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.impl.set.strategy.mutable.UnifiedSetWithHashingStrategy;

import java.util.Collection;
import java.util.Queue;
import java.util.function.Supplier;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 12:31 PM
 **/

public final class CollectionUtils {
    private CollectionUtils() {}

    public static <E> UnifiedSetWithHashingStrategy<E> compact(Iterable<E> iterable, HashingStrategy<E> strategy) {
        UnifiedSetWithHashingStrategy<E> compacted = new UnifiedSetWithHashingStrategy<>(strategy);
        iterable.forEach(compacted::addOrReplace);
        return compacted;
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
