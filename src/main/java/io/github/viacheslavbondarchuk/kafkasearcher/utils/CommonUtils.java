package io.github.viacheslavbondarchuk.kafkasearcher.utils;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 10:39 PM
 **/

import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

public final class CommonUtils {
    private CommonUtils() {}

    public static <T> void acceptIfNonNull(T value, Consumer<T> consumer) {
        acceptIfNonNull(value, consumer, (Runnable) null);
    }

    public static <T> void acceptIfNonNull(T value, Consumer<T> consumer, Runnable runnable) {
        if (value != null) {
            consumer.accept(value);
        } else if (runnable != null) {
            runnable.run();
        }
    }

    public static <K, V> void acceptIfNonNull(K key, V value, BiConsumer<K, V> consumer) {
        if (key != null && value != null) {
            consumer.accept(key, value);
        }
    }

    public static String createGroupId(String groupName, String topic) {
        return new StringBuilder()
                .append(groupName)
                .append("-")
                .append(topic)
                .append("-")
                .append(UUID.randomUUID())
                .toString();
    }

    public static <T> void acceptByPredicate(T value, Predicate<T> predicate, Consumer<T> consumer, Runnable orElse) {
        if (predicate.test(value)) {
            consumer.accept(value);
        } else if (orElse != null) {
            orElse.run();
        }
    }

    public static <T> void acceptByPredicate(T value, Predicate<T> predicate, Consumer<T> consumer) {
        acceptByPredicate(value, predicate, consumer, null);
    }

    public static void executeByPredicate(boolean condition, Runnable runnable) {
        if (condition) {
            runnable.run();
        }
    }

    @SafeVarargs
    public static <T> void arrayForEach(Consumer<T> consumer, T... values) {
        for (T value : values) {
            consumer.accept(value);
        }
    }

}
