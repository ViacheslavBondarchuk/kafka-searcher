package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import java.time.Duration;
import java.util.concurrent.Future;

/**
 * author: vbondarchuk
 * date: 5/3/2024
 * time: 1:37 PM
 **/

public final class AsyncUtils {
    private AsyncUtils() {}

    public static void awaitAllOf(Duration duration, Future... futures) {
        for (Future<?> future : futures) {
            while (!future.isDone()) {
                ThreadUtils.sleep(duration);
            }
        }
    }
}
