package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 3:46 PM
 **/

public final class ExecutorUtils {
    private ExecutorUtils() {}

    public static void shutdown(ExecutorService executor, long timeout, TimeUnit unit) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeout, unit)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
