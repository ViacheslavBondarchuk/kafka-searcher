package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import java.time.Duration;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:34 PM
 **/

public final class ThreadUtils {
    private ThreadUtils() {}

    public static void interrupt() {
        Thread.currentThread().interrupt();
    }

    public static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException ex) {
            ThreadUtils.interrupt();
        }
    }

    public static String getName() {
        return Thread.currentThread().getName();
    }

    public static long getId() {
        return Thread.currentThread().threadId();
    }
}
