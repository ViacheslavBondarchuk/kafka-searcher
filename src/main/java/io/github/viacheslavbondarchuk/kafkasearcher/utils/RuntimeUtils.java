package io.github.viacheslavbondarchuk.kafkasearcher.utils;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 7:26 PM
 **/

public final class RuntimeUtils {
    private static final Runtime runtime = Runtime.getRuntime();

    private RuntimeUtils() {}

    public static int availableProcessors() {
        return runtime.availableProcessors();
    }

    public static long freeMemory() {
        return runtime.freeMemory();
    }

    public static long maxMemory() {
        return runtime.maxMemory();
    }
}
