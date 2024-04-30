package io.github.viacheslavbondarchuk.kafkasearcher.utils;

import com.rits.cloning.Cloner;

import java.util.ConcurrentModificationException;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 4:10 PM
 **/

public final class CloneUtils {
    private static final Cloner cloner = Cloner.standard();

    private CloneUtils() {}

    public static <T> T clone(T object) {
        T result;
        for (int i = 0; ; i++) {
            try {
                result = cloner.deepClone(object);
                break;
            } catch (ConcurrentModificationException ex) {
                if (i >= 3) {
                    throw ex;
                }
            }
        }
        return result;
    }
}
