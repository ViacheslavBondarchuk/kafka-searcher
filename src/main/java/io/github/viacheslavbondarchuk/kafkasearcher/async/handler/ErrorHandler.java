package io.github.viacheslavbondarchuk.kafkasearcher.async.handler;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 7:34 PM
 **/

public interface ErrorHandler {
    void onError(Throwable e);
}
