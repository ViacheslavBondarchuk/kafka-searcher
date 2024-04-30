package io.github.viacheslavbondarchuk.kafkasearcher.async.task;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 7:32 PM
 **/

public final class CallableTask<T> extends AbstractListenableTask {
    private final Callable<T> callable;
    private final Consumer<T> onComplete;

    CallableTask(String name, Callable<T> callable, Consumer<T> onComplete, ErrorHandler errorHandler) {
        super(name, errorHandler);
        this.callable = callable;
        this.onComplete = onComplete;
    }

    @Override
    public void run() {
        try {
            T result = callable.call();
            onComplete.accept(result);
        } catch (Throwable ex) {
            errorHandler.onError(ex);
        }
    }


}
