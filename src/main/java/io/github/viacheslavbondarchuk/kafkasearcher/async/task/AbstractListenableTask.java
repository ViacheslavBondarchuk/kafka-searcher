package io.github.viacheslavbondarchuk.kafkasearcher.async.task;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 10:59 PM
 **/

public abstract class AbstractListenableTask implements Runnable {
    protected ErrorHandler errorHandler;
    protected String name;

    protected AbstractListenableTask(String name, ErrorHandler errorHandler) {
        this.name = name;
        this.errorHandler = errorHandler;
    }

    public static <T> AbstractListenableTask newCallableTask(String name, Callable<T> callable, Consumer<T> onComplete, ErrorHandler errorHandler) {
        return new CallableTask<>(name, callable, onComplete, errorHandler);
    }

    public static AbstractListenableTask newRunnableTask(String name, Runnable runnable, Runnable onComplete, ErrorHandler errorHandler) {
        return new RunnableTask(name, runnable, onComplete, errorHandler);
    }

    public static AbstractListenableTask newRunnableTask(String name, Runnable runnable, ErrorHandler errorHandler) {
        return AbstractListenableTask.newRunnableTask(name, runnable, null, errorHandler);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        AbstractListenableTask that = (AbstractListenableTask) obj;
        if (this == that) {
            return true;
        }

        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }
}
