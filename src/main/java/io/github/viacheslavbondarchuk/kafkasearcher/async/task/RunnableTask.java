package io.github.viacheslavbondarchuk.kafkasearcher.async.task;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 11:06 PM
 **/

public final class RunnableTask extends AbstractListenableTask {
    private final Runnable runnable;
    private final Runnable onComplete;

    RunnableTask(String name, Runnable runnable, Runnable onComplete, ErrorHandler errorHandler) {
        super(name, errorHandler);
        this.runnable = runnable;
        this.onComplete = onComplete;
    }

    @Override
    public void run() {
        try {
            runnable.run();
            if (onComplete != null) {
                onComplete.run();
            }
        } catch (Throwable ex) {
            errorHandler.onError(ex);
        }
    }
}
