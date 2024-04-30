package io.github.viacheslavbondarchuk.kafkasearcher.async.policy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:29 PM
 **/

public interface RejectionPolicy extends RejectedExecutionHandler {

    @Override
    default void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
        onReject(task, executor.getQueue(), executor);
    }

    void onReject(Runnable task, BlockingQueue<Runnable> queue, ThreadPoolExecutor executor);
}
