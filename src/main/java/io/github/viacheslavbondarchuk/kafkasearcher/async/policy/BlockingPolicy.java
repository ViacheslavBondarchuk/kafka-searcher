package io.github.viacheslavbondarchuk.kafkasearcher.async.policy;

import io.github.viacheslavbondarchuk.kafkasearcher.utils.ThreadUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:32 PM
 **/

public final class BlockingPolicy implements RejectionPolicy {
    private final long timeout;
    private final TimeUnit timeUnit;

    public BlockingPolicy(long timeout, TimeUnit timeUnit) {
        this.timeout = timeout;
        this.timeUnit = timeUnit;
    }

    @Override
    public void onReject(Runnable task, BlockingQueue<Runnable> queue, ThreadPoolExecutor executor) {
        try {
            queue.offer(task, timeout, timeUnit);
        } catch (InterruptedException ex) {
            ThreadUtils.interrupt();
        }
    }
}
