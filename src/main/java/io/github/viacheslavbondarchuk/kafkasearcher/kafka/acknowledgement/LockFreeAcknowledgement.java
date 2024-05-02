package io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement;

import io.github.viacheslavbondarchuk.kafkasearcher.utils.ThreadUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * author: vbondarchuk
 * date: 5/2/2024
 * time: 10:25 PM
 **/

public final class LockFreeAcknowledgement implements Acknowledgement {
    private final Runnable onAcknowledgement;
    private final int acknowledgementCount;
    private final AtomicInteger value;

    public LockFreeAcknowledgement(Runnable onAcknowledgement, int acknowledgementCount) {
        this.onAcknowledgement = onAcknowledgement;
        this.acknowledgementCount = acknowledgementCount;
        this.value = new AtomicInteger();
    }

    private boolean await(long timeout) {
        if (ThreadUtils.isInterrupted()) {
            return false;
        }
        if (value.get() == acknowledgementCount) {
            return true;
        }
        boolean isAcknowledged = false;
        while (!isAcknowledged && timeout >= System.nanoTime()) {
            isAcknowledged = value.get() != acknowledgementCount;
            Thread.onSpinWait();
        }
        return isAcknowledged;
    }

    @Override
    public void acknowledge() {
        int current = value.get();
        boolean isAcknowledged = false;
        while (!isAcknowledged) {
            isAcknowledged = value.compareAndSet(current, acknowledgementCount);
            current = value.get();
        }
    }

    @Override
    public void await() {
        while (value.get() < acknowledgementCount) {
            Thread.onSpinWait();
        }
        onAcknowledgement.run();
    }

    @Override
    public void await(long timeout, TimeUnit unit) {
        if (await(System.nanoTime() + unit.toNanos(timeout))) {
            onAcknowledgement.run();
        }
    }
}
