package io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement;

import io.github.viacheslavbondarchuk.kafkasearcher.utils.ThreadUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 7:47 AM
 **/

public final class BlockingAcknowledgement implements Acknowledgement {
    private final CountDownLatch latch;
    private final Runnable onAcknowledgement;

    public BlockingAcknowledgement(int count, Runnable onAcknowledgement) {
        this.latch = new CountDownLatch(count);
        this.onAcknowledgement = onAcknowledgement;
    }

    @Override
    public void acknowledge() {
        latch.countDown();
    }

    @Override
    public void await() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            ThreadUtils.interrupt();
        }
    }

    @Override
    public void await(long timeout, TimeUnit unit) {
        try {
            if (latch.await(timeout, unit)) {
                onAcknowledgement.run();
            }
        } catch (InterruptedException ex) {
            ThreadUtils.interrupt();
        }
    }
}
