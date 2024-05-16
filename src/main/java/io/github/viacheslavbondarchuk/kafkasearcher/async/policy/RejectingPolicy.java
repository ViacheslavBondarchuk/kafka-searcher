package io.github.viacheslavbondarchuk.kafkasearcher.async.policy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * author: vbondarchuk
 * date: 5/15/2024
 * time: 3:08 PM
 **/

public class RejectingPolicy implements RejectionPolicy {
    private static final Logger logger = LoggerFactory.getLogger(RejectingPolicy.class);

    @Override
    public void onReject(Runnable task, BlockingQueue<Runnable> queue, ThreadPoolExecutor executor) {
        logger.info("Rejecting task: " + task + ", executor: " + executor);
    }
}
