package io.github.viacheslavbondarchuk.kafkasearcher.async.factory;

import java.text.MessageFormat;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:19 PM
 **/

public final class SchedulerThreadFactory implements ThreadFactory {
    private static final String THREAD_NAME_TEMPLATE = "{0}-scheduler-{1}-th";
    private static final String GROUP_NAME_TEMPLATE = "{0}-scheduler-group";

    private final AtomicInteger counter;
    private final ThreadGroup group;
    private final String name;
    private final boolean daemon;


    private SchedulerThreadFactory(String name, boolean daemon) {
        this.name = name;
        this.group = new ThreadGroup(MessageFormat.format(GROUP_NAME_TEMPLATE, name));
        this.counter = new AtomicInteger();
        this.daemon = daemon;
    }

    public static SchedulerThreadFactory newSchedulerThreadFactory(String name, boolean daemon) {
        return new SchedulerThreadFactory(name, daemon);
    }

    @Override
    public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(group, runnable, MessageFormat.format(THREAD_NAME_TEMPLATE, name, counter.getAndIncrement()));
        thread.setDaemon(daemon);
        return thread;
    }
}
