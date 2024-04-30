package io.github.viacheslavbondarchuk.kafkasearcher.async.scheduler;

import io.github.viacheslavbondarchuk.kafkasearcher.async.config.SchedulerConfig;
import io.github.viacheslavbondarchuk.kafkasearcher.async.factory.SchedulerThreadFactory;
import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.async.task.AbstractListenableTask;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.ExecutorUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 7:25 PM
 **/

public final class Scheduler implements Closeable, AutoCloseable {
    private final ScheduledExecutorService executor;
    private final Map<String, Future> taskMap;

    private Scheduler(SchedulerConfig config) {
        this.executor = new ScheduledThreadPoolExecutor(config.corePoolSize(),
                SchedulerThreadFactory.newSchedulerThreadFactory(config.name(), config.daemon()), config.policy());
        this.taskMap = new ConcurrentHashMap<>();
    }

    public static Scheduler newScheduler(SchedulerConfig config) {
        return new Scheduler(config);
    }

    private void assertUniqueTaskName(String taskName) {
        if (taskMap.containsKey(taskName)) {
            throw new RuntimeException("Duplicate task name: " + taskName);
        }
    }

    public <T> void schedule(String name, Callable<T> callable, Consumer<T> consumer, long delay, TimeUnit timeUnit, ErrorHandler errorHandler) {
        assertUniqueTaskName(name);
        ScheduledFuture<?> task = executor.schedule(AbstractListenableTask.newCallableTask(name, callable, consumer, errorHandler), delay, timeUnit);
        taskMap.put(name, task);
    }

    public <T> void schedule(String name, Runnable runnable, Runnable onComplete, long delay, TimeUnit timeUnit, ErrorHandler errorHandler) {
        assertUniqueTaskName(name);
        ScheduledFuture<?> task = executor.schedule(AbstractListenableTask.newRunnableTask(name, runnable, onComplete, errorHandler), delay,
                timeUnit);
        taskMap.put(name, task);
    }

    public void schedule(String name, Runnable runnable, long delay, TimeUnit timeUnit, ErrorHandler errorHandler) {
        schedule(name, runnable, null, delay, timeUnit, errorHandler);
    }

    public <T> void scheduleWithFixedDelay(String name, Callable<T> callable, Consumer<T> onComplete,
                                           long initialDelay, long delay, TimeUnit timeUnit, ErrorHandler errorHandler) {
        assertUniqueTaskName(name);
        ScheduledFuture<?> task = executor.scheduleWithFixedDelay(
                AbstractListenableTask.newCallableTask(name, callable, onComplete, errorHandler), initialDelay, delay, timeUnit);
        taskMap.put(name, task);
    }

    public void scheduleWithFixedDelay(String name, Runnable runnable, Runnable onComplete,
                                       long initialDelay, long delay, TimeUnit timeUnit, ErrorHandler errorHandler) {
        assertUniqueTaskName(name);
        ScheduledFuture<?> task = executor.scheduleWithFixedDelay(
                AbstractListenableTask.newRunnableTask(name, runnable, onComplete, errorHandler), initialDelay, delay, timeUnit);
        taskMap.put(name, task);
    }

    public void scheduleWithFixedDelay(String name, Runnable runnable, long initialDelay, long delay, TimeUnit timeUnit, ErrorHandler errorHandler) {
        scheduleWithFixedDelay(name, runnable, null, initialDelay, delay, timeUnit, errorHandler);
    }

    public <T> void scheduleAtFixedRate(String name, Callable<T> callable,
                                        Consumer<T> consumer, long initialDelay, long period, TimeUnit timeUnit, ErrorHandler errorHandler) {
        assertUniqueTaskName(name);
        ScheduledFuture<?> task = executor.scheduleAtFixedRate(
                AbstractListenableTask.newCallableTask(name, callable, consumer, errorHandler), initialDelay, period, timeUnit);
        taskMap.put(name, task);
    }

    public void scheduleAtFixedRate(String name, Runnable runnable, Runnable onComplete,
                                    long initialDelay, long period, TimeUnit timeUnit, ErrorHandler errorHandler) {
        assertUniqueTaskName(name);
        ScheduledFuture<?> task = executor.scheduleAtFixedRate(
                AbstractListenableTask.newRunnableTask(name, runnable, onComplete, errorHandler), initialDelay, period, timeUnit);
        taskMap.put(name, task);
    }

    public void scheduleAtFixedRate(String name, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit, ErrorHandler errorHandler) {
        scheduleAtFixedRate(name, runnable, null, initialDelay, period, timeUnit, errorHandler);
    }

    public void cancel(String name, boolean mayInterruptIfRunning) {
        Future task = taskMap.get(name);
        if (task != null) {
            task.cancel(mayInterruptIfRunning);
            taskMap.remove(name);
        }
    }

    @Override
    public void close() throws IOException {
        ExecutorUtils.shutdown(executor, 15, TimeUnit.SECONDS);
    }
}
