package io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement.Acknowledgement;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement.BlockingAcknowledgement;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.KafkaConsumerStatus;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaConsumerSubscriber;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.CloneUtils;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.ThreadUtils;
import io.vavr.control.Try;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:51 PM
 **/

public abstract class AbstractObservableKafkaConsumer<K, V, T> implements ObservableKafkaConsumer<T> {
    protected final static Logger logger = LoggerFactory.getLogger(AbstractObservableKafkaConsumer.class);

    protected final List<KafkaConsumerSubscriber<T>> subscribers;
    protected final KafkaConsumer<K, V> consumer;
    protected final Duration pollTimeout;
    protected final String topic;
    protected final ExecutorService executor;
    protected final ErrorHandler errorHandler;
    protected final AtomicBoolean isReady = new AtomicBoolean();
    protected final AtomicLong remaining = new AtomicLong(-1);
    protected final Map<TopicPartition, Long> topicMaxOffsetPair;
    protected final Map<TopicPartition, Long> topicCurrentOffsetPair;

    public AbstractObservableKafkaConsumer(String topic, Map<String, Object> config, Duration pollTimeout, ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
        this.subscribers = new LinkedList<>();
        this.consumer = new KafkaConsumer<>(config);
        this.pollTimeout = pollTimeout;
        this.topic = topic;
        this.executor = Executors.newFixedThreadPool(2);
        this.topicMaxOffsetPair = new HashMap<>();
        this.topicCurrentOffsetPair = new HashMap<>();
        this.init();
    }

    protected void init() {
        consumer.subscribe(Collections.singletonList(topic));
        consumer.endOffsets(consumer.partitionsFor(topic)
                        .stream()
                        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                        .toList())
                .forEach((partition, offset) -> topicMaxOffsetPair.put(partition, offset - 1));
    }

    private void wrapInTry(KafkaConsumerSubscriber<T> subscriber, T value, Acknowledgement acknowledgement) {
        Try.run(() -> subscriber.onNotify(CloneUtils.clone(value), acknowledgement))
                .onFailure(errorHandler::onError);
    }

    protected void notifySubscribers(T value) throws InterruptedException {
        Acknowledgement acknowledgement = new BlockingAcknowledgement(subscribers.size(), consumer::commitSync);
        subscribers.forEach(subscriber -> executor.submit(() -> wrapInTry(subscriber, value, acknowledgement)));
        acknowledgement.await(5, TimeUnit.MINUTES);
    }

    private void updateCurrentOffset(ConsumerRecords<K, V> records) {
        records.forEach(record -> topicCurrentOffsetPair.put(new TopicPartition(topic, record.partition()), record.offset()));
    }

    private void updateRemaining() {
        if (remaining.get() != 0) {
            remaining.set(Long.max(0, getRemaining()));
        }
    }

    private void updateReady() {
        if (!isReady.get()) {
            isReady.set(remaining.get() == 0);
        }
    }

    private void logRecordsCount(long count) {
        logger.info("Topic: {}, received messages: {}", topic, count);
    }

    private long getRemaining() {
        long maxOffset = 0L;
        long currentOffset = 0L;
        for (Map.Entry<TopicPartition, Long> entry : topicMaxOffsetPair.entrySet()) {
            currentOffset += Optional.ofNullable(topicCurrentOffsetPair.get(entry.getKey())).orElse(0L);
            maxOffset += entry.getValue();
        }
        return maxOffset - currentOffset;
    }

    protected abstract T transform(ConsumerRecords<K, V> records);

    @Override
    public void poll() {
        try {
            ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
            updateCurrentOffset(records);
            updateRemaining();
            updateReady();
            int count = records.count();
            if (count > 0) {
                logRecordsCount(count);
                T transformed = transform(records);
                notifySubscribers(transformed);
            }
        } catch (InterruptedException exception) {
            ThreadUtils.interrupt();
        }
    }

    @Override
    public void subscribe(KafkaConsumerSubscriber<T> subscriber) {
        if (!subscribers.contains(subscriber)) {
            subscribers.add(subscriber);
            subscriber.onSubscribe();
        }
    }

    @Override
    public void unsubscribe(KafkaConsumerSubscriber<T> subscriber) {
        subscribers.remove(subscriber);
        subscriber.onUnsubscribe();
    }

    @Override
    public KafkaConsumerStatus getStatus() {
        return new KafkaConsumerStatus(topic, isReady.get(), remaining.get());
    }

    @Override
    public boolean isReady() {
        return isReady.get();
    }

    @Override
    public void close() throws IOException {
        consumer.unsubscribe();
        consumer.close();

    }
}
