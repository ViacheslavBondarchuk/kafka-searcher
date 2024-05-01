package io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement.Acknowledgement;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement.BlockingAcknowledgement;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.KafkaConsumerStatus;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaConsumerSubscriber;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.ThreadUtils;
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
import java.util.concurrent.TimeUnit;

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
    protected final ErrorHandler errorHandler;
    protected final Map<TopicPartition, Long> topicMaxOffsetMap;
    protected final Map<TopicPartition, Long> topicCurrentOffsetMap;

    public AbstractObservableKafkaConsumer(String topic, Map<String, Object> config, Duration pollTimeout, ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
        this.subscribers = new LinkedList<>();
        this.consumer = new KafkaConsumer<>(config);
        this.pollTimeout = pollTimeout;
        this.topic = topic;
        this.topicMaxOffsetMap = new HashMap<>();
        this.topicCurrentOffsetMap = new HashMap<>();
        this.init();
    }

    protected void init() {
        consumer.subscribe(Collections.singletonList(topic));
        consumer.endOffsets(consumer.partitionsFor(topic)
                        .stream()
                        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                        .toList())
                .forEach((partition, offset) -> topicMaxOffsetMap.put(partition, offset - 1L));
    }

    protected void notifySubscribers(T value) throws InterruptedException {
        Acknowledgement acknowledgement = new BlockingAcknowledgement(subscribers.size(), consumer::commitSync);
        subscribers.forEach(subscriber -> subscriber.onNotify(value, acknowledgement));
        acknowledgement.await(15, TimeUnit.SECONDS);
    }

    private void updateCurrentOffset(ConsumerRecords<K, V> records) {
        records.forEach(record -> topicCurrentOffsetMap.put(new TopicPartition(topic, record.partition()), record.offset()));
    }

    private void logRecordsCount(long count) {
        logger.info("Topic: {}, received messages: {}", topic, count);
    }

    private long getRemaining() {
        long maxOffset = 0L;
        long currentOffset = 0L;
        for (Map.Entry<TopicPartition, Long> entry : topicMaxOffsetMap.entrySet()) {
            currentOffset += Optional.ofNullable(topicCurrentOffsetMap.get(entry.getKey())).orElse(0L);
            maxOffset += entry.getValue();
        }
        return Long.max(maxOffset - currentOffset, 0L);
    }

    protected abstract T transform(ConsumerRecords<K, V> records);

    @Override
    public void poll() {
        try {
            ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
            updateCurrentOffset(records);
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

    private boolean checkReady(Long offset) {
        return offset < 100L;
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
        long remaining = getRemaining();
        boolean ready = checkReady(remaining);
        return new KafkaConsumerStatus(topic, ready, ready ? 0L : remaining);
    }

    @Override
    public boolean isReady() {
        return checkReady(getRemaining());
    }

    @Override
    public void close() throws IOException {
        consumer.unsubscribe();
        consumer.close();

    }
}
