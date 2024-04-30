package io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain.KafkaConsumerStatus;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber.KafkaConsumerSubscriber;

import java.io.Closeable;
import java.util.concurrent.Flow;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:47 PM
 **/

public interface ObservableKafkaConsumer<T> extends Closeable, AutoCloseable {

    void poll();

    void subscribe(KafkaConsumerSubscriber<T> subscriber);

    void unsubscribe(KafkaConsumerSubscriber<T> subscriber);

    boolean isReady();

    KafkaConsumerStatus getStatus();
}
