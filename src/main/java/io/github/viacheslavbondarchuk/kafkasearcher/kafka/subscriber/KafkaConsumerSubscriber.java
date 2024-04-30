package io.github.viacheslavbondarchuk.kafkasearcher.kafka.subscriber;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement.Acknowledgement;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:48 PM
 **/

public interface KafkaConsumerSubscriber<T> {
    default void onSubscribe() {

    }

    default void onUnsubscribe() {

    }

    void onNotify(T value, Acknowledgement ack);
}
