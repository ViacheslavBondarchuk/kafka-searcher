package io.github.viacheslavbondarchuk.kafkasearcher.kafka.consumer;

import java.io.Closeable;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:47 PM
 **/

public interface ListenableKafkaConsumer<K, V> extends Closeable, AutoCloseable {

    void poll();

    String getTopic();

}
