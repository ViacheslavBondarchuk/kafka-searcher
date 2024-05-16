package io.github.viacheslavbondarchuk.kafkasearcher.kafka.domain;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement.Acknowledgement;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 8:56 PM
 **/

public final class RecordsBatch<K, V> implements Iterable<ConsumerRecord<K, V>> {
    private final String topic;
    private final ConsumerRecords<K, V> records;
    private final Acknowledgement acknowledgement;

    public Acknowledgement getAcknowledgement() {
        return acknowledgement;
    }

    public RecordsBatch(String topic, ConsumerRecords<K, V> records, Acknowledgement acknowledgement) {
        this.topic = topic;
        this.records = records;
        this.acknowledgement = acknowledgement;
    }

    public String getTopic() {
        return topic;
    }

    public ConsumerRecords<K, V> getRecords() {
        return records;
    }

    public int size() {
        return records.count();
    }

    public Stream<ConsumerRecord<K, V>> stream() {
        return StreamSupport.stream(records.spliterator(), false);
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> iterator() {
        return records.iterator();
    }

    @Override
    public void forEach(Consumer<? super ConsumerRecord<K, V>> action) {
        records.forEach(action);
    }

    @Override
    public Spliterator<ConsumerRecord<K, V>> spliterator() {
        return records.spliterator();
    }

}
