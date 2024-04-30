package io.github.viacheslavbondarchuk.kafkasearcher.kafka.acknowledgement;

import java.util.concurrent.TimeUnit;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 9:12 PM
 **/

public interface Acknowledgement {

    void acknowledge();

    void await();

    void await(long timeout, TimeUnit unit);
}
