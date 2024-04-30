package io.github.viacheslavbondarchuk.kafkasearcher.async.config;

import io.github.viacheslavbondarchuk.kafkasearcher.async.policy.RejectionPolicy;

/**
 * author: vbondarchuk
 * date: 4/29/2024
 * time: 10:15 PM
 **/

public record SchedulerConfig(String name, int corePoolSize, RejectionPolicy policy, boolean daemon) {

}
