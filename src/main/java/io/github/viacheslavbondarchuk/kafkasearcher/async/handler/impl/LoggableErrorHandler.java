package io.github.viacheslavbondarchuk.kafkasearcher.async.handler.impl;

import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 7:35 PM
 **/


@Component
public final class LoggableErrorHandler implements ErrorHandler {
    private static final Logger log = LoggerFactory.getLogger(LoggableErrorHandler.class);

    @Override
    public void onError(Throwable e) {
        log.error("Error: {}", e.getMessage(), e);
    }
}
