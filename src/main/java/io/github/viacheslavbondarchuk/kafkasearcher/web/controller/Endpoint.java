package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import io.github.viacheslavbondarchuk.kafkasearcher.web.domain.ErrorResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 9:31 PM
 **/

public interface Endpoint {

    @ExceptionHandler(Throwable.class)
    default ResponseEntity<ErrorResponse> errorHandler(Throwable ex) {
        return ResponseEntity.status(500)
                .body(new ErrorResponse(ex.getMessage()));
    }
}
