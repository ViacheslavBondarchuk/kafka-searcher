package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import io.github.viacheslavbondarchuk.kafkasearcher.web.domain.ErrorResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 9:31 PM
 **/

public interface Endpoint {

    default ResponseEntity<?> emptyOkResponse() {
        return ResponseEntity.ok()
                .build();
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    default ResponseEntity<ErrorResponse<Set<String>>> validationErrorHandler(MethodArgumentNotValidException ex) {
        return ResponseEntity.status(500)
                .body(new ErrorResponse<>(ex.getBindingResult()
                        .getAllErrors()
                        .stream()
                        .map(ObjectError::getDefaultMessage)
                        .collect(Collectors.toSet())));
    }

    @ExceptionHandler(Throwable.class)
    default ResponseEntity<ErrorResponse<String>> errorHandler(Throwable ex) {
        return ResponseEntity.status(500)
                .body(new ErrorResponse<>(ex.getMessage()));
    }
}
