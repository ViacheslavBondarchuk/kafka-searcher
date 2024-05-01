package io.github.viacheslavbondarchuk.kafkasearcher.web.exception;

/**
 * author: vbondarchuk
 * date: 5/1/2024
 * time: 11:32 PM
 **/

public final class AuthorizationException extends RuntimeException {
    public AuthorizationException(String message) {
        super(message);
    }
}
