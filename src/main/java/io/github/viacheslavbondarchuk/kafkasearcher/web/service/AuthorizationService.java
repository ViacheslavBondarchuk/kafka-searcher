package io.github.viacheslavbondarchuk.kafkasearcher.web.service;

import io.github.viacheslavbondarchuk.kafkasearcher.web.exception.AuthorizationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;

/**
 * author: vbondarchuk
 * date: 5/1/2024
 * time: 11:26 PM
 **/

@Service
public class AuthorizationService {
    private final char[] secret;

    public AuthorizationService(@Value("${io.offer-searcher.authorization.secret}") char[] secret) {
        this.secret = secret;
    }

    public void check(char[] secret) {
        if (!Arrays.equals(this.secret, secret)) {
            throw new AuthorizationException("Non authorized");
        }
    }
}
