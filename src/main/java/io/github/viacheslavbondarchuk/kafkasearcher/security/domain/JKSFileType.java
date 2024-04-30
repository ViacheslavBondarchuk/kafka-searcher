package io.github.viacheslavbondarchuk.kafkasearcher.security.domain;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 10:19 PM
 **/

public enum JKSFileType {
    OFFER_SEARCHER_KEYSTORE(
            "offer-searcher-keystore.jks",
            "io.offer-searcher.transport.kafka.ssl-properties.keystore",
            "io.offer-searcher.transport.kafka.ssl-properties.keystore-password"
    ),
    OFFER_SEARCHER_TRUSTSTORE(
            "offer-searcher-truststore.jks",
            "io.offer-searcher.transport.kafka.ssl-properties.truststore",
            "io.offer-searcher.transport.kafka.ssl-properties.truststore-password"
    );

    private final String name;
    private final String propertyName;
    private final String password;

    JKSFileType(String name, String propertyName, String password) {
        this.name = name;
        this.propertyName = propertyName;
        this.password = password;
    }

    public String getName() {
        return name;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getPassword() {
        return password;
    }
}
