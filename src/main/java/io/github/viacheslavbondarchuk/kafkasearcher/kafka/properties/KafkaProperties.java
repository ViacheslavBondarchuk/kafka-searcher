package io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties;

import io.github.viacheslavbondarchuk.kafkasearcher.security.service.JKSFileService;
import io.github.viacheslavbondarchuk.kafkasearcher.utils.CommonUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.github.viacheslavbondarchuk.kafkasearcher.security.domain.JKSFileType.OFFER_SEARCHER_KEYSTORE;
import static io.github.viacheslavbondarchuk.kafkasearcher.security.domain.JKSFileType.OFFER_SEARCHER_TRUSTSTORE;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 10:29 PM
 **/

@ConfigurationProperties("io.offer-searcher.transport.kafka")
public record KafkaProperties(String bootstrapServers,
                              Integer requestTimeout,
                              Integer fetchMaxWait,
                              Integer maxMessageBytes,
                              Integer batchSize,
                              Integer fetchMaxBytes,
                              String securityProtocol,
                              SSLProperties sslProperties) {
    public static final String SECURITY_INTER_BROKER_PROTOCOL = "security.inter.broker.protocol";
    public static final String MAX_MESSAGE_BYTES = "max.message.bytes";
    public static final String SSL_PROTOCOL = "SSL";
    public static final String KAFKA_SEARCHER = "kafka-searcher";

    public <T> T map(Function<KafkaProperties, T> function) {
        return function.apply(this);
    }

    public record SSLProperties(String enabledProtocols,
                                String keyPassword,
                                String keystore,
                                String keystorePassword,
                                String keystoreType,
                                String securityInterBrokerProtocol,
                                String truststore,
                                String truststorePassword,
                                String truststoreType,
                                String endpointIdentificationAlgorithm) {
    }

    public Map<String, Object> load(JKSFileService jksFileService, String topic) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(GROUP_ID_CONFIG, CommonUtils.createGroupId(KAFKA_SEARCHER, topic));
        properties.put(ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);

        Optional.ofNullable(batchSize).ifPresent(bs -> properties.put(MAX_POLL_RECORDS_CONFIG, bs));
        Optional.ofNullable(fetchMaxWait).ifPresent(fmw -> properties.put(FETCH_MAX_WAIT_MS_CONFIG, fmw));
        Optional.ofNullable(requestTimeout).ifPresent(rt -> properties.put(REQUEST_TIMEOUT_MS_CONFIG, rt));
        Optional.ofNullable(fetchMaxBytes).ifPresent(fmb -> properties.put(FETCH_MAX_BYTES_CONFIG, fmb));
        Optional.ofNullable(maxMessageBytes).ifPresent(mmb -> properties.put(MAX_MESSAGE_BYTES, mmb));

        if (SSL_PROTOCOL.equals(securityProtocol)) {
            properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            CommonUtils.acceptIfNonNull(SECURITY_INTER_BROKER_PROTOCOL, sslProperties.securityInterBrokerProtocol(), properties::put);
            CommonUtils.acceptIfNonNull(SSL_KEYSTORE_TYPE_CONFIG, sslProperties.keystoreType(), properties::put);
            CommonUtils.acceptIfNonNull(SSL_KEYSTORE_LOCATION_CONFIG, jksFileService.getFilePath(OFFER_SEARCHER_KEYSTORE), properties::put);
            CommonUtils.acceptIfNonNull(SSL_KEYSTORE_PASSWORD_CONFIG, sslProperties.keystorePassword(), properties::put);
            CommonUtils.acceptIfNonNull(SSL_TRUSTSTORE_TYPE_CONFIG, sslProperties.truststoreType(), properties::put);
            CommonUtils.acceptIfNonNull(SSL_TRUSTSTORE_LOCATION_CONFIG, jksFileService.getFilePath(OFFER_SEARCHER_TRUSTSTORE), properties::put);
            CommonUtils.acceptIfNonNull(SSL_TRUSTSTORE_PASSWORD_CONFIG, sslProperties.truststorePassword(), properties::put);
            CommonUtils.acceptIfNonNull(SSL_ENABLED_PROTOCOLS_CONFIG, sslProperties.enabledProtocols(), properties::put);
            CommonUtils.acceptIfNonNull(SSL_KEY_PASSWORD_CONFIG, sslProperties.keyPassword(), properties::put);
            CommonUtils.acceptIfNonNull(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslProperties.endpointIdentificationAlgorithm, properties::put);
        }
        return properties;
    }


}


