package io.github.viacheslavbondarchuk.kafkasearcher.mongo.config;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.properties.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;

import java.util.List;

/**
 * author: vbondarchuk
 * date: 4/30/2024
 * time: 10:23 AM
 **/

@Configuration
public class AutoConfig extends AbstractMongoClientConfiguration {
    private final MongoProperties properties;

    public AutoConfig(MongoProperties properties) {
        this.properties = properties;
    }

    @Override
    protected String getDatabaseName() {
        return properties.database();
    }

    @Override
    public MongoClient mongoClient() {
        return MongoClients.create(MongoClientSettings.builder()
                .retryReads(true)
                .retryWrites(true)
                .credential(MongoCredential.createCredential(properties.username(), properties.database(), properties.password()))
                .applicationName("offer-searcher")
                .applyToClusterSettings(builder -> builder.hosts(List.of(new ServerAddress(properties.host()))))
                .build());
    }

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory databaseFactory) {
        return new MongoTransactionManager(databaseFactory);
    }
}
