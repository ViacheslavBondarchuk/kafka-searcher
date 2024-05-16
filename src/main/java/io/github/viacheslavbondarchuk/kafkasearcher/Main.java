package io.github.viacheslavbondarchuk.kafkasearcher;

import io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties.KafkaProperties;
import io.github.viacheslavbondarchuk.kafkasearcher.kafka.properties.KafkaSchedulerProperties;
import io.github.viacheslavbondarchuk.kafkasearcher.mongo.properties.MongoProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({
        KafkaProperties.class,
        KafkaSchedulerProperties.class,
        MongoProperties.class
})
public class Main {

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

}
