package io.github.viacheslavbondarchuk.kafkasearcher.security.service;

import io.github.viacheslavbondarchuk.kafkasearcher.security.domain.JKSFileType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * author: vbondarchuk
 * date: 4/28/2024
 * time: 10:16 PM
 **/

@Service
public final class JKSFileService {
    public static final String JKS_KEY_STORE = "JKS";

    private final Map<JKSFileType, String> jksFilePathMap;
    private final String configFilePath;

    private String load(String name, char[] password, String value) {
        Objects.requireNonNull(password, "Name is null");
        Objects.requireNonNull(password, "Password is null");
        Objects.requireNonNull(value, "Value is null");

        String filePath = configFilePath.concat("/").concat(name);
        try (FileOutputStream outputStream = new FileOutputStream(filePath)) {
            KeyStore keyStore = KeyStore.getInstance(JKS_KEY_STORE);
            byte[] bytes = Base64.getMimeDecoder().decode(value);
            keyStore.load(new ByteArrayInputStream(bytes), password);
            keyStore.store(outputStream, password);
        } catch (Exception e) {
//            log.error("Can not load file: ", e);
        }
        return filePath;
    }

    public JKSFileService(Environment environment, @Value("${config.path}") String configPath) {
        this.configFilePath = Objects.requireNonNull(configPath, "config.path is null");
        this.jksFilePathMap = Arrays.stream(JKSFileType.values())
                .collect(Collectors.toMap(Function.identity(), type -> load(
                        type.getName(),
                        environment.getProperty(type.getPassword()).toCharArray(),
                        environment.getProperty(type.getPropertyName()))));
    }

    public String getFilePath(JKSFileType type) {
        return jksFilePathMap.get(type);
    }
}
