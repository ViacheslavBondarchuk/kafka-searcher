package io.github.viacheslavbondarchuk.kafkasearcher.web.service;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import io.github.viacheslavbondarchuk.kafkasearcher.async.handler.ErrorHandler;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * author: vbondarchuk
 * date: 5/1/2024
 * time: 11:51 PM
 **/

@Service
public class OpenApiService {
    private static final String OPENDOC_FILE_PATH = "swagger/kafka-searcher-api.yml";

    private final ErrorHandler errorHandler;
    private final YAMLFactory yamlFactory;
    private final Map<Class<?>, TreeNode> openAPIMap;

    public OpenApiService(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
        this.yamlFactory = new YAMLFactory(new YAMLMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES));
        this.openAPIMap = new ConcurrentHashMap<>();

    }

    private TreeNode loadOpenAPI(Class<?> unusedParameter) {
        TreeNode treeNode = MissingNode.getInstance();
        YAMLParser yamlParser = null;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(OPENDOC_FILE_PATH)) {
            if (Objects.nonNull(inputStream)) {
                yamlParser = yamlFactory.createParser(inputStream);
                treeNode = yamlParser.readValueAsTree();
            }
        } catch (Exception ex) {
            errorHandler.onError(ex);
        } finally {
            if (yamlParser != null) {
                try {
                    yamlParser.close();
                } catch (IOException ex) {
                    errorHandler.onError(ex);
                }
            }
        }
        return treeNode;
    }

    public TreeNode get() {
        return openAPIMap.computeIfAbsent(TreeNode.class, this::loadOpenAPI);
    }
}
