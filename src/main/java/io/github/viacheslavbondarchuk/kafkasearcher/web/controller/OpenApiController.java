package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import com.fasterxml.jackson.core.TreeNode;
import io.github.viacheslavbondarchuk.kafkasearcher.web.service.OpenApiService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * author: vbondarchuk
 * date: 5/1/2024
 * time: 11:51 PM
 **/

@RestController
@RequestMapping("/swagger/openapi")
public final class OpenApiController {
    private final OpenApiService openApiService;

    public OpenApiController(OpenApiService openApiService) {
        this.openApiService = openApiService;
    }

    @GetMapping
    public TreeNode getOpenApi() {
        return openApiService.get();
    }
}
