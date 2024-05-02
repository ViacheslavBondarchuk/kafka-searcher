package io.github.viacheslavbondarchuk.kafkasearcher.web.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * author: vbondarchuk
 * date: 5/2/2024
 * time: 10:34 AM
 **/

@Controller
public class IndexController {

    @GetMapping
    public String index() {
        return "index";
    }
}
