package ru.spasibo.react_netty_ignite_postgre.controller;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import ru.spasibo.react_netty_ignite_postgre.service.PartnerService;

@RestController
@RequestMapping("/v1")
@Slf4j
public class PartnerController {

    private final PartnerService service;

    public PartnerController(PartnerService service) {
        this.service = service;
    }

    @GetMapping("/check")
    public Mono<Map<String, Object>> check(
            @RequestParam String merchant,
            @RequestParam String terminal) {
        return service.checkBoth(merchant, terminal)
                      .doOnNext(m -> log.info("check result: {}", m));
    }

}
