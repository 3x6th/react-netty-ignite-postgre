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
@RequestMapping("/check")
@Slf4j
public class PartnerController {

    private final PartnerService service;

    public PartnerController(PartnerService service) {
        this.service = service;
    }

    @GetMapping("/postgres")
    public Mono<Map<String, Object>> checkPg(
            @RequestParam String merchant,
            @RequestParam String terminal) {
        log.info("call");
        return service.getFromPostgres(merchant, terminal)
                      .doOnNext(m -> log.info("check result: {}", m));
    }

    @GetMapping("/ignite")
    public Mono<Map<String, Object>> checkIg(
            @RequestParam String merchant,
            @RequestParam String terminal) {
        log.info("call");
        return service.getFromIgnite(merchant, terminal)
                      .doOnNext(m -> log.info("check result: {}", m));
    }

    @GetMapping("/map")
    public Mono<Map<String, Object>> checkMp(
            @RequestParam String merchant,
            @RequestParam String terminal) {
        log.info("call");
        return service.getFromMap(merchant, terminal)
                      .doOnNext(m -> log.info("check result: {}", m));
    }

    @GetMapping("/hazelcast")
    public Mono<Map<String, Object>> checkHz(
            @RequestParam String merchant,
            @RequestParam String terminal) {
        log.info("call");
        return service.getFromHazelcast(merchant, terminal)
                      .doOnNext(m -> log.info("check result: {}", m));
    }

}
