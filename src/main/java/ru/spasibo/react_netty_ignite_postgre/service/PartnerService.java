package ru.spasibo.react_netty_ignite_postgre.service;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.apache.ignite.client.ClientCache;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.spasibo.react_netty_ignite_postgre.postgres.PartnerFlag;
import ru.spasibo.react_netty_ignite_postgre.postgres.PartnerFlagRepository;

@Service
public class PartnerService {

    private final PartnerFlagRepository repo;
    private final ClientCache<String, Boolean> cache;

    public PartnerService(PartnerFlagRepository repo, ClientCache<String, Boolean> cache) {
        this.repo = repo;
        this.cache = cache;
    }

    private String cacheKey(String merchant, String terminal) {
        return merchant + "|" + terminal;
    }

    public Mono<Map<String, Object>> checkBoth(String merchant, String terminal) {
        // Postgres (полностью реактивно)
        Instant pStart = Instant.now();
        Mono<Boolean> pg = repo.findFirstByMerchantCodeAndTerminalId(merchant, terminal)
                               .map(PartnerFlag::partner)
                               .defaultIfEmpty(false)
                               .map(v -> {
                                   // side-timing через map
                                   long ms = Duration.between(pStart, Instant.now()).toMillis();
                                   return v; // лог сделаем в zip
                               });

        // Ignite (синхронный thin client) -> обёртка в boundedElastic
        Instant iStart = Instant.now();
        Mono<Boolean> ig = Mono.fromCallable(() -> cache.get(cacheKey(merchant, terminal)))
                               .subscribeOn(Schedulers.boundedElastic())
                               .map(v -> v != null && v)
                               .onErrorReturn(false);

        return Mono.zip(pg, ig)
                   .map(tuple -> Map.of(
                           "merchant", merchant,
                           "terminal", terminal,
                           "postgresPartner", tuple.getT1(),
                           "ignitePartner", tuple.getT2(),
                           "pgMillis", Duration.between(pStart, Instant.now()).toMillis(),
                           "igniteMillis", Duration.between(iStart, Instant.now()).toMillis()
                   ));
    }

}