package ru.spasibo.react_netty_ignite_postgre.service;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.client.ClientCache;
import org.openjdk.jol.info.GraphLayout;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.spasibo.react_netty_ignite_postgre.postgres.PartnerFlag;
import ru.spasibo.react_netty_ignite_postgre.postgres.PartnerFlagRepository;

@Service
@Slf4j
public class PartnerService {

    private final PartnerFlagRepository repo;
    private final ClientCache<String, Boolean> igniteCache;
    private final IMap<String, Boolean> hazelcastCache;

    private final ConcurrentHashMap<String, Boolean> map = new ConcurrentHashMap<>();

    public PartnerService(PartnerFlagRepository repo, ClientCache<String, Boolean> igniteCache, IMap<String, Boolean> hazelcastCache) {
        this.repo = repo;
        this.igniteCache = igniteCache;
        this.hazelcastCache = hazelcastCache;
    }

    public void putInMap(Map<String, Boolean> keyValue) {
        map.putAll(keyValue);
    }

    public void clearMap() {
        map.clear();
    }

    public void logSizeMap() {
        long totalSize = GraphLayout.parseInstance(map).totalSize();
        double mb = totalSize / 1024.0 / 1024.0;
        log.info(
                "ConcurrentHashMap deep size: {} bytes (~{} MB)",
                totalSize,
                String.format("%.2f", mb)
        );
    }

    public void putInHazelcast(Map<String, Boolean> keyValue) {
        hazelcastCache.putAll(keyValue);
    }

    public void clearHazelcast() {
        hazelcastCache.clear();
    }

    public void logHazelcastStats() {
        log.info("Hazelcast cache size: {} entries", hazelcastCache.size());
    }

    private String cacheKey(String merchant, String terminal) {
        return merchant + "|" + terminal;
    }

    /** 1) Postgres (полностью реактивно) */
    public Mono<Map<String, Object>> getFromPostgres(String merchant, String terminal) {
        long startNanos = System.nanoTime();
        return repo.findFirstByMerchantCodeAndTerminalId(merchant, terminal)
                   .map(PartnerFlag::partner)
                   .defaultIfEmpty(false)
                   .map(v -> {
                       long nanos = System.nanoTime() - startNanos;
                       return Map.of(
                               "merchant", merchant,
                               "terminal", terminal,
                               "postgresPartner", v,
                               "nanos", nanos,
                               "millis", nanos / 1_000_000
                       );
                   });
    }

    /** 2) Ignite (блокирующий thin client — на boundedElastic) */
    public Mono<Map<String, Object>> getFromIgnite(String merchant, String terminal) {
        long startNanos = System.nanoTime();
        return Mono.fromCallable(() -> igniteCache.get(cacheKey(merchant, terminal)))
                   .subscribeOn(Schedulers.boundedElastic())
                   .map(v -> v != null && v)
                   .map(v -> {
                       long nanos = System.nanoTime() - startNanos;
                       return Map.of(
                               "merchant", merchant,
                               "terminal", terminal,
                               "ignitePartner", v,
                               "nanos", nanos,
                               "millis", nanos / 1_000_000
                       );
                   });
        // Ошибки НЕ гасим: пробрасываем наружу
    }

    /** 3) ConcurrentHashMap (локальный кэш, неблокирующий) */
    public Mono<Map<String, Object>> getFromMap(String merchant, String terminal) {
        long startNanos = System.nanoTime();
        return Mono.fromSupplier(() -> map.get(cacheKey(merchant, terminal)))
                   .map(v -> v != null && v)
                   .map(v -> {
                       long nanos = System.nanoTime() - startNanos;
                       return Map.of(
                               "merchant", merchant,
                               "terminal", terminal,
                               "mPartner", v,
                               "nanos", nanos,
                               "millis", nanos / 1_000_000
                       );
                   });
        // Ошибки НЕ гасим: пробрасываем наружу
    }

    /** 4) Hazelcast (встроенный кэш, реактивный) */
    public Mono<Map<String, Object>> getFromHazelcast(String merchant, String terminal) {
        long startNanos = System.nanoTime();
        return Mono.fromCompletionStage(hazelcastCache.getAsync(cacheKey(merchant, terminal)))
                   .map(v -> v != null && v)
                   .map(v -> {
                       long nanos = System.nanoTime() - startNanos;
                       return Map.of(
                               "merchant", merchant,
                               "terminal", terminal,
                               "hazelcastPartner", v,
                               "nanos", nanos,
                               "millis", nanos / 1_000_000
                       );
                   });
        // Ошибки НЕ гасим: пробрасываем наружу
    }

}