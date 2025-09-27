package ru.spasibo.react_netty_ignite_postgre.service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import ru.spasibo.react_netty_ignite_postgre.postgres.PartnerFlag;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataWarmup {

    private final DatabaseClient dbc;
    private final IgniteClient igniteClient;
    private final ClientCache<String, Boolean> partnerCache;

    @Value("${warmup.enabled:true}")
    private boolean enabled;

    @Value("${warmup.records:1500000}")
    private int rows;

    @Value("${warmup.batch-size:5000}")
    private int batchSize;

    @Value("${warmup.block-until-done:true}")
    private boolean blockUntilDone;

    private static String key(String m, String t) {
        return m + "|" + t;
    }

    private PartnerFlag gen(int i) {
        String merchant = "M%07d".formatted(i % 500_000);
        String terminal = "T%07d".formatted(i);
        boolean partner = (i % 10) != 0; // 90% партнёры
        return new PartnerFlag(null, merchant, terminal, partner);
    }

    @Bean
    ApplicationRunner warmupRunner() {
        return args -> {
            if (!enabled) {
                log.info("Warmup is disabled");
                return;
            }
            log.info(
                    "Warmup start: rows={}, batchSize={}, block={}",
                    rows,
                    batchSize,
                    blockUntilDone
            );

            Mono<Void> task =
                    Mono.when(clearPg(), clearIgnite()) // 1. сначала очистка
                        .thenMany(Flux.range(1, rows)
                                      .map(this::gen)
                                      .buffer(batchSize)
                                      .concatMap(batch -> {
                                          Instant started = Instant.now();

                                          Mono<Long> pg = insertBatchPg(batch)
                                                  .doOnSuccess(cnt -> log.info(
                                                          "PG batch inserted: {} rows in {} ms",
                                                          cnt,
                                                          Duration.between(started, Instant.now())
                                                                  .toMillis()
                                                  ));

                                          Mono<Integer> ig = insertBatchIgnite(batch)
                                                  .doOnSuccess(cnt -> log.info(
                                                          "Ignite batch inserted: {} rows in {} ms",
                                                          cnt,
                                                          Duration.between(started, Instant.now())
                                                                  .toMillis()
                                                  ));

                                          return Mono.when(pg, ig).then();
                                      }))
                        .then()
                        .doOnSubscribe(s -> log.info("Warmup streaming started"))
                        .doOnSuccess(v -> log.info("Warmup finished successfully"))
                        .doOnError(e -> log.error("Warmup failed", e));

            if (blockUntilDone) {
                task.block();
            } else {
                task.subscribe();
            }
        };
    }

    /** Очистка таблицы partner_flag в Postgres. */
    private Mono<Void> clearPg() {
        log.info("Clearing Postgres table partner_flag...");
        return dbc.sql("DELETE FROM partner_flag")
                  .fetch()
                  .rowsUpdated()
                  .doOnNext(cnt -> log.info("Postgres cleared, {} rows removed", cnt))
                  .then();
    }

    /** Очистка Ignite кэша. */
    private Mono<Void> clearIgnite() {
        return Mono.fromRunnable(() -> {
                       log.info("Clearing Ignite cache...");
                       partnerCache.clear();
                       log.info("Ignite cache cleared");
                   })
                   .subscribeOn(Schedulers.boundedElastic())
                   .then();
    }

    /** Батч-вставка в Postgres. */
    private Mono<Long> insertBatchPg(List<PartnerFlag> batch) {
        final String sql = """
                    INSERT INTO partner_flag(merchant_code, terminal_id, partner)
                    VALUES (:m, :t, :p)
                """;

        return Flux.fromIterable(batch)
                   .flatMap(p -> dbc.sql(sql)
                                    .bind("m", p.merchantCode())
                                    .bind("t", p.terminalId())
                                    .bind("p", p.partner())
                                    .fetch()
                                    .rowsUpdated())
                   .reduce(0L, Long::sum);
    }

    /** Батч-вставка в Ignite. */
    private Mono<Integer> insertBatchIgnite(List<PartnerFlag> batch) {
        return Mono.fromCallable(() -> {
                       Map<String, Boolean> map = batch.stream()
                                                       .collect(Collectors.toMap(
                                                               r -> key(r.merchantCode(), r.terminalId()),
                                                               PartnerFlag::partner,
                                                               (a, b) -> b,
                                                               () -> new HashMap<>(batch.size())
                                                       ));
                       partnerCache.putAll(map);
                       return map.size();
                   })
                   .subscribeOn(Schedulers.boundedElastic());
    }

}
