package ru.spasibo.react_netty_ignite_postgre.postgres;

import org.springframework.data.r2dbc.repository.R2dbcRepository;
import reactor.core.publisher.Mono;

public interface PartnerFlagRepository extends R2dbcRepository<PartnerFlag, Long> {
    Mono<PartnerFlag> findFirstByMerchantCodeAndTerminalId(String merchantCode, String terminalId);
}
