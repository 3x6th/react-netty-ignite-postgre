package ru.spasibo.react_netty_ignite_postgre.postgres;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("partner_flag")
public record PartnerFlag(
        @Id Long id,
        String merchantCode,
        String terminalId,
        Boolean partner
) {}