package ru.spasibo.react_netty_ignite_postgre.ignite;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IgniteConfig {

    @Bean(destroyMethod = "close")
    public IgniteClient igniteClient(@Value("${ignite.addresses}") String addresses) {
        ClientConfiguration cfg = new ClientConfiguration()
                .setAddresses(addresses.split(","));

        return Ignition.startClient(cfg);
    }

    @Bean
    public ClientCache<String, Boolean> partnerCache(
            IgniteClient client,
            @Value("${ignite.cache-name}") String cacheName
    ) {
        // key = merchantCode + "|" + terminalId, value = partner flag
        return client.getOrCreateCache(cacheName);
    }

}
