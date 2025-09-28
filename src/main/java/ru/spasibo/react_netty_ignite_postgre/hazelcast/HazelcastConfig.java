package ru.spasibo.react_netty_ignite_postgre.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HazelcastConfig {

    @Bean
    public HazelcastInstance hazelcastInstance(@Value("${hazelcast.cache-name:partner_cache}") String cacheName) {
        Config config = new Config();
        config.setInstanceName("react-netty-ignite-postgre");
        
        // Настройка кеша
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(cacheName);
        mapConfig.setTimeToLiveSeconds(3600); // 1 час TTL
        mapConfig.setMaxIdleSeconds(1800);    // 30 минут idle
        
        config.addMapConfig(mapConfig);
        
        return Hazelcast.newHazelcastInstance(config);
    }

    @Bean
    public IMap<String, Boolean> hazelcastCache(
            HazelcastInstance hazelcastInstance,
            @Value("${hazelcast.cache-name:partner_cache}") String cacheName
    ) {
        return hazelcastInstance.getMap(cacheName);
    }
}
