package org.apache.kafka.common.config.manager;

import kafka.server.DynamicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*** Didi-Kafka 灾备 1***/
public class UserConfigManager {
    public static final String DidiHAActiveClusterProp = "didi.ha.active.cluster";
    private static final Logger log = LoggerFactory.getLogger(UserConfigManager.class);
    private static Map<String, Map<String, Object>> didiHAUserConfigs = new HashMap<>();
    private static Set<String> didiHAUserConfigNames = new HashSet<>() {
        add(DidiHAActiveClusterProp);
    };
    private static Set<String> clusterRelatedConfigs = new HashSet<>() {
        add(DidiHAActiveClusterProp);
    };

    public void configure(String user, Properties configs) {
        Map<String, Object> realConfigs = DynamicConfig.getUserConfigs().parse(configs);
        Map<String, Object> realKMConfigs = new HashMap<>();
        for (String configName : didiHAUserConfigNames) {
            if (realConfigs.containsKey(configName) && !Objects.isNull(realConfigs.get(configName)))
                realKMConfigs.put(configName, realConfigs.get(configName));
        }
        if (realKMConfigs.isEmpty()) {
            if (didiHAUserConfigs.containsKey(user)) didiHAUserConfigs.remove(user);
            log.info("clean configs for user: {}", user);
        } else didiHAUserConfigs.put(user, realKMConfigs);
        log.info("set configs for user {} : {}", user, getConfigs(user));
    }

    //某User的配置中是否有对集群cluster的依赖
    public static Boolean ifConfigsClusterRelated(Properties configs, String cluster) {
        for (String clusterRelatedConfig : clusterRelatedConfigs) {
            if (configs.containsKey(clusterRelatedConfig) && Objects.equals(configs.getProperty(clusterRelatedConfig), cluster))
                return true;
        }
        return false;
    }

    public static Object getConfig(String user, String configName) {
            return didiHAUserConfigs.get(user).get(configName);
    }

    public static Map<String, Object> getConfigs(String user) {
        return didiHAUserConfigs.get(user);
    }
}