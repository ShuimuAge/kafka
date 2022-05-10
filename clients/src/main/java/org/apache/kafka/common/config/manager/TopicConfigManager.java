package org.apache.kafka.common.config.manager;

import kafka.log.LogConfig;
import kafka.server.MirrorTopic;
import kafka.server.ReplicaManager;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.*;

import java.util.*;

/*** Didi-Kafka 灾备 1 ↓ ***/
public class TopicConfigManager {

    public static final String DIDI_HA_REMOTE_CLUSTER = TopicConfig.DIDI_HA_REMOTE_CLUSTER;
    public static final String DIDI_HA_REMOTE_TOPIC = TopicConfig.DIDI_HA_REMOTE_TOPIC;
    public static final String DIDI_HA_SYNC_TOPIC_PARTITIONS_ENABLED = TopicConfig.DIDI_HA_SYNC_TOPIC_PARTITIONS_ENABLED;
    public static final String DIDI_HA_SYNC_TOPIC_CONFIGS_ENABLED = TopicConfig.DIDI_HA_SYNC_TOPIC_CONFIGS_ENABLED;
    public static final String DIDI_HA_SYNC_TOPIC_ACLS_ENABLED = TopicConfig.DIDI_HA_SYNC_TOPIC_ACLS_ENABLED;

    private static Set<String> clusterRelatedConfigs = new HashSet() {
        {
            add(DIDI_HA_REMOTE_CLUSTER);
        }
    };

    public static final Logger log = LoggerFactory.getLogger(TopicConfigManager.class);

    private ReplicaManager replicaManager;

    public TopicConfigManager(ReplicaManager replicaManager) {
        this.replicaManager = replicaManager;
    }

    //某Topic的配置中是否有对集群cluster的依赖
    public static Boolean ifConfigsClusterRelated(Properties configs, String cluster) {
        for (String clusterRelatedConfig : clusterRelatedConfigs) {
            if (configs.containsKey(clusterRelatedConfig) && Objects.equals(configs.getProperty(clusterRelatedConfig), cluster))
                return true;
        }
        return false;
    }

    public void configure(String topicName, Properties configs) {
        if (configs.isEmpty()) {
            log.info("didi-HA_config is empty");
            return;
        }

        String remoteTopicName = (String) configs.getOrDefault(DIDI_HA_REMOTE_TOPIC, topicName);
        if (Topic.isInternal(remoteTopicName) && !Objects.equals(remoteTopicName, Topic.GROUP_METADATA_TOPIC_NAME))
            throw new IllegalArgumentException(String.format("({} cannot be remote-Topic", remoteTopicName));
        //the remoteTopic cannot be any of InternalTopics
        Map<String, Object> realConfigs = JavaConverters.mapAsJavaMap(LogConfig.getRealConfigs(configs));
        if (configs.containsKey(DIDI_HA_REMOTE_CLUSTER)) {
            Boolean syncTopicPartitions = (Boolean)
                    realConfigs.getOrDefault(DIDI_HA_SYNC_TOPIC_PARTITIONS_ENABLED, false);
            Boolean syncTopicConfigs = (Boolean)
                    realConfigs.getOrDefault(DIDI_HA_SYNC_TOPIC_CONFIGS_ENABLED, false);
            Boolean syncTopicAcls = (Boolean)
                    realConfigs.getOrDefault(DIDI_HA_SYNC_TOPIC_ACLS_ENABLED, false);
            replicaManager.addMirrorTopics(new MirrorTopic(remoteTopicName,
                    configs.getProperty(DIDI_HA_REMOTE_CLUSTER),
                    topicName,
                    syncTopicPartitions,
                    syncTopicConfigs,
                    syncTopicAcls));
            log.info("add mirror topic for {} with remote topic {} {rom remote cluster {},  syncTopicPartitions:{}, syncTopicConfigs:{}, syncTopicAcls:{} ",
                    topicName,
                    remoteTopicName,
                    configs.getProperty(DIDI_HA_REMOTE_CLUSTER),
                    syncTopicPartitions,
                    syncTopicConfigs,
                    syncTopicAcls);
        } else {
            replicaManager.removeMirrorTopics(remoteTopicName);
            log.info("remove mirror topic ()", remoteTopicName);
        }
    }
}