package cn.abelib.kafka.admin;

import cn.abelib.kafka.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @Author: abel.huang
 * @Date: 2019-09-03 23:41
 */
@Slf4j
public class KafkaAdminFacade {
    private AdminClient adminClient;

    public KafkaAdminFacade() {
        Properties properties = new KafkaConfig().adminConfig();
        this.adminClient = AdminClient.create(properties);
    }

    /**
     *  创建话题
     * @param topic
     */
    public boolean createTopic(String topic, int numPartitions, short replicationFactor) {
        NewTopic newTopic = new NewTopic(topic, numPartitions, replicationFactor);
        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
            return false;
        }
    }

    /**
     * 删除话题
     * @param topic
     */
    public boolean deleteTopic(String topic) {
        DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topic));
        try {
            result.all().get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
            return false;
        }
    }

    /**
     * 获得指定Topic信息
     * @param topic
     * @return
     */
    public Optional<Collection<ConfigEntry>> describeTopic(String topic) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        DescribeConfigsResult result = adminClient.describeConfigs(Collections.singleton(resource));
        Config config;
        try {
            config = result.all().get().get(resource);
            return Optional.ofNullable(config.entries());
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * 遍历所有的Topic
     */
    public Optional<Map<String, TopicListing>> listTopics() {
        ListTopicsResult result = adminClient.listTopics();
        try {
            Map<String, TopicListing> results = result.namesToListings().get();
            return Optional.ofNullable(results);
        } catch (InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
            return Optional.empty();
        }
    }

    /**
     *  修改Topic信息
     */
    public void alterConfigs() {

    }


    public void close() {
        adminClient.close();
    }
}
