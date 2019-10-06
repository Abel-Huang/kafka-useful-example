package cn.abelib.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Author: abel.huang
 * @Date: 2019-08-31 23:16
 */
@Slf4j
public class KafkaConfig {
    public Properties producerConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置自定义的Json序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, cn.abelib.kafka.producer.JsonSerializer.class.getName());
        // 失败后的最大重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 设置自定义的生产者分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, cn.abelib.kafka.producer.HashKeyPartitioner.class.getName());
        // 设置自定义生产者消息拦截器
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, cn.abelib.kafka.producer.QuotaProducerInterceptor.class.getName());
        // 是否设置幂等生产者
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // 设置transactional.id
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-producer");
        return props;
    }

    public Properties consumerConfig() {
        String groupId = "local-group";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "local-client-id");
        // 禁止自动提交位移
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 设置隔离级别
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return props;
    }

    public Properties adminConfig() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        return props;
    }
}
