package cn.abelib.kafka.admin;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * @Author: abel.huang
 * @Date: 2019-09-21 16:26
 *  用于创建主题时验证合法性, 规则是主题的分区数>=3，副本因子>=1
 *  需要在broker端配置文件配置
 */
public class ValidCreateTopicPolicy implements CreateTopicPolicy {

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (requestMetadata.numPartitions() != null || requestMetadata.replicationFactor() != null) {
            if (requestMetadata.numPartitions() < 3) {
                throw new PolicyViolationException("Topic partition at least 3, but found: " + requestMetadata.numPartitions());
            }
            if (requestMetadata.replicationFactor() < 1) {
                throw new PolicyViolationException("Topic replication at least 1, but found: " + requestMetadata.replicationFactor());
            }
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

