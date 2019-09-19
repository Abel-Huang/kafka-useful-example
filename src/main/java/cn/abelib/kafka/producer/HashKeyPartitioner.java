package cn.abelib.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @Author: abel.huang
 * @Date: 2019-09-01 16:11
 *  自定义的分区算法
 */
public class HashKeyPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (Objects.isNull(keyBytes)) {
            // random partition
            return ThreadLocalRandom.current().nextInt(partitions.size());
        }
        // hash the keyBytes to choose a partition
        return Math.abs(key.hashCode() % numPartitions);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
