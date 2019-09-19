package cn.abelib.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: abel.huang
 * @Date: 2019-09-02 21:44
 */
@Slf4j
public class DefaultConsumerFacade {
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private KafkaConsumer consumer;
    private String topic;

    public DefaultConsumerFacade(Properties consumerConfig, String topic) {
        this.consumer =  new KafkaConsumer(consumerConfig);
        this.topic = topic;
    }

    public void startConsumer() {
        Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>(16);
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(currentOffset);
                currentOffset.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // do nothing
            }
        });
        try {
            while (isRunning.get()){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                log.info("message batch size: {}", records.count());
                for (ConsumerRecord<String, String> record : records) {
                    ConsumerUtil.solveMsg(record);
                    consumer.commitSync(currentOffset, null);
                }
                if (records.count() > 0) {
                    break;
                }
            }
        }catch (Exception e) {
            log.error(e.getMessage());
        }finally {
            consumer.close();
        }
    }

    /**
     *  暂停消费，并返回Topic和Partition信息
     * @return
     */
    public Optional<Set<TopicPartition>> pausedAndReturnTopics() {
        Set<TopicPartition> partitions = consumer.paused();
        return Optional.ofNullable(partitions);
    }
}
