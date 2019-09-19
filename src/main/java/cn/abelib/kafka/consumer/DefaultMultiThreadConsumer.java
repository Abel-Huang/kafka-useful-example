package cn.abelib.kafka.consumer;

import cn.abelib.kafka.config.ApplicationExecutor;
import cn.abelib.kafka.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @Author: abel.huang
 * @Date: 2019-09-18 23:00
 * 多个消费者线程同时拉取消息，处理逻辑也放在同一个线程里面
 * N:N
 */
@Slf4j
public class DefaultMultiThreadConsumer {
    private static final String TOPIC_NAME = "test-topic1";

    public static void main(String[] args) {
        Properties consumerConfig = new KafkaConfig().consumerConfig();
        // 预先获得分区数量
        int consumerNum = ConsumerUtil.partitionsForTopic(consumerConfig, TOPIC_NAME);
        Executor executor = ApplicationExecutor.getInstance();
        for (int i = 0; i < consumerNum; i++) {
            executor.execute(new KafkaConsumerClient(consumerConfig, TOPIC_NAME));
        }
    }

    public static class KafkaConsumerClient implements Runnable {
        private KafkaConsumer consumer;

        public KafkaConsumerClient(Properties props, String topic) {
            this.consumer = new KafkaConsumer(props);
            this.consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            try {
                while (true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    // 当本次没有拿到消息时，让出CPU时间片
                    if (records.count() == 0) {
                        Thread.yield();
                    }
                    for (ConsumerRecord<String, String> record : records) {
                        // 处理消息的具体消息
                        ConsumerUtil.solveMsg(record);
                    }
                }
            }catch (Exception e) {
                log.error(e.getMessage());
            }finally {
                consumer.close();
            }
        }
    }
}
