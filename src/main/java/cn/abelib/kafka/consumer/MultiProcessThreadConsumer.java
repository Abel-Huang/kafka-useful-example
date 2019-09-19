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
 * @Date: 2019-09-19 00:37
 * 一个消费者对应多个处理线程，1:N
 */
@Slf4j
public class MultiProcessThreadConsumer {
    private static final String TOPIC_NAME = "test-topic1";

    public static void main(String[] args) {
        Properties consumerConfig = new KafkaConfig().consumerConfig();
        Executor executor = ApplicationExecutor.getInstance();
        executor.execute(new KafkaConsumerClient(consumerConfig, TOPIC_NAME));
    }

    public static class KafkaConsumerClient implements Runnable {
        private KafkaConsumer consumer;
        private Executor executor = ApplicationExecutor.getInstance();

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
                    executor.execute(new RecordsHandler(records));
                }
            }catch (Exception e) {
                log.error(e.getMessage());
            }finally {
                consumer.close();
            }
        }
    }

    public static class RecordsHandler implements Runnable {
        public final ConsumerRecords<String, String> records;

        public RecordsHandler(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        // todo 位移提交保证
        @Override
        public void run() {
            // 处理消息的具体消息
            for (ConsumerRecord<String, String> record : records) {
                ConsumerUtil.solveMsg(record);
            }
        }
    }
}
