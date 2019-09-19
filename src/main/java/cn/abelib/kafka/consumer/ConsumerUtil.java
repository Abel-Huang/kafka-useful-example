package cn.abelib.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @Author: abel.huang
 * @Date: 2019-09-18 23:22
 */
@Slf4j
public class ConsumerUtil {

    public static  void solveMsg(ConsumerRecord<String, String> record) {
        log.info(record.toString());
    }

    public static int partitionsForTopic(Properties props, String topic) {
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer.partitionsFor(topic).size();
    }
}
