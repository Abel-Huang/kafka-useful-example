package cn.abelib.kafka.consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @Author: abel.huang
 * @Date: 2019-09-16 23:22
 */
public class NotEmptyConsumerInterceptor implements ConsumerInterceptor<String, JsonObject> {
    /**
     * 过滤掉值为空的消息
     * @param records
     * @return
     */
    @Override
    public ConsumerRecords<String, JsonObject> onConsume(ConsumerRecords<String, JsonObject> records) {
        Map<TopicPartition, List<ConsumerRecord<String, JsonObject>>> newRecords = Maps.newHashMap();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String, JsonObject>> tpRecords = records.records(tp);
            List<ConsumerRecord<String, JsonObject>> newTpRecords = Lists.newArrayList();
            for (ConsumerRecord<String, JsonObject> tpRecord: tpRecords) {
                if (! Objects.isNull(tpRecord.value())) {
                    newTpRecords.add(tpRecord);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(tp, newTpRecords);
            }
        }
        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
