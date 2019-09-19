package cn.abelib.kafka.producer;

import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.Objects;

/**
 * @Author: abel.huang
 * @Date: 2019-09-14 22:56
 */
@Slf4j
public class QuotaProducerInterceptor implements ProducerInterceptor<String, JsonObject> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;
    @Override
    public ProducerRecord<String, JsonObject> onSend(ProducerRecord<String, JsonObject> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (Objects.nonNull(exception)) {
            sendFailure++;
        } else {
            sendSuccess ++;
        }
    }

    @Override
    public void close() {
        double successRatio = sendSuccess / (sendFailure + sendSuccess);
        log.info("send success ratio={}%", successRatio * 100);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
