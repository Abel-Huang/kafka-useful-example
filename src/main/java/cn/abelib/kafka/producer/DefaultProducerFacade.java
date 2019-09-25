package cn.abelib.kafka.producer;

import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Instant;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @Author: abel.huang
 * @Date: 2019-08-31 23:25
 */
@Slf4j
public class DefaultProducerFacade {
    private KafkaProducer producer;

    public DefaultProducerFacade(Properties producerConfig) {
        this.producer = new KafkaProducer(producerConfig);
    }

    /**
     * 同步发送后忽略返回值
     */
    public void sendSyncIgnore(String key, JsonObject value, String topic) {
        try {
            String uuid = UUID.randomUUID().toString();
            JsonObject json = new JsonObject();
            json.addProperty("uuid", uuid);
            json.addProperty("timeStamp", Instant.now().getEpochSecond());
            ProducerRecord<String, JsonObject> record = new ProducerRecord<>(topic, key, value);
            producer.send(record).get();
        }catch (ExecutionException | InterruptedException e) {
            log.error(e.getMessage());
        } finally {
            producer.close();
        }
    }

    /**
     * 同步发送
     */
    public void sendSync(String key, JsonObject value, String topic) {
        try {
            ProducerRecord<String, JsonObject> record = new ProducerRecord<>(topic, key, value);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            log.info(metadata.topic() + " " + metadata.offset());
        }catch (ExecutionException | InterruptedException e) {
            log.error(e.getMessage());
        }
    }

    /**
     * 异步发送
     */
    public void sendAsync(String key, JsonObject value, String topic) {
        ProducerRecord<String, JsonObject> record = new ProducerRecord<>(topic, key, value);
        // 回调, metadata 和 exception 是互斥的，只有一个不为 null
        producer.send(record, (metadata, exception) -> {
            if (Objects.nonNull(exception)) {
                log.error(exception.getMessage());
            } else {
                log.info("topic=" + metadata.topic() + ", offset=" +  + metadata.offset());
            }
        });
    }
}
