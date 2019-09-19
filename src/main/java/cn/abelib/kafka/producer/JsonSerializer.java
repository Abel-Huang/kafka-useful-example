package cn.abelib.kafka.producer;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Objects;

/**
 * @Author: abel.huang
 * @Date: 2019-09-01 16:05
 *  自定义序列化器，基于 Gson
 */
@Slf4j
public class JsonSerializer implements Serializer<JsonObject> {

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, JsonObject data) {
        if (Objects.isNull(data)) {
            return null;
        }
        Gson gson = new Gson();
        try {
            String jsonString =  gson.toJson(data, JsonObject.class);
            return jsonString.getBytes();
        } catch (JsonIOException e) {
            log.error(e.getMessage());
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
