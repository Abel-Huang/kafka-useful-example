package cn.abelib.kafka.consumer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.Objects;

/**
 * @Author: abel.huang
 * @Date: 2019-09-05 22:51
 */
@Slf4j
public class JsonDeserializer implements Deserializer<JsonObject> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public JsonObject deserialize(String topic, byte[] data) {
        if (Objects.isNull(data)) {
            return null;
        }
        Gson gson = new Gson();
        try {
            String jsonString = new String(data);
            return gson.fromJson(jsonString, JsonObject.class);
        }catch (Exception e) {
            throw new SerializationException(e.getMessage());
        }
    }

    @Override
    public void close() {

    }
}
