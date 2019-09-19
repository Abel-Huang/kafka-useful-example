package cn.abelib.kafka.producer

import cn.abelib.kafka.config.KafkaConfig
import org.junit.Test

/**
 * @Author: abel.huang
 * @Date: 2019-09-20 01:11
 */
class DefaultProducerFacadeTest {
    private val props = KafkaConfig().producerConfig()
    private val topic = "test-topic1"
    private val producer = DefaultProducerFacade(props)

    @Test
    fun startConsumerTest() {
        producer.sendAsync(topic)
    }
}