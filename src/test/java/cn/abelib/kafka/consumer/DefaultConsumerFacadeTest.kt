package cn.abelib.kafka.consumer

import cn.abelib.kafka.config.KafkaConfig
import org.junit.Test

/**
 * @Author: abel.huang
 * @Date: 2019-09-20 01:03
 */
class DefaultConsumerFacadeTest {
    private val props = KafkaConfig().consumerConfig()
    private val topic = "test-topic1"
    private val consumer = DefaultConsumerFacade(props, topic)

    @Test
    fun startConsumerTest() {
        consumer.startConsumer()
    }
}