package cn.abelib.kafka.admin

import org.junit.Test

/**
 * @Author: abel.huang
 * @Date: 2019-09-19 23:48
 */

class AdminClientTest {
    private val kafkaAdmin = KafkaAdminFacade()

    @Test
    fun createTopicTest() {
        val topic = "test-topic1"
        val result = kafkaAdmin.createTopic(topic, 3, 1)
        println(result)
    }

    @Test
    fun deleteTopicTest() {
        val topic = "test-topic1"
        val result = kafkaAdmin.deleteTopic(topic)
        println(result)
    }

    @Test
    fun describeTopicTest() {
        val topic = "test-topic1"
        val result = kafkaAdmin.describeTopic(topic)
        if (result.isPresent) {
            println(result.get())
        }
    }

    @Test
    fun listTopicTest() {
        val result = kafkaAdmin.listTopics()
        if (result.isPresent) {
            println(result.get())
        }
    }
}