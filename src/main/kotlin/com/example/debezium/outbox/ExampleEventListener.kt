package com.example.debezium.outbox

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.Message
import org.springframework.stereotype.Component

@Component
class ExampleEventListener {

    private val logger = LoggerFactory.getLogger(ExampleEventListener::class.java)

    @KafkaListener(topics = ["\${example.topic-name}"])
    fun listen(message: Message<String>) {
        message.headers.forEach { header, value -> logger.info("Header $header: $value") }
        logger.info("Received: ${message.payload}")
    }
}
