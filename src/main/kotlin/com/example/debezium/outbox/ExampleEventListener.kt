package com.example.debezium.outbox

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class ExampleEventListener {

    private val logger = LoggerFactory.getLogger(ExampleEventListener::class.java)

    @KafkaListener(topics = ["\${example.topic-name}"])
    fun listen(@Payload value: String) {
        logger.info("Received: $value")
    }
}