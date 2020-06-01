package com.example.debezium.outbox

import brave.Tracer
import brave.propagation.B3SingleFormat
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
class ExampleEventService(
        private val outboxEventEntityRepository: OutboxEventEntityRepository,
        private val objectMapper: ObjectMapper,
        private val tracer: Tracer,
        @Value("\${example.topic-name}")
        private val exampleTopicName: String
) {

    fun publishEvent(event: ExampleEvent) {
        val traceId = B3SingleFormat.writeB3SingleFormat(tracer.currentSpan().context())
        val outboxEvent = OutboxEventEntity(
                destinationTopic = exampleTopicName,
                aggregateId = event.exampleId,
                type = event.javaClass.simpleName,
                payload = objectMapper.writeValueAsString(event),
                traceId = traceId
        )
        outboxEventEntityRepository.save(outboxEvent)
    }
}
