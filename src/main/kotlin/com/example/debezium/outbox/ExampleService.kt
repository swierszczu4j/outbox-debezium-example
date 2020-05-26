package com.example.debezium.outbox

import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.*

@Service
class ExampleService(
        private val exampleEntityRepository: ExampleEntityRepository,
        private val exampleEventService: ExampleEventService
) {

    @Transactional
    fun addExample(example: ExampleEntity) {
        exampleEntityRepository.save(example)
        exampleEventService.publishEvent(ExampleEvent(example.id, "Example added"))
    }

}
