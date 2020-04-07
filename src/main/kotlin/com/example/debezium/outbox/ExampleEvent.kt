package com.example.debezium.outbox

import java.time.Instant
import java.util.*

data class ExampleEvent(
        val id: UUID,
        val timestamp: Instant,
        val description: String
)
