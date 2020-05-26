package com.example.debezium.outbox

import java.util.*

data class ExampleEvent(
        val exampleId: UUID,
        val description: String
)
