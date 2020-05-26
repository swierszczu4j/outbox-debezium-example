package com.example.debezium.outbox

import java.util.*
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table

@Entity
@Table(name = "example", schema = "public")
data class ExampleEntity(
        @Id
        val id: UUID = UUID.randomUUID(),
        val data: String
)
