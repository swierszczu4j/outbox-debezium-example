package com.example.debezium.outbox

import com.vladmihalcea.hibernate.type.json.JsonBinaryType
import org.hibernate.annotations.Type
import org.hibernate.annotations.TypeDef
import java.time.Instant
import java.time.Instant.now
import java.util.*
import java.util.UUID.randomUUID
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table

@Entity
@Table(name = "outbox_event", schema = "public")
@TypeDef(name = "jsonb", typeClass = JsonBinaryType::class)
data class OutboxEventEntity(
        @Id
        val id: UUID = randomUUID(),
        val timestamp: Instant = now(),
        @Column(name = "aggregate_id", nullable = false)
        val aggregateId: UUID,
        @Column(name = "destination_topic", nullable = false)
        val destinationTopic: String,
        @Type(type = "jsonb")
        @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
        val payload: String,
        val type: String
)
