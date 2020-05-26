package com.example.debezium.outbox

import org.springframework.data.jpa.repository.JpaRepository
import java.util.*

interface ExampleEntityRepository: JpaRepository<ExampleEntity, UUID>
