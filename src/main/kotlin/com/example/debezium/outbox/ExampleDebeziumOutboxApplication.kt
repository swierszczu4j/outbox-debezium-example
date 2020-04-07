package com.example.debezium.outbox

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ExampleDebeziumOutboxApplication

fun main(args: Array<String>) {
    runApplication<ExampleDebeziumOutboxApplication>(*args)
}
