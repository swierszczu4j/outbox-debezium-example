package com.example.debezium.outbox

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest(properties = [
    "spring.datasource.driver-class-name=org.testcontainers.jdbc.ContainerDatabaseDriver",
    "spring.datasource.url=jdbc:tc:postgresql:11://localhost/example-database"
])
class ExampleDebeziumOutboxApplicationTests {

    @Test
    fun contextLoads() {
    }

}
