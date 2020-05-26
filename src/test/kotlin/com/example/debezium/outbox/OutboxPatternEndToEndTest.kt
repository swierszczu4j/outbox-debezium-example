package com.example.debezium.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import io.debezium.testing.testcontainers.Connector
import io.debezium.testing.testcontainers.DebeziumContainer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.UUIDDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.awaitility.Awaitility
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.skyscreamer.jsonassert.JSONAssert
import org.skyscreamer.jsonassert.JSONCompareMode
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.TestConstructor
import org.springframework.web.client.RestTemplate
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.lifecycle.Startables
import java.io.ByteArrayInputStream
import java.nio.charset.Charset
import java.util.*
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import java.util.stream.Stream

/**
 * Test based on implementation given here:
 * https://github.com/debezium/debezium-examples/blob/master/testcontainers/src/test/java/io/debezium/examples/testcontainers/DebeziumContainerTest.java
 */
@SpringBootTest(
        classes = [
            ExampleDebeziumOutboxApplication::class
        ],
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
internal class OutboxPatternEndToEndTest(
        private val objectMapper: ObjectMapper,
        @Value("\${example.topic-name}")
        private val exampleTopicName: String
) {

    private val restTemplate = RestTemplate()

    @BeforeEach
    fun setupConnector() {
        val routeByValue = "\${routedByValue}"
        val debeziumConfig = """
            {
                "name": "outbox-connector",
                "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "plugin.name": "pgoutput",
                    "tasks.max": "1",
                    "database.hostname": "${postgresContainer.containerInfo.config.hostName}",
                    "database.port": "${postgresContainer.exposedPorts[0]}",
                    "database.user": "${postgresContainer.username}",
                    "database.password": "${postgresContainer.password}",
                    "database.dbname" : "${postgresContainer.databaseName}",
                    "database.server.name": "outbox-test-postgres-server",
                    "schema.whitelist": "public",
                    "table.whitelist" : "public.outbox_event",
                    "tombstones.on.delete" : "false",
                    "transforms" : "outbox",
                    "transforms.outbox.type" : "io.debezium.transforms.outbox.EventRouter",
                    "transforms.outbox.route.by.field" : "destination_topic",
                    "transforms.outbox.table.field.event.key":  "aggregate_id",
                    "transforms.outbox.table.field.event.payload.id": "aggregate_id",
                    "transforms.outbox.route.topic.replacement" : "${routeByValue}",
                    "transforms.outbox.table.fields.additional.placement": "type:header:eventType,trace_id:header:b3",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.storage.StringConverter"
                }
            }
        """

        registerConnector(debeziumConfig)
    }

    @Test
    fun `should receive persisted event on kafka with b3 (tracing) header`() {
        val exampleId = randomUUID()
        val exampleEntity = ExampleEntity(exampleId, "My example")
        val exampleEvent = ExampleEvent(exampleId, "Example added")

        restTemplate.postForLocation("http://localhost:8080/examples", exampleEntity)

        getConsumerForKafkaContainer().use { consumer ->

            consumer.subscribe(listOf(exampleTopicName))

            val record = KafkaTestUtils.getSingleRecord(consumer, exampleTopicName, TimeUnit.SECONDS.toMillis(10))
            assertThat(record.key()).isEqualTo(exampleId)
            JSONAssert.assertEquals(objectMapper.writeValueAsString(exampleEvent), record.value(), JSONCompareMode.LENIENT)
            assertThatCode {
                val b3Header = record.headers().headers("b3").single()
                assertThat(String(b3Header.value())).isNotBlank()
            }.doesNotThrowAnyException()

            consumer.unsubscribe()
        }
    }

    private fun getConsumerForKafkaContainer(): KafkaConsumer<UUID, String> {
        return KafkaConsumer(
                mapOf<String, Any>(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG to "test-group-${randomUUID()}",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
                ),
                UUIDDeserializer(),
                StringDeserializer())
    }

    private fun registerConnector(json: String) {
        val connector = Connector.fromJson(ByteArrayInputStream(json.toByteArray(Charset.forName("UTF-8"))))
        registerDebeziumConnector(connector.toJson(), debeziumContainer.connectors)
        Awaitility.await().atMost(10L, TimeUnit.SECONDS).until { isConnectorConfigured(connector.name) }
    }

    private fun registerDebeziumConnector(payload: String, fullUrl: String) {
        val headers = HttpHeaders().apply {
            contentType = MediaType.APPLICATION_JSON
        }
        if(!restTemplate.postForEntity(fullUrl, HttpEntity<String>(payload, headers), Any::class.java).statusCode.is2xxSuccessful) {
            throw IllegalStateException("Connector not registered!")
        }
    }

    private fun isConnectorConfigured(connectorName: String): Boolean {
        return runCatching {
            restTemplate.getForEntity(debeziumContainer.getConnector(connectorName), Any::class.java).statusCode.is2xxSuccessful
        }.getOrElse { false }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(OutboxPatternEndToEndTest::class.java)

        @BeforeAll
        @JvmStatic
        fun setup() {
            Startables.deepStart(Stream.of(kafkaContainer, postgresContainer, debeziumContainer)).join()
        }

        @JvmStatic
        @DynamicPropertySource
        fun registerDynamicProperties(registry: DynamicPropertyRegistry) {
            registry.apply {
                add("spring.datasource.url") { "jdbc:postgresql://${postgresContainer.containerIpAddress}:${postgresContainer.firstMappedPort}/${postgresContainer.databaseName}" }
                add("spring.datasource.username") { postgresContainer.username }
                add("spring.datasource.password") { postgresContainer.password }
                add("spring.kafka.bootstrap-servers") { kafkaContainer.bootstrapServers }
            }
        }

        private val network = Network.newNetwork()

        private val kafkaContainer = KafkaContainer()
                .withNetwork(network)

        /**
         * Simple DebeziumPostgresContainer could've been used, but here's the plain one to show how to achieve it
         * in more production-like scenario.
         */
        private val postgresContainer = KotlinPostgreSQLContainer()
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withCommand("postgres -c wal_level=logical")

        private val debeziumContainer = DebeziumContainer("1.1.1.Final")
                .withNetwork(network)
                .withKafka(kafkaContainer)
                .withLogConsumer(Slf4jLogConsumer(logger))
                .dependsOn(kafkaContainer)
    }

    private class KotlinPostgreSQLContainer : PostgreSQLContainer<KotlinPostgreSQLContainer>("postgres:11")
}
