package com.example.debezium.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import io.debezium.testing.testcontainers.Connector
import io.debezium.testing.testcontainers.DebeziumContainer
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Response
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.skyscreamer.jsonassert.JSONAssert
import org.skyscreamer.jsonassert.JSONCompareMode
import org.slf4j.LoggerFactory
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.test.utils.KafkaTestUtils
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.TestConstructor
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.lifecycle.Startables
import java.io.ByteArrayInputStream
import java.io.IOException
import java.nio.charset.Charset
import java.time.Instant.now
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import java.util.stream.Stream

/**
 * Test based on implementation given here:
 * https://github.com/debezium/debezium-examples/blob/master/testcontainers/src/test/java/io/debezium/examples/testcontainers/DebeziumContainerTest.java
 */
@SpringBootTest(classes = [ExampleDebeziumOutboxApplication::class], webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
internal class OutboxPatternEndToEndTest(
        private val outboxEventEntityRepository: OutboxEventEntityRepository,
        private val objectMapper: ObjectMapper
) {

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
                    "transforms.outbox.table.fields.additional.placement": "type:header:eventType",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "value.converter": "org.apache.kafka.connect.storage.StringConverter"
                }
            }
        """

        registerConnector(debeziumConfig)
    }

    @Test
    fun `should receive persisted event on kafka`() {
        val topicName = "topic.of.your.choice"
        val examplePayload = ExampleEvent(randomUUID(), now(), "Here goes a test description!")
        val examplePayloadAsJson = objectMapper.writeValueAsString(examplePayload)
        val outboxEvent = OutboxEventEntity(
                destinationTopic = topicName,
                aggregateId = examplePayload.id,
                type = examplePayload.javaClass.simpleName,
                payload = examplePayloadAsJson
        )

        outboxEventEntityRepository.save(outboxEvent)

        getConsumerForKafkaContainer().use { consumer ->

            consumer.subscribe(listOf(topicName))

            val record = KafkaTestUtils.getSingleRecord(consumer, topicName, TimeUnit.SECONDS.toMillis(10))
            assertThat(record.key()).isEqualTo(outboxEvent.aggregateId.toString())
            JSONAssert.assertEquals(examplePayloadAsJson, record.value(), JSONCompareMode.LENIENT)

            consumer.unsubscribe()
        }

    }

    private fun getConsumerForKafkaContainer(): KafkaConsumer<String, String> {
        return KafkaConsumer(
                mapOf<String, Any>(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG to "test-group-${randomUUID()}",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest"
                ),
                StringDeserializer(),
                StringDeserializer())
    }

    private fun registerConnector(json: String) {
        val connector = Connector.fromJson(ByteArrayInputStream(json.toByteArray(Charset.forName("UTF-8"))))
        registerConnectorToDebezium(connector.toJson(), debeziumContainer.connectors)
        Awaitility.await().atMost(10L, TimeUnit.SECONDS).until { isConnectorConfigured(connector.name) }
    }

    private fun registerConnectorToDebezium(payload: String, fullUrl: String) {
        val body: RequestBody = RequestBody.create(DebeziumContainer.JSON, payload)
        val request = Request.Builder().url(fullUrl).post(body).build()
        val response: Response? = OkHttpClient().newCall(request).execute()
        response?.use {
            if (!response.isSuccessful) {
                throw IOException("Unexpected code " + response + "Message: " + response.body()!!.string())
            }
        } ?: throw RuntimeException("Problem with creating Response object!")
    }

    private fun isConnectorConfigured(connectorName: String): Boolean {
        val request: Request = Request.Builder().url(debeziumContainer.getConnector(connectorName)).build()
        val response: Response? = OkHttpClient().newCall(request).execute()
        return response?.use {
            response.isSuccessful
        } ?: throw RuntimeException("Problem with creating Response object!")
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

        private val debeziumContainer = DebeziumContainer("1.1.0.Final")
                .withNetwork(network)
                .withKafka(kafkaContainer)
                .withLogConsumer(Slf4jLogConsumer(logger))
                .dependsOn(kafkaContainer)
    }

    private class KotlinPostgreSQLContainer : PostgreSQLContainer<KotlinPostgreSQLContainer>("postgres:11")
}
