curl -X PUT 'http://localhost:8083/connectors/outbox-connector/config' \
-H 'Content-Type: application/json' \
-d '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname" : "example-database",
    "database.server.name": "outbox-test-postgres-server",
    "schema.include.list": "public",
    "table.include.list" : "public.outbox_event",
    "tombstones.on.delete" : "false",
    "transforms" : "outbox",
    "transforms.outbox.type" : "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.by.field" : "destination_topic",
    "transforms.outbox.table.field.event.key":  "aggregate_id",
    "transforms.outbox.table.field.event.payload.id": "aggregate_id",
    "transforms.outbox.route.topic.replacement" : "${routedByValue}",
    "transforms.outbox.table.fields.additional.placement": "type:header:eventType,trace_id:header:b3",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter"
  }'
