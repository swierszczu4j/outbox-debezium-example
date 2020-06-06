# outbox-debezium-example

This is a simple Kotlin based Spring Boot app demonstrating how to achieve
an outbox pattern using Debezium Connector.

The whole solution is provided in the form of an end-to-end test.

Additionally, you can take advantage of docker-compose file, start
the application locally and register connector using
register-debezium-connector.sh. This will let you run the application
on your local machine. Beforehand, you will need to download the debezium
postgres connector plugin archive as it's described here: https://debezium.io/documentation/reference/install.html
and Kafka Connect container relies on the content of this archive.
