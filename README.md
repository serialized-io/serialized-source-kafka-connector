# Kafka Source Connector for Serialized

Based on the [example code](https://github.com/confluentinc/kafka/tree/trunk/connect/file/src/main/java/org/apache/kafka/connect/file) from Confluent.

## Building

```
mvn clean install
```

## Deploying

Copy `target/kafka-source-connector-jar-with-dependencies.jar` to your `kafka/plugins` directory.

## Configure

Create a config file eg. `kafka/config/serialized-source.properties`

### Sample config

```
name=SerializedSourceConnector
connector.class=io.serialized.kafka.connect.SerializedSourceConnector
topic=<destination-topic-name>
serialized.access.key=<your-access-key>
serialized.secret.access.key=<your-secret-access-key>
```

### Optional config parameters

```
feed.name (defaults to _all)
batch.size (defaults to 100)
poll.delay.ms (defaults to 2000)
```
