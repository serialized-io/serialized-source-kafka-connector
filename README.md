# Kafka Source Connector for Serialized

Based on the [example code](https://github.com/confluentinc/kafka/tree/trunk/connect/file/src/main/java/org/apache/kafka/connect/file) from Confluent.

Check [here](https://docs.confluent.io/current/connect/userguide.html) for full Connect documentation.

## Building

```
mvn clean install
```

## Configure

Ensure your destination topic is created. If not run the following:

```
kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic serialized-events
```

Make sure your plugin directory (plugin.path) is set in `kafka/config/connect-standalone.properties` or `kafka/config/connect-distributed.properties` depending on which one your are using.

Create a config file, eg. `kafka/config/serialized-source.properties`. See example config params below.

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

## Deploying

Copy `target/kafka-source-connector-jar-with-dependencies.jar` to your `kafka/plugins` directory.

## Running

```
kafka/bin/connect-standalone.sh ../config/connect-standalone.properties ../config/serialized-source.properties
```

