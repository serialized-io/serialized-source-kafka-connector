package io.serialized.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SerializedSourceConnector extends SourceConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerializedSourceConnector.class);

  static final String SERIALIZED_ACCESS_KEY = "serialized.access.key";
  static final String SERIALIZED_SECRET_ACCESS_KEY = "serialized.secret.access.key";
  static final String FEED_NAME = "feed.name";
  static final String TOPIC_CONFIG = "topic";
  static final String TASK_BATCH_SIZE_CONFIG = "batch.size";
  static final String POLL_DELAY_MS_CONFIG = "poll.delay.ms";

  private static final String DEFAULT_FEED_NAME = "_all";
  private static final int DEFAULT_TASK_BATCH_SIZE = 100;
  private static final int DEFAULT_POLL_DELAY_MS = 2000;

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(SERIALIZED_ACCESS_KEY, Type.STRING, null, Importance.HIGH, "Serialized Access Key")
      .define(SERIALIZED_SECRET_ACCESS_KEY, Type.STRING, null, Importance.HIGH, "Serialized Secret Access Key")
      .define(FEED_NAME, Type.STRING, DEFAULT_FEED_NAME, Importance.LOW, "Name of feed to poll events from")
      .define(TOPIC_CONFIG, Type.LIST, Importance.HIGH, "The topic to publish events to")
      .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW, "The maximum number of records the Source task can fetch at a time")
      .define(POLL_DELAY_MS_CONFIG, Type.INT, DEFAULT_POLL_DELAY_MS, Importance.LOW, "Delay between polls in ms");

  private String feedName;
  private String topic;
  private int batchSize;
  private int pollDelayMs;
  private String accessKey;
  private String secretAccessKey;

  @Override
  public void start(Map<String, String> props) {
    LOGGER.info("Starting connector...");

    AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
    List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
    if (topics.size() != 1) {
      throw new ConfigException("'topic' in SerializedSourceConnector configuration requires definition of a single topic");
    }
    feedName = parsedConfig.getString(FEED_NAME);
    topic = topics.get(0);
    batchSize = parsedConfig.getInt(TASK_BATCH_SIZE_CONFIG);
    pollDelayMs = parsedConfig.getInt(POLL_DELAY_MS_CONFIG);
    accessKey = parsedConfig.getString(SERIALIZED_ACCESS_KEY);
    secretAccessKey = parsedConfig.getString(SERIALIZED_SECRET_ACCESS_KEY);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SerializedSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>();
    // Only one input partition
    Map<String, String> config = new HashMap<>();
    config.put(FEED_NAME, feedName);
    config.put(TOPIC_CONFIG, topic);
    config.put(TASK_BATCH_SIZE_CONFIG, String.valueOf(batchSize));
    config.put(POLL_DELAY_MS_CONFIG, String.valueOf(pollDelayMs));
    config.put(SERIALIZED_ACCESS_KEY, accessKey);
    config.put(SERIALIZED_SECRET_ACCESS_KEY, secretAccessKey);
    configs.add(config);
    return configs;
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping connector...");
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

}