package io.serialized.kafka.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS;
import static io.serialized.kafka.connect.SerializedSourceConnector.FEED_NAME;
import static io.serialized.kafka.connect.SerializedSourceConnector.POLL_DELAY_MS_CONFIG;
import static io.serialized.kafka.connect.SerializedSourceConnector.SERIALIZED_ACCESS_KEY;
import static io.serialized.kafka.connect.SerializedSourceConnector.SERIALIZED_SECRET_ACCESS_KEY;
import static io.serialized.kafka.connect.SerializedSourceConnector.TASK_BATCH_SIZE_CONFIG;
import static io.serialized.kafka.connect.SerializedSourceConnector.TOPIC_CONFIG;
import static io.serialized.kafka.connect.SerializedSourceConnector.WAIT_TIME_MS_CONFIG;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

public class SerializedSourceTask extends SourceTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerializedSourceTask.class);

  private static final String SERIALIZED_API_URL = "https://api.serialized.io/";
  private static final String ID_FIELD = "id";
  private static final String SEQUENCE_NUMBER = "seqno";

  private OkHttpClient httpClient;
  private ObjectMapper objectMapper;

  private String feedName;
  private String topic;
  private int batchSize;
  private int pollDelayMs;
  private int waitTimeMs;
  private long lastConsumedSequenceNumber;

  @Override
  public void start(Map<String, String> props) {
    feedName = props.get(FEED_NAME);
    topic = props.get(TOPIC_CONFIG);
    batchSize = Integer.parseInt(props.get(TASK_BATCH_SIZE_CONFIG));
    pollDelayMs = Integer.parseInt(props.get(POLL_DELAY_MS_CONFIG));
    waitTimeMs = Integer.parseInt(props.get(WAIT_TIME_MS_CONFIG));
    objectMapper = newObjectMapper();
    httpClient = newHttpClient(props.get(SERIALIZED_ACCESS_KEY), props.get(SERIALIZED_SECRET_ACCESS_KEY));
    lastConsumedSequenceNumber = calculateLastConsumedSequenceNumber();
  }

  @Override
  public List<SourceRecord> poll() {
    try {
      LOGGER.info("Polling [{}] since: {}", feedName, lastConsumedSequenceNumber);

      Feed feed = fetchEventsSince(lastConsumedSequenceNumber);

      List<SourceRecord> records = new ArrayList<>();
      for (Feed.FeedEntry entry : feed.entries) {
        Map<String, ?> sourcePartition = singletonMap(ID_FIELD, feedName);
        Map<String, ?> sourceOffset = singletonMap(SEQUENCE_NUMBER, entry.sequenceNumber);
        records.add(new SourceRecord(sourcePartition, sourceOffset, topic, STRING_SCHEMA, objectMapper.writeValueAsString(entry)));
        lastConsumedSequenceNumber = entry.sequenceNumber;
      }

      if (records.isEmpty()) {
        Thread.sleep(pollDelayMs);
        return null;
      } else {
        LOGGER.info("Returning [{}] records", records.size());
        return records;
      }
    } catch (Exception e) {
      LOGGER.warn(format("Error polling feed: %s", e.getMessage()));
      throw new ConnectException(e);
    }
  }

  private Feed fetchEventsSince(long sequenceNumber) {
    Request request = newRequest(sequenceNumber);
    try (Response response = httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        throw new ConnectException(format("Error response [%s] from [%s]", response.message(), request.url().toString()));
      } else {
        return objectMapper.readValue(response.body().string(), Feed.class);
      }
    } catch (IOException ioex) {
      throw new ConnectException(ioex);
    }
  }

  private Request newRequest(long sequenceNumber) {
    HttpUrl url = HttpUrl.get(SERIALIZED_API_URL).newBuilder()
        .addPathSegment("feeds")
        .addPathSegment(feedName)
        .addQueryParameter("since", Long.toString(sequenceNumber))
        .addQueryParameter("limit", Long.toString(batchSize))
        .addQueryParameter("waitTime", Long.toString(waitTimeMs))
        .build();
    return new Request.Builder().url(url).build();
  }

  private Long calculateLastConsumedSequenceNumber() {
    Map<String, Object> offset = context.offsetStorageReader().offset(singletonMap(ID_FIELD, feedName));
    if (offset != null) {
      Long lastRecordedOffset = (Long) offset.get(SEQUENCE_NUMBER);
      if (lastRecordedOffset != null) {
        LOGGER.info("Found previous offset: {}", lastRecordedOffset);
        return lastRecordedOffset;
      }
    }
    LOGGER.warn("Did not find any previous offset. Defaulting to zero");
    return 0L;
  }

  private static OkHttpClient newHttpClient(String accessKey, String secretAccessKey) {
    return new OkHttpClient.Builder()
        .addInterceptor(chain -> chain.proceed(chain.request().newBuilder()
            .headers(new Headers.Builder()
                .add("Serialized-Access-Key", accessKey)
                .add("Serialized-Secret-Access-Key", secretAccessKey)
                .build())
            .build()))
        .build();
  }

  private static ObjectMapper newObjectMapper() {
    return new ObjectMapper()
        .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(FAIL_ON_EMPTY_BEANS, false)
        .setSerializationInclusion(NON_NULL);
  }

  public void stop() {
  }

  @Override
  public String version() {
    return new SerializedSourceConnector().version();
  }

  private static class Feed {

    public List<FeedEntry> entries;

    @SuppressWarnings("unused")
    public static class FeedEntry {
      public long sequenceNumber;
      public String aggregateId;
      public long timestamp;
      public String feedName;
      public List<Event> events;

      public static class Event {
        public String eventId;
        public String eventType;
        public Map<String, Object> data;
        public String encryptedData;
      }
    }
  }

}
