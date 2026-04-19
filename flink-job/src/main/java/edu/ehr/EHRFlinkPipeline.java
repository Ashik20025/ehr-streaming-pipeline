package edu.ehr;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.JedisPooled;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class EHRFlinkPipeline {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    String bootstrap = params.get("kafka.bootstrap.servers", "kafka:9092");
    String redisHost = params.get("redis.host", "redis");
    int redisPort = params.getInt("redis.port", 6379);
    String cassandraHost = params.get("cassandra.host", "cassandra");
    int cassandraPort = params.getInt("cassandra.port", 9042);
    int allowedLatenessSeconds = params.getInt("allowed.lateness.seconds", 30);
    int parallelism = params.getInt("parallelism", 2);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    env.enableCheckpointing(10_000L, CheckpointingMode.EXACTLY_ONCE);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10_000L));

    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(bootstrap)
        .setTopics("ehr.raw")
        .setGroupId("ehr-flink-validation")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

    DataStream<RawEvent> parsed = env
        .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka ehr.raw")
        .map(RawEvent::fromJson)
        .returns(RawEvent.class)
        .assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<RawEvent>forBoundedOutOfOrderness(Duration.ofSeconds(allowedLatenessSeconds))
                .withTimestampAssigner((SerializableTimestampAssigner<RawEvent>)
                    (event, timestamp) -> event.eventEpochMillis > 0
                        ? event.eventEpochMillis
                        : System.currentTimeMillis()));

    SingleOutputStreamOperator<PipelineRecord> processed = parsed
        .keyBy(event -> event.eventId == null || event.eventId.isBlank() ? event.rawHash : event.eventId)
        .process(new ValidateDeduplicateFunction(redisHost, redisPort, allowedLatenessSeconds * 1000L))
        .name("Validate, deduplicate, enrich, and quarantine")
        .returns(PipelineRecord.class);

    DataStream<PipelineRecord> valid = processed.filter(record -> record.valid).name("Valid events");
    DataStream<PipelineRecord> quarantine = processed.filter(record -> !record.valid).name("Quarantined events");

    valid.map(record -> record.rawJson)
        .returns(String.class)
        .sinkTo(stringKafkaSink(bootstrap, "ehr.valid"))
        .name("Kafka sink ehr.valid");

    quarantine.map(PipelineRecord::toJson)
        .returns(String.class)
        .sinkTo(stringKafkaSink(bootstrap, "ehr.quarantine"))
        .name("Kafka sink ehr.quarantine");

    processed.addSink(new CassandraEventSink(cassandraHost, cassandraPort))
        .name("Cassandra event/quarantine sink");

    DataStream<AlertRecord> alerts = valid
        .keyBy(record -> record.patientId)
        .process(new SepsisRuleFunction())
        .name("Stateful possible-sepsis CEP rule")
        .returns(AlertRecord.class);

    alerts.map(AlertRecord::toJson)
        .returns(String.class)
        .sinkTo(stringKafkaSink(bootstrap, "ehr.alerts"))
        .name("Kafka sink ehr.alerts");

    alerts.addSink(new CassandraAlertSink(cassandraHost, cassandraPort))
        .name("Cassandra alert sink");

    env.execute("Streaming EHR Kafka-Flink-Redis-Cassandra Pipeline");
  }

  private static KafkaSink<String> stringKafkaSink(String bootstrap, String topic) {
    return KafkaSink.<String>builder()
        .setBootstrapServers(bootstrap)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  public static class RawEvent implements Serializable {
    public String eventId;
    public String patientId;
    public String eventType;
    public String eventTime;
    public String source;
    public String schemaVersion;
    public String payloadJson;
    public String rawJson;
    public String rawHash;
    public long eventEpochMillis;
    public long sourceEmitEpochMillis;

    public RawEvent() {
    }

    public static RawEvent fromJson(String json) {
      RawEvent event = new RawEvent();
      event.rawJson = json;
      event.rawHash = sha256(json).substring(0, 24);
      try {
        JsonNode node = MAPPER.readTree(json);
        event.eventId = textOrNull(node, "event_id");
        event.patientId = textOrNull(node, "patient_id");
        event.eventType = textOrNull(node, "event_type");
        event.eventTime = textOrNull(node, "event_time");
        event.source = textOrNull(node, "source");
        event.schemaVersion = textOrNull(node, "schema_version");
        event.payloadJson = node.has("payload") ? node.get("payload").toString() : "{}";
        event.eventEpochMillis = parseMillis(event.eventTime);
        event.sourceEmitEpochMillis = parseMillis(textOrNull(node, "source_emit_time"));
      } catch (Exception e) {
        event.eventId = event.rawHash;
        event.eventType = "unparseable";
        event.payloadJson = "{}";
        event.eventEpochMillis = -1L;
      }
      return event;
    }
  }

  public static class PipelineRecord implements Serializable {
    public boolean valid;
    public String eventId;
    public String patientId;
    public String eventType;
    public String eventTime;
    public String source;
    public String payloadJson;
    public String patientMetadataJson;
    public String rawJson;
    public List<String> errors;
    public long eventEpochMillis;
    public long sourceEmitEpochMillis;
    public long processedAtMillis;
    public double ingestionLatencyMs;

    public PipelineRecord() {
    }

    public static PipelineRecord valid(RawEvent event, String metadataJson, long processedAtMillis) {
      PipelineRecord record = base(event, processedAtMillis);
      record.valid = true;
      record.patientMetadataJson = metadataJson;
      record.errors = Collections.emptyList();
      return record;
    }

    public static PipelineRecord quarantine(RawEvent event, List<String> errors, long processedAtMillis) {
      PipelineRecord record = base(event, processedAtMillis);
      record.valid = false;
      record.patientMetadataJson = "{}";
      record.errors = errors;
      return record;
    }

    private static PipelineRecord base(RawEvent event, long processedAtMillis) {
      PipelineRecord record = new PipelineRecord();
      record.eventId = event.eventId == null || event.eventId.isBlank() ? event.rawHash : event.eventId;
      record.patientId = event.patientId == null ? "" : event.patientId;
      record.eventType = event.eventType == null ? "" : event.eventType;
      record.eventTime = event.eventTime == null ? "" : event.eventTime;
      record.source = event.source == null ? "" : event.source;
      record.payloadJson = event.payloadJson == null ? "{}" : event.payloadJson;
      record.rawJson = event.rawJson;
      record.eventEpochMillis = event.eventEpochMillis;
      record.sourceEmitEpochMillis = event.sourceEmitEpochMillis;
      record.processedAtMillis = processedAtMillis;
      record.ingestionLatencyMs = event.sourceEmitEpochMillis > 0
          ? processedAtMillis - event.sourceEmitEpochMillis
          : 0.0;
      return record;
    }

    public String firstReason() {
      return errors == null || errors.isEmpty() ? "unknown" : errors.get(0);
    }

    public String toJson() {
      try {
        return MAPPER.writeValueAsString(this);
      } catch (Exception e) {
        return "{\"serialization_error\":\"" + e.getMessage() + "\"}";
      }
    }
  }

  public static class AlertRecord implements Serializable {
    public String patientId;
    public String alertType;
    public long alertTimeMillis;
    public String eventId;
    public String detailsJson;

    public AlertRecord() {
    }

    public AlertRecord(String patientId, long alertTimeMillis, String eventId, String detailsJson) {
      this.patientId = patientId;
      this.alertType = "possible_sepsis";
      this.alertTimeMillis = alertTimeMillis;
      this.eventId = eventId;
      this.detailsJson = detailsJson;
    }

    public String toJson() {
      try {
        return MAPPER.writeValueAsString(this);
      } catch (Exception e) {
        return "{\"serialization_error\":\"" + e.getMessage() + "\"}";
      }
    }
  }

  public static class ValidateDeduplicateFunction
      extends KeyedProcessFunction<String, RawEvent, PipelineRecord> {
    private final String redisHost;
    private final int redisPort;
    private final long allowedLatenessMs;
    private transient ValueState<Boolean> seen;
    private transient JedisPooled redis;

    public ValidateDeduplicateFunction(String redisHost, int redisPort, long allowedLatenessMs) {
      this.redisHost = redisHost;
      this.redisPort = redisPort;
      this.allowedLatenessMs = allowedLatenessMs;
    }

    @Override
    public void open(Configuration parameters) {
      seen = getRuntimeContext().getState(new ValueStateDescriptor<>("seen-event-id", Boolean.class));
      redis = new JedisPooled(redisHost, redisPort);
    }

    @Override
    public void processElement(RawEvent event, Context ctx, Collector<PipelineRecord> out) throws Exception {
      long now = System.currentTimeMillis();
      List<String> errors = validate(event);

      Boolean alreadySeen = seen.value();
      if (alreadySeen != null && alreadySeen) {
        errors.add("duplicate_event_id");
      }

      long watermark = ctx.timerService().currentWatermark();
      if (errors.isEmpty()
          && watermark > Long.MIN_VALUE
          && event.eventEpochMillis + allowedLatenessMs < watermark) {
        errors.add("late_event");
      }

      if (!errors.isEmpty()) {
        out.collect(PipelineRecord.quarantine(event, errors, now));
        return;
      }

      seen.update(true);
      String metadataJson = "{}";
      try {
        Map<String, String> metadata = redis.hgetAll("patient:" + event.patientId);
        metadataJson = MAPPER.writeValueAsString(metadata == null ? new HashMap<>() : metadata);
      } catch (Exception ignored) {
        metadataJson = "{\"redis_lookup\":\"failed\"}";
      }
      out.collect(PipelineRecord.valid(event, metadataJson, now));
    }

    @Override
    public void close() {
      if (redis != null) {
        redis.close();
      }
    }
  }

  public static class SepsisRuleFunction extends KeyedProcessFunction<String, PipelineRecord, AlertRecord> {
    private transient ValueState<Double> heartRate;
    private transient ValueState<Double> temperature;
    private transient ValueState<Double> wbc;
    private transient ValueState<Double> lactate;
    private transient ValueState<Long> lastAlert;

    @Override
    public void open(Configuration parameters) {
      heartRate = getRuntimeContext().getState(new ValueStateDescriptor<>("heart-rate", Double.class));
      temperature = getRuntimeContext().getState(new ValueStateDescriptor<>("temperature", Double.class));
      wbc = getRuntimeContext().getState(new ValueStateDescriptor<>("wbc", Double.class));
      lactate = getRuntimeContext().getState(new ValueStateDescriptor<>("lactate", Double.class));
      lastAlert = getRuntimeContext().getState(new ValueStateDescriptor<>("last-alert", Long.class));
    }

    @Override
    public void processElement(PipelineRecord record, Context ctx, Collector<AlertRecord> out) throws Exception {
      JsonNode payload = MAPPER.readTree(record.payloadJson);
      if ("vital".equals(record.eventType)) {
        String name = payload.path("name").asText("");
        double value = payload.path("value").asDouble(Double.NaN);
        if ("heart_rate".equals(name)) {
          heartRate.update(value);
        } else if ("temperature_c".equals(name)) {
          temperature.update(value);
        }
      } else if ("lab".equals(record.eventType)) {
        String test = payload.path("test").asText("");
        double value = payload.path("result").asDouble(Double.NaN);
        if ("wbc".equals(test)) {
          wbc.update(value);
        } else if ("lactate".equals(test)) {
          lactate.update(value);
        }
      }

      double hr = valueOrZero(heartRate.value());
      double temp = valueOrZero(temperature.value());
      double wbcValue = valueOrZero(wbc.value());
      double lactateValue = valueOrZero(lactate.value());
      boolean ruleHit = hr >= 110.0 && temp >= 38.0 && (wbcValue >= 12.0 || lactateValue >= 2.2);
      if (!ruleHit) {
        return;
      }

      Long previous = lastAlert.value();
      if (previous != null && record.eventEpochMillis - previous < 600_000L) {
        return;
      }

      Map<String, Object> details = new HashMap<>();
      details.put("heart_rate", hr);
      details.put("temperature_c", temp);
      details.put("wbc", wbcValue);
      details.put("lactate", lactateValue);
      details.put("rule", "HR >= 110 and temp >= 38 and (WBC >= 12 or lactate >= 2.2)");
      out.collect(new AlertRecord(record.patientId, record.eventEpochMillis, record.eventId, MAPPER.writeValueAsString(details)));
      lastAlert.update(record.eventEpochMillis);
    }
  }

  public static class CassandraEventSink extends RichSinkFunction<PipelineRecord> {
    private final String host;
    private final int port;
    private transient CqlSession session;
    private transient PreparedStatement insertEvent;
    private transient PreparedStatement insertEventType;
    private transient PreparedStatement insertQuarantine;

    public CassandraEventSink(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public void open(Configuration parameters) {
      session = connect(host, port);
      insertEvent = session.prepare("insert into events_by_id "
          + "(event_id, patient_id, event_type, event_time, source, payload, patient_metadata, "
          + "ingestion_latency_ms, raw_event, processed_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
      insertEventType = session.prepare("insert into events_by_type "
          + "(event_type, event_id, patient_id, event_time) values (?, ?, ?, ?)");
      insertQuarantine = session.prepare("insert into quarantine_by_reason "
          + "(reason, observed_at, event_id, errors, raw_event) values (?, ?, ?, ?, ?)");
    }

    @Override
    public void invoke(PipelineRecord record, Context context) {
      if (record.valid) {
        Instant eventTime = instantOrNow(record.eventEpochMillis);
        session.execute(insertEvent.bind(
            record.eventId,
            record.patientId,
            record.eventType,
            eventTime,
            record.source,
            record.payloadJson,
            record.patientMetadataJson,
            record.ingestionLatencyMs,
            record.rawJson,
            instantOrNow(record.processedAtMillis)));
        session.execute(insertEventType.bind(record.eventType, record.eventId, record.patientId, eventTime));
      } else {
        session.execute(insertQuarantine.bind(
            record.firstReason(),
            instantOrNow(record.processedAtMillis),
            record.eventId,
            record.errors,
            record.rawJson));
      }
    }

    @Override
    public void close() {
      if (session != null) {
        session.close();
      }
    }
  }

  public static class CassandraAlertSink extends RichSinkFunction<AlertRecord> {
    private final String host;
    private final int port;
    private transient CqlSession session;
    private transient PreparedStatement insertPatientAlert;
    private transient PreparedStatement insertTypeAlert;

    public CassandraAlertSink(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public void open(Configuration parameters) {
      session = connect(host, port);
      insertPatientAlert = session.prepare("insert into alerts_by_patient "
          + "(patient_id, alert_time, alert_type, details) values (?, ?, ?, ?)");
      insertTypeAlert = session.prepare("insert into alerts_by_type "
          + "(alert_type, alert_time, patient_id, details) values (?, ?, ?, ?)");
    }

    @Override
    public void invoke(AlertRecord alert, Context context) {
      Instant alertTime = instantOrNow(alert.alertTimeMillis);
      session.execute(insertPatientAlert.bind(alert.patientId, alertTime, alert.alertType, alert.detailsJson));
      session.execute(insertTypeAlert.bind(alert.alertType, alertTime, alert.patientId, alert.detailsJson));
    }

    @Override
    public void close() {
      if (session != null) {
        session.close();
      }
    }
  }

  private static CqlSession connect(String host, int port) {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= 30; attempt++) {
      try {
        CqlSession session = CqlSession.builder()
            .addContactPoint(new InetSocketAddress(host, port))
            .withLocalDatacenter("datacenter1")
            .build();
        session.execute(SimpleStatement.newInstance("use ehr"));
        return session;
      } catch (RuntimeException e) {
        last = e;
        try {
          Thread.sleep(2_000L);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
    throw last == null ? new IllegalStateException("Could not connect to Cassandra") : last;
  }

  private static List<String> validate(RawEvent event) {
    List<String> errors = new ArrayList<>();
    if (event.rawJson == null || event.rawJson.isBlank()) {
      errors.add("empty_record");
      return errors;
    }
    if (event.eventId == null || event.eventId.isBlank()) {
      errors.add("missing_event_id");
    } else {
      try {
        UUID.fromString(event.eventId);
      } catch (IllegalArgumentException ignored) {
        errors.add("bad_event_id");
      }
    }
    if (event.patientId == null || event.patientId.isBlank()) {
      errors.add("missing_patient_id");
    }
    if (event.eventEpochMillis <= 0) {
      errors.add("bad_event_time");
    }
    if (!"ehr.v1".equals(event.schemaVersion)) {
      errors.add("unsupported_schema_version");
    }
    if (event.eventType == null || !List.of("vital", "lab", "medication", "admission").contains(event.eventType)) {
      errors.add("unknown_event_type");
    }
    if (event.rawJson.contains("\"patient_name\"")
        || event.rawJson.contains("\"ssn\"")
        || event.rawJson.contains("\"address\"")) {
      errors.add("contains_direct_phi");
    }
    errors.addAll(validatePayload(event));
    return errors;
  }

  private static List<String> validatePayload(RawEvent event) {
    List<String> errors = new ArrayList<>();
    try {
      JsonNode payload = MAPPER.readTree(event.payloadJson == null ? "{}" : event.payloadJson);
      if ("vital".equals(event.eventType)) {
        String name = payload.path("name").asText("");
        double value = payload.path("value").asDouble(Double.NaN);
        if (!List.of("heart_rate", "systolic_bp", "diastolic_bp", "temperature_c", "spo2").contains(name)) {
          errors.add("bad_vital_name");
        }
        if (Double.isNaN(value)) {
          errors.add("bad_vital_value");
        } else if ("heart_rate".equals(name) && (value < 20 || value > 250)) {
          errors.add("vital_out_of_range");
        } else if ("temperature_c".equals(name) && (value < 25 || value > 45)) {
          errors.add("vital_out_of_range");
        } else if ("spo2".equals(name) && (value < 50 || value > 100)) {
          errors.add("vital_out_of_range");
        }
      } else if ("lab".equals(event.eventType)) {
        String test = payload.path("test").asText("");
        double result = payload.path("result").asDouble(Double.NaN);
        if (!List.of("wbc", "lactate", "creatinine").contains(test)) {
          errors.add("bad_lab_test");
        }
        if (Double.isNaN(result) || result < 0 || result > 80) {
          errors.add("lab_out_of_range");
        }
      } else if ("medication".equals(event.eventType)) {
        double dose = payload.path("dose").asDouble(Double.NaN);
        if (payload.path("medication").asText("").isBlank()) {
          errors.add("missing_medication");
        }
        if (Double.isNaN(dose) || dose <= 0) {
          errors.add("bad_medication_dose");
        }
      } else if ("admission".equals(event.eventType)) {
        int acuity = payload.path("acuity").asInt(-1);
        if (payload.path("department").asText("").isBlank()) {
          errors.add("missing_department");
        }
        if (acuity < 1 || acuity > 5) {
          errors.add("bad_acuity");
        }
      }
    } catch (Exception e) {
      errors.add("bad_payload_json");
    }
    return errors;
  }

  private static String textOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }

  private static long parseMillis(String value) {
    if (value == null || value.isBlank()) {
      return -1L;
    }
    try {
      return Instant.parse(value).toEpochMilli();
    } catch (Exception e) {
      return -1L;
    }
  }

  private static Instant instantOrNow(long epochMillis) {
    return epochMillis > 0 ? Instant.ofEpochMilli(epochMillis) : Instant.now();
  }

  private static double valueOrZero(Double value) {
    return value == null || value.isNaN() ? 0.0 : value;
  }

  private static String sha256(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder builder = new StringBuilder();
      for (byte b : bytes) {
        builder.append(String.format("%02x", b));
      }
      return builder.toString();
    } catch (Exception e) {
      return Integer.toHexString(value.hashCode());
    }
  }
}
