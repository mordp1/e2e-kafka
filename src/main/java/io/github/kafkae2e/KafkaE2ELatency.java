package io.github.kafkae2e;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaE2ELatency {

  public static void main(String[] args) throws Exception {
    String propsPath       = getArg(args, "--kafkaProps",       "kafka-client.properties");
    String topic           = getArg(args, "--topic",           "invoice-topic");
    String groupIdBase     = getArg(args, "--groupId",         "invoice-e2e-group");
    int sampleEverySeconds = Integer.parseInt(getArg(args, "--sampleEverySeconds", "1"));
    int maxWaitSeconds     = Integer.parseInt(getArg(args, "--maxWaitSeconds",     "120"));
    String outDir          = getArg(args, "--outputDir",       "out");
    // messageType  — schema of the generated message (invoice | payment | order)
    // mode         — batch: send --numMessages total; continuous: rate × duration
    // keyStrategy  — messageId (invoiceId): main ID field; entityId (customerId): grouping field; none: null
    String messageType  = getArg(args, "--messageType",  "invoice");  // invoice|payment|order
    String mode         = getArg(args, "--mode",         "batch");     // batch|continuous
    int ratePerSecond   = Integer.parseInt(getArg(args, "--ratePerSecond",   "500"));
    int durationSeconds = Integer.parseInt(getArg(args, "--durationSeconds", "60"));
    String keyStrategy  = getArg(args, "--keyStrategy",  "messageId"); // messageId|entityId|none

    if (!List.of("invoice","payment","order").contains(messageType))
      throw new IllegalArgumentException("--messageType must be invoice|payment|order, got: " + messageType);
    if (!List.of("batch","continuous").contains(mode))
      throw new IllegalArgumentException("--mode must be batch|continuous, got: " + mode);
    if (!List.of("messageId","invoiceId","entityId","customerId","none").contains(keyStrategy))
      throw new IllegalArgumentException("--keyStrategy must be messageId|entityId|none, got: " + keyStrategy);

    // In continuous mode numMessages = rate × duration; otherwise use --numMessages
    int numMessages = "continuous".equals(mode)
        ? ratePerSecond * durationSeconds
        : Integer.parseInt(getArg(args, "--numMessages", "10000"));

    Files.createDirectories(Path.of(outDir));
    String runId = UUID.randomUUID().toString().substring(0, 8);
    // Append runId so each execution uses a fresh consumer group — prevents consuming
    // stale messages from previous runs when the topic is not re-created between tests.
    String groupId = groupIdBase + "-" + runId;
    Path jsonPath = Path.of(outDir, "report-" + runId + ".json");
    Path htmlPath = Path.of(outDir, "report-" + runId + ".html");

    String modeDesc = "continuous".equals(mode)
        ? "continuous (" + ratePerSecond + " msg/s \u00d7 " + durationSeconds + "s)"
        : "batch";
    System.out.printf(
        "Run ID: %s | topic: %s | groupId: %s | messages: %d | type: %s | mode: %s | keyStrategy: %s%n",
        runId, topic, groupId, numMessages, messageType, modeDesc, keyStrategy);

    Properties base = new Properties();
    try (FileInputStream fis = new FileInputStream(propsPath)) {
      base.load(fis);
    }

    AtomicLong produced = new AtomicLong(0);
    AtomicLong consumed = new AtomicLong(0);
    AtomicLong failed = new AtomicLong(0);
    AtomicLong sumLatency = new AtomicLong(0);
    // Per-sample-window accumulators — reset by the sampler each tick so the
    // chart shows the avg latency of messages consumed *in that window*, not
    // the running all-time mean which only trends toward its final value.
    AtomicLong windowSumLatency = new AtomicLong(0);
    AtomicLong windowCount      = new AtomicLong(0);
    AtomicBoolean done = new AtomicBoolean(false);
    AtomicInteger writeIdx = new AtomicInteger(0);
    long[] latenciesMs = new long[numMessages];
    List<Sample> samples = new ArrayList<>();
    List<MetricSnapshot> metricSnapshots = new ArrayList<>();
    // Live references to Kafka client metric maps; updated once each client is created.
    AtomicReference<Map<MetricName, ? extends Metric>> producerMetricsRef =
        new AtomicReference<>(Collections.emptyMap());
    AtomicReference<Map<MetricName, ? extends Metric>> consumerMetricsRef =
        new AtomicReference<>(Collections.emptyMap());
    long startMs = System.currentTimeMillis();

    // Latch that the consumer signals once it holds partition assignment.
    // The producer must not send until then; otherwise messages arrive before
    // the consumer's "latest" seek position and are silently skipped.
    CountDownLatch consumerReady = new CountDownLatch(1);

    Thread consumerThread = new Thread(() -> {
      try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(base, groupId))) {
        consumer.subscribe(List.of(topic));
        // Poll in a tight loop until Kafka completes the rebalance and assigns
        // partitions. auto.offset.reset=latest will seek to the current end of
        // each partition, so any message produced after this point is received.
        while (consumer.assignment().isEmpty()) {
          consumer.poll(Duration.ofMillis(100));
        }
        consumerMetricsRef.set(consumer.metrics());
        consumerReady.countDown();
        while (!done.get()) {
          // 50ms poll timeout: reduces the "sitting in broker" gap from up to
          // 500ms per cycle to up to 50ms, directly lowering E2E latency floor.
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(50));
          for (ConsumerRecord<String, String> rec : records) {
            Header header = rec.headers().lastHeader("sendTsMs");
            if (header == null || header.value() == null || header.value().length != Long.BYTES) {
              continue;
            }
            long sentTs = ByteBuffer.wrap(header.value()).getLong();
            long latency = Math.max(0, System.currentTimeMillis() - sentTs);
            int idx = writeIdx.getAndIncrement();
            if (idx < numMessages) {
              latenciesMs[idx] = latency;
              sumLatency.addAndGet(latency);            windowSumLatency.addAndGet(latency);
            windowCount.incrementAndGet();              long c = consumed.incrementAndGet();
              if (c % 1000 == 0) {
                System.out.printf("  consumed %d / %d%n", c, numMessages);
              }
              if (c >= numMessages) {
                done.set(true);
                break;
              }
            }
          }
        }
      } catch (Exception e) {
        System.err.println("Consumer error: " + e.getMessage());
        // Ensure the main thread is never left waiting on the latch if the
        // consumer crashes before it can signal readiness.
        consumerReady.countDown();
        done.set(true);
      }
    });
    consumerThread.setName("e2e-consumer");
    consumerThread.start();

    Thread samplerThread = new Thread(() -> {
      try {
        while (!done.get()) {
          Thread.sleep(sampleEverySeconds * 1000L);
          long now = System.currentTimeMillis();
          long c = consumed.get();
          long avg = c > 0 ? sumLatency.get() / c : 0;
          // Window avg: reset accumulators atomically so each sample reflects
          // only the messages consumed since the previous sample tick.
          long wSum = windowSumLatency.getAndSet(0);
          long wCnt = windowCount.getAndSet(0);
          long windowAvg = wCnt > 0 ? wSum / wCnt : 0;
          long lag = produced.get() - c;
          samples.add(new Sample(now - startMs, produced.get(), c, avg, windowAvg, lag));
          metricSnapshots.add(captureMetrics(now - startMs,
              producerMetricsRef.get(), consumerMetricsRef.get()));
        }
      } catch (InterruptedException ignored) {
      }
    });
    samplerThread.setName("e2e-sampler");
    samplerThread.start();

    // Block until the consumer holds its partition assignment (and has seeked to
    // "latest").  Any message produced before this point would be at an offset
    // lower than the consumer's start position and would be silently skipped.
    System.out.println("Waiting for consumer partition assignment...");
    if (!consumerReady.await(30, TimeUnit.SECONDS)) {
      done.set(true);
      throw new RuntimeException("Consumer failed to get partition assignment within 30s. Is Kafka reachable at " +
          base.getProperty("bootstrap.servers") + "?");
    }
    System.out.println("Consumer ready. Starting producer...");

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(base))) {
      producerMetricsRef.set(producer.metrics());
      // Rate-limit: inter-message interval in nanoseconds; 0 = unlimited (batch mode)
      long intervalNs = "continuous".equals(mode) ? 1_000_000_000L / Math.max(1, ratePerSecond) : 0L;
      long nextSendNs = System.nanoTime();
      for (int i = 0; i < numMessages; i++) {
        if (intervalNs > 0) {
          long rem = nextSendNs - System.nanoTime();
          if (rem > 0) LockSupport.parkNanos(rem);
          nextSendNs += intervalNs;
        }
        long now = System.currentTimeMillis();
        KafkaMessage msg = buildMessage(i, runId, messageType);
        String messageKey = switch (keyStrategy) {
          case "customerId", "entityId" -> msg.entityId;
          case "none"                   -> null;
          default                       -> msg.messageId;  // "messageId"/"invoiceId"
        };
        RecordHeaders headers = new RecordHeaders();
        headers.add("sendTsMs",    ByteBuffer.allocate(Long.BYTES).putLong(now).array());
        headers.add("messageId",   msg.messageId.getBytes(StandardCharsets.UTF_8));
        headers.add("entityId",    msg.entityId.getBytes(StandardCharsets.UTF_8));
        headers.add("messageType", messageType.getBytes(StandardCharsets.UTF_8));
        headers.add("keyStrategy", keyStrategy.getBytes(StandardCharsets.UTF_8));
        ProducerRecord<String, String> rec = new ProducerRecord<>(topic, null, messageKey, msg.json, headers);
        // Async send — the callback fires on the I/O thread when the broker ACKs.
        // This is ~10-100× faster than blocking per-message while still measuring
        // the correct E2E latency (sendTsMs is stamped before the send call).
        producer.send(rec, (metadata, ex) -> {
          if (ex != null) {
            failed.incrementAndGet();
            System.err.printf("Send failed: %s%n", ex.getMessage());
          } else {
            produced.incrementAndGet();
          }
        });
        if (i % 5000 == 0 && i > 0) {
          System.out.printf("  sent %d / %d%n", i, numMessages);
        }
      }
      // Wait for all in-flight sends to complete before we close the producer.
      producer.flush();
    }
    System.out.printf("Producer done. Produced ok: %d, failed: %d. Waiting for consumer...%n",
        produced.get(), failed.get());

    consumerThread.join(maxWaitSeconds * 1000L);
    if (consumerThread.isAlive()) {
      done.set(true);
      System.err.printf("Timeout waiting for consumer. consumed=%d, produced=%d%n",
          consumed.get(), produced.get());
      throw new RuntimeException("Consumer timeout after " + maxWaitSeconds + "s");
    }
    done.set(true);
    samplerThread.join();

    long c = consumed.get();
    long avg = c > 0 ? sumLatency.get() / c : 0;
    long p95 = percentile(latenciesMs, 95);
    long p99 = percentile(latenciesMs, 99);
    long wSum = windowSumLatency.getAndSet(0);
    long wCnt = windowCount.getAndSet(0);
    long finalWindowAvg = wCnt > 0 ? wSum / wCnt : 0;
    samples.add(new Sample(System.currentTimeMillis() - startMs, produced.get(), c, avg, finalWindowAvg, 0L));
    metricSnapshots.add(captureMetrics(System.currentTimeMillis() - startMs,
        producerMetricsRef.get(), consumerMetricsRef.get()));

    String reportJson = buildJson(runId, topic, numMessages, avg, p95, p99, samples);
    Files.writeString(jsonPath, reportJson, StandardCharsets.UTF_8);
    Files.writeString(htmlPath,
        buildHtml(runId, topic, numMessages, avg, p95, p99, samples, metricSnapshots,
            messageType, modeDesc),
        StandardCharsets.UTF_8);

    System.out.println("Run ID: " + runId);
    System.out.println("JSON report: " + jsonPath);
    System.out.println("HTML report: " + htmlPath);
    System.out.println("Produced OK: " + produced.get() + ", failed sends: " + failed.get() + ", consumed: " + consumed.get());
    System.out.println("Latency avg/p95/p99 ms: " + avg + "/" + p95 + "/" + p99);
  }

  private static Properties producerProps(Properties base) {
    Properties p = new Properties();
    p.putAll(base);
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    p.put(ProducerConfig.ACKS_CONFIG, "all");
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
    p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
    // Batch messages arriving within 10ms together for higher throughput.  
    p.put(ProducerConfig.LINGER_MS_CONFIG, "10");
    return p;
  }

  private static Properties consumerProps(Properties base, String groupId) {
    Properties p = new Properties();
    p.putAll(base);
    p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    // "latest" combined with the consumer-readiness latch ensures we only read
    // messages produced during this run, not stale messages from prior runs.
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    return p;
  }

  private static String getArg(String[] args, String key, String defaultValue) {
    for (int i = 0; i < args.length; i++) {
      if (key.equals(args[i]) && i + 1 < args.length) {
        return args[i + 1];
      }
    }
    return defaultValue;
  }

  private static long percentile(long[] data, int p) {
    long[] copy = Arrays.stream(data).filter(x -> x > 0).toArray();
    if (copy.length == 0) {
      return 0;
    }
    Arrays.sort(copy);
    int idx = (int) Math.ceil((p / 100.0) * copy.length) - 1;
    idx = Math.max(0, Math.min(idx, copy.length - 1));
    return copy[idx];
  }

  private static String buildJson(String runId, String topic, int numMessages, long avg, long p95, long p99, List<Sample> samples) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"runId\":\"").append(runId).append("\",");
    sb.append("\"topic\":\"").append(topic).append("\",");
    sb.append("\"numMessages\":").append(numMessages).append(",");
    sb.append("\"avgLatencyMs\":").append(avg).append(",");
    sb.append("\"p95LatencyMs\":").append(p95).append(",");
    sb.append("\"p99LatencyMs\":").append(p99).append(",");
    sb.append("\"samples\":[");
    for (int i = 0; i < samples.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(samples.get(i).toJson());
    }
    sb.append("]}");
    return sb.toString();
  }

  // ── Metric helpers ──────────────────────────────────────────────────────

  private static String fmt(double v) {
    return String.format("%.4f", v);
  }

  private static double getMetric(Map<MetricName, ? extends Metric> metrics,
      String group, String name) {
    for (Map.Entry<MetricName, ? extends Metric> e : metrics.entrySet()) {
      if (group.equals(e.getKey().group()) && name.equals(e.getKey().name())) {
        Object v = e.getValue().metricValue();
        if (v instanceof Double d) return (Double.isNaN(d) || Double.isInfinite(d)) ? 0.0 : d;
        if (v instanceof Number n) return n.doubleValue();
      }
    }
    return 0.0;
  }

  private static MetricSnapshot captureMetrics(long tMs,
      Map<MetricName, ? extends Metric> pm,
      Map<MetricName, ? extends Metric> cm) {
    return new MetricSnapshot(tMs,
        // producer-metrics
        getMetric(pm, "producer-metrics", "produce-throttle-time-avg"),
        getMetric(pm, "producer-metrics", "record-retry-rate"),
        getMetric(pm, "producer-metrics", "record-error-rate"),
        getMetric(pm, "producer-metrics", "request-latency-avg"),
        getMetric(pm, "producer-metrics", "request-rate"),
        getMetric(pm, "producer-metrics", "io-ratio"),
        getMetric(pm, "producer-metrics", "io-wait-ratio"),
        // consumer-fetch-manager-metrics
        getMetric(cm, "consumer-fetch-manager-metrics", "bytes-consumed-rate"),
        getMetric(cm, "consumer-fetch-manager-metrics", "fetch-latency-avg"),
        getMetric(cm, "consumer-fetch-manager-metrics", "fetch-rate"),
        getMetric(cm, "consumer-fetch-manager-metrics", "fetch-throttle-time-avg"),
        getMetric(cm, "consumer-fetch-manager-metrics", "records-consumed-rate"),
        // consumer-coordinator-metrics
        getMetric(cm, "consumer-coordinator-metrics", "commit-latency-avg"),
        getMetric(cm, "consumer-coordinator-metrics", "failed-rebalance-rate-per-hour"),
        // consumer-metrics (network)
        getMetric(cm, "consumer-metrics", "io-ratio"),
        getMetric(cm, "consumer-metrics", "io-wait-ratio"),
        // producer — additional common metrics
        getMetric(pm, "producer-metrics", "request-latency-max"),
        getMetric(pm, "producer-metrics", "record-send-rate"),
        getMetric(pm, "producer-metrics", "buffer-available-bytes"),
        getMetric(pm, "producer-metrics", "connection-count"),
        getMetric(pm, "producer-metrics", "io-wait-time-ns-avg"),
        // consumer — additional common metrics
        getMetric(cm, "consumer-fetch-manager-metrics", "records-lag-max"),
        getMetric(cm, "consumer-metrics", "poll-idle-ratio-avg"),
        getMetric(cm, "consumer-metrics", "connection-count"),
        getMetric(cm, "consumer-metrics", "io-wait-time-ns-avg"));
  }

  private static String buildHtml(String runId, String topic, int numMessages,
      long avg, long p95, long p99,
      List<Sample> samples, List<MetricSnapshot> metricSnapshots,
      String messageType, String modeDesc) {
    // ─ E2E throughput / latency series ───────────────────────────────────
    StringBuilder ex = new StringBuilder();
    StringBuilder el = new StringBuilder();
    StringBuilder ew = new StringBuilder(); // window avg latency
    StringBuilder ep = new StringBuilder();
    StringBuilder ec = new StringBuilder();
    StringBuilder eg = new StringBuilder(); // consumer lag
    for (int i = 0; i < samples.size(); i++) {
      if (i > 0) { ex.append(","); el.append(","); ew.append(","); ep.append(","); ec.append(","); eg.append(","); }
      Sample s = samples.get(i);
      ex.append(s.tMs); el.append(s.avgLatencyMs); ew.append(s.windowAvgLatencyMs);
      ep.append(s.produced); ec.append(s.consumed); eg.append(s.lag);
    }

    // ─ Kafka client metric time-series ───────────────────────────────────
    StringBuilder mt = new StringBuilder();
    StringBuilder[] ma = new StringBuilder[25];
    for (int i = 0; i < ma.length; i++) ma[i] = new StringBuilder();
    for (int i = 0; i < metricSnapshots.size(); i++) {
      if (i > 0) { mt.append(","); for (StringBuilder sb : ma) sb.append(","); }
      MetricSnapshot s = metricSnapshots.get(i);
      mt.append(s.tMs);
      ma[0].append(fmt(s.requestLatencyAvg));
      ma[1].append(fmt(s.requestRate));
      ma[2].append(fmt(s.recordRetryRate));
      ma[3].append(fmt(s.recordErrorRate));
      ma[4].append(fmt(s.produceThrottleTimeAvg));
      ma[5].append(fmt(s.producerIoRatio));
      ma[6].append(fmt(s.producerIoWaitRatio));
      ma[7].append(fmt(s.fetchLatencyAvg));
      ma[8].append(fmt(s.commitLatencyAvg));
      ma[9].append(fmt(s.recordsConsumedRate));
      ma[10].append(fmt(s.bytesConsumedRate));
      ma[11].append(fmt(s.fetchRate));
      ma[12].append(fmt(s.fetchThrottleTimeAvg));
      ma[13].append(fmt(s.failedRebalanceRatePerHour));
      ma[14].append(fmt(s.consumerIoRatio));
      ma[15].append(fmt(s.consumerIoWaitRatio));
      ma[16].append(fmt(s.requestLatencyMax));
      ma[17].append(fmt(s.recordSendRate));
      ma[18].append(fmt(s.bufferAvailableBytes));
      ma[19].append(fmt(s.producerConnectionCount));
      ma[20].append(fmt(s.producerIoWaitTimeNsAvg));
      ma[21].append(fmt(s.recordsLagMax));
      ma[22].append(fmt(s.pollIdleRatioAvg));
      ma[23].append(fmt(s.consumerConnectionCount));
      ma[24].append(fmt(s.consumerIoWaitTimeNsAvg));
    }

    String jsData = String.format(
        "const mt=[%s];\n" +
        "const mReqLat=[%s],mReqLatMax=[%s],mSendR=[%s],mReqR=[%s],mRetry=[%s],mErrR=[%s];\n" +
        "const mThrotP=[%s],mBufAvail=[%s],mPConnCnt=[%s],mPIoR=[%s],mPIoW=[%s],mPIoWaitNs=[%s];\n" +
        "const mFLat=[%s],mCLat=[%s],mRecR=[%s],mBytR=[%s],mFR=[%s],mFThr=[%s];\n" +
        "const mFailReb=[%s],mCIoR=[%s],mCIoW=[%s];\n" +
        "const mLagMax=[%s],mPollIdle=[%s],mCConnCnt=[%s],mCIoWaitNs=[%s];",
        mt,
        ma[0], ma[16], ma[17], ma[1], ma[2], ma[3],
        ma[4], ma[18], ma[19], ma[5], ma[6], ma[20],
        ma[7], ma[8], ma[9], ma[10], ma[11], ma[12],
        ma[13], ma[14], ma[15],
        ma[21], ma[22], ma[23], ma[24]);

    return """
        <!doctype html>
        <html>
        <head>
          <meta charset="utf-8"/>
          <title>Kafka E2E Report %s</title>
          <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
          <style>
            body{font-family:Arial,sans-serif;margin:20px;background:#f8f9fa}
            h2{color:#1a1a2e}h3{color:#16213e;margin-top:28px;border-bottom:2px solid #dee2e6;padding-bottom:6px}
            .summary{background:#fff;border:1px solid #dee2e6;border-radius:6px;padding:12px 20px;margin-bottom:20px;display:inline-block}
            .grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:6px}
            .card{background:#fff;border:1px solid #dee2e6;border-radius:4px;overflow:hidden}
            .chart{height:320px}
            .desc{font-size:12px;color:#555;line-height:1.5;padding:6px 12px 10px;margin:0;border-top:1px solid #f0f0f0;background:#fafafa}
            .desc b{color:#333}
            .good{color:#2ca02c;font-weight:600}
            .warn{color:#d62728;font-weight:600}
          </style>
        </head>
        <body>
          <h2>Kafka Invoice E2E Latency Report</h2>
          <div class="summary">
            <b>Run ID:</b> %s &nbsp;|&nbsp;
            <b>Topic:</b> %s &nbsp;|&nbsp;
            <b>Type:</b> %s &nbsp;|&nbsp;
            <b>Mode:</b> %s &nbsp;|&nbsp;
            <b>Messages:</b> %d &nbsp;|&nbsp;
            <b>Avg / P95 / P99:</b> %d / %d / %d ms
          </div>

          <h3>E2E Throughput &amp; Latency</h3>
          <div class="grid">
            <div class="card">
              <div id="e2eLatency" class="chart"></div>
              <p class="desc"><b>What:</b> End-to-end latency from the moment the producer stamped the message (<code>sendTsMs</code> header) to the moment the consumer processed it.<br>
              <b>Red line</b> = window avg — latency of messages consumed <i>in that sample interval</i> (real-time view).<br>
              <b>Blue dotted</b> = cumulative avg — running mean since t=0 (trends toward final stable value).<br>
              <span class="good">Healthy:</span> red line flat after initial ramp-up. <span class="warn">Watch for:</span> steadily climbing red line (consumer falling behind), or red line much higher than Fetch Latency Avg (consumer poll gap).</p>
            </div>
            <div class="card">
              <div id="e2eCounts" class="chart"></div>
              <p class="desc"><b>What:</b> Cumulative count of messages sent by the producer and messages received by the consumer over time.<br>
              <span class="good">Healthy:</span> both lines track closely — the gap (consumer lag) stays near zero. <span class="warn">Watch for:</span> a growing gap between produced and consumed, which means the consumer cannot keep up and backlog is building in the broker.</p>
            </div>
            <div class="card">
              <div id="e2eLag" class="chart"></div>
              <p class="desc"><b>What:</b> Instantaneous consumer lag — the number of messages produced but not yet consumed at each sample tick (<code>produced − consumed</code>).<br>
              <span class="good">Healthy:</span> stays at or near zero, or spikes briefly and recovers. <span class="warn">Watch for:</span> sustained non-zero lag indicating the consumer throughput is lower than the producer rate; sudden spikes often indicate a rebalance or GC pause.</p>
            </div>
          </div>

          <h3>Producer Metrics</h3>
          <div class="grid">
            <div class="card">
              <div id="pReqLat" class="chart"></div>
              <p class="desc"><b>What:</b> Round-trip time for a produce request from send to broker ACK — shown as both <b>avg</b> (steady-state) and <b>max</b> (worst single-batch spike hidden by the average). Network + broker only; does <b>not</b> include consumer poll wait time.<br>
              <span class="good">Healthy:</span> avg and max tracking closely and low (1–10 ms local, 5–30 ms cross-region). <span class="warn">Watch for:</span> max diverging far above avg (bursty broker stalls); both rising together (sustained overload or network congestion).</p>
            </div>
            <div class="card">
              <div id="pReqRate" class="chart"></div>
              <p class="desc"><b>What:</b> Number of produce requests (batches) the producer sends to the broker per second. A higher value relative to message rate means smaller batches; a lower value means larger, more efficient batches.<br>
              <span class="good">Healthy:</span> steady value proportional to send throughput. <span class="warn">Watch for:</span> sudden drops — may indicate the producer is being throttled or is blocked waiting for broker ACKs.</p>
            </div>
            <div class="card">
              <div id="pRetryErr" class="chart"></div>
              <p class="desc"><b>What:</b> <b>Retry rate</b> — messages re-sent after a transient failure (retriable errors like <code>LEADER_NOT_AVAILABLE</code>). <b>Error rate</b> — messages permanently failed and dropped (non-retriable or retries exhausted).<br>
              <span class="good">Healthy:</span> both lines at zero throughout the run. <span class="warn">Watch for:</span> any non-zero error rate (data loss risk); spiky retry rate during broker leader elections or network blips.</p>
            </div>
            <div class="card">
              <div id="pThrottle" class="chart"></div>
              <p class="desc"><b>What:</b> Average time (ms) the broker forced the producer to pause before accepting more data — quota enforcement. This is added delay injected by the broker, directly increasing producer-side latency.<br>
              <span class="good">Healthy:</span> zero at all times. <span class="warn">Watch for:</span> any non-zero value meaning the cluster or topic has a byte-rate quota the producer is exceeding; reduce send rate or raise the quota.</p>
            </div>
            <div class="card">
              <div id="pSendRate" class="chart"></div>
              <p class="desc"><b>What:</b> Individual records sent per second by the producer — the message-level write throughput. Unlike request-rate (batches), this counts individual messages and directly matches the consumer's records-consumed-rate for balance checks.<br>
              <span class="good">Healthy:</span> stable and close to the intended send rate (e.g. matches <code>--ratePerSecond</code> in continuous mode). <span class="warn">Watch for:</span> rate consistently below target (producer blocked by buffer pressure or slow ACKs); sudden drops paired with rising retry rate.</p>
            </div>
            <div class="card">
              <div id="pBufAvail" class="chart"></div>
              <p class="desc"><b>What:</b> Bytes of producer send-buffer memory currently free for new records (default buffer is 32 MB). When this reaches zero, <code>producer.send()</code> blocks until space is freed or <code>max.block.ms</code> is exceeded, stalling the sending thread.<br>
              <span class="good">Healthy:</span> stays near the maximum configured value — buffer is rarely depleted. <span class="warn">Watch for:</span> sustained decline toward zero, which means records accumulate faster than the broker ACKs them; increase <code>buffer.memory</code>, reduce send rate, or investigate broker ACK latency.</p>
            </div>
          </div>

          <h3>Consumer Metrics</h3>
          <div class="grid">
            <div class="card">
              <div id="cFetchLat" class="chart"></div>
              <p class="desc"><b>What:</b> Average round-trip time for a fetch request — from when the consumer sends a <code>Fetch</code> RPC to when the broker responds with records. This measures the <i>network + broker read</i> time only; it does <b>not</b> include the time a message waited between consumer poll cycles.<br>
              <span class="good">Healthy:</span> low and stable (typically 1–20 ms). <span class="warn">Watch for:</span> values much lower than the E2E window avg — the gap between them is the consumer poll gap (time messages sit unread in the broker).</p>
            </div>
            <div class="card">
              <div id="cCommitLat" class="chart"></div>
              <p class="desc"><b>What:</b> Average time to commit consumed offsets back to Kafka's <code>__consumer_offsets</code> topic. High commit latency can cause the consumer group coordinator to assume the consumer is stuck, triggering an unwanted rebalance.<br>
              <span class="good">Healthy:</span> low and stable (&lt; 20 ms typical). <span class="warn">Watch for:</span> spikes or sustained high values — may indicate broker coordinator overload or network issues.</p>
            </div>
            <div class="card">
              <div id="cRecRate" class="chart"></div>
              <p class="desc"><b>What:</b> Number of records the consumer processes per second across all partitions. This is the consumer's effective throughput and should closely match the producer's send rate in steady state.<br>
              <span class="good">Healthy:</span> stable, close to the producer rate. <span class="warn">Watch for:</span> a rate consistently lower than the producer — the gap manifests as the consumer lag seen above; identify the bottleneck (slow processing, throttle, rebalance).</p>
            </div>
            <div class="card">
              <div id="cBytesRate" class="chart"></div>
              <p class="desc"><b>What:</b> Bytes consumed per second across all partitions. Combine with records-consumed-rate to derive average message size (bytes/rate ÷ records/rate). Useful for network utilisation and capacity planning.<br>
              <span class="good">Healthy:</span> proportional to and stable with the records rate. <span class="warn">Watch for:</span> sudden drops without a corresponding drop in records-consumed-rate (could indicate very small messages or a metric anomaly).</p>
            </div>
            <div class="card">
              <div id="cFetchRate" class="chart"></div>
              <p class="desc"><b>What:</b> Number of fetch RPCs the consumer sends to the broker per second — effectively the consumer poll loop frequency. Multiply fetch-rate by fetch-latency-avg to estimate how much time per second the consumer spends waiting on the broker.<br>
              <span class="good">Healthy:</span> proportional to throughput. <span class="warn">Watch for:</span> high fetch-rate combined with low records-consumed-rate, which means many empty or near-empty fetches (consumer is outpacing the producer, or topic is sparse).</p>
            </div>
            <div class="card">
              <div id="cFetchThrot" class="chart"></div>
              <p class="desc"><b>What:</b> Average time (ms) the broker forced the consumer to wait before returning data — quota enforcement on the consumer side.<br>
              <span class="good">Healthy:</span> zero at all times. <span class="warn">Watch for:</span> non-zero values indicating the consumer is hitting a byte-rate or fetch-rate quota; reduce <code>max.partition.fetch.bytes</code> or raise the quota.</p>
            </div>
            <div class="card">
              <div id="cFailReb" class="chart"></div>
              <p class="desc"><b>What:</b> Rate of <i>failed</i> consumer group rebalances per hour. A rebalance temporarily pauses all consumers in the group while partitions are reassigned. Failed rebalances can cause repeated pauses and duplicate message processing.<br>
              <span class="good">Healthy:</span> zero throughout. <span class="warn">Watch for:</span> any non-zero value — common causes are missed heartbeats (long <code>max.poll.interval.ms</code> exceeded), network timeouts, or coordinator overload.</p>
            </div>
            <div class="card">
              <div id="cLagMax" class="chart"></div>
              <p class="desc"><b>What:</b> Maximum consumer lag across all assigned partitions — the largest number of records any single partition is behind the log-end-offset, reported directly by the Kafka client. This is the canonical real-time health metric for consumers; it reflects how far behind the slowest partition is.<br>
              <span class="good">Healthy:</span> stays at or very near zero throughout the run. <span class="warn">Watch for:</span> any sustained growth (consumer cannot keep pace with writes); or even a lag spike that doesn't recover (may indicate a stalled partition, a GC pause, or consumer processing bottleneck).</p>
            </div>
            <div class="card">
              <div id="cPollIdle" class="chart"></div>
              <p class="desc"><b>What:</b> Fraction of time (0–1) the consumer's poll loop spends idle waiting for new records. High value = consumer is faster than the producer (waiting for data). Low value = consumer is always busy processing batches.<br>
              <span class="good">Healthy:</span> moderate (e.g. 0.3–0.7) indicating balanced throughput. <span class="warn">Watch for:</span> near-zero (consumer saturated, lag may build); near-one (topic under-producing or too many consumer instances for the partition count).</p>
            </div>
          </div>

          <h3>I/O &amp; Connection Metrics</h3>
          <div class="grid">
            <div class="card">
              <div id="pIo" class="chart"></div>
              <p class="desc"><b>What:</b> How the producer's single network I/O thread spends its time. <b>io-ratio</b> = fraction of time doing active I/O (sending/receiving bytes). <b>io-wait-ratio</b> = fraction of time blocked waiting for the network to become ready. Both are 0–1 and should not sum to 1 (idle time is the remainder).<br>
              <span class="good">Healthy:</span> both low — the thread is rarely busy. <span class="warn">Watch for:</span> high io-wait-ratio (network is the bottleneck); high io-ratio with low throughput (CPU pressure on the I/O thread).</p>
            </div>
            <div class="card">
              <div id="cIo" class="chart"></div>
              <p class="desc"><b>What:</b> Same as the producer I/O ratios but for the consumer's network thread. <b>io-ratio</b> = active I/O time fraction. <b>io-wait-ratio</b> = waiting-for-network time fraction.<br>
              <span class="good">Healthy:</span> both low. <span class="warn">Watch for:</span> high io-wait-ratio on the consumer — if it coincides with low records-consumed-rate, the network path to the broker is the bottleneck; consider broker colocation or increased fetch parallelism.</p>
            </div>
            <div class="card">
              <div id="connCount" class="chart"></div>
              <p class="desc"><b>What:</b> Number of active TCP connections each client holds to the Kafka brokers. Both producer and consumer maintain one or more connections per broker. Unusually high counts may indicate connection leaks; sudden drops may indicate broker restarts or network partitions.<br>
              <span class="good">Healthy:</span> stable low count proportional to the number of brokers (typically 1–5 per broker). <span class="warn">Watch for:</span> climbing count (connection leak); sudden drop to zero (connectivity lost); producer and consumer counts diverging without explanation.</p>
            </div>
            <div class="card">
              <div id="ioWaitNs" class="chart"></div>
              <p class="desc"><b>What:</b> Absolute nanoseconds the network I/O thread spends blocked waiting for the network per observation interval — for both producer and consumer. Complements the io-wait-ratio (a fraction) by showing the raw cost; useful when the ratio looks small but the absolute delay is still significant at high throughput.<br>
              <span class="good">Healthy:</span> low and stable on both sides. <span class="warn">Watch for:</span> high values while throughput is normal (I/O thread is the bottleneck); correlated spikes with request-latency-max (single slow I/O operations stalling the network thread).</p>
            </div>
          </div>

          <script>
            const cfg={height:320,margin:{t:42,b:46,l:58,r:16}};
            // E2E series
            const ex=[%s],el=[%s],ew=[%s],ep=[%s],ec=[%s],eg=[%s];
            // Kafka client metrics
            %s
            // Latency: cumulative all-time avg + per-window avg (instantaneous)
            Plotly.newPlot('e2eLatency',
              [{x:ex,y:ew,mode:'lines',name:'window avg (per tick)',line:{color:'#d62728',width:2}},
               {x:ex,y:el,mode:'lines',name:'cumulative avg',line:{color:'#1f77b4',dash:'dot'}}],
              {...cfg,title:'E2E Latency (ms) \u2014 window=real-time, cumulative=all-time mean',
               xaxis:{title:'ms since start'},yaxis:{title:'ms'}});
            Plotly.newPlot('e2eCounts',
              [{x:ex,y:ep,mode:'lines',name:'produced',line:{color:'#2ca02c'}},
               {x:ex,y:ec,mode:'lines',name:'consumed',line:{color:'#ff7f0e'}}],
              {...cfg,title:'Produced vs Consumed',xaxis:{title:'ms since start'},yaxis:{title:'messages'}});
            Plotly.newPlot('e2eLag',
              [{x:ex,y:eg,mode:'lines',name:'consumer lag (produced \u2212 consumed)',
                line:{color:'#9467bd'},fill:'tozeroy',fillcolor:'rgba(148,103,189,0.15)'}],
              {...cfg,title:'Consumer Lag (unread messages in broker)',
               xaxis:{title:'ms since start'},yaxis:{title:'messages'}});

            Plotly.newPlot('pReqLat',
              [{x:mt,y:mReqLat,mode:'lines',name:'request-latency-avg',line:{color:'#1f77b4'}},
               {x:mt,y:mReqLatMax,mode:'lines',name:'request-latency-max',line:{color:'#d62728',dash:'dot'}}],
              {...cfg,title:'Producer: Request Latency Avg + Max (ms)',xaxis:{title:'ms since start'},yaxis:{title:'ms'}});
            Plotly.newPlot('pReqRate',
              [{x:mt,y:mReqR,mode:'lines',name:'request-rate',line:{color:'#2ca02c'}}],
              {...cfg,title:'Producer: Request Rate (req/s)',xaxis:{title:'ms since start'},yaxis:{title:'req/s'}});
            Plotly.newPlot('pRetryErr',
              [{x:mt,y:mRetry,mode:'lines',name:'record-retry-rate',line:{color:'#ff7f0e'}},
               {x:mt,y:mErrR, mode:'lines',name:'record-error-rate', line:{color:'#d62728'}}],
              {...cfg,title:'Producer: Retry & Error Rate (rec/s)',xaxis:{title:'ms since start'},yaxis:{title:'rec/s'}});
            Plotly.newPlot('pThrottle',
              [{x:mt,y:mThrotP,mode:'lines',name:'produce-throttle-time-avg',line:{color:'#9467bd'}}],
              {...cfg,title:'Producer: Throttle Time Avg (ms)',xaxis:{title:'ms since start'},yaxis:{title:'ms'}});
            Plotly.newPlot('pSendRate',
              [{x:mt,y:mSendR,mode:'lines',name:'record-send-rate',line:{color:'#2ca02c'}}],
              {...cfg,title:'Producer: Record Send Rate (rec/s)',xaxis:{title:'ms since start'},yaxis:{title:'rec/s'}});
            Plotly.newPlot('pBufAvail',
              [{x:mt,y:mBufAvail,mode:'lines',name:'buffer-available-bytes',
                line:{color:'#1f77b4'},fill:'tozeroy',fillcolor:'rgba(31,119,180,0.1)'}],
              {...cfg,title:'Producer: Buffer Available Bytes',xaxis:{title:'ms since start'},yaxis:{title:'bytes'}});

            Plotly.newPlot('cFetchLat',
              [{x:mt,y:mFLat,mode:'lines+markers',name:'fetch-latency-avg',line:{color:'#1f77b4'}}],
              {...cfg,title:'Consumer: Fetch Latency Avg (ms)',xaxis:{title:'ms since start'},yaxis:{title:'ms'}});
            Plotly.newPlot('cCommitLat',
              [{x:mt,y:mCLat,mode:'lines',name:'commit-latency-avg',line:{color:'#8c564b'}}],
              {...cfg,title:'Consumer: Commit Latency Avg (ms)',xaxis:{title:'ms since start'},yaxis:{title:'ms'}});
            Plotly.newPlot('cRecRate',
              [{x:mt,y:mRecR,mode:'lines',name:'records-consumed-rate',line:{color:'#2ca02c'}}],
              {...cfg,title:'Consumer: Records Consumed Rate (rec/s)',xaxis:{title:'ms since start'},yaxis:{title:'rec/s'}});
            Plotly.newPlot('cBytesRate',
              [{x:mt,y:mBytR,mode:'lines',name:'bytes-consumed-rate',line:{color:'#17becf'}}],
              {...cfg,title:'Consumer: Bytes Consumed Rate (B/s)',xaxis:{title:'ms since start'},yaxis:{title:'B/s'}});
            Plotly.newPlot('cFetchRate',
              [{x:mt,y:mFR,mode:'lines',name:'fetch-rate',line:{color:'#7f7f7f'}}],
              {...cfg,title:'Consumer: Fetch Rate (req/s)',xaxis:{title:'ms since start'},yaxis:{title:'req/s'}});
            Plotly.newPlot('cFetchThrot',
              [{x:mt,y:mFThr,mode:'lines',name:'fetch-throttle-time-avg',line:{color:'#bcbd22'}}],
              {...cfg,title:'Consumer: Fetch Throttle Time Avg (ms)',xaxis:{title:'ms since start'},yaxis:{title:'ms'}});
            Plotly.newPlot('cFailReb',
              [{x:mt,y:mFailReb,mode:'lines',name:'failed-rebalance-rate-per-hour',line:{color:'#d62728'}}],
              {...cfg,title:'Consumer: Failed Rebalance Rate (/h)',xaxis:{title:'ms since start'},yaxis:{title:'rate/h'}});
            Plotly.newPlot('cLagMax',
              [{x:mt,y:mLagMax,mode:'lines',name:'records-lag-max',
                line:{color:'#ff7f0e',width:2},fill:'tozeroy',fillcolor:'rgba(255,127,14,0.15)'}],
              {...cfg,title:'Consumer: Max Partition Lag (records-lag-max)',xaxis:{title:'ms since start'},yaxis:{title:'records'}});
            Plotly.newPlot('cPollIdle',
              [{x:mt,y:mPollIdle,mode:'lines',name:'poll-idle-ratio-avg',line:{color:'#8c564b'}}],
              {...cfg,title:'Consumer: Poll Idle Ratio (0=saturated, 1=idle)',xaxis:{title:'ms since start'},yaxis:{title:'ratio [0-1]',range:[0,1]}});

            Plotly.newPlot('pIo',
              [{x:mt,y:mPIoR,mode:'lines',name:'io-ratio',     line:{color:'#1f77b4'}},
               {x:mt,y:mPIoW,mode:'lines',name:'io-wait-ratio',line:{color:'#aec7e8'}}],
              {...cfg,title:'Producer: I/O Ratios',xaxis:{title:'ms since start'},yaxis:{title:'ratio [0-1]'}});
            Plotly.newPlot('cIo',
              [{x:mt,y:mCIoR,mode:'lines',name:'io-ratio',     line:{color:'#ff7f0e'}},
               {x:mt,y:mCIoW,mode:'lines',name:'io-wait-ratio',line:{color:'#ffbb78'}}],
              {...cfg,title:'Consumer: I/O Ratios',xaxis:{title:'ms since start'},yaxis:{title:'ratio [0-1]'}});
            Plotly.newPlot('connCount',
              [{x:mt,y:mPConnCnt,mode:'lines',name:'producer connections',line:{color:'#2ca02c'}},
               {x:mt,y:mCConnCnt,mode:'lines',name:'consumer connections',line:{color:'#1f77b4'}}],
              {...cfg,title:'Active Broker Connections (producer + consumer)',xaxis:{title:'ms since start'},yaxis:{title:'connections'}});
            Plotly.newPlot('ioWaitNs',
              [{x:mt,y:mPIoWaitNs,mode:'lines',name:'producer io-wait-time-ns-avg',line:{color:'#9467bd'}},
               {x:mt,y:mCIoWaitNs,mode:'lines',name:'consumer io-wait-time-ns-avg',line:{color:'#e377c2'}}],
              {...cfg,title:'I/O Wait Time ns/avg (producer + consumer)',xaxis:{title:'ms since start'},yaxis:{title:'ns'}});
          </script>
        </body>
        </html>
        """.formatted(runId,
            runId, topic, messageType, modeDesc, numMessages, avg, p95, p99,
            ex, el, ew, ep, ec, eg,
            jsData);
  }

  // ── Mock invoice data & generator ────────────────────────────────────────

  private static final Random RNG = new Random();

  // {customerId, companyName, contactName, email, phone, tier}
  private static final String[][] CUSTOMERS = {
    {"CUST-001","Stark Industries","Tony Stark","tony.stark@stark.com","+1-212-555-0100","PLATINUM"},
    {"CUST-002","Wayne Enterprises","Bruce Wayne","b.wayne@wayne.com","+1-212-555-0200","GOLD"},
    {"CUST-003","Acme Corporation","Road Runner","r.runner@acme.com","+1-303-555-0300","GOLD"},
    {"CUST-004","Globex Industries","Homer Simpson","h.simpson@globex.com","+1-217-555-0400","SILVER"},
    {"CUST-005","Initech LLC","Peter Gibbons","p.gibbons@initech.com","+1-512-555-0500","SILVER"},
    {"CUST-006","Umbrella Corp","Albert Wesker","a.wesker@umbrella.com","+1-415-555-0600","GOLD"},
    {"CUST-007","Cyberdyne Systems","Miles Dyson","m.dyson@cyberdyne.com","+1-408-555-0700","PLATINUM"},
    {"CUST-008","Soylent Corp","Saul Goodman","s.goodman@soylent.com","+1-305-555-0800","BRONZE"},
    {"CUST-009","Massive Dynamic","Walter Bishop","w.bishop@massive.com","+1-617-555-0900","GOLD"},
    {"CUST-010","Oscorp Industries","Norman Osborn","n.osborn@oscorp.com","+1-212-555-1000","SILVER"},
  };

  // {line1, city, state, zipCode}
  private static final String[][] ADDRESSES = {
    {"100 Technology Dr","San Francisco","CA","94105"},
    {"200 Innovation Blvd","New York","NY","10001"},
    {"300 Commerce St","Chicago","IL","60601"},
    {"400 Enterprise Ave","Austin","TX","78701"},
    {"500 Digital Way","Seattle","WA","98101"},
  };

  // {sku, description, unitPrice}
  private static final Object[][] PRODUCTS = {
    {"SKU-ENT-LIC","Enterprise License (annual)",1200.00},
    {"SKU-STD-LIC","Standard License (annual)",450.00},
    {"SKU-SUP-PRO","Professional Support Package",799.00},
    {"SKU-SUP-BSC","Basic Support Package",199.00},
    {"SKU-TRN-WEB","Web Training Package",350.00},
    {"SKU-TRN-ONS","On-site Training (per day)",2500.00},
    {"SKU-INT-API","API Integration Pack",599.00},
    {"SKU-STO-100","Cloud Storage 100 GB/month",49.00},
  };

  private static final String[] STATUSES        = {"ISSUED","ISSUED","ISSUED","ISSUED","ISSUED","PAID","PAID","PAID","OVERDUE","DRAFT"};
  private static final String[] PAYMENT_METHODS = {"WIRE_TRANSFER","CREDIT_CARD","ACH","CHECK","BANK_TRANSFER"};
  private static final String[] PAYMENT_TERMS   = {"NET_15","NET_30","NET_30","NET_45","NET_60","DUE_ON_RECEIPT"};
  private static final String[] CURRENCIES      = {"USD","USD","USD","EUR","GBP"};

  private static KafkaMessage buildInvoice(int seq, String runId) {
    String[] customer = CUSTOMERS[RNG.nextInt(CUSTOMERS.length)];
    String customerId = customer[0];
    String invoiceId  = "INV-" + runId + "-" + String.format("%04d", seq);
    String orderId    = "ORD-" + String.format("%08X", RNG.nextInt());
    String status     = STATUSES[RNG.nextInt(STATUSES.length)];
    String currency   = CURRENCIES[RNG.nextInt(CURRENCIES.length)];
    String payMethod  = PAYMENT_METHODS[RNG.nextInt(PAYMENT_METHODS.length)];
    String payTerms   = PAYMENT_TERMS[RNG.nextInt(PAYMENT_TERMS.length)];
    String[] billAddr = ADDRESSES[RNG.nextInt(ADDRESSES.length)];
    String[] shipAddr = ADDRESSES[RNG.nextInt(ADDRESSES.length)];

    int itemCount = 1 + RNG.nextInt(4);
    int[] picked  = RNG.ints(0, PRODUCTS.length).distinct().limit(itemCount).toArray();

    double subtotal = 0;
    StringBuilder lineItems = new StringBuilder();
    for (int li = 0; li < picked.length; li++) {
      Object[] p = PRODUCTS[picked[li]];
      int    qty          = 1 + RNG.nextInt(5);
      double unitPrice    = (Double) p[2];
      double lineDiscount = RNG.nextInt(10) < 2 ? Math.round(unitPrice * 0.1 * 100) / 100.0 : 0.0;
      double lineSub      = Math.round((unitPrice * qty - lineDiscount) * 100) / 100.0;
      subtotal += lineSub;
      if (li > 0) lineItems.append(",");
      lineItems.append(String.format(
          "{\"lineNo\":%d,\"sku\":\"%s\",\"description\":\"%s\"," +
          "\"quantity\":%d,\"unitPrice\":%.2f,\"discount\":%.2f,\"subtotal\":%.2f}",
          li + 1, p[0], p[1], qty, unitPrice, lineDiscount, lineSub));
    }
    subtotal = Math.round(subtotal * 100) / 100.0;

    double discountPct = RNG.nextInt(10) < 3 ? 5 + RNG.nextInt(16) : 0;
    double discountAmt = Math.round(subtotal * discountPct / 100 * 100) / 100.0;
    double taxable     = subtotal - discountAmt;
    double taxRate     = 8.875;
    double taxAmt      = Math.round(taxable * taxRate / 100 * 100) / 100.0;
    double total       = Math.round((taxable + taxAmt) * 100) / 100.0;

    int dueDays = switch (payTerms) {
      case "NET_15" -> 15;
      case "NET_45" -> 45;
      case "NET_60" -> 60;
      case "NET_30" -> 30;
      default       -> 0;  // DUE_ON_RECEIPT
    };
    String issueDate = "2026-03-26";
    String dueDate   = java.time.LocalDate.of(2026, 3, 26).plusDays(dueDays).toString();

    String json = String.format(
        "{" +
        "\"invoiceId\":\"%s\",\"orderId\":\"%s\"," +
        "\"customer\":{\"customerId\":\"%s\",\"companyName\":\"%s\",\"contactName\":\"%s\"," +
          "\"email\":\"%s\",\"phone\":\"%s\",\"tier\":\"%s\"}," +
        "\"billingAddress\":{\"line1\":\"%s\",\"city\":\"%s\",\"state\":\"%s\",\"zipCode\":\"%s\",\"country\":\"US\"}," +
        "\"shippingAddress\":{\"line1\":\"%s\",\"city\":\"%s\",\"state\":\"%s\",\"zipCode\":\"%s\",\"country\":\"US\"}," +
        "\"issueDate\":\"%s\",\"dueDate\":\"%s\",\"status\":\"%s\"," +
        "\"lineItems\":[%s]," +
        "\"subtotal\":%.2f,\"discountPct\":%.1f,\"discountAmount\":%.2f," +
        "\"taxRate\":%.3f,\"taxAmount\":%.2f,\"total\":%.2f," +
        "\"currency\":\"%s\",\"paymentTerms\":\"%s\",\"paymentMethod\":\"%s\"," +
        "\"notes\":\"Please reference invoice number on payment.\"," +
        "\"tags\":[\"%s\",\"%s\"]," +
        "\"metadata\":{\"source\":\"billing-service\",\"environment\":\"e2e-test\"," +
          "\"schemaVersion\":\"2.1\",\"generatedBy\":\"KafkaE2ELatency\"}}",
        invoiceId, orderId,
        customerId, customer[1], customer[2], customer[3], customer[4], customer[5],
        billAddr[0], billAddr[1], billAddr[2], billAddr[3],
        shipAddr[0], shipAddr[1], shipAddr[2], shipAddr[3],
        issueDate, dueDate, status,
        lineItems,
        subtotal, discountPct, discountAmt,
        taxRate, taxAmt, total,
        currency, payTerms, payMethod,
        customer[5].toLowerCase(), status.toLowerCase());

    return new KafkaMessage(invoiceId, customerId, json);
  }

  // ── Message factory ───────────────────────────────────────────────────────

  private static KafkaMessage buildMessage(int seq, String runId, String messageType) {
    return switch (messageType) {
      case "payment" -> buildPayment(seq, runId);
      case "order"   -> buildOrder(seq, runId);
      default        -> buildInvoice(seq, runId);
    };
  }

  // ── Payment message data & builder ───────────────────────────────────────

  private static final String[] TRANSACTION_TYPES = {"PAYMENT","PAYMENT","PAYMENT","REFUND","TRANSFER","AUTHORIZATION","CAPTURE","VOID"};
  private static final String[] TX_STATUSES  = {"APPROVED","APPROVED","APPROVED","APPROVED","PENDING","DECLINED","PROCESSING","SETTLED"};
  private static final String[] TX_CHANNELS  = {"ONLINE","POS","MOBILE","CONTACTLESS","ATM"};
  private static final String[] CARD_TYPES   = {"VISA","MASTERCARD","AMEX","DISCOVER","VISA","MASTERCARD"};

  // {accountId, accountType, bankCode, maskedIban}
  private static final String[][] ACCOUNTS = {
    {"ACC-00123456","CHECKING","BOFAUS3N","US12 BOFA **** **** 1234"},
    {"ACC-00234567","SAVINGS", "CHASUS33","US34 CHAS **** **** 5678"},
    {"ACC-00345678","CHECKING","WFBIUS6S","US56 WFBI **** **** 9012"},
    {"ACC-00456789","BUSINESS","CITIUS33","US78 CITI **** **** 3456"},
    {"ACC-00567890","CHECKING","USBUS44P","US90 USBA **** **** 7890"},
    {"ACC-00678901","SAVINGS", "HBUKGB4B","GB12 HSBC **** **** 2345"},
    {"ACC-00789012","BUSINESS","DEUTDEDB","DE34 DEUT **** **** 6789"},
    {"ACC-00890123","CHECKING","BNPAFRPP","FR56 BNPA **** **** 0123"},
  };

  // {merchantId, name, category, mcc, country}
  private static final String[][] MERCHANTS = {
    {"MCH-1001","Amazon Web Services",  "Cloud Services",   "7372","US"},
    {"MCH-1002","Stripe Inc",           "Payment Services", "7389","US"},
    {"MCH-1003","Slack Technologies",   "SaaS Software",    "7372","US"},
    {"MCH-1004","Shopify Inc",          "E-Commerce",       "5961","CA"},
    {"MCH-1005","Twilio Inc",           "Telecom API",      "4813","US"},
    {"MCH-1006","GitHub Inc",           "Dev Tools SaaS",   "7372","US"},
    {"MCH-1007","Datadog Inc",          "Monitoring SaaS",  "7372","US"},
    {"MCH-1008","Confluent Inc",        "Streaming SaaS",   "7372","US"},
  };

  private static KafkaMessage buildPayment(int seq, String runId) {
    String transactionId = "TXN-" + runId + "-" + String.format("%04d", seq);
    String[] src      = ACCOUNTS[RNG.nextInt(ACCOUNTS.length)];
    String[] dst      = ACCOUNTS[RNG.nextInt(ACCOUNTS.length)];
    String[] merchant = MERCHANTS[RNG.nextInt(MERCHANTS.length)];
    String txType     = TRANSACTION_TYPES[RNG.nextInt(TRANSACTION_TYPES.length)];
    String status     = TX_STATUSES[RNG.nextInt(TX_STATUSES.length)];
    String channel    = TX_CHANNELS[RNG.nextInt(TX_CHANNELS.length)];
    String currency   = CURRENCIES[RNG.nextInt(CURRENCIES.length)];
    String cardType   = CARD_TYPES[RNG.nextInt(CARD_TYPES.length)];
    double amount     = Math.round((1.0 + RNG.nextDouble() * 9999) * 100) / 100.0;
    int    riskScore  = RNG.nextInt(100);
    int    procMs     = 50 + RNG.nextInt(450);
    String json = String.format(
        "{" +
        "\"transactionId\":\"%s\"," +
        "\"type\":\"%s\"," +
        "\"timestamp\":\"2026-03-26T%02d:%02d:%02dZ\"," +
        "\"channel\":\"%s\"," +
        "\"sourceAccount\":{\"accountId\":\"%s\",\"accountType\":\"%s\",\"bankCode\":\"%s\",\"maskedIban\":\"%s\"}," +
        "\"destinationAccount\":{\"accountId\":\"%s\",\"accountType\":\"%s\",\"bankCode\":\"%s\",\"maskedIban\":\"%s\"}," +
        "\"merchant\":{\"merchantId\":\"%s\",\"name\":\"%s\",\"category\":\"%s\",\"mcc\":\"%s\",\"country\":\"%s\"}," +
        "\"card\":{\"maskedPan\":\"**** **** **** %04d\",\"cardType\":\"%s\",\"expiryMonth\":%d,\"expiryYear\":%d,\"tokenized\":true}," +
        "\"amount\":%.2f," +
        "\"currency\":\"%s\"," +
        "\"status\":\"%s\"," +
        "\"riskScore\":%d," +
        "\"fraudFlag\":%b," +
        "\"processingTimeMs\":%d," +
        "\"metadata\":{\"source\":\"payment-gateway\",\"environment\":\"e2e-test\"," +
          "\"schemaVersion\":\"1.0\",\"generatedBy\":\"KafkaE2ELatency\"}}",
        transactionId, txType,
        RNG.nextInt(24), RNG.nextInt(60), RNG.nextInt(60),
        channel,
        src[0], src[1], src[2], src[3],
        dst[0], dst[1], dst[2], dst[3],
        merchant[0], merchant[1], merchant[2], merchant[3], merchant[4],
        1000 + RNG.nextInt(9000), cardType, 1 + RNG.nextInt(12), 2026 + RNG.nextInt(5),
        amount, currency, status,
        riskScore, riskScore > 80,
        procMs);
    return new KafkaMessage(transactionId, src[0], json);
  }

  // ── Order message data & builder ─────────────────────────────────────────

  private static final String[] ORDER_STATUSES  = {"PLACED","PLACED","CONFIRMED","PROCESSING","SHIPPED","DELIVERED","CANCELLED"};
  private static final String[] ORDER_CHANNELS  = {"WEB","MOBILE","API","STORE","PHONE"};
  private static final String[] CARRIERS        = {"UPS","FedEx","USPS","DHL","OnTrac","LaserShip"};

  // {sku, name, unitPrice, weightKg}
  private static final Object[][] ECOMMERCE_PRODUCTS = {
    {"SKU-ELEC-001","Wireless Headphones Pro",       149.99, 0.32},
    {"SKU-ELEC-002","USB-C Hub 7-in-1",               49.99, 0.18},
    {"SKU-APPL-001","Smart Watch Series 5",           299.00, 0.45},
    {"SKU-APPL-002","Portable Bluetooth Speaker",     79.00, 0.60},
    {"SKU-COMP-001","Mechanical Keyboard (TKL)",      129.00, 0.95},
    {"SKU-COMP-002","27-inch 4K USB-C Monitor",       499.00, 5.80},
    {"SKU-COMP-003","Gaming Mouse RGB",                69.00, 0.12},
    {"SKU-BOOK-001","Designing Data-Intensive Apps",   49.99, 0.85},
    {"SKU-BOOK-002","Clean Code",                      38.00, 0.55},
    {"SKU-SOFT-001","Annual IDE License",             249.00, 0.00},
  };

  private static KafkaMessage buildOrder(int seq, String runId) {
    String orderId    = "ORD-" + runId + "-" + String.format("%04d", seq);
    String[] customer = CUSTOMERS[RNG.nextInt(CUSTOMERS.length)];
    String channel    = ORDER_CHANNELS[RNG.nextInt(ORDER_CHANNELS.length)];
    String status     = ORDER_STATUSES[RNG.nextInt(ORDER_STATUSES.length)];
    String currency   = CURRENCIES[RNG.nextInt(CURRENCIES.length)];
    String[] shipAddr = ADDRESSES[RNG.nextInt(ADDRESSES.length)];
    String carrier    = CARRIERS[RNG.nextInt(CARRIERS.length)];
    int itemCount = 1 + RNG.nextInt(4);
    int[] picked  = RNG.ints(0, ECOMMERCE_PRODUCTS.length).distinct().limit(itemCount).toArray();
    double subtotal = 0;
    StringBuilder items = new StringBuilder();
    for (int li = 0; li < picked.length; li++) {
      Object[] p  = ECOMMERCE_PRODUCTS[picked[li]];
      int qty      = 1 + RNG.nextInt(3);
      double price = (Double) p[2];
      double sub   = Math.round(price * qty * 100) / 100.0;
      subtotal    += sub;
      if (li > 0) items.append(",");
      items.append(String.format(
          "{\"sku\":\"%s\",\"name\":\"%s\",\"quantity\":%d,\"unitPrice\":%.2f,\"subtotal\":%.2f,\"weightKg\":%.2f}",
          p[0], p[1], qty, price, sub, (Double) p[3]));
    }
    subtotal = Math.round(subtotal * 100) / 100.0;
    double shipping = RNG.nextInt(4) == 0 ? 0.0 : Math.round((4.99 + RNG.nextDouble() * 20) * 100) / 100.0;
    double tax      = Math.round(subtotal * 0.08875 * 100) / 100.0;
    double total    = Math.round((subtotal + shipping + tax) * 100) / 100.0;
    String tracking = carrier.substring(0, 2).toUpperCase()
        + String.format("%012d", Math.abs(RNG.nextLong() % 1_000_000_000_000L));
    String json = String.format(
        "{" +
        "\"orderId\":\"%s\"," +
        "\"customerId\":\"%s\"," +
        "\"companyName\":\"%s\"," +
        "\"channel\":\"%s\"," +
        "\"orderDate\":\"2026-03-26\"," +
        "\"estimatedDelivery\":\"2026-04-%02d\"," +
        "\"status\":\"%s\"," +
        "\"items\":[%s]," +
        "\"shippingAddress\":{\"line1\":\"%s\",\"city\":\"%s\",\"state\":\"%s\",\"zipCode\":\"%s\",\"country\":\"US\"}," +
        "\"subtotal\":%.2f," +
        "\"shippingCost\":%.2f," +
        "\"taxAmount\":%.2f," +
        "\"total\":%.2f," +
        "\"currency\":\"%s\"," +
        "\"carrier\":\"%s\"," +
        "\"trackingNumber\":\"%s\"," +
        "\"giftOrder\":%b," +
        "\"metadata\":{\"source\":\"order-service\",\"environment\":\"e2e-test\"," +
          "\"schemaVersion\":\"1.0\",\"generatedBy\":\"KafkaE2ELatency\"}}",
        orderId, customer[0], customer[1],
        channel,
        1 + RNG.nextInt(28),
        status,
        items,
        shipAddr[0], shipAddr[1], shipAddr[2], shipAddr[3],
        subtotal, shipping, tax, total, currency,
        carrier, tracking,
        RNG.nextInt(10) == 0);
    return new KafkaMessage(orderId, customer[0], json);
  }

  private static class MetricSnapshot {
    final long   tMs;
    // producer-metrics
    final double produceThrottleTimeAvg;
    final double recordRetryRate;
    final double recordErrorRate;
    final double requestLatencyAvg;
    final double requestRate;
    final double producerIoRatio;
    final double producerIoWaitRatio;
    // consumer-fetch-manager-metrics
    final double bytesConsumedRate;
    final double fetchLatencyAvg;
    final double fetchRate;
    final double fetchThrottleTimeAvg;
    final double recordsConsumedRate;
    // consumer-coordinator-metrics
    final double commitLatencyAvg;
    final double failedRebalanceRatePerHour;
    // consumer-metrics (network)
    final double consumerIoRatio;
    final double consumerIoWaitRatio;
    // producer — additional common metrics
    final double requestLatencyMax;
    final double recordSendRate;
    final double bufferAvailableBytes;
    final double producerConnectionCount;
    final double producerIoWaitTimeNsAvg;
    // consumer — additional common metrics
    final double recordsLagMax;
    final double pollIdleRatioAvg;
    final double consumerConnectionCount;
    final double consumerIoWaitTimeNsAvg;

    MetricSnapshot(long tMs,
        double produceThrottleTimeAvg, double recordRetryRate, double recordErrorRate,
        double requestLatencyAvg, double requestRate,
        double producerIoRatio, double producerIoWaitRatio,
        double bytesConsumedRate, double fetchLatencyAvg, double fetchRate,
        double fetchThrottleTimeAvg, double recordsConsumedRate,
        double commitLatencyAvg, double failedRebalanceRatePerHour,
        double consumerIoRatio, double consumerIoWaitRatio,
        double requestLatencyMax, double recordSendRate, double bufferAvailableBytes,
        double producerConnectionCount, double producerIoWaitTimeNsAvg,
        double recordsLagMax, double pollIdleRatioAvg,
        double consumerConnectionCount, double consumerIoWaitTimeNsAvg) {
      this.tMs = tMs;
      this.produceThrottleTimeAvg   = produceThrottleTimeAvg;
      this.recordRetryRate          = recordRetryRate;
      this.recordErrorRate          = recordErrorRate;
      this.requestLatencyAvg        = requestLatencyAvg;
      this.requestRate              = requestRate;
      this.producerIoRatio          = producerIoRatio;
      this.producerIoWaitRatio      = producerIoWaitRatio;
      this.bytesConsumedRate        = bytesConsumedRate;
      this.fetchLatencyAvg          = fetchLatencyAvg;
      this.fetchRate                = fetchRate;
      this.fetchThrottleTimeAvg     = fetchThrottleTimeAvg;
      this.recordsConsumedRate      = recordsConsumedRate;
      this.commitLatencyAvg         = commitLatencyAvg;
      this.failedRebalanceRatePerHour = failedRebalanceRatePerHour;
      this.consumerIoRatio          = consumerIoRatio;
      this.consumerIoWaitRatio      = consumerIoWaitRatio;
      this.requestLatencyMax        = requestLatencyMax;
      this.recordSendRate           = recordSendRate;
      this.bufferAvailableBytes     = bufferAvailableBytes;
      this.producerConnectionCount  = producerConnectionCount;
      this.producerIoWaitTimeNsAvg  = producerIoWaitTimeNsAvg;
      this.recordsLagMax            = recordsLagMax;
      this.pollIdleRatioAvg         = pollIdleRatioAvg;
      this.consumerConnectionCount  = consumerConnectionCount;
      this.consumerIoWaitTimeNsAvg  = consumerIoWaitTimeNsAvg;
    }
  }

  private static class KafkaMessage {
    final String messageId;  // invoiceId / transactionId / orderId
    final String entityId;   // customerId / accountId
    final String json;

    KafkaMessage(String messageId, String entityId, String json) {
      this.messageId = messageId;
      this.entityId  = entityId;
      this.json      = json;
    }
  }

  private static class Sample {
    long tMs;
    long produced;
    long consumed;
    long avgLatencyMs;       // cumulative all-time mean
    long windowAvgLatencyMs; // mean only for messages consumed in this sample window
    long lag;                // produced - consumed at sample time

    Sample(long tMs, long produced, long consumed,
           long avgLatencyMs, long windowAvgLatencyMs, long lag) {
      this.tMs = tMs;
      this.produced = produced;
      this.consumed = consumed;
      this.avgLatencyMs = avgLatencyMs;
      this.windowAvgLatencyMs = windowAvgLatencyMs;
      this.lag = lag;
    }

    String toJson() {
      return "{\"tMs\":" + tMs
          + ",\"produced\":" + produced
          + ",\"consumed\":" + consumed
          + ",\"avgLatencyMs\":" + avgLatencyMs
          + ",\"windowAvgLatencyMs\":" + windowAvgLatencyMs
          + ",\"lag\":" + lag + "}";
    }
  }
}
