package FlinkEcommerce;

import DTO.EventEnvelope;
import Deserializer.JsonEventDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import DTO.TopNWindowResult;
import sink.RedisTopNZSetSink;
import process.AbandonedCartProcessFunction;
import sink.RedisAbandonedSink;
import DTO.ActiveUsersMinute;
import DTO.TopProductHourly;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;

public class MultiEventJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Reliability — checkpoints + restart strategy (at-least-once)
        env.enableCheckpointing(10_000L); // checkpoint every 10s
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
        env.getCheckpointConfig().setCheckpointTimeout(60_000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.seconds(10)));

        String topic = "financial_transactions"; // matches SalesDateGenerator/main.py
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");

        // Kafka offsets start mode for replay drills: latest | earliest | timestamp
        String startMode = System.getenv().getOrDefault("KAFKA_START_MODE", "latest").toLowerCase();
        OffsetsInitializer startOffsets;
        switch (startMode) {
            case "earliest":
                startOffsets = OffsetsInitializer.earliest();
                break;
            case "timestamp":
                long ts = 0L;
                try { ts = Long.parseLong(System.getenv().getOrDefault("KAFKA_START_TIMESTAMP_MS", "0")); } catch (Exception ignored) {}
                startOffsets = ts > 0 ? OffsetsInitializer.timestamp(ts) : OffsetsInitializer.latest();
                break;
            default:
                startOffsets = OffsetsInitializer.latest();
        }

        // Source: Kafka -> EventEnvelope with event-time watermarks (5s out-of-orderness)
        WatermarkStrategy<EventEnvelope> wm = WatermarkStrategy
                .<EventEnvelope>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> {
                    try {
                        // robust for ISO8601 with offset ("+00:00")
                        return OffsetDateTime.parse(e.getTimestamp()).toInstant().toEpochMilli();
                    } catch (Exception ex) {
                        return ts; // fallback: use provided ts when parsing fails
                    }
                });

        KafkaSource<EventEnvelope> source = KafkaSource.<EventEnvelope>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("flink-multievent")
                .setStartingOffsets(startOffsets)
                .setValueOnlyDeserializer(new JsonEventDeserializationSchema())
                .build();

        DataStream<EventEnvelope> events = env.fromSource(source, wm, "kafka-events");

        // Route by eventType
        DataStream<EventEnvelope> pageViews = events.filter(e -> "page_view".equals(e.getEventType()))
                                                   .name("filter-page-views");
        DataStream<EventEnvelope> addToCarts = events.filter(e -> "add_to_cart".equals(e.getEventType()))
                                                     .name("filter-add-to-cart");
        DataStream<EventEnvelope> checkouts = events.filter(e -> "checkout".equals(e.getEventType()))
                                                    .name("filter-checkout");

        // Quick sanity: print routed streams (can be commented later)
        pageViews.map(e -> Tuple2.of("page_view", e.getUserId()))
                 .returns(Types.TUPLE(Types.STRING, Types.STRING))
                 .name("peek-page-views").print();
        addToCarts.map(e -> Tuple2.of("add_to_cart", e.getProductId()))
                  .returns(Types.TUPLE(Types.STRING, Types.STRING))
                  .name("peek-add-to-cart").print();
        checkouts.map(e -> Tuple2.of("checkout", e.getTransactionId()))
                 .returns(Types.TUPLE(Types.STRING, Types.STRING))
                 .name("peek-checkout").print();

        // 5-minute active users (slide 1 minute) using userId from page_view
        SingleOutputStreamOperator<Long> activeUsers5m =
                pageViews
                        .map(EventEnvelope::getUserId)
                        .returns(Types.STRING)
                        .windowAll(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                        .apply(new AllWindowFunction<String, Long, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow w, Iterable<String> values, Collector<Long> out) {
                                HashSet<String> set = new HashSet<>();
                                for (String u : values) {
                                    if (u != null) set.add(u);
                                }
                                out.collect((long) set.size());
                            }
                        })
                        .name("active-users-5m");

        // Sink to Redis hash for real-time metric consumption
        activeUsers5m.addSink(new sink.RedisActiveUsersSink()).name("redis-active-users");

        // For now, just print the metric; next step we’ll sink to Redis
        activeUsers5m.map(cnt -> "active_users_5m=" + cnt)
                .returns(Types.STRING)
                .name("print-active-users-5m")
                .print();

        // Active users minute record for PG/ES (using window end as minute_ts)
        SingleOutputStreamOperator<ActiveUsersMinute> activeUsers5mRecord =
                pageViews
                        .map(EventEnvelope::getUserId)
                        .returns(Types.STRING)
                        .windowAll(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                        .apply(new AllWindowFunction<String, ActiveUsersMinute, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow w, Iterable<String> values, Collector<ActiveUsersMinute> out) {
                                HashSet<String> set = new HashSet<>();
                                for (String u : values) { if (u != null) set.add(u); }
                                out.collect(new ActiveUsersMinute(new java.sql.Timestamp(w.getEnd()), set.size()));
                            }
                        })
                        .name("active-users-5m-record");

        // Hourly TopN products by amount (from add_to_cart)
        // Map to (productId, amount)
        DataStream<Tuple2<String, Double>> productSales = addToCarts
                .map(e -> {
                    Double amount = e.getTotalAmount();
                    if (amount == null) {
                        Double price = e.getProductPrice();
                        Integer qty = e.getProductQuantity();
                        amount = (price != null && qty != null) ? price * qty : 0.0;
                    }
                    String pid = e.getProductId();
                    return Tuple2.of(pid == null ? "unknown" : pid, amount);
                })
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .name("map-product-sales");

        // Single-parallelism global window: aggregate and emit TopN for the hour
        DataStream<TopNWindowResult> topNHourly = productSales
                .windowAll(org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new AllWindowFunction<Tuple2<String, Double>, TopNWindowResult, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow w, Iterable<Tuple2<String, Double>> values, Collector<TopNWindowResult> out) {
                        Map<String, Double> agg = new HashMap<>();
                        for (Tuple2<String, Double> t : values) {
                            if (t == null || t.f0 == null || t.f1 == null) continue;
                            agg.merge(t.f0, t.f1, Double::sum);
                        }
                        List<Tuple2<String, Double>> list = new ArrayList<>();
                        for (Map.Entry<String, Double> e : agg.entrySet()) {
                            list.add(Tuple2.of(e.getKey(), e.getValue()));
                        }
                        list = list.stream()
                                .sorted((a, b) -> Double.compare(b.f1, a.f1))
                                .limit(10)
                                .collect(Collectors.toList());
                        TopNWindowResult res = new TopNWindowResult();
                        java.time.Instant start = java.time.Instant.ofEpochMilli(w.getStart());
                        java.time.LocalDateTime dt = java.time.LocalDateTime.ofInstant(start, ZoneOffset.UTC);
                        String hourKey = String.format("%04d%02d%02d%02d", dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), dt.getHour());
                        res.setHourKey(hourKey);
                        res.setWindowStartMs(w.getStart());
                        res.setTopN(list);
                        out.collect(res);
                    }
                })
                .name("topN-hourly");

        // Sink TopN to Redis ZSET per hour
        topNHourly.addSink(new RedisTopNZSetSink()).name("redis-topN-hourly");

        // Also print hour key for visibility
        topNHourly
                .map(r -> "topN_hour_key=" + r.getHourKey() + ", size=" + (r.getTopN() == null ? 0 : r.getTopN().size()))
                .returns(Types.STRING)
                .name("print-topN-hour")
                .print();

        // Flatten TopN to per-product hourly records for PG/ES
        SingleOutputStreamOperator<TopProductHourly> topProductHourlyRecords = topNHourly
                .flatMap(new org.apache.flink.api.common.functions.FlatMapFunction<TopNWindowResult, TopProductHourly>() {
                    @Override
                    public void flatMap(TopNWindowResult res, org.apache.flink.util.Collector<TopProductHourly> out) {
                        java.sql.Timestamp ts = new java.sql.Timestamp(res.getWindowStartMs());
                        if (res.getTopN() != null) {
                            for (Tuple2<String, Double> t : res.getTopN()) {
                                if (t != null && t.f0 != null && t.f1 != null) {
                                    out.collect(new TopProductHourly(ts, t.f0, t.f1));
                                }
                            }
                        }
                    }
                })
                .returns(TopProductHourly.class)
                .name("top-products-hourly-records");

        // ---------- JDBC (PostgreSQL) sinks ----------
        final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
        final String username = "postgres";
        final String password = "postgres";

        JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(500)
                .withBatchIntervalMs(500)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();

        // Create tables (executed via sink side-effect; simple demo style)
        activeUsers5mRecord.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS active_users_minute (" +
                        "minute_ts TIMESTAMP PRIMARY KEY, " +
                        "users BIGINT" +
                        ")",
                (JdbcStatementBuilder<ActiveUsersMinute>) (ps, v) -> {},
                execOptions, connOptions
        )).name("Create active_users_minute");

        topProductHourlyRecords.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS top_products_hourly (" +
                        "hour_ts TIMESTAMP, " +
                        "product_id TEXT, " +
                        "amount DOUBLE PRECISION, " +
                        "PRIMARY KEY(hour_ts, product_id)" +
                        ")",
                (JdbcStatementBuilder<TopProductHourly>) (ps, v) -> {},
                execOptions, connOptions
        )).name("Create top_products_hourly");

        // Upsert active_users_minute
        activeUsers5mRecord.addSink(JdbcSink.sink(
                "INSERT INTO active_users_minute(minute_ts, users) VALUES (?, ?) " +
                        "ON CONFLICT (minute_ts) DO UPDATE SET users = EXCLUDED.users",
                (JdbcStatementBuilder<ActiveUsersMinute>) (ps, v) -> {
                    ps.setTimestamp(1, v.getMinuteTs());
                    ps.setLong(2, v.getUsers());
                },
                execOptions, connOptions
        )).name("Upsert active_users_minute");

        // Upsert top_products_hourly
        topProductHourlyRecords.addSink(JdbcSink.sink(
                "INSERT INTO top_products_hourly(hour_ts, product_id, amount) VALUES (?, ?, ?) " +
                        "ON CONFLICT (hour_ts, product_id) DO UPDATE SET amount = EXCLUDED.amount",
                (JdbcStatementBuilder<TopProductHourly>) (ps, v) -> {
                    ps.setTimestamp(1, v.getHourTs());
                    ps.setString(2, v.getProductId());
                    ps.setDouble(3, v.getAmount());
                },
                execOptions, connOptions
        )).name("Upsert top_products_hourly");

        // ---------- Elasticsearch sinks ----------
        // active_users_minute index
        activeUsers5mRecord.sinkTo(
                new Elasticsearch7SinkBuilder<ActiveUsersMinute>()
                        .setHosts(new HttpHost("localhost", 9200, "http"))
                        .setEmitter((val, ctx2, indexer) -> {
                            String id = String.valueOf(val.getMinuteTs().getTime());
                            String json = "{\"minute_ts\":" + val.getMinuteTs().getTime() + ",\"users\":" + val.getUsers() + "}";
                            IndexRequest req = Requests.indexRequest()
                                    .index("active_users_minute")
                                    .id(id)
                                    .source(json, XContentType.JSON);
                            indexer.add(req);
                        })
                        .setBulkFlushMaxActions(1)
                        .build()
        ).name("ES active_users_minute");

        // top_products_hourly index
        topProductHourlyRecords.sinkTo(
                new Elasticsearch7SinkBuilder<TopProductHourly>()
                        .setHosts(new HttpHost("localhost", 9200, "http"))
                        .setEmitter((val, ctx2, indexer) -> {
                            String id = val.getHourTs().getTime() + ":" + val.getProductId();
                            String json = "{\"hour_ts\":" + val.getHourTs().getTime() + ",\"product_id\":\"" + val.getProductId() + "\",\"amount\":" + val.getAmount() + "}";
                            IndexRequest req = Requests.indexRequest()
                                    .index("top_products_hourly")
                                    .id(id)
                                    .source(json, XContentType.JSON);
                            indexer.add(req);
                        })
                        .setBulkFlushMaxActions(1)
                        .build()
        ).name("ES top_products_hourly");

        // Step 5: Abandoned cart detection (processing-time timer)
        // Configurable timeout via env ABANDONED_TIMEOUT_MINUTES (default 30 minutes)
        DataStream<String> abandoned = events
                .filter(e -> e.getCartId() != null)
                .keyBy(EventEnvelope::getCartId)
                .process(new AbandonedCartProcessFunction())
                .name("abandoned-carts");

        // Sink abandoned carts to Redis list + counter
        abandoned.addSink(new RedisAbandonedSink()).name("redis-abandoned-carts");
        abandoned.map(id -> "abandoned_cart=" + id).returns(Types.STRING).name("print-abandoned").print();

        env.execute("CartPulse Multi-Event Routing + ActiveUsers5m");
    }
}
