package sink;

import DTO.TopNWindowResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;

public class RedisTopNZSetSink extends RichSinkFunction<TopNWindowResult> {
    private transient JedisPooled jedis;

    @Override
    public void open(Configuration parameters) {
        String host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int port;
        try {
            port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        } catch (NumberFormatException e) {
            port = 6379;
        }
        jedis = new JedisPooled(host, port);
    }

    @Override
    public void invoke(TopNWindowResult value, Context context) {
        String key = "cartpulse:top_products_hourly:" + value.getHourKey();
        Pipeline p = jedis.pipelined();
        p.del(key);
        if (value.getTopN() != null) {
            value.getTopN().forEach(t -> p.zadd(key, t.f1, t.f0));
        }
        p.sync();
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }
}

