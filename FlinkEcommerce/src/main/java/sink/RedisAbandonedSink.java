package sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.JedisPooled;

public class RedisAbandonedSink extends RichSinkFunction<String> {
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
    public void invoke(String cartId, Context context) {
        // List for IDs and a counter for quick metric
        jedis.lpush("cartpulse:abandoned_carts", cartId);
        jedis.incr("cartpulse:abandoned_carts_count");
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }
}

