package sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.JedisPooled;

public class RedisActiveUsersSink extends RichSinkFunction<Long> {
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
    public void invoke(Long value, Context context) {
        // HSET cartpulse:metrics active_users_5m <value>
        jedis.hset("cartpulse:metrics", "active_users_5m", String.valueOf(value));
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }
}

