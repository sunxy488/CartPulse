package process;

import DTO.EventEnvelope;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AbandonedCartProcessFunction extends KeyedProcessFunction<String, EventEnvelope, String> {
    private transient ValueState<Boolean> added;
    private transient ValueState<Long> timerTs;
    private long timeoutMs;

    @Override
    public void open(Configuration parameters) {
        added = getRuntimeContext().getState(new ValueStateDescriptor<>("added", Boolean.class));
        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Long.class));
        int minutes;
        try {
            minutes = Integer.parseInt(System.getenv().getOrDefault("ABANDONED_TIMEOUT_MINUTES", "30"));
        } catch (NumberFormatException e) {
            minutes = 30;
        }
        timeoutMs = minutes * 60_000L;
    }

    @Override
    public void processElement(EventEnvelope e, Context ctx, Collector<String> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        String type = e.getEventType();
        if ("add_to_cart".equals(type)) {
            added.update(true);
            Long prev = timerTs.value();
            if (prev != null) {
                ctx.timerService().deleteProcessingTimeTimer(prev);
            }
            long ts = now + timeoutMs;
            ctx.timerService().registerProcessingTimeTimer(ts);
            timerTs.update(ts);
        } else if ("checkout".equals(type)) {
            Long ts = timerTs.value();
            if (ts != null) {
                ctx.timerService().deleteProcessingTimeTimer(ts);
            }
            added.clear();
            timerTs.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (Boolean.TRUE.equals(added.value())) {
            out.collect(ctx.getCurrentKey());
        }
        added.clear();
        timerTs.clear();
    }
}
