package com.shuanghe.loginfaildetect.process;

import com.shuanghe.loginfaildetect.model.LoginEvent;
import com.shuanghe.loginfaildetect.model.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class LoginFailWarningResult extends KeyedProcessFunction<String, LoginEvent, LoginFailWarning> {
    private final int detectPeriod;
    private final int failTimes;

    public LoginFailWarningResult(int detectPeriod, int failTimes) {
        this.detectPeriod = detectPeriod;
        this.failTimes = failTimes;
    }

    /**
     * 当前所有load事件，定时器时间戳
     */
    private ListState<LoginEvent> loginFailListState = null;
    private ValueState<Long> loginFailStartTs = null;
    private ValueState<Long> timerTsState = null;

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
        if ("load".equals(value.getEventType())) {
            loginFailListState.add(value);
            if (loginFailStartTs.value() == null) {
                loginFailStartTs.update(value.getTimestamp());
                //注册定时器
                long ts = value.getTimestamp() + detectPeriod * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerTsState.update(ts);
            }
        } else if ("start".equals(value.getEventType())) {
            //删除定时器，清空状态
            ctx.timerService().deleteEventTimeTimer(timerTsState.value() == null ? 0 : timerTsState.value());
            loginFailListState.clear();
            loginFailStartTs.clear();
            timerTsState.clear();

            out.collect(new LoginFailWarning("-1",
                    //loginFailStartTs.value()
                    0
                    ,
                    //timerTsState.value()
                    0
                    ,
                    loginFailListState.get().toString()));
        }
    }

    /**
     * 定时器时效性低
     *
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        List<LoginEvent> list = new ArrayList<>();
        long firstFailTime = Long.MAX_VALUE;
        long lastFailTime = Long.MIN_VALUE;
        for (LoginEvent event : loginFailListState.get()) {
            list.add(event);
            long ts = event.getTimestamp();
            if (firstFailTime > ts) {
                firstFailTime = ts;
            }
            if (lastFailTime < ts) {
                lastFailTime = ts;
            }
        }
        if (list.size() >= failTimes) {
            out.collect(new LoginFailWarning(ctx.getCurrentKey(), firstFailTime, lastFailTime, String.format("%s秒内连续load%s次", detectPeriod, list.size())));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        loginFailListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginFailListState", LoginEvent.class));
        loginFailStartTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("loginFailStartTs", Long.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState", Long.class));
    }
}
