package com.shuanghe.loginfaildetect.process;

import com.shuanghe.loginfaildetect.model.LoginEvent;
import com.shuanghe.loginfaildetect.model.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yushu
 */
public class LoginFailWarningAdvanceResult extends KeyedProcessFunction<String, LoginEvent, LoginFailWarning> {
    private final int detectPeriod;
    private final int failTimes;

    public LoginFailWarningAdvanceResult(int detectPeriod, int failTimes) {
        this.detectPeriod = detectPeriod;
        this.failTimes = failTimes;
    }

    private ListState<LoginEvent> loginFailListState = null;

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
        if ("load".equals(value.getEventType())) {
            loginFailListState.add(value);

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

            if (lastFailTime - firstFailTime > detectPeriod * 1000L) {
                //删除过期的event
                List<LoginEvent> events = new ArrayList<>();
                long newFirstFailTime = Long.MAX_VALUE;
                for (LoginEvent event : list) {
                    if (lastFailTime - event.getTimestamp() <= detectPeriod * 1000L) {
                        events.add(event);
                        long ts = event.getTimestamp();
                        if (newFirstFailTime > ts) {
                            newFirstFailTime = ts;
                        }
                    }
                }
                firstFailTime = newFirstFailTime;
                loginFailListState.clear();
                loginFailListState.addAll(events);
            }

            if (list.size() >= failTimes) {
                out.collect(new LoginFailWarning(ctx.getCurrentKey(), firstFailTime, lastFailTime, String.format("%s秒内连续load%s次", detectPeriod, list.size())));
            }
        } else if ("start".equals(value.getEventType())) {
            //清空状态
            // TODO: 2021-04-03 乱序事件bug，需要只删除在他时间之前的load
            loginFailListState.clear();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        loginFailListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginFailListState", LoginEvent.class));

    }
}
