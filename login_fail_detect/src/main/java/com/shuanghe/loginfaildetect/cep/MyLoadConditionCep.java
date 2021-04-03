package com.shuanghe.loginfaildetect.cep;

import com.shuanghe.loginfaildetect.model.LoginEvent;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

/**
 * @author yushu
 */
public class MyLoadConditionCep extends IterativeCondition<LoginEvent> {
    @Override
    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
        return "load".equals(value.getEventType());
    }
}
