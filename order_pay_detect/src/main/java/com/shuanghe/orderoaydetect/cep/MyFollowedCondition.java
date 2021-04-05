package com.shuanghe.orderoaydetect.cep;

import com.shuanghe.orderoaydetect.model.OrderEvent;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

/**
 * @author yushu
 */
public class MyFollowedCondition extends IterativeCondition<OrderEvent> {
    @Override
    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
        return "start".equals(value.getEventType());
    }
}
