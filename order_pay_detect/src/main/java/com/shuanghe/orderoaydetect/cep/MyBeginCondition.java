package com.shuanghe.orderoaydetect.cep;

import com.shuanghe.orderoaydetect.model.OrderEvent;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

/**
 * @author yushu
 */
public class MyBeginCondition extends IterativeCondition<OrderEvent> {
    @Override
    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
        return "load".equals(value.getEventType());
    }
}
