package com.shuanghe.orderoaydetect.cep;

import com.shuanghe.orderoaydetect.model.OrderEvent;
import com.shuanghe.orderoaydetect.model.OrderResult;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;

/**
 * Description:自定义 PatternSelectFunction
 * Date: 2021-04-06
 * Time: 17:16
 *
 * @author yushu
 */
public class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {
    @Override
    public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
        String orderId = map.get("pay").get(0).getOrderId();
        return new OrderResult(orderId, String.format("payed successfully"));
    }
}
