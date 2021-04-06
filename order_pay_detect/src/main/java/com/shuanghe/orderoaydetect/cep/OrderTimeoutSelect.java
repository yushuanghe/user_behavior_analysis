package com.shuanghe.orderoaydetect.cep;

import com.shuanghe.orderoaydetect.model.OrderEvent;
import com.shuanghe.orderoaydetect.model.OrderResult;
import org.apache.flink.cep.PatternTimeoutFunction;

import java.util.List;
import java.util.Map;

/**
 * Description:自定义 PatternTimeoutFunction
 * Date: 2021-04-06
 * Time: 17:15
 *
 * @author yushu
 */
public class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {
    /**
     * @param map
     * @param l   超时的时间
     * @return
     * @throws Exception
     */
    @Override
    public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
        String orderId = map.get("create").iterator().next().getOrderId();
        //超时map里只有create，没有pay
        int payListSize = map.containsKey("pay") ? map.get("pay").size() : 0;
        return new OrderResult(orderId, String.format("timeout:%s,payListSize:%s", l, payListSize));
    }
}
