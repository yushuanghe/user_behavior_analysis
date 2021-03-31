package com.shuanghe.networkanalysis.process;

import com.shuanghe.networkanalysis.Bloom;
import com.shuanghe.networkanalysis.model.RawData6Event;
import com.shuanghe.networkanalysis.model.UvCount;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 * Description:
 * Date: 2021-03-30
 * Time: 21:46
 *
 * @author yushu
 */
public class UvCountWithBloom extends ProcessWindowFunction<RawData6Event, UvCount, String, TimeWindow> {
    /**
     * 定义redis连接以及布隆过滤器
     * 连接池
     */
    private Jedis jedis = null;
    /**
     * bit的个数
     * 2^7(128)*2^20(m)*2^3(8bit)=128m
     */
    private final Bloom bloomFilter = new Bloom(1L << 30);

    /**
     * 正常收集齐所有数据，窗口触发计算的时候才会调用
     * 因为自定义触发器，每来一条数据调用一次
     *
     * @param s
     * @param context
     * @param elements
     * @param out
     * @throws Exception
     */
    @Override
    public void process(String s, Context context, Iterable<RawData6Event> elements, Collector<UvCount> out) throws Exception {
        //定义redis中存储bitmap的key
        String storedBitMapKey = String.valueOf(context.window().getEnd());

        /*
        将当前窗口的uv count值，作为状态保存到redis里
        用一个hash表来保存<windowEnd,count>
         */
        String uvCountMap = "uv_count";
        String currentKey = String.valueOf(context.window().getEnd());
        long count = 0;
        //从redis中取出当前窗口的uv count值
        if (jedis.hget(uvCountMap, currentKey) != null) {
            count = Long.parseLong(jedis.hget(uvCountMap, currentKey));
        }

        //去重，判断当前uid的hash值对应的bitmap位置，是否为0
        String uid = elements.iterator().next().getUid();
        //计算hash值，对应着bitmap中的偏移量
        long offset = bloomFilter.hash(uid, 666);
        //用redis的位操作命令，取bitmap中对应位的值
        boolean isExist = jedis.getbit(storedBitMapKey, offset);

        if (!isExist) {
            //key不存在，bitmap对应位置置1，count值加1
            jedis.setbit(storedBitMapKey, offset, true);
            jedis.hset(uvCountMap, currentKey, String.valueOf(count + 1));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = new Jedis("localhost", 6379);
        jedis.auth("123456");
    }
}
