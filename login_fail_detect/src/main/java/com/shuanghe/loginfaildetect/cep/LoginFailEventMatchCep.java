package com.shuanghe.loginfaildetect.cep;

import com.shuanghe.loginfaildetect.model.LoginEvent;
import com.shuanghe.loginfaildetect.model.LoginFailWarning;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;

public class LoginFailEventMatchCep implements PatternSelectFunction<LoginEvent, LoginFailWarning> {
    /**
     * @param pattern key：检测的名称
     *                value：检测到的事件
     * @return
     * @throws Exception
     */
    @Override
    public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
        LoginEvent firstFailEvent = pattern.get("load1").get(0);
        LoginEvent lastFailEvent = pattern.get("load3").iterator().next();
        return new LoginFailWarning(firstFailEvent.getUid(), firstFailEvent.getTimestamp(),
                lastFailEvent.getTimestamp(), "cep 检测");
    }
}
