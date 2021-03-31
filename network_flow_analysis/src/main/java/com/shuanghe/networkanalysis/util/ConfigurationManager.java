package com.shuanghe.networkanalysis.util;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Description:
 * Date: 2021-02-24
 * Time: 14:22
 *
 * @author yushu
 */
public class ConfigurationManager {
    private static final Logger logger = Logger.getLogger(ConfigurationManager.class);

    public static ParameterTool getFlinkConfig(String configName) throws IOException {
        if (StringUtilsPlus.isNotBlank(configName)) {
            return ParameterTool.fromPropertiesFile(configName);
        } else {
            throw new IOException(String.format(" configPath : %s is empty", configName));
        }
    }
}
