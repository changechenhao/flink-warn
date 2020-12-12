package com.flink.warn.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

/**
 * @Author : chenhao
 * @Date : 2020/8/18 0018 9:45
 */
public class PropertiesConfig {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesConfig.class);

    private Properties properties;

    public  String filePath;

    public PropertiesConfig(String filePath){
        this.filePath = filePath;
        try {
            logger.info("开始加载{}配置文件", filePath);
            load();
        } catch (IOException e) {
            logger.error("配置文件{}加载失败", filePath, e);
        }
        logger.info("配置文件加载成功");
    }

    private void load() throws IOException {
        File file = new File(filePath);
        FileInputStream fileInputStream = new FileInputStream(file);
        properties = new Properties();
        properties.load(fileInputStream);
    }

    public String getString(String key){
        return Optional.ofNullable(properties.getProperty(key)).orElseGet(() -> "");
    }

    public String getString(String key1, String key2){
        return Optional.ofNullable(properties.getProperty(key1 + key2)).orElseGet(() -> "");
    }

    public int getIntValue(String key){
        return Optional.of(properties.getProperty(key))
                .map(item -> Integer.parseInt(item))
                .orElseGet(() -> 0);
    }

    public long getLongValue(String key){
        return Optional.of(properties.getProperty(key))
                .map(item -> Long.parseLong(item))
                .orElseGet(() -> 0L);
    }

    public Properties getProperties() {
        return properties;
    }
}
