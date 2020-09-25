package com.flink.warn.util;

import com.flink.warn.entiy.ElasticsearchConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @Author : chenhao
 * @Date : 2020/9/4 0004 9:54
 */
public class ElasticSearchSinkUtil {


    /**
     * es sink
     *
     * @param data 数据
     * @param func
     * @param <T>
     */
    public static <T> void addSink(ElasticsearchConfig config, String sinkName,
                                   SingleOutputStreamOperator<T> data, ElasticsearchSinkFunction<T> func) {
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(config.getEsAddresses(), func);
        if(Objects.nonNull(config.getBulkSize())){
            esSinkBuilder.setBulkFlushMaxActions(config.getBulkSize());
        }

        if(Objects.nonNull(config.getInterval())){
            esSinkBuilder.setBulkFlushInterval(config.getInterval());
        }

        data.addSink(esSinkBuilder.build()).name(sinkName).setParallelism(config.getSinkParallelism());
    }

    /**
     * 解析配置文件的 es hosts
     *
     * @param hosts
     * @return
     * @throws MalformedURLException
     */
    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }

}
