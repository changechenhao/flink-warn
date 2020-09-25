package com.flink.warn.entiy;

import com.flink.warn.config.PropertiesConfig;
import com.flink.warn.util.ElasticSearchSinkUtil;
import org.apache.http.HttpHost;

import java.net.MalformedURLException;
import java.util.List;

/**
 * @Author : chenhao
 * @Date : 2020/9/4 0004 10:31
 */
public class ElasticsearchConfig {

    public static final String ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "elasticsearch.bulk.flush.max.actions";
    public static final String ELASTICSEARCH_HOSTS = "elasticsearch.hosts";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final String BULK_FLUSH_INTERVAL_MS = "bulk.flush.interval.ms";

    private List<HttpHost> esAddresses;

    private Integer bulkSize;

    private Integer sinkParallelism;

    private Integer interval;

    public ElasticsearchConfig(List<HttpHost> esAddresses, Integer sinkParallelism) {
        this.esAddresses = esAddresses;
        this.sinkParallelism = sinkParallelism;
    }

    public List<HttpHost> getEsAddresses() {
        return esAddresses;
    }

    public void setEsAddresses(List<HttpHost> esAddresses) {
        this.esAddresses = esAddresses;
    }

    public static String getElasticsearchBulkFlushMaxActions() {
        return ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS;
    }

    public static String getElasticsearchHosts() {
        return ELASTICSEARCH_HOSTS;
    }

    public static String getStreamSinkParallelism() {
        return STREAM_SINK_PARALLELISM;
    }

    public static String getBulkFlushIntervalMs() {
        return BULK_FLUSH_INTERVAL_MS;
    }

    public Integer getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(Integer bulkSize) {
        this.bulkSize = bulkSize;
    }

    public Integer getSinkParallelism() {
        return sinkParallelism;
    }

    public void setSinkParallelism(Integer sinkParallelism) {
        this.sinkParallelism = sinkParallelism;
    }

    public Integer getInterval() {
        return interval;
    }

    public void setInterval(Integer interval) {
        this.interval = interval;
    }

    public static ElasticsearchConfig create(PropertiesConfig propertiesConfig) throws MalformedURLException {
        List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddresses(propertiesConfig.getString(ELASTICSEARCH_HOSTS));
        int parallelism = propertiesConfig.getIntValue(STREAM_SINK_PARALLELISM);
        int interval = propertiesConfig.getIntValue(BULK_FLUSH_INTERVAL_MS);
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(esAddresses, parallelism);
        elasticsearchConfig.setInterval(interval);
        return elasticsearchConfig;
    }

}
