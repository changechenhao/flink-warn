/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flink.warn.dynamicrules;

import com.alibaba.fastjson.JSONObject;
import com.flink.warn.CustomEventTimeTrigger;
import com.flink.warn.config.Config;
import com.flink.warn.config.PropertiesConfig;
import com.flink.warn.dynamicrules.entity.OriginalEvent;
import com.flink.warn.dynamicrules.entity.WarnRule;
import com.flink.warn.dynamicrules.functions.CountAggregateFunction;
import com.flink.warn.dynamicrules.functions.DynamicAlertFunction;
import com.flink.warn.dynamicrules.functions.DynamicKeyFunction;
import com.flink.warn.dynamicrules.functions.MongoDBStreamFunction;
import com.flink.warn.dynamicrules.sources.RulesSource;
import com.flink.warn.dynamicrules.sources.TransactionsSource;
import com.flink.warn.entiy.ElasticsearchConfig;
import com.flink.warn.entiy.WorkList;
import com.flink.warn.sink.WorkListToMongodbSink;
import com.flink.warn.util.DateUtil;
import com.flink.warn.util.ElasticSearchSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.flink.warn.config.Parameters.*;

@Slf4j
public class RulesEvaluator {

    private Config config;

    private static final String[] keyByFields = {"srcIp", "dstIp", "srcPort", "dstPort", "srcCountry", "dstCountry"
            , "srcProProvince", "dstProProvince", "srcCity", "dstCity", "deviceIp", "logType", "eventType"
            , "eventSubType", "eventName", "level", "protocol", "assetType", "assetSubType"};

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    RulesEvaluator(Config config) {
        this.config = config;
    }

    public void run() throws Exception {

        /**
         * 配置读取与环境配置
         */
        RulesSource.Type rulesSourceType = getRulesSourceType();
        boolean isLocal = config.get(LOCAL_EXECUTION);
        String esFilePath = "/home/es.properties";
        ElasticsearchConfig esConfig = ElasticsearchConfig.create(new PropertiesConfig(esFilePath));
        StreamExecutionEnvironment env = configureStreamExecutionEnvironment(rulesSourceType, isLocal);

        /**
         * 获取告警规则、日志等流
         */
        DataStream<JSONObject> transactions = getLogStream(env);
        DataStream<WarnRule> rulesUpdateStream = getRulesUpdateStream(env);
        BroadcastStream<WarnRule> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);

        /**
         * 数据分流，当前分流方式为复制，以用于不同业务
         */
        SingleOutputStreamOperator<JSONObject> streamOperator = transactions
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        out.collect(value);
//                        ctx.output(Descriptors.statisticsTag, value);
                        OriginalEvent event = JSONObject.parseObject(value.toJSONString(), OriginalEvent.class);
                        ctx.output(Descriptors.statisticsTag, event);
                    }
                }).name("side-output");


        /**
         * 告警处理，根据告警规则产生告警，写入ES
         */
        SingleOutputStreamOperator<JSONObject> warnStream = streamOperator
                .connect(rulesStream)
                .process(new DynamicKeyFunction())
                .uid("DynamicKeyFunction")
                .name("Dynamic Partitioning Function")
                .keyBy((keyed) -> keyed.getKey())
                .connect(rulesStream)
                .process(new DynamicAlertFunction())
                .uid("DynamicAlertFunction")
                .name("Dynamic WarnRule Evaluation Function");
        ElasticSearchSinkUtil.addSink(esConfig, "es-warn", warnStream,
                (JSONObject result, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index("warn")
                            .source(JSONObject.toJSONBytes(result), XContentType.JSON));
                });
        DataStream<String> allRuleEvaluations = warnStream.getSideOutput(Descriptors.allRuleEvaluationsTag);
        allRuleEvaluations.print().setParallelism(1).name("WarnRule Evaluation Sink");


        /**
         * 统计聚合, 聚合每分钟的相同事件存于ES中
         */
        SingleOutputStreamOperator<JSONObject> staisticsStream = streamOperator.getSideOutput(Descriptors.statisticsTag)
                .keyBy(keyByFields)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.minutes(1)))
                .trigger(CustomEventTimeTrigger.create())
                .aggregate(new CountAggregateFunction(), new ProcessWindowFunction<Long, JSONObject, Tuple, TimeWindow>() {

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Long> elements, Collector<JSONObject> out) throws Exception {

                        Iterator<Long> iterator = elements.iterator();
                        Long count = iterator.next();
                        long currentTimeMillis = System.currentTimeMillis();
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        JSONObject result = new JSONObject();
                        for (int i = 0; i < keyByFields.length; i++) {
                            Object value = tuple.getField(i);
                            if (Objects.nonNull(value)) {
                                result.put(keyByFields[i], value);
                            }
                        }
                        result.put("startTime", DateUtil.formatTime(start, formatter));
                        result.put("endTime", DateUtil.formatTime(end, formatter));
                        result.put("createTime", DateUtil.formatTime(currentTimeMillis, formatter));
                        result.put("count", count);
                        out.collect(result);
                    }
                }).name("statistics-task");
        ElasticSearchSinkUtil.addSink(esConfig, "es-statistics", staisticsStream,
                (JSONObject result, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index("event-statistics")
                            .source(JSONObject.toJSONBytes(result), XContentType.JSON));
                });

        /**
         * 根据告警生成工单，写入MongoDB
         */
        streamOperator.getSideOutput(Descriptors.workListTag)
                .addSink(new WorkListToMongodbSink())
                .name("work_list");


        /**
         * 事件聚合
         */
        DataStream<JSONObject> statisticsStream = streamOperator.getSideOutput(Descriptors.eventTag);
//        statisticsStream.print().setParallelism(1);
//        statisticsStream.keyBy();

        env.execute("Fraud Detection Engine");
    }

    private DataStream<JSONObject> getLogStream(StreamExecutionEnvironment env) {
        // Data stream setup
        SourceFunction<String> logSource = TransactionsSource.createTransactionsSource(config);
        int sourceParallelism = config.get(SOURCE_PARALLELISM);
        DataStream<String> logStringsStream =
                env.addSource(logSource)
                        .name("Log Source")
                        .setParallelism(sourceParallelism);
        DataStream<JSONObject> logStream =
                TransactionsSource.stringsStreamToJSONObject(logStringsStream);
        SimpleBoundedOutOfOrdernessTimestampExtractor<JSONObject> extractor = new SimpleBoundedOutOfOrdernessTimestampExtractor<>(config.get(OUT_OF_ORDERNESS));
        return logStream.assignTimestampsAndWatermarks(extractor);
    }

    private DataStream<WarnRule> getRulesUpdateStream(StreamExecutionEnvironment env) throws IOException {
        RulesSource.Type rulesSourceEnumType = getRulesSourceType();
        if (rulesSourceEnumType == RulesSource.Type.MONGODB) {
            return env.addSource(new MongoDBStreamFunction("warn_rule", WarnRule.class))
                    .name(rulesSourceEnumType.getName()).setParallelism(1);
        }
        SourceFunction<String> rulesSource = RulesSource.createRulesSource(config);
        DataStream<String> rulesStrings =
                env.addSource(rulesSource).name(rulesSourceEnumType.getName()).setParallelism(1);
        return RulesSource.stringsStreamToRules(rulesStrings);
    }

    private RulesSource.Type getRulesSourceType() {
        String rulesSource = config.get(RULES_SOURCE);
        return RulesSource.Type.valueOf(rulesSource.toUpperCase());
    }

    private StreamExecutionEnvironment configureStreamExecutionEnvironment(
            RulesSource.Type rulesSourceEnumType, boolean isLocal) {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        StreamExecutionEnvironment env =
                isLocal
                        ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
                        : StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointInterval(config.get(CHECKPOINT_INTERVAL));
        env.getCheckpointConfig()
                .setMinPauseBetweenCheckpoints(config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS));

        configureRestartStrategy(env, rulesSourceEnumType);
        return env;
    }

    private static class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends JSONObject>
            extends BoundedOutOfOrdernessTimestampExtractor<T> {

        public SimpleBoundedOutOfOrdernessTimestampExtractor(int outOfOrderdnessMillis) {
            super(Time.of(outOfOrderdnessMillis, TimeUnit.MILLISECONDS));
        }

        @Override
        public long extractTimestamp(T element) {
            return element.getLongValue("startTime");
        }
    }

    private void configureRestartStrategy(
            StreamExecutionEnvironment env, RulesSource.Type rulesSourceEnumType) {
        switch (rulesSourceEnumType) {
            case SOCKET:
                env.setRestartStrategy(
                        RestartStrategies.fixedDelayRestart(
                                10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
                break;
            case KAFKA:
                // Default - unlimited restart strategy.
                //        env.setRestartStrategy(RestartStrategies.noRestart());
        }
    }

    public static class Descriptors {
        public static final MapStateDescriptor<String, WarnRule> rulesDescriptor =
                new MapStateDescriptor<>(
                        "rules", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(WarnRule.class));

        public static final OutputTag<String> allRuleEvaluationsTag = new OutputTag<String>("all-rule-evaluations-tag") {
        };
        public static final OutputTag<OriginalEvent> statisticsTag = new OutputTag<OriginalEvent>("statistics-sink") {
        };
        public static final OutputTag<JSONObject> eventTag = new OutputTag<JSONObject>("event-tag") {
        };
        public static final OutputTag<WorkList> workListTag = new OutputTag<WorkList>("worklist-tag") {
        };
        public static final OutputTag<WarnRule> currentRulesSinkTag =
                new OutputTag<WarnRule>("current-rules-sink") {
                };

    }
}
