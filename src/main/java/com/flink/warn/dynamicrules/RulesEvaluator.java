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
import com.flink.warn.config.Config;
import com.flink.warn.config.PropertiesConfig;
import com.flink.warn.dynamicrules.functions.DynamicAlertFunction2;
import com.flink.warn.dynamicrules.functions.DynamicKeyFunction2;
import com.flink.warn.dynamicrules.sources.RulesSource;
import com.flink.warn.dynamicrules.sources.TransactionsSource;
import com.flink.warn.entiy.ElasticsearchConfig;
import com.flink.warn.util.ElasticSearchSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.OutputTag;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.flink.warn.config.Parameters.*;

@Slf4j
public class RulesEvaluator {

  private Config config;

  RulesEvaluator(Config config) {
    this.config = config;
  }

  public void run() throws Exception {

    RulesSource.Type rulesSourceType = getRulesSourceType();

    boolean isLocal = config.get(LOCAL_EXECUTION);

    // Environment setup
    StreamExecutionEnvironment env = configureStreamExecutionEnvironment(rulesSourceType, isLocal);

    // Streams setup
    DataStream<WarnRule> rulesUpdateStream = getRulesUpdateStream(env);
    DataStream<JSONObject> transactions = getTransactionsStream(env);

    BroadcastStream<WarnRule> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);

    // Processing pipeline setup

    SingleOutputStreamOperator<JSONObject> alerts = transactions
            .connect(rulesStream)
            .process(new DynamicKeyFunction2())
            .uid("DynamicKeyFunction")
            .name("Dynamic Partitioning Function")
            .keyBy((keyed) -> keyed.getKey())
            .connect(rulesStream)
            .process(new DynamicAlertFunction2())
            .uid("DynamicAlertFunction")
            .name("Dynamic WarnRule Evaluation Function");

    DataStream<String> allRuleEvaluations = alerts.getSideOutput(Descriptors.demoSinkTag);
    allRuleEvaluations.print().setParallelism(1).name("WarnRule Evaluation Sink");

    DataStream<String> statisticsStream = alerts.getSideOutput(Descriptors.statisticsSinkTag);
    statisticsStream.print().setParallelism(1);

    alerts.print().name("Alert STDOUT Sink");

    DataStream<JSONObject> warnStream = alerts.getSideOutput(Descriptors.warnSinkTag);
    String esFilePath = "/home/es.properties";
    ElasticsearchConfig esConfig = ElasticsearchConfig.create(new PropertiesConfig(esFilePath));
    ElasticSearchSinkUtil.addSink(esConfig, "es-warn", alerts,
            (JSONObject result, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
              requestIndexer.add(Requests.indexRequest()
                      .index("warn")
                      .source(JSONObject.toJSONBytes(result), XContentType.JSON));
            });

//    DataStream<String> alertsJson = AlertsSink.alertsStreamToJson(alerts);
//    DataStream<String> currentRulesJson = CurrentRulesSink.rulesStreamTo{ "ruleId": 1, "ruleState": "ACTIVE", "groupingKeyNames": ["paymentType"], "unique": [], "aggregateFieldName": "paymentAmount", "aggregatorFunctionType": "SUM","limitOperatorType": "GREATER","limit": 200, "windowMinutes": 1}Json(currentRules);

//    currentRulesJson.print();

/*    alertsJson
        .addSink(AlertsSink.createAlertsSink(config))
        .setParallelism(1)
        .name("Alerts JSON Sink");*/
//    currentRulesJson.addSink(CurrentRulesSink.createRulesSink(config)).setParallelism(1);

   /* DataStream<String> latencies =
        latency
            .timeWindowAll(Time.seconds(10))
            .aggregate(new AverageAggregate())
            .map(String::valueOf);
    latencies.addSink(LatencySink.createLatencySink(config));*/

    env.execute("Fraud Detection Engine");
  }

  private DataStream<JSONObject> getTransactionsStream(StreamExecutionEnvironment env) {
    // Data stream setup
    SourceFunction<String> transactionSource = TransactionsSource.createTransactionsSource(config);
    int sourceParallelism = config.get(SOURCE_PARALLELISM);
    DataStream<String> transactionsStringsStream =
        env.addSource(transactionSource)
            .name("Transactions Source")
            .setParallelism(sourceParallelism);
    DataStream<JSONObject> transactionsStream =
        TransactionsSource.stringsStreamToJSONObject(transactionsStringsStream);
    SimpleBoundedOutOfOrdernessTimestampExtractor<JSONObject> extractor = new SimpleBoundedOutOfOrdernessTimestampExtractor<>(config.get(OUT_OF_ORDERNESS));
    return transactionsStream.assignTimestampsAndWatermarks(extractor);
  }

  private DataStream<WarnRule> getRulesUpdateStream(StreamExecutionEnvironment env) throws IOException {

    RulesSource.Type rulesSourceEnumType = getRulesSourceType();

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

 /* private static class SimpleBoundedOutOfOrdernessTimestampExtractor<T extends Transaction>
      extends BoundedOutOfOrdernessTimestampExtractor<T> {

    public SimpleBoundedOutOfOrdernessTimestampExtractor(int outOfOrderdnessMillis) {
      super(Time.of(outOfOrderdnessMillis, TimeUnit.MILLISECONDS));
    }

    @Override
    public long extractTimestamp(T element) {
      return element.getEventTime();
    }
  }*/

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

    public static final OutputTag<String> demoSinkTag = new OutputTag<String>("demo-sink") {};
    public static final OutputTag<String> statisticsSinkTag = new OutputTag<String>("statistics-sink") {};
    public static final OutputTag<JSONObject> warnSinkTag = new OutputTag<JSONObject>("warn-sink") {};
    public static final OutputTag<Long> latencySinkTag = new OutputTag<Long>("latency-sink") {};
    public static final OutputTag<WarnRule> currentRulesSinkTag =
        new OutputTag<WarnRule>("current-rules-sink") {};

  }
}
