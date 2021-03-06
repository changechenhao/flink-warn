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

package com.flink.warn.dynamicrules.sources;

import com.alibaba.fastjson.JSONObject;
import com.flink.warn.config.Config;
import com.flink.warn.dynamicrules.KafkaUtils;
import com.flink.warn.dynamicrules.functions.JSONObjectGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

import static com.flink.warn.config.Parameters.RECORDS_PER_SECOND;
import static com.flink.warn.config.Parameters.DATA_SOURCE;

public class LogSource {

  public static SourceFunction<String> createTransactionsSource(Config config) {

    String sourceType = config.get(DATA_SOURCE);
    LogSource.Type transactionsSourceType =
        LogSource.Type.valueOf(sourceType.toUpperCase());

    int transactionsPerSecond = config.get(RECORDS_PER_SECOND);

    switch (transactionsSourceType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String topic = kafkaProps.getProperty("toptic");
        FlinkKafkaConsumer010<String> kafkaConsumer =
            new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
      default:
        return new JSONObjectGenerator(transactionsPerSecond);
    }
  }

  public static DataStream<JSONObject> stringsStreamToJSONObject(
          DataStream<String> transactionStrings) {
    return transactionStrings
            .flatMap(new FlatMapFunction<String, JSONObject>() {
              @Override
              public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                out.collect(JSONObject.parseObject(value));
              }
            })
            .returns(JSONObject.class)
            .name("Transactions Deserialization");
  }

  public enum Type {
    GENERATOR("Transactions Source (generated locally)"),
    KAFKA("Transactions Source (Kafka)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
