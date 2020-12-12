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

import com.flink.warn.config.Config;
import com.flink.warn.dynamicrules.entity.WarnRule;
import com.flink.warn.dynamicrules.functions.RuleDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.flink.warn.config.Parameters.RULES_SOURCE;
import static com.flink.warn.config.Parameters.SOCKET_PORT;

public class RulesSource {

  private static final int RULES_STREAM_PARALLELISM = 1;

  public static SourceFunction<String> createRulesSource(Config config) throws IOException {

    String sourceType = config.get(RULES_SOURCE);
    RulesSource.Type rulesSourceType = RulesSource.Type.valueOf(sourceType.toUpperCase());

    switch (rulesSourceType) {
      case SOCKET:
        return new SocketTextStreamFunction("localhost", config.get(SOCKET_PORT), "\n", -1);
      default:
        throw new IllegalArgumentException(
            "Source \"" + rulesSourceType + "\" unknown. Known values are:" + Type.values());
    }
  }

  public static DataStream<WarnRule> stringsStreamToRules(DataStream<String> ruleStrings) {
    return ruleStrings
        .flatMap(new RuleDeserializer())
        .name("WarnRule Deserialization")
        .setParallelism(RULES_STREAM_PARALLELISM)
        .assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor<WarnRule>(Time.of(0, TimeUnit.MILLISECONDS)) {
              @Override
              public long extractTimestamp(WarnRule element) {
                // Prevents connected data+update stream watermark stalling.
                return Long.MAX_VALUE;
              }
            });
  }

  public enum Type {
    KAFKA("Rules Source (Kafka)"),
    PUBSUB("Rules Source (Pub/Sub)"),
    MONGODB("Rules Source (Mongodb)"),
    SOCKET("Rules Source (Socket)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
