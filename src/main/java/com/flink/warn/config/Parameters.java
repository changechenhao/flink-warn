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

package com.flink.warn.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;
import java.util.List;

public class Parameters {

  private final ParameterTool tool;

  public Parameters(ParameterTool tool) {
    this.tool = tool;
  }

  <T> T getOrDefault(Param<T> param) {
    if (!tool.has(param.getName())) {
      return param.getDefaultValue();
    }
    Object value;
    if (param.getType() == Integer.class) {
      value = tool.getInt(param.getName());
    } else if (param.getType() == Long.class) {
      value = tool.getLong(param.getName());
    } else if (param.getType() == Double.class) {
      value = tool.getDouble(param.getName());
    } else if (param.getType() == Boolean.class) {
      value = tool.getBoolean(param.getName());
    } else {
      value = tool.get(param.getName());
    }
    return param.getType().cast(value);
  }

  public static Parameters fromArgs(String[] args) {
    ParameterTool tool = ParameterTool.fromArgs(args);
    return new Parameters(tool);
  }


  /**
   * config file path
   */
  public static final Param<String> KAFKA_CONFIG_PATH = Param.string("kafka-config-path", "/home/task.properties");
  public static final Param<String> ES_CONFIG_PATH = Param.string("es-config-path", "/home/es.properties");

  /**
   * source type
   */
  public static final Param<String> RULES_SOURCE = Param.string("rules-source", "MONGODB");
  public static final Param<String> DATA_SOURCE = Param.string("data-source", "KAFKA");

  /**
   * Socket
   */
  public static final Param<Integer> SOCKET_PORT = Param.integer("pubsub-rules-export", 9999);
  public static final Param<Integer> RECORDS_PER_SECOND = Param.integer("records-per-second", 2);
  public static final Param<Boolean> LOCAL_EXECUTION = Param.bool("local", false);

  /**
   * source
   */
  public static final Param<Integer> SOURCE_PARALLELISM = Param.integer("source-parallelism", 2);

  /**
   * checkpoint
   */
  public static final Param<Integer> CHECKPOINT_INTERVAL =
      Param.integer("checkpoint-interval", 60_000_0);
  public static final Param<Integer> MIN_PAUSE_BETWEEN_CHECKPOINTS =
      Param.integer("min-pause-btwn-checkpoints", 60_000_0);
  public static final Param<Integer> OUT_OF_ORDERNESS = Param.integer("out-of-orderdness", 500);


  public static final List<Param<String>> STRING_PARAMS =
      Arrays.asList(
              KAFKA_CONFIG_PATH,
              ES_CONFIG_PATH,
              RULES_SOURCE,
              DATA_SOURCE);

  public static final List<Param<Integer>> INT_PARAMS =
      Arrays.asList(
          SOCKET_PORT,
          RECORDS_PER_SECOND,
          SOURCE_PARALLELISM,
          CHECKPOINT_INTERVAL,
          MIN_PAUSE_BETWEEN_CHECKPOINTS,
          OUT_OF_ORDERNESS);

  public static final List<Param<Boolean>> BOOL_PARAMS = Arrays.asList(LOCAL_EXECUTION);
}
