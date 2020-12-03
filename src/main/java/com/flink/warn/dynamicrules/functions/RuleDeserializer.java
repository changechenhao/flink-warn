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

package com.flink.warn.dynamicrules.functions;

import com.flink.warn.dynamicrules.entity.WarnRule;
import com.flink.warn.dynamicrules.RuleParser;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class RuleDeserializer extends RichFlatMapFunction<String, WarnRule> {

  private RuleParser ruleParser;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    ruleParser = new RuleParser();
  }

  @Override
  public void flatMap(String value, Collector<WarnRule> out) throws Exception {
    try {
      WarnRule warnRule = ruleParser.fromString(value);
      if (warnRule.getRuleState() != WarnRule.RuleState.CONTROL && warnRule.getRuleId() == null) {
        throw new NullPointerException("ruleId cannot be null: " + warnRule.toString());
      }
      out.collect(warnRule);
    } catch (Exception e) {
    }
  }
}
