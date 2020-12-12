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

import com.flink.warn.dynamicrules.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map.Entry;

import static com.flink.warn.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;


/** Implements dynamic data partitioning based on a set of broadcasted rules. */
@Slf4j
public class DynamicKeyFunction
    extends BroadcastProcessFunction<Transaction, WarnRule, Keyed<Transaction, String, String>> {

  private RuleCounterGauge ruleCounterGauge;

  @Override
  public void open(Configuration parameters) {
    ruleCounterGauge = new RuleCounterGauge();
    getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGauge);
  }

  @Override
  public void processElement(
      Transaction event, ReadOnlyContext ctx, Collector<Keyed<Transaction, String, String>> out)
      throws Exception {
    ReadOnlyBroadcastState<String, WarnRule> rulesState =
        ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor);
    forkEventForEachGroupingKey(event, rulesState, out, ctx);
  }

  private void forkEventForEachGroupingKey(
      Transaction event,
      ReadOnlyBroadcastState<String, WarnRule> rulesState,
      Collector<Keyed<Transaction, String, String>> out, ReadOnlyContext ctx)
      throws Exception {
    int ruleCounter = 0;
    for (Entry<String, WarnRule> entry : rulesState.immutableEntries()) {
      final WarnRule warnRule = entry.getValue();
      String key = KeysExtractor.getKey(warnRule.getGroupingKeyNames(), event, warnRule.getRuleId());
      Keyed keyed = new Keyed(event, key, warnRule.getRuleId());
      out.collect(keyed);
      ruleCounter++;
    }
    ruleCounterGauge.setValue(ruleCounter);
  }

  @Override
  public void processBroadcastElement(
          WarnRule warnRule, Context ctx, Collector<Keyed<Transaction, String, String>> out) throws Exception {
//    log.info("{}", warnRule);
    BroadcastState<String, WarnRule> broadcastState =
        ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor);
    handleRuleBroadcast(warnRule, broadcastState);
    if (warnRule.getRuleState() == WarnRule.RuleState.CONTROL) {
      handleControlCommand(warnRule.getControlType(), broadcastState);
    }
  }

  private void handleControlCommand(
          WarnRule.ControlType controlType, BroadcastState<String, WarnRule> rulesState) throws Exception {
    switch (controlType) {
      case DELETE_RULES_ALL:
        Iterator<Entry<String, WarnRule>> entriesIterator = rulesState.iterator();
        while (entriesIterator.hasNext()) {
          Entry<String, WarnRule> ruleEntry = entriesIterator.next();
          rulesState.remove(ruleEntry.getKey());
          log.info("Removed WarnRule {}", ruleEntry.getValue());
        }
        break;
    }
  }

  private static class RuleCounterGauge implements Gauge<Integer> {

    private int value = 0;

    public void setValue(int value) {
      this.value = value;
    }

    @Override
    public Integer getValue() {
      return value;
    }
  }
}
