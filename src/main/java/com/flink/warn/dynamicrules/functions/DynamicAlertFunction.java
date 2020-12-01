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
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.flink.warn.dynamicrules.functions.ProcessingUtils.addToStateValuesSet;
import static com.flink.warn.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;


/** Implements main rule evaluation and alerting logic. */
@Slf4j
public class DynamicAlertFunction
    extends KeyedBroadcastProcessFunction<
        String, Keyed<Transaction, String, String>, Rule, Alert> {

  private static final String COUNT = "COUNT_FLINK";
  private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

  private static int WIDEST_RULE_KEY = Integer.MIN_VALUE;
  private static int CLEAR_STATE_COMMAND_KEY = Integer.MIN_VALUE + 1;

  private transient MapState<Long, Set<Transaction>> windowState;
  private Meter alertMeter;

  private String ruleId;

  private MapStateDescriptor<Long, Set<Transaction>> windowStateDescriptor =
      new MapStateDescriptor<>(
          "windowState",
          BasicTypeInfo.LONG_TYPE_INFO,
          TypeInformation.of(new TypeHint<Set<Transaction>>() {}));

  @Override
  public void open(Configuration parameters) {

    windowState = getRuntimeContext().getMapState(windowStateDescriptor);

    alertMeter = new MeterView(60);
    getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);
  }

  @Override
  public void processElement(
      Keyed<Transaction, String, String> value, ReadOnlyContext ctx, Collector<Alert> out)
      throws Exception {

    long currentEventTime = value.getWrapped().getEventTime();
    addToStateValuesSet(windowState, currentEventTime, value.getWrapped());

    long ingestionTime = value.getWrapped().getIngestionTimestamp();
    ctx.output(RulesEvaluator.Descriptors.latencySinkTag, System.currentTimeMillis() - ingestionTime);

    Rule rule = ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor).get(value.getId());
    if(ruleId == null){
      ruleId = rule.getRuleId();
    }
    if (noRuleAvailable(rule)) {
      return;
    }

    if (rule.getRuleState() == Rule.RuleState.ACTIVE) {
      Long windowStartForEvent = rule.getWindowStartFor(currentEventTime);

      long cleanupTime = (currentEventTime / 1000) * 1000;
      ctx.timerService().registerEventTimeTimer(cleanupTime);

      SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
      for (Long stateEventTime : windowState.keys()) {
        if (isStateValueInWindow(stateEventTime, windowStartForEvent, currentEventTime)) {
          aggregateValuesInState(stateEventTime, aggregator, rule);
        }
      }

      BigDecimal aggregateResult = aggregator.getLocalValue();
      boolean ruleResult = rule.apply(aggregateResult);

      ctx.output(
          RulesEvaluator.Descriptors.demoSinkTag, this.toString() + "-KEY="+value.getKey());

      if (ruleResult) {
        if (COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
          evictAllStateElements();
        }
        alertMeter.markEvent();
        out.collect(
            new Alert<>(
                rule.getRuleId(), rule, value.getKey(), value.getWrapped(), aggregateResult));
      }
    }
  }

  @Override
  public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out)
      throws Exception {
    BroadcastState<String, Rule> broadcastState =
        ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor);
    handleRuleBroadcast(rule, broadcastState);
    updateWidestWindowRule(rule, broadcastState);
    if (rule.getRuleState() == Rule.RuleState.CONTROL) {
      handleControlCommand(rule, broadcastState, ctx);
    }
  }

  private void handleControlCommand(
      Rule command, BroadcastState<String, Rule> rulesState, Context ctx) throws Exception {
    Rule.ControlType controlType = command.getControlType();
    switch (controlType) {
      case EXPORT_RULES_CURRENT:
        for (Entry<String, Rule> entry : rulesState.entries()) {
          ctx.output(RulesEvaluator.Descriptors.currentRulesSinkTag, entry.getValue());
        }
        break;
      case CLEAR_STATE_ALL:
        ctx.applyToKeyedState(windowStateDescriptor, (key, state) -> state.clear());
        break;
      case CLEAR_STATE_ALL_STOP:
        rulesState.remove("");
        break;
      case DELETE_RULES_ALL:
        Iterator<Entry<String, Rule>> entriesIterator = rulesState.iterator();
        while (entriesIterator.hasNext()) {
          Entry<String, Rule> ruleEntry = entriesIterator.next();
          rulesState.remove(ruleEntry.getKey());
        }
        break;
    }
  }

  private boolean isStateValueInWindow(
      Long stateEventTime, Long windowStartForEvent, long currentEventTime) {
    return stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime;
  }

  private void aggregateValuesInState(
      Long stateEventTime, SimpleAccumulator<BigDecimal> aggregator, Rule rule) throws Exception {
    Set<Transaction> inWindow = windowState.get(stateEventTime);
    if (COUNT.equals(rule.getAggregateFieldName())
        || COUNT_WITH_RESET.equals(rule.getAggregateFieldName())) {
      for (Transaction event : inWindow) {
        aggregator.add(BigDecimal.ONE);
      }
    } else {
      for (Transaction event : inWindow) {
        BigDecimal aggregatedValue =
            FieldsExtractor.getBigDecimalByName(rule.getAggregateFieldName(), event);
        aggregator.add(aggregatedValue);
      }
    }
  }

  private boolean noRuleAvailable(Rule rule) {
    // This could happen if the BroadcastState in this CoProcessFunction was updated after it was
    // updated and used in `DynamicKeyFunction`
    if (rule == null) {
      return true;
    }
    return false;
  }

  private void updateWidestWindowRule(Rule rule, BroadcastState<String, Rule> broadcastState)
      throws Exception {
    /*Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);
    if (widestWindowRule == null) {
      broadcastState.put(WIDEST_RULE_KEY, rule);
      return;
    }
    if (widestWindowRule != null && widestWindowRule.getRuleState() == Rule.RuleState.ACTIVE) {
      if (widestWindowRule.getWindowMillis() < rule.getWindowMillis()) {
        broadcastState.put(WIDEST_RULE_KEY, rule);
      }
    }*/
  }

  @Override
  public void onTimer(final long timestamp, final OnTimerContext ctx, final Collector<Alert> out)
      throws Exception {
    Rule widestWindowRule = ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor).get(ruleId);
    Optional<Long> cleanupEventTimeWindow =
        Optional.ofNullable(widestWindowRule).map(Rule::getWindowMillis);
    Optional<Long> cleanupEventTimeThreshold =
        cleanupEventTimeWindow.map(window -> timestamp - window);

    cleanupEventTimeThreshold.ifPresent(this::evictAgedElementsFromWindow);
  }

  private void evictAgedElementsFromWindow(Long threshold) {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        Long stateEventTime = keys.next();
        if (stateEventTime < threshold) {
          keys.remove();
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


  private void evictAllStateElements() {
    try {
      Iterator<Long> keys = windowState.keys().iterator();
      while (keys.hasNext()) {
        keys.next();
        keys.remove();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}