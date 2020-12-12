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

import com.alibaba.fastjson.JSONObject;
import com.flink.warn.dynamicrules.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import static com.flink.warn.dynamicrules.functions.ProcessingUtils.handleRuleBroadcast;


/**
 * Implements dynamic data partitioning based on a set of broadcasted rules.
 */
@Slf4j
public class DynamicKeyFunction2
        extends BroadcastProcessFunction<JSONObject, WarnRule, Keyed<JSONObject, String, String>> {

    private RuleCounterGauge ruleCounterGauge;

    @Override
    public void open(Configuration parameters) {
        ruleCounterGauge = new RuleCounterGauge();
        getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGauge);
    }

    @Override
    public void processElement(
            JSONObject event, ReadOnlyContext ctx, Collector<Keyed<JSONObject, String, String>> out)
            throws Exception {
        ctx.output(RulesEvaluator.Descriptors.statisticsSinkTag, event.toJSONString());
        ReadOnlyBroadcastState<String, WarnRule> rulesState =
                ctx.getBroadcastState(RulesEvaluator.Descriptors.rulesDescriptor);
        forkEventForEachGroupingKey(event, rulesState, out, ctx);
    }

    @Override
    public void processBroadcastElement(
            WarnRule warnRule, Context ctx, Collector<Keyed<JSONObject, String, String>> out) throws Exception {
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

    private void forkEventForEachGroupingKey(
            JSONObject event,
            ReadOnlyBroadcastState<String, WarnRule> rulesState,
            Collector<Keyed<JSONObject, String, String>> out, ReadOnlyContext ctx)
            throws Exception {
        int ruleCounter = 0;
        Long startTime = event.getLong("startTime");
        for (Entry<String, WarnRule> entry : rulesState.immutableEntries()) {
            final WarnRule warnRule = entry.getValue();
            String aggregateValue = event.getString(warnRule.getAggregateFieldName());
            if (isMatch(event, warnRule)) {
                String key = KeysExtractor.getKey(warnRule.getGroupingKeyNames(), warnRule.getDefaultGroupingKeyNames(), warnRule.getRuleId(), event);
                JSONObject in = KeysExtractor.getJSONObject(warnRule.getGroupingKeyNames(), warnRule.getDefaultGroupingKeyNames(), warnRule.getRuleId(), event);
                Keyed<JSONObject, String, String> keyed = new Keyed<>(in, key, warnRule.getRuleId());
                keyed.setStartTime(startTime);
                keyed.setAggregateValue(aggregateValue);
                out.collect(keyed);
                ruleCounter++;
            }
        }
        ruleCounterGauge.setValue(ruleCounter);
    }

    private boolean isMatch(JSONObject object, WarnRule warnRule) {
        List<FieldRule> fieldRules = warnRule.getConditionList();
        if (Objects.isNull(fieldRules) || fieldRules.isEmpty()) {
            return true;
        }

        for (FieldRule fieldRule : fieldRules) {
            final FieldRule.OperatorType operatorType = fieldRule.getOperatorType();
            if (Objects.isNull(operatorType)) {
                continue;
            }

            if (!fieldRule.apply(object)) {
                return false;
            }
        }

        return true;
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
