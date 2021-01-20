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
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;

import java.util.HashSet;
import java.util.Set;

class ProcessingUtils {

    static void handleRuleBroadcast(WarnRule warnRule, BroadcastState<String, WarnRule> broadcastState)
            throws Exception {
        if(warnRule.getRuleState() == null){
            return;
        }
        switch (warnRule.getRuleState()) {
            case ACTIVE:
            case PAUSE:
                broadcastState.put(warnRule.getRuleId(), warnRule);
                break;
            case DELETE:
                broadcastState.remove(warnRule.getRuleId());
                break;
            default:
                break;
        }
    }

    static <K, V> Set<V> addToStateValuesSet(MapState<K, Set<V>> mapState, K key, V value)
            throws Exception {

        Set<V> valuesSet = mapState.get(key);

        if (valuesSet != null) {
            valuesSet.add(value);
        } else {
            valuesSet = new HashSet<>();
            valuesSet.add(value);
        }
        mapState.put(key, valuesSet);
        return valuesSet;
    }

    static Long addCountToState(MapState<Long, Long> mapState, Long key) throws Exception {
        Long count = mapState.get(key) == null ? 0L : mapState.get(key);
        mapState.put(key, count);
        return count;
    }

}
