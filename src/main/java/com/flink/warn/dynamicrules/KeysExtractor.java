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
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;

/** Utilities for dynamic keys extraction by field name. */
public class KeysExtractor {

  /**
   * Extracts and concatenates field values by names.
   *
   * @param keyNames list of field names
   * @param object target for values extraction
   */
  public static String getKey(List<String> keyNames, Object object, String ruleId)
      throws NoSuchFieldException, IllegalAccessException {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    if (keyNames.size() > 0) {
      sb.append("warnRuleId=" + ruleId + ";");
      Iterator<String> it = keyNames.iterator();
      appendKeyValue(sb, object, it.next());

      while (it.hasNext()) {
        sb.append(";");
        appendKeyValue(sb, object, it.next());
      }
    }
    sb.append("}");
    return sb.toString();
  }

  public static String getKey(List<String> keyNames, List<String> keyNames2, String ruleId, Object object)
          throws NoSuchFieldException, IllegalAccessException {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    if (keyNames.size() > 0) {
      sb.append("warnRuleId=" + ruleId + ";");
      Iterator<String> it = keyNames.iterator();
      appendKeyValue(sb, object, it.next());

      while (it.hasNext()) {
        sb.append(";");
        appendKeyValue(sb, object, it.next());
      }

      Iterator<String> it2 = keyNames2.iterator();
      while (it2.hasNext()) {
        sb.append(";");
        appendKeyValue(sb, object, it2.next());
      }
    }
    sb.append("}");
    return sb.toString();
  }

  public static JSONObject getJSONObject(List<String> keyNames, List<String> keyNames2, String ruleId, Object object) {
    JSONObject jsonObject = (JSONObject) object;
    JSONObject result = new JSONObject();
    Iterator<String> it = keyNames.iterator();
    Iterator<String> it2 = keyNames2.iterator();
    while (it.hasNext()){
      String next = it.next();
      result.put(next, jsonObject.getString(next));
    }

    while (it2.hasNext()){
      String next = it2.next();
      result.put(next, jsonObject.getString(next));
    }
    result.put("warnRuleId", ruleId);
    return result;
  }

  private static void appendKeyValue(StringBuilder sb, Object object, String fieldName)
      throws IllegalAccessException, NoSuchFieldException {
    sb.append(fieldName);
    sb.append("=");
    String fieldAsString = FieldsExtractor.getFieldAsString(object, fieldName);
    if(StringUtils.isBlank(fieldAsString)){
      sb.append("");
    }else{
      sb.append(fieldAsString);
    }
  }
}
