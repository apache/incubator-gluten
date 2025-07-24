/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.table.runtime.config;

import io.github.zhztheplayer.velox4j.config.Config;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.config.TableConfigOptions;

import java.util.HashMap;
import java.util.Map;

public class VeloxQueryConfig {

  public static ConfigOption<Boolean> ADJUST_TIMESTMP_TO_SESSION_TIMEZONE =
      ConfigOptions.key("velox.adjust_timestamp_to_session_timezone")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "adjust the timestamp accroding to the given session timezone in the velox backend");

  private static final String keyVeloxAdjustTimestampToSessionTimeZone =
      "adjust_timestamp_to_session_timezone";
  private static final String keyVeloxSessionTimezone = "session_timezone";

  public static Config getConfig(RuntimeContext context) {
    if (!(context instanceof StreamingRuntimeContext)) {
      return Config.empty();
    }
    Configuration config = ((StreamingRuntimeContext) context).getJobConfiguration();
    Map<String, String> configMap = new HashMap<>();
    if (config.get(ADJUST_TIMESTMP_TO_SESSION_TIMEZONE)) {
      String localTimeZone = config.get(TableConfigOptions.LOCAL_TIME_ZONE);
      configMap.put(keyVeloxAdjustTimestampToSessionTimeZone, "true");
      configMap.put(keyVeloxSessionTimezone, localTimeZone);
    } else {
      configMap.put(keyVeloxAdjustTimestampToSessionTimeZone, "false");
    }
    return Config.create(configMap);
  }
}
