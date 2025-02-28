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
package org.apache.gluten.qt.support;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A utility class for parsing and storing durations from string representations like "123.45 s",
 * "12 ms", or "1.5 h". If the input string cannot be parsed or is empty, the duration defaults to
 * zero.
 */
public class DurationMetric {
  private final Duration duration;
  private static final Pattern PATTERN = Pattern.compile("([0-9.]+) (ms|h|m|s)");

  public DurationMetric() {
    this.duration = Duration.ZERO;
  }

  public DurationMetric(String string) {
    this.duration = parseDuration(string);
  }

  private Duration parseDuration(String string) {
    if (string.isEmpty()) {
      return Duration.ZERO;
    }

    String[] splitString = string.split("\n", -1);
    if (splitString.length > 2) {
      return Duration.ZERO;
    }

    String valueString = splitString[splitString.length - 1];

    Matcher matcher = PATTERN.matcher(valueString);

    if (matcher.find() && matcher.groupCount() == 2) {
      String value = matcher.group(1);
      String unit = matcher.group(2);

      try {
        float parsedValue = Float.parseFloat(value);
        switch (unit) {
          case "h":
            return Duration.ofMillis((long) (parsedValue * 60 * 60 * 1000));
          case "m":
            return Duration.ofMillis((long) (parsedValue * 60 * 1000));
          case "s":
            return Duration.ofMillis((long) (parsedValue * 1000));
          case "ms":
            return Duration.ofMillis((long) parsedValue);
          default:
            return Duration.ZERO;
        }
      } catch (NumberFormatException e) {
        return Duration.ZERO;
      }
    } else {
      return Duration.ZERO;
    }
  }

  public Duration getDuration() {
    return duration;
  }
}
