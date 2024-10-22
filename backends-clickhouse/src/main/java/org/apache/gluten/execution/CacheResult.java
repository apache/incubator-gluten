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
package org.apache.gluten.execution;

import java.io.Serializable;

public class CacheResult implements Serializable {
  public enum Status {
    RUNNING(0),
    SUCCESS(1),
    ERROR(2);

    private final int value;

    Status(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public static Status fromInt(int value) {
      for (Status myEnum : Status.values()) {
        if (myEnum.getValue() == value) {
          return myEnum;
        }
      }
      throw new IllegalArgumentException("No enum constant for value: " + value);
    }
  }

  private final Status status;
  private final String message;

  public CacheResult(int status, String message) {
    this.status = Status.fromInt(status);
    this.message = message;
  }

  public CacheResult(Status status, String message) {
    this.status = status;
    this.message = message;
  }

  public Status getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }
}
