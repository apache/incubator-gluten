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

package org.apache.gluten.util;

import com.google.common.base.Preconditions;
import io.github.zhztheplayer.velox4j.serde.NativeBean;
import io.github.zhztheplayer.velox4j.serde.Serde;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A serializable container for an instance of Velox4J's bean class, with the Java
 * Serializable interface implemented.
 * <p>
 * Velox4J's JSON serde is well-maintained so we simply route the serialization
 * calls to the JSON serdes.
 */
public class Velox4JBean<T extends NativeBean> implements Serializable {
  private transient String className;
  private transient T bean;

  private Velox4JBean(T bean) {
    this.className = bean.getClass().getName();
    this.bean = bean;
  }

  public static <T extends NativeBean> Velox4JBean<T> of(T bean) {
    return new Velox4JBean<>(bean);
  }

  public T get() {
    return bean;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    final String json = Serde.toJson(bean);
    out.writeUTF(className);
    out.writeUTF(json);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    className = in.readUTF();
    final Class<?> clazz = Class.forName(className);
    Preconditions.checkState(NativeBean.class.isAssignableFrom(clazz),
        "Class %s is not a NativeBean", className);
    bean = (T) Serde.fromJson(in.readUTF(), (Class<? extends NativeBean>) clazz);
  }
}
