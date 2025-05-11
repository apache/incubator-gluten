package org.apache.gluten.util;

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
  private transient Class<? extends NativeBean> clazz;
  private transient T bean;

  private Velox4JBean(T bean) {
    this.clazz = bean.getClass();
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
    out.writeObject(clazz);
    out.writeUTF(json);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    clazz = (Class<? extends NativeBean>) in.readObject();
    bean = (T) Serde.fromJson(in.readUTF(), clazz);
  }
}
