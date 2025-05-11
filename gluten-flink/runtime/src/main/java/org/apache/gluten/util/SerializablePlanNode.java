package org.apache.gluten.util;

import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.serde.Serde;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A serializable container for an instance Velox4J's PlanNode, with the Java
 * Serializable interface implemented.
 * <p>
 * Velox4J's JSON serde is well-maintained so we simply route the serialization
 * calls to the JSON serdes.
 */
public class SerializablePlanNode implements Serializable {
  private transient PlanNode planNode;

  public SerializablePlanNode(PlanNode planNode) {
    this.planNode = planNode;
  }

  public PlanNode node() {
    return planNode;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    final String json = Serde.toJson(planNode);
    out.writeUTF(json);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    final String json = in.readUTF();
    planNode = Serde.fromJson(json, PlanNode.class);
  }

}
