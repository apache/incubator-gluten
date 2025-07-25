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
package org.apache.gluten.table.runtime.typeutils;

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Serializer for {@link RowVector}. */
@Internal
public class GlutenRowVectorSerializer extends TypeSerializer<StatefulRecord> {
  private static final long serialVersionUID = 1L;
  private final RowType rowType;
  private MemoryManager memoryManager;
  private Session session;

  public GlutenRowVectorSerializer(RowType rowType) {
    this.rowType = rowType;
  }

  @Override
  public TypeSerializer<StatefulRecord> duplicate() {
    return new GlutenRowVectorSerializer(rowType);
  }

  @Override
  public StatefulRecord createInstance() {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public void serialize(StatefulRecord record, DataOutputView target) throws IOException {
    String vectorStr = record.getRowVector().serialize();
    target.writeInt(vectorStr.getBytes().length);
    target.write(vectorStr.getBytes());
  }

  private static volatile Session sharedSession;
  private static final Object LOCK = new Object();

  public static void setSessionForCurrentOperator(Session session) {
    synchronized (LOCK) {
      sharedSession = session;
    }
  }

  @Override
  public StatefulRecord deserialize(DataInputView source) throws IOException {
    Session currentSession;
    synchronized (LOCK) {
      currentSession = sharedSession;
      if (currentSession == null) {
        throw new IllegalStateException("No Velox session available for serialization");
      }
    }

    int len = source.readInt();
    byte[] str = new byte[len];
    source.readFully(str);
    RowVector rowVector =
        currentSession.baseVectorOps().deserializeOne(new String(str)).asRowVector();
    StatefulRecord record = new StatefulRecord(null, 0, 0, false, -1);
    record.setRowVector(rowVector);
    return record;
  }

  @Override
  public StatefulRecord deserialize(StatefulRecord reuse, DataInputView source) throws IOException {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public StatefulRecord copy(StatefulRecord from) {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public StatefulRecord copy(StatefulRecord from, StatefulRecord reuse) {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public void copy(DataInputView source, DataOutputView target) throws IOException {
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof GlutenRowVectorSerializer) {
      GlutenRowVectorSerializer other = (GlutenRowVectorSerializer) obj;
      return rowType.equals(other.rowType);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return rowType.hashCode();
  }

  @Override
  public boolean isImmutableType() {
    return false;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public TypeSerializerSnapshot<StatefulRecord> snapshotConfiguration() {
    return new RowVectorSerializerSnapshot(rowType);
  }

  /** {@link TypeSerializerSnapshot} for Gluten RowVector.. */
  public static final class RowVectorSerializerSnapshot
      implements TypeSerializerSnapshot<StatefulRecord> {
    private static final int CURRENT_VERSION = 1;

    private RowType rowType;

    @SuppressWarnings("unused")
    public RowVectorSerializerSnapshot() {
      // this constructor is used when restoring from a checkpoint/savepoint.
    }

    RowVectorSerializerSnapshot(RowType rowType) {
      this.rowType = rowType;
    }

    @Override
    public int getCurrentVersion() {
      return CURRENT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {}

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
        throws IOException {}

    @Override
    public GlutenRowVectorSerializer restoreSerializer() {
      return new GlutenRowVectorSerializer(rowType);
    }

    @Override
    public TypeSerializerSchemaCompatibility<StatefulRecord> resolveSchemaCompatibility(
        TypeSerializerSnapshot<StatefulRecord> oldSerializerSnapshot) {
      return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }
  }
}
