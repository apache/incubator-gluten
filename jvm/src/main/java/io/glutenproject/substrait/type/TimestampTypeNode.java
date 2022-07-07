package io.glutenproject.substrait.type;

import io.substrait.proto.Type;

import java.io.Serializable;

public class TimestampTypeNode implements TypeNode, Serializable {
  private final Boolean nullable;

  TimestampTypeNode(Boolean nullable) {
    this.nullable = nullable;
  }

  @Override
  public Type toProtobuf() {
    Type.Timestamp.Builder timestampBuilder = Type.Timestamp.newBuilder();
    if (nullable) {
      timestampBuilder.setNullability(Type.Nullability.NULLABILITY_NULLABLE);
    } else {
      timestampBuilder.setNullability(Type.Nullability.NULLABILITY_REQUIRED);
    }

    Type.Builder builder = Type.newBuilder();
    builder.setTimestamp(timestampBuilder.build());
    return builder.build();
  }
}
