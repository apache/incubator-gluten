package io.glutenproject.substrait.type;

import io.substrait.proto.Type;
import java.io.Serializable;

public class ListNode implements TypeNode, Serializable {
  private final Boolean nullable;
  private final TypeNode nestedType;

  ListNode(Boolean nullable, TypeNode nestedType) {
    this.nullable = nullable;
    this.nestedType = nestedType;
  }

  @Override
  public Type toProtobuf() {
    Type.List.Builder listBuilder = Type.List.newBuilder();
    listBuilder.setType(nestedType.toProtobuf());
    listBuilder.setNullability(
        nullable ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED);

    Type.Builder builder = Type.newBuilder();
    builder.setList(listBuilder.build());
    return builder.build();
  }
}
