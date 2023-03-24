package io.glutenproject.substrait.type;

import io.substrait.proto.Type;

import java.io.Serializable;
import java.util.ArrayList;

public class MapNode implements TypeNode, Serializable {
  private final Boolean nullable;
  private final TypeNode keyType;
  private final TypeNode valType;

  MapNode(Boolean nullable, TypeNode keyType, TypeNode valType) {
    this.nullable = nullable;
    this.keyType = keyType;
    this.valType = valType;
  }

  // It's used in ExplodeTransformer to determine output datatype from children.
  public TypeNode getNestedType() {
    ArrayList<TypeNode> types = new ArrayList<>();
    types.add(keyType);
    types.add(valType);
    return TypeBuilder.makeStruct(false, types);
  }

  @Override
  public Type toProtobuf() {
    Type.Map.Builder mapBuilder = Type.Map.newBuilder();
    mapBuilder.setKey(keyType.toProtobuf());
    mapBuilder.setValue(valType.toProtobuf());
    mapBuilder.setNullability(
        nullable ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED);

    Type.Builder builder = Type.newBuilder();
    builder.setMap(mapBuilder.build());
    return builder.build();
  }

}
