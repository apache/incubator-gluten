package io.glutenproject.substrait.type;

import io.substrait.proto.Type;

import java.io.Serializable;

public class NothingNode implements TypeNode, Serializable {
  NothingNode() {
  }

  @Override
  public Type toProtobuf() {
    Type.Nothing.Builder nothingBuilder = Type.Nothing.newBuilder();
    Type.Builder builder = Type.newBuilder();
    builder.setNothing(nothingBuilder.build());
    return builder.build();
  }
}
